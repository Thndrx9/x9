# engine_runtime.py

import asyncio
import signal
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils import load_symbols
from websocket_connect import run_market_data_feeds
from ohlc import OHLCCollector, PreviousCandleGuard
from depth_store import DepthStore
from tick_writer import TickWriter, HistoryCandleStore
from indicators import IndicatorEngine
from signal_generator import SignalGenerator
from executor import TradeExecutor
from market_time import (
    MARKET_OPEN,
    MARKET_CLOSE,
    is_market_open,
    is_trading_day,
    now_kolkata,
    tz_kolkata,
    refresh_trading_calendar,
)

load_dotenv()

# If the market opens within this many seconds, run backfill in parallel
# with the WebSocket (can't afford to block that close to open)
PARALLEL_BACKFILL_THRESHOLD_SECS = 60

# Mid-session auto-heal retry tuning — see _on_reconnect()'s docstring.
_HEAL_MAX_ATTEMPTS       = 3
_HEAL_RETRY_BACKOFF_SECS = 3   # attempt N waits N * this many seconds before retrying

# How long shutdown will WAIT (not cancel — see the shutdown block at
# the bottom of run_engine()) for an in-flight backfill/heal task to
# finish before giving up and logging loudly instead.
BACKFILL_SHUTDOWN_TIMEOUT_SECS = int(os.getenv("BACKFILL_SHUTDOWN_TIMEOUT_SECS", "60"))

# Where DAY_STARTED / RECONNECTED / DISCONNECTED events get logged for
# the Quote-mode websocket connection (the one backfill/gap-detection
# cares about for figuring out when the feed actually dropped).
CONN_LOG_DIR = os.getenv("CONN_LOG_DIR", "connection_logs")

# How many trading days of local SQLite tick history to retain (pruned
# at market close / on shutdown — see TickWriter.prune_older_than_days).
TICK_CACHE_RETENTION_DAYS = int(os.getenv("TICK_CACHE_RETENTION_DAYS", "3"))
# Depth's local cache only ever needs to cover what backfill actually
# looks back over — see BackfillManager._compute_depth_lookback_start
# (1 trading day, deliberately much shorter than quote's window since
# depth rows are far heavier). Keeping depth data locally any longer
# than this is pure wasted disk with no corresponding use, so it gets
# its own, separate, much shorter retention setting.
DEPTH_CACHE_RETENTION_DAYS = int(os.getenv("DEPTH_CACHE_RETENTION_DAYS", "1"))


def _next_market_open(dt: datetime) -> datetime:
    day = dt.date()
    while True:
        candidate = datetime.combine(day, MARKET_OPEN, tzinfo=tz_kolkata)
        if candidate > dt and is_trading_day(candidate):
            return candidate
        day += timedelta(days=1)


async def _run_backfill_safe(ohlc, symbols, backfill_lock: asyncio.Lock):
    """
    Wraps ensure_backfill_async so a failure is actually visible.
    Without this, an exception inside a fire-and-forget asyncio.Task
    (like the parallel-backfill task below) is silently swallowed and
    only surfaces much later as an unhelpful "exception was never
    retrieved" warning — or never at all.

    Holds backfill_lock for the duration of the actual fetch — see
    run_engine()'s "Backfill/heal serialization" note just above where
    the lock is created. Without this, a mid-session auto-heal firing
    while this is still running competes with it for the GIL and for
    main-db Postgres connections — both synchronous, CPU-heavy fetches
    running concurrently is exactly what caused a websocket ping-timeout
    disconnect in practice (the GIL starvation delayed keepalive
    responses), which is the very thing the heal exists to recover
    from. Serializing them means a heal simply waits its turn instead
    of racing an in-flight backfill.
    """
    try:
        async with backfill_lock:
            await ohlc.ensure_backfill_async(symbols)
    except Exception:
        import traceback
        print("[BACKFILL][FATAL] Backfill task crashed:", flush=True)
        traceback.print_exc()


async def _run_backfill_and_release(ohlc, symbols, tick_writer, backfill_lock: asyncio.Lock):
    """
    Same as _run_backfill_safe(), plus releasing tick_writer's
    hold/release gate afterward. Used when backfill runs in PARALLEL
    with the live websocket (CASE 1/2 below): tick_writer.hold() is
    called right before start_live_tasks(), so any live ticks that
    arrive while this backfill's own PG tick-fetch is still in flight
    get buffered in RAM instead of racing those (earlier-timestamp)
    historical rows onto disk out of order. Releasing here — after the
    backfill (and its inline tick-cache writes) has fully finished —
    flushes that buffer in the correct order and resumes direct writes.
    """
    try:
        await _run_backfill_safe(ohlc, symbols, backfill_lock)
    finally:
        tick_writer.release()


async def _catchup_and_release(ohlc, symbols, tick_writer, backfill_lock: asyncio.Lock):
    """
    Standalone pre-live catch-up: fetches whatever PG ticks landed
    since the local SQLite tick cache's last row and writes them
    straight to it, then releases the hold/release gate. Used in CASE 3
    below, where the full historical backfill already ran and
    completed earlier (while the market was closed, no live ticks
    existed yet) — this only needs to cover the short gap between then
    and the websocket actually going live.

    Also holds backfill_lock — see _run_backfill_safe()'s docstring for
    why any DB-heavy backfill/heal work needs to be serialized.
    """
    try:
        from backfill_manager import BackfillManager
        backfill = BackfillManager(ohlc)
        async with backfill_lock:
            await asyncio.to_thread(backfill.cache_recent_ticks_to_sqlite, symbols)
    except Exception:
        import traceback
        print("[TICK_CACHE][WARN] pre-live catch-up fetch failed:", flush=True)
        traceback.print_exc()
    finally:
        tick_writer.release()


async def _auto_stop_after_close(stop_event, poll_secs=30):
    """
    Automatically triggers graceful shutdown POST_CLOSE_STOP_MINUTES
    (default 2) after TODAY's market close — instead of sitting there
    with a live-but-idle WebSocket connection until someone kills the
    process manually. Assumes a scheduler (cron/systemd/etc.) restarts
    the process fresh before the next session; this does NOT itself wait
    for or resume at the next open.

    Only ever fires on an actual trading day, and only once today's
    close + the grace period has passed — so it never triggers during
    the pre-open wait (CASE 3 in run_engine below), and never triggers
    at all on a weekend/holiday.
    """
    stop_after_min = int(os.getenv("POST_CLOSE_STOP_MINUTES", "2"))

    while not stop_event.is_set():
        now = now_kolkata()

        if is_trading_day(now.date()):
            close_dt = datetime.combine(now.date(), MARKET_CLOSE, tzinfo=tz_kolkata)
            stop_at  = close_dt + timedelta(minutes=stop_after_min)

            if now >= stop_at:
                print(
                    f"[SYSTEM] Market closed {stop_after_min}+ minute(s) ago — "
                    f"stopping.",
                    flush=True,
                )
                stop_event.set()
                return

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=poll_secs)
        except asyncio.TimeoutError:
            pass


async def run_engine(enable_trading: bool):
    print("[SYSTEM] Starting trading system", flush=True)

    # Blocking network call (urllib, up to 10s) — keep off the event loop.
    # Fails silently internally, so this is safe even with no internet access.
    await asyncio.to_thread(refresh_trading_calendar)

    symbols = load_symbols("symbols.csv")
    if not symbols:
        print("[SYSTEM][ERROR] No valid symbols found in symbols.csv", flush=True)
        return
    api_key = os.getenv("API_KEY")

    # Local tick cache, shared by quote ticks (via ohlc) and depth
    # snapshots (via depth_store) — see tick_writer.py. Backend is
    # SQLite by default; set TICK_WRITER_BACKEND=postgres in .env to
    # use a local PostgreSQL instance instead — TickWriter() itself
    # picks the right implementation, nothing here needs to branch.
    tick_writer = TickWriter(base_dir="tickdata")
    # Local SQLite cache for candles fetched from the history (fallback)
    # DB — see tick_writer.py's HistoryCandleStore. Same folder as the tick cache.
    history_store    = HistoryCandleStore(base_dir="tickdata")
    ohlc             = OHLCCollector(tick_writer=tick_writer, history_store=history_store, conn_log_dir=CONN_LOG_DIR)
    depth_store      = DepthStore(tick_writer=tick_writer)
    # Let BackfillManager seed the RAM-window (see DepthStore's
    # DEPTH_RAM_WINDOW_MINUTES) from freshly backfilled/corrected local
    # data right after depth backfill completes — otherwise the window
    # starts empty on every restart and only fills gradually as live
    # ticks arrive, leaving up to DEPTH_RAM_WINDOW_MINUTES of "no
    # history yet" right when it's most needed (right after a restart).
    ohlc.depth_store = depth_store
    indicators       = IndicatorEngine(ohlc)
    prev_candle_guard = PreviousCandleGuard(ohlc)
    signal_generator  = SignalGenerator(ohlc) if enable_trading else None
    executor          = TradeExecutor(ohlc) if enable_trading else None

    stop_event = asyncio.Event()

    def on_signal():
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT,  on_signal)
    loop.add_signal_handler(signal.SIGTERM, on_signal)

    tasks = []

    # Tasks that hold backfill_lock (_run_backfill_and_release,
    # _catchup_and_release below) spend nearly all their time inside
    # asyncio.to_thread(...) running SYNCHRONOUS BackfillManager/
    # psycopg2/pandas code. Cancelling such a task only detaches the
    # *awaiting coroutine* — a running Python thread can't be
    # force-killed, so the actual DB fetch/write underneath keeps going
    # regardless, completely unsupervised. Tracked separately from
    # `tasks` so shutdown WAITS for these instead of cancelling them —
    # see the shutdown block at the bottom of this function.
    backfill_tasks = []

    # Mid-session heals (see _on_reconnect below) are fired from inside
    # websocket_connect.py as bare asyncio.create_task() calls with no
    # caller ever holding a reference — the standard fire-and-forget
    # pattern, but it means shutdown here had literally no way to even
    # know a heal was in flight, let alone wait for it. This set is
    # threaded through run_market_data_feeds() -> websocket_client() so
    # every heal task gets registered here the moment it's created (and
    # removes itself once done via add_done_callback) — see
    # websocket_client()'s docstring for the full story. Same
    # "can't cancel, can only wait" situation as backfill_tasks, since
    # a heal is just another asyncio.to_thread(...) wrapper.
    heal_tasks = set()

    # Serializes every synchronous, DB-heavy backfill/heal operation
    # (startup backfill, pre-live catch-up, mid-session auto-heal) so
    # at most one runs at a time — see _run_backfill_safe()'s docstring
    # for why running them concurrently is a real problem (GIL
    # contention between synchronous psycopg2/pandas work in different
    # threads was the actual cause of a websocket ping-timeout
    # disconnect in practice, and a heal firing to recover from that
    # disconnect while the original backfill is still in flight only
    # compounds the same contention right when it matters most).
    backfill_lock = asyncio.Lock()

    async def indicator_loop():
        while not stop_event.is_set():
            for s in symbols:
                indicators.update(s["symbol"])
            await asyncio.sleep(1)

    async def _on_reconnect(mode_label: str, disconnect_dt, reconnect_dt):
        """
        Fired by websocket_connect.py right after a REAL mid-session
        reconnect (never the day's first connect). Runs a small,
        targeted backfill for just [disconnect_dt, reconnect_dt] —
        NOT a full re-scan — for whichever feed actually reconnected
        (mode_label is "Quote" or "Depth", independently).

        Holds tick_writer's gate for the duration — same reasoning as
        the startup CASE 1/2 backfill: live ticks for this mode keep
        arriving the whole time the heal's own catch-up rows are being
        fetched and written, and without the hold those live ticks
        could land on disk BEFORE the (earlier-timestamped) catch-up
        rows the heal is still fetching, putting them out of order.
        release() is reference-counted specifically so this is safe
        even if Quote's and Depth's heals overlap (both connections
        dropping around the same time is a real scenario, not an edge
        case) — see SQLiteTickWriter.release()'s docstring. (On the
        Postgres backend hold/release are no-ops — see tick_writer.py's
        PostgresTickWriter docstring — since live/backfill write to
        separate tables there; kept here so this code works unchanged
        on either backend.)

        The actual DB work is synchronous (psycopg2/sqlite3), so it's
        offloaded to a thread — this coroutine itself never blocks the
        event loop, meaning the live feed keeps flowing normally while
        the heal runs in the background.

        Retries up to _HEAL_MAX_ATTEMPTS times with a short backoff
        before giving up — a failed heal used to just print one [WARN]
        and vanish, permanently losing that exact disconnect window
        with nothing but a log line as evidence. Each attempt (and any
        retry) acquires backfill_lock, so a heal never runs concurrently
        with an in-flight startup backfill/catch-up — see
        _run_backfill_safe()'s docstring for why that matters (GIL
        contention between concurrent synchronous DB fetches was the
        original trigger for the disconnect this heal is responding
        to).
        """
        mode = mode_label.lower()
        window_label = f"{disconnect_dt.strftime('%H:%M:%S')}–{reconnect_dt.strftime('%H:%M:%S')} IST"

        def _heal():
            from backfill_manager import BackfillManager
            backfill = BackfillManager(ohlc)
            try:
                backfill.run_targeted_heal(symbols, mode, disconnect_dt, reconnect_dt)
            finally:
                try:
                    backfill.conn.close()
                except Exception:
                    pass
                if backfill.conn_history is not None:
                    try:
                        backfill.conn_history.close()
                    except Exception:
                        pass

        tick_writer.hold()
        try:
            for attempt in range(1, _HEAL_MAX_ATTEMPTS + 1):
                try:
                    async with backfill_lock:
                        await asyncio.to_thread(_heal)
                    break  # success — stop retrying
                except Exception as exc:
                    if attempt < _HEAL_MAX_ATTEMPTS:
                        backoff = _HEAL_RETRY_BACKOFF_SECS * attempt
                        print(
                            f"[BACKFILL][WARN] mid-session auto-heal ({mode_label}) "
                            f"attempt {attempt}/{_HEAL_MAX_ATTEMPTS} failed for "
                            f"{window_label}: {exc} — retrying in {backoff}s",
                            flush=True,
                        )
                        await asyncio.sleep(backoff)
                    else:
                        # Exhausted every retry — this window is now
                        # PERMANENTLY unhealed unless someone notices
                        # this line and re-runs a targeted fetch for it
                        # by hand. [ERROR], not [WARN], and the exact
                        # window spelled out, on purpose — this should
                        # not be easy to miss in the log.
                        print(
                            f"[BACKFILL][ERROR] mid-session auto-heal ({mode_label}) "
                            f"failed after {_HEAL_MAX_ATTEMPTS} attempts — gap "
                            f"{window_label} is UNHEALED: {exc}",
                            flush=True,
                        )
        finally:
            # Always release, even on failure — otherwise a failed heal
            # would leave this mode's live ticks buffering in RAM
            # forever, never actually reaching disk.
            tick_writer.release()

    def start_live_tasks():
        tasks.append(asyncio.create_task(
            run_market_data_feeds(
                api_key, symbols,
                conn_log_dir=CONN_LOG_DIR,
                depth_levels=int(os.getenv("DEPTH_LEVELS", "5")),
                on_reconnect=_on_reconnect,
                background_tasks=heal_tasks,
            )
        ))
        tasks.append(asyncio.create_task(ohlc.run()))
        tasks.append(asyncio.create_task(depth_store.run()))
        tasks.append(asyncio.create_task(indicator_loop()))
        # Only matters once actually live for today — starting this
        # earlier (e.g. unconditionally at process start) would fire
        # immediately and wrongly short-circuit CASE 3's "waiting for
        # tomorrow's open" path if the process happens to be started
        # after today's close specifically to sit and wait overnight.
        tasks.append(asyncio.create_task(_auto_stop_after_close(stop_event)))

        if executor:
            tasks.append(asyncio.create_task(signal_generator.run()))
            tasks.append(asyncio.create_task(executor.run()))
        else:
            tasks.append(asyncio.create_task(ohlc.monitor_loop(symbols, indicators)))

    # ── Backfill sequencing ───────────────────────────────────────────
    #
    #   CASE 1 — Market already open
    #       Start WebSocket immediately so no ticks are missed.
    #       Run backfill as a parallel background task.
    #
    #   CASE 2 — Market opens within PARALLEL_BACKFILL_THRESHOLD_SECS (60 s)
    #       Too close to open to wait for backfill.
    #       Start WebSocket immediately, backfill runs in parallel.
    #
    #   CASE 3 — Market closed, opens in > 60 s
    #       Plenty of time. Await backfill completion first.
    #       Then wait until 30 s before open, then start WebSocket.
    #
    # ─────────────────────────────────────────────────────────────────

    now = now_kolkata()

    if is_market_open(now):
        print(
            "[SYSTEM] Market is open — starting WebSocket immediately "
            "and running backfill in parallel.",
            flush=True,
        )
        # Backfill's own PG tick-fetch runs in parallel with live ticks
        # from here on — hold the gate so they can't land out of order
        # (see _run_backfill_and_release's docstring).
        tick_writer.hold()
        start_live_tasks()
        backfill_tasks.append(asyncio.create_task(_run_backfill_and_release(ohlc, symbols, tick_writer, backfill_lock)))

    else:
        next_open      = _next_market_open(now)
        secs_to_open   = (next_open - now).total_seconds()

        if secs_to_open <= PARALLEL_BACKFILL_THRESHOLD_SECS:
            print(
                f"[SYSTEM] Market opens in {secs_to_open:.0f}s — "
                f"starting WebSocket now and running backfill in parallel.",
                flush=True,
            )
            tick_writer.hold()
            start_live_tasks()
            backfill_tasks.append(asyncio.create_task(_run_backfill_and_release(ohlc, symbols, tick_writer, backfill_lock)))

        else:
            # Market is closed and not imminent — block until backfill finishes
            print("[SYSTEM] Market closed — running backfill first.", flush=True)
            await _run_backfill_safe(ohlc, symbols, backfill_lock)
            print("[SYSTEM] Backfill complete — waiting for market to open.", flush=True)

            ws_start     = next_open - timedelta(seconds=30)
            wait_seconds = (ws_start - now_kolkata()).total_seconds()

            if wait_seconds > 0:
                print(
                    f"[SYSTEM] WebSocket will start at "
                    f"{ws_start.strftime('%Y-%m-%d %H:%M:%S %Z')}",
                    flush=True,
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
                except asyncio.TimeoutError:
                    pass

            if not stop_event.is_set():
                print("[SYSTEM] Starting WebSocket 30s before market open.", flush=True)
                # Blocking backfill above already cached PG ticks to
                # SQLite directly (no live producer existed yet, so no
                # ordering risk there). From here on live ticks may
                # start arriving any moment — hold the gate, start the
                # feed, then run one last small catch-up fetch for
                # whatever landed in PG since backfill finished.
                tick_writer.hold()
                start_live_tasks()
                backfill_tasks.append(asyncio.create_task(_catchup_and_release(ohlc, symbols, tick_writer, backfill_lock)))

    await stop_event.wait()

    if executor:
        await executor.shutdown_async()

    # `tasks` only ever holds plain async loops (websocket feed dispatch,
    # ohlc.run(), indicator_loop(), etc.) — none of them run synchronous
    # work on a background thread, so cancelling them is genuinely
    # effective and immediate.
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # backfill_tasks and heal_tasks are different: both spend nearly all
    # their time inside asyncio.to_thread(...) running synchronous
    # BackfillManager/psycopg2/pandas code, which — being a real OS
    # thread — cannot be force-stopped by cancelling the asyncio task
    # that's awaiting it. Cancelling here would just detach our only
    # reference and let that thread keep writing to the DB completely
    # unsupervised, with the log printing "Shutdown complete" while it's
    # still going. Since it truly can't be stopped, the honest thing to
    # do is WAIT for it — bounded, so a stuck thread can't hang the
    # process forever — and say so loudly if the timeout is hit, instead
    # of silently declaring victory either way.
    pending_thread_backed = [t for t in (*backfill_tasks, *heal_tasks) if not t.done()]
    if pending_thread_backed:
        print(
            f"[SYSTEM] Waiting up to {BACKFILL_SHUTDOWN_TIMEOUT_SECS}s for "
            f"{len(pending_thread_backed)} in-flight backfill/heal task(s) to finish "
            f"(can't be force-stopped mid-write)...",
            flush=True,
        )
        done, still_pending = await asyncio.wait(pending_thread_backed, timeout=BACKFILL_SHUTDOWN_TIMEOUT_SECS)
        if still_pending:
            print(
                f"[SYSTEM][WARN] {len(still_pending)} backfill/heal task(s) still running after "
                f"{BACKFILL_SHUTDOWN_TIMEOUT_SECS}s — they will keep writing to the local/main DB "
                f"in the background even though this process is reporting shutdown below. This is "
                f"a real Python limitation (a running OS thread can't be force-killed from here), "
                f"not something this timeout can fix — if it happens often, the actual fix is "
                f"reducing what triggers a slow backfill/heal this close to shutdown, not waiting "
                f"longer here.",
                flush=True,
            )
            for t in still_pending:
                t.cancel()  # detach our reference at least, even though the thread itself won't stop

    ohlc.shutdown()

    # Market end or manual stop — both come through here (stop_event
    # fires from _auto_stop_after_close() or SIGINT/SIGTERM either way)
    # — trim the local tick cache back down to the retention window
    # before closing the writer out.
    tick_writer.prune_older_than_days(TICK_CACHE_RETENTION_DAYS, DEPTH_CACHE_RETENTION_DAYS)
    tick_writer.shutdown()
    print("[SYSTEM] Shutdown complete", flush=True)
