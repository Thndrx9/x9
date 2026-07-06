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
from depth_writer import DepthWriter
from indicators import IndicatorEngine
from signal_generator import SignalGenerator
from executor import TradeExecutor
from market_time import (
    MARKET_OPEN,
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

# Where DAY_STARTED / RECONNECTED / DISCONNECTED events get logged for
# the Quote-mode websocket connection (the one backfill/gap-detection
# cares about for figuring out when the feed actually dropped).
CONN_LOG_DIR = os.getenv("CONN_LOG_DIR", "connection_logs")


def _next_market_open(dt: datetime) -> datetime:
    day = dt.date()
    while True:
        candidate = datetime.combine(day, MARKET_OPEN, tzinfo=tz_kolkata)
        if candidate > dt and is_trading_day(candidate):
            return candidate
        day += timedelta(days=1)


async def _run_backfill_safe(ohlc, symbols):
    """
    Wraps ensure_backfill_async so a failure is actually visible.
    Without this, an exception inside a fire-and-forget asyncio.Task
    (like the parallel-backfill task below) is silently swallowed and
    only surfaces much later as an unhelpful "exception was never
    retrieved" warning — or never at all.
    """
    try:
        await ohlc.ensure_backfill_async(symbols)
    except Exception:
        import traceback
        print("[BACKFILL][FATAL] Backfill task crashed:", flush=True)
        traceback.print_exc()


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

    ohlc             = OHLCCollector()
    depth_writer     = DepthWriter(base_dir="depthdata")
    depth_store      = DepthStore(depth_writer=depth_writer)
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

    async def indicator_loop():
        while not stop_event.is_set():
            for s in symbols:
                indicators.update(s["symbol"])
            await asyncio.sleep(1)

    def start_live_tasks():
        tasks.append(asyncio.create_task(
            run_market_data_feeds(
                api_key, symbols,
                conn_log_dir=CONN_LOG_DIR,
                depth_levels=int(os.getenv("DEPTH_LEVELS", "5")),
            )
        ))
        tasks.append(asyncio.create_task(ohlc.run()))
        tasks.append(asyncio.create_task(depth_store.run()))
        tasks.append(asyncio.create_task(indicator_loop()))

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
        start_live_tasks()
        tasks.append(asyncio.create_task(_run_backfill_safe(ohlc, symbols)))

    else:
        next_open      = _next_market_open(now)
        secs_to_open   = (next_open - now).total_seconds()

        if secs_to_open <= PARALLEL_BACKFILL_THRESHOLD_SECS:
            print(
                f"[SYSTEM] Market opens in {secs_to_open:.0f}s — "
                f"starting WebSocket now and running backfill in parallel.",
                flush=True,
            )
            start_live_tasks()
            tasks.append(asyncio.create_task(_run_backfill_safe(ohlc, symbols)))

        else:
            # Market is closed and not imminent — block until backfill finishes
            print("[SYSTEM] Market closed — running backfill first.", flush=True)
            await _run_backfill_safe(ohlc, symbols)
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
                start_live_tasks()

    await stop_event.wait()

    if executor:
        await executor.shutdown_async()

    for t in tasks:
        t.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    ohlc.shutdown()
    depth_writer.shutdown()
    print("[SYSTEM] Shutdown complete", flush=True)