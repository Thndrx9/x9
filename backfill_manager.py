# backfill_manager.py

import os
import re
import math
import psycopg2
import psycopg2.extras
import pandas as pd
from collections import deque
from typing import Optional
from datetime import datetime, timedelta, time as dtime
from dotenv import load_dotenv
from market_time import (
    tz_kolkata, MARKET_OPEN, MARKET_CLOSE, is_trading_day, now_kolkata,
    is_market_open, trading_day_n_back,
)
from tick_writer import DEPTH_LEVEL_COLUMNS, QUOTE_EXTRA_COLUMNS
from gap_detector import (
    GapDetector,
    is_market_hours_weekday_vectorized,
)
from tick_gap_detector import TickGapDetector

load_dotenv()

# Comma-joined QUOTE_EXTRA_COLUMNS for inline use inside f-string SQL —
# these are the extra quote_<symbol> columns beyond timestamp/ltp/qty
# (see tick_writer.QUOTE_EXTRA_COLUMNS docstring for the full list and
# why they're additive rather than replacing the original three).
_QUOTE_EXTRA_SELECT_COLS = ", ".join(QUOTE_EXTRA_COLUMNS)
_QUOTE_FULL_DF_COLUMNS = ["symbol", "timestamp", "ltp", "qty"] + list(QUOTE_EXTRA_COLUMNS)

# Minutes in one full trading session (9:15 → 15:30)
SESSION_MINUTES = 375


def _tf_to_seconds(tf_str: str) -> int:
    """
    Parse a timeframe string into total seconds.
    Supports:  '5s' → 5   '30s' → 30   '1m' → 60
               '5m' → 300  '1m30s' → 90  '15m' → 900
    """
    total = 0
    for value, unit in re.findall(r'(\d+)([ms])', tf_str.lower()):
        total += int(value) * (60 if unit == 'm' else 1)
    if total == 0:
        raise ValueError(f"[BACKFILL] Invalid timeframe string: '{tf_str}'")
    return total


class BackfillManager:
    """
    BackfillManager — ORCHESTRATION + DB I/O ONLY
    ------------------------------------------------
    Owns:
        • PostgreSQL connections (main db + history/fallback db)
        • Tick fetching + aggregation into candles
        • The multi-tier fill order per symbol/TF:
            1. Aggregate straight from primary ticks
            2. Fill remaining gaps via direct match in history db — only
               fires when TF == history_native_tf (default "1m"), since
               that db has no interval column and only ever holds one
               fixed candle granularity
            3. Fill remaining gaps by deriving from finalized 1m candles
               (which, for TF == history_native_tf, already include
               whatever tier 2 recovered)
            4. Whatever's left is reported as a genuine, unrecoverable gap
        • Loading results into ohlc.save_candle() and validating counts

    Delegates all "what's missing" / "roll up 1m into higher TF" logic
    to GapDetector (gap_detector.py) — this class has no bucket math of
    its own beyond what it needs to run SQL queries.

    Delegates all silent same-session tick-cache gap detection (is a
    cached range's high-water mark actually trustworthy, or does it
    hide a hole?) to TickGapDetector (tick_gap_detector.py). This
    class only fetches from Postgres and reads/writes the local
    SQLite tick cache — TickGapDetector tells it which cached ranges
    are missing/untrusted and need re-fetching.

    Delegates all candle building (raw tick→OHLC aggregation, and the
    history-priority merge across timeframes) to OHLCCollector
    (ohlc.py) — this class hands it whatever ticks_df/history_df it
    fetched and gets candles back; it does no bucket-grouping itself.

    Schema (main db, confirmed against pg_writer.py's _QUOTE_COLUMN_DEFS —
    typed columns, NOT JSONB):
        table : quote_{symbol}   (e.g. quote_reliance; pg_writer always
                lowercases the table name, Postgres folds unquoted idents
                to lowercase anyway so this matters cosmetically only)
        cols  : timestamp BIGINT (exchange ms, indexed), ingest_ns BIGINT,
                ltp DOUBLE PRECISION, ltt BIGINT, volume BIGINT,
                open/high/low/close DOUBLE PRECISION,
                last_quantity BIGINT, oi BIGINT,
                upper_circuit/lower_circuit DOUBLE PRECISION

    Schema (history db, PG_HDBNAME) — confirmed against x9_data_fetcher's
    own BackfillManager (writes here) and pg_writer.py (defines the
    columns): SAME typed quote_{symbol} table shape as the main db, but
    populated differently — one row per pre-built candle from the
    broker's REST history API, at a single fixed granularity
    (self.history_native_tf, from OPENALGO_HISTORY_INTERVAL, default
    "1m"). open/high/low/close/volume are populated directly; ltp/ltt are
    NULL (the history API doesn't return them); there is no interval
    column to distinguish timeframes by, because none was ever written.

    History-db candles are treated as the authoritative 1m source
    whenever present: at history_native_tf itself they win over
    tick-aggregated candles on any bucket collision, and every higher
    TF is derived FIRST from that (history-priority) 1m sequence — see
    _aggregate_symbol_with_history()'s docstring — rather than from
    raw-tick aggregation directly at that TF. Ticks only fill in
    buckets history_native_tf's own data doesn't cover.

    Batching (run() overview):
    Per-symbol DB round trips used to dominate wall-clock time for large
    symbol universes — every symbol paid its own network round trip(s)
    to a remote AWS RDS instance, strictly sequentially. run() now does
    this in distinct phases instead of one big per-symbol loop:
        Phase 0:  list which quote_{SYMBOL} tables actually exist in
                  the main db AND the history db (1 query each)
        Phase 1:  local-tick-cache-only pass — pure disk I/O, no DB at
                  all (checks ticks.db via TickWriter.max_ts for what's
                  already cached per symbol)
        Phase 1h: local-history-candle-cache-only pass — same idea,
                  against history_candles.db via HistoryCandleStore
        Phase 2:  ONE batched (UNION ALL, chunked) tick fetch for every
                  symbol that still needs data newer than the local
                  tick cache
        Phase 2h: ONE batched history-db candle fetch for every symbol
                  that still needs data newer than the local history-
                  candle cache — always run now, not just for gaps —
                  caches fetched rows into history_candles.db
        Phase 3:  per-symbol aggregation (pure pandas, no DB) with
                  history-db candles given priority over tick-
                  aggregated ones (see _aggregate_symbol_with_history)
        Phase 4:  per-symbol save to ohlc + missing-bucket reporting
                  (pure pandas/RAM, no DB)
        Phase 5:  tick-buffer seeding — reuses Phase 2's fetch where
                  possible (zero extra queries), batches the rest
    Every "batched" step above fetches every symbol in ONE query by
    default (BACKFILL_BATCH_SIZE=0) — set it to a positive number in
    .env to chunk instead, if a single mega-query ever proves too slow
    or memory-heavy for your data volume.
    """

    def __init__(self, ohlc):
        self.ohlc        = ohlc
        # Local SQLite tick cache — set on the OHLCCollector instance by
        # engine_runtime.py. Optional: None just means "don't cache PG
        # ticks to disk", everything else still works.
        self.tick_writer = getattr(ohlc, "tick_writer", None)
        # Local SQLite cache for history-db candles — same deal, set by
        # engine_runtime.py, optional. See tick_writer.py's HistoryCandleStore and this
        # class's Phase 1h/2h.
        self.history_store = getattr(ohlc, "history_store", None)
        # Optional — see engine_runtime.py's wiring. Used only to seed
        # DepthStore's RAM window right after depth backfill completes
        # (see _run_depth_backfill's final step); None just means that
        # seed step is skipped, DepthStore still fills in normally from
        # live ticks, just starting empty instead of pre-warmed.
        self.depth_store = getattr(ohlc, "depth_store", None)
        # Directory for connection_log.db — set by engine_runtime.py via
        # OHLCCollector. Optional: None just means gaps can't be
        # cross-checked against confirmed disconnect windows, they're
        # still detected/repaired via the tick-scan heuristic alone.
        self.conn_log_dir = getattr(ohlc, "conn_log_dir", None)
        self.min_candles = int(os.getenv("MIN_CANDLES", "15"))
        self.timeframes  = self._load_timeframes()   # [(tf_str, tf_seconds), ...]
        self.conn        = self._connect()
        self.gaps        = GapDetector()
        # Tick-cache gap detection (silent same-session holes in
        # already-cached ticks/depth) — see tick_gap_detector.py.
        # BackfillManager only fetches from Postgres/reads the local
        # cache; this is what tells it a range is actually missing.
        self.tick_gaps   = TickGapDetector()
        self.batch_size  = int(os.getenv("BACKFILL_BATCH_SIZE", "0"))  # 0 = no chunking, all symbols in one query

        # How large a same-session silent gap in cached ticks (ticks.db)
        # has to be before we stop trusting the cache from that point
        # onward and re-fetch from the main db instead. max_ts() is only
        # a high-water mark — it doesn't guarantee everything before it
        # is actually present (crash mid-write, a failed insert batch,
        # etc.) — see run()'s Phase 1 and TickGapDetector.find_cache_gap().
        self.gap_threshold_secs = int(os.getenv("TICK_CACHE_GAP_THRESHOLD_SECS", "180"))
        # Populated by _fetch_ticks_batch()/_fetch_depth_batch() each run —
        # symbols whose main-db re-fetch chunk genuinely failed (network/
        # connection error), as opposed to symbols confirmed to have zero
        # new rows. See run()'s phantom-row cleanup for why this
        # distinction matters — treating a failed fetch as "confirmed
        # empty" was deleting real cached ticks whenever the confirming
        # re-fetch itself happened to fail.
        self._last_fetch_failed_symbols = set()

        # History (fallback) DB — only connected lazily, on first gap found
        self.conn_history           = None
        self._history_connect_tried = False

        # market_history's quote_{SYMBOL} rows are pre-built candles from
        # x9_data_fetcher's own BackfillManager (OpenAlgo history API), at
        # whatever OPENALGO_HISTORY_INTERVAL that collector was configured
        # with — default "1m". There is no interval column in pg_writer's
        # schema to read this back from, so it has to be supplied here to
        # match, via the same env var name.
        self.history_native_tf = (
            os.getenv("OPENALGO_HISTORY_INTERVAL", "1m").strip() or "1m"
        )

        # (symbol, tf_str) -> count of candles that stayed missing after
        # every fallback tier. Populated during _finalize_symbol, read by
        # _validate.
        self.missing_counts = {}

    # ─────────────────────────────────────────────
    # Setup
    # ─────────────────────────────────────────────

    def _load_timeframes(self):
        tf_env = os.getenv("TIMEFRAMES", "1m,5m")
        result = []
        for tf in tf_env.split(","):
            tf = tf.strip()
            if tf:
                result.append((tf, _tf_to_seconds(tf)))
        return sorted(result, key=lambda x: x[1])

    def _connect(self):
        timeout_sec = int(os.getenv("PG_STATEMENT_TIMEOUT_SEC", "60"))
        try:
            conn = psycopg2.connect(
                host            = os.getenv("PG_HOST"),
                port            = int(os.getenv("PG_PORT", "5432")),
                dbname          = os.getenv("PG_DBNAME") or os.getenv("PG_DATABASE"),
                user            = os.getenv("PG_USER"),
                password        = os.getenv("PG_PASSWORD"),
                sslmode         = os.getenv("PG_SSLMODE", "require"),
                connect_timeout = 15,
                # Bound how long any single query can run server-side.
                # Without this, a slow/large batched query (or a stuck
                # connection) blocks the backfill thread forever with no
                # error and no way to notice — exactly the "silently
                # stops after local-tick-cache checks" symptom this fixes.
                options         = f"-c statement_timeout={timeout_sec * 1000}",
            )
            conn.autocommit = True
            print("[BACKFILL] Connected to PostgreSQL (main db)", flush=True)
            return conn
        except Exception as exc:
            print(f"[BACKFILL][ERROR] PostgreSQL connection failed: {exc}", flush=True)
            return None

    def _is_connection_dead(self, exc: Exception) -> bool:
        """
        True if `exc` looks like the connection itself is gone (server
        closed it, network drop, etc.) rather than a query-level
        problem (bad SQL, statement_timeout, etc.) — the former is
        worth reconnecting and retrying once; the latter isn't (retrying
        a broken query on a fresh connection would just fail the same
        way).
        """
        if isinstance(exc, (psycopg2.InterfaceError, psycopg2.OperationalError)):
            return True
        # psycopg2 sometimes surfaces a dead connection as a generic
        # Error subclass depending on driver/OS — fall back to matching
        # the message text for the common phrasings.
        msg = str(exc).lower()
        return any(s in msg for s in (
            "server closed the connection",
            "connection already closed",
            "could not connect",
            "terminating connection",
            "connection reset",
            "broken pipe",
        ))

    def _ensure_connected(self):
        """
        Reconnects self.conn if it's closed or unresponsive. Cheap to
        call before any batch of queries — a no-op when the connection
        is already fine (checks .closed first, avoiding a round trip in
        the common case).
        """
        if self.conn is not None and not self.conn.closed:
            return
        print("[BACKFILL] Main-db connection is closed — reconnecting...", flush=True)
        self.conn = self._connect()

    def _get_history_conn(self):
        """
        Lazily connects to the history DB (PG_HDBNAME) — cached after
        the first call. run_low_memory() calls this right before its
        history-candle step (not up front) so the connection doesn't
        sit idle through the earlier tick-fetch phase and get dropped
        for being idle by the time it's actually used — see the call
        site's comment. The lazy-connect-and-cache shape here still
        matters for the other call sites (_fetch_history_candles_batch,
        fetch_daily_candles) that call this mid-run and expect the same
        cached connection without reconnecting.
        """
        if self.conn_history is not None:
            if not self.conn_history.closed:
                return self.conn_history
            print("[BACKFILL] History-db connection is closed — reconnecting...", flush=True)
            self.conn_history = None
            self._history_connect_tried = False
        if self._history_connect_tried:
            return None

        self._history_connect_tried = True
        timeout_sec = int(os.getenv("PG_STATEMENT_TIMEOUT_SEC", "60"))
        try:
            conn = psycopg2.connect(
                host            = os.getenv("PG_HOST"),
                port            = int(os.getenv("PG_PORT", "5432")),
                dbname          = os.getenv("PG_HDBNAME"),
                user            = os.getenv("PG_USER"),
                password        = os.getenv("PG_PASSWORD"),
                sslmode         = os.getenv("PG_SSLMODE", "require"),
                connect_timeout = 15,
                options         = f"-c statement_timeout={timeout_sec * 1000}",
            )
            conn.autocommit = True
            print("[BACKFILL] Connected to PostgreSQL (history db)", flush=True)
            self.conn_history = conn
            return conn
        except Exception as exc:
            print(f"[BACKFILL][WARN] History DB connection failed: {exc}", flush=True)
            return None

    def _progress(self, label: str, current: int, total: int):
        """
        Single in-place-updating progress line (like a progress bar) —
        overwrites itself via carriage return instead of printing one
        line per symbol. Prints a trailing newline once current==total
        so the next phase's output starts on a fresh line.

        \\033[K (clear-to-end-of-line) after the text, not just \\r, so
        a shorter new line can never leave stray trailing characters
        from a longer previous one still visible — \\r alone only moves
        the cursor back to column 0, it doesn't erase anything. That
        mismatch is exactly what produced garbled output like
        "1/1 symbols9 symbols" when a shorter progress string
        overwrote a longer one (e.g. "...199/199 symbols" followed by
        "...20/199 symbols") — \\r left the old line's unwritten tail
        sitting there untouched. Standard ANSI escape, supported by
        every terminal this project actually runs in (Linux/WSL/macOS
        terminals, VS Code's integrated terminal); if this is ever run
        somewhere that doesn't support ANSI escapes, the raw sequence
        would print literally rather than clear the line — not a
        concern for this project's actual deployment targets.
        """
        end = "\n" if current >= total else ""
        print(f"\r[BACKFILL] {label}: {current}/{total} symbols\033[K", end=end, flush=True)

    def _load_existing_tables(self, conn, prefix: str = "quote") -> Optional[set]:
        """
        One query: which <prefix>_% tables actually exist in this db
        (prefix is "quote" or "depth"). Needed BEFORE building any
        UNION ALL batch — Postgres fails the entire batched query if
        even one clause references a table that doesn't exist, so
        symbols without a table yet get filtered out up front instead
        of blowing up the whole chunk.

        Returns None (not an empty set) on a genuine query failure —
        callers MUST treat that as "unknown, don't trust this" rather
        than "confirmed: no tables exist". Those used to be the same
        return value (set()), which meant a plain connection drop
        during this check (a real ERROR, printed right below) was
        silently indistinguishable from every single symbol genuinely
        having no table — and every downstream fetch quietly skipped
        the entire universe with only a WARN, not the connection-level
        failure that actually caused it. Seen in practice: this check's
        connection died mid-query while the DB was under load from an
        unrelated in-flight backfill, and all 199 symbols got skipped
        for depth with nothing louder than "no depth_ table found".
        """
        if conn is None:
            return set()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname='public' AND tablename LIKE %s",
                (f"{prefix}_%",),
            )
            tables = {row[0] for row in cur.fetchall()}
            cur.close()
            return tables
        except Exception as exc:
            print(f"[BACKFILL][ERROR] failed to list {prefix}_ tables: {exc}", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
            return None

    def _load_existing_quote_tables(self, conn) -> Optional[set]:
        """Back-compat alias — see _load_existing_tables()."""
        return self._load_existing_tables(conn, prefix="quote")

    def _existing_tables_or_empty(self, existing_tables: Optional[set], prefix: str, context: str) -> set:
        """
        Every call site below needs a plain set to hand to downstream
        code (UNION ALL batch building, membership checks, etc.), so
        this is where None (a genuine query/connection failure — see
        _load_existing_tables()'s docstring) gets converted to one.
        Kept as a single, explicit conversion point specifically so
        that conversion is loud and distinguishable from a confirmed
        empty result, rather than silently indistinguishable the way
        it used to be. Downstream code proceeds as if there are no
        {prefix}_ tables either way (avoids aborting the whole run over
        what's often a transient blip), but at least the log now says
        clearly which one actually happened.
        """
        if existing_tables is None:
            print(
                f"[BACKFILL][ERROR] {context}: could not confirm which {prefix}_ tables exist "
                f"(see failure above) — proceeding as if NONE do for this run. This is NOT the "
                f"same as confirmed-empty; every symbol will be skipped below with a WARN, but "
                f"the real cause is this connection failure, not missing tables.",
                flush=True,
            )
            return set()
        return existing_tables

    # ─────────────────────────────────────────────
    # Entry point
    # ─────────────────────────────────────────────

    def run_low_memory(self, symbols, chunk_size: int = 20):
        """
        Same end result as run() — local caches synced against the
        main/history db, candles built and saved — but every phase is
        chunked and discards each chunk's data before the next, so
        peak RAM stays roughly proportional to one chunk of symbols
        instead of all of them. Deliberately runs IN this process (not
        a subprocess) because save_candles_bulk() populates the live
        OHLCCollector's in-RAM ohlc_data dict, which live trading reads
        from directly — see candle_builder.py's module docstring for
        why a separate process would leave that dict empty.
        """
        if self.conn is None:
            print("[BACKFILL][ERROR] No DB connection — skipping backfill", flush=True)
            return

        tf_names = [tf for tf, _ in self.timeframes]
        start_ts = self._compute_lookback_start()
        now      = datetime.now(tz_kolkata)

        if is_market_open(now):
            mode = "market open"
        elif is_trading_day(now.date()) and now.time() < MARKET_OPEN:
            mode = "pre-market"
        else:
            mode = "market closed"

        print(
            f"[BACKFILL] Mode: {mode} | symbols={len(symbols)} "
            f"| TFs={tf_names} | from={start_ts.strftime('%Y-%m-%d %H:%M %Z')} "
            f"| low-memory chunked path (chunk_size={chunk_size})",
            flush=True,
        )

        existing_tables = self._existing_tables_or_empty(
            self._load_existing_quote_tables(self.conn), "quote", "startup tick backfill (main db)"
        )

        quote_outage_windows = []
        if self.conn_log_dir:
            try:
                from gap_detector import connection_outage_windows
                quote_outage_windows = connection_outage_windows(self.conn_log_dir, now.date(), "Quote")
            except Exception as exc:
                print(f"[BACKFILL][WARN] could not read connection log: {exc}", flush=True)

        # Step 1 — make the local tick cache correct (no candles yet).
        self.sync_local_cache_with_main_db(
            symbols, start_ts, now, existing_tables, quote_outage_windows, chunk_size=chunk_size
        )

        # History-db connection is opened here, right before it's
        # actually used — not up front. Step 1 above can take a while
        # (a full main-db tick fetch), and opening this connection
        # before that ran left it sitting idle for the whole tick
        # phase, long enough that the server/proxy would drop it for
        # being idle — showing up as a "connection dropped ...
        # reconnecting" on the very first history-db query. Connecting
        # here instead means it's opened right when it's about to be
        # used.
        history_conn = self._get_history_conn()
        history_existing_tables = self._existing_tables_or_empty(
            self._load_existing_quote_tables(history_conn) if history_conn is not None else set(),
            "quote", "startup tick backfill (history db)",
        )

        # Step 2 — make the local history-candle cache correct.
        if history_conn is not None:
            self.sync_history_cache_with_main_db(
                symbols, start_ts, now, history_existing_tables, chunk_size=chunk_size
            )

        # Step 2d: mirror market_history's daily_<symbol> tables locally —
        # unchanged from the old run(), not part of the RAM problem.
        self.fetch_daily_candles(symbols)

        # Step 3 — build + save candles from the now-correct local
        # caches, chunked, discarding each symbol's raw ticks right
        # after use (see build_candles_from_local_cache's docstring).
        self.build_candles_from_local_cache(symbols, start_ts, now, chunk_size=chunk_size)

        # Step 4 — depth backfill (already chunked — see _run_depth_backfill).
        self._run_depth_backfill(symbols, now)

        self._validate(symbols)

        try:
            self.conn.close()
        except Exception:
            pass
        if self.conn_history is not None:
            try:
                self.conn_history.close()
            except Exception:
                pass

        print("[BACKFILL] Completed (low-memory path)", flush=True)

    # ─────────────────────────────────────────────
    # Depth backfill — LOOKBACK IS INTENTIONALLY SHORTER THAN QUOTE:
    # only the most recent trading day, not Quote's full multi-day
    # min_candles-driven window. Depth rows are far heavier than Quote
    # rows (reconstructed bids/asks, not a few flat scalars) and much
    # higher frequency, so matching Quote's full lookback here was
    # materializing multiple trading days of order-book data in RAM
    # at once across every symbol — this cuts that down to one day's
    # worth by design, not just as a memory workaround.
    #
    # Otherwise mirrors Quote's Phase 1/Phase 3 logic exactly: same
    # gap→refetch→upsert-correct approach, same phantom-row check,
    # just against depth_ tables and cross-checked against Depth-mode
    # connection-log outage windows instead of Quote's. Depth never
    # feeds candle building, so there's no Phase 3/4 candle step here.
    # ─────────────────────────────────────────────

    def _compute_depth_lookback_start(self, now) -> datetime:
        """
        Start of the most recent trading day (09:15 IST) — today's, if
        today is itself a trading day (even before market open), else
        the most recent prior trading day. This is deliberately NOT
        the same window Quote uses (_compute_lookback_start) — see
        the note above.
        """
        day = now.date()
        while not is_trading_day(day):
            day -= timedelta(days=1)
        return datetime.combine(day, dtime(9, 15, 0)).replace(tzinfo=tz_kolkata)

    def _run_depth_backfill(self, symbols, now):
        if self.tick_writer is None or self.conn is None:
            return

        start_ts = self._compute_depth_lookback_start(now)
        start_ms = int(start_ts.timestamp() * 1000)
        now_ms   = int(now.timestamp() * 1000)
        chunk_size = 20

        print(
            f"[BACKFILL] Depth backfill starting | lookback: last 1 trading "
            f"day (from={start_ts.strftime('%Y-%m-%d %H:%M %Z')})",
            flush=True,
        )

        depth_outage_windows = []
        if self.conn_log_dir:
            try:
                from gap_detector import connection_outage_windows
                depth_outage_windows = connection_outage_windows(self.conn_log_dir, now.date(), "Depth")
            except Exception as exc:
                print(f"[BACKFILL][WARN] could not read connection log for depth: {exc}", flush=True)

        # ── Phase D1: check local depth cache, flag gaps ──
        # Same helper as the quote-tick path — tick_gaps.scan_local_cache()
        # owns all local-cache reading, chunked, and hands back only
        # small per-symbol facts (fetch-start ms, gap_window). No
        # depth row (the heaviest row type — reconstructed order-book
        # dicts) is retained past the chunk it was read in.
        depth_cache_check_conn = self.tick_writer.open_read_connection() if self.tick_writer is not None else None
        try:
            gap_map = self.tick_gaps.scan_local_cache(
                symbols, self.tick_writer, start_ms, now_ms,
                self.gap_threshold_secs, depth_outage_windows,
                chunk_size=chunk_size, conn=depth_cache_check_conn, kind="depth",
                progress=lambda i, n: self._progress("Checking local depth cache", i, n),
            )
        finally:
            if depth_cache_check_conn is not None:
                try:
                    depth_cache_check_conn.close()
                except Exception as exc:
                    print(f"[BACKFILL][WARN] closing depth cache-check connection failed: {exc}", flush=True)

        gap_flagged = [s for s, e in gap_map.items() if e["gap_window"] is not None]
        if gap_flagged:
            preview = ", ".join(gap_flagged[:10])
            more = f" (+{len(gap_flagged) - 10} more)" if len(gap_flagged) - 10 > 0 else ""
            print(
                f"[BACKFILL][WARN] {len(gap_flagged)} symbol(s) had a silent gap "
                f"(>{self.gap_threshold_secs}s within a session) in their cached "
                f"depth data — checking main db for the exact window: "
                f"{preview}{more}",
                flush=True,
            )

        fetch_starts = {
            s: datetime.fromtimestamp(e["fetch_start_ms"] / 1000, tz=tz_kolkata)
            for s, e in gap_map.items() if e["fetch_start_ms"] is not None
        }
        # Only present for symbols where the leading part of the window
        # was found missing (widened lookback) — caps that fetch at the
        # row already trusted locally instead of re-pulling everything
        # through now. See tick_gap_detector.scan_local_cache().
        fetch_ends = {
            s: datetime.fromtimestamp(e["fetch_end_ms"] / 1000, tz=tz_kolkata)
            for s, e in gap_map.items() if e.get("fetch_end_ms") is not None
        }

        if not fetch_starts and not gap_flagged:
            print("[BACKFILL] Depth backfill: nothing to fetch — all symbols fully cached", flush=True)
            return

        depth_existing_tables = self._existing_tables_or_empty(
            self._load_existing_tables(self.conn, prefix="depth"), "depth", "depth backfill"
        )
        last_fetch_failed_symbols = set()

        # ── Phase D2: normal top-up (unrelated to gap resolution) ──
        fetch_items = list(fetch_starts.items())
        for i in range(0, len(fetch_items), chunk_size):
            chunk_fetch_starts = dict(fetch_items[i:i + chunk_size])
            chunk_fetch_ends = {s: fetch_ends[s] for s in chunk_fetch_starts if s in fetch_ends}
            chunk_fetched = self._fetch_depth_batch(chunk_fetch_starts, depth_existing_tables, chunk_fetch_ends)
            last_fetch_failed_symbols |= self._last_fetch_failed_symbols
            del chunk_fetched
            self._progress("Fetching + writing depth gaps", min(i + chunk_size, len(fetch_items)), len(fetch_items))

        # ── Phase D3: gap resolution — narrow-window main-db check ──
        # Same reasoning as sync_local_cache_with_main_db()'s Step 3:
        # check main db for the EXACT narrow (gap_prev_ms+1,
        # gap_next_ms-1) window instead of re-verifying everything
        # since the gap on every run. No local-candle cross-check here
        # — depth (order-book snapshots) has no candle equivalent.
        resolved_symbols, settled_symbols = [], []
        if gap_flagged:
            gap_windows_ms = {s: gap_map[s]["gap_window"] for s in gap_flagged}
            gap_items = list(gap_windows_ms.items())
            for i in range(0, len(gap_items), chunk_size):
                chunk = gap_items[i:i + chunk_size]
                gap_starts_chunk = {s: datetime.fromtimestamp(w[0] / 1000, tz=tz_kolkata) for s, w in chunk}
                gap_ends_chunk   = {s: datetime.fromtimestamp(w[1] / 1000, tz=tz_kolkata) for s, w in chunk}

                gap_fetched = self._fetch_depth_batch(gap_starts_chunk, depth_existing_tables, gap_ends_chunk)
                last_fetch_failed_symbols |= self._last_fetch_failed_symbols

                for symbol, _ in chunk:
                    if symbol in self._last_fetch_failed_symbols:
                        continue  # never reached main db this run — try again next time
                    if gap_fetched.get(symbol):
                        resolved_symbols.append(symbol)
                    else:
                        settled_symbols.append(symbol)

                del gap_fetched
                self._progress("Checking + resolving depth gaps", min(i + chunk_size, len(gap_items)), len(gap_items))

        self._last_fetch_failed_symbols = last_fetch_failed_symbols

        if resolved_symbols:
            preview = ", ".join(resolved_symbols[:10])
            more = f" (+{len(resolved_symbols) - 10} more)" if len(resolved_symbols) - 10 > 0 else ""
            print(
                f"[BACKFILL] {len(resolved_symbols)} symbol(s) had a real depth gap — "
                f"main db had data in the window, fetched + wrote it: {preview}{more}",
                flush=True,
            )
        if settled_symbols:
            preview = ", ".join(settled_symbols[:10])
            more = f" (+{len(settled_symbols) - 10} more)" if len(settled_symbols) - 10 > 0 else ""
            print(
                f"[BACKFILL] {len(settled_symbols)} symbol(s)' depth gap confirmed real "
                f"and permanent — main db has nothing there either (nothing to fetch, "
                f"settled): {preview}{more}",
                flush=True,
            )

        # Wait for every depth row fetched above to actually be written
        # to disk before reading it back for RAM seeding — fetching and
        # enqueueing finishes fast, but the writer thread (SQLite,
        # single-threaded) can lag well behind. Without this wait,
        # _seed_depth_ram's read_ticks() call races the writer thread
        # and finds nothing yet, which is exactly why "Depth RAM window
        # seeded: 0/N symbols" was showing up even on a successful
        # fetch — and why fetched-but-unwritten depth data was still
        # sitting in RAM (in the write queue) well after this function
        # printed "complete".
        if self.tick_writer is not None:
            self.tick_writer.flush_and_wait()

        self._seed_depth_ram(symbols, now)

        print(f"[BACKFILL] Depth backfill complete | {len(symbols)} symbol(s)", flush=True)

    def _seed_depth_ram(self, symbols, now):
        """
        Pre-warm DepthStore's rolling RAM window (see depth_store.py,
        DEPTH_RAM_WINDOW_MINUTES env var) from the now-corrected local
        depth cache, right after backfill finishes. Without this,
        DepthStore starts every restart with an EMPTY window and only
        fills back up gradually as live ticks arrive — meaning
        anything reading depth RAM history in the first
        DEPTH_RAM_WINDOW_MINUTES after a restart would see less
        history than it should, even though the correct data is
        already sitting right there in SQLite.

        No-op if depth_store wasn't wired in (see __init__) or if
        tick_writer is unavailable.
        """
        if self.depth_store is None or self.tick_writer is None:
            return

        window_secs = self.depth_store.ram_window_secs
        start_ms = int(now.timestamp() * 1000) - window_secs * 1000
        end_ms   = int(now.timestamp() * 1000)

        seeded_symbols = 0
        seeded_snapshots = 0
        for inst in symbols:
            symbol = inst["symbol"]
            rows = self.tick_writer.read_ticks(symbol, start_ms=start_ms, end_ms=end_ms, kind="depth")
            if not rows:
                continue

            history = self.depth_store.depth_history.setdefault(symbol, deque())
            history.clear()  # backfill's version is authoritative — replace any partial live data
            for r in rows:
                # rows come back flat (buy0_price/buy0_qty/... columns,
                # same shape as PG) — reassemble into the nested
                # bids/asks list-of-dicts DepthStore's in-RAM consumers
                # (best_bid_ask, get_history, etc.) expect. This is the
                # ONLY place that reconstruction happens now — once per
                # symbol at startup, not per-row on the backfill hot path.
                bids = []
                for lvl in range(5):
                    price = r.get(f"buy{lvl}_price")
                    if price is None:
                        continue
                    bids.append({
                        "price": price,
                        "quantity": r.get(f"buy{lvl}_qty"),
                        "orders": r.get(f"buy{lvl}_orders"),
                    })
                asks = []
                for lvl in range(5):
                    price = r.get(f"sell{lvl}_price")
                    if price is None:
                        continue
                    asks.append({
                        "price": price,
                        "quantity": r.get(f"sell{lvl}_qty"),
                        "orders": r.get(f"sell{lvl}_orders"),
                    })
                history.append({
                    "bids":      bids,
                    "asks":      asks,
                    "ltp":       r.get("ltp"),
                    "timestamp": r["timestamp"],
                })
            seeded_symbols += 1
            seeded_snapshots += len(rows)

        print(
            f"[BACKFILL] Depth RAM window seeded: {seeded_symbols}/{len(symbols)} "
            f"symbol(s), {seeded_snapshots} snapshot(s) ({window_secs // 60:.0f} min window)",
            flush=True,
        )

    # ─────────────────────────────────────────────
    # Mid-session auto-heal — triggered by websocket_connect.py right
    # after a RECONNECTED event (NOT the first connect of the day —
    # there's nothing to heal then). Unlike run()'s full Phase 1-5
    # pass, this is deliberately light: we already know EXACTLY when
    # and why the gap happened (the reconnect event itself confirms
    # it), so there's no need to re-scan every symbol's local cache
    # for suspicious gaps first — just fetch that one known window
    # from the main db and let upsert correct whatever's there.
    #
    # mode: "quote" or "depth" (lowercase) — only that one feed gets
    # healed, matching whichever connection actually reconnected.
    # ─────────────────────────────────────────────

    def run_targeted_heal(self, symbols, mode: str, gap_start, gap_end):
        if self.conn is None or not symbols:
            return

        fetch_starts = {inst["symbol"]: gap_start for inst in symbols}
        print(
            f"[BACKFILL] Mid-session auto-heal ({mode}): re-fetching "
            f"{gap_start.strftime('%H:%M:%S')}\u2013{gap_end.strftime('%H:%M:%S')} IST "
            f"for {len(symbols)} symbol(s)",
            flush=True,
        )

        existing_tables = self._existing_tables_or_empty(
            self._load_existing_tables(self.conn, prefix=mode), mode, f"{mode} targeted heal"
        )
        if mode == "depth":
            self._fetch_depth_batch(fetch_starts, existing_tables)
        else:
            self._fetch_ticks_batch(fetch_starts, existing_tables)

        print(f"[BACKFILL] Mid-session auto-heal ({mode}) complete", flush=True)

    # ─────────────────────────────────────────────
    # Per-symbol aggregation (Phase 3) — pure pandas, no DB
    # ─────────────────────────────────────────────

    def _ticks_rows_to_df(self, rows: list) -> pd.DataFrame:
        """Shape a raw read_ticks()-style row list into the DataFrame
        _cached_ticks_df() used to build directly — factored out so the
        batched read_ticks_batch() path (which already has the rows
        fetched) can build the same shape without a redundant DB call."""
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["timestamp", "ltp", "qty"])
        df["ist_ts"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)
        return df

    def _cached_ticks_df(self, symbol: str, start_ms: int, end_ms: int, conn=None) -> pd.DataFrame:
        """
        Read symbol's already-cached quote ticks back out of ticks.db
        (TickWriter) for [start_ms, end_ms], shaped into the same
        columns _fetch_ticks_batch()'s output uses (timestamp, ltp,
        qty, ist_ts) so it can be concatenated directly with freshly
        fetched rows before aggregation.

        conn: optional pre-opened connection to pass through to
        tick_writer.read_ticks() — see Phase 1's cache-check loop,
        which opens one connection for the whole loop instead of one
        per symbol. Prefer read_ticks_batch() + _ticks_rows_to_df() for
        multiple symbols at once — this single-symbol path still does
        one full network round-trip per call.
        """
        if self.tick_writer is None:
            return pd.DataFrame()

        rows = self.tick_writer.read_ticks(symbol, start_ms=start_ms, end_ms=end_ms, kind="quote", conn=conn)
        return self._ticks_rows_to_df(rows)

    def _cached_history_df(self, symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
        """
        Read symbol's already-cached history-db candles back out of
        history_candles.db (HistoryCandleStore) for [start_ms, end_ms],
        shaped into the same columns _fetch_history_candles_batch()'s
        output uses (timestamp as tz-aware Kolkata, open/high/low/close/
        volume) so it can be concatenated directly with freshly fetched
        rows.
        """
        if self.history_store is None:
            return pd.DataFrame()

        rows = self.history_store.read_candles(symbol, start_ms=start_ms, end_ms=end_ms)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)
        return df

    def _aggregate_symbol_with_history(self, ticks_df, history_df, start_ts, now):
        """
        Thin pass-through to OHLCCollector.build_symbol_candles() —
        candle building itself lives in ohlc.py now; this file only
        supplies what it fetched (ticks_df/history_df) plus its own
        config (timeframes, history_native_tf) and its GapDetector.
        """
        return self.ohlc.build_symbol_candles(
            ticks_df, history_df, start_ts, now,
            self.timeframes, self.history_native_tf, self.gaps,
        )

    # ─────────────────────────────────────────────
    # Per-symbol finalization (Phase 4) — save to ohlc + report
    # whatever's still missing after the history-priority merge in
    # _aggregate_symbol_with_history(). No DB calls in here at all.
    # ─────────────────────────────────────────────

    def _finalize_symbol(self, symbol, per_tf):
        """
        Saves every aggregated candle to ohlc and tracks missing-bucket /
        zero-candle counts on self for run() to report as one aggregate
        summary afterward — no per-symbol/per-tf lines printed here.
        """
        zero_candle_tfs = []

        for tf_str, tf_seconds in self.timeframes:
            state   = per_tf[tf_str]
            candles = state["candles"]
            missing = state["missing"]

            if missing:
                self.missing_counts[(symbol, tf_str)] = len(missing)

            if candles.empty:
                zero_candle_tfs.append(tf_str)
                continue

            # Bulk save — ONE call for the whole symbol/timeframe's
            # candle set instead of one save_candle() call per row.
            # save_candle() (still used for live ticks) triggers a full
            # parquet read+rewrite EVERY call; looping it here meant N
            # historical candles cost O(N^2) file I/O — the dominant
            # cost of backfill's candle-building phase. save_candles_bulk()
            # does one read-merge-write for the entire batch instead.
            self.ohlc.save_candles_bulk(symbol, tf_str, candles)

        return zero_candle_tfs

    # ─────────────────────────────────────────────
    # Candle building from the (now gap-free) local cache — meant to be
    # called from candle_builder.py, a SEPARATE process from the one
    # that ran sync_local_cache_with_main_db(). Reads fresh from local
    # cache/history_store per chunk, builds + saves, discards before
    # the next chunk. Never touches Postgres/main db.
    # ─────────────────────────────────────────────

    def build_candles_from_local_cache(self, symbols, start_ts, now, chunk_size: int = 20):
        start_ms = int(start_ts.timestamp() * 1000)
        now_ms   = int(now.timestamp() * 1000)
        window_secs = getattr(self.ohlc, "tick_ram_window_secs", 9 * 60)
        seed_stats = {"seeded": 0, "empty": 0, "ticks": 0}

        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]

            for inst in chunk:
                symbol = inst["symbol"]

                ticks_df = self._cached_ticks_df(symbol, start_ms, now_ms)
                history_df = self._cached_history_df(symbol, start_ms, now_ms)

                per_tf = self._aggregate_symbol_with_history(ticks_df, history_df, start_ts, now)
                self._finalize_symbol(symbol, per_tf)

                # Only the trailing seed window is worth keeping in RAM
                # after this — not the full lookback range.
                if not ticks_df.empty:
                    cutoff_ms = now_ms - window_secs * 1000
                    seed_df = ticks_df[ticks_df["timestamp"] >= cutoff_ms]
                    n = self._seed_recent_ticks(symbol, seed_df)
                    if n:
                        seed_stats["seeded"] += 1
                        seed_stats["ticks"] += n
                    else:
                        seed_stats["empty"] += 1
                else:
                    seed_stats["empty"] += 1

                # ticks_df/history_df/per_tf all die with this loop
                # iteration — nothing accumulates across symbols.
                del ticks_df, history_df, per_tf

            self._progress("Building candles", min(i + chunk_size, len(symbols)), len(symbols))

        total_missing = sum(self.missing_counts.values())
        if total_missing:
            print(
                f"[CANDLE_BUILDER][WARN] {total_missing} candle(s) still missing "
                f"across {len(self.missing_counts)} symbol/TF pair(s)",
                flush=True,
            )
        print(
            f"[CANDLE_BUILDER] Tick RAM buffers seeded: {seed_stats['seeded']}/{len(symbols)} "
            f"symbol(s), {seed_stats['ticks']} total tick(s), "
            f"{seed_stats['empty']} symbol(s) had none ({window_secs // 60:.0f} min window)",
            flush=True,
        )

    # ─────────────────────────────────────────────
    # Tick-buffer seeding (OHLCCollector.raw_ticks)
    # ─────────────────────────────────────────────

    def _seed_recent_ticks(self, symbol: str, ticks_df: Optional[pd.DataFrame] = None) -> int:
        """
        Pre-populate OHLCCollector.raw_ticks[symbol] with the last
        tick_ram_window_secs (TICK_RAM_WINDOW_MINUTES, 9 min by default)
        of ticks, anchored to the latest tick actually present rather
        than wall-clock now — so the in-RAM tick buffer other modules
        read via ohlc.get_recent_ticks()/ohlc.raw_ticks is already warm
        at startup, whether the market is currently open or closed.

        Called from build_candles_from_local_cache() with each symbol's
        own trailing-window slice of its local-cache ticks — no extra
        DB round trip needed.

        Returns the number of ticks loaded (0 if none) — the caller
        aggregates this across every symbol into a single summary print
        instead of one line per symbol.
        """
        window_secs = getattr(self.ohlc, "tick_ram_window_secs", 9 * 60)

        if ticks_df is None or ticks_df.empty:
            return 0

        cutoff = ticks_df["ist_ts"].max() - timedelta(seconds=window_secs)
        window_df = ticks_df[ticks_df["ist_ts"] >= cutoff]
        return self._load_ticks_into_ram(symbol, window_df)

    def _load_ticks_into_ram(self, symbol, df) -> int:
        if df.empty:
            return 0

        entries = [
            {"timestamp": row.ist_ts, "ltp": row.ltp, "qty": row.qty}
            for row in df.itertuples(index=False)
        ]

        with self.ohlc._ram_lock:
            dq = self.ohlc.raw_ticks.setdefault(symbol, deque())
            dq.clear()
            dq.extend(entries)

        return len(entries)

    # ─────────────────────────────────────────────
    # Standalone catch-up: fetch PG ticks since each symbol's last
    # cached row and write them straight to the local SQLite tick
    # cache. Used by engine_runtime.py for the final catch-up right
    # before going live (see tick_writer.py's hold()/release() gate) —
    # NOT part of the main run()/backfill flow, which caches ticks
    # inline in _fetch_ticks_batch() above as a side effect of its own
    # candle-gap fetches.
    # ─────────────────────────────────────────────

    def cache_recent_ticks_to_sqlite(self, symbols: list, default_window_secs: int = 9 * 60):
        """
        symbols: list of {"symbol": ...} dicts (same shape load_symbols()
        returns) or plain symbol strings.

        For each symbol, resumes from tick_writer.max_ts(symbol) if
        anything's cached already, else falls back to
        default_window_secs ago. Safe to call with tick_writer=None
        (no-op) or when nothing new has arrived (writes nothing).
        """
        if self.tick_writer is None or not symbols:
            return

        existing_tables = self._existing_tables_or_empty(
            self._load_existing_quote_tables(self.conn) if self.conn else set(),
            "quote", "pre-live catch-up",
        )
        now = now_kolkata()
        default_start = now - timedelta(seconds=default_window_secs)

        fetch_starts = {}
        for s in symbols:
            symbol = s["symbol"] if isinstance(s, dict) else s
            last_ms = self.tick_writer.max_ts(symbol)
            fetch_starts[symbol] = (
                datetime.fromtimestamp(last_ms / 1000, tz=tz_kolkata) + timedelta(milliseconds=1)
                if last_ms is not None else default_start
            )

        # _fetch_ticks_batch already caches whatever it fetches to
        # self.tick_writer as a side effect (see the groupby loop
        # below) — nothing further to do with its return value here.
        self._fetch_ticks_batch(fetch_starts, existing_tables)

    # ─────────────────────────────────────────────
    # Fetch ticks from PostgreSQL (main db) — BATCHED across symbols
    # ─────────────────────────────────────────────

    def sync_local_cache_with_main_db(
        self, symbols, start_ts, now, existing_tables, quote_outage_windows, chunk_size: int = 20
    ):
        """
        Replaces the old Phase 1 (local-cache scan) + Phase 2 (main-db
        fetch) + the quote-tick half of the old Phase 3 phantom-row
        cleanup — WITHOUT building any candles. Candle building has
        moved to candle_builder.py, which runs as its own process and
        reads the local cache fresh, AFTER this method has guaranteed
        it's gap-free. This method's only job is: make the local tick
        cache correct. It never holds more than one chunk's ticks in
        RAM at a time, and never retains a DataFrame past the chunk
        it was fetched for.

        Step 1 — tick_gaps.scan_local_cache() does ALL local-cache
        reading (read-only), chunked, and reports back only small
        per-symbol facts (a fetch-start timestamp, maybe a narrow
        gap_window) — no tick data survives this step.

        Step 2 — normal top-up: for symbols with new data since last
        run (fetch_start_ms), fetch just that range from main db and
        write it. No re-verification of anything older — a silent gap
        no longer drags the entire tail back into question (see below).

        Step 3 — gap resolution: for symbols with a detected silent
        gap (gap_window), check whether it's real by asking the ONE
        source that actually knows — main db itself — for ticks in
        that EXACT narrow window (not the whole span since the gap,
        the way this used to work).
          - Local connection_log.db is NOT used for this anymore: it
            only records this local process's own websocket connects/
            disconnects, and tells you nothing about whether the
            upstream/main database itself ever had the data — a real
            main-db-side outage (the actual scenario this exists to
            handle) can easily leave no matching entry there at all.
          - Also cheaply cross-checked against the local 1m history-
            candle cache (candles_1m_<symbol> — HistoryCandleStore):
            these candles get saved specifically to mark outage
            periods, so one being present for this exact window is
            EXTRA CONFIRMATION the gap is a genuine outage, not a
            contradiction — it's expected to line up with the "main
            db has no ticks either" case below, not with the
            "resolved" case. Its absence proves nothing either way
            (candles are saved occasionally, not for every gap), only
            its presence is informative.
          - If main db has ticks in the window → genuinely missing
            data, fetch + write just that narrow slice.
          - If main db has nothing there either → confirmed real,
            permanent gap (a real outage, same as the Sep 2 example) —
            nothing to fetch, nothing wrong, and because only the tiny
            gap window itself gets re-checked (not everything since
            it), this stays cheap on every future run instead of
            re-verifying a ever-growing tail forever.
        """
        start_ms = int(start_ts.timestamp() * 1000)
        now_ms   = int(now.timestamp() * 1000)

        cache_check_conn = self.tick_writer.open_read_connection() if self.tick_writer is not None else None
        try:
            gap_map = self.tick_gaps.scan_local_cache(
                symbols, self.tick_writer, start_ms, now_ms,
                self.gap_threshold_secs, quote_outage_windows,
                chunk_size=chunk_size, conn=cache_check_conn,
                progress=lambda i, n: self._progress("Checking local tick cache", i, n),
            )
        finally:
            if cache_check_conn is not None:
                try:
                    cache_check_conn.close()
                except Exception as exc:
                    print(f"[BACKFILL][WARN] closing cache-check connection failed: {exc}", flush=True)

        gap_flagged = [s for s, e in gap_map.items() if e["gap_window"] is not None]
        if gap_flagged:
            preview = ", ".join(gap_flagged[:10])
            more = f" (+{len(gap_flagged) - 10} more)" if len(gap_flagged) - 10 > 0 else ""
            print(
                f"[BACKFILL][WARN] {len(gap_flagged)} symbol(s) had a silent gap "
                f"(>{self.gap_threshold_secs}s within a session) in their cached "
                f"ticks — checking main db for the exact window: "
                f"{preview}{more}",
                flush=True,
            )

        fully_covered = [s for s, e in gap_map.items() if e["fetch_start_ms"] is None]
        if fully_covered:
            print(
                f"[BACKFILL] {len(fully_covered)}/{len(symbols)} symbol(s) fully "
                f"caught up in the local tick cache — skipping main db fetch for them",
                flush=True,
            )

        # ── Step 2: normal top-up (unrelated to gap resolution) ──
        to_fetch = [s for s, e in gap_map.items() if e["fetch_start_ms"] is not None]
        total_fetched = 0
        no_data_symbols = []
        self._last_fetch_failed_symbols = set()

        for i in range(0, len(to_fetch), chunk_size):
            chunk_symbols = to_fetch[i:i + chunk_size]
            fetch_starts_chunk = {
                s: datetime.fromtimestamp(gap_map[s]["fetch_start_ms"] / 1000, tz=tz_kolkata)
                for s in chunk_symbols
            }
            # Only present for symbols where the leading part of the
            # window was found missing (widened lookback) — caps that
            # fetch at the row already trusted locally, instead of
            # re-pulling (and re-writing on top of) everything through
            # now. See tick_gap_detector.scan_local_cache().
            fetch_ends_chunk = {
                s: datetime.fromtimestamp(gap_map[s]["fetch_end_ms"] / 1000, tz=tz_kolkata)
                for s in chunk_symbols if gap_map[s].get("fetch_end_ms") is not None
            }

            fetched = self._fetch_ticks_batch(fetch_starts_chunk, existing_tables, fetch_ends_chunk)
            for symbol in chunk_symbols:
                fresh_df = fetched.get(symbol, pd.DataFrame())
                if fresh_df.empty:
                    no_data_symbols.append(symbol)
                total_fetched += len(fresh_df)

            del fetched
            self._progress("Fetching + writing tick gaps", min(i + chunk_size, len(to_fetch)), len(to_fetch))

        if total_fetched:
            print(f"[BACKFILL] Tick gaps filled | {total_fetched} tick(s) fetched from main db", flush=True)
        if no_data_symbols:
            preview = ", ".join(no_data_symbols[:10])
            more = f" (+{len(no_data_symbols) - 10} more)" if len(no_data_symbols) - 10 > 0 else ""
            print(
                f"[BACKFILL][WARN] {len(no_data_symbols)} symbol(s) had no new "
                f"tick data in main db: {preview}{more}",
                flush=True,
            )

        # ── Step 3: gap resolution — narrow-window main-db check ──
        if not gap_flagged:
            return

        gap_windows_ms = {s: gap_map[s]["gap_window"] for s in gap_flagged}

        # Cheap local cross-check first, purely for the log line —
        # candles_1m_<symbol> get saved specifically to mark outage
        # periods, so one being present here is expected corroboration
        # that this window really was an outage, not a red flag; its
        # ABSENCE proves nothing (candles are saved occasionally, not
        # for every gap — see HistoryCandleStore), so it's never used
        # to skip the real main-db check below, only to annotate the
        # outcome.
        candle_confirmed = set()
        if self.history_store is not None:
            for symbol in gap_flagged:
                g_start, g_end = gap_windows_ms[symbol]
                if self.history_store.read_timestamps(symbol, g_start, g_end):
                    candle_confirmed.add(symbol)

        resolved_symbols = []      # main db had ticks here — fetched + written
        settled_symbols = []       # main db confirmed genuinely empty too
        gap_fetch_failed = set()

        for i in range(0, len(gap_flagged), chunk_size):
            chunk_symbols = gap_flagged[i:i + chunk_size]
            gap_starts_chunk = {
                s: datetime.fromtimestamp(gap_windows_ms[s][0] / 1000, tz=tz_kolkata)
                for s in chunk_symbols
            }
            gap_ends_chunk = {
                s: datetime.fromtimestamp(gap_windows_ms[s][1] / 1000, tz=tz_kolkata)
                for s in chunk_symbols
            }

            gap_fetched = self._fetch_ticks_batch(gap_starts_chunk, existing_tables, gap_ends_chunk)
            gap_fetch_failed |= self._last_fetch_failed_symbols

            for symbol in chunk_symbols:
                if symbol in self._last_fetch_failed_symbols:
                    continue  # never reached main db this run — try again next time
                gap_df = gap_fetched.get(symbol, pd.DataFrame())
                if gap_df.empty:
                    settled_symbols.append(symbol)
                else:
                    resolved_symbols.append(symbol)
                    total_fetched += len(gap_df)

            del gap_fetched
            self._progress("Checking + resolving tick gaps", min(i + chunk_size, len(gap_flagged)), len(gap_flagged))

        if resolved_symbols:
            preview = ", ".join(resolved_symbols[:10])
            more = f" (+{len(resolved_symbols) - 10} more)" if len(resolved_symbols) - 10 > 0 else ""
            candle_note = f" ({sum(1 for s in resolved_symbols if s in candle_confirmed)} also had a local 1m candle there)" if candle_confirmed else ""
            print(
                f"[BACKFILL] {len(resolved_symbols)} symbol(s) had a real gap — "
                f"main db had ticks in the window, fetched + wrote them{candle_note}: "
                f"{preview}{more}",
                flush=True,
            )
        if settled_symbols:
            preview = ", ".join(settled_symbols[:10])
            more = f" (+{len(settled_symbols) - 10} more)" if len(settled_symbols) - 10 > 0 else ""
            candle_note = f" ({sum(1 for s in settled_symbols if s in candle_confirmed)} also had a local 1m candle there, consistent with a genuine outage — those candles get saved specifically to mark outage periods)" if any(s in candle_confirmed for s in settled_symbols) else ""
            print(
                f"[BACKFILL] {len(settled_symbols)} symbol(s)' gap confirmed real and "
                f"permanent — main db has no ticks there either (nothing to fetch, "
                f"settled){candle_note}: {preview}{more}",
                flush=True,
            )
        if gap_fetch_failed:
            preview = ", ".join(sorted(gap_fetch_failed)[:10])
            print(
                f"[BACKFILL][WARN] {len(gap_fetch_failed)} symbol(s)' gap check never "
                f"reached main db this run — will retry next run: {preview}",
                flush=True,
            )

    def sync_history_cache_with_main_db(
        self, symbols, start_ts, now, history_existing_tables, chunk_size: int = 20
    ):
        """
        History-candle counterpart to sync_local_cache_with_main_db().

        candles_1m_<symbol> is populated only OCCASIONALLY, by design —
        it does NOT hold one row for every market minute. That rules
        out diffing against a theoretical full bucket schedule
        (GapDetector.expected_buckets(), the way tick/candle-building
        gap checks do elsewhere) — every never-populated minute would
        look "missing" and get endlessly, pointlessly re-fetched.

        Gap-checking here instead means: whatever candle timestamps
        upstream (market_history's quote_<symbol> table, via the
        history db) actually has in [start_ts, now) — fetched cheaply,
        timestamps only, via _fetch_history_timestamps_batch() — get
        diffed directly against what's already cached locally
        (history_store.read_timestamps()). Anything upstream that
        isn't in the local set yet is genuinely missing and gets
        fetched + written. Nothing is ever assumed missing just
        because a given minute has no candle upstream at all — that's
        expected, not a gap.

        Two-phase per chunk: first the lightweight timestamp diff to
        find which symbols actually have a gap, THEN — only for those
        — the real OHLCV fetch (_fetch_history_candles_batch, which
        pulls everything from the earliest missing timestamp onward
        and upserts; already covers the intermediate gap along with
        the tail, and re-upserting already-correct rows in between is
        harmless — same idempotent-on-ts_ms guarantee as before).
        """
        start_ms = int(start_ts.timestamp() * 1000)
        now_ms   = int(now.timestamp() * 1000)

        all_symbols = [inst["symbol"] for inst in symbols]
        fetch_starts = {}
        fully_covered = []
        gap_symbols = []

        for i in range(0, len(all_symbols), chunk_size):
            chunk = all_symbols[i:i + chunk_size]

            upstream_ts_by_symbol = self._fetch_history_timestamps_batch(
                chunk, start_ms, now_ms, history_existing_tables
            )

            for symbol in chunk:
                upstream_ts = upstream_ts_by_symbol.get(symbol)
                if not upstream_ts:
                    # Nothing upstream in this window for this symbol at
                    # all — nothing to compare against, nothing to fetch.
                    fully_covered.append(symbol)
                    continue

                local_ts = (
                    self.history_store.read_timestamps(symbol, start_ms, now_ms)
                    if self.history_store is not None else set()
                )
                missing_ts = upstream_ts - local_ts

                if missing_ts:
                    gap_symbols.append(symbol)
                    fetch_starts[symbol] = datetime.fromtimestamp(min(missing_ts) / 1000, tz=tz_kolkata)
                else:
                    fully_covered.append(symbol)

            self._progress("Checking history-candle cache for gaps", min(i + chunk_size, len(all_symbols)), len(all_symbols))

        if fully_covered:
            print(
                f"[BACKFILL] {len(fully_covered)}/{len(symbols)} symbol(s) fully "
                f"caught up in the local history-candle cache — skipping history "
                f"db fetch for them",
                flush=True,
            )
        if gap_symbols:
            preview = ", ".join(gap_symbols[:10])
            more = f" (+{len(gap_symbols) - 10} more)" if len(gap_symbols) - 10 > 0 else ""
            print(
                f"[BACKFILL][WARN] {len(gap_symbols)} symbol(s) had candle(s) present "
                f"upstream but missing from the local history-candle cache "
                f"(candles_1m_<symbol>) — fetching + writing them: {preview}{more}",
                flush=True,
            )

        items = list(fetch_starts.items())
        total_fetched = 0
        for i in range(0, len(items), chunk_size):
            chunk = dict(items[i:i + chunk_size])
            fetched = self._fetch_history_candles_batch(chunk, history_existing_tables)
            total_fetched += sum(len(df) for df in fetched.values())
            # Already written to history_store inside
            # _fetch_history_candles_batch — discard before next chunk.
            del fetched
            self._progress("Fetching + writing history-candle gaps", min(i + chunk_size, len(items)), len(items))

        if total_fetched:
            print(
                f"[BACKFILL] History-db candle gaps filled | {total_fetched} candle(s) fetched",
                flush=True,
            )

    def _fetch_ticks_batch(self, fetch_starts: dict, existing_tables: set, fetch_ends: dict = None) -> dict:
        """
        fetch_starts: {symbol: start_ts} — symbols needing a main-db tick
        fetch, each with its own start time (already narrowed to just
        what's missing beyond the local tick cache, in run()'s Phase 1).

        fetch_ends: optional {symbol: end_ts}, only for symbols whose
        fetch must stop before an already-cached, already-validated
        stretch (the widened-lookback case — see
        tick_gap_detector.scan_local_cache()). A symbol absent from
        this dict is fetched open-ended, same as before.

        Returns {symbol: DataFrame} — same columns/session-filtering as
        the old per-symbol _fetch_ticks() used to return — but issued as
        one UNION ALL query covering every symbol by default
        (BACKFILL_BATCH_SIZE=0), instead of one round trip per symbol.
        This is the main lever: ~200 symbols → 1 query instead of ~200.

        Also sets self._last_fetch_failed_symbols to the set of symbols
        whose chunk genuinely failed to query main db this call (network
        drop, connection error, etc.) — as opposed to symbols that were
        successfully queried and confirmed to have zero new rows. The
        caller (run()'s phantom-row cleanup) needs this distinction: a
        symbol simply absent from the returned dict is ambiguous between
        "confirmed empty" and "we never got an answer", and treating a
        failed fetch as confirmed-empty was deleting real cached ticks
        whenever the confirming re-fetch itself happened to fail — see
        run()'s phantom-cleanup block for the full explanation.
        """
        out = {}
        self._last_fetch_failed_symbols = set()
        if self.conn is None or not fetch_starts:
            return out

        fetch_ends = fetch_ends or {}
        items = list(fetch_starts.items())
        skipped_no_table = []

        chunk_size = self.batch_size if self.batch_size > 0 else max(len(items), 1)
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]

            clauses = []
            params  = []
            for symbol, start_ts in chunk:
                safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_")
                table = f"quote_{safe_sym}".lower()
                if table not in existing_tables:
                    skipped_no_table.append(symbol)
                    continue
                start_ms = int(start_ts.timestamp() * 1000)
                end_ts = fetch_ends.get(symbol)
                if end_ts is not None:
                    end_ms = int(end_ts.timestamp() * 1000)
                    clauses.append(
                        f"SELECT %s AS symbol, timestamp, ltp, "
                        f"COALESCE(last_quantity, 0) AS qty, "
                        f"{_QUOTE_EXTRA_SELECT_COLS} FROM {table} "
                        f"WHERE timestamp >= %s AND timestamp <= %s AND ltp IS NOT NULL"
                    )
                    params.extend([symbol, start_ms, end_ms])
                else:
                    clauses.append(
                        f"SELECT %s AS symbol, timestamp, ltp, "
                        f"COALESCE(last_quantity, 0) AS qty, "
                        f"{_QUOTE_EXTRA_SELECT_COLS} FROM {table} "
                        f"WHERE timestamp >= %s AND ltp IS NOT NULL"
                    )
                    params.extend([symbol, start_ms])

            if not clauses:
                continue

            query = " UNION ALL ".join(clauses) + " ORDER BY symbol, timestamp"

            chunk_num = i // chunk_size + 1
            total_chunks = (len(items) + chunk_size - 1) // chunk_size
            # No per-chunk print here on purpose — every caller of this
            # method (the day-start loop, mid-session heals) already
            # owns its own single overwriting _progress() line based on
            # ITS OWN cumulative symbol count, which is the number that
            # actually means something to someone watching (e.g. "20 of
            # 199 symbols done"). This method's own chunk_num/total_chunks
            # is just an internal batching detail — usually 1/1, since a
            # caller typically hands this method one already-small group
            # at a time — and printing it on its own line here (as this
            # used to) produced noise at best and, when it interleaved
            # with a DIFFERENT progress line's carriage-return, visibly
            # garbled output at worst.

            try:
                self._ensure_connected()
                if self.conn is None:
                    raise psycopg2.OperationalError("no connection available")
                cur = self.conn.cursor()
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
            except Exception as exc:
                if self._is_connection_dead(exc):
                    print(
                        f"[BACKFILL][WARN] chunk {chunk_num}/{total_chunks}: "
                        f"connection dropped ({exc}) — reconnecting and retrying "
                        f"this chunk once",
                        flush=True,
                    )
                    self.conn = self._connect()
                    try:
                        if self.conn is not None:
                            cur = self.conn.cursor()
                            cur.execute(query, params)
                            rows = cur.fetchall()
                            cur.close()
                        else:
                            raise psycopg2.OperationalError("reconnect failed")
                    except Exception as exc2:
                        batch_symbols = [s for s, _ in chunk]
                        self._last_fetch_failed_symbols.update(batch_symbols)
                        print(
                            f"[BACKFILL][ERROR] batched tick fetch failed again after "
                            f"reconnect for a chunk of {len(batch_symbols)} symbol(s) "
                            f"(starting {batch_symbols[0]}) — these symbols' phantom-row "
                            f"cleanup (if any) will be SKIPPED this run rather than "
                            f"treated as confirmed-empty: {exc2}",
                            flush=True,
                        )
                        continue
                else:
                    batch_symbols = [s for s, _ in chunk]
                    self._last_fetch_failed_symbols.update(batch_symbols)
                    print(
                        f"[BACKFILL][ERROR] batched tick fetch failed for a chunk of "
                        f"{len(batch_symbols)} symbol(s) (starting {batch_symbols[0]}) — "
                        f"these symbols' phantom-row cleanup (if any) will be SKIPPED "
                        f"this run rather than treated as confirmed-empty: {exc}",
                        flush=True,
                    )
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    continue

            # (no "Chunk N/M: X row(s) received" print here either —
            # same reasoning as above; the outer caller's progress line
            # already reflects work completed)

            if not rows:
                continue

            df = pd.DataFrame(rows, columns=_QUOTE_FULL_DF_COLUMNS)
            df["ist_ts"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)

            # Keep only market-hours rows on weekdays — same filter the old
            # single-symbol _fetch_ticks() applied. Vectorized once across
            # the whole chunk (all symbols together), instead of re-running
            # a .dt.time / .dt.dayofweek comparison per symbol inside the
            # groupby loop below — same anti-pattern class as _aggregate()'s
            # bucket/market-open filters. See
            # is_market_hours_weekday_vectorized()'s docstring in
            # gap_detector.py.
            df = df[is_market_hours_weekday_vectorized(df["ist_ts"])]

            for symbol, group in df.groupby("symbol"):
                g = group.drop(columns=["symbol"]).reset_index(drop=True)
                out[symbol] = g

                # Cache these PG-fetched ticks to the local SQLite tick
                # store too. `g` is already ascending by timestamp (the
                # query is ORDER BY symbol, timestamp), so this is a
                # single ordered block — see tick_writer.py's ordering
                # guarantee docstring for why that matters. Every column
                # PG has (not just timestamp/ltp/qty) rides along so the
                # local cache is a full mirror, not a stripped-down copy.
                if self.tick_writer is not None:
                    rows = [
                        {
                            "timestamp": int(row.timestamp),
                            "ltp": row.ltp,
                            "qty": row.qty,
                            **{c: getattr(row, c) for c in QUOTE_EXTRA_COLUMNS},
                        }
                        for row in g.itertuples(index=False)
                    ]
                    self.tick_writer.enqueue_backfill_rows(symbol, rows)

        if skipped_no_table:
            preview = ", ".join(skipped_no_table[:10])
            more = f" (+{len(skipped_no_table) - 10} more)" if len(skipped_no_table) > 10 else ""
            print(
                f"[BACKFILL][WARN] {len(skipped_no_table)} symbol(s) skipped — "
                f"no quote_ table found: {preview}{more}",
                flush=True,
            )

        # (no "Batched main-db fetch: X/Y symbols returned data" print
        # here either — same reasoning as the two removed above; the
        # caller's own progress line already covers this)
        return out

    # ─────────────────────────────────────────────
    # Fetch depth from PostgreSQL (main db) — BATCHED across symbols
    #
    # Mirrors _fetch_ticks_batch() above, but against depth_<symbol>
    # tables, which have the full typed order-book column set (see
    # pg_writer.py's _DEPTH_COLUMN_DEFS) instead of quote's handful of
    # columns — buy0..buy4/sell0..sell4 × price/qty/orders. Those get
    # reassembled here into the {"bids": [...], "asks": [...]} shape
    # tick_writer.py's local depth storage already uses (matching what
    # enqueue_live() writes for a live depth snapshot), so a backfilled
    # depth row is indistinguishable from a live one once it's cached.
    # ─────────────────────────────────────────────

    # Column order must exactly match pg_writer.py's _DEPTH_COLUMN_DEFS
    # ordering for buy/sell levels (side, level, field) — imported from
    # tick_writer.py (DEPTH_LEVEL_COLUMNS) so there's one single source
    # of truth for this column list, since the local depth_<symbol>
    # SQLite cache now uses the exact same flat column layout instead
    # of a raw_json blob.
    _DEPTH_LEVEL_COLUMNS = DEPTH_LEVEL_COLUMNS

    def _fetch_depth_batch(self, fetch_starts: dict, existing_tables: set, fetch_ends: dict = None) -> dict:
        """
        fetch_starts: {symbol: start_ts} — same shape as
        _fetch_ticks_batch's parameter, just for depth_<symbol> tables.

        fetch_ends: optional {symbol: end_ts} — same meaning as
        _fetch_ticks_batch's fetch_ends; a symbol absent from it is
        fetched open-ended, same as before.

        Returns {symbol: set(timestamp_ms)} — ONLY timestamps, not the
        full bids/asks payload (see the comment at the return-value
        assignment below for why). The full reconstructed order-book
        rows (bids/asks lists of {"price","quantity","orders"} dicts,
        matching what enqueue_live() expects for kind="depth") are
        built and handed to tick_writer.enqueue_backfill_rows() as
        each chunk is processed, then allowed to go out of scope
        immediately rather than being retained in this function's
        return value.
        Also sets self._last_fetch_failed_symbols the same way
        _fetch_ticks_batch does — see that method's docstring and
        run()'s phantom-row cleanup for why this distinction (confirmed
        empty vs. never actually asked) matters.
        """
        out = {}
        self._last_fetch_failed_symbols = set()
        if self.conn is None or not fetch_starts:
            return out

        fetch_ends = fetch_ends or {}
        items = list(fetch_starts.items())
        skipped_no_table = []

        level_cols_sql = ", ".join(self._DEPTH_LEVEL_COLUMNS)

        chunk_size = self.batch_size if self.batch_size > 0 else max(len(items), 1)
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]

            clauses = []
            params  = []
            for symbol, start_ts in chunk:
                safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_")
                table = f"depth_{safe_sym}".lower()
                if table not in existing_tables:
                    skipped_no_table.append(symbol)
                    continue
                start_ms = int(start_ts.timestamp() * 1000)
                end_ts = fetch_ends.get(symbol)
                if end_ts is not None:
                    end_ms = int(end_ts.timestamp() * 1000)
                    clauses.append(
                        f"SELECT %s AS symbol, timestamp, ltp, {level_cols_sql} "
                        f"FROM {table} WHERE timestamp >= %s AND timestamp <= %s"
                    )
                    params.extend([symbol, start_ms, end_ms])
                else:
                    clauses.append(
                        f"SELECT %s AS symbol, timestamp, ltp, {level_cols_sql} "
                        f"FROM {table} WHERE timestamp >= %s"
                    )
                    params.extend([symbol, start_ms])

            if not clauses:
                continue

            query = " UNION ALL ".join(clauses) + " ORDER BY symbol, timestamp"

            chunk_num = i // chunk_size + 1
            total_chunks = (len(items) + chunk_size - 1) // chunk_size
            # No per-chunk print here — see _fetch_ticks_batch()'s
            # identical comment; the caller owns progress reporting.

            try:
                self._ensure_connected()
                if self.conn is None:
                    raise psycopg2.OperationalError("no connection available")
                cur = self.conn.cursor()
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
            except Exception as exc:
                if self._is_connection_dead(exc):
                    print(
                        f"[BACKFILL][WARN] depth chunk {chunk_num}/{total_chunks}: "
                        f"connection dropped ({exc}) — reconnecting and retrying "
                        f"this chunk once",
                        flush=True,
                    )
                    self.conn = self._connect()
                    try:
                        if self.conn is not None:
                            cur = self.conn.cursor()
                            cur.execute(query, params)
                            rows = cur.fetchall()
                            cur.close()
                        else:
                            raise psycopg2.OperationalError("reconnect failed")
                    except Exception as exc2:
                        batch_symbols = [s for s, _ in chunk]
                        self._last_fetch_failed_symbols.update(batch_symbols)
                        print(
                            f"[BACKFILL][ERROR] batched depth fetch failed again after "
                            f"reconnect for a chunk of {len(batch_symbols)} symbol(s) "
                            f"(starting {batch_symbols[0]}) — these symbols' phantom-row "
                            f"cleanup (if any) will be SKIPPED this run rather than "
                            f"treated as confirmed-empty: {exc2}",
                            flush=True,
                        )
                        continue
                else:
                    batch_symbols = [s for s, _ in chunk]
                    self._last_fetch_failed_symbols.update(batch_symbols)
                    print(
                        f"[BACKFILL][ERROR] batched depth fetch failed for a chunk of "
                        f"{len(batch_symbols)} symbol(s) (starting {batch_symbols[0]}) — "
                        f"these symbols' phantom-row cleanup (if any) will be SKIPPED "
                        f"this run rather than treated as confirmed-empty: {exc}",
                        flush=True,
                    )
                    try:
                        self.conn.rollback()
                    except Exception:
                        pass
                    continue

            # (no "Depth chunk N/M: X row(s) received" print here — see
            # _fetch_ticks_batch()'s identical comment)

            if not rows:
                continue

            columns = ["symbol", "timestamp", "ltp"] + list(self._DEPTH_LEVEL_COLUMNS)
            df = pd.DataFrame(rows, columns=columns)
            df["ist_ts"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)
            df = df[is_market_hours_weekday_vectorized(df["ist_ts"])]

            for symbol, group in df.groupby("symbol"):
                g = group.drop(columns=["symbol", "ist_ts"]).reset_index(drop=True)

                # Rows are handed to tick_writer AS-IS from PG — same
                # flat buy0_price/buy0_qty/.../sell4_orders columns,
                # no bids/asks reconstruction into nested dicts and no
                # JSON encoding. That reconstruction (10 dicts/row) used
                # to be the main driver of depth backfill's RAM usage;
                # now depth rows cost the same "just numbers" memory a
                # quote row does. Nested bids/asks shape is only ever
                # built where something in RAM actually needs it (see
                # _seed_depth_ram), not on this hot fetch/write path.
                bulk_rows = g.to_dict("records")

                if self.tick_writer is not None:
                    self.tick_writer.enqueue_backfill_rows(symbol, bulk_rows, kind="depth")

                # Only ever accumulate timestamps in `out`, NOT the full
                # depth payload — the only caller (the phantom-row check
                # in _run_depth_backfill) only reads timestamps. Kept
                # deliberately separate from bulk_rows (which goes out
                # of scope right here) rather than retained in `out` for
                # the rest of the entire depth backfill run.
                # `out[symbol]` accumulates across chunks (a symbol can
                # appear in more than one chunk), so union with anything
                # already there instead of overwriting it.
                new_ts = set(int(t) for t in g["timestamp"])
                out[symbol] = out.get(symbol, set()) | new_ts

        if skipped_no_table:
            preview = ", ".join(skipped_no_table[:10])
            more = f" (+{len(skipped_no_table) - 10} more)" if len(skipped_no_table) > 10 else ""
            print(
                f"[BACKFILL][WARN] {len(skipped_no_table)} symbol(s) skipped — "
                f"no depth_ table found: {preview}{more}",
                flush=True,
            )

        # (no "Batched main-db depth fetch: X/Y symbols returned data"
        # print here — see _fetch_ticks_batch()'s identical comment)
        return out

    # ─────────────────────────────────────────────
    # Aggregate ticks → OHLC for one TF
    # ─────────────────────────────────────────────

    # ─────────────────────────────────────────────
    # Fetch history-db candles for every symbol newer than what's
    # already locally cached — BATCHED. Always run now (priority
    # source), not just as a last-resort gap filler.
    # ─────────────────────────────────────────────

    def _fetch_history_candles_batch(self, fetch_starts: dict, existing_tables: set) -> dict:
        """
        market_history DB — confirmed against x9_data_fetcher's own
        BackfillManager/pg_writer.py: rows here are pre-built candles
        fetched from the broker's REST history API, ONE ROW PER CANDLE
        AT self.history_native_tf granularity (default "1m") — typed
        open/high/low/close/volume columns (same schema as the main db),
        ltp/ltt left NULL since the history API never returns them, and
        NO interval column at all (pg_writer's typed schema has none —
        there was never anything to tag "1m" vs "5m" with).

        fetch_starts: {symbol: datetime} — mirrors _fetch_ticks_batch's
        shape exactly: fetch everything from that point to now, instead
        of a specific list of missing buckets, since history-db candles
        are now always pulled in full (Phase 1h/2h) rather than only
        for gaps tick-aggregation left behind.

        Returns {symbol: DataFrame[timestamp, open, high, low, close,
        volume]}, timestamp as tz-aware Kolkata, ascending. Also caches
        every fetched row into self.history_store (if configured) as a
        side effect — same pattern _fetch_ticks_batch uses for
        self.tick_writer.
        """
        out = {}
        conn = self._get_history_conn()
        if conn is None or not fetch_starts:
            return out

        items = list(fetch_starts.items())
        skipped_no_table = []

        chunk_size = self.batch_size if self.batch_size > 0 else max(len(items), 1)
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]

            clauses = []
            params  = []
            for symbol, start_ts in chunk:
                safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_")
                table = f"quote_{safe_sym}".lower()
                if table not in existing_tables:
                    skipped_no_table.append(symbol)
                    continue
                start_ms = int(start_ts.timestamp() * 1000)
                clauses.append(
                    f"SELECT %s AS symbol, timestamp, open, high, low, close, "
                    f"COALESCE(volume, 0) AS volume FROM {table} "
                    f"WHERE timestamp >= %s AND open IS NOT NULL"
                )
                params.extend([symbol, start_ms])

            if not clauses:
                continue

            query = " UNION ALL ".join(clauses) + " ORDER BY symbol, timestamp"

            chunk_num = i // chunk_size + 1
            total_chunks = (len(items) + chunk_size - 1) // chunk_size
            # No per-chunk print here — see _fetch_ticks_batch()'s
            # identical comment; the caller owns progress reporting.

            try:
                conn = self._get_history_conn()
                if conn is None:
                    raise psycopg2.OperationalError("no history-db connection available")
                cur = conn.cursor()
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
            except Exception as exc:
                retried_ok = False
                if self._is_connection_dead(exc):
                    print(
                        f"[BACKFILL][WARN] history chunk {chunk_num}/{total_chunks}: "
                        f"connection dropped ({exc}) — reconnecting and retrying "
                        f"this chunk once",
                        flush=True,
                    )
                    self.conn_history = None
                    self._history_connect_tried = False
                    conn = self._get_history_conn()
                    try:
                        if conn is not None:
                            cur = conn.cursor()
                            cur.execute(query, params)
                            rows = cur.fetchall()
                            cur.close()
                            retried_ok = True
                        else:
                            raise psycopg2.OperationalError("reconnect failed")
                    except Exception as exc2:
                        print(
                            f"[BACKFILL][WARN] batched history fetch failed again "
                            f"after reconnect for a chunk of {len(chunk)} symbol(s): {exc2}",
                            flush=True,
                        )
                else:
                    print(
                        f"[BACKFILL][WARN] batched history fetch failed for a chunk "
                        f"of {len(chunk)} symbol(s): {exc}",
                        flush=True,
                    )

                if not retried_ok:
                    try:
                        if conn is not None:
                            conn.rollback()
                    except Exception:
                        pass
                    continue

            # (no "History chunk N/M: X row(s) received" print here —
            # see _fetch_ticks_batch()'s identical comment)

            if not rows:
                continue

            df = pd.DataFrame(
                rows, columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"]
            )

            for symbol, group in df.groupby("symbol"):
                g = group.drop(columns=["symbol"]).sort_values("timestamp").reset_index(drop=True)

                # Cache these PG-fetched candles to the local history
                # store BEFORE converting timestamp to a tz-aware
                # Kolkata Timestamp — HistoryCandleStore wants raw ms
                # ints, same convention as TickWriter.
                if self.history_store is not None:
                    cache_rows = [
                        {
                            "timestamp": int(row.timestamp),
                            "open": row.open, "high": row.high,
                            "low": row.low, "close": row.close,
                            "volume": row.volume,
                        }
                        for row in g.itertuples(index=False)
                    ]
                    self.history_store.save_candles(symbol, cache_rows)

                g["timestamp"] = pd.to_datetime(g["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)
                out[symbol] = g

        if skipped_no_table:
            preview = ", ".join(skipped_no_table[:10])
            more = f" (+{len(skipped_no_table) - 10} more)" if len(skipped_no_table) > 10 else ""
            print(
                f"[BACKFILL][WARN] {len(skipped_no_table)} symbol(s) skipped in "
                f"history-db batch — no quote_ table found: {preview}{more}",
                flush=True,
            )

        # (no "Batched history-db fetch: X/Y symbols returned data"
        # print here — see _fetch_ticks_batch()'s identical comment)
        return out

    def _fetch_history_timestamps_batch(self, symbols: list, start_ms: int, end_ms: int, existing_tables: set) -> dict:
        """
        Lightweight sibling of _fetch_history_candles_batch(): same
        upstream table/window, but SELECTs only `timestamp` (no OHLCV
        columns, no DataFrame, no history_store write) — just enough
        to know EXACTLY which candle timestamps upstream actually has
        in [start_ms, end_ms) for each symbol. Used to diff against
        what's already cached locally (history_store.read_timestamps())
        and find real gaps, without assuming a full per-minute schedule
        — see sync_history_cache_with_main_db()'s docstring for why
        that assumption doesn't hold here.

        Returns {symbol: set(ts_ms)}. A symbol with nothing upstream in
        the window (or no quote_<symbol> table at all) is simply absent
        from the result rather than mapped to an empty set.
        """
        out = {}
        conn = self._get_history_conn()
        if conn is None or not symbols:
            return out

        chunk_size = self.batch_size if self.batch_size > 0 else max(len(symbols), 1)
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]

            clauses, params = [], []
            for symbol in chunk:
                safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_")
                table = f"quote_{safe_sym}".lower()
                if table not in existing_tables:
                    continue
                clauses.append(
                    f"SELECT %s AS symbol, timestamp FROM {table} "
                    f"WHERE timestamp >= %s AND timestamp < %s AND open IS NOT NULL"
                )
                params.extend([symbol, start_ms, end_ms])

            if not clauses:
                continue

            query = " UNION ALL ".join(clauses)

            try:
                conn = self._get_history_conn()
                if conn is None:
                    raise psycopg2.OperationalError("no history-db connection available")
                cur = conn.cursor()
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
            except Exception as exc:
                retried_ok = False
                if self._is_connection_dead(exc):
                    print(
                        f"[BACKFILL][WARN] history-timestamp chunk: connection dropped "
                        f"({exc}) — reconnecting and retrying this chunk once",
                        flush=True,
                    )
                    self.conn_history = None
                    self._history_connect_tried = False
                    conn = self._get_history_conn()
                    try:
                        if conn is not None:
                            cur = conn.cursor()
                            cur.execute(query, params)
                            rows = cur.fetchall()
                            cur.close()
                            retried_ok = True
                        else:
                            raise psycopg2.OperationalError("reconnect failed")
                    except Exception as exc2:
                        print(
                            f"[BACKFILL][WARN] batched history-timestamp fetch failed "
                            f"again after reconnect for a chunk of {len(chunk)} "
                            f"symbol(s): {exc2}",
                            flush=True,
                        )
                else:
                    print(
                        f"[BACKFILL][WARN] batched history-timestamp fetch failed for "
                        f"a chunk of {len(chunk)} symbol(s): {exc}",
                        flush=True,
                    )

                if not retried_ok:
                    try:
                        if conn is not None:
                            conn.rollback()
                    except Exception:
                        pass
                    continue

            for symbol, ts_ms in rows:
                out.setdefault(symbol, set()).add(int(ts_ms))

        return out

    # ─────────────────────────────────────────────
    # Daily candle mirror — market_history's daily_<symbol> tables
    # ─────────────────────────────────────────────

    def _local_pg_params(self) -> dict:
        """Connection params for the LOCAL Postgres cache — same env vars
        as tick_writer.py's PostgresTickWriter._params (PG_LOCAL_HOST/
        PORT/DBNAME/USER/PASSWORD). Defined here independently rather
        than reaching into self.tick_writer, since that may be a
        SQLiteTickWriter (or None) — daily_<symbol> is mirrored verbatim
        as a real Postgres/JSONB table, so this always needs its own
        direct Postgres connection regardless of which backend the tick
        cache itself is using."""
        return {
            "host":            os.getenv("PG_LOCAL_HOST", "localhost"),
            "port":            int(os.getenv("PG_LOCAL_PORT", "5432")),
            "dbname":          os.getenv("PG_LOCAL_DBNAME", "tickcache"),
            "user":            os.getenv("PG_LOCAL_USER", "tickcache"),
            "password":        os.getenv("PG_LOCAL_PASSWORD", ""),
            "connect_timeout": 10,
        }

    def _daily_retention_cutoff_ms(self) -> int:
        """
        Epoch-ms cutoff for the local daily-candle cache's retention
        window: keep only the most recent 30 TRADING days (not 30
        calendar days) — same trading-day sizing convention used
        everywhere else in this file (see _compute_lookback_start()).
        Anything timestamped before this cutoff gets pruned from the
        LOCAL mirror only in fetch_daily_candles()'s prune phase — the
        source market_history table is never touched.
        """
        cutoff_date = trading_day_n_back(30)
        cutoff_dt   = datetime.combine(cutoff_date, dtime.min, tzinfo=tz_kolkata)
        return int(cutoff_dt.timestamp() * 1000)

    def fetch_daily_candles(self, symbols) -> None:
        """
        Mirror market_history's daily_<symbol> tables (written by
        x9_data_fetcher's DailyCloseManager — see pg_writer.py's
        PgWriter(table="daily", ...)) into the local Postgres cache,
        under the EXACT SAME table name and schema as the source:

            daily_<symbol> (timestamp BIGINT NOT NULL, ingest_ns BIGINT,
                             raw_json JSONB NOT NULL)

        This is a straight mirror — no schema conversion, unlike quote_/
        depth_'s typed-column local cache (see PG_LOCAL_QUOTE_COLUMNS in
        tick_writer.py). Nothing else in x9 reads daily_<symbol> — the
        similarly-named "history-db candles" phase above
        (_fetch_history_candles_batch) reads market_history's
        quote_<symbol> tables instead, at 1m granularity; that's a
        completely separate pipeline that happens to share the same
        database. This method only makes daily_<symbol> queryable
        locally — wiring it into candle-building is a separate step.

        Four explicit phases, matching the check-then-fetch pattern
        used for ticks elsewhere in this file (TickGapDetector.
        scan_local_cache() + sync_local_cache_with_main_db()) instead
        of doing the local-cache check and the fetch interleaved,
        symbol-by-symbol, in one pass like this used to:

          Phase 1 — check: for every symbol, look at what the LOCAL
          daily_<symbol> table already has (just MAX(timestamp) — see
          Phase 1d below for the mid-range case that alone can't
          catch) and note the resume point.
          Phase 1d — mid-range gap check: MAX(timestamp) alone only
          proves the cache is caught up at the LEADING edge; a day
          silently missing in the MIDDLE of already-cached history
          (e.g. a run that died partway through) would never surface
          from a resume point alone, since every later day's presence
          still makes MAX(timestamp) look fully caught up. Unlike
          tick_gap_detector's LAG()-based approach, this can't use a
          fixed gap-size threshold — daily candles don't have one
          uniform expected cadence the way ticks do within a session
          (weekends/holidays make the calendar itself irregular, so a
          single missing weekday can be a SMALLER gap than a normal
          weekend) — so instead it's an anti-join against the actual
          trading-day calendar: the ~30 expected trading dates in the
          retention window are computed once in Python and passed as
          one array parameter, and each symbol's branch does
          `expected EXCEPT actual` server-side to get back just its
          missing dates, never a full row pull.
          Phase 2 — fetch: pull only rows newer than that resume point
          from market_history for each symbol — never a full re-fetch.
          A per-symbol UNIQUE index on timestamp (matching the source's
          own dedup_on_timestamp=True) makes the insert idempotent via
          ON CONFLICT DO NOTHING too, so even a re-fetched overlapping
          range would be harmless — belt-and-suspenders alongside the
          "only fetch what's newer" resume logic.
          Phase 3 — prune: delete local rows older than the
          30-trading-day retention window (_daily_retention_cutoff_ms()).
          Runs every time for every locally-cached symbol, independent
          of whether that symbol had anything new to fetch this run —
          this is what keeps the local mirror from growing forever.
        """
        history_conn = self._get_history_conn()
        if history_conn is None:
            print(
                "[BACKFILL][WARN] Daily candle fetch skipped — no history-db connection",
                flush=True,
            )
            return

        daily_existing = self._existing_tables_or_empty(
            self._load_existing_tables(history_conn, prefix="daily"), "daily", "daily candle backfill"
        )
        if not daily_existing:
            print("[BACKFILL] Daily candle fetch: no daily_ tables found in history db", flush=True)
            return

        try:
            local_conn = psycopg2.connect(**self._local_pg_params())
            local_conn.autocommit = False
        except Exception as exc:
            print(
                f"[BACKFILL][ERROR] Daily candle fetch: local cache connection failed: {exc}",
                flush=True,
            )
            return

        cutoff_ts_ms = self._daily_retention_cutoff_ms()

        resume_points          = {}   # symbol -> local MAX(timestamp), or None if empty
        tables_by_symbol       = {}   # symbol -> local table name (only symbols with a source table)
        total_symbols_skipped  = 0
        total_rows_fetched     = 0
        total_symbols_written  = 0
        total_rows_pruned      = 0

        try:
            local_cur = local_conn.cursor()

            # Phase 1 — check: what does the local cache already have?
            #
            # This used to run CREATE TABLE + 2x CREATE INDEX + SELECT
            # MAX(timestamp) separately for EVERY symbol — up to 4 real
            # round trips x 199 symbols, every single run, even though
            # the table/indexes only ever need creating once (after
            # that, IF NOT EXISTS was just paying the round-trip cost to
            # confirm something that was already true). Same shape as
            # the quote/depth "199 individual queries" problem
            # max_ts_batch() fixed in tick_writer.py, so it gets the
            # same two-step fix here:
            #   1a. one batched existence check (tablename = ANY(%s))
            #       for which LOCAL daily_ tables already exist
            #   1b. CREATE TABLE/INDEX only for the symbols actually
            #       missing one locally (first run only, in practice)
            #   1c. one UNION ALL per chunk for MAX(timestamp) across
            #       every symbol whose local table now exists, instead
            #       of one SELECT per symbol
            symbol_table = {}
            for inst in symbols:
                symbol   = inst["symbol"]
                safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_").lower()
                table    = f"daily_{safe_sym}"
                if table in daily_existing:
                    symbol_table[symbol] = table
                else:
                    total_symbols_skipped += 1

            # 1a. Which of these tables already exist in the LOCAL cache?
            # One round trip regardless of symbol count.
            all_local_tables = list(symbol_table.values())
            existing_local_tables = set()
            if all_local_tables:
                local_cur.execute(
                    "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename = ANY(%s)",
                    (all_local_tables,),
                )
                existing_local_tables = {row[0] for row in local_cur.fetchall()}

            # 1b. DDL can't be batched into one round trip the way a read
            # can (each CREATE TABLE is its own statement) — but this
            # only runs for a symbol the very first time its daily_
            # table is mirrored locally, so after the first run this
            # loop is empty for everyone.
            newly_created = 0
            for i, (symbol, table) in enumerate(symbol_table.items(), start=1):
                if table not in existing_local_tables:
                    # Same DDL as x9_data_fetcher's pg_writer.py
                    # _ensure_table for prefix="daily" — table name
                    # included, verbatim.
                    local_cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {table} ("
                        f"    timestamp BIGINT NOT NULL,"
                        f"    ingest_ns BIGINT,"
                        f"    raw_json JSONB NOT NULL"
                        f")"
                    )
                    local_cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table} ON {table} (timestamp)")
                    local_cur.execute(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS uidx_{table}_ts ON {table} (timestamp)"
                    )
                    local_conn.commit()
                    newly_created += 1
                self._progress("Checking local daily cache", i, len(symbol_table))

            tables_by_symbol = dict(symbol_table)
            resume_points = {s: None for s in tables_by_symbol}

            # 1c. Batched MAX(timestamp) resume-point check — one UNION
            # ALL per chunk instead of one SELECT per symbol, same
            # savepoint-per-chunk isolation as tick_writer.max_ts_batch()
            # so one bad/corrupt table can't blank out the rest of the
            # chunk's results.
            _DAILY_CHUNK_SIZE = 150
            symbol_items = list(tables_by_symbol.items())
            for start in range(0, len(symbol_items), _DAILY_CHUNK_SIZE):
                chunk = symbol_items[start:start + _DAILY_CHUNK_SIZE]
                branches, params = [], []
                for s, t in chunk:
                    branches.append(f"SELECT %s AS symbol, MAX(timestamp) AS max_ts FROM {t}")
                    params.append(s)
                query = " UNION ALL ".join(branches)

                local_cur.execute("SAVEPOINT sp_daily_maxts_chunk")
                try:
                    local_cur.execute(query, params)
                    for sym, max_ts in local_cur.fetchall():
                        resume_points[sym] = max_ts
                    local_cur.execute("RELEASE SAVEPOINT sp_daily_maxts_chunk")
                except Exception as exc:
                    local_cur.execute("ROLLBACK TO SAVEPOINT sp_daily_maxts_chunk")
                    print(
                        f"[BACKFILL][WARN] daily cache MAX(timestamp) batch failed ({exc}); "
                        f"falling back to per-symbol checks for this chunk ({len(chunk)} symbols)",
                        flush=True,
                    )
                    for s, t in chunk:
                        try:
                            local_cur.execute(f"SELECT MAX(timestamp) FROM {t}")
                            row = local_cur.fetchone()
                            resume_points[s] = row[0] if row else None
                            local_conn.commit()
                        except Exception as inner_exc:
                            print(f"[BACKFILL][WARN] daily MAX(timestamp) failed for {s}: {inner_exc}", flush=True)
                            local_conn.rollback()
                local_conn.commit()
                self._progress(
                    "Checking local daily cache", min(start + _DAILY_CHUNK_SIZE, len(symbol_items)), len(symbol_items)
                )

            print(
                f"[BACKFILL] Daily cache check complete | {len(tables_by_symbol)}/{len(symbols)} "
                f"symbol(s) have a daily_ table in history db "
                f"({total_symbols_skipped} skipped — no daily_ table in history db, "
                f"{newly_created} newly mirrored locally)",
                flush=True,
            )

            # Phase 1d — mid-range gap check: Phase 1c's resume points
            # only catch gaps at the LEADING edge (Phase 2 fetches
            # "newer than local MAX(timestamp)") — a day silently
            # missing in the MIDDLE of already-cached history (a run
            # that died partway through, a transient history-db hiccup
            # on one specific date) would never surface from a resume
            # point alone, since every later day's presence still makes
            # MAX(timestamp) look fully caught up.
            #
            # Tried a straight port of tick_gap_detector's LAG()
            # approach first (flag any consecutive-row gap bigger than
            # a fixed day count) — it doesn't actually work here. Ticks
            # have one uniform expected cadence throughout a session, so
            # "bigger than usual" is a clean signal; daily candles don't
            # — the calendar itself is irregular (weekends, holidays),
            # so a single midweek day silently missing (Tue -> Thu, a
            # 2-day gap) is SMALLER than a completely ordinary Friday ->
            # Monday weekend (3 days). Any fixed threshold generous
            # enough to not misfire on every normal weekend also lets a
            # single missing day sail straight through — confirmed by
            # testing both cases before shipping this.
            #
            # The correct check is an anti-join against the actual
            # trading calendar (already known in Python via
            # trading_day_n_back()/is_trading_day() — the same source
            # of truth _daily_retention_cutoff_ms() uses), not a
            # distance-based heuristic. Still pushed into Postgres in
            # the same spirit as session_gap_batch() though: the full
            # list of expected trading dates in the retention window
            # (~30 of them) is passed once as an array parameter, and
            # each symbol's branch does `expected EXCEPT actual` to get
            # back only the missing dates — a handful of small date
            # values per symbol, never a full row pull.
            expected_dates = []
            day = trading_day_n_back(30)
            yesterday = (now_kolkata() - timedelta(days=1)).date()
            while day <= yesterday:
                if is_trading_day(day):
                    expected_dates.append(day)
                day += timedelta(days=1)

            missing_dates_by_symbol = {}
            if expected_dates:
                for start in range(0, len(symbol_items), _DAILY_CHUNK_SIZE):
                    chunk = symbol_items[start:start + _DAILY_CHUNK_SIZE]
                    branches, params = [], []
                    for s, t in chunk:
                        branches.append(f"""
                            SELECT %s AS symbol, missing.d AS missing_date
                            FROM (
                                SELECT unnest(%s::date[]) AS d
                                EXCEPT
                                SELECT (to_timestamp(timestamp / 1000.0) AT TIME ZONE 'Asia/Kolkata')::date
                                FROM {t}
                                WHERE timestamp >= %s
                            ) missing
                        """)
                        params.extend([s, expected_dates, cutoff_ts_ms])
                    query = " UNION ALL ".join(branches)

                    local_cur.execute("SAVEPOINT sp_daily_gap_chunk")
                    try:
                        local_cur.execute(query, params)
                        for sym, missing_date in local_cur.fetchall():
                            missing_dates_by_symbol.setdefault(sym, []).append(missing_date)
                        local_cur.execute("RELEASE SAVEPOINT sp_daily_gap_chunk")
                    except Exception as exc:
                        local_cur.execute("ROLLBACK TO SAVEPOINT sp_daily_gap_chunk")
                        print(
                            f"[BACKFILL][WARN] daily gap-check batch failed ({exc}) — "
                            f"skipping mid-range gap check for this chunk ({len(chunk)} symbols)",
                            flush=True,
                        )
                    local_conn.commit()

            if missing_dates_by_symbol:
                preview = ", ".join(list(missing_dates_by_symbol)[:10])
                more = f" (+{len(missing_dates_by_symbol) - 10} more)" if len(missing_dates_by_symbol) - 10 > 0 else ""
                total_missing_dates = sum(len(v) for v in missing_dates_by_symbol.values())
                print(
                    f"[BACKFILL][WARN] {len(missing_dates_by_symbol)} symbol(s) have "
                    f"{total_missing_dates} confirmed missing trading day(s) in their "
                    f"cached daily candles — re-fetching just those dates: {preview}{more}",
                    flush=True,
                )

                total_gap_rows_filled = 0
                for symbol, missing_dates in missing_dates_by_symbol.items():
                    table = tables_by_symbol[symbol]
                    day_start_ms = int(datetime.combine(min(missing_dates), dtime.min, tzinfo=tz_kolkata).timestamp() * 1000)
                    day_end_ms   = int(datetime.combine(max(missing_dates) + timedelta(days=1), dtime.min, tzinfo=tz_kolkata).timestamp() * 1000)

                    hist_cur = history_conn.cursor()
                    try:
                        hist_cur.execute(
                            f"SELECT timestamp, ingest_ns, raw_json FROM {table} "
                            f"WHERE timestamp >= %s AND timestamp < %s ORDER BY timestamp",
                            (day_start_ms, day_end_ms),
                        )
                        rows = hist_cur.fetchall()
                    except Exception as exc:
                        print(f"[BACKFILL][WARN] daily gap re-fetch failed for {symbol}: {exc}", flush=True)
                        rows = []
                    finally:
                        hist_cur.close()

                    if rows:
                        rows = [
                            (
                                ts,
                                ingest_ns,
                                psycopg2.extras.Json(raw_json) if isinstance(raw_json, dict) else raw_json,
                            )
                            for ts, ingest_ns, raw_json in rows
                        ]
                        psycopg2.extras.execute_values(
                            local_cur,
                            f"INSERT INTO {table} (timestamp, ingest_ns, raw_json) VALUES %s "
                            f"ON CONFLICT (timestamp) DO NOTHING",
                            rows,
                        )
                        local_conn.commit()
                        total_gap_rows_filled += len(rows)

                if total_gap_rows_filled:
                    print(
                        f"[BACKFILL] Daily mid-range gap fill complete | "
                        f"{total_gap_rows_filled} row(s) recovered across "
                        f"{len(missing_dates_by_symbol)} symbol(s)",
                        flush=True,
                    )

            # Phase 2 — fetch: pull only what's not present locally.
            for i, (symbol, table) in enumerate(tables_by_symbol.items(), start=1):
                local_max_ts = resume_points[symbol]

                hist_cur = history_conn.cursor()
                try:
                    if local_max_ts is not None:
                        hist_cur.execute(
                            f"SELECT timestamp, ingest_ns, raw_json FROM {table} "
                            f"WHERE timestamp > %s ORDER BY timestamp",
                            (local_max_ts,),
                        )
                    else:
                        hist_cur.execute(
                            f"SELECT timestamp, ingest_ns, raw_json FROM {table} ORDER BY timestamp"
                        )
                    rows = hist_cur.fetchall()
                finally:
                    hist_cur.close()

                if rows:
                    # psycopg2 auto-deserializes a JSONB column on SELECT
                    # into a plain Python dict — rows[i][2] here is
                    # already a dict, not a string. Handing that dict
                    # straight back to execute_values() for the INSERT
                    # fails with "can't adapt type 'dict'": psycopg2 needs
                    # an explicit psycopg2.extras.Json(...) wrapper to
                    # serialize a dict back INTO a JSONB column — the
                    # auto-adaptation only works in the read direction.
                    # raw_json can also come back as a plain str (if the
                    # source driver/version didn't auto-parse it) or None
                    # (NULL) — only wrap actual dicts, pass anything else
                    # through unchanged.
                    rows = [
                        (
                            ts,
                            ingest_ns,
                            psycopg2.extras.Json(raw_json) if isinstance(raw_json, dict) else raw_json,
                        )
                        for ts, ingest_ns, raw_json in rows
                    ]
                    psycopg2.extras.execute_values(
                        local_cur,
                        f"INSERT INTO {table} (timestamp, ingest_ns, raw_json) VALUES %s "
                        f"ON CONFLICT (timestamp) DO NOTHING",
                        rows,
                    )
                    local_conn.commit()
                    total_rows_fetched    += len(rows)
                    total_symbols_written += 1

                self._progress("Fetching daily candles", i, len(tables_by_symbol))

            print(
                f"[BACKFILL] Daily candle fetch complete | {total_rows_fetched} row(s) "
                f"across {total_symbols_written} symbol(s)",
                flush=True,
            )

            # Phase 3 — prune: drop anything older than the 30-trading-day
            # retention window, for every locally-cached symbol (not just
            # ones that had something new to fetch this run).
            for i, (symbol, table) in enumerate(tables_by_symbol.items(), start=1):
                local_cur.execute(f"DELETE FROM {table} WHERE timestamp < %s", (cutoff_ts_ms,))
                total_rows_pruned += local_cur.rowcount
                local_conn.commit()
                self._progress("Pruning local daily cache", i, len(tables_by_symbol))

            print(
                f"[BACKFILL] Daily candle prune complete | {total_rows_pruned} row(s) older than "
                f"30 trading days removed across {len(tables_by_symbol)} symbol(s)",
                flush=True,
            )
        except Exception as exc:
            print(f"[BACKFILL][ERROR] Daily candle fetch failed: {exc}", flush=True)
            try:
                local_conn.rollback()
            except Exception:
                pass
        finally:
            try:
                local_conn.close()
            except Exception:
                pass

    # ─────────────────────────────────────────────
    # Lookback window calculation
    # ─────────────────────────────────────────────

    def _compute_lookback_start(self) -> datetime:
        """
        Go back far enough to guarantee MIN_CANDLES of the largest configured TF.
        Formula:
            trading_days_needed = ceil( (MIN_CANDLES × largest_tf_minutes) / 375 ) + 1 buffer day
        375 = market minutes per session (9:15 → 15:30).
        """
        largest_tf_secs     = max(s for _, s in self.timeframes)
        minutes_needed      = (self.min_candles * largest_tf_secs) / 60
        trading_days_needed = math.ceil(minutes_needed / SESSION_MINUTES) + 1

        now = datetime.now(tz_kolkata)
        day = now.date()
        counted = 0
        while counted < trading_days_needed:
            day -= timedelta(days=1)
            if is_trading_day(day):   # weekday AND not a holiday, incl. special_open
                counted += 1

        return datetime.combine(day, dtime(9, 15, 0)).replace(tzinfo=tz_kolkata)

    # ─────────────────────────────────────────────
    # Validation
    # ─────────────────────────────────────────────

    def _validate(self, symbols):
        print("[BACKFILL] Validating candle counts...", flush=True)
        all_ok   = True
        any_gaps = False

        for inst in symbols:
            symbol = inst["symbol"]
            for tf_str, _ in self.timeframes:
                count = len(
                    self.ohlc.ohlc_data.get(tf_str, {}).get(symbol, [])
                )
                missing = self.missing_counts.get((symbol, tf_str), 0)
                verdict = self.tick_gaps.classify_symbol_tf(count, missing, self.min_candles)

                if verdict == "insufficient":
                    print(
                        f"[BACKFILL][WARN] {symbol} {tf_str}: "
                        f"{count}/{self.min_candles} candles — insufficient",
                        flush=True,
                    )
                    all_ok = False
                elif verdict == "gap":
                    print(
                        f"[BACKFILL] ⚠️  {symbol} {tf_str}: {count} candles "
                        f"loaded, but {missing} candle(s) are DATA MISSING "
                        f"(gap in main db + history db, not recoverable)",
                        flush=True,
                    )
                    any_gaps = True
                else:
                    print(
                        f"[BACKFILL] ✅ {symbol} {tf_str}: {count} candles",
                        flush=True,
                    )

        if not all_ok:
            print("[BACKFILL][WARN] Some symbols have insufficient candle history", flush=True)
        elif any_gaps:
            print(
                "[BACKFILL] ⚠️  All symbols meet minimum candle count, "
                "but some have DATA GAPS — see warnings above",
                flush=True,
            )
        else:
            print("[BACKFILL] ✅ All symbols have sufficient candle history, no gaps", flush=True)
