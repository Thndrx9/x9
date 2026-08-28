# backfill_manager.py

import os
import re
import math
import psycopg2
import pandas as pd
from collections import deque
from typing import Optional
from datetime import datetime, timedelta, time as dtime
from dotenv import load_dotenv
from market_time import tz_kolkata, MARKET_OPEN, MARKET_CLOSE, is_trading_day, now_kolkata, is_market_open
from tick_writer import DEPTH_LEVEL_COLUMNS
from gap_detector import (
    GapDetector,
    is_market_hours_weekday_vectorized,
)
from tick_gap_detector import TickGapDetector

load_dotenv()

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
        the first call. run() now calls this unconditionally near the
        top (Phase 0h) since history-db candles are always fetched for
        every symbol (not just as a last-resort gap filler anymore —
        see the module docstring and _aggregate_symbol_with_history()),
        but the lazy-connect-and-cache shape is kept as-is so anything
        else calling this mid-run still gets the same connection
        without reconnecting.
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
        """
        end = "\n" if current >= total else ""
        print(f"\r[BACKFILL] {label}: {current}/{total} symbols", end=end, flush=True)

    def _load_existing_tables(self, conn, prefix: str = "quote") -> set:
        """
        One query: which <prefix>_% tables actually exist in this db
        (prefix is "quote" or "depth"). Needed BEFORE building any
        UNION ALL batch — Postgres fails the entire batched query if
        even one clause references a table that doesn't exist, so
        symbols without a table yet get filtered out up front instead
        of blowing up the whole chunk.
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
            return set()

    def _load_existing_quote_tables(self, conn) -> set:
        """Back-compat alias — see _load_existing_tables()."""
        return self._load_existing_tables(conn, prefix="quote")

    # ─────────────────────────────────────────────
    # Entry point
    # ─────────────────────────────────────────────

    def run(self, symbols):
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
            f"| TFs={tf_names} | from={start_ts.strftime('%Y-%m-%d %H:%M %Z')}",
            flush=True,
        )

        # ── Phase 0: which quote_ tables exist in the main db AND history db ──
        # History db tables are loaded here too (not lazily on first gap
        # anymore) since history-db candles are now always fetched for
        # every symbol — see Phase 2h.
        existing_tables = self._load_existing_quote_tables(self.conn)
        history_conn = self._get_history_conn()
        history_existing_tables = self._load_existing_quote_tables(history_conn)

        # ── Phase 1: local-SQLite-only pass — pure disk I/O, no main-db query ──
        # Instead of checking pre-computed OHLC parquet files for
        # completeness, check the local raw-tick cache (ticks.db, via
        # TickWriter.max_ts) for what's already cached per symbol, and
        # only fetch the ticks NEWER than that from the main db in
        # Phase 2. Whatever's already cached gets combined with the
        # freshly fetched ticks before aggregation (Phase 3), so
        # candles are still built over the FULL lookback window — just
        # without re-downloading ticks Postgres already gave us on a
        # previous run.
        #
        # Before trusting the cached range, verify it: max_ts is only
        # a high-water mark — it doesn't guarantee everything before it
        # is actually present (a crash mid-write, a failed insert
        # batch, etc. can leave a silent hole earlier in the range).
        # TickGapDetector.find_cache_gap() checks for same-session silent
        # gaps larger than gap_threshold_secs (overnight/weekend/
        # holiday gaps between sessions are expected and never
        # flagged). If one's found, everything from just after it
        # onward is treated as untrusted and re-fetched from the main
        # db — NOT deleted first. The re-fetch corrects any wrong rows
        # in place via upsert (see tick_writer.py's unique-index/
        # upsert change), and a separate phantom-row check afterward
        # (once we know exactly what the main db returned for this
        # range) removes only rows PROVABLY absent from the main db —
        # see _phantom_row_check() below, called from Phase 4.
        start_ms = int(start_ts.timestamp() * 1000)
        now_ms   = int(now.timestamp() * 1000)

        quote_outage_windows = []
        if self.conn_log_dir:
            try:
                from gap_detector import connection_outage_windows
                quote_outage_windows = connection_outage_windows(self.conn_log_dir, now.date(), "Quote")
            except Exception as exc:
                print(f"[BACKFILL][WARN] could not read connection log: {exc}", flush=True)

        symbol_state = {}
        gap_flagged = []
        confirmed_outage_count = 0

        # Three levels of batching now, all aimed at the same 199-symbol
        # loop:
        #  1. One connection reused for the whole loop instead of a
        #     fresh psycopg2.connect() per symbol.
        #  2. max_ts_batch() replaces 199 sequential single-symbol
        #     round-trips with ~2 round-trips total.
        #  3. read_ticks_batch() does the same for the full-tick-range
        #     read that used to happen per-symbol via _cached_ticks_df()
        #     for every symbol that already has cached data — on a live
        #     system that's most/all symbols, and each of those reads
        #     pulls back a symbol's WHOLE cached range (not just a
        #     scalar like max_ts), making this the heavier of the two
        #     per-symbol costs in practice. Confirmed via live logs:
        #     even after (2), Phase 1 was still crawling through
        #     "17/199, 20/199, 27/199..." one slow symbol at a time
        #     while the live queue overflowed — this was why.
        cache_check_conn = self.tick_writer.open_read_connection() if self.tick_writer is not None else None
        try:
            all_symbol_names = [inst["symbol"] for inst in symbols]
            last_ms_by_symbol = (
                self.tick_writer.max_ts_batch(all_symbol_names, conn=cache_check_conn)
                if self.tick_writer is not None else {}
            )

            # Every symbol with cached data covering [start_ms, last_ms]
            # needs its full tick range read for gap detection — fetch
            # ALL of them in one batched call instead of one per symbol.
            ranges_needed = {
                s: (start_ms, last_ms_by_symbol[s])
                for s in all_symbol_names
                if last_ms_by_symbol.get(s) is not None and last_ms_by_symbol[s] >= start_ms
            }
            cached_rows_by_symbol = (
                self.tick_writer.read_ticks_batch(ranges_needed, conn=cache_check_conn)
                if self.tick_writer is not None and ranges_needed else {}
            )

            for i, inst in enumerate(symbols, start=1):
                symbol  = inst["symbol"]
                last_ms = last_ms_by_symbol.get(symbol)

                cached_df       = pd.DataFrame()
                cached_until_ms = None
                phantom_range   = None   # (start_ms, end_ms) of the untrusted local range, for Phase 4

                if last_ms is not None and last_ms >= start_ms:
                    cached_df = self._ticks_rows_to_df(cached_rows_by_symbol.get(symbol, []))
                    gap_result = self.tick_gaps.find_cache_gap(
                        cached_df, last_ms, self.gap_threshold_secs, quote_outage_windows
                    )

                    if gap_result["gap_ms"] is not None:
                        gap_flagged.append(symbol)
                        phantom_local_ts = gap_result["phantom_local_ts"]
                        phantom_range = gap_result["phantom_range"]

                        if gap_result["confirmed_outage"]:
                            confirmed_outage_count += 1

                        # Trusted portion stays; the tail (after the gap)
                        # is dropped from what we treat as cached here so
                        # it gets re-fetched below — the row itself is
                        # NOT deleted from SQLite, upsert corrects it once
                        # the re-fetch lands.
                        cached_df = gap_result["trusted_df"]
                        cached_until_ms = gap_result["cached_until_ms"]
                    else:
                        cached_until_ms = last_ms

                fetch_start_ms = (cached_until_ms + 1) if cached_until_ms is not None else start_ms

                symbol_state[symbol] = {
                    "cached_df": cached_df,
                    "cached_until_ms": cached_until_ms,
                    "phantom_range": phantom_range,
                    "phantom_local_ts": phantom_local_ts if phantom_range else None,
                    "fetch_start": (
                        datetime.fromtimestamp(fetch_start_ms / 1000, tz=tz_kolkata)
                        if fetch_start_ms < now_ms else None
                    ),
                }

                self._progress("Checking local tick cache", i, len(symbols))
        finally:
            if cache_check_conn is not None:
                try:
                    cache_check_conn.close()
                except Exception as exc:
                    print(f"[BACKFILL][WARN] closing cache-check connection failed: {exc}", flush=True)

        if gap_flagged:
            preview = ", ".join(gap_flagged[:10])
            more = f" (+{len(gap_flagged) - 10} more)" if len(gap_flagged) - 10 > 0 else ""
            confirmed_note = (
                f" ({confirmed_outage_count} confirmed against connection log)"
                if quote_outage_windows else " (connection log not available to verify)"
            )
            print(
                f"[BACKFILL][WARN] {len(gap_flagged)} symbol(s) had a silent gap "
                f"(>{self.gap_threshold_secs}s within a session) in their cached "
                f"ticks{confirmed_note} — will re-fetch and correct: "
                f"{preview}{more}",
                flush=True,
            )

        fully_covered = [s for s, st in symbol_state.items() if st["fetch_start"] is None]
        if fully_covered:
            print(
                f"[BACKFILL] {len(fully_covered)}/{len(symbols)} symbol(s) fully "
                f"caught up in the local tick cache — skipping main db fetch "
                f"for them",
                flush=True,
            )

        # ── Phase 1h: local-history-candle-cache-only pass ──────────────
        # Same idea as Phase 1, but against history_candles.db
        # (HistoryCandleStore) instead of ticks.db — history-db candles
        # are idempotent (upserted, not appended) so this doesn't need
        # the same silent-gap verification tick caching does; a stale
        # partial range there just gets topped up by the next fetch,
        # never duplicated.
        history_state = {}
        for i, inst in enumerate(symbols, start=1):
            symbol   = inst["symbol"]
            last_hms = self.history_store.max_ts(symbol) if self.history_store is not None else None

            cached_hdf        = pd.DataFrame()
            cached_h_until_ms = None

            if last_hms is not None and last_hms >= start_ms:
                cached_hdf        = self._cached_history_df(symbol, start_ms, last_hms)
                cached_h_until_ms = last_hms

            h_fetch_start_ms = (cached_h_until_ms + 1) if cached_h_until_ms is not None else start_ms

            history_state[symbol] = {
                "cached_df": cached_hdf,
                "fetch_start": (
                    datetime.fromtimestamp(h_fetch_start_ms / 1000, tz=tz_kolkata)
                    if h_fetch_start_ms < now_ms else None
                ),
            }

            self._progress("Checking local history-candle cache", i, len(symbols))

        history_fully_covered = [s for s, st in history_state.items() if st["fetch_start"] is None]
        if history_fully_covered:
            print(
                f"[BACKFILL] {len(history_fully_covered)}/{len(symbols)} symbol(s) "
                f"fully caught up in the local history-candle cache — skipping "
                f"history db fetch for them",
                flush=True,
            )

        # ── Phase 2: ONE batched fetch for every symbol that still needs data ──
        fetch_starts = {
            s: st["fetch_start"] for s, st in symbol_state.items()
            if st["fetch_start"] is not None
        }
        batched_ticks = self._fetch_ticks_batch(fetch_starts, existing_tables)

        # ── Phase 2h: ONE batched history-db candle fetch ────────────────
        # Always run now (not just as a last-resort gap filler) — see
        # _aggregate_symbol_with_history() for how the result gets
        # priority over tick-aggregated candles.
        history_fetch_starts = {
            s: st["fetch_start"] for s, st in history_state.items()
            if st["fetch_start"] is not None
        }
        batched_history = self._fetch_history_candles_batch(
            history_fetch_starts, history_existing_tables
        )

        # ── Phase 3: per-symbol aggregation, history-priority (pure pandas) ──
        # Aggregate totals only — no per-symbol lines. A single progress
        # line updates in place; per-symbol tick/candle counts are summed
        # into totals and reported once, after every symbol is done.
        total_ticks = total_ticks_cached = total_ticks_fetched = 0
        total_hist  = total_hist_cached  = total_hist_fetched  = 0
        no_data_symbols = []
        total_phantom_rows = 0
        phantom_symbols = set()

        for i, inst in enumerate(symbols, start=1):
            symbol    = inst["symbol"]
            state     = symbol_state[symbol]
            fresh_df  = batched_ticks.get(symbol, pd.DataFrame())
            cached_df = state.get("cached_df", pd.DataFrame())

            if not cached_df.empty and not fresh_df.empty:
                ticks_df = (
                    pd.concat([cached_df, fresh_df], ignore_index=True)
                    .sort_values("timestamp")
                    .reset_index(drop=True)
                )
            elif not cached_df.empty:
                ticks_df = cached_df
            else:
                ticks_df = fresh_df

            state["ticks_df"] = ticks_df

            phantom_range    = state.get("phantom_range")
            phantom_local_ts = state.get("phantom_local_ts")
            if phantom_range and phantom_local_ts:
                if symbol in self._last_fetch_failed_symbols:
                    # The confirming re-fetch for this symbol never
                    # actually reached main db this run (chunk failed) —
                    # we have NO information about whether these
                    # timestamps are real or not, so we must NOT delete
                    # them. They stay in the local table exactly as
                    # before; cached_df already excludes them from
                    # ticks_df above, so nothing untrusted gets used
                    # either. Phase 1 will re-detect the same suspicious
                    # gap next run and try the confirming fetch again —
                    # this only resolves once that fetch actually
                    # succeeds.
                    pass
                elif not fresh_df.empty:
                    in_range = fresh_df[
                        (fresh_df["timestamp"] >= phantom_range[0])
                        & (fresh_df["timestamp"] <= phantom_range[1])
                    ]
                    pg_confirmed_ts = set(int(t) for t in in_range["timestamp"])
                    phantom_ts = phantom_local_ts - pg_confirmed_ts
                    if phantom_ts:
                        total_phantom_rows += len(phantom_ts)
                        phantom_symbols.add(symbol)
                        # wait=False — fire-and-forget, same reasoning as
                        # the other tick_writer calls in this loop.
                        self.tick_writer.delete_timestamps(symbol, phantom_ts, kind="quote", wait=False)
                else:
                    # fresh_df IS genuinely empty here — the fetch
                    # succeeded (symbol not in failed set) and main db
                    # confirmed zero rows in phantom_range, so every
                    # locally-cached phantom timestamp really is fake.
                    # Safe to delete all of them.
                    phantom_ts = phantom_local_ts
                    total_phantom_rows += len(phantom_ts)
                    phantom_symbols.add(symbol)
                    self.tick_writer.delete_timestamps(symbol, phantom_ts, kind="quote", wait=False)

            if cached_df.empty and fresh_df.empty and state["fetch_start"] is not None:
                no_data_symbols.append(symbol)
            elif not ticks_df.empty:
                total_ticks         += len(ticks_df)
                total_ticks_cached  += len(cached_df)
                total_ticks_fetched += len(fresh_df)

            hstate     = history_state[symbol]
            fresh_hdf  = batched_history.get(symbol, pd.DataFrame())
            cached_hdf = hstate.get("cached_df", pd.DataFrame())

            if not cached_hdf.empty and not fresh_hdf.empty:
                history_df = (
                    pd.concat([cached_hdf, fresh_hdf], ignore_index=True)
                    .sort_values("timestamp")
                    .reset_index(drop=True)
                )
            elif not cached_hdf.empty:
                history_df = cached_hdf
            else:
                history_df = fresh_hdf

            if not history_df.empty:
                total_hist         += len(history_df)
                total_hist_cached  += len(cached_hdf)
                total_hist_fetched += len(fresh_hdf)

            per_tf = self._aggregate_symbol_with_history(ticks_df, history_df, start_ts, now)
            state["per_tf"] = per_tf

            self._progress("Building candles", i, len(symbols))

        if total_ticks:
            print(
                f"[BACKFILL] Tick data complete | {total_ticks} tick(s) total "
                f"({total_ticks_cached} from local cache + {total_ticks_fetched} fetched)",
                flush=True,
            )
        if total_hist:
            print(
                f"[BACKFILL] History-db candle data complete | {total_hist} candle(s) total "
                f"({total_hist_cached} from local cache + {total_hist_fetched} fetched)",
                flush=True,
            )
        if no_data_symbols:
            preview = ", ".join(no_data_symbols[:10])
            more = f" (+{len(no_data_symbols) - 10} more)" if len(no_data_symbols) - 10 > 0 else ""
            print(
                f"[BACKFILL][WARN] {len(no_data_symbols)}/{len(symbols)} symbol(s) had no "
                f"tick data in main db or local cache: {preview}{more}",
                flush=True,
            )
        if total_phantom_rows:
            print(
                f"[BACKFILL] Phantom-row check: {total_phantom_rows} row(s) removed "
                f"across {len(phantom_symbols)} symbol(s) (locally cached but "
                f"confirmed absent from main db)",
                flush=True,
            )

        # ── Phase 4: per-symbol save to ohlc + missing-bucket reporting ──
        zero_candle_counts = {}  # tf_str -> count of symbols with 0 candles
        for i, inst in enumerate(symbols, start=1):
            symbol = inst["symbol"]
            zero_tfs = self._finalize_symbol(symbol, symbol_state[symbol]["per_tf"])
            for tf_str in zero_tfs:
                zero_candle_counts[tf_str] = zero_candle_counts.get(tf_str, 0) + 1
            self._progress("Saving candles", i, len(symbols))

        total_missing = sum(self.missing_counts.values())
        if total_missing:
            print(
                f"[BACKFILL][WARN] {total_missing} candle(s) still missing across "
                f"{len(self.missing_counts)} symbol/TF pair(s) (not in local tick "
                f"cache, local history cache, main db, or history db)",
                flush=True,
            )
        for tf_str, count in zero_candle_counts.items():
            print(
                f"[BACKFILL][WARN] {tf_str}: {count}/{len(symbols)} symbol(s) had 0 candles aggregated",
                flush=True,
            )

        # ── Phase 5: tick-buffer seeding — reuse Phase 2 fetch, batch the rest ──
        window_secs = getattr(self.ohlc, "tick_ram_window_secs", 9 * 60)
        seed_stats  = {"seeded": 0, "empty": 0, "ticks": 0}
        need_seed_batch = []

        for inst in symbols:
            symbol   = inst["symbol"]
            ticks_df = symbol_state[symbol].get("ticks_df", pd.DataFrame())
            if ticks_df is not None and not ticks_df.empty:
                n = self._seed_recent_ticks(symbol, ticks_df)
                if n:
                    seed_stats["seeded"] += 1
                    seed_stats["ticks"]  += n
                else:
                    seed_stats["empty"] += 1
            else:
                need_seed_batch.append(symbol)

        if need_seed_batch:
            batch_stats = self._seed_recent_ticks_batch(need_seed_batch, existing_tables)
            seed_stats["seeded"] += batch_stats["seeded"]
            seed_stats["empty"]  += batch_stats["empty"]
            seed_stats["ticks"]  += batch_stats["ticks"]

        print(
            f"[BACKFILL] Tick RAM buffers seeded: {seed_stats['seeded']}/{len(symbols)} "
            f"symbol(s), {seed_stats['ticks']} total tick(s), "
            f"{seed_stats['empty']} symbol(s) had none ({window_secs // 60:.0f} min window)",
            flush=True,
        )

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

        print("[BACKFILL] Completed", flush=True)

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
        depth_state = {}
        fetch_starts = {}
        gap_flagged = []
        confirmed_outage_count = 0

        # Same fix as Phase 1's quote cache-check loop above: reuse one
        # connection, batch max_ts lookups via max_ts_batch(), AND batch
        # the full-range cache reads via read_ticks_batch() instead of
        # one round-trip per symbol for each.
        depth_cache_check_conn = self.tick_writer.open_read_connection() if self.tick_writer is not None else None
        try:
            all_symbol_names = [inst["symbol"] for inst in symbols]
            last_ms_by_symbol = (
                self.tick_writer.max_ts_batch(all_symbol_names, kind="depth", conn=depth_cache_check_conn)
                if self.tick_writer is not None else {}
            )

            ranges_needed = {
                s: (start_ms, last_ms_by_symbol[s])
                for s in all_symbol_names
                if last_ms_by_symbol.get(s) is not None and last_ms_by_symbol[s] >= start_ms
            }
            cached_rows_by_symbol = (
                self.tick_writer.read_ticks_batch(ranges_needed, kind="depth", conn=depth_cache_check_conn)
                if self.tick_writer is not None and ranges_needed else {}
            )

            for i, inst in enumerate(symbols, start=1):
                symbol  = inst["symbol"]
                last_ms = last_ms_by_symbol.get(symbol)

                cached_until_ms = None
                phantom_range   = None
                phantom_local_ts = None

                if last_ms is not None and last_ms >= start_ms:
                    cached_df = self._depth_rows_to_df(cached_rows_by_symbol.get(symbol, []))
                    gap_result = self.tick_gaps.find_cache_gap(
                        cached_df, last_ms, self.gap_threshold_secs, depth_outage_windows
                    )

                    if gap_result["gap_ms"] is not None:
                        gap_flagged.append(symbol)
                        phantom_local_ts = gap_result["phantom_local_ts"]
                        phantom_range = gap_result["phantom_range"]

                        if gap_result["confirmed_outage"]:
                            confirmed_outage_count += 1

                        cached_until_ms = gap_result["gap_ms"]
                    else:
                        cached_until_ms = last_ms

                fetch_start_ms = (cached_until_ms + 1) if cached_until_ms is not None else start_ms
                depth_state[symbol] = {
                    "phantom_range": phantom_range,
                    "phantom_local_ts": phantom_local_ts,
                }
                if fetch_start_ms < now_ms:
                    fetch_starts[symbol] = datetime.fromtimestamp(fetch_start_ms / 1000, tz=tz_kolkata)

                self._progress("Checking local depth cache", i, len(symbols))
        finally:
            if depth_cache_check_conn is not None:
                try:
                    depth_cache_check_conn.close()
                except Exception as exc:
                    print(f"[BACKFILL][WARN] closing depth cache-check connection failed: {exc}", flush=True)

        if gap_flagged:
            preview = ", ".join(gap_flagged[:10])
            more = f" (+{len(gap_flagged) - 10} more)" if len(gap_flagged) - 10 > 0 else ""
            confirmed_note = (
                f" ({confirmed_outage_count} confirmed against connection log)"
                if depth_outage_windows else " (connection log not available to verify)"
            )
            print(
                f"[BACKFILL][WARN] {len(gap_flagged)} symbol(s) had a silent gap "
                f"(>{self.gap_threshold_secs}s within a session) in their cached "
                f"depth data{confirmed_note} — will re-fetch and correct: "
                f"{preview}{more}",
                flush=True,
            )

        if not fetch_starts:
            print("[BACKFILL] Depth backfill: nothing to fetch — all symbols fully cached", flush=True)
            return

        # ── Phase D2: batched fetch from Postgres depth_<symbol> ──
        # (also writes freshly fetched rows into local depth cache via
        # enqueue_backfill_rows — see _fetch_depth_batch's docstring)
        depth_existing_tables = self._load_existing_tables(self.conn, prefix="depth")
        fetched = self._fetch_depth_batch(fetch_starts, depth_existing_tables)

        # ── Phase D3: phantom-row check ──
        total_phantom_rows = 0
        phantom_symbols = set()
        for symbol, state in depth_state.items():
            phantom_range    = state.get("phantom_range")
            phantom_local_ts = state.get("phantom_local_ts")
            if not (phantom_range and phantom_local_ts):
                continue

            if symbol in self._last_fetch_failed_symbols:
                # Same reasoning as the quote-phase phantom check above:
                # the confirming re-fetch for this symbol never actually
                # reached main db this run, so we have no basis to
                # delete anything — leave it for next run to retry.
                continue

            fetched_ts = fetched.get(symbol, set())
            pg_confirmed_ts = {
                t for t in fetched_ts
                if phantom_range[0] <= t <= phantom_range[1]
            }

            phantom_ts = phantom_local_ts - pg_confirmed_ts
            if phantom_ts:
                total_phantom_rows += len(phantom_ts)
                phantom_symbols.add(symbol)
                self.tick_writer.delete_timestamps(symbol, phantom_ts, kind="depth", wait=False)

        if total_phantom_rows:
            print(
                f"[BACKFILL] Depth phantom-row check: {total_phantom_rows} row(s) "
                f"removed across {len(phantom_symbols)} symbol(s) (locally cached "
                f"but confirmed absent from main db)",
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

        existing_tables = self._load_existing_tables(self.conn, prefix=mode)
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

    def _depth_rows_to_df(self, rows: list) -> pd.DataFrame:
        """Same as _ticks_rows_to_df but for depth rows (timestamp/ltp
        only — gap detection doesn't need the flat level columns)."""
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([{"timestamp": r["timestamp"], "ltp": r.get("ltp")} for r in rows])
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

    def _cached_depth_df(self, symbol: str, start_ms: int, end_ms: int, conn=None) -> pd.DataFrame:
        """
        Same idea as _cached_ticks_df but for the local depth_<symbol>
        SQLite cache — used by the Depth backfill pass purely for gap
        detection, which only needs timestamp/ist_ts. The full flat
        level columns come back from read_ticks() too but aren't
        needed here, so they're dropped immediately rather than kept
        around in this DataFrame.

        conn: optional pre-opened connection, passed through to
        tick_writer.read_ticks() — see Phase D1's cache-check loop.
        Prefer read_ticks_batch() + _depth_rows_to_df() for multiple
        symbols at once.
        """
        if self.tick_writer is None:
            return pd.DataFrame()

        rows = self.tick_writer.read_ticks(symbol, start_ms=start_ms, end_ms=end_ms, kind="depth", conn=conn)
        return self._depth_rows_to_df(rows)

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

        Only handles the reuse case now (ticks_df already fetched by
        Phase 2's batched query) — zero extra DB round trips. Symbols
        with no reusable ticks_df go through _seed_recent_ticks_batch()
        instead (called once, batched, from run()'s Phase 6).

        Returns the number of ticks loaded (0 if none) — run() aggregates
        this across every symbol into a single summary print instead of
        one line per symbol.
        """
        window_secs = getattr(self.ohlc, "tick_ram_window_secs", 9 * 60)

        if ticks_df is None or ticks_df.empty:
            return 0

        cutoff = ticks_df["ist_ts"].max() - timedelta(seconds=window_secs)
        window_df = ticks_df[ticks_df["ist_ts"] >= cutoff]
        return self._load_ticks_into_ram(symbol, window_df)

    def _seed_recent_ticks_batch(self, symbols: list, existing_tables: set) -> dict:
        """
        Batched fallback path for symbols where no reusable ticks_df was
        available from Phase 2 (i.e. the local tick cache already covered
        the full window, so nothing needed fetching for them). Each symbol still
        gets its own self-anchored subquery (own MAX(timestamp) - window,
        computed server-side), combined into chunked UNION ALL queries —
        replacing what used to be one query per symbol.

        Returns {"seeded": n, "empty": n, "ticks": n} — run() folds this
        into the single Phase 6 summary print instead of per-chunk/
        per-symbol output.
        """
        stats = {"seeded": 0, "empty": 0, "ticks": 0}
        if self.conn is None or not symbols:
            return stats

        window_secs = getattr(self.ohlc, "tick_ram_window_secs", 9 * 60)
        window_ms   = window_secs * 1000

        seeded           = set()
        skipped_no_table = []

        chunk_size = self.batch_size if self.batch_size > 0 else max(len(symbols), 1)
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]

            clauses = []
            params  = []
            for symbol in chunk:
                safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_")
                table = f"quote_{safe_sym}".lower()
                if table not in existing_tables:
                    skipped_no_table.append(symbol)
                    continue
                clauses.append(
                    f"SELECT %s AS symbol, timestamp, ltp, "
                    f"COALESCE(last_quantity, 0) AS qty FROM {table} "
                    f"WHERE timestamp >= (SELECT MAX(timestamp) FROM {table}) - %s "
                    f"AND ltp IS NOT NULL"
                )
                params.extend([symbol, window_ms])

            if not clauses:
                continue

            query = " UNION ALL ".join(clauses) + " ORDER BY symbol, timestamp"

            try:
                cur = self.conn.cursor()
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
            except Exception as exc:
                print(
                    f"[BACKFILL][WARN] batched tick-buffer seed failed for a "
                    f"chunk of {len(chunk)} symbol(s): {exc}",
                    flush=True,
                )
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                continue

            if not rows:
                continue

            df = pd.DataFrame(rows, columns=["symbol", "timestamp", "ltp", "qty"])
            df["ist_ts"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)

            for symbol, group in df.groupby("symbol"):
                g = group.drop(columns=["symbol"]).reset_index(drop=True)
                n = self._load_ticks_into_ram(symbol, g)
                seeded.add(symbol)
                if n:
                    stats["seeded"] += 1
                    stats["ticks"]  += n
                else:
                    stats["empty"] += 1

        stats["empty"] += len(skipped_no_table)
        stats["empty"] += sum(1 for s in symbols if s not in seeded and s not in skipped_no_table)

        return stats

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

        existing_tables = self._load_existing_quote_tables(self.conn) if self.conn else set()
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

    def _fetch_ticks_batch(self, fetch_starts: dict, existing_tables: set) -> dict:
        """
        fetch_starts: {symbol: start_ts} — symbols needing a main-db tick
        fetch, each with its own start time (already narrowed to just
        what's missing beyond the local tick cache, in run()'s Phase 1).

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
                    f"SELECT %s AS symbol, timestamp, ltp, "
                    f"COALESCE(last_quantity, 0) AS qty FROM {table} "
                    f"WHERE timestamp >= %s AND ltp IS NOT NULL"
                )
                params.extend([symbol, start_ms])

            if not clauses:
                continue

            query = " UNION ALL ".join(clauses) + " ORDER BY symbol, timestamp"

            chunk_num = i // chunk_size + 1
            total_chunks = (len(items) + chunk_size - 1) // chunk_size
            print(
                f"[BACKFILL] Fetching main-db ticks: chunk {chunk_num}/{total_chunks} "
                f"({len(clauses)} symbol(s))...",
                flush=True,
            )

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

            print(f"[BACKFILL] Chunk {chunk_num}/{total_chunks}: {len(rows)} row(s) received", flush=True)

            if not rows:
                continue

            df = pd.DataFrame(rows, columns=["symbol", "timestamp", "ltp", "qty"])
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
                # guarantee docstring for why that matters.
                if self.tick_writer is not None:
                    rows = [
                        {"timestamp": int(row.timestamp), "ltp": row.ltp, "qty": row.qty}
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

        print(
            f"[BACKFILL] Batched main-db fetch: {len(out)}/{len(fetch_starts)} "
            f"symbol(s) returned data",
            flush=True,
        )
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

    def _fetch_depth_batch(self, fetch_starts: dict, existing_tables: set) -> dict:
        """
        fetch_starts: {symbol: start_ts} — same shape as
        _fetch_ticks_batch's parameter, just for depth_<symbol> tables.

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
            print(
                f"[BACKFILL] Fetching main-db depth: chunk {chunk_num}/{total_chunks} "
                f"({len(clauses)} symbol(s))...",
                flush=True,
            )

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

            print(f"[BACKFILL] Depth chunk {chunk_num}/{total_chunks}: {len(rows)} row(s) received", flush=True)

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

        print(
            f"[BACKFILL] Batched main-db depth fetch: {len(out)}/{len(fetch_starts)} "
            f"symbol(s) returned data",
            flush=True,
        )
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
            print(
                f"[BACKFILL] Fetching history-db candles: chunk {chunk_num}/{total_chunks} "
                f"({len(clauses)} symbol(s))...",
                flush=True,
            )

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

            print(
                f"[BACKFILL] History chunk {chunk_num}/{total_chunks}: {len(rows)} row(s) received",
                flush=True,
            )

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

        print(
            f"[BACKFILL] Batched history-db fetch: {len(out)}/{len(fetch_starts)} "
            f"symbol(s) returned data",
            flush=True,
        )
        return out

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
