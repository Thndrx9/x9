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
from market_time import tz_kolkata, MARKET_OPEN, MARKET_CLOSE, is_trading_day, now_kolkata
from gap_detector import (
    GapDetector,
    compute_bucket,
    compute_bucket_vectorized,
    is_at_or_after_market_open_vectorized,
    is_market_hours_weekday_vectorized,
)

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
    See _fetch_history_candles_batch()'s docstring for how this
    constrains tier 2.

    Batching (run() overview):
    Per-symbol DB round trips used to dominate wall-clock time for large
    symbol universes — every symbol paid its own network round trip(s)
    to a remote AWS RDS instance, strictly sequentially. run() now does
    this in distinct phases instead of one big per-symbol loop:
        Phase 0: list which quote_{SYMBOL} tables actually exist (1 query)
        Phase 1: local-parquet-only pass — pure disk I/O, no DB at all
        Phase 2: ONE batched (UNION ALL, chunked) tick fetch for every
                 symbol that still needs data beyond local parquet
        Phase 3: per-symbol aggregation (pure pandas, no DB) — also
                 collects each symbol's still-missing buckets at
                 history_native_tf
        Phase 4: ONE batched history-db fetch across every symbol's
                 native-tf gaps at once
        Phase 5: per-symbol merge of the history fill + derive-from-1m
                 tier + save to ohlc (pure pandas/RAM, no DB)
        Phase 6: tick-buffer seeding — reuses Phase 2's fetch where
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
        self.min_candles = int(os.getenv("MIN_CANDLES", "15"))
        self.timeframes  = self._load_timeframes()   # [(tf_str, tf_seconds), ...]
        self.conn        = self._connect()
        self.gaps        = GapDetector()
        self.batch_size  = int(os.getenv("BACKFILL_BATCH_SIZE", "0"))  # 0 = no chunking, all symbols in one query

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
                # stops after local-parquet checks" symptom this fixes.
                options         = f"-c statement_timeout={timeout_sec * 1000}",
            )
            conn.autocommit = True
            print("[BACKFILL] Connected to PostgreSQL (main db)", flush=True)
            return conn
        except Exception as exc:
            print(f"[BACKFILL][ERROR] PostgreSQL connection failed: {exc}", flush=True)
            return None

    def _get_history_conn(self):
        """
        Lazily connects to the history DB (PG_HDBNAME) — only opened the
        first time a gap is actually found, so symbols with clean primary
        data never pay for a second connection.
        """
        if self.conn_history is not None:
            return self.conn_history
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

    def _load_existing_quote_tables(self, conn) -> set:
        """
        One query: which quote_% tables actually exist in this db.
        Needed BEFORE building any UNION ALL batch — Postgres fails the
        entire batched query if even one clause references a table that
        doesn't exist, so symbols without a table yet get filtered out
        up front instead of blowing up the whole chunk.
        """
        if conn is None:
            return set()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname='public' AND tablename LIKE 'quote_%'"
            )
            tables = {row[0] for row in cur.fetchall()}
            cur.close()
            return tables
        except Exception as exc:
            print(f"[BACKFILL][ERROR] failed to list quote_ tables: {exc}", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
            return set()

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

        print(
            f"[BACKFILL] Starting | symbols={len(symbols)} "
            f"| TFs={tf_names} | from={start_ts.strftime('%Y-%m-%d %H:%M %Z')}",
            flush=True,
        )

        # ── Phase 0: which quote_ tables exist in the main db ───────────
        existing_tables = self._load_existing_quote_tables(self.conn)

        # ── Phase 1: local-parquet-only pass — pure disk I/O, no DB ─────
        symbol_state = {}
        for inst in symbols:
            symbol = inst["symbol"]
            local_by_tf     = {}
            earliest_needed = None

            for tf_str, tf_seconds in self.timeframes:
                local_df = self.gaps.read_local_parquet(self.ohlc.base_dir, symbol, tf_str)
                expected = self.gaps.expected_buckets(start_ts, now, tf_seconds)
                missing  = self.gaps.find_missing(local_df, expected)
                local_by_tf[tf_str] = (local_df, missing)

                if not local_df.empty:
                    print(
                        f"[BACKFILL] {symbol} {tf_str}: {len(local_df)} candle(s) "
                        f"already on disk (local parquet)",
                        flush=True,
                    )

                if missing and (earliest_needed is None or missing[0] < earliest_needed):
                    earliest_needed = missing[0]

            symbol_state[symbol] = {
                "local_by_tf": local_by_tf,
                "fetch_start": max(start_ts, earliest_needed) if earliest_needed else None,
            }

        fully_covered = [s for s, st in symbol_state.items() if st["fetch_start"] is None]
        if fully_covered:
            print(
                f"[BACKFILL] {len(fully_covered)}/{len(symbols)} symbol(s) fully "
                f"covered by local parquet — skipping main db fetch for them",
                flush=True,
            )

        # ── Phase 2: ONE batched fetch for every symbol that still needs data ──
        fetch_starts = {
            s: st["fetch_start"] for s, st in symbol_state.items()
            if st["fetch_start"] is not None
        }
        batched_ticks = self._fetch_ticks_batch(fetch_starts, existing_tables)

        # ── Phase 3: per-symbol aggregation (pure pandas) + native-tf gaps ──
        history_requests = {}   # symbol -> missing buckets at history_native_tf

        for inst in symbols:
            symbol   = inst["symbol"]
            state    = symbol_state[symbol]
            ticks_df = batched_ticks.get(symbol, pd.DataFrame())
            state["ticks_df"] = ticks_df

            if fetch_starts.get(symbol) is not None and ticks_df.empty:
                print(f"[BACKFILL][WARN] {symbol}: no tick data in main db", flush=True)
            elif not ticks_df.empty:
                print(f"[BACKFILL] {symbol}: {len(ticks_df)} ticks fetched", flush=True)

            per_tf, native_missing = self._aggregate_symbol(
                state["local_by_tf"], ticks_df, start_ts, now
            )
            state["per_tf"] = per_tf

            if native_missing:
                history_requests[symbol] = native_missing

        # ── Phase 4: ONE batched history-db fetch across all symbols' gaps ──
        filled_from_history = {}
        if history_requests:
            history_conn = self._get_history_conn()
            history_tables = self._load_existing_quote_tables(history_conn)
            filled_from_history = self._fetch_history_candles_batch(
                history_requests, history_tables
            )

        # ── Phase 5: merge history fill + derive-from-1m + save (per symbol) ──
        for inst in symbols:
            symbol = inst["symbol"]
            self._finalize_symbol(
                symbol,
                symbol_state[symbol]["per_tf"],
                filled_from_history.get(symbol),
            )

        # ── Phase 6: tick-buffer seeding — reuse Phase 2 fetch, batch the rest ──
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
    # Per-symbol aggregation (Phase 3) — pure pandas, no DB
    # ─────────────────────────────────────────────

    def _aggregate_symbol(self, local_by_tf, ticks_df, start_ts, now):
        """
        For every configured TF: aggregate ticks_df into candles, merge
        with local parquet, compute what's still missing. Deliberately
        stops there — history-db fill and the derive-from-1m tier are
        deferred to _finalize_symbol() so the history-db fetch for every
        symbol's native-tf gaps can be batched across the whole symbol
        set first (see run()'s Phase 4), instead of one query per symbol.

        Returns (per_tf, native_missing):
            per_tf: {tf_str: {"candles", "missing", "expected"}}
            native_missing: the missing-bucket list for
                            self.history_native_tf (empty list if that TF
                            isn't even configured, or if there's nothing
                            missing there)
        """
        per_tf = {}
        native_missing = []

        for tf_str, tf_seconds in self.timeframes:
            local_df, _ = local_by_tf[tf_str]

            fresh   = self._aggregate(ticks_df, tf_seconds, now)
            candles = self.gaps.merge_candles(local_df, fresh)

            expected = self.gaps.expected_buckets(start_ts, now, tf_seconds)
            missing  = self.gaps.find_missing(candles, expected)

            per_tf[tf_str] = {"candles": candles, "missing": missing, "expected": expected}

            if tf_str == self.history_native_tf:
                native_missing = missing

        return per_tf, native_missing

    # ─────────────────────────────────────────────
    # Per-symbol finalization (Phase 5) — merge history fill,
    # derive-from-1m, save to ohlc. No DB calls in here at all — the
    # history-db data was already fetched in one batch (Phase 4).
    # ─────────────────────────────────────────────

    def _finalize_symbol(self, symbol, per_tf, history_filled_df):
        base_1m_candles = None

        for tf_str, tf_seconds in self.timeframes:
            state    = per_tf[tf_str]
            candles  = state["candles"]
            missing  = state["missing"]
            expected = state["expected"]

            # ── Step 1: history db direct match (native TF only) ───────
            # history_filled_df, when present, is already this symbol's
            # own rows only (each UNION ALL clause in
            # _fetch_history_candles_batch carries its own symbol +
            # WHERE timestamp = ANY(its own missing list)) — no further
            # filtering needed here.
            if (
                missing
                and tf_str == self.history_native_tf
                and history_filled_df is not None
                and not history_filled_df.empty
            ):
                candles = self.gaps.merge_candles(candles, history_filled_df)
                print(
                    f"[BACKFILL] {symbol} {tf_str}: "
                    f"{len(history_filled_df)}/{len(missing)} gap candle(s) "
                    f"filled from history db (direct match)",
                    flush=True,
                )
                missing = self.gaps.find_missing(candles, expected)

            # ── Step 2: derive from finalized 1m candles ────────────────
            # Only applies to TFs above 1m, and only once 1m itself has
            # been finalized (timeframes are processed smallest-first).
            if missing and tf_seconds > 60 and base_1m_candles is not None:
                derived = self.gaps.derive_from_1m(base_1m_candles, missing, tf_seconds)
                if not derived.empty:
                    candles = self.gaps.merge_candles(candles, derived)
                    print(
                        f"[BACKFILL] {symbol} {tf_str}: "
                        f"{len(derived)}/{len(missing)} gap candle(s) "
                        f"derived from 1m data",
                        flush=True,
                    )
                    missing = self.gaps.find_missing(candles, expected)

            if missing:
                self.missing_counts[(symbol, tf_str)] = len(missing)
                print(
                    f"[BACKFILL][WARN] {symbol} {tf_str}: "
                    f"{len(missing)} candle(s) still missing "
                    f"(not in local parquet, main db, history db, or derivable from 1m)",
                    flush=True,
                )

            if tf_seconds == 60:
                base_1m_candles = candles.copy() if not candles.empty else pd.DataFrame(
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )

            if candles.empty:
                print(f"[BACKFILL][WARN] {symbol} {tf_str}: 0 candles aggregated", flush=True)
                continue

            # itertuples() instead of iterrows() — same one-row-at-a-time
            # save_candle() calls (its per-symbol upsert/sort logic in
            # ohlc.py is unchanged), just avoids the per-row pandas Series
            # construction that iterrows() does, which is pure overhead here
            # since we only read scalar fields off each row.
            for row in candles.itertuples(index=False):
                self.ohlc.save_candle(symbol, tf_str, {
                    "timestamp": row.timestamp,
                    "open":      float(row.open),
                    "high":      float(row.high),
                    "low":       float(row.low),
                    "close":     float(row.close),
                    "volume":    float(row.volume),
                })

            print(
                f"[BACKFILL] {symbol} {tf_str}: {len(candles)} closed candles loaded",
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
        available from Phase 2 (i.e. local parquet already covered every
        candle gap, so nothing was fetched for them). Each symbol still
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
        what's missing beyond local parquet, in run()'s Phase 1).

        Returns {symbol: DataFrame} — same columns/session-filtering as
        the old per-symbol _fetch_ticks() used to return — but issued as
        one UNION ALL query covering every symbol by default
        (BACKFILL_BATCH_SIZE=0), instead of one round trip per symbol.
        This is the main lever: ~200 symbols → 1 query instead of ~200.
        """
        out = {}
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
                cur = self.conn.cursor()
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
            except Exception as exc:
                batch_symbols = [s for s, _ in chunk]
                print(
                    f"[BACKFILL][ERROR] batched tick fetch failed for a chunk of "
                    f"{len(batch_symbols)} symbol(s) (starting {batch_symbols[0]}): {exc}",
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
    # Aggregate ticks → OHLC for one TF
    # ─────────────────────────────────────────────

    def _aggregate(
        self,
        df:         pd.DataFrame,
        tf_seconds: int,
        now:        datetime,
    ) -> pd.DataFrame:
        """
        Group ticks into OHLC candles using market-open-aligned buckets.
        Excludes the currently-forming candle (bucket == current_bucket).
        """
        df = df.copy()

        if df.empty or "ist_ts" not in df.columns:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )

        # Vectorized — was previously a per-row .apply(compute_bucket), which
        # is a pure-Python loop over every tick and dominated Phase 3's wall
        # time (measured ~2.5s per 130k-tick symbol per timeframe; with 199
        # symbols x 2 TFs that adds up to several minutes). This does the
        # same bucket math as numpy array ops instead of a scalar function
        # call per row. See compute_bucket_vectorized()'s docstring in
        # gap_detector.py for the equivalence guarantee with compute_bucket().
        df["bucket"] = compute_bucket_vectorized(df["ist_ts"], tf_seconds)

        # Drop any tick that landed before market open (bucket would be 9:15:00
        # even for pre-market ticks — filter them by comparing raw ist_ts).
        # Vectorized (int64 seconds-of-day) instead of .dt.time — see
        # is_at_or_after_market_open_vectorized()'s docstring in
        # gap_detector.py; this was the dominant cost (~86%) of _aggregate().
        df = df[is_at_or_after_market_open_vectorized(df["ist_ts"])]

        # Exclude the currently-forming (incomplete) candle
        current_bucket = compute_bucket(now, tf_seconds)
        df = df[df["bucket"] < current_bucket]

        if df.empty:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )

        grouped = (
            df.groupby("bucket", sort=True)
            .agg(
                open   = ("ltp", "first"),
                high   = ("ltp", "max"),
                low    = ("ltp", "min"),
                close  = ("ltp", "last"),
                volume = ("qty", "sum"),
            )
            .reset_index()
            .rename(columns={"bucket": "timestamp"})
        )

        return grouped.reset_index(drop=True)

    # ─────────────────────────────────────────────
    # Fetch missing candles from the history DB (fallback) — BATCHED
    # ─────────────────────────────────────────────

    def _fetch_history_candles_batch(self, requests: dict, existing_tables: set) -> dict:
        """
        market_history DB — confirmed against x9_data_fetcher's own
        BackfillManager/pg_writer.py: rows here are pre-built candles
        fetched from the broker's REST history API, ONE ROW PER CANDLE
        AT self.history_native_tf granularity (default "1m") — typed
        open/high/low/close/volume columns (same schema as the main db),
        ltp/ltt left NULL since the history API never returns them, and
        NO interval column at all (pg_writer's typed schema has none —
        there was never anything to tag "1m" vs "5m" with).

        requests: {symbol: [missing_bucket_timestamps]} — already only
        ever populated for tf_str == self.history_native_tf (see run()'s
        Phase 3 / _aggregate_symbol) — coarser TFs are covered instead
        by _finalize_symbol's separate "derive from finalized 1m" tier.

        Returns {symbol: DataFrame}, each already containing ONLY that
        symbol's own requested rows (each UNION ALL clause carries its
        own WHERE timestamp = ANY(its own missing list)) — no further
        per-symbol filtering needed by the caller.
        """
        out = {}
        conn = self._get_history_conn()
        if conn is None or not requests:
            return out

        items = list(requests.items())
        skipped_no_table = []

        chunk_size = self.batch_size if self.batch_size > 0 else max(len(items), 1)
        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]

            clauses = []
            params  = []
            for symbol, missing_buckets in chunk:
                if not missing_buckets:
                    continue
                safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_")
                table = f"quote_{safe_sym}".lower()
                if table not in existing_tables:
                    skipped_no_table.append(symbol)
                    continue
                ms_list = [int(b.timestamp() * 1000) for b in missing_buckets]
                clauses.append(
                    f"SELECT %s AS symbol, timestamp, open, high, low, close, "
                    f"COALESCE(volume, 0) AS volume FROM {table} "
                    f"WHERE timestamp = ANY(%s) AND open IS NOT NULL"
                )
                params.extend([symbol, ms_list])

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
                cur = conn.cursor()
                cur.execute(query, params)
                rows = cur.fetchall()
                cur.close()
            except Exception as exc:
                print(
                    f"[BACKFILL][WARN] batched history fetch failed for a chunk "
                    f"of {len(chunk)} symbol(s): {exc}",
                    flush=True,
                )
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue

            if not rows:
                continue

            df = pd.DataFrame(
                rows, columns=["symbol", "timestamp", "open", "high", "low", "close", "volume"]
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)

            for symbol, group in df.groupby("symbol"):
                out[symbol] = group.drop(columns=["symbol"]).reset_index(drop=True)

        if skipped_no_table:
            preview = ", ".join(skipped_no_table[:10])
            more = f" (+{len(skipped_no_table) - 10} more)" if len(skipped_no_table) > 10 else ""
            print(
                f"[BACKFILL][WARN] {len(skipped_no_table)} symbol(s) skipped in "
                f"history-db batch — no quote_ table found: {preview}{more}",
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

                if count < self.min_candles:
                    print(
                        f"[BACKFILL][WARN] {symbol} {tf_str}: "
                        f"{count}/{self.min_candles} candles — insufficient",
                        flush=True,
                    )
                    all_ok = False
                elif missing > 0:
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