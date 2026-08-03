# backfill_manager.py

import os
import re
import math
import psycopg2
import pandas as pd
from datetime import datetime, timedelta, time as dtime
from dotenv import load_dotenv
from market_time import tz_kolkata, MARKET_OPEN, MARKET_CLOSE, is_trading_day
from gap_detector import GapDetector, compute_bucket

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
            2. Fill remaining gaps via direct interval match in history db
            3. Fill remaining gaps by deriving from finalized 1m candles
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

    Schema (history db, PG_HDBNAME) — UNCONFIRMED, pending clarification:
        pg_writer.py never shows PG_HDBNAME holding a pre-aggregated,
        interval-tagged quote_{SYMBOL} table. The only JSONB-schema table
        it defines is 'daily_{SYMBOL}' (one row per symbol per day, EOD
        candle). _fetch_history_candles() below still queries the OLD
        (wrong) shape and will simply fail safely — caught, logged as a
        WARN, treated as "no history fallback available" — until this is
        confirmed and rewritten to match whatever PG_HDBNAME actually holds.
    """

    def __init__(self, ohlc):
        self.ohlc        = ohlc
        self.min_candles = int(os.getenv("MIN_CANDLES", "15"))
        self.timeframes  = self._load_timeframes()   # [(tf_str, tf_seconds), ...]
        self.conn        = self._connect()
        self.gaps        = GapDetector()

        # History (fallback) DB — only connected lazily, on first gap found
        self.conn_history           = None
        self._history_connect_tried = False

        # (symbol, tf_str) -> count of candles that stayed missing after
        # every fallback tier. Populated during _backfill_symbol, read by
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
        try:
            conn = psycopg2.connect(
                host            = os.getenv("PG_HOST"),
                port            = int(os.getenv("PG_PORT", "5432")),
                dbname          = os.getenv("PG_DBNAME") or os.getenv("PG_DATABASE"),
                user            = os.getenv("PG_USER"),
                password        = os.getenv("PG_PASSWORD"),
                sslmode         = os.getenv("PG_SSLMODE", "require"),
                connect_timeout = 15,
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
        try:
            conn = psycopg2.connect(
                host            = os.getenv("PG_HOST"),
                port            = int(os.getenv("PG_PORT", "5432")),
                dbname          = os.getenv("PG_HDBNAME"),
                user            = os.getenv("PG_USER"),
                password        = os.getenv("PG_PASSWORD"),
                sslmode         = os.getenv("PG_SSLMODE", "require"),
                connect_timeout = 15,
            )
            conn.autocommit = True
            print("[BACKFILL] Connected to PostgreSQL (history db)", flush=True)
            self.conn_history = conn
            return conn
        except Exception as exc:
            print(f"[BACKFILL][WARN] History DB connection failed: {exc}", flush=True)
            return None

    # ─────────────────────────────────────────────
    # Entry point
    # ─────────────────────────────────────────────

    def run(self, symbols):
        if self.conn is None:
            print("[BACKFILL][ERROR] No DB connection — skipping backfill", flush=True)
            return

        tf_names = [tf for tf, _ in self.timeframes]
        start_ts = self._compute_lookback_start()

        print(
            f"[BACKFILL] Starting | symbols={len(symbols)} "
            f"| TFs={tf_names} | from={start_ts.strftime('%Y-%m-%d %H:%M %Z')}",
            flush=True,
        )

        for inst in symbols:
            self._backfill_symbol(inst["symbol"], start_ts)

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
    # Per-symbol
    # ─────────────────────────────────────────────

    def _backfill_symbol(self, symbol: str, start_ts: datetime):
        now = datetime.now(tz_kolkata)

        # ── Step 0: check local parquet FIRST, before touching any DB ──────
        # Candles already on disk (from a previous run, or OHLCCollector's
        # own live saves) need no re-fetch at all. Only the buckets still
        # missing after this check ever reach the main db.
        local_by_tf     = {}   # tf_str -> (local_df, missing_after_local)
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

        if earliest_needed is None:
            print(
                f"[BACKFILL] {symbol}: fully covered by local parquet for all "
                f"TFs — skipping main db fetch entirely",
                flush=True,
            )
            ticks_df = pd.DataFrame()
        else:
            # Narrow the fetch to only the range not already covered locally,
            # instead of the full lookback window.
            fetch_start = max(start_ts, earliest_needed)
            print(
                f"[BACKFILL] {symbol}: fetching main db only from "
                f"{fetch_start.strftime('%Y-%m-%d %H:%M')} (rest covered locally)",
                flush=True,
            )
            ticks_df = self._fetch_ticks(symbol, fetch_start)

            if ticks_df is None:
                # Query itself failed (not just "no rows") — nothing usable
                # from primary. Still fall through so history DB gets a
                # chance below.
                ticks_df = pd.DataFrame()

            if ticks_df.empty:
                print(f"[BACKFILL][WARN] {symbol}: no tick data in main db", flush=True)
            else:
                print(f"[BACKFILL] {symbol}: {len(ticks_df)} ticks fetched", flush=True)

        # Finalized 1m candles (post gap-fill) — reused to derive higher
        # timeframes when the history db only stores 1m rows (as observed:
        # it may have no "5m"/"15m" interval rows at all).
        base_1m_candles = None

        for tf_str, tf_seconds in self.timeframes:
            local_df, _ = local_by_tf[tf_str]

            fresh   = self._aggregate(ticks_df, tf_seconds, now)
            candles = self.gaps.merge_candles(local_df, fresh)

            expected = self.gaps.expected_buckets(start_ts, now, tf_seconds)
            missing  = self.gaps.find_missing(candles, expected)

            # ── Step 1: direct interval match in history db ────────────
            if missing:
                filled = self._fetch_history_candles(symbol, tf_str, missing)
                if not filled.empty:
                    candles = self.gaps.merge_candles(candles, filled)
                    print(
                        f"[BACKFILL] {symbol} {tf_str}: "
                        f"{len(filled)}/{len(missing)} gap candle(s) "
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

            for _, row in candles.iterrows():
                self.ohlc.save_candle(symbol, tf_str, {
                    "timestamp": row["timestamp"],
                    "open":      float(row["open"]),
                    "high":      float(row["high"]),
                    "low":       float(row["low"]),
                    "close":     float(row["close"]),
                    "volume":    float(row["volume"]),
                })

            print(
                f"[BACKFILL] {symbol} {tf_str}: {len(candles)} closed candles loaded",
                flush=True,
            )

    # ─────────────────────────────────────────────
    # Fetch ticks from PostgreSQL (main db)
    # ─────────────────────────────────────────────

    def _fetch_ticks(self, symbol: str, start_ts: datetime):
        # Sanitize symbol for table name (alphanumeric + underscore only).
        # pg_writer._ensure_table always lowercases; Postgres folds unquoted
        # identifiers to lowercase too, but lowering here keeps the SQL
        # text matching what's actually on disk instead of relying on
        # that fold happening implicitly.
        safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_")
        table    = f"quote_{safe_sym}".lower()
        start_ms = int(start_ts.timestamp() * 1000)

        # Real schema (pg_writer._QUOTE_COLUMN_DEFS) is typed columns —
        # there is no raw_json column on quote_*/depth_* tables, and the
        # tick-quantity column is named last_quantity, not
        # last_trade_quantity.
        query = f"""
            SELECT
                timestamp,
                ltp,
                COALESCE(last_quantity, 0) AS qty
            FROM {table}
            WHERE timestamp >= %s
              AND ltp IS NOT NULL
            ORDER BY timestamp
        """

        try:
            cur = self.conn.cursor()
            cur.execute(query, (start_ms,))
            rows = cur.fetchall()
            cur.close()
        except Exception as exc:
            print(f"[BACKFILL][ERROR] {symbol}: query failed — {exc}", flush=True)
            try:
                self.conn.rollback()
            except Exception:
                pass
            return None

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["timestamp", "ltp", "qty"])

        # Convert exchange-ms → IST datetime
        df["ist_ts"] = (
            pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            .dt.tz_convert(tz_kolkata)
        )

        # Keep only market-hours rows on weekdays
        t = df["ist_ts"].dt.time
        df = df[
            (t >= MARKET_OPEN) &
            (t <  MARKET_CLOSE) &
            (df["ist_ts"].dt.dayofweek < 5)
        ].reset_index(drop=True)

        return df

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

        df["bucket"] = df["ist_ts"].apply(
            lambda ts: compute_bucket(ts, tf_seconds)
        )

        # Drop any tick that landed before market open (bucket would be 9:15:00
        # even for pre-market ticks — filter them by comparing raw ist_ts)
        df = df[df["ist_ts"].dt.time >= MARKET_OPEN]

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
    # Fetch missing candles from the history DB (fallback)
    # ─────────────────────────────────────────────

    def _fetch_history_candles(self, symbol: str, tf_str: str, missing_buckets: list):
        """
        market_history DB — same quote_{SYMBOL} table naming as the main
        DB, but raw_json already holds pre-aggregated OHLCV + an
        "interval" tag (e.g. "1m"), so no aggregation needed — just pull
        the exact rows matching the missing bucket timestamps.
        """
        conn = self._get_history_conn()
        empty = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        if conn is None or not missing_buckets:
            return empty

        safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_")
        table    = f"quote_{safe_sym}"
        ms_list  = [int(b.timestamp() * 1000) for b in missing_buckets]

        query = f"""
            SELECT
                timestamp,
                (raw_json->>'open')::float                              AS open,
                (raw_json->>'high')::float                              AS high,
                (raw_json->>'low')::float                               AS low,
                (raw_json->>'close')::float                             AS close,
                COALESCE((raw_json->>'volume')::float, 0)               AS volume
            FROM {table}
            WHERE timestamp = ANY(%s)
              AND raw_json->>'interval' = %s
        """

        try:
            cur = conn.cursor()
            cur.execute(query, (ms_list, tf_str))
            rows = cur.fetchall()
            cur.close()
        except Exception as exc:
            print(f"[BACKFILL][WARN] {symbol} {tf_str}: history query failed — {exc}", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
            return empty

        if not rows:
            return empty

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = (
            pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            .dt.tz_convert(tz_kolkata)
        )

        return df

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