# ohlc_backfill.py

import os
import re
import math
import psycopg2
import pandas as pd
from datetime import datetime, timedelta, time as dtime
from dotenv import load_dotenv
from market_time import tz_kolkata, MARKET_OPEN, MARKET_CLOSE

load_dotenv()

# 9:15:00 in seconds from midnight
MARKET_OPEN_SECS = MARKET_OPEN.hour * 3600 + MARKET_OPEN.minute * 60  # 33300
# Minutes in one full trading session (9:15 → 15:30)
SESSION_MINUTES   = 375


# ─────────────────────────────────────────────
# Shared utilities (mirrors ohlc.py)
# ─────────────────────────────────────────────

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


def _compute_bucket(ist_ts: datetime, tf_seconds: int) -> datetime:
    """
    Return the candle-open timestamp for a given tick time and TF interval.
    All buckets are aligned to market open (9:15:00 IST) — not to midnight.

    Examples  (tf_seconds=5):   9:15:03 → 9:15:00 | 9:15:07 → 9:15:05
    Examples  (tf_seconds=300): 9:17:42 → 9:15:00 | 9:21:00 → 9:20:00
    """
    secs = ist_ts.hour * 3600 + ist_ts.minute * 60 + ist_ts.second
    secs_since_open = max(secs - MARKET_OPEN_SECS, 0)
    bucket_secs = MARKET_OPEN_SECS + (secs_since_open // tf_seconds) * tf_seconds
    return ist_ts.replace(
        hour        = bucket_secs // 3600,
        minute      = (bucket_secs % 3600) // 60,
        second      = bucket_secs % 60,
        microsecond = 0,
    )


class OHLCBackfill:
    """
    OHLCBackfill — PostgreSQL tick-based backfill
    ─────────────────────────────────────────────
    Schema expected (from pg_writer.py):
        table : quote_{SYMBOL}   (e.g. quote_RELIANCE)
        cols  : timestamp BIGINT (exchange ms, indexed)
                ingest_ns BIGINT
                raw_json  JSONB  → fields used: ltp, last_trade_quantity

    Strategy:
        • Fetch raw ticks per symbol from PostgreSQL
        • Aggregate directly into every configured TF (no derivation chain)
        • All TFs use the same market-open-aligned bucket formula
        • Only closed candles are loaded (current forming candle excluded)
        • Validates MIN_CANDLES after loading
    """

    def __init__(self, ohlc):
        self.ohlc        = ohlc
        self.min_candles = int(os.getenv("MIN_CANDLES", "15"))
        self.timeframes  = self._load_timeframes()   # [(tf_str, tf_seconds), ...]
        self.conn        = self._connect()

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
                dbname          = os.getenv("PG_DATABASE"),
                user            = os.getenv("PG_USER"),
                password        = os.getenv("PG_PASSWORD"),
                sslmode         = os.getenv("PG_SSLMODE", "require"),
                connect_timeout = 15,
            )
            conn.autocommit = True
            print("[BACKFILL] Connected to PostgreSQL", flush=True)
            return conn
        except Exception as exc:
            print(f"[BACKFILL][ERROR] PostgreSQL connection failed: {exc}", flush=True)
            return None

    # ─────────────────────────────────────────────
    # Entry point
    # ─────────────────────────────────────────────

    def run(self, symbols):
        if self.conn is None:
            print("[BACKFILL][ERROR] No DB connection — skipping backfill", flush=True)
            return

        tf_names   = [tf for tf, _ in self.timeframes]
        start_ts   = self._compute_lookback_start()

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

        print("[BACKFILL] Completed", flush=True)

    # ─────────────────────────────────────────────
    # Per-symbol
    # ─────────────────────────────────────────────

    def _backfill_symbol(self, symbol: str, start_ts: datetime):
        ticks_df = self._fetch_ticks(symbol, start_ts)

        if ticks_df is None or ticks_df.empty:
            print(f"[BACKFILL][WARN] {symbol}: no tick data found — skipping", flush=True)
            return

        print(f"[BACKFILL] {symbol}: {len(ticks_df)} ticks fetched", flush=True)

        now = datetime.now(tz_kolkata)

        for tf_str, tf_seconds in self.timeframes:
            candles = self._aggregate(ticks_df, tf_seconds, now)
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
    # Fetch ticks from PostgreSQL
    # ─────────────────────────────────────────────

    def _fetch_ticks(self, symbol: str, start_ts: datetime):
        # Sanitize symbol for table name (alphanumeric + underscore only)
        safe_sym = "".join(c for c in symbol if c.isalnum() or c == "_")
        table    = f"quote_{safe_sym}"
        start_ms = int(start_ts.timestamp() * 1000)

        query = f"""
            SELECT
                timestamp,
                (raw_json->>'ltp')::float                                    AS ltp,
                COALESCE((raw_json->>'last_trade_quantity')::float, 0)       AS qty
            FROM {table}
            WHERE timestamp >= %s
              AND raw_json->>'ltp' IS NOT NULL
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

        df["bucket"] = df["ist_ts"].apply(
            lambda ts: _compute_bucket(ts, tf_seconds)
        )

        # Drop any tick that landed before market open (bucket would be 9:15:00
        # even for pre-market ticks — filter them by comparing raw ist_ts)
        df = df[df["ist_ts"].dt.time >= MARKET_OPEN]

        # Exclude the currently-forming (incomplete) candle
        current_bucket = _compute_bucket(now, tf_seconds)
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
    # Lookback window calculation
    # ─────────────────────────────────────────────

    def _compute_lookback_start(self) -> datetime:
        """
        Go back far enough to guarantee MIN_CANDLES of the largest configured TF.
        Formula:
            trading_days_needed = ceil( (MIN_CANDLES × largest_tf_minutes) / 375 ) + 1 buffer day
        375 = market minutes per session (9:15 → 15:30).
        """
        largest_tf_secs    = max(s for _, s in self.timeframes)
        minutes_needed     = (self.min_candles * largest_tf_secs) / 60
        trading_days_needed = math.ceil(minutes_needed / SESSION_MINUTES) + 1

        now = datetime.now(tz_kolkata)
        day = now.date()
        counted = 0
        while counted < trading_days_needed:
            day -= timedelta(days=1)
            if day.weekday() < 5:   # Mon–Fri only
                counted += 1

        return datetime.combine(day, dtime(9, 15, 0)).replace(tzinfo=tz_kolkata)

    # ─────────────────────────────────────────────
    # Validation
    # ─────────────────────────────────────────────

    def _validate(self, symbols):
        print("[BACKFILL] Validating candle counts...", flush=True)
        all_ok = True

        for inst in symbols:
            symbol = inst["symbol"]
            for tf_str, _ in self.timeframes:
                count = len(
                    self.ohlc.ohlc_data.get(tf_str, {}).get(symbol, [])
                )
                if count < self.min_candles:
                    print(
                        f"[BACKFILL][WARN] {symbol} {tf_str}: "
                        f"{count}/{self.min_candles} candles — insufficient",
                        flush=True,
                    )
                    all_ok = False
                else:
                    print(
                        f"[BACKFILL] ✅ {symbol} {tf_str}: {count} candles",
                        flush=True,
                    )

        if all_ok:
            print("[BACKFILL] ✅ All symbols have sufficient candle history", flush=True)