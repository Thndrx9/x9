# gap_detector.py

import os
import pandas as pd
from datetime import datetime, timedelta
from market_time import tz_kolkata, MARKET_OPEN, MARKET_CLOSE, is_trading_day

# 9:15:00 in seconds from midnight
MARKET_OPEN_SECS = MARKET_OPEN.hour * 3600 + MARKET_OPEN.minute * 60  # 33300

_CANDLE_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


def compute_bucket(ist_ts: datetime, tf_seconds: int) -> datetime:
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


class GapDetector:
    """
    GapDetector — PURE LOGIC, NO DB / NO I/O
    ------------------------------------------
    • Knows the market-open-aligned bucket schedule for any TF
    • Diffs "what should exist" against "what we actually have"
    • Can derive a higher-TF candle from its constituent 1m candles
      (a self-contained, in-memory fallback — no external source needed)

    BackfillManager owns all DB connections / queries and calls into
    this class to figure out what's missing and to roll up 1m data.
    """

    # ─────────────────────────────────────────────
    # Expected bucket schedule
    # ─────────────────────────────────────────────

    def expected_buckets(self, start_ts: datetime, now: datetime, tf_seconds: int):
        """
        Every candle-open timestamp that SHOULD exist between start_ts and
        now, across all weekday trading sessions, for one timeframe.
        Excludes the still-forming candle, same as the aggregator does.
        """
        current_bucket = compute_bucket(now, tf_seconds)

        buckets = []
        day = start_ts.date()
        end_day = now.date()

        while day <= end_day:
            if is_trading_day(day):
                day_start = datetime.combine(day, MARKET_OPEN, tzinfo=tz_kolkata)
                day_end   = datetime.combine(day, MARKET_CLOSE, tzinfo=tz_kolkata)

                cur = day_start
                while cur < day_end:
                    if cur >= start_ts and cur < current_bucket:
                        buckets.append(cur)
                    cur += timedelta(seconds=tf_seconds)

            day += timedelta(days=1)

        return buckets

    # ─────────────────────────────────────────────
    # Diff expected vs. actual
    # ─────────────────────────────────────────────

    def find_missing(self, candles: pd.DataFrame, expected: list) -> list:
        """Sorted list of expected bucket timestamps not present in candles."""
        have = set(candles["timestamp"]) if candles is not None and not candles.empty else set()
        return sorted(b for b in expected if b not in have)

    # ─────────────────────────────────────────────
    # Derive higher-TF candles from finalized 1m data
    # ─────────────────────────────────────────────

    def derive_from_1m(self, base_1m: pd.DataFrame, missing_buckets: list, tf_seconds: int) -> pd.DataFrame:
        """
        Build a higher-TF candle from its constituent 1m candles.
        Only fills a bucket if ALL of its 1m sub-candles are present —
        a partial roll-up would silently understate the true high/low,
        so an incomplete bucket is left missing rather than guessed at.
        """
        empty = pd.DataFrame(columns=_CANDLE_COLUMNS)

        if base_1m is None or base_1m.empty or not missing_buckets:
            return empty

        lookup = base_1m.set_index("timestamp")
        n_sub  = tf_seconds // 60
        rows   = []

        for bucket in missing_buckets:
            sub_times = [bucket + timedelta(minutes=i) for i in range(n_sub)]
            if not all(t in lookup.index for t in sub_times):
                continue  # incomplete coverage — leave genuinely missing

            sub = lookup.loc[sub_times].sort_index()
            rows.append({
                "timestamp": bucket,
                "open":      float(sub.iloc[0]["open"]),
                "high":      float(sub["high"].max()),
                "low":       float(sub["low"].min()),
                "close":     float(sub.iloc[-1]["close"]),
                "volume":    float(sub["volume"].sum()),
            })

        return pd.DataFrame(rows) if rows else empty

    # ─────────────────────────────────────────────
    # Local parquet check — read what's already on disk BEFORE any DB
    # is touched. This is the first tier BackfillManager consults; a
    # symbol/TF fully covered here means zero main-db/history-db queries
    # for it at all.
    # ─────────────────────────────────────────────

    def read_local_parquet(self, base_dir: str, symbol: str, tf_str: str) -> pd.DataFrame:
        """
        Read {base_dir}/{symbol}/{tf_str}.parquet if it exists (written by
        OHLCCollector's own live candle saves, or a previous backfill run).
        Returns empty DataFrame if missing, unreadable, or empty — never
        raises, since a bad/partial local file should just fall through to
        the DB tiers rather than crash the whole backfill.
        """
        empty = pd.DataFrame(columns=_CANDLE_COLUMNS)
        path = os.path.join(base_dir, symbol, f"{tf_str}.parquet")

        if not os.path.exists(path):
            return empty

        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            print(f"[GAP_DETECTOR][WARN] Failed to read {path}: {exc}", flush=True)
            return empty

        if df.empty or "timestamp" not in df.columns:
            return empty

        ts = df["timestamp"]
        try:
            if ts.dt.tz is None:
                df["timestamp"] = ts.dt.tz_localize(tz_kolkata)
            else:
                df["timestamp"] = ts.dt.tz_convert(tz_kolkata)
        except Exception as exc:
            print(f"[GAP_DETECTOR][WARN] Bad timestamp column in {path}: {exc}", flush=True)
            return empty

        present = [c for c in _CANDLE_COLUMNS if c in df.columns]
        if "timestamp" not in present:
            return empty

        return df[present].dropna(subset=["timestamp"]).reset_index(drop=True)

    # ─────────────────────────────────────────────
    # Merge helper
    # ─────────────────────────────────────────────

    def merge_candles(self, candles: pd.DataFrame, extra: pd.DataFrame) -> pd.DataFrame:
        """Concat + de-dup (existing rows win) + re-sort by timestamp."""
        if extra is None or extra.empty:
            return candles
        if candles is None or candles.empty:
            candles = pd.DataFrame(columns=_CANDLE_COLUMNS)

        return (
            pd.concat([candles, extra], ignore_index=True)
            .drop_duplicates(subset="timestamp", keep="first")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )