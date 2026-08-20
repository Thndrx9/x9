# gap_detector.py

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from market_time import tz_kolkata, MARKET_OPEN, MARKET_CLOSE, is_trading_day

# 9:15:00 in seconds from midnight
MARKET_OPEN_SECS = MARKET_OPEN.hour * 3600 + MARKET_OPEN.minute * 60  # 33300
# 15:30:00 in seconds from midnight
MARKET_CLOSE_SECS = MARKET_CLOSE.hour * 3600 + MARKET_CLOSE.minute * 60  # 55800

_CANDLE_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]

_SEC_NS = 1_000_000_000
_DAY_NS = 86400 * _SEC_NS

# Asia/Kolkata has no DST — a single fixed UTC offset holds for every date,
# so it's safe to compute this once at import time rather than per call.
_IST_OFFSET_NS = int(pd.Timestamp.now(tz=tz_kolkata).utcoffset().total_seconds()) * _SEC_NS


def compute_bucket_vectorized(ist_ts: pd.Series, tf_seconds: int) -> pd.Series:
    """
    Vectorized (bulk) version of compute_bucket() — same market-open-aligned
    bucket math, applied to an entire pandas Series of tz-aware Asia/Kolkata
    timestamps at once via numpy int64 arithmetic, instead of one Python
    function call per row. This is the ONLY implementation of the bucket
    math; compute_bucket() (the scalar version below) just wraps this for
    single-timestamp callers, so the two can never drift out of sync.

    Avoids pandas' tz-aware `.dt.normalize()` / `.tz_localize()`, which are
    ~100-1000x slower than plain numpy int64 ops for this — they do
    per-element DST-safety checks that are pointless here since IST has no
    DST. We instead read the tz-aware Series' underlying UTC-epoch-ns
    values directly, do the offset/bucket math as plain integers, then
    reattach the tz once at the end.

    ist_ts must be a tz-aware Series (Asia/Kolkata). Returns a Series of
    the same length/index, same tz.
    """
    if ist_ts.empty:
        return ist_ts.copy()

    utc_ns = ist_ts.to_numpy(dtype="datetime64[ns]").view("int64")
    local_ns = utc_ns + _IST_OFFSET_NS

    secs_of_day = (local_ns // _SEC_NS) % 86400
    secs_since_open = np.clip(secs_of_day - MARKET_OPEN_SECS, 0, None)
    bucket_secs = MARKET_OPEN_SECS + (secs_since_open // tf_seconds) * tf_seconds

    day_start_local_ns = (local_ns // _DAY_NS) * _DAY_NS
    bucket_local_ns = day_start_local_ns + bucket_secs * _SEC_NS
    bucket_utc_ns = bucket_local_ns - _IST_OFFSET_NS

    bucket_index = (
        pd.DatetimeIndex(bucket_utc_ns.astype("datetime64[ns]"))
        .tz_localize("UTC")
        .tz_convert(ist_ts.dt.tz)
    )
    return pd.Series(bucket_index, index=ist_ts.index)


def _local_ns_and_secs_of_day(ist_ts: pd.Series):
    """
    Shared core for every vectorized time-of-day comparison in this module:
    tz-aware Series -> (local_ns int64 array, secs_of_day int64 array).

    Centralizes the UTC -> IST offset math (_IST_OFFSET_NS) so it's
    computed exactly one way — compute_bucket_vectorized(),
    is_at_or_after_market_open_vectorized(), and
    is_market_hours_weekday_vectorized() all agree on what "local time" a
    tick falls on because they'd all break together if this were wrong,
    rather than each reimplementing the offset arithmetic separately.

    Raises ValueError on a tz-naive Series. Silently accepting one is
    dangerous: verified that a naive 10:00 timestamp gets interpreted as
    already-UTC and then shifted forward by +5:30, landing on 15:30 and
    passing the market-hours check it should have failed — wrong, but not
    obviously wrong, so it's rejected outright rather than guessed at.
    Any tz-aware input is safe regardless of *which* tz it's labeled with
    (pandas stores tz-aware timestamps as UTC internally either way).
    """
    if ist_ts.dt.tz is None:
        raise ValueError(
            "expects a tz-aware Series (Asia/Kolkata); got tz-naive input, "
            "which would be silently misinterpreted as UTC rather than "
            "correctly filtered. Localize or convert it before calling."
        )
    utc_ns = ist_ts.to_numpy(dtype="datetime64[ns]").view("int64")
    local_ns = utc_ns + _IST_OFFSET_NS
    secs_of_day = (local_ns // _SEC_NS) % 86400
    return local_ns, secs_of_day


def is_at_or_after_market_open_vectorized(ist_ts: pd.Series) -> pd.Series:
    """
    Vectorized replacement for `ist_ts.dt.time >= MARKET_OPEN`.

    .dt.time builds a full array of Python datetime.time objects (one per
    row) and then compares them one at a time — the same anti-pattern that
    made the unvectorized compute_bucket() slow. This does the comparison
    as plain int64 seconds-of-day arithmetic instead (see
    _local_ns_and_secs_of_day()).

    NaT rows are explicitly forced to False, matching what
    `ist_ts.dt.time >= MARKET_OPEN` does for NaT. (The underlying int64
    arithmetic happens to also produce False for NaT's sentinel value on
    its own, but that's incidental to the current offset/threshold
    constants, not a guarantee — so it's masked explicitly here rather
    than relied upon.)

    ist_ts must be a tz-aware Series (Asia/Kolkata). Returns a boolean
    Series of the same length/index.
    """
    if ist_ts.empty:
        return pd.Series([], index=ist_ts.index, dtype=bool)

    _, secs_of_day = _local_ns_and_secs_of_day(ist_ts)
    result = (secs_of_day >= MARKET_OPEN_SECS) & ~ist_ts.isna().to_numpy()
    return pd.Series(result, index=ist_ts.index)


def is_market_hours_weekday_vectorized(ist_ts: pd.Series) -> pd.Series:
    """
    Vectorized replacement for the compound filter:
        (t >= MARKET_OPEN) & (t < MARKET_CLOSE) & (dayofweek < 5)

    Both .dt.time (object comparison) and .dt.dayofweek carry real cost at
    chunk scale (measured ~2.4s combined for 1.5M rows vs ~0.4s here).
    Weekday is derived as plain int64 math too: 1970-01-01 (Unix epoch) was
    a Thursday, so `(local_day_index + 3) % 7` gives Mon=0..Sun=6 without
    going through pandas' dayofweek accessor.

    NaT rows are explicitly forced to False, matching what the original
    `.dt.time` / `.dt.dayofweek` compound filter does for NaT — see the
    note in is_at_or_after_market_open_vectorized() about why this is
    explicit rather than left to incidental sentinel-value arithmetic.

    ist_ts must be a tz-aware Series (Asia/Kolkata). Returns a boolean
    Series of the same length/index.
    """
    if ist_ts.empty:
        return pd.Series([], index=ist_ts.index, dtype=bool)

    local_ns, secs_of_day = _local_ns_and_secs_of_day(ist_ts)
    day_idx = local_ns // _DAY_NS
    dow = (day_idx + 3) % 7  # epoch (1970-01-01) was a Thursday -> Mon=0

    mask = (
        (secs_of_day >= MARKET_OPEN_SECS)
        & (secs_of_day < MARKET_CLOSE_SECS)
        & (dow < 5)
        & ~ist_ts.isna().to_numpy()
    )
    return pd.Series(mask, index=ist_ts.index)


def compute_bucket(ist_ts: datetime, tf_seconds: int) -> datetime:
    """
    Return the candle-open timestamp for a given tick time and TF interval.
    All buckets are aligned to market open (9:15:00 IST) — not to midnight.

    Examples  (tf_seconds=5):   9:15:03 → 9:15:00 | 9:15:07 → 9:15:05
    Examples  (tf_seconds=300): 9:17:42 → 9:15:00 | 9:21:00 → 9:20:00

    Thin wrapper around compute_bucket_vectorized() (single source of
    truth — see its docstring). This scalar path is only ever called a
    handful of times per symbol (not once per tick), so the one-element
    Series overhead here is negligible.
    """
    s = pd.Series([pd.Timestamp(ist_ts)])
    return compute_bucket_vectorized(s, tf_seconds).iloc[0].to_pydatetime()


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
