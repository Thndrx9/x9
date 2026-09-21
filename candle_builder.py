# candle_builder.py
#
# Owns ALL candle-building logic: turning cached ticks + cached
# history-db candles into per-timeframe OHLC candles and saving them.
# This is a SEPARATE, self-contained module — it does NOT import or
# depend on backfill_manager.py in any way. Its own config
# (TIMEFRAMES, OPENALGO_HISTORY_INTERVAL, MIN_CANDLES) is read
# directly from the environment (.env), the same env vars
# backfill_manager.py reads, but independently — there's no shared
# object between the two files. BackfillManager.run_low_memory()
# imports CandleBuilder (the class) and calls it in-process, passing
# in whatever config it already loaded; this file never reaches back
# into backfill_manager.py to get that config itself.
#
# This file is ALSO a standalone / offline candle-rebuild script when
# run directly — NOT part of the live startup path. Do NOT run it
# before starting live trading.
#
# Why in-process matters (for the BackfillManager.run_low_memory()
# call path): save_candles_bulk() (ohlc.py) writes candles to BOTH the
# parquet files on disk AND the in-RAM ohlc_data dict that indicators/
# signal_generator read from live. Running candle building in a
# separate OS process means that RAM dict belongs to THIS process and
# is discarded the instant it exits — the live process's
# OHLCCollector would never see those candles. There is currently no
# code path that reloads parquet files back into a running
# OHLCCollector's RAM, so using `python3 candle_builder.py` before
# live trading starts would leave indicators with no candle history.
#
# What running THIS script directly is for: rebuilding/backfilling
# candles on disk only, from whatever's already in the local tick/
# history cache — e.g. regenerating parquet history for analysis/
# backtesting. Run it manually, separately from the live system. It
# never opens a Postgres/main-db connection itself.
#
# Usage:
#   python3 candle_builder.py

import os
import re
import sys
import math
from collections import deque
from datetime import datetime, timedelta, time as dtime
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from market_time import tz_kolkata, is_trading_day
from gap_detector import compute_tick_volume_vectorized, MARKET_CLOSE_SECS, CAS_CONTINUOUS_CLOSE_SECS
from fo_symbols import get_fo_underlyings

load_dotenv()

CHUNK_SIZE = 20

# Minutes in one full trading session (9:15 → 15:30) — used by
# _compute_lookback_start() below. Kept as its own constant (not
# imported from backfill_manager.py) to keep this file dependency-free.
SESSION_MINUTES = 375


def _tf_to_seconds(tf_str: str) -> int:
    """
    Parse a timeframe string into total seconds.
    Supports:  '5s' → 5   '30s' → 30   '1m' → 60
               '5m' → 300  '1m30s' → 90  '15m' → 900
    Own copy — not imported from backfill_manager.py, to keep this
    file independent.
    """
    total = 0
    for value, unit in re.findall(r'(\d+)([ms])', tf_str.lower()):
        total += int(value) * (60 if unit == 'm' else 1)
    if total == 0:
        raise ValueError(f"[CANDLE_BUILDER] Invalid timeframe string: '{tf_str}'")
    return total


def load_timeframes_from_env():
    """
    Reads TIMEFRAMES straight from the environment (.env) — e.g.
    "1m,5m" → [("1m", 60), ("5m", 300)], sorted smallest-first. Same
    env var backfill_manager.py reads, read independently here.
    """
    tf_env = os.getenv("TIMEFRAMES", "1m,5m")
    result = []
    for tf in tf_env.split(","):
        tf = tf.strip()
        if tf:
            result.append((tf, _tf_to_seconds(tf)))
    return sorted(result, key=lambda x: x[1])


def load_history_native_tf_from_env() -> str:
    """
    market_history's quote_{SYMBOL} rows are pre-built candles from
    x9_data_fetcher's own collector, at whatever
    OPENALGO_HISTORY_INTERVAL that collector was configured with —
    default "1m". Read directly from env here, independently of
    backfill_manager.py.
    """
    return os.getenv("OPENALGO_HISTORY_INTERVAL", "1m").strip() or "1m"


def compute_lookback_start(timeframes, min_candles: int) -> datetime:
    """
    Go back far enough to guarantee min_candles of the largest
    configured TF.
    Formula:
        trading_days_needed = ceil( (min_candles × largest_tf_secs / 60) / 375 ) + 1 buffer day
    375 = market minutes per session (9:15 → 15:30).
    Own copy of BackfillManager._compute_lookback_start(), taking
    timeframes/min_candles as arguments instead of reading them off a
    shared object.
    """
    largest_tf_secs     = max(s for _, s in timeframes)
    minutes_needed      = (min_candles * largest_tf_secs) / 60
    trading_days_needed = math.ceil(minutes_needed / SESSION_MINUTES) + 1

    now = datetime.now(tz_kolkata)
    day = now.date()
    counted = 0
    while counted < trading_days_needed:
        day -= timedelta(days=1)
        if is_trading_day(day):   # weekday AND not a holiday, incl. special_open
            counted += 1

    return datetime.combine(day, dtime(9, 15, 0)).replace(tzinfo=tz_kolkata)


class CandleBuilder:
    """
    Candle building from the (now gap-free) local cache — meant to be
    called AFTER backfill_manager.py's Postgres<->local-cache sync
    phases have already made the local tick/history caches correct.
    Reads fresh from local cache/history_store per chunk, builds +
    saves, discards before the next chunk. Never touches Postgres/
    main db itself.
    """

    def __init__(
        self,
        ohlc,
        timeframes,
        history_native_tf: str,
        gaps,
        tick_writer=None,
        history_store=None,
        candle1m_store=None,
        api_key: str = None,
    ):
        self.ohlc = ohlc
        # [(tf_str, tf_seconds), ...] — same config BackfillManager loaded.
        self.timeframes = timeframes
        # market_history's quote_{SYMBOL} rows are pre-built candles at
        # whatever interval x9_data_fetcher's collector used — needed
        # here to know how to treat history_df during aggregation.
        self.history_native_tf = history_native_tf
        # GapDetector instance — shared with BackfillManager so gap
        # bookkeeping is consistent across fetch and build phases.
        self.gaps = gaps
        self.tick_writer = tick_writer if tick_writer is not None else getattr(ohlc, "tick_writer", None)
        self.history_store = history_store if history_store is not None else getattr(ohlc, "history_store", None)
        # Tier-2 pre-aggregated 1-minute candle cache (candle1m_<symbol>)
        # — optional. Days older than the raw-tick Tier-1 window live
        # here instead of quote_<symbol>; see
        # BackfillManager.sync_1m_candle_cache()'s docstring for the
        # full two-tier design. None just means every day in range is
        # still expected to have raw ticks (e.g. the standalone script
        # entry point, which doesn't do the tiered rollover at all).
        self.candle1m_store = candle1m_store
        # Passed to fo_symbols.get_fo_underlyings() once per
        # build_candles_from_local_cache() run (see that method) to
        # determine, per symbol, whether continuous-trading candles
        # should stop at 15:15 (F&O/CAS) or 15:30 — see
        # OHLCCollector.aggregate_ticks_to_candles()'s docstring and
        # fo_symbols.py's module docstring. Defaults to the same
        # API_KEY env var every other OpenAlgo call in this app already
        # uses (see engine_runtime.py) — not plumbed through
        # BackfillManager's constructor since every caller already has
        # it available in the environment. Empty/missing means
        # "unknown F&O status" — every symbol is then treated as
        # non-F&O (15:30), same as get_fo_underlyings() itself would
        # return with a missing key.
        self.api_key = api_key if api_key is not None else os.getenv("API_KEY", "")
        self._fo_underlyings = set()

        # (symbol, tf_str) -> count of candles that stayed missing after
        # every fallback tier. Populated during _finalize_symbol; the
        # caller (BackfillManager._validate()) reads this back via
        # CandleBuilder.missing_counts after build_candles_from_local_cache()
        # returns.
        self.missing_counts = {}

    # ─────────────────────────────────────────────
    # Progress reporting — own copy of BackfillManager's helper so this
    # class has no dependency back on BackfillManager.
    # ─────────────────────────────────────────────

    def _progress(self, label: str, current: int, total: int):
        """
        Single in-place-updating progress line (like a progress bar) —
        overwrites itself via carriage return instead of printing one
        line per symbol. Prints a trailing newline once current==total
        so the next phase's output starts on a fresh line.
        """
        end = "\n" if current >= total else ""
        print(f"\r[CANDLE_BUILDER] {label}: {current}/{total} symbols\033[K", end=end, flush=True)

    # ─────────────────────────────────────────────
    # Per-symbol aggregation (Phase 3) — pure pandas, no DB
    # ─────────────────────────────────────────────

    def _ticks_rows_to_df(self, rows: list) -> pd.DataFrame:
        """Shape a raw read_ticks()-style row list into the DataFrame
        _cached_ticks_df() builds — factored out so callers that
        already have rows fetched can build the same shape without a
        redundant DB call.

        Includes the cumulative "volume" column (present on every
        read_ticks()/read_ticks_combined() row) alongside "qty" — see
        OHLCCollector.aggregate_ticks_to_candles()'s docstring for why
        candle volume is diffed from cumulative "volume" rather than
        summed from "qty" (last_trade_quantity).

        "ist_ts" is derived from "ltt" (last TRADE time), not
        "timestamp" (packet/quote broadcast time) — falling back to
        "timestamp" only if ltt is null — so this matches
        OHLCCollector._process_tick()'s live bucket assignment (already
        keyed off ltt). See aggregate_ticks_to_candles()'s docstring."""
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["timestamp", "ltp", "qty", "volume", "ltt"])
        eff_ts_ms = df["ltt"].fillna(df["timestamp"])
        df["ist_ts"] = pd.to_datetime(eff_ts_ms, unit="ms", utc=True).dt.tz_convert(tz_kolkata)
        return df

    def _cached_ticks_df(self, symbol: str, start_ms: int, end_ms: int, conn=None) -> pd.DataFrame:
        """
        Read symbol's already-cached quote ticks back out of ticks.db
        (TickWriter) for [start_ms, end_ms], shaped into columns
        (timestamp, ltp, qty, ist_ts) ready for aggregation.

        conn: optional pre-opened connection to pass through to
        tick_writer.read_ticks().
        """
        if self.tick_writer is None:
            return pd.DataFrame()

        rows = self.tick_writer.read_ticks(symbol, start_ms=start_ms, end_ms=end_ms, kind="quote", conn=conn)
        return self._ticks_rows_to_df(rows)

    def _cached_history_df(self, symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
        """
        Read symbol's already-cached history-db candles back out of
        history_candles.db (HistoryCandleStore) for [start_ms, end_ms],
        shaped into columns (timestamp as tz-aware Kolkata, open/high/
        low/close/volume) ready to merge with the tick-aggregated
        candles. Also folds in any Tier-2 pre-aggregated 1m candles
        (candle1m_store) covering the same range — see
        _cached_candle1m_df() — since both are "already-built 1m
        candles that should win over tick-aggregation," they merge
        into the exact same slot ohlc.build_symbol_candles() already
        knows how to prioritize (history_df).
        """
        frames = []

        if self.history_store is not None:
            rows = self.history_store.read_candles(symbol, start_ms=start_ms, end_ms=end_ms)
            if rows:
                df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)
                frames.append(df)

        candle1m_df = self._cached_candle1m_df(symbol, start_ms, end_ms)
        if not candle1m_df.empty:
            frames.append(candle1m_df)

        if not frames:
            return pd.DataFrame()

        merged = pd.concat(frames, ignore_index=True)
        # Tier-2 and the history-db mirror are expected to cover
        # disjoint day ranges by construction (Tier 2 only ever holds
        # days already outside the raw-tick window) — this de-dupe is
        # just a safety net against any overlap, keeping the first
        # (history_store) row on a collision.
        merged = merged.drop_duplicates(subset="timestamp", keep="first").sort_values("timestamp")
        return merged.reset_index(drop=True)

    def _cached_candle1m_df(self, symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
        """
        Read symbol's already-rolled-over Tier-2 1-minute candles
        (candle1m_<symbol> via candle1m_store) for [start_ms, end_ms].
        Empty DataFrame if no candle1m_store was given (see __init__)
        or nothing's cached in range yet.
        """
        if self.candle1m_store is None:
            return pd.DataFrame()

        rows = self.candle1m_store.read_candles(symbol, start_ms=start_ms, end_ms=end_ms)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)
        return df

    def _aggregate_symbol_with_history(self, ticks_df, history_df, start_ts, now, symbol):
        """
        Thin pass-through to OHLCCollector.build_symbol_candles() —
        candle building itself lives in ohlc.py; this class only
        supplies what it fetched (ticks_df/history_df) plus its own
        config (timeframes, history_native_tf) and its GapDetector.

        Looks up symbol's F&O status (self._fo_underlyings, refreshed
        once per build_candles_from_local_cache() run) to pick the
        right continuous-trading close cutoff — 15:15 for F&O/CAS
        symbols, 15:30 otherwise. See
        OHLCCollector.aggregate_ticks_to_candles()'s docstring.
        """
        continuous_close_secs = (
            CAS_CONTINUOUS_CLOSE_SECS if symbol.upper() in self._fo_underlyings
            else MARKET_CLOSE_SECS
        )
        return self.ohlc.build_symbol_candles(
            ticks_df, history_df, start_ts, now,
            self.timeframes, self.history_native_tf, self.gaps,
            continuous_close_secs,
        )

    # ─────────────────────────────────────────────
    # Per-symbol finalization (Phase 4) — save to ohlc + report
    # whatever's still missing after the history-priority merge in
    # _aggregate_symbol_with_history(). No DB calls in here at all.
    # ─────────────────────────────────────────────

    def _finalize_symbol(self, symbol, per_tf):
        """
        Saves every aggregated candle to ohlc and tracks missing-bucket /
        zero-candle counts on self for the caller to report as one
        aggregate summary afterward — no per-symbol/per-tf lines
        printed here.
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
            # cost of the candle-building phase. save_candles_bulk()
            # does one read-merge-write for the entire batch instead.
            self.ohlc.save_candles_bulk(symbol, tf_str, candles)

        return zero_candle_tfs

    # ─────────────────────────────────────────────
    # Candle building entry point
    # ─────────────────────────────────────────────

    def build_candles_from_local_cache(self, symbols, start_ts, now, chunk_size: int = 20):
        start_ms = int(start_ts.timestamp() * 1000)
        now_ms   = int(now.timestamp() * 1000)
        window_secs = getattr(self.ohlc, "tick_ram_window_secs", 9 * 60)
        seed_stats = {"seeded": 0, "empty": 0, "ticks": 0}

        # Refreshed once per run (not per symbol/chunk) — get_fo_underlyings()
        # has its own 24h cache internally, so this costs nothing extra
        # on a normal day; calling it here rather than once at __init__
        # time just means a long-running process picks up the daily
        # refresh naturally on its next build pass instead of needing a
        # restart. Empty self.api_key -> empty set -> every symbol
        # treated as non-F&O (15:30 close), same safe default
        # get_fo_underlyings() itself would produce.
        self._fo_underlyings = get_fo_underlyings(self.api_key) if self.api_key else set()

        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]

            for inst in chunk:
                symbol = inst["symbol"]

                ticks_df = self._cached_ticks_df(symbol, start_ms, now_ms)
                history_df = self._cached_history_df(symbol, start_ms, now_ms)

                per_tf = self._aggregate_symbol_with_history(ticks_df, history_df, start_ts, now, symbol)
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

        # Diff the cumulative "volume" column into each tick's own
        # incremental contribution on the FULL df, BEFORE slicing to
        # the trailing window — see compute_tick_volume_vectorized()'s
        # docstring for why (it needs each day's own first tick as a
        # baseline; slicing first could cut that baseline tick out and
        # make the window's first retained tick's diff wrong).
        if "volume" in ticks_df.columns:
            ticks_df = ticks_df.copy()
            ticks_df["tick_volume"] = compute_tick_volume_vectorized(
                ticks_df["ist_ts"], ticks_df["volume"],
                tiebreaker=ticks_df["timestamp"] if "timestamp" in ticks_df.columns else None,
            )
        else:
            ticks_df["tick_volume"] = ticks_df.get("qty", 0)

        cutoff = ticks_df["ist_ts"].max() - timedelta(seconds=window_secs)
        window_df = ticks_df[ticks_df["ist_ts"] >= cutoff]
        return self._load_ticks_into_ram(symbol, window_df)

    def _load_ticks_into_ram(self, symbol, df) -> int:
        if df.empty:
            return 0

        entries = [
            {"timestamp": row.ist_ts, "ltp": row.ltp, "qty": row.tick_volume}
            for row in df.itertuples(index=False)
        ]

        with self.ohlc._ram_lock:
            dq = self.ohlc.raw_ticks.setdefault(symbol, deque())
            dq.clear()
            dq.extend(entries)

        return len(entries)


# ─────────────────────────────────────────────
# Standalone / offline entry point — see module docstring at top for
# why this must NOT be run before live trading starts.
# ─────────────────────────────────────────────

def main():
    from utils import load_symbols
    from tick_writer import TickWriter, HistoryCandleStore
    from ohlc import OHLCCollector
    from gap_detector import GapDetector

    symbols = load_symbols("symbols.csv")
    if not symbols:
        print("[CANDLE_BUILDER][ERROR] No valid symbols found in symbols.csv", flush=True)
        sys.exit(1)

    # Own config, read straight from env — no BackfillManager involved.
    timeframes        = load_timeframes_from_env()
    history_native_tf = load_history_native_tf_from_env()
    min_candles       = int(os.getenv("MIN_CANDLES", "15"))
    gaps              = GapDetector()

    # Same local-cache backends the live process uses — this script
    # only reads them, it never opens a Postgres/main-db connection.
    tick_writer   = TickWriter(base_dir="tickdata")
    history_store = HistoryCandleStore(base_dir="tickdata")
    ohlc          = OHLCCollector(tick_writer=tick_writer, history_store=history_store)

    start_ts = compute_lookback_start(timeframes, min_candles)
    now      = datetime.now(tz_kolkata)

    print(
        f"[CANDLE_BUILDER] Starting | {len(symbols)} symbol(s) | "
        f"from={start_ts.strftime('%Y-%m-%d %H:%M %Z')} | chunk_size={CHUNK_SIZE}",
        flush=True,
    )

    # Builds purely from whatever's already in the local tick/history
    # cache — this script never syncs against the main Postgres db
    # itself (that's the live process's job, via
    # BackfillManager.run_low_memory()). Run the live process first
    # (or recently) if the local cache needs topping up.
    candle_builder = CandleBuilder(
        ohlc, timeframes, history_native_tf, gaps,
        tick_writer=tick_writer, history_store=history_store,
    )
    candle_builder.build_candles_from_local_cache(symbols, start_ts, now, chunk_size=CHUNK_SIZE)

    print("[CANDLE_BUILDER] Completed", flush=True)


if __name__ == "__main__":
    main()
