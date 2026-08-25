# ohlc.py

import os
import re
import pandas as pd
from datetime import datetime, timedelta
from market_time import tz_kolkata, MARKET_OPEN
from event_bus import market_data_queue
import asyncio
import threading
from collections import deque
from parquet_writer import ParquetWriter

# 9:15:00 in seconds from midnight
MARKET_OPEN_SECS = MARKET_OPEN.hour * 3600 + MARKET_OPEN.minute * 60  # 33300


# ─────────────────────────────────────────────
# TF utilities  (also mirrored in gap_detector.py's compute_bucket)
# ─────────────────────────────────────────────

def _tf_to_seconds(tf_str: str) -> int:
    """
    Parse a timeframe string into total seconds.
    '5s' → 5   '30s' → 30   '1m' → 60
    '5m' → 300  '1m30s' → 90  '15m' → 900
    """
    total = 0
    for value, unit in re.findall(r'(\d+)([ms])', tf_str.lower()):
        total += int(value) * (60 if unit == 'm' else 1)
    if total == 0:
        raise ValueError(f"[OHLC] Invalid timeframe string: '{tf_str}'")
    return total


def _compute_bucket(ts: datetime, tf_seconds: int) -> datetime:
    """
    Return the candle-open timestamp aligned to market open (9:15:00 IST).
    Works for every TF — seconds, mixed, minutes.

    9:15:03 @ 5s  → 9:15:00
    9:17:42 @ 5m  → 9:15:00
    9:16:10 @ 90s → 9:15:00   (1m30s bucket)
    9:16:31 @ 90s → 9:16:30
    """
    secs = ts.hour * 3600 + ts.minute * 60 + ts.second
    secs_since_open = max(secs - MARKET_OPEN_SECS, 0)
    bucket_secs = MARKET_OPEN_SECS + (secs_since_open // tf_seconds) * tf_seconds
    return ts.replace(
        hour        = bucket_secs // 3600,
        minute      = (bucket_secs % 3600) // 60,
        second      = bucket_secs % 60,
        microsecond = 0,
    )


class PreviousCandleGuard:
    """
    Ensures the immediately previous closed candle exists continuously.
    Repairs 1m gaps via backfill and re-derives 5m.
    """

    def __init__(self, ohlc, poll_seconds=2.0):
        self.ohlc = ohlc
        self.poll_seconds = poll_seconds
        self._attempted = set()

    def _has_minute(self, symbol, minute_ts):
        candles = self.ohlc.ohlc_1m.get(symbol, [])
        if not candles:
            return False

        target = pd.Timestamp(minute_ts)
        for c in candles:
            ts = pd.Timestamp(c["timestamp"])
            if ts == target:
                return True
        return False


class OHLCCollector:
    """
    OHLCCollector
    ─────────────
    • Builds candles for ALL configured TFs directly from ticks
    • No derivation chain — every TF is updated on every tick
    • TFs set via TIMEFRAMES env var  e.g. "5s,30s,1m,1m30s,5m,15m"
    • Candle buckets are aligned to market open (9:15:00 IST)
    • SINGLE instance enforced via _WRITER_LOCK
    """

    _WRITER_LOCK = False

    def __init__(self, base_dir="ohlcdata", tick_writer=None, history_store=None, conn_log_dir=None):
        if OHLCCollector._WRITER_LOCK:
            raise RuntimeError("[OHLC][FATAL] Multiple OHLCCollector instances detected")
        OHLCCollector._WRITER_LOCK = True

        self.base_dir = base_dir
        # Passed through to BackfillManager so it can cross-check
        # detected gaps against confirmed disconnect windows in
        # connection_log.db (see gap_detector.connection_outage_windows).
        self.conn_log_dir = conn_log_dir

        # ── Load configured timeframes from env ───────────────────────
        tf_env = os.getenv("TIMEFRAMES", "1m,5m")
        self._configured_tfs = [tf.strip() for tf in tf_env.split(",") if tf.strip()]
        self._tf_list = [(tf, _tf_to_seconds(tf)) for tf in self._configured_tfs]

        # ── RAM stores — keyed by timeframe string ────────────────────
        # ohlc_data[tf][symbol] = [ {timestamp, open, high, low, close, volume}, ... ]
        self.ohlc_data = {tf: {} for tf in self._configured_tfs}

        # ── Forming (not-yet-closed) candles per TF ───────────────────
        # current_candles[tf][symbol] = { timestamp, open, high, low, close, volume }
        self.current_candles = {tf: {} for tf in self._configured_tfs}

        # ── RAM retention (rolling window) ─────────────────────────────
        # Raw ticks + closed candles are pruned from RAM (NOT from disk —
        # Parquet keeps full history) once they age out of this window.
        # Per-TF window = max(RAM_WINDOW_MINUTES, MIN_CANDLES * tf_seconds)
        # so higher TFs always retain at least MIN_CANDLES candles, which
        # indicators.py / ADX depend on — a flat 30-min cap alone would
        # starve 5m/15m of enough history.
        self.ram_window_secs = int(os.getenv("RAM_WINDOW_MINUTES", "30")) * 60
        self.min_candles     = int(os.getenv("MIN_CANDLES", "15"))
        self._tf_seconds_map = dict(self._tf_list)

        # Raw Quote ticks, kept only for a dedicated flat window — separate
        # from RAM_WINDOW_MINUTES (candle retention) so tuning one never
        # accidentally shifts the other. No MIN_CANDLES-style floor applies
        # to raw ticks since they're not candles.
        self.tick_ram_window_secs = int(os.getenv("TICK_RAM_WINDOW_MINUTES", "9")) * 60
        # raw_ticks[symbol] = deque of {timestamp, ltp, qty}
        self.raw_ticks = {}

        # ── Backward-compat references ────────────────────────────────
        # executor.py / indicators.py / PreviousCandleGuard use these names directly.
        # They point to the SAME dict objects inside ohlc_data / current_candles
        # so mutations through either name are always in sync.
        self.ohlc_1m    = self.ohlc_data.get("1m", {})
        self.ohlc_5m    = self.ohlc_data.get("5m", {})
        self.current_1m = self.current_candles.get("1m", {})

        self.latest_ltp = {}
        self._ram_lock  = threading.RLock()
        self._ensured_parquet = set()

        self._backfilled     = False
        self.backfill_complete = False
        self.parquet_writer  = ParquetWriter(base_dir=base_dir, tz=tz_kolkata)

        # Local SQLite tick cache (shared with DepthStore, kind='quote'
        # here) — optional; BackfillManager also writes PG catch-up
        # rows through this same writer, so it's read here as
        # `self.tick_writer` (see backfill_manager.py).
        self.tick_writer = tick_writer

        # Local SQLite cache for candles fetched from the history
        # (fallback) DB — optional; BackfillManager reads/writes this
        # as `self.history_store` (see tick_writer.py's HistoryCandleStore).
        self.history_store = history_store

        print(
            f"[OHLC] Initialized | TFs: {self._configured_tfs} | base_dir: {base_dir}",
            flush=True,
        )

    # =====================================================
    # PUBLIC SAVE API  (used by live tick processing + backfill)
    # =====================================================

    def save_candle(self, symbol, timeframe, candle):
        key = (symbol, timeframe)
        with self._ram_lock:
            if key not in self._ensured_parquet:
                self.parquet_writer.ensure_file(symbol, timeframe)
                self._ensured_parquet.add(key)
        self._append_ram(symbol, timeframe, candle)
        self.parquet_writer.enqueue(symbol, timeframe, candle)

    def save_candles_bulk(self, symbol, timeframe, candles_df):
        """
        Same effect as calling save_candle() once per row of
        candles_df, but O(N) instead of O(N^2) on BOTH the RAM side and
        the disk side — this is what BackfillManager._finalize_symbol()
        uses instead of looping save_candle() per historical candle.

        RAM side: builds the merged/deduped series in one pass (a dict
        keyed by timestamp) instead of _append_ram()'s per-call linear
        scan for an existing same-timestamp row — that scan is fine for
        one live tick at a time, but re-scanning the whole growing
        series for every one of a few hundred historical candles adds
        up.

        Disk side: delegates to parquet_writer.enqueue_bulk(), which
        does one read-merge-write for the whole batch instead of one
        per candle — see that method's docstring for why the per-candle
        version was the dominant cost of slow candle building.
        """
        if candles_df is None or candles_df.empty:
            return

        key = (symbol, timeframe)
        with self._ram_lock:
            if key not in self._ensured_parquet:
                self.parquet_writer.ensure_file(symbol, timeframe)
                self._ensured_parquet.add(key)

            store = self.ohlc_data.get(timeframe)
            if store is not None:
                series = store.setdefault(symbol, [])
                by_ts = {pd.Timestamp(r["timestamp"]): r for r in series}
                for row in candles_df.itertuples(index=False):
                    ts = pd.Timestamp(row.timestamp)
                    by_ts[ts] = {
                        "timestamp": ts,
                        "open":      float(row.open),
                        "high":      float(row.high),
                        "low":       float(row.low),
                        "close":     float(row.close),
                        "volume":    float(row.volume),
                    }
                merged = sorted(by_ts.values(), key=lambda r: r["timestamp"])

                tf_seconds = self._tf_seconds_map.get(timeframe)
                if tf_seconds and merged:
                    cutoff = merged[-1]["timestamp"] - timedelta(seconds=self._retention_secs(tf_seconds))
                    merged = [r for r in merged if r["timestamp"] >= cutoff]

                store[symbol] = merged

        self.parquet_writer.enqueue_bulk(symbol, timeframe, candles_df)

    def _retention_secs(self, tf_seconds: int) -> int:
        """max(flat RAM window, enough seconds for MIN_CANDLES) for this TF."""
        return max(self.ram_window_secs, self.min_candles * tf_seconds)

    def _append_ram(self, symbol, timeframe, candle):
        with self._ram_lock:
            store = self.ohlc_data.get(timeframe)
            if store is None:
                return                  # TF not configured — ignore

            row = dict(candle)
            row["timestamp"] = pd.Timestamp(row["timestamp"])
            series = store.setdefault(symbol, [])

            # Upsert: replace existing candle with same timestamp
            replaced = False
            for i, existing in enumerate(series):
                if pd.Timestamp(existing["timestamp"]) == row["timestamp"]:
                    series[i] = row
                    replaced = True
                    break

            if not replaced:
                series.append(row)
                series.sort(key=lambda x: pd.Timestamp(x["timestamp"]))

            # ── Prune RAM to this TF's retention window ─────────────────
            # Disk (Parquet) always keeps full history — this only trims
            # what stays resident in memory.
            tf_seconds = self._tf_seconds_map.get(timeframe)
            if tf_seconds and series:
                cutoff = row["timestamp"] - timedelta(seconds=self._retention_secs(tf_seconds))
                while series and pd.Timestamp(series[0]["timestamp"]) < cutoff:
                    series.pop(0)

    # =====================================================
    # BACKFILL TRIGGER  (called once at startup)
    # =====================================================

    def ensure_backfill(self, symbols):
        """
        Loads historical candles from PostgreSQL before live trading starts.
        Safe to call multiple times — runs only once.
        """
        if self._backfilled:
            return

        from backfill_manager import BackfillManager
        backfill = BackfillManager(self)
        backfill.run(symbols)

        print(
            f"[CANDLE] Candle building complete | {len(symbols)} symbol(s) | "
            f"live updates running",
            flush=True,
        )

        self._backfilled       = True
        self.backfill_complete = True

    async def ensure_backfill_async(self, symbols):
        await asyncio.to_thread(self.ensure_backfill, symbols)

    # =====================================================
    # MAIN ASYNC LOOP
    # =====================================================

    async def run(self):
        print("[OHLC] Collector running", flush=True)
        while True:
            tick = await market_data_queue.get()
            self._process_tick(tick)

    async def monitor_loop(self, symbols, indicators):
        pass

    # =====================================================
    # TICK PROCESSING
    # All configured TFs are updated on every single tick.
    # No derivation chain — each TF is independent.
    # =====================================================

    @staticmethod
    def _normalize_symbol(raw_symbol):
        if raw_symbol is None:
            return None
        s = str(raw_symbol).strip().upper()
        if ":" in s:
            return s.split(":")[-1]
        return s

    def _process_tick(self, tick):
        data       = tick.get("data", {})
        raw_symbol = data.get("symbol", tick.get("symbol"))
        symbol     = self._normalize_symbol(raw_symbol)
        if not symbol:
            return

        if "ltp" not in data:
            return
        ltp = data["ltp"]
        self.latest_ltp[symbol] = ltp

        if "ltt" not in data:
            return
        ts = datetime.fromtimestamp(data["ltt"] / 1000, tz=tz_kolkata)

        # ── Raw tick RAM store — flat rolling window, no MIN_CANDLES rule ──
        with self._ram_lock:
            ticks = self.raw_ticks.setdefault(symbol, deque())
            ticks.append({
                "timestamp": ts,
                "ltp":       ltp,
                "qty":       data.get("last_trade_quantity", 0),
            })
            cutoff = ts - timedelta(seconds=self.tick_ram_window_secs)
            while ticks and ticks[0]["timestamp"] < cutoff:
                ticks.popleft()

        # ── Local SQLite tick cache (kind='quote') ─────────────────────
        if self.tick_writer is not None:
            self.tick_writer.enqueue_live(symbol, "quote", {
                "timestamp": data["ltt"],
                "ltp":       ltp,
                "qty":       data.get("last_trade_quantity", 0),
            })

        # ── Update every configured TF directly from this tick ────────
        for tf_str, tf_seconds in self._tf_list:
            bucket   = _compute_bucket(ts, tf_seconds)
            tf_store = self.current_candles[tf_str]
            current  = tf_store.get(symbol)

            if current is not None and current["timestamp"] != bucket:
                # Current candle just closed — save it and reset
                self.save_candle(symbol, tf_str, current)
                tf_store[symbol] = None
                current = None

            if current is None:
                # Open a new forming candle
                tf_store[symbol] = {
                    "timestamp": bucket,
                    "open":      ltp,
                    "high":      ltp,
                    "low":       ltp,
                    "close":     ltp,
                    "volume":    data.get("last_trade_quantity", 0),
                }
            else:
                # Update the existing forming candle
                current["high"]    = max(current["high"], ltp)
                current["low"]     = min(current["low"],  ltp)
                current["close"]   = ltp
                current["volume"] += data.get("last_trade_quantity", 0)

    # =====================================================
    # SHUTDOWN
    # =====================================================

    def shutdown(self):
        self.parquet_writer.shutdown()

    # =====================================================
    # READ ACCESS  (for other in-process modules)
    # =====================================================

    def get_recent_ticks(self, symbol, seconds=None):
        """
        Return buffered ticks for `symbol` as a list of
        {"timestamp", "ltp", "qty"} dicts, oldest first.

        Defaults to the full retained window (TICK_RAM_WINDOW_MINUTES,
        9 minutes by default). Pass `seconds` for a narrower slice of
        that same buffer — it can't exceed what's actually retained.
        """
        with self._ram_lock:
            ticks = self.raw_ticks.get(symbol)
            if not ticks:
                return []
            if seconds is None:
                return list(ticks)
            cutoff = ticks[-1]["timestamp"] - timedelta(seconds=seconds)
            return [t for t in ticks if t["timestamp"] >= cutoff]
