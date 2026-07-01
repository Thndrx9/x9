# ohlc.py

import os
import re
import pandas as pd
from datetime import datetime
from market_time import tz_kolkata, MARKET_OPEN
from event_bus import market_data_queue
import asyncio
import threading
from parquet_writer import ParquetWriter

# 9:15:00 in seconds from midnight
MARKET_OPEN_SECS = MARKET_OPEN.hour * 3600 + MARKET_OPEN.minute * 60  # 33300


# ─────────────────────────────────────────────
# TF utilities  (also mirrored in ohlc_backfill.py)
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

    def __init__(self, base_dir="ohlcdata"):
        if OHLCCollector._WRITER_LOCK:
            raise RuntimeError("[OHLC][FATAL] Multiple OHLCCollector instances detected")
        OHLCCollector._WRITER_LOCK = True

        self.base_dir = base_dir

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

        from ohlc_backfill import OHLCBackfill
        backfill = OHLCBackfill(self)
        backfill.run(symbols)

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






        


        
