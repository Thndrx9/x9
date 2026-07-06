import os
import time
import queue
import threading
import pandas as pd
import pyarrow.parquet as pq


class ParquetWriter:
    """
    Dedicated single-writer service for parquet.
    All parquet writes must flow through this class.

    Handles two kinds of writes on the SAME background thread:
      • OHLC candles  (enqueue)        — one row per closed candle, low
                                          frequency (~1/min/symbol/TF)
      • Depth snapshots (enqueue_depth) — much higher frequency, so these
                                          are buffered (DEPTH_BUFFER_LIMIT
                                          rows or DEPTH_FLUSH_SECS,
                                          whichever comes first) instead
                                          of rewriting the whole file on
                                          every single tick.
    """

    _DEPTH_MARKER      = "__depth__"
    DEPTH_BUFFER_LIMIT = 50     # rows per symbol before a forced flush
    DEPTH_FLUSH_SECS   = 5.0    # max staleness before a forced flush

    def __init__(self, base_dir, tz):
        self.base_dir = base_dir
        self.tz = tz
        self._queue = queue.Queue()
        self._stop = threading.Event()
        self._depth_buffers = {}          # symbol -> list[row dict]
        self._last_depth_flush = time.time()
        self._thread = threading.Thread(target=self._run, name="parquet-writer", daemon=True)
        self._thread.start()

    def enqueue(self, symbol, timeframe, candle):
        self._queue.put((symbol, timeframe, dict(candle)))

    def enqueue_depth(self, symbol, snapshot):
        """Write a depth snapshot through the same background thread as OHLC candles."""
        self._queue.put((symbol, self._DEPTH_MARKER, dict(snapshot)))

    def ensure_file(self, symbol, timeframe):
        """
        Ensure parquet file exists for symbol/timeframe.
        Creation also goes through writer thread.
        """
        self._queue.put((symbol, timeframe, None))

    def shutdown(self, timeout=10):
        self._stop.set()
        self._thread.join(timeout=timeout)

    def _run(self):
        while True:
            if self._stop.is_set() and self._queue.empty():
                self._flush_all_depth_buffers()
                break
            try:
                symbol, timeframe, candle = self._queue.get(timeout=0.25)
            except queue.Empty:
                self._maybe_flush_stale_depth_buffers()
                continue
            try:
                self._write_one(symbol, timeframe, candle)
            finally:
                self._queue.task_done()
            self._maybe_flush_stale_depth_buffers()

    def _maybe_flush_stale_depth_buffers(self):
        if time.time() - self._last_depth_flush >= self.DEPTH_FLUSH_SECS:
            self._flush_all_depth_buffers()

    def _flush_all_depth_buffers(self):
        for symbol in list(self._depth_buffers.keys()):
            self._flush_depth_buffer(symbol)
        self._last_depth_flush = time.time()

    @staticmethod
    def _empty_frame():
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    def _normalize_to_ist(self, series):
        """
        Keep wall-clock correctness:
        - naive timestamps are assumed IST and localized
        - tz-aware timestamps are converted to IST
        """
        def _one(v):
            try:
                ts = pd.Timestamp(v)
            except Exception:
                return pd.NaT
            if ts.tzinfo is None:
                try:
                    return ts.tz_localize(self.tz)
                except Exception:
                    return pd.NaT
            try:
                return ts.tz_convert(self.tz)
            except Exception:
                return pd.NaT

        out = series.apply(_one)
        return out

    def _read_existing(self, path):
        try:
            return pd.read_parquet(path)
        except Exception:
            # Fallback path for broken parquet pandas metadata.
            # Reads whatever columns actually exist — needed for both
            # OHLC (fixed columns) and depth (variable columns) files.
            try:
                pf = pq.ParquetFile(path)
                rows = []
                for i in range(pf.num_row_groups):
                    table = pf.read_row_group(i)
                    rows.extend(table.to_pylist())
                return pd.DataFrame(rows) if rows else pd.DataFrame()
            except Exception:
                return pd.DataFrame()

    def _ensure_parquet_file(self, path):
        if os.path.exists(path):
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
        self._empty_frame().to_parquet(tmp, engine="pyarrow", index=False)
        os.replace(tmp, path)

    def _write_one(self, symbol, timeframe, candle):
        if timeframe == self._DEPTH_MARKER:
            self._write_depth_one(symbol, candle)
            return

        path = os.path.join(self.base_dir, symbol, f"{timeframe}.parquet")
        self._ensure_parquet_file(path)

        if candle is None:
            return

        df_new = pd.DataFrame([candle])
        if df_new.empty:
            return

        required = ["timestamp", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df_new.columns]
        if missing:
            return

        df_new = df_new[required].copy()
        df_new["timestamp"] = self._normalize_to_ist(df_new["timestamp"])
        df_new = df_new.dropna(subset=["timestamp"])
        if df_new.empty:
            return

        if os.path.exists(path):
            df_old = self._read_existing(path)
            if not df_old.empty and "timestamp" in df_old.columns:
                df_old = df_old[required].copy()
                df_old["timestamp"] = self._normalize_to_ist(df_old["timestamp"])
                df_old = df_old.dropna(subset=["timestamp"])
                if not df_old.empty:
                    df = pd.concat([df_old, df_new], ignore_index=True)
                else:
                    df = df_new
            else:
                df = df_new
        else:
            df = df_new

        df = df.sort_values("timestamp")
        df = df.drop_duplicates(subset=["timestamp"], keep="last")

        tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
        df.to_parquet(tmp, engine="pyarrow", index=False)
        os.replace(tmp, path)
    # =====================================================
    # DEPTH WRITE PATH
    # =====================================================
    # Depth ticks arrive far more often than OHLC candles, so instead of
    # the read-whole-file/rewrite-whole-file pattern above running on
    # every single tick, snapshots are buffered per symbol and flushed
    # in batches (DEPTH_BUFFER_LIMIT rows, or DEPTH_FLUSH_SECS elapsed —
    # whichever comes first, both driven from _run()).

    def _flatten_depth_row(self, snapshot):
        row = {
            "timestamp": snapshot.get("timestamp"),
            "ltp":       snapshot.get("ltp"),
        }
        for i, level in enumerate(snapshot.get("bids") or [], start=1):
            if isinstance(level, dict):
                row[f"bid_{i}_price"] = level.get("price")
                row[f"bid_{i}_qty"]   = level.get("qty")
        for i, level in enumerate(snapshot.get("asks") or [], start=1):
            if isinstance(level, dict):
                row[f"ask_{i}_price"] = level.get("price")
                row[f"ask_{i}_qty"]   = level.get("qty")
        return row

    def _ensure_depth_file(self, path):
        if os.path.exists(path):
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
        pd.DataFrame(columns=["timestamp", "ltp"]).to_parquet(tmp, engine="pyarrow", index=False)
        os.replace(tmp, path)

    def _write_depth_one(self, symbol, snapshot):
        path = os.path.join(self.base_dir, symbol, "depth.parquet")
        self._ensure_depth_file(path)

        if snapshot is None:
            return  # this call was only an "ensure file exists" ping

        row = self._flatten_depth_row(snapshot)
        if row.get("timestamp") is None:
            return

        buf = self._depth_buffers.setdefault(symbol, [])
        buf.append(row)

        if len(buf) >= self.DEPTH_BUFFER_LIMIT:
            self._flush_depth_buffer(symbol)

    def _flush_depth_buffer(self, symbol):
        buf = self._depth_buffers.get(symbol)
        if not buf:
            return

        # Clear immediately — any snapshot arriving while we write below
        # starts a fresh buffer rather than being dropped or double-written.
        self._depth_buffers[symbol] = []

        path = os.path.join(self.base_dir, symbol, "depth.parquet")
        self._ensure_depth_file(path)

        df_new = pd.DataFrame(buf)
        df_new["timestamp"] = (
            pd.to_datetime(df_new["timestamp"], unit="ms", utc=True, errors="coerce")
            .dt.tz_convert(self.tz)
        )
        df_new = df_new.dropna(subset=["timestamp"])
        if df_new.empty:
            return

        if os.path.exists(path):
            df_old = self._read_existing(path)
            if not df_old.empty and "timestamp" in df_old.columns:
                df_old["timestamp"] = self._normalize_to_ist(df_old["timestamp"])
                df_old = df_old.dropna(subset=["timestamp"])
                df = pd.concat([df_old, df_new], ignore_index=True) if not df_old.empty else df_new
            else:
                df = df_new
        else:
            df = df_new

        df = df.sort_values("timestamp")
        df = df.drop_duplicates(subset=["timestamp"], keep="last")

        tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
        df.to_parquet(tmp, engine="pyarrow", index=False)
        os.replace(tmp, path)