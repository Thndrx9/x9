import os
import queue
import threading
import pandas as pd
import pyarrow.parquet as pq


class ParquetWriter:
    """
    Dedicated single-writer service for parquet.
    All parquet writes must flow through this class.
    """

    def __init__(self, base_dir, tz):
        self.base_dir = base_dir
        self.tz = tz
        self._queue = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="parquet-writer", daemon=True)
        self._thread.start()

    def enqueue(self, symbol, timeframe, candle):
        self._queue.put((symbol, timeframe, dict(candle)))

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
                break
            try:
                symbol, timeframe, candle = self._queue.get(timeout=0.25)
            except queue.Empty:
                continue
            try:
                self._write_one(symbol, timeframe, candle)
            finally:
                self._queue.task_done()

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
            try:
                pf = pq.ParquetFile(path)
                rows = []
                for i in range(pf.num_row_groups):
                    table = pf.read_row_group(
                        i,
                        columns=["timestamp", "open", "high", "low", "close", "volume"],
                    )
                    rows.extend(table.to_pylist())
                return pd.DataFrame(rows) if rows else self._empty_frame()
            except Exception:
                return self._empty_frame()

    def _ensure_parquet_file(self, path):
        if os.path.exists(path):
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
        self._empty_frame().to_parquet(tmp, engine="pyarrow", index=False)
        os.replace(tmp, path)

    def _write_one(self, symbol, timeframe, candle):
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
