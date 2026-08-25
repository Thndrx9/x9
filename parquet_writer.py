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

    OHLC candles only — quote/depth ticks are written separately by
    TickWriter (SQLite-backed, see tick_writer.py), since their
    write pattern (frequent inserts) doesn't suit Parquet's
    read-whole-file/rewrite-whole-file append model the way low-frequency
    candle writes do.
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

    def enqueue_bulk(self, symbol, timeframe, candles_df):
        """
        Queues an ENTIRE batch of candles (a DataFrame with timestamp/
        open/high/low/close/volume columns) as ONE item, written with a
        single read-existing + merge + write cycle — not one per
        candle. This is the path backfill's _finalize_symbol() uses.

        Without this, saving N historical candles for one symbol/
        timeframe meant N separate _write_one() calls, and EVERY one of
        those reads the WHOLE existing file and rewrites the WHOLE
        file — so writing N candles cost O(1+2+...+N) = O(N^2) file
        I/O instead of O(N). For a real backfill (hundreds of candles
        per symbol, ~200 symbols, 2 timeframes), that quadratic blowup
        was the dominant cost of "candle building" being slow — not
        the pandas aggregation math itself. One bulk write per symbol/
        timeframe instead makes this genuinely O(N): one read, one
        merge, one write, no matter how many candles are in the batch.
        """
        if candles_df is None or candles_df.empty:
            return
        self._queue.put((symbol, timeframe, candles_df))

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
                if isinstance(candle, pd.DataFrame):
                    self._write_bulk(symbol, timeframe, candle)
                else:
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

    def _write_bulk(self, symbol, timeframe, candles_df):
        """
        Same merge/sort/dedupe/atomic-write logic as _write_one(), but
        ONE read of the existing file and ONE write, for the WHOLE
        batch — see enqueue_bulk()'s docstring for why this matters.
        """
        path = os.path.join(self.base_dir, symbol, f"{timeframe}.parquet")
        self._ensure_parquet_file(path)

        required = ["timestamp", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in candles_df.columns]
        if missing:
            return

        df_new = candles_df[required].copy()
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
