# depth_writer.py

import os
import time
import json
import queue
import sqlite3
import threading


class DepthWriter:
    """
    SQLite-backed depth snapshot writer — dedicated background thread,
    one DB file per base_dir, single table indexed by (symbol, ts_ms).

    Why SQLite instead of Parquet for depth (same reasoning as
    connection_log.py): depth ticks arrive far more often than OHLC
    candles, and Parquet's append model is read-whole-file ->
    rewrite-whole-file — fine for ~1 write/min/symbol, not for
    potentially several writes/sec/symbol. SQLite's INSERT is O(1)
    regardless of table size, so no buffering/batching workaround is
    needed the way it was for the Parquet-based attempt.

    bid/ask levels are stored as a JSON blob (raw_json) rather than
    fixed flattened columns — OpenAlgo's exact Depth payload shape
    wasn't confirmed when this was written, and JSON storage needs no
    schema change if the level count or field names shift.
    """

    DB_FILENAME    = "depth.db"
    BATCH_SIZE     = 100    # rows before a forced commit
    BATCH_MAX_SECS = 1.0    # max staleness before a forced commit

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._queue   = queue.Queue()
        self._stop    = threading.Event()
        self._thread  = threading.Thread(target=self._run, name="depth-writer", daemon=True)
        self._thread.start()

    def enqueue(self, symbol: str, snapshot: dict):
        self._queue.put((symbol, dict(snapshot)))

    def shutdown(self, timeout=10):
        self._stop.set()
        self._thread.join(timeout=timeout)

    # ─────────────────────────────────────────────

    def _db_path(self):
        return os.path.join(self.base_dir, self.DB_FILENAME)

    def _connect(self):
        path = self._db_path()
        os.makedirs(self.base_dir, exist_ok=True)
        conn = sqlite3.connect(path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS depth_ticks (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol   TEXT    NOT NULL,
                ts_ms    INTEGER NOT NULL,
                ltp      REAL,
                raw_json TEXT    NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_depth_symbol_ts ON depth_ticks(symbol, ts_ms)"
        )
        conn.commit()
        return conn

    def _run(self):
        conn = self._connect()
        batch = []
        last_commit = time.time()
        try:
            while True:
                stopping = self._stop.is_set() and self._queue.empty()

                if not batch and stopping:
                    break

                try:
                    item = self._queue.get(timeout=0.25)
                    batch.append(item)
                except queue.Empty:
                    pass

                should_flush = batch and (
                    len(batch) >= self.BATCH_SIZE
                    or time.time() - last_commit >= self.BATCH_MAX_SECS
                    or stopping
                )
                if should_flush:
                    self._flush(conn, batch)
                    batch = []
                    last_commit = time.time()
        finally:
            conn.close()

    def _flush(self, conn, batch):
        rows = []
        for symbol, snapshot in batch:
            ts_ms = snapshot.get("timestamp")
            if ts_ms is None:
                continue
            rows.append((
                symbol,
                int(ts_ms),
                snapshot.get("ltp"),
                json.dumps({
                    "bids": snapshot.get("bids", []),
                    "asks": snapshot.get("asks", []),
                }),
            ))

        if not rows:
            return

        try:
            conn.executemany(
                "INSERT INTO depth_ticks (symbol, ts_ms, ltp, raw_json) VALUES (?, ?, ?, ?)",
                rows,
            )
            conn.commit()
        except Exception as exc:
            print(f"[DEPTH_WRITER][WARN] batch insert failed: {exc}", flush=True)