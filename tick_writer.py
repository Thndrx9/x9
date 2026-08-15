# tick_writer.py
#
# Replaces depth_writer.py. Same dedicated-background-thread /
# batched-commit SQLite design, but now backs BOTH quote ticks and
# depth snapshots in a single `ticks` table (distinguished by `kind`),
# in a single ticks.db file — instead of a depth-only DB.
#
# ── Ordering guarantee ──────────────────────────────────────────────
# Two different producers feed this writer:
#   1. Historical "catch-up" ticks fetched from PostgreSQL in bulk
#      (already ascending-timestamp, per symbol) — via enqueue_backfill_rows().
#   2. Live ticks arriving one at a time off the websocket, in real
#      time — via enqueue_live().
#
# A PG fetch takes real wall-clock time (network round trip). If live
# ticks were allowed to enqueue directly while a catch-up fetch is in
# flight, they'd land in the write queue BEFORE the (earlier-timestamp)
# historical rows the fetch eventually returns — rows on disk would go
# out of timestamp order. hold()/release() close that gap: while held,
# enqueue_live() buffers in RAM instead of touching the queue; release()
# drains that buffer onto the queue (in arrival order, which is also
# timestamp order) immediately after the caller's catch-up fetch has
# been enqueued via enqueue_backfill_rows(). Net effect: PG rows land
# on disk first, buffered live ticks next, future live ticks after —
# row after row, timestamp after timestamp, never interleaved.

import os
import time
import json
import queue
import sqlite3
import threading
from datetime import datetime, timedelta

from market_time import tz_kolkata, trading_day_n_back


class TickWriter:
    """
    SQLite-backed tick writer — dedicated background thread, one DB
    file per base_dir, single `ticks` table for both quote ticks
    (kind='quote': ltp, qty) and depth snapshots (kind='depth': ltp,
    raw_json bid/ask blob).
    """

    DB_FILENAME    = "ticks.db"
    BATCH_SIZE     = 100    # rows before a forced commit
    BATCH_MAX_SECS = 1.0    # max staleness before a forced commit

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._queue   = queue.Queue()
        self._stop    = threading.Event()

        # ── hold/release gate (see module docstring) ───────────────
        self._gate_lock   = threading.Lock()
        self._held        = False
        self._held_buffer = []   # [(symbol, kind, snapshot), ...] arrival order

        self._thread = threading.Thread(target=self._run, name="tick-writer", daemon=True)
        self._thread.start()

    # ─────────────────────────────────────────────
    # Live tick ingestion (websocket path)
    # ─────────────────────────────────────────────

    def enqueue_live(self, symbol: str, kind: str, snapshot: dict):
        """kind: 'quote' or 'depth'. Buffers in RAM instead of writing
        while the gate is held (see hold())."""
        item = (symbol, kind, dict(snapshot))
        with self._gate_lock:
            if self._held:
                self._held_buffer.append(item)
                return
        self._queue.put(("live", *item))

    # Back-compat alias — depth_store.py used to call this on DepthWriter.
    def enqueue(self, symbol: str, snapshot: dict):
        self.enqueue_live(symbol, "depth", snapshot)

    # ─────────────────────────────────────────────
    # Bulk catch-up ingestion (PG-fetched historical ticks)
    # ─────────────────────────────────────────────

    def enqueue_backfill_rows(self, symbol: str, rows: list):
        """
        rows: list of {"timestamp": ms_int, "ltp": float, "qty": float},
        already ascending by timestamp (as returned by BackfillManager's
        PG fetch). Written as kind='quote'. Queued as a single unit so
        it flushes as one contiguous ordered block ahead of anything
        enqueued after it — see module docstring.
        """
        if not rows:
            return
        self._queue.put(("bulk", symbol, list(rows)))

    # ─────────────────────────────────────────────
    # Hold/release gate
    # ─────────────────────────────────────────────

    def hold(self):
        """Start buffering live ticks in RAM instead of writing them."""
        with self._gate_lock:
            self._held = True

    def release(self):
        """
        Stop buffering; drain whatever accumulated while held onto the
        write queue (in arrival/timestamp order), then resume writing
        live ticks straight through. Call this AFTER the corresponding
        catch-up enqueue_backfill_rows() call so the buffered ticks
        land after the historical rows they followed in real time.
        """
        with self._gate_lock:
            buffered = self._held_buffer
            self._held_buffer = []
            self._held = False
        for symbol, kind, snapshot in buffered:
            self._queue.put(("live", symbol, kind, snapshot))

    def shutdown(self, timeout=10):
        self._stop.set()
        self._thread.join(timeout=timeout)

    # ─────────────────────────────────────────────
    # Reads (used by BackfillManager to know where to resume a catch-up fetch)
    # ─────────────────────────────────────────────

    def max_ts(self, symbol: str):
        """Most recent ts_ms already cached for symbol, or None. Opens
        its own short-lived read connection — infrequent, off the hot
        path, so no need to share the writer thread's connection."""
        path = self._db_path()
        if not os.path.exists(path):
            return None
        try:
            conn = sqlite3.connect(path, timeout=5)
            try:
                row = conn.execute(
                    "SELECT MAX(ts_ms) FROM ticks WHERE symbol = ?", (symbol,)
                ).fetchone()
            finally:
                conn.close()
            return int(row[0]) if row and row[0] is not None else None
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] max_ts({symbol}) failed: {exc}", flush=True)
            return None

    # ─────────────────────────────────────────────
    # Retention — keep only the last N trading days
    # ─────────────────────────────────────────────

    def prune_older_than_days(self, n_trading_days: int = 3):
        """
        Delete every row whose ts_ms falls before the start (00:00 IST)
        of the n_trading_days-th most recent trading day — i.e. keep
        exactly the last n_trading_days trading days' worth of ticks.
        Meant to be called at market close / on shutdown, not on the
        hot write path.
        """
        cutoff_date = trading_day_n_back(n_trading_days - 1)
        cutoff_dt   = datetime.combine(cutoff_date, datetime.min.time(), tzinfo=tz_kolkata)
        cutoff_ms   = int(cutoff_dt.timestamp() * 1000)

        path = self._db_path()
        if not os.path.exists(path):
            return
        try:
            conn = sqlite3.connect(path, timeout=5)
            try:
                cur = conn.execute("DELETE FROM ticks WHERE ts_ms < ?", (cutoff_ms,))
                conn.commit()
                print(
                    f"[TICK_WRITER] Pruned {cur.rowcount} row(s) older than "
                    f"{cutoff_date.isoformat()} (keeping last {n_trading_days} trading days)",
                    flush=True,
                )
            finally:
                conn.close()
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] prune failed: {exc}", flush=True)

    # ─────────────────────────────────────────────
    # Internals
    # ─────────────────────────────────────────────

    def _db_path(self):
        return os.path.join(self.base_dir, self.DB_FILENAME)

    def _connect(self):
        path = self._db_path()
        os.makedirs(self.base_dir, exist_ok=True)
        conn = sqlite3.connect(path, timeout=5)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol   TEXT    NOT NULL,
                ts_ms    INTEGER NOT NULL,
                kind     TEXT    NOT NULL,
                ltp      REAL,
                qty      REAL,
                raw_json TEXT
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts_ms)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ticks_ts ON ticks(ts_ms)"
        )
        conn.commit()
        return conn

    def _run(self):
        conn = self._connect()
        batch = []  # list of ("live", symbol, kind, snapshot) items pending flush
        last_commit = time.time()
        try:
            while True:
                stopping = self._stop.is_set() and self._queue.empty()

                if not batch and stopping:
                    break

                try:
                    item = self._queue.get(timeout=0.25)
                except queue.Empty:
                    item = None

                if item is not None and item[0] == "bulk":
                    # Flush whatever live rows are pending first, so the
                    # bulk block lands in the right relative position —
                    # then insert the bulk rows as one contiguous,
                    # already-ordered block.
                    if batch:
                        self._flush_live(conn, batch)
                        batch = []
                        last_commit = time.time()
                    _, symbol, rows = item
                    self._flush_bulk(conn, symbol, rows)
                    last_commit = time.time()
                elif item is not None:
                    batch.append(item)

                should_flush = batch and (
                    len(batch) >= self.BATCH_SIZE
                    or time.time() - last_commit >= self.BATCH_MAX_SECS
                    or stopping
                )
                if should_flush:
                    self._flush_live(conn, batch)
                    batch = []
                    last_commit = time.time()
        finally:
            conn.close()

    def _flush_live(self, conn, batch):
        rows = []
        for _, symbol, kind, snapshot in batch:
            ts_ms = snapshot.get("timestamp")
            if ts_ms is None:
                continue
            if kind == "depth":
                rows.append((
                    symbol, int(ts_ms), "depth", snapshot.get("ltp"), None,
                    json.dumps({
                        "bids": snapshot.get("bids", []),
                        "asks": snapshot.get("asks", []),
                    }),
                ))
            else:
                rows.append((
                    symbol, int(ts_ms), "quote",
                    snapshot.get("ltp"), snapshot.get("qty"), None,
                ))

        self._insert(conn, rows)

    def _flush_bulk(self, conn, symbol, rows_in):
        rows = []
        for r in rows_in:
            ts_ms = r.get("timestamp")
            if ts_ms is None:
                continue
            rows.append((symbol, int(ts_ms), "quote", r.get("ltp"), r.get("qty"), None))
        self._insert(conn, rows)

    def _insert(self, conn, rows):
        if not rows:
            return
        try:
            conn.executemany(
                "INSERT INTO ticks (symbol, ts_ms, kind, ltp, qty, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] batch insert failed: {exc}", flush=True)


# Back-compat alias — old code importing DepthWriter from depth_writer.py
# should now import TickWriter from tick_writer.py instead.
DepthWriter = TickWriter
