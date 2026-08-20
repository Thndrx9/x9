# tick_writer.py
#
# Replaces depth_writer.py. Same dedicated-background-thread /
# batched-commit SQLite design, but now backs BOTH quote ticks and
# depth snapshots — each symbol getting its own two tables,
# quote_<symbol> and depth_<symbol>, inside a single ticks.db file.
# This mirrors the quote_{symbol} table convention backfill_manager.py
# already uses against the main PostgreSQL db, instead of the old
# single shared `ticks` table with symbol/kind columns.
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


def _safe_symbol(symbol: str) -> str:
    """Same sanitization backfill_manager.py uses for quote_{symbol}
    PG table names, so the SQLite table names line up with it."""
    return "".join(c for c in str(symbol) if c.isalnum() or c == "_").lower()


class TickWriter:
    """
    SQLite-backed tick writer — dedicated background thread, one DB
    file per base_dir. Two tables PER SYMBOL:
        quote_<symbol>  (ts_ms, ltp, qty)
        depth_<symbol>  (ts_ms, ltp, raw_json)
    instead of one shared `ticks` table for every symbol/kind.
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

        # tracks which quote_<symbol>/depth_<symbol> tables already
        # exist, so the writer thread doesn't re-run CREATE TABLE IF
        # NOT EXISTS on every single insert
        self._known_tables = set()

        self._thread = threading.Thread(target=self._run, name="tick-writer", daemon=True)
        self._thread.start()

    # ─────────────────────────────────────────────
    # Table naming
    # ─────────────────────────────────────────────

    @staticmethod
    def table_name(symbol: str, kind: str) -> str:
        """kind: 'quote' or 'depth' -> e.g. 'quote_reliance', 'depth_reliance'."""
        return f"{kind}_{_safe_symbol(symbol)}"

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
        PG fetch). Written into quote_<symbol>. Queued as a single unit
        so it flushes as one contiguous ordered block ahead of anything
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

    def shutdown(self, timeout=60):
        """
        timeout: how long to wait for the writer thread to drain its
        queue and exit. Default is generous (rather than the old 10s)
        because a prune_older_than_days() call immediately beforehand
        may still be running in the writer thread — see
        prune_older_than_days()'s docstring. If the timeout is
        exceeded, the (daemon) thread is left to finish or be killed
        with the process; no data corruption results either way since
        prune commits happen in bounded batches, not one giant
        transaction.
        """
        self._stop.set()
        self._thread.join(timeout=timeout)

    # ─────────────────────────────────────────────
    # Reads (used by BackfillManager to know where to resume a catch-up fetch)
    # ─────────────────────────────────────────────

    def max_ts(self, symbol: str, kind: str = "quote"):
        """Most recent ts_ms already cached for symbol in quote_<symbol>
        (or depth_<symbol> if kind='depth'), or None. Opens its own
        short-lived read connection — infrequent, off the hot path, so
        no need to share the writer thread's connection."""
        path = self._db_path()
        if not os.path.exists(path):
            return None
        table = self.table_name(symbol, kind)
        try:
            conn = sqlite3.connect(path, timeout=5)
            try:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone()
                if not exists:
                    return None
                row = conn.execute(f"SELECT MAX(ts_ms) FROM {table}").fetchone()
            finally:
                conn.close()
            return int(row[0]) if row and row[0] is not None else None
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] max_ts({symbol}) failed: {exc}", flush=True)
            return None

    def read_ticks(self, symbol: str, start_ms: int = None, end_ms: int = None, kind: str = "quote"):
        """
        Read back cached rows for symbol from quote_<symbol> (or
        depth_<symbol> if kind='depth'), optionally bounded by
        start_ms/end_ms (either or both may be omitted), ascending by
        ts_ms. Used by BackfillManager to reuse ticks already cached
        locally instead of re-fetching them from the main db — see
        backfill_manager.py's Phase 1.

        Returns a list of dicts: {"timestamp", "ltp", "qty"} for
        kind='quote', or {"timestamp", "ltp", "raw_json"} for
        kind='depth'. Empty list if the table doesn't exist or nothing
        matches. Opens its own short-lived read connection — not on
        the hot write path.
        """
        path = self._db_path()
        if not os.path.exists(path):
            return []
        table = self.table_name(symbol, kind)
        col = "raw_json" if kind == "depth" else "qty"

        clauses, params = [], []
        if start_ms is not None:
            clauses.append("ts_ms >= ?")
            params.append(int(start_ms))
        if end_ms is not None:
            clauses.append("ts_ms <= ?")
            params.append(int(end_ms))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""

        try:
            conn = sqlite3.connect(path, timeout=5)
            try:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone()
                if not exists:
                    return []
                rows = conn.execute(
                    f"SELECT ts_ms, ltp, {col} FROM {table}{where} ORDER BY ts_ms",
                    params,
                ).fetchall()
            finally:
                conn.close()
            return [{"timestamp": r[0], "ltp": r[1], col: r[2]} for r in rows]
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] read_ticks({symbol}) failed: {exc}", flush=True)
            return []

    def delete_range(self, symbol: str, start_ms: int, end_ms: int = None, kind: str = "quote", timeout: float = 30):
        """
        Delete cached rows for symbol from quote_<symbol> (or
        depth_<symbol>) where ts_ms >= start_ms (and <= end_ms if
        given). Used by BackfillManager to purge a range of cached
        ticks it no longer trusts (e.g. a detected silent gap) before
        re-fetching that range from the main db — without this, the
        re-fetch would leave duplicate rows sitting next to the
        untrusted ones instead of replacing them.

        Routed through the writer thread's own connection (same
        pattern as prune_older_than_days) so it never fights the
        writer thread for SQLite's single-writer lock.
        """
        if not self._thread.is_alive():
            conn = sqlite3.connect(self._db_path(), timeout=timeout) if os.path.exists(self._db_path()) else None
            if conn is None:
                return
            try:
                self._delete_range(conn, symbol, start_ms, end_ms, kind)
            finally:
                conn.close()
            return

        done = threading.Event()
        self._queue.put(("delete_range", symbol, start_ms, end_ms, kind, done))
        if not done.wait(timeout=timeout):
            print(
                f"[TICK_WRITER][WARN] delete_range({symbol}) timed out after "
                f"{timeout}s waiting for writer thread",
                flush=True,
            )

    def _delete_range(self, conn, symbol: str, start_ms: int, end_ms, kind: str):
        table = self.table_name(symbol, kind)
        try:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if not exists:
                return
            clauses = ["ts_ms >= ?"]
            params = [int(start_ms)]
            if end_ms is not None:
                clauses.append("ts_ms <= ?")
                params.append(int(end_ms))
            cur = conn.execute(f"DELETE FROM {table} WHERE {' AND '.join(clauses)}", params)
            conn.commit()
            if cur.rowcount:
                print(
                    f"[TICK_WRITER] Deleted {cur.rowcount} untrusted row(s) from "
                    f"{table} (ts_ms >= {start_ms})",
                    flush=True,
                )
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] delete_range({table}) failed: {exc}", flush=True)

    # ─────────────────────────────────────────────
    # Retention — keep only the last N trading days
    # ─────────────────────────────────────────────

    # Prune can genuinely take a while with ~200 symbols × 2 tables each
    # if ticks.db has accumulated multiple trading days of raw tick data
    # — one DELETE per table, inside a single background-thread
    # connection. Default timeout is generous and overridable via env
    # var for slower disks / larger symbol universes.
    DEFAULT_PRUNE_TIMEOUT_SECS = float(os.getenv("TICK_WRITER_PRUNE_TIMEOUT_SECS", "180"))
    # How many per-symbol tables to DELETE from before committing —
    # keeps any single transaction bounded instead of one giant
    # transaction across all ~400 tables, and gives visible progress.
    PRUNE_COMMIT_EVERY = 25

    def prune_older_than_days(self, n_trading_days: int = 3, timeout: float = None):
        """
        Delete every row whose ts_ms falls before the start (00:00 IST)
        of the n_trading_days-th most recent trading day — i.e. keep
        exactly the last n_trading_days trading days' worth of ticks.
        Applies to every quote_<symbol> and depth_<symbol> table.
        Meant to be called at market close / on shutdown, not on the
        hot write path.

        Runs INSIDE the writer thread's own connection (via the queue)
        rather than opening a second connection, so it never fights the
        writer thread for SQLite's single-writer lock.

        `timeout` bounds how long THIS CALL waits for the writer thread
        to finish — it does not cancel the prune itself. If it times
        out, the writer thread keeps pruning in the background and
        logs its own completion line when done; only the caller stops
        waiting. Defaults to DEFAULT_PRUNE_TIMEOUT_SECS (env-tunable)
        since with ~400 per-symbol tables this can take a while.
        """
        if timeout is None:
            timeout = self.DEFAULT_PRUNE_TIMEOUT_SECS

        if not self._thread.is_alive():
            # Writer thread already stopped (e.g. called after shutdown()) —
            # safe to use our own connection since nothing else is writing.
            conn = sqlite3.connect(self._db_path(), timeout=timeout) if os.path.exists(self._db_path()) else None
            if conn is None:
                return
            try:
                self._prune_tables(conn, n_trading_days)
            finally:
                conn.close()
            return

        done = threading.Event()
        self._queue.put(("prune", n_trading_days, done))
        if not done.wait(timeout=timeout):
            print(
                f"[TICK_WRITER][WARN] prune still running after {timeout}s — "
                f"giving up waiting, but it continues in the background and "
                f"will log its own completion line when done",
                flush=True,
            )

    def _prune_tables(self, conn, n_trading_days: int):
        """Shared prune logic — always called with a connection that is
        the ONLY writer touching the DB at that moment (either the
        writer thread's own connection, or a fresh one after the
        thread has stopped)."""
        cutoff_date = trading_day_n_back(n_trading_days - 1)
        cutoff_dt   = datetime.combine(cutoff_date, datetime.min.time(), tzinfo=tz_kolkata)
        cutoff_ms   = int(cutoff_dt.timestamp() * 1000)

        start = time.time()
        try:
            tables = [
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND (name LIKE 'quote_%' OR name LIKE 'depth_%')"
                ).fetchall()
            ]
            total_pruned = 0
            for i, table in enumerate(tables, start=1):
                cur = conn.execute(f"DELETE FROM {table} WHERE ts_ms < ?", (cutoff_ms,))
                total_pruned += cur.rowcount

                if i % self.PRUNE_COMMIT_EVERY == 0:
                    conn.commit()
                    print(
                        f"[TICK_WRITER] Pruning… {i}/{len(tables)} table(s) done "
                        f"({total_pruned} row(s) so far, "
                        f"{time.time() - start:.1f}s elapsed)",
                        flush=True,
                    )
            conn.commit()
            print(
                f"[TICK_WRITER] Pruned {total_pruned} row(s) across {len(tables)} table(s) "
                f"older than {cutoff_date.isoformat()} "
                f"(keeping last {n_trading_days} trading days, "
                f"{time.time() - start:.1f}s total)",
                flush=True,
            )
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
        conn.commit()
        return conn

    def _ensure_table(self, conn, table: str, kind: str):
        if table in self._known_tables:
            return
        if kind == "depth":
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms    INTEGER NOT NULL,
                    ltp      REAL,
                    raw_json TEXT
                )
            """)
        else:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms    INTEGER NOT NULL,
                    ltp      REAL,
                    qty      REAL
                )
            """)
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ts ON {table}(ts_ms)")
        conn.commit()
        self._known_tables.add(table)

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
                elif item is not None and item[0] == "prune":
                    # Flush pending live rows first so pruning sees a
                    # consistent, up-to-date table set, then run the
                    # prune on THIS connection (see prune_older_than_days
                    # docstring for why it must be this connection).
                    if batch:
                        self._flush_live(conn, batch)
                        batch = []
                        last_commit = time.time()
                    _, n_trading_days, done_event = item
                    self._prune_tables(conn, n_trading_days)
                    done_event.set()
                elif item is not None and item[0] == "delete_range":
                    if batch:
                        self._flush_live(conn, batch)
                        batch = []
                        last_commit = time.time()
                    _, symbol, start_ms, end_ms, kind, done_event = item
                    self._delete_range(conn, symbol, start_ms, end_ms, kind)
                    done_event.set()
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
        # group rows by destination table, since a single batch can mix
        # quote and depth ticks for many different symbols
        rows_by_table = {}   # table_name -> (kind, [row_tuple, ...])

        for _, symbol, kind, snapshot in batch:
            ts_ms = snapshot.get("timestamp")
            if ts_ms is None:
                continue
            table = self.table_name(symbol, kind)
            bucket = rows_by_table.setdefault(table, (kind, []))
            if kind == "depth":
                bucket[1].append((
                    int(ts_ms), snapshot.get("ltp"),
                    json.dumps({
                        "bids": snapshot.get("bids", []),
                        "asks": snapshot.get("asks", []),
                    }),
                ))
            else:
                bucket[1].append((int(ts_ms), snapshot.get("ltp"), snapshot.get("qty")))

        for table, (kind, rows) in rows_by_table.items():
            self._ensure_table(conn, table, kind)
            self._insert(conn, table, kind, rows)

    def _flush_bulk(self, conn, symbol, rows_in):
        table = self.table_name(symbol, "quote")
        rows = []
        for r in rows_in:
            ts_ms = r.get("timestamp")
            if ts_ms is None:
                continue
            rows.append((int(ts_ms), r.get("ltp"), r.get("qty")))
        self._ensure_table(conn, table, "quote")
        self._insert(conn, table, "quote", rows)

    def _insert(self, conn, table, kind, rows):
        if not rows:
            return
        col = "raw_json" if kind == "depth" else "qty"
        try:
            conn.executemany(
                f"INSERT INTO {table} (ts_ms, ltp, {col}) VALUES (?, ?, ?)",
                rows,
            )
            conn.commit()
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] batch insert into {table} failed: {exc}", flush=True)


# ═════════════════════════════════════════════════════════════════════
# HistoryCandleStore
#
# Local SQLite cache for candles fetched from the history (fallback)
# PostgreSQL DB — one table per symbol, `candles_<symbol>`, inside
# history_candles.db (a separate file from ticks.db, same base_dir).
# Mirrors TickWriter's per-symbol-table naming convention above, but
# for pre-built candles instead of raw ticks, and with only ONE table
# per symbol since the history db only ever holds one granularity
# (self.history_native_tf in backfill_manager.py, default "1m" — see
# that file's docstring on _fetch_history_candles_batch for how that's
# confirmed).
#
# Rows are upserted (ts_ms is the PRIMARY KEY), unlike ticks.db's
# append-only design — candles are idempotent (the same bucket always
# has the same OHLCV once closed), so re-fetching an overlapping range
# just overwrites in place instead of risking duplicate rows.
#
# Much lower write volume than raw ticks (~375 rows/symbol/day instead
# of thousands), so this stays a simple synchronous SQLite wrapper
# behind a lock — no dedicated background writer thread the way
# TickWriter above has one.
# ═════════════════════════════════════════════════════════════════════

class HistoryCandleStore:

    DB_FILENAME = "history_candles.db"

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._lock = threading.Lock()
        self._known_tables = set()

    @staticmethod
    def table_name(symbol: str) -> str:
        return f"candles_{_safe_symbol(symbol)}"

    def _db_path(self):
        return os.path.join(self.base_dir, self.DB_FILENAME)

    def _connect(self):
        os.makedirs(self.base_dir, exist_ok=True)
        conn = sqlite3.connect(self._db_path(), timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.commit()
        return conn

    def _ensure_table(self, conn, table: str):
        if table in self._known_tables:
            return
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                ts_ms  INTEGER PRIMARY KEY,
                open   REAL,
                high   REAL,
                low    REAL,
                close  REAL,
                volume REAL
            )
        """)
        conn.commit()
        self._known_tables.add(table)

    # ─────────────────────────────────────────────
    # Writes
    # ─────────────────────────────────────────────

    def save_candles(self, symbol: str, rows: list):
        """
        rows: list of {"timestamp": ms_int, "open", "high", "low",
        "close", "volume"}. Upserts on ts_ms — safe to call repeatedly
        with overlapping ranges.
        """
        if not rows:
            return
        with self._lock:
            try:
                conn = self._connect()
            except Exception as exc:
                print(f"[HISTORY_STORE][WARN] save_candles({symbol}) connect failed: {exc}", flush=True)
                return
            try:
                table = self.table_name(symbol)
                self._ensure_table(conn, table)
                conn.executemany(
                    f"""
                    INSERT INTO {table} (ts_ms, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(ts_ms) DO UPDATE SET
                        open=excluded.open, high=excluded.high,
                        low=excluded.low, close=excluded.close,
                        volume=excluded.volume
                    """,
                    [
                        (
                            int(r["timestamp"]),
                            r.get("open"), r.get("high"),
                            r.get("low"), r.get("close"), r.get("volume"),
                        )
                        for r in rows
                    ],
                )
                conn.commit()
            except Exception as exc:
                print(f"[HISTORY_STORE][WARN] save_candles({symbol}) failed: {exc}", flush=True)
            finally:
                conn.close()

    # ─────────────────────────────────────────────
    # Reads
    # ─────────────────────────────────────────────

    def max_ts(self, symbol: str):
        """Most recent ts_ms already cached for symbol, or None."""
        path = self._db_path()
        if not os.path.exists(path):
            return None
        table = self.table_name(symbol)
        with self._lock:
            try:
                conn = sqlite3.connect(path, timeout=15)
                try:
                    exists = conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                    ).fetchone()
                    if not exists:
                        return None
                    row = conn.execute(f"SELECT MAX(ts_ms) FROM {table}").fetchone()
                finally:
                    conn.close()
                return int(row[0]) if row and row[0] is not None else None
            except Exception as exc:
                print(f"[HISTORY_STORE][WARN] max_ts({symbol}) failed: {exc}", flush=True)
                return None

    def read_candles(self, symbol: str, start_ms: int = None, end_ms: int = None):
        """
        Rows for symbol within [start_ms, end_ms] (either bound
        optional), ascending by ts_ms. Empty list if the table doesn't
        exist or nothing matches.
        """
        path = self._db_path()
        if not os.path.exists(path):
            return []
        table = self.table_name(symbol)

        clauses, params = [], []
        if start_ms is not None:
            clauses.append("ts_ms >= ?")
            params.append(int(start_ms))
        if end_ms is not None:
            clauses.append("ts_ms <= ?")
            params.append(int(end_ms))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""

        with self._lock:
            try:
                conn = sqlite3.connect(path, timeout=15)
                try:
                    exists = conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                    ).fetchone()
                    if not exists:
                        return []
                    rows = conn.execute(
                        f"SELECT ts_ms, open, high, low, close, volume FROM {table}{where} ORDER BY ts_ms",
                        params,
                    ).fetchall()
                finally:
                    conn.close()
                return [
                    {
                        "timestamp": r[0], "open": r[1], "high": r[2],
                        "low": r[3], "close": r[4], "volume": r[5],
                    }
                    for r in rows
                ]
            except Exception as exc:
                print(f"[HISTORY_STORE][WARN] read_candles({symbol}) failed: {exc}", flush=True)
                return []


# Back-compat alias — old code importing DepthWriter from depth_writer.py
# should now import TickWriter from tick_writer.py instead.
DepthWriter = TickWriter
