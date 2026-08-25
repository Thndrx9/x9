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
import re
import time
import queue
import shutil
import sqlite3
import subprocess
import sys
import threading
import zlib
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

from market_time import tz_kolkata, trading_day_n_back


def _safe_symbol(symbol: str) -> str:
    """Same sanitization backfill_manager.py uses for quote_{symbol}
    PG table names, so the SQLite table names line up with it."""
    return "".join(c for c in str(symbol) if c.isalnum() or c == "_").lower()


# depth_<symbol> columns — deliberately identical, name-for-name, to
# PostgreSQL's depth_<symbol> layout (see pg_writer.py's
# _DEPTH_COLUMN_DEFS / backfill_manager.py's old _DEPTH_LEVEL_COLUMNS,
# which now imports this instead of keeping its own copy). Rows are
# stored as plain flat columns — NOT reassembled into nested
# bids/asks dicts and NOT JSON-serialized — so a depth row costs the
# same "just numbers" write as a quote row instead of paying a
# per-row json.dumps() and a much heavier TEXT payload. Nested
# bids/asks shape is only ever reconstructed where something actually
# needs it in memory (DepthStore's RAM window), not on the hot
# fetch/write path.
DEPTH_LEVEL_COLUMNS = tuple(
    f"{side}{lvl}_{field}"
    for side in ("buy", "sell")
    for lvl in range(5)
    for field in ("price", "qty", "orders")
)


def _flatten_depth_levels(levels, side):
    """levels: list of up to 5 {"price","quantity"/"qty","orders"} dicts
    (bids or asks, best-first) as DepthStore hands them to enqueue_live().
    Returns a flat {"<side>0_price": ..., ..., "<side>4_orders": ...}
    dict — same column names as DEPTH_LEVEL_COLUMNS — so live ticks land
    in the same flat schema the batched PG backfill path writes into.
    Missing levels are filled with None."""
    out = {}
    for i in range(5):
        lvl = levels[i] if i < len(levels) and levels[i] else {}
        out[f"{side}{i}_price"]  = lvl.get("price")
        out[f"{side}{i}_qty"]    = lvl.get("quantity", lvl.get("qty"))
        out[f"{side}{i}_orders"] = lvl.get("orders")
    return out


class SQLiteTickWriter:
    """
    SQLite-backed tick writer — dedicated background thread, one DB
    file per base_dir. Two tables PER SYMBOL:
        quote_<symbol>  (ts_ms, ltp, qty)
        depth_<symbol>  (ts_ms, ltp, buy0_price, buy0_qty, buy0_orders, ...,
                          sell4_price, sell4_qty, sell4_orders) — flat
                          columns matching PostgreSQL's depth_<symbol>
                          layout name-for-name (see DEPTH_LEVEL_COLUMNS)
    instead of one shared `ticks` table for every symbol/kind.
    """

    DB_FILENAME    = "ticks.db"
    BATCH_SIZE     = 100    # live snapshots buffered before one executemany() insert
    BATCH_MAX_SECS = 1.0    # max staleness before that insert is forced

    # Commit (transaction) boundary — decoupled from the insert grouping
    # above. Multiple _flush_live()/_flush_bulk() inserts accumulate in
    # ONE open transaction until either threshold below is hit, instead
    # of committing (fsync-ing) after every single insert. This is the
    # main lever on writer throughput: fewer commits = fewer real disk
    # waits, since each commit is a hard wait on the disk, not CPU work.
    COMMIT_MAX_ROWS = 50_000  # rows accumulated in the open txn before a commit
    COMMIT_MAX_SECS = 1.0     # max time an insert can sit uncommitted

    # A single executemany() over hundreds of thousands of rows can
    # stall the writer thread for a long, unresponsive stretch (no
    # chance to notice stop signals, service prune/delete_range/barrier
    # requests, etc). Paging keeps each individual executemany() call
    # bounded while still landing all pages inside the same transaction
    # — same commit count, just a responsive writer in between.
    INSERT_PAGE_SIZE = 10_000

    # Caps how many queue ITEMS (not rows) can sit unwritten at once.
    # Each "bulk" item can itself be an entire symbol's worth of rows
    # for one chunk, so this doesn't need to be large to meaningfully
    # cap RAM — it needs to be small enough that the fetch loop
    # actually feels backpressure instead of racing arbitrarily far
    # ahead of the writer. See enqueue_backfill_rows() for why this is
    # safe to block on, and enqueue_live()/enqueue() for why live ticks
    # deliberately do NOT block on this same limit.
    QUEUE_MAXSIZE = 40

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._queue   = queue.Queue(maxsize=self.QUEUE_MAXSIZE)
        self._stop    = threading.Event()

        # ── hold/release gate (see module docstring) ───────────────
        self._gate_lock   = threading.Lock()
        self._hold_count   = 0
        self._held_buffer = []   # [(symbol, kind, snapshot), ...] arrival order

        # tracks which quote_<symbol>/depth_<symbol> tables already
        # exist, so the writer thread doesn't re-run CREATE TABLE IF
        # NOT EXISTS on every single insert
        self._known_tables = set()

        # Tables created without a timestamp index yet (only populated
        # when this is a brand-new/empty ticks.db — see _run()'s
        # is_fresh_db check and build_pending_indexes()). Empty on an
        # existing/incremental db, where indexes are still created
        # immediately as before.
        self._pending_index_tables = set()
        self._is_fresh_db = False  # actual value set at the top of _run()

        # ── writer metrics (see get_metrics()) — best-effort, not
        # lock-protected; only the writer thread mutates these, and
        # a reader racing a torn read of a float/int is harmless here. ──
        self._metrics_started_at   = time.time()
        self._dropped_live_ticks   = 0
        self._rows_written_total   = 0
        self._commits_total        = 0
        self._commit_time_total    = 0.0
        self._last_commit_duration = 0.0
        self._last_commit_rows     = 0
        self._last_txn_duration    = 0.0

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
        while the gate is held (see hold()).

        Uses put_nowait(), NOT put() — this runs synchronously on the
        asyncio event loop (called directly from OhlcEngine.run() /
        DepthStore.run()'s tick-processing loops), so blocking here
        when the queue is full would freeze the entire event loop —
        every symbol, every websocket connection, everything — not
        just this one tick. enqueue_backfill_rows() safely blocks
        instead because it only ever runs via asyncio.to_thread(), off
        the event loop.

        On QueueFull (writer thread badly behind), this tick is
        dropped rather than risking that freeze. That should be rare
        in practice — live volume is naturally rate-limited by the
        market feed, unlike backfill's "dump months of history at
        once" pattern — but it's a real, deliberate tradeoff: a
        dropped live tick during writer overload vs. a frozen engine.
        """
        item = (symbol, kind, dict(snapshot))
        with self._gate_lock:
            if self._hold_count > 0:
                self._held_buffer.append(item)
                return
        try:
            self._queue.put_nowait(("live", *item))
        except queue.Full:
            self._dropped_live_ticks += 1
            if self._dropped_live_ticks == 1 or self._dropped_live_ticks % 500 == 0:
                print(
                    f"[TICK_WRITER][WARN] write queue full — dropped live tick "
                    f"for {symbol} ({self._dropped_live_ticks} dropped total). "
                    f"Writer thread is behind; see get_metrics().",
                    flush=True,
                )

    # Back-compat alias — depth_store.py used to call this on DepthWriter.
    def enqueue(self, symbol: str, snapshot: dict):
        self.enqueue_live(symbol, "depth", snapshot)

    # ─────────────────────────────────────────────
    # Bulk catch-up ingestion (PG-fetched historical ticks)
    # ─────────────────────────────────────────────

    def enqueue_backfill_rows(self, symbol: str, rows: list, kind: str = "quote"):
        """
        kind='quote' (default): rows is a list of
        {"timestamp": ms_int, "ltp": float, "qty": float}, written into
        quote_<symbol>.

        kind='depth': rows is a list of flat dicts —
        {"timestamp": ms_int, "ltp": float, "buy0_price": ..., "buy0_qty": ...,
        "buy0_orders": ..., ..., "sell4_orders": ...} — same column
        names as DEPTH_LEVEL_COLUMNS / PostgreSQL's depth_<symbol>
        table, written into depth_<symbol> as-is (no bids/asks nesting,
        no JSON encoding).

        Either way, rows must already be ascending by timestamp (as
        returned by BackfillManager's PG fetch). Queued as a single unit
        so it flushes as one contiguous ordered block ahead of anything
        enqueued after it — see module docstring.
        """
        if not rows:
            return
        # Deliberately BLOCKS if the queue is full (default put()
        # behavior) — this is the real fix for RAM not freeing quickly
        # during backfill. Without this, the fetch loop in
        # BackfillManager could dump chunk after chunk onto the queue
        # far faster than the writer thread drains them, so fetched
        # data just piled up in RAM for the whole fetch phase and only
        # started draining once flush_and_wait() forced a catch-up at
        # the very end. Blocking here means the fetch loop now pauses
        # naturally whenever the writer falls behind, capping how much
        # can ever be queued at once — real backpressure instead of an
        # unbounded buffer.
        #
        # Safe to block: every caller of this method (BackfillManager's
        # fetch loops) runs via asyncio.to_thread(...), never directly
        # on the event loop — see enqueue_live()'s docstring for why
        # THAT path can't use the same blocking behavior.
        self._queue.put(("bulk", symbol, list(rows), kind))

    # ─────────────────────────────────────────────
    # Hold/release gate
    # ─────────────────────────────────────────────

    def hold(self):
        """Start buffering live ticks in RAM instead of writing them."""
        with self._gate_lock:
            self._hold_count += 1

    def release(self):
        """
        Decrements the hold count; only actually stops buffering and
        drains the buffer once the count reaches zero. Reference-counted
        rather than a simple flag because more than one caller can hold
        the gate concurrently — e.g. Quote's and Depth's mid-session
        auto-heal can both be in flight at once (both connections
        dropping around the same time is a real, not rare, scenario).
        With a plain boolean, whichever finishes first would release()
        and prematurely resume direct writes while the OTHER heal is
        still in progress, defeating the whole point of the gate for
        that other mode. A simple counter fixes this: the buffer only
        actually drains once every concurrent holder has released.

        Call this AFTER the corresponding catch-up
        enqueue_backfill_rows() call so the buffered ticks land after
        the historical rows they followed in real time.

        Like enqueue_live(), this is called directly on the event loop
        (engine_runtime.py calls it without asyncio.to_thread), so it
        must not block on a full queue either — same put_nowait/drop
        tradeoff as enqueue_live().
        """
        with self._gate_lock:
            self._hold_count = max(0, self._hold_count - 1)
            if self._hold_count > 0:
                return  # still held by another concurrent caller — don't drain yet
            buffered = self._held_buffer
            self._held_buffer = []
        for symbol, kind, snapshot in buffered:
            try:
                self._queue.put_nowait(("live", symbol, kind, snapshot))
            except queue.Full:
                self._dropped_live_ticks += 1
                print(
                    f"[TICK_WRITER][WARN] write queue full during release() — "
                    f"dropped a buffered live tick for {symbol}",
                    flush=True,
                )

    def flush_and_wait(self, timeout: float = 120):
        """
        Block until every item enqueued so far (bulk backfill rows AND
        live ticks) has actually been written to disk by the writer
        thread — not just handed to the queue.

        enqueue_backfill_rows()/enqueue_live() are fire-and-forget:
        put() returns immediately, regardless of how far behind the
        writer thread is. Callers that fetch data, enqueue it, then
        immediately try to READ it back (e.g. BackfillManager's
        _seed_depth_ram, which reads depth_<symbol> right after
        _fetch_depth_batch enqueues it) would otherwise race the
        writer thread and see stale/empty results — this is exactly
        why depth RAM seeding was logging "0/199 symbols, 0 snapshots"
        even though the fetch itself had succeeded: the read ran
        before the writer thread had drained the backlog. It also
        means the fetched data was still sitting in the queue, in
        RAM, well after "[BACKFILL] ... complete" printed.

        Since queue items are processed strictly in order, putting a
        barrier item on the queue and waiting for the writer thread to
        reach it guarantees everything enqueued BEFORE this call has
        been committed to SQLite by the time this returns.
        """
        done = threading.Event()
        self._queue.put(("barrier", done))
        if not done.wait(timeout=timeout):
            print(
                f"[TICK_WRITER][WARN] flush_and_wait timed out after {timeout}s "
                f"— writer thread still behind; proceeding anyway",
                flush=True,
            )
        return done.is_set()

    def build_pending_indexes(self, timeout: float = 300):
        """
        For a brand-new/empty ticks.db, _ensure_table() deliberately
        skips creating each table's timestamp index at CREATE TABLE
        time — building an index while millions of rows are still
        being bulk-inserted during the very first backfill import is
        wasted, repeated work (the index gets rebalanced continuously
        as rows stream in) and slows that initial import down for no
        benefit, since nothing is querying these tables yet anyway.

        Call this once, after the initial startup backfill has fully
        finished (for both quote_* and depth_*), to build all of those
        deferred indexes in one pass over the now-settled tables. A
        no-op (returns immediately) on an existing/incremental
        ticks.db, where indexes were already created immediately as
        rows arrived, same as before — see _run()'s is_fresh_db check.
        """
        done = threading.Event()
        self._queue.put(("build_indexes", done))
        if not done.wait(timeout=timeout):
            print(
                f"[TICK_WRITER][WARN] build_pending_indexes timed out after {timeout}s",
                flush=True,
            )
        return done.is_set()

    def get_metrics(self) -> dict:
        """
        Best-effort snapshot of writer throughput, for diagnosing
        whether disk sync, indexes, or data conversion is the current
        bottleneck (see plan item 5). Safe to call from any thread.
        """
        elapsed = max(time.time() - self._metrics_started_at, 1e-9)
        return {
            "queue_depth":            self._queue.qsize(),
            "rows_written_total":     self._rows_written_total,
            "rows_per_sec_avg":       self._rows_written_total / elapsed,
            "commits_total":          self._commits_total,
            "avg_commit_secs":        (self._commit_time_total / self._commits_total) if self._commits_total else 0.0,
            "last_commit_secs":       self._last_commit_duration,
            "last_commit_rows":       self._last_commit_rows,
            "last_txn_secs":          self._last_txn_duration,
            "pending_index_tables":   len(self._pending_index_tables),
            "dropped_live_ticks":     self._dropped_live_ticks,
        }

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
        kind='quote', or {"timestamp", "ltp", <DEPTH_LEVEL_COLUMNS...>}
        (flat buy0_price/buy0_qty/.../sell4_orders, same names as the
        PG source table) for kind='depth'. Empty list if the table
        doesn't exist or nothing matches. Opens its own short-lived
        read connection — not on the hot write path.
        """
        path = self._db_path()
        if not os.path.exists(path):
            return []
        table = self.table_name(symbol, kind)
        if kind == "depth":
            select_cols = ["ts_ms", "ltp"] + list(DEPTH_LEVEL_COLUMNS)
        else:
            select_cols = ["ts_ms", "ltp", "qty"]

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
                    f"SELECT {', '.join(select_cols)} FROM {table}{where} ORDER BY ts_ms",
                    params,
                ).fetchall()
            finally:
                conn.close()
            out_cols = ["timestamp", "ltp"] + (list(DEPTH_LEVEL_COLUMNS) if kind == "depth" else ["qty"])
            return [dict(zip(out_cols, r)) for r in rows]
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

    def delete_timestamps(self, symbol: str, timestamps, kind: str = "quote", wait: bool = True, timeout: float = 30):
        """
        Delete specific (possibly non-contiguous) ts_ms values from
        quote_<symbol> (or depth_<symbol>) — used by the phantom-row
        check to remove rows that are cached locally but weren't
        confirmed present in the main db for the range just re-checked.
        Unlike delete_range(), these timestamps aren't necessarily a
        contiguous block, so they're matched individually rather than
        with a >=/<= range.

        wait=False: fire-and-forget, matching the other tick_writer
        calls in BackfillManager's phantom-row loops — the delete is
        still ordered correctly relative to other queued writes (same
        queue, same single writer thread), it just doesn't block the
        caller waiting for confirmation.
        """
        timestamps = [int(t) for t in timestamps]
        if not timestamps:
            return

        if not self._thread.is_alive():
            conn = sqlite3.connect(self._db_path(), timeout=timeout) if os.path.exists(self._db_path()) else None
            if conn is None:
                return
            try:
                self._delete_timestamps(conn, symbol, timestamps, kind)
            finally:
                conn.close()
            return

        done = threading.Event()
        self._queue.put(("delete_timestamps", symbol, timestamps, kind, done))
        if wait:
            if not done.wait(timeout=timeout):
                print(
                    f"[TICK_WRITER][WARN] delete_timestamps({symbol}) timed out after "
                    f"{timeout}s waiting for writer thread",
                    flush=True,
                )

    def _delete_timestamps(self, conn, symbol: str, timestamps, kind: str):
        table = self.table_name(symbol, kind)
        try:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            if not exists:
                return
            total_deleted = 0
            # SQLite has a default limit (~999) on bound parameters per
            # statement, so page the IN(...) clause instead of binding
            # every phantom timestamp in one call.
            page_size = 500
            for start in range(0, len(timestamps), page_size):
                page = timestamps[start:start + page_size]
                placeholders = ", ".join("?" * len(page))
                cur = conn.execute(
                    f"DELETE FROM {table} WHERE ts_ms IN ({placeholders})", page
                )
                total_deleted += cur.rowcount
            conn.commit()
            if total_deleted:
                print(
                    f"[TICK_WRITER] Deleted {total_deleted} phantom row(s) from {table}",
                    flush=True,
                )
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] delete_timestamps({table}) failed: {exc}", flush=True)

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
        conn = sqlite3.connect(path, timeout=15)
        # Configured once per writer-thread lifetime, on the single
        # writer connection — see plan item 2. synchronous=FULL is kept
        # (not relaxed to NORMAL) so durability guarantees don't
        # change; the throughput win instead comes from committing far
        # less often (see COMMIT_MAX_ROWS/COMMIT_MAX_SECS), which
        # amortizes each fsync over many more rows without weakening
        # what a completed commit means.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=15000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-65536")
        conn.commit()
        return conn

    def _ensure_table(self, conn, table: str, kind: str):
        if table in self._known_tables:
            return
        is_new_table = not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if kind == "depth":
            level_cols_sql = ",\n                    ".join(f"{c} REAL" for c in DEPTH_LEVEL_COLUMNS)
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms    INTEGER NOT NULL,
                    ltp      REAL,
                    {level_cols_sql}
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
        # On a brand-new/empty ticks.db, defer this table's index until
        # build_pending_indexes() is called after the initial import
        # finishes (plan item 6) — building it row-by-row during a huge
        # first bulk import is wasted, repeated rebalancing work with
        # nothing querying the table yet. On an existing/incremental
        # db, keep creating the index immediately as before, since
        # ongoing lookups (gap detection, RAM seeding, etc.) need to
        # stay fast throughout.
        if self._is_fresh_db and is_new_table:
            self._pending_index_tables.add(table)
        else:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ts ON {table}(ts_ms)")
        conn.commit()
        self._known_tables.add(table)

    def _commit_if_due(self, conn, force: bool = False):
        """
        Commits the currently-open transaction if `force`, or if either
        commit-window threshold (COMMIT_MAX_ROWS / COMMIT_MAX_SECS) has
        been hit. No-ops if nothing is pending. This is the actual
        throughput lever — previously every _insert() committed
        immediately (one fsync-wait per flush); now many flushes worth
        of rows share one commit.
        """
        if self._pending_commit_rows == 0:
            return
        due = (
            force
            or self._pending_commit_rows >= self.COMMIT_MAX_ROWS
            or (self._txn_started_at is not None
                and time.time() - self._txn_started_at >= self.COMMIT_MAX_SECS)
        )
        if not due:
            return
        t0 = time.time()
        conn.commit()
        dt = time.time() - t0
        self._commits_total += 1
        self._commit_time_total += dt
        self._last_commit_duration = dt
        self._last_commit_rows = self._pending_commit_rows
        self._last_txn_duration = time.time() - (self._txn_started_at or t0)
        self._pending_commit_rows = 0
        self._txn_started_at = None

    def _note_inserted(self, n: int):
        if n <= 0:
            return
        if self._txn_started_at is None:
            self._txn_started_at = time.time()
        self._pending_commit_rows += n
        self._rows_written_total += n

    def _build_pending_indexes(self, conn):
        if not self._pending_index_tables:
            return
        for table in self._pending_index_tables:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ts ON {table}(ts_ms)")
        conn.commit()
        self._pending_index_tables.clear()

    def _run(self):
        # Detected once, before _connect() creates the file if it
        # doesn't already exist — governs whether _ensure_table() defers
        # index creation (plan item 6). An existing ticks.db (this
        # process restarting against an already-populated cache) always
        # gets indexes immediately, same as before.
        self._is_fresh_db = not os.path.exists(self._db_path())

        conn = self._connect()
        batch = []  # list of ("live", symbol, kind, snapshot) items pending one executemany()
        last_batch_flush = time.time()   # groups live snapshots into one insert call
        self._pending_commit_rows = 0    # rows inserted but not yet committed
        self._txn_started_at = None      # when the currently-open txn's first insert landed
        try:
            while True:
                stopping = self._stop.is_set() and self._queue.empty()

                if not batch and stopping:
                    self._commit_if_due(conn, force=True)
                    break

                try:
                    item = self._queue.get(timeout=0.25)
                except queue.Empty:
                    item = None

                if item is not None and item[0] == "bulk":
                    # Flush whatever live rows are pending first, so the
                    # bulk block lands in the right relative position —
                    # then insert the bulk rows as one contiguous,
                    # already-ordered block. Neither call commits by
                    # itself; both just add to the open transaction.
                    if batch:
                        self._note_inserted(self._flush_live(conn, batch))
                        batch = []
                        last_batch_flush = time.time()
                    _, symbol, rows, kind = item
                    self._note_inserted(self._flush_bulk(conn, symbol, rows, kind))
                elif item is not None and item[0] == "prune":
                    # These are rare, explicit operations — commit
                    # whatever's pending first (flush live rows into the
                    # same txn, then commit) so they run against fully
                    # durable state, then commit their own work
                    # immediately too rather than leaving it open.
                    if batch:
                        self._note_inserted(self._flush_live(conn, batch))
                        batch = []
                        last_batch_flush = time.time()
                    self._commit_if_due(conn, force=True)
                    _, n_trading_days, done_event = item
                    self._prune_tables(conn, n_trading_days)
                    done_event.set()
                elif item is not None and item[0] == "delete_range":
                    if batch:
                        self._note_inserted(self._flush_live(conn, batch))
                        batch = []
                        last_batch_flush = time.time()
                    self._commit_if_due(conn, force=True)
                    _, symbol, start_ms, end_ms, kind, done_event = item
                    self._delete_range(conn, symbol, start_ms, end_ms, kind)
                    done_event.set()
                elif item is not None and item[0] == "delete_timestamps":
                    if batch:
                        self._note_inserted(self._flush_live(conn, batch))
                        batch = []
                        last_batch_flush = time.time()
                    self._commit_if_due(conn, force=True)
                    _, symbol, timestamps, kind, done_event = item
                    self._delete_timestamps(conn, symbol, timestamps, kind)
                    done_event.set()
                elif item is not None and item[0] == "barrier":
                    # Flush pending live rows first, then force a commit
                    # — the barrier's whole guarantee (see
                    # flush_and_wait()'s docstring) is that everything
                    # queued before it is DURABLE by the time it fires,
                    # not just inserted into an open transaction.
                    if batch:
                        self._note_inserted(self._flush_live(conn, batch))
                        batch = []
                        last_batch_flush = time.time()
                    self._commit_if_due(conn, force=True)
                    _, done_event = item
                    done_event.set()
                elif item is not None and item[0] == "build_indexes":
                    if batch:
                        self._note_inserted(self._flush_live(conn, batch))
                        batch = []
                        last_batch_flush = time.time()
                    self._commit_if_due(conn, force=True)
                    self._build_pending_indexes(conn)
                    _, done_event = item
                    done_event.set()
                elif item is not None:
                    batch.append(item)

                should_flush_batch = batch and (
                    len(batch) >= self.BATCH_SIZE
                    or time.time() - last_batch_flush >= self.BATCH_MAX_SECS
                    or stopping
                )
                if should_flush_batch:
                    self._note_inserted(self._flush_live(conn, batch))
                    batch = []
                    last_batch_flush = time.time()

                # Commit-window check runs every loop iteration (cheap
                # no-op when nothing's pending) so a burst of small
                # queue items still gets committed within COMMIT_MAX_SECS
                # even if no single flush call crosses COMMIT_MAX_ROWS.
                self._commit_if_due(conn, force=stopping)
        finally:
            conn.close()

    def _flush_live(self, conn, batch) -> int:
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
                flat = {}
                flat.update(_flatten_depth_levels(snapshot.get("bids", []), "buy"))
                flat.update(_flatten_depth_levels(snapshot.get("asks", []), "sell"))
                bucket[1].append(
                    (int(ts_ms), snapshot.get("ltp"))
                    + tuple(flat[c] for c in DEPTH_LEVEL_COLUMNS)
                )
            else:
                bucket[1].append((int(ts_ms), snapshot.get("ltp"), snapshot.get("qty")))

        n = 0
        for table, (kind, rows) in rows_by_table.items():
            self._ensure_table(conn, table, kind)
            n += self._insert(conn, table, kind, rows)
        return n

    def _flush_bulk(self, conn, symbol, rows_in, kind="quote") -> int:
        table = self.table_name(symbol, kind)
        rows = []
        if kind == "depth":
            # rows_in items are already flat (same DEPTH_LEVEL_COLUMNS
            # keys straight from PG, or already-flattened live rows) —
            # no bids/asks reconstruction, no json.dumps, just pass the
            # matching columns through.
            for r in rows_in:
                ts_ms = r.get("timestamp")
                if ts_ms is None:
                    continue
                rows.append(
                    (int(ts_ms), r.get("ltp"))
                    + tuple(r.get(c) for c in DEPTH_LEVEL_COLUMNS)
                )
        else:
            for r in rows_in:
                ts_ms = r.get("timestamp")
                if ts_ms is None:
                    continue
                rows.append((int(ts_ms), r.get("ltp"), r.get("qty")))
        self._ensure_table(conn, table, kind)
        return self._insert(conn, table, kind, rows)

    def _insert(self, conn, table, kind, rows) -> int:
        """
        Executes the insert(s) for `rows` but deliberately does NOT
        commit — see _run()'s commit-window logic (plan item 1). Pages
        very large row lists into INSERT_PAGE_SIZE-row executemany()
        calls (plan item 3) so one huge bulk chunk can't stall the
        writer thread for an unresponsive stretch; all pages still land
        in the same open transaction as everything else, so this
        doesn't add extra commits.

        Returns the number of rows actually executed (0 if `rows` is
        empty or the insert failed).
        """
        if not rows:
            return 0
        if kind == "depth":
            cols = "ts_ms, ltp, " + ", ".join(DEPTH_LEVEL_COLUMNS)
            placeholders = ", ".join("?" * (2 + len(DEPTH_LEVEL_COLUMNS)))
        else:
            cols = "ts_ms, ltp, qty"
            placeholders = "?, ?, ?"
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        try:
            for start in range(0, len(rows), self.INSERT_PAGE_SIZE):
                conn.executemany(sql, rows[start:start + self.INSERT_PAGE_SIZE])
            return len(rows)
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] batch insert into {table} failed: {exc}", flush=True)
            return 0



import re
import shutil
import subprocess
import sys

import psycopg2
import psycopg2.extras



# ═════════════════════════════════════════════════════════════════════
# Auto-setup — installs/configures/starts a LOCAL PostgreSQL, only when
# PG_LOCAL_HOST is localhost. Adapted from the pattern already proven
# out for the live-mirror PgWriter; trimmed to just what this module
# needs (one db, no history db, no multi-db loop).
# ═════════════════════════════════════════════════════════════════════

def _conn_params(dbname=None, statement_timeout_sec=10) -> dict:
    return {
        "host":     os.getenv("PG_LOCAL_HOST", "localhost"),
        "port":     int(os.getenv("PG_LOCAL_PORT", "5432")),
        "dbname":   dbname or os.getenv("PG_LOCAL_DBNAME", "tickcache"),
        "user":     os.getenv("PG_LOCAL_USER", "tickcache"),
        "password": os.getenv("PG_LOCAL_PASSWORD", ""),
        "connect_timeout": 10,
        "options": f"-c statement_timeout={statement_timeout_sec * 1000}",
    }


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _run(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)


def _sudo_read(path) -> str:
    result = subprocess.run(["sudo", "cat", str(path)], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to read {path}: {result.stderr}")
    return result.stdout


def _sudo_write(path, content: str) -> None:
    result = subprocess.run(["sudo", "tee", str(path)], input=content, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to write {path}: {result.stderr}")


def _is_pg_installed() -> bool:
    return shutil.which("pg_lsclusters") is not None


def _install_pg() -> None:
    print("[PG_LOCAL_SETUP] PostgreSQL not found — installing...", flush=True)
    _run("sudo apt-get update -qq")
    _run("sudo apt-get install -y postgresql postgresql-contrib")
    print("[PG_LOCAL_SETUP] PostgreSQL installed", flush=True)


def _detect_pg_version():
    result = _run("pg_lsclusters", check=False)
    for line in result.stdout.splitlines():
        parts = line.split()
        if parts and parts[0].isdigit():
            return parts[0]
    return None


def _ensure_service_running(version: str) -> None:
    result = _run(f"sudo systemctl is-active postgresql@{version}-main", check=False)
    if result.stdout.strip() != "active":
        print(f"[PG_LOCAL_SETUP] Starting PostgreSQL {version}...", flush=True)
        _run(f"sudo systemctl start postgresql@{version}-main")
        time.sleep(2)


def _configure_pg(version: str) -> None:
    """Local-only db, so listen_addresses can stay at its default
    (localhost) — unlike the live-mirror PgWriter, this deliberately
    does NOT open listen_addresses='*' or add a 0.0.0.0/0 pg_hba.conf
    rule, since nothing outside this machine should ever need to reach
    it. Only pg_hba.conf gets a scoped local-only entry."""
    from pathlib import Path
    hba_path = Path(f"/etc/postgresql/{version}/main/pg_hba.conf")
    user = os.getenv("PG_LOCAL_USER", "tickcache")
    dbname = os.getenv("PG_LOCAL_DBNAME", "tickcache")
    hba_text = _sudo_read(hba_path)
    needed = f"host    {dbname}    {user}    127.0.0.1/32    scram-sha-256\n"
    if needed.strip() not in hba_text:
        _sudo_write(hba_path, hba_text + "\n" + needed)
        print("[PG_LOCAL_SETUP] pg_hba.conf updated (localhost-only)", flush=True)


def _create_user_and_db() -> None:
    user = os.getenv("PG_LOCAL_USER", "tickcache")
    password = os.getenv("PG_LOCAL_PASSWORD", "")
    dbname = os.getenv("PG_LOCAL_DBNAME", "tickcache")
    safe_pw = password.replace("'", "''")
    safe_user = _quote_ident(user)

    def _psql(sql: str):
        return subprocess.run(["sudo", "-u", "postgres", "psql", "-c", sql], capture_output=True, text=True)

    result = _psql(f"CREATE USER {safe_user} WITH PASSWORD '{safe_pw}'")
    if "already exists" in result.stderr:
        _psql(f"ALTER USER {safe_user} WITH PASSWORD '{safe_pw}'")
    else:
        print(f"[PG_LOCAL_SETUP] user '{user}' created", flush=True)

    safe_dbname = _quote_ident(dbname)
    result = _psql(f"CREATE DATABASE {safe_dbname} OWNER {safe_user}")
    if "already exists" not in result.stderr:
        print(f"[PG_LOCAL_SETUP] database '{dbname}' created", flush=True)
    _psql(f"GRANT ALL PRIVILEGES ON DATABASE {safe_dbname} TO {safe_user}")


def _restart_pg(version: str) -> None:
    result = subprocess.run(
        f"sudo systemctl restart postgresql@{version}-main", shell=True, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"PostgreSQL restart failed: {result.stderr.strip()}")
    time.sleep(2)


def auto_setup() -> dict:
    """
    Returns conn_params dict for psycopg2.connect(**params). Only runs
    install/configure steps when PG_LOCAL_HOST is localhost/127.0.0.1 —
    a genuinely remote PG_LOCAL_HOST (someone repurposing this module
    against a real server) is left completely alone.

    Fast path: if a connection already succeeds, every install/configure
    step below is skipped — this is why setup is instant on every run
    after the very first one.

    First-run prerequisite: the install/configure steps below shell out
    to `sudo`. If your user needs an interactive password for sudo, run
    this the FIRST time directly in a terminal (not backgrounded/under a
    process manager) — sudo will prompt on your actual terminal (via
    /dev/tty) the first time it's needed, then cache that credential for
    ~15 minutes, so every later sudo call in this same setup run reuses
    it silently. For unattended restarts before Postgres exists yet,
    passwordless sudo (scoped to just these commands, not blanket
    NOPASSWD:ALL) is the robust long-term answer instead.
    """
    host = os.getenv("PG_LOCAL_HOST", "localhost")
    params = _conn_params()

    if host not in ("localhost", "127.0.0.1"):
        print("[PG_LOCAL_SETUP] Remote PG_LOCAL_HOST — skipping auto-setup", flush=True)
        return params

    try:
        conn = psycopg2.connect(**params)
        conn.close()
        print("[PG_LOCAL_SETUP] Local PostgreSQL already running and connectable", flush=True)
        return params
    except Exception:
        pass

    print("[PG_LOCAL_SETUP] Starting local PostgreSQL auto-setup...", flush=True)
    if not _is_pg_installed():
        _install_pg()

    version = _detect_pg_version()
    if not version:
        raise RuntimeError("[PG_LOCAL_SETUP] Could not detect PostgreSQL version")
    print(f"[PG_LOCAL_SETUP] Detected PostgreSQL version: {version}", flush=True)

    _ensure_service_running(version)
    _configure_pg(version)
    _create_user_and_db()
    _restart_pg(version)

    for attempt in range(1, 6):
        try:
            conn = psycopg2.connect(**params)
            conn.close()
            print("[PG_LOCAL_SETUP] Setup complete — connection verified", flush=True)
            return params
        except Exception as exc:
            print(f"[PG_LOCAL_SETUP] Connection attempt {attempt}/5: {exc}", flush=True)
            time.sleep(3)

    raise RuntimeError("[PG_LOCAL_SETUP] Setup completed but connection still failing")


# ═════════════════════════════════════════════════════════════════════
# PostgresTickWriter — same public interface as SQLiteTickWriter above,
# selected via the TickWriter() factory function below based on
# TICK_WRITER_BACKEND.
# ═════════════════════════════════════════════════════════════════════

class PostgresTickWriter:
    """
    Drop-in replacement for SQLiteTickWriter above — never instantiate
    this directly, use the TickWriter() factory function so the same
    `from tick_writer import TickWriter` callers everywhere else in the
    codebase don't need to know or care which backend is active. Same
    method names, same argument shapes, same return shapes (read_ticks()
    returns the identical flat-dict format).

    Unlike SQLiteTickWriter, this runs a POOL of writer threads
    (WRITER_POOL_SIZE, default 9, overridable via PG_LOCAL_WRITER_THREADS)
    instead of one — Postgres's MVCC lets genuinely concurrent writes to
    DIFFERENT tables proceed without blocking each other, unlike SQLite's
    single file-level write lock, so a pool is a real throughput lever
    here in a way it wouldn't be for the SQLite backend.

    ── Ordering guarantee, still intact ────────────────────────────────
    Every symbol is routed to exactly ONE shard for its entire lifetime
    (via a deterministic hash — see _shard_for()), so all of one symbol's
    rows — live and backfilled alike — always pass through the SAME
    queue, processed by the SAME single thread, in arrival order. That's
    the same ordering guarantee SQLiteTickWriter's single writer gives
    you; sharding by symbol never lets two writers race on the same
    table, so a symbol's ticks can never land out of order OR in another
    symbol's table (the destination table name is computed from the
    row's own `symbol` field regardless of which shard processes it —
    sharding only decides WHICH thread handles a row, never WHERE it's
    written).

    hold()/release() still gate ALL symbols together (one shared
    _held flag/buffer) — release() re-routes each buffered item to its
    own symbol's shard when draining, same ordering guarantee applies.

    ── RAM ceiling kept from multiplying by pool size ──────────────────
    QUEUE_MAXSIZE and COMMIT_MAX_ROWS are both divided by the pool size
    (see __init__) so the AGGREGATE worst-case backlog across every
    shard stays roughly where a single writer's ceiling was, instead of
    silently multiplying by 9x just because there are 9 queues/9 open
    transactions now.

    ── Cross-shard maintenance ops ──────────────────────────────────────
    flush_and_wait() fans a barrier out to every shard and waits for all
    of them. build_pending_indexes()/prune_older_than_days() are DB-wide
    operations (not per-symbol), so they call flush_and_wait() first
    (ensuring every shard's pending work is durable), then run once
    against shard 0's connection only — no need to run them once per
    shard, any single connection can CREATE INDEX / DELETE across every
    table in the database.
    """

    BATCH_SIZE       = 100
    BATCH_MAX_SECS   = 1.0
    COMMIT_MAX_ROWS  = 50_000   # aggregate target across the whole pool — see __init__
    COMMIT_MAX_SECS  = 1.0
    INSERT_PAGE_SIZE = 10_000
    BULK_QUEUE_MAXSIZE = 40     # aggregate target across the whole pool — see __init__
    LIVE_QUEUE_MAXSIZE = 4_500  # aggregate target — separate pool from bulk, see class docstring.
    # Raised from 200 -> 4,500 (was ~22/shard, now ~500/shard): a live
    # tick item is a small dict, cheap to hold, and this is real
    # headroom against genuine burstiness (quote+depth ticks for busy
    # symbols landing close together) — the earlier size was tuned
    # assuming steady arrival, which real market data isn't. Combined
    # with the polling-delay fix above (the actual root cause), this is
    # defense-in-depth, not the primary fix.
    WRITER_POOL_SIZE = 9        # overridable via PG_LOCAL_WRITER_THREADS

    def __init__(self):
        self._params = auto_setup()

        self.pool_size = max(1, int(os.getenv("PG_LOCAL_WRITER_THREADS", str(self.WRITER_POOL_SIZE))))

        # Divided by pool size so the AGGREGATE ceiling across every
        # shard's queue/open-transaction stays close to what a single
        # writer's ceiling was — see class docstring's "RAM ceiling"
        # section. Floored so a large pool_size can't shrink either
        # value to something degenerate (e.g. a queue that can't even
        # hold one bulk chunk).
        #
        # LIVE and BULK get entirely SEPARATE queues per shard, not one
        # shared queue — a bulk catch-up job (e.g. mid-session auto-heal
        # after a websocket reconnect) can enqueue thousands of rows in
        # a tight loop, and with a shared queue that traffic fills every
        # available slot far faster than live ticks can claim one,
        # starving live data even though live ticks matter more
        # (missing a live tick is a real gap; a catch-up job finishing a
        # few seconds later than ideal is not). Giving live its own
        # dedicated queue means a busy bulk job can never crowd it out.
        self._bulk_queue_maxsize_per_shard = max(4, self.BULK_QUEUE_MAXSIZE // self.pool_size)
        self._live_queue_maxsize_per_shard = max(10, self.LIVE_QUEUE_MAXSIZE // self.pool_size)
        self._commit_max_rows_per_shard    = max(1_000, self.COMMIT_MAX_ROWS // self.pool_size)

        self._bulk_queues = [queue.Queue(maxsize=self._bulk_queue_maxsize_per_shard) for _ in range(self.pool_size)]
        self._live_queues = [queue.Queue(maxsize=self._live_queue_maxsize_per_shard) for _ in range(self.pool_size)]
        self._stop   = threading.Event()

        self._gate_lock   = threading.Lock()
        self._hold_count   = 0
        self._held_buffer = []

        # Per-shard state — each dict is only ever mutated by its OWN
        # shard thread (inside _run(shard_idx)), so no locking is
        # needed for any of these; get_metrics() only READS them.
        self._shard_state = [
            {
                "known_tables":         set(),
                "pending_commit_rows":  0,
                "txn_started_at":       None,
                "rows_written_total":   0,
                "commits_total":        0,
                "commit_time_total":    0.0,
                "last_commit_duration": 0.0,
                "last_commit_rows":     0,
                "last_txn_duration":    0.0,
            }
            for _ in range(self.pool_size)
        ]

        # SHARED across shards (a table can only ever be created by the
        # one shard that owns its symbol, but build_pending_indexes()
        # needs to see every shard's pending tables, not just shard 0's)
        # — writes are rare (once per symbol, at first table creation),
        # so a simple lock is more than sufficient, no contention risk.
        self._pending_index_tables = set()
        self._pending_index_lock   = threading.Lock()

        # Detected ONCE, up front, via a single throwaway connection —
        # not per-shard — so every shard agrees on whether this is a
        # fresh db, and so 9 threads don't all independently race to
        # query pg_tables at startup.
        self._is_fresh_db = self._detect_fresh_db()

        self._metrics_started_at = time.time()
        self._dropped_live_ticks = 0  # only ever touched from the event-loop thread — see enqueue_live()/release()

        self._threads = []
        for i in range(self.pool_size):
            # Shard index passed as an explicit thread argument, NOT
            # captured via a closure over the loop variable — the
            # classic "every thread sees the same final value of a
            # loop variable" bug. threading.Thread(args=(i,)) binds the
            # value of i at Thread-creation time, so each thread gets
            # its own correct, fixed shard index.
            t = threading.Thread(target=self._run, name=f"pg-writer-{i}", args=(i,), daemon=True)
            t.start()
            self._threads.append(t)

    def _detect_fresh_db(self) -> bool:
        conn = None
        try:
            conn = psycopg2.connect(**self._params)
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'quote_%%' LIMIT 1")
            return cur.fetchone() is None
        except Exception:
            return False
        finally:
            if conn is not None:
                conn.close()

    def _shard_for(self, symbol: str) -> int:
        """
        Deterministic (NOT Python's randomized built-in hash()) so a
        given symbol always maps to the same shard for the lifetime of
        this process — required for the ordering guarantee. Doesn't
        need to be stable ACROSS restarts (a symbol landing on a
        different shard after a restart doesn't corrupt anything —
        shards don't own data, tables do, and every shard writes to the
        same shared set of tables), only within one running process.
        """
        return zlib.crc32(symbol.encode()) % self.pool_size

    # ─────────────────────────────────────────────
    # Live tick ingestion — mirrors TickWriter.enqueue_live() exactly,
    # including the non-blocking put_nowait(): this runs synchronously
    # on the asyncio event loop (OhlcEngine.run()/DepthStore.run()),
    # so it must never block on a full queue. Routed to the symbol's
    # shard queue only — other shards are entirely unaffected if this
    # one particular symbol's shard is momentarily full.
    # ─────────────────────────────────────────────

    def enqueue_live(self, symbol: str, kind: str, snapshot: dict):
        item = (symbol, kind, dict(snapshot))
        with self._gate_lock:
            if self._hold_count > 0:
                self._held_buffer.append(item)
                return
        try:
            self._live_queues[self._shard_for(symbol)].put_nowait(("live", *item))
        except queue.Full:
            self._dropped_live_ticks += 1
            if self._dropped_live_ticks == 1 or self._dropped_live_ticks % 500 == 0:
                print(
                    f"[PG_LOCAL_WRITER][WARN] LIVE queue full — dropped live tick "
                    f"for {symbol} ({self._dropped_live_ticks} dropped total)",
                    flush=True,
                )

    def hold(self):
        with self._gate_lock:
            self._hold_count += 1

    # Back-compat alias, matching TickWriter's — nothing in this repo
    # currently calls it, kept only so PostgresTickWriter is a true
    # drop-in.
    def enqueue(self, symbol: str, snapshot: dict):
        self.enqueue_live(symbol, "depth", snapshot)

    def release(self):
        """Reference-counted — see SQLiteTickWriter.release()'s
        docstring for why (concurrent Quote/Depth mid-session heals can
        both hold the gate at once)."""
        with self._gate_lock:
            self._hold_count = max(0, self._hold_count - 1)
            if self._hold_count > 0:
                return
            buffered = self._held_buffer
            self._held_buffer = []
        for symbol, kind, snapshot in buffered:
            try:
                self._live_queues[self._shard_for(symbol)].put_nowait(("live", symbol, kind, snapshot))
            except queue.Full:
                self._dropped_live_ticks += 1
                print(
                    f"[PG_LOCAL_WRITER][WARN] LIVE queue full during release() — "
                    f"dropped a buffered live tick for {symbol}",
                    flush=True,
                )

    # ─────────────────────────────────────────────
    # Bulk catch-up ingestion — same signature as
    # TickWriter.enqueue_backfill_rows(), including the blocking put():
    # safe here because BackfillManager always calls this via
    # asyncio.to_thread(), never directly on the event loop. Routed to
    # its own dedicated BULK queue, entirely separate from the LIVE
    # queue above — a busy catch-up job filling this queue can never
    # crowd out live ticks, since they're not competing for the same
    # slots anymore (see class docstring + __init__ for why this split
    # exists: it's what a shared queue got wrong during mid-session
    # auto-heal after a websocket reconnect).
    # ─────────────────────────────────────────────

    def enqueue_backfill_rows(self, symbol: str, rows: list, kind: str = "quote"):
        if not rows:
            return
        self._bulk_queues[self._shard_for(symbol)].put(("bulk", symbol, list(rows), kind))

    def flush_and_wait(self, timeout: float = 120):
        """
        Fans a barrier out to EVERY shard's BOTH queues (live and bulk)
        and waits for all of them — guarantees everything queued before
        this call, on either queue, is committed by the time it
        returns. Since shards run concurrently, the real wall-clock
        cost is bounded by the slowest shard, not the sum of all of
        them.
        """
        events = []
        for q in list(self._bulk_queues) + list(self._live_queues):
            done = threading.Event()
            q.put(("barrier", done))
            events.append(done)
        all_ok = True
        for done in events:
            if not done.wait(timeout=timeout):
                print(f"[PG_LOCAL_WRITER][WARN] flush_and_wait timed out after {timeout}s on one queue", flush=True)
                all_ok = False
        return all_ok

    def build_pending_indexes(self, timeout: float = 300):
        """DB-wide op — flush every shard first so nothing's still
        in-flight, then build every pending index (across ALL shards'
        contributions to the shared _pending_index_tables set) using
        just shard 0's connection."""
        self.flush_and_wait(timeout=timeout)
        done = threading.Event()
        self._bulk_queues[0].put(("build_indexes", done))
        if not done.wait(timeout=timeout):
            print(f"[PG_LOCAL_WRITER][WARN] build_pending_indexes timed out after {timeout}s", flush=True)
        return done.is_set()

    def delete_range(self, symbol: str, start_ms: int, end_ms=None, kind: str = "quote", timeout: float = 30):
        done = threading.Event()
        self._bulk_queues[self._shard_for(symbol)].put(("delete_range", symbol, start_ms, end_ms, kind, done))
        if not done.wait(timeout=timeout):
            print(f"[PG_LOCAL_WRITER][WARN] delete_range({symbol}) timed out", flush=True)

    def delete_timestamps(self, symbol: str, timestamps, kind: str = "quote", wait: bool = True, timeout: float = 30):
        timestamps = [int(t) for t in timestamps]
        if not timestamps:
            return
        done = threading.Event()
        self._bulk_queues[self._shard_for(symbol)].put(("delete_timestamps", symbol, timestamps, kind, done))
        if wait:
            if not done.wait(timeout=timeout):
                print(f"[PG_LOCAL_WRITER][WARN] delete_timestamps({symbol}) timed out", flush=True)

    def prune_older_than_days(self, n_trading_days: int = 3, timeout=None):
        """DB-wide op — flush every shard first, then prune once using
        just shard 0's connection (it queries pg_tables directly for
        every quote_%/depth_% table, not any shard's local knowledge)."""
        self.flush_and_wait(timeout=timeout if timeout is not None else 120)
        done = threading.Event()
        self._bulk_queues[0].put(("prune", n_trading_days, done))
        if timeout is not None:
            if not done.wait(timeout=timeout):
                print(f"[PG_LOCAL_WRITER][WARN] prune_older_than_days timed out", flush=True)
        else:
            done.wait()

    def get_metrics(self) -> dict:
        elapsed = max(time.time() - self._metrics_started_at, 1e-9)
        rows_written_total = sum(s["rows_written_total"] for s in self._shard_state)
        commits_total       = sum(s["commits_total"] for s in self._shard_state)
        commit_time_total   = sum(s["commit_time_total"] for s in self._shard_state)
        bulk_queue_depth = sum(q.qsize() for q in self._bulk_queues)
        live_queue_depth = sum(q.qsize() for q in self._live_queues)
        return {
            "pool_size":            self.pool_size,
            "queue_depth":          bulk_queue_depth + live_queue_depth,
            "bulk_queue_depth":     bulk_queue_depth,
            "live_queue_depth":     live_queue_depth,
            "rows_written_total":   rows_written_total,
            "rows_per_sec_avg":     rows_written_total / elapsed,
            "commits_total":        commits_total,
            "avg_commit_secs":      (commit_time_total / commits_total) if commits_total else 0.0,
            "pending_index_tables": len(self._pending_index_tables),
            "dropped_live_ticks":   self._dropped_live_ticks,
            "per_shard": [
                {
                    "bulk_queue_depth":   self._bulk_queues[i].qsize(),
                    "live_queue_depth":   self._live_queues[i].qsize(),
                    "rows_written_total": s["rows_written_total"],
                    "commits_total":      s["commits_total"],
                    "last_commit_secs":   s["last_commit_duration"],
                    "last_commit_rows":   s["last_commit_rows"],
                    "last_txn_secs":      s["last_txn_duration"],
                }
                for i, s in enumerate(self._shard_state)
            ],
        }

    def shutdown(self, timeout=60):
        self._stop.set()
        # Each thread gets a fair share of the overall timeout budget —
        # they're shutting down concurrently (all see _stop set at
        # roughly the same time), so this isn't "9x timeout", it's
        # "up to timeout seconds total, checked per-thread".
        per_thread_timeout = max(1.0, timeout / max(1, len(self._threads)))
        deadline = time.time() + timeout
        for t in self._threads:
            remaining = max(0.1, deadline - time.time())
            t.join(timeout=min(per_thread_timeout, remaining))
        still_alive = [t.name for t in self._threads if t.is_alive()]
        if still_alive:
            print(f"[PG_LOCAL_WRITER][ERROR] shutdown timed out for: {', '.join(still_alive)}", flush=True)

    # ─────────────────────────────────────────────
    # Reads — separate short-lived connection, not on any writer thread,
    # unaffected by sharding
    # ─────────────────────────────────────────────

    def table_name(self, symbol: str, kind: str) -> str:
        return f"{kind}_{_safe_symbol(symbol)}"

    def read_ticks(self, symbol: str, start_ms=None, end_ms=None, kind: str = "quote"):
        """Same return shape as TickWriter.read_ticks(): list of dicts,
        {"timestamp","ltp","qty"} for quote, {"timestamp","ltp",
        <DEPTH_LEVEL_COLUMNS...>} for depth."""
        table = self.table_name(symbol, kind)
        if kind == "depth":
            select_cols = ["ts_ms", "ltp"] + list(DEPTH_LEVEL_COLUMNS)
        else:
            select_cols = ["ts_ms", "ltp", "qty"]

        clauses, params = [], []
        if start_ms is not None:
            clauses.append("ts_ms >= %s")
            params.append(int(start_ms))
        if end_ms is not None:
            clauses.append("ts_ms <= %s")
            params.append(int(end_ms))
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""

        try:
            conn = psycopg2.connect(**self._params)
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s", (table,)
                )
                if not cur.fetchone():
                    return []
                cur.execute(f"SELECT {', '.join(select_cols)} FROM {table}{where} ORDER BY ts_ms", params)
                rows = cur.fetchall()
            finally:
                conn.close()
            out_cols = ["timestamp", "ltp"] + (list(DEPTH_LEVEL_COLUMNS) if kind == "depth" else ["qty"])
            return [dict(zip(out_cols, r)) for r in rows]
        except Exception as exc:
            print(f"[PG_LOCAL_WRITER][WARN] read_ticks({symbol}) failed: {exc}", flush=True)
            return []

    def max_ts(self, symbol: str, kind: str = "quote"):
        table = self.table_name(symbol, kind)
        try:
            conn = psycopg2.connect(**self._params)
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s", (table,)
                )
                if not cur.fetchone():
                    return None
                cur.execute(f"SELECT MAX(ts_ms) FROM {table}")
                row = cur.fetchone()
                return row[0] if row else None
            finally:
                conn.close()
        except Exception as exc:
            print(f"[PG_LOCAL_WRITER][WARN] max_ts({symbol}) failed: {exc}", flush=True)
            return None

    # ─────────────────────────────────────────────
    # Writer thread internals — every method below takes an explicit
    # `state` dict (this shard's own self._shard_state[shard_idx]) or
    # `shard_idx` where needed. No locking anywhere in here: each
    # shard's state dict, connection, and batch are touched by exactly
    # one thread for that shard's entire lifetime.
    # ─────────────────────────────────────────────

    def _connect(self):
        conn = psycopg2.connect(**self._params)
        conn.autocommit = False
        return conn

    def _ensure_table(self, conn, table: str, kind: str, state: dict):
        if table in state["known_tables"]:
            return
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s", (table,))
        is_new_table = not cur.fetchone()

        # UNLOGGED: this cache is entirely disposable (re-fetchable from
        # AWS Postgres via BackfillManager on any loss), so skipping WAL
        # entirely here is a safe, deliberate durability trade for
        # close-to-memory-speed writes — see module docstring.
        if kind == "depth":
            level_cols_sql = ",\n                ".join(f"{c} DOUBLE PRECISION" for c in DEPTH_LEVEL_COLUMNS)
            cur.execute(f"""
                CREATE UNLOGGED TABLE IF NOT EXISTS {table} (
                    id     BIGSERIAL PRIMARY KEY,
                    ts_ms  BIGINT NOT NULL,
                    ltp    DOUBLE PRECISION,
                    {level_cols_sql}
                )
            """)
        else:
            cur.execute(f"""
                CREATE UNLOGGED TABLE IF NOT EXISTS {table} (
                    id    BIGSERIAL PRIMARY KEY,
                    ts_ms BIGINT NOT NULL,
                    ltp   DOUBLE PRECISION,
                    qty   DOUBLE PRECISION
                )
            """)

        if self._is_fresh_db and is_new_table:
            with self._pending_index_lock:
                self._pending_index_tables.add(table)
        else:
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ts ON {table}(ts_ms)")
        conn.commit()
        state["known_tables"].add(table)

    def _build_pending_indexes(self, conn):
        with self._pending_index_lock:
            tables = list(self._pending_index_tables)
            self._pending_index_tables.clear()
        if not tables:
            return
        cur = conn.cursor()
        for table in tables:
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_ts ON {table}(ts_ms)")
        conn.commit()

    def _insert(self, conn, table, kind, rows) -> int:
        if not rows:
            return 0
        if kind == "depth":
            cols = ["ts_ms", "ltp"] + list(DEPTH_LEVEL_COLUMNS)
        else:
            cols = ["ts_ms", "ltp", "qty"]
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
        try:
            cur = conn.cursor()
            for start in range(0, len(rows), self.INSERT_PAGE_SIZE):
                psycopg2.extras.execute_values(cur, sql, rows[start:start + self.INSERT_PAGE_SIZE])
            return len(rows)
        except Exception as exc:
            print(f"[PG_LOCAL_WRITER][WARN] batch insert into {table} failed: {exc}", flush=True)
            conn.rollback()
            return 0

    def _flush_live(self, conn, batch, state) -> int:
        rows_by_table = {}
        for _, symbol, kind, snapshot in batch:
            ts_ms = snapshot.get("timestamp")
            if ts_ms is None:
                continue
            table = self.table_name(symbol, kind)
            bucket = rows_by_table.setdefault(table, (kind, []))
            if kind == "depth":
                flat = {}
                flat.update(_flatten_depth_levels(snapshot.get("bids", []), "buy"))
                flat.update(_flatten_depth_levels(snapshot.get("asks", []), "sell"))
                bucket[1].append((int(ts_ms), snapshot.get("ltp")) + tuple(flat[c] for c in DEPTH_LEVEL_COLUMNS))
            else:
                bucket[1].append((int(ts_ms), snapshot.get("ltp"), snapshot.get("qty")))

        n = 0
        for table, (kind, rows) in rows_by_table.items():
            self._ensure_table(conn, table, kind, state)
            n += self._insert(conn, table, kind, rows)
        return n

    def _flush_bulk(self, conn, symbol, rows_in, kind, state) -> int:
        table = self.table_name(symbol, kind)
        rows = []
        if kind == "depth":
            for r in rows_in:
                ts_ms = r.get("timestamp")
                if ts_ms is None:
                    continue
                rows.append((int(ts_ms), r.get("ltp")) + tuple(r.get(c) for c in DEPTH_LEVEL_COLUMNS))
        else:
            for r in rows_in:
                ts_ms = r.get("timestamp")
                if ts_ms is None:
                    continue
                rows.append((int(ts_ms), r.get("ltp"), r.get("qty")))
        self._ensure_table(conn, table, kind, state)
        return self._insert(conn, table, kind, rows)

    def _delete_range(self, conn, symbol, start_ms, end_ms, kind):
        table = self.table_name(symbol, kind)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s", (table,))
        if not cur.fetchone():
            return
        if end_ms is not None:
            cur.execute(f"DELETE FROM {table} WHERE ts_ms >= %s AND ts_ms <= %s", (start_ms, end_ms))
        else:
            cur.execute(f"DELETE FROM {table} WHERE ts_ms >= %s", (start_ms,))
        n = cur.rowcount
        conn.commit()
        if n:
            print(f"[PG_LOCAL_WRITER] Deleted {n} row(s) from {table}", flush=True)

    def _delete_timestamps(self, conn, symbol, timestamps, kind):
        table = self.table_name(symbol, kind)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s", (table,))
        if not cur.fetchone():
            return
        total = 0
        page_size = 1000
        for start in range(0, len(timestamps), page_size):
            page = timestamps[start:start + page_size]
            cur.execute(f"DELETE FROM {table} WHERE ts_ms = ANY(%s)", (page,))
            total += cur.rowcount
        conn.commit()
        if total:
            print(f"[PG_LOCAL_WRITER] Deleted {total} phantom row(s) from {table}", flush=True)

    def _prune_tables(self, conn, n_trading_days: int):
        cutoff_date = trading_day_n_back(n_trading_days - 1)
        cutoff_dt   = datetime.combine(cutoff_date, datetime.min.time(), tzinfo=tz_kolkata)
        cutoff_ms   = int(cutoff_dt.timestamp() * 1000)
        cur = conn.cursor()
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' "
            "AND (tablename LIKE 'quote_%%' OR tablename LIKE 'depth_%%')"
        )
        tables = [r[0] for r in cur.fetchall()]
        total_deleted, affected = 0, 0
        for table in tables:
            cur.execute(f"DELETE FROM {table} WHERE ts_ms < %s", (cutoff_ms,))
            if cur.rowcount:
                total_deleted += cur.rowcount
                affected += 1
        conn.commit()
        print(
            f"[PG_LOCAL_WRITER] Pruned {total_deleted} row(s) across {affected} table(s) "
            f"older than {cutoff_date} (keeping last {n_trading_days} trading days)",
            flush=True,
        )

    def _commit_if_due(self, conn, state, shard_idx: int, force: bool = False):
        """
        Returns the connection to use going forward — usually the same
        `conn` passed in, but a FRESH one if the commit failed and had
        to reconnect. Callers must always do `conn = self._commit_if_due(...)`
        and keep using the returned value, never the original variable.

        Without a try/except here, ANY commit failure (a transient
        network blip, a Postgres-side timeout, a dropped connection)
        would propagate straight out of _run()'s while loop and kill
        this shard's thread silently — a daemon thread with no
        supervisor to notice or restart it. From that point on this
        shard's queue never drains again: enqueue_backfill_rows() for
        every symbol mapped to this dead shard blocks forever on a full
        queue, and whatever's already queued sits in RAM permanently.
        That's a real, previously-unhandled path to exactly the
        "fetched data but RAM never comes back down" symptom.
        """
        if state["pending_commit_rows"] == 0:
            return conn
        due = (
            force
            or state["pending_commit_rows"] >= self._commit_max_rows_per_shard
            or (state["txn_started_at"] is not None and time.time() - state["txn_started_at"] >= self.COMMIT_MAX_SECS)
        )
        if not due:
            return conn
        t0 = time.time()
        try:
            conn.commit()
        except Exception as exc:
            lost_rows = state["pending_commit_rows"]
            print(
                f"[PG_LOCAL_WRITER][ERROR] shard {shard_idx} commit failed "
                f"({lost_rows} row(s) in this transaction lost): {exc} — reconnecting",
                flush=True,
            )
            try:
                conn.close()
            except Exception:
                pass
            # Reconnect so this shard keeps working instead of being
            # permanently wedged — the rows in the failed transaction
            # are gone (never buffered separately for retry, same
            # trade-off SQLiteTickWriter makes on an insert failure),
            # but every symbol mapped to this shard keeps flowing
            # instead of hanging forever on a dead queue.
            conn = self._connect()
            state["pending_commit_rows"] = 0
            state["txn_started_at"] = None
            return conn
        dt = time.time() - t0
        state["commits_total"]        += 1
        state["commit_time_total"]    += dt
        state["last_commit_duration"]  = dt
        state["last_commit_rows"]      = state["pending_commit_rows"]
        state["last_txn_duration"]     = time.time() - (state["txn_started_at"] or t0)
        state["pending_commit_rows"]   = 0
        state["txn_started_at"]        = None
        return conn

    def _note_inserted(self, n: int, state: dict):
        if n <= 0:
            return
        if state["txn_started_at"] is None:
            state["txn_started_at"] = time.time()
        state["pending_commit_rows"] += n
        state["rows_written_total"]  += n

    def _run(self, shard_idx: int):
        live_q = self._live_queues[shard_idx]
        bulk_q = self._bulk_queues[shard_idx]
        state  = self._shard_state[shard_idx]

        conn = self._connect()
        batch = []
        last_batch_flush = time.time()
        loop_count = 0
        try:
            while True:
                stopping = self._stop.is_set() and live_q.empty() and bulk_q.empty()
                if not batch and stopping:
                    conn = self._commit_if_due(conn, state, shard_idx, force=True)
                    break

                loop_count += 1
                item = None
                if loop_count % 20 == 0:
                    # Periodic bulk-priority check — pure "live always
                    # wins" could otherwise starve bulk indefinitely
                    # under sustained heavy live traffic (live refilling
                    # just fast enough to never be empty at the check
                    # moment). Every 20th iteration, give bulk first
                    # crack instead, guaranteeing it always makes
                    # progress even during a busy live session.
                    try:
                        item = bulk_q.get_nowait()
                    except queue.Empty:
                        try:
                            item = live_q.get_nowait()
                        except queue.Empty:
                            item = None
                else:
                    # LIVE checked first, non-blocking. The fallback
                    # wait on bulk uses a SHORT timeout (not 0.25s) —
                    # blocking on bulk for any longer means this thread
                    # stops checking live_q entirely for that whole
                    # window. Live ticks don't arrive perfectly evenly
                    # spaced — a brief gap (live_q momentarily empty)
                    # followed by a burst (several ticks landing close
                    # together, e.g. quote+depth for busy symbols) is
                    # completely normal. A 250ms blind spot was enough
                    # for a burst to overflow the live queue with
                    # nothing draining it — this was happening
                    # continuously, independent of any bulk/backfill
                    # activity, which is why drops kept climbing across
                    # every shard even with no heavy backfill running.
                    try:
                        item = live_q.get_nowait()
                    except queue.Empty:
                        try:
                            item = bulk_q.get(timeout=0.02)
                        except queue.Empty:
                            item = None

                try:
                    if item is not None and item[0] == "bulk":
                        if batch:
                            self._note_inserted(self._flush_live(conn, batch, state), state)
                            batch = []
                            last_batch_flush = time.time()
                        _, symbol, rows, kind = item
                        self._note_inserted(self._flush_bulk(conn, symbol, rows, kind, state), state)
                    elif item is not None and item[0] == "prune":
                        if batch:
                            self._note_inserted(self._flush_live(conn, batch, state), state)
                            batch = []
                            last_batch_flush = time.time()
                        conn = self._commit_if_due(conn, state, shard_idx, force=True)
                        _, n_trading_days, done_event = item
                        self._prune_tables(conn, n_trading_days)
                        done_event.set()
                    elif item is not None and item[0] == "delete_range":
                        if batch:
                            self._note_inserted(self._flush_live(conn, batch, state), state)
                            batch = []
                            last_batch_flush = time.time()
                        conn = self._commit_if_due(conn, state, shard_idx, force=True)
                        _, symbol, start_ms, end_ms, kind, done_event = item
                        self._delete_range(conn, symbol, start_ms, end_ms, kind)
                        done_event.set()
                    elif item is not None and item[0] == "delete_timestamps":
                        if batch:
                            self._note_inserted(self._flush_live(conn, batch, state), state)
                            batch = []
                            last_batch_flush = time.time()
                        conn = self._commit_if_due(conn, state, shard_idx, force=True)
                        _, symbol, timestamps, kind, done_event = item
                        self._delete_timestamps(conn, symbol, timestamps, kind)
                        done_event.set()
                    elif item is not None and item[0] == "barrier":
                        if batch:
                            self._note_inserted(self._flush_live(conn, batch, state), state)
                            batch = []
                            last_batch_flush = time.time()
                        conn = self._commit_if_due(conn, state, shard_idx, force=True)
                        _, done_event = item
                        done_event.set()
                    elif item is not None and item[0] == "build_indexes":
                        if batch:
                            self._note_inserted(self._flush_live(conn, batch, state), state)
                            batch = []
                            last_batch_flush = time.time()
                        conn = self._commit_if_due(conn, state, shard_idx, force=True)
                        self._build_pending_indexes(conn)
                        _, done_event = item
                        done_event.set()
                    elif item is not None:
                        batch.append(item)

                    should_flush_batch = batch and (
                        len(batch) >= self.BATCH_SIZE
                        or time.time() - last_batch_flush >= self.BATCH_MAX_SECS
                        or stopping
                    )
                    if should_flush_batch:
                        self._note_inserted(self._flush_live(conn, batch, state), state)
                        batch = []
                        last_batch_flush = time.time()

                    conn = self._commit_if_due(conn, state, shard_idx, force=stopping)

                except Exception as exc:
                    # Catch-all safety net: _insert()/_commit_if_due()
                    # already handle the failures they can anticipate,
                    # but this guarantees NOTHING unexpected can kill
                    # this thread outright. A dead shard thread means
                    # its queue never drains again — every symbol
                    # mapped to it would block forever on
                    # enqueue_backfill_rows() and its queued data would
                    # sit in RAM permanently. Reconnecting and dropping
                    # whatever event/done_event was in-flight (if any)
                    # is a far better outcome than a silently wedged
                    # shard — any waiter on that done_event will simply
                    # time out and log a warning instead of hanging
                    # forever, which is the existing, already-handled
                    # behavior for a timeout.
                    print(
                        f"[PG_LOCAL_WRITER][ERROR] shard {shard_idx} hit an unexpected "
                        f"error, reconnecting and continuing: {exc}",
                        flush=True,
                    )
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self._connect()
                    batch = []
                    state["pending_commit_rows"] = 0
                    state["txn_started_at"] = None
        finally:
            conn.close()


# ═════════════════════════════════════════════════════════════════════
# TickWriter — the factory every other module imports and calls. Picks
# SQLiteTickWriter (default) or PostgresTickWriter based on
# TICK_WRITER_BACKEND, so `from tick_writer import TickWriter` plus
# `TickWriter(base_dir="tickdata")` keeps working unchanged everywhere
# in the codebase regardless of which backend is active — engine_runtime.py
# doesn't need an if/else, and nothing needs to know PostgresTickWriter
# exists at all unless it's explicitly selected.
# ═════════════════════════════════════════════════════════════════════

def TickWriter(base_dir: str = "tickdata", **kwargs):
    """
    TICK_WRITER_BACKEND=sqlite (default, or unset): SQLiteTickWriter,
    writing to base_dir/ticks.db — base_dir is required and used as
    before.

    TICK_WRITER_BACKEND=postgres: PostgresTickWriter, writing to a
    local PostgreSQL instance configured via PG_LOCAL_* env vars (see
    auto_setup() above) — base_dir is accepted but ignored, kept only
    so callers don't need a conditional just to construct this.
    """
    backend = os.getenv("TICK_WRITER_BACKEND", "sqlite").strip().lower()
    if backend == "postgres":
        return PostgresTickWriter(**kwargs)
    return SQLiteTickWriter(base_dir=base_dir, **kwargs)



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
