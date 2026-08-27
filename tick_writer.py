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

    def open_read_connection(self):
        """Open a connection suitable for passing as conn= to max_ts()/
        read_ticks() across repeated calls (e.g. a per-symbol loop).
        Caller owns it and must close it when done. Mirrors
        PostgresTickWriter.open_read_connection() so backend-agnostic
        callers like BackfillManager don't need to know which backend
        is active."""
        return sqlite3.connect(self._db_path(), timeout=5)

    def max_ts(self, symbol: str, kind: str = "quote", conn=None):
        """Most recent ts_ms already cached for symbol in quote_<symbol>
        (or depth_<symbol> if kind='depth'), or None. Opens its own
        short-lived read connection by default — infrequent, off the
        hot path, so no need to share the writer thread's connection.
        A local sqlite3.connect() is cheap (no network round-trip), so
        this doesn't need the same per-call-cost fix as
        PostgresTickWriter.max_ts() — the conn= param exists purely so
        callers that don't know which backend they're talking to (e.g.
        BackfillManager's Phase 1 loop) can pass one uniformly; a
        caller-supplied conn is reused and left open, otherwise a
        fresh one is opened and closed here as before."""
        path = self._db_path()
        if not os.path.exists(path):
            return None
        table = self.table_name(symbol, kind)
        own_conn = conn is None
        try:
            if own_conn:
                conn = sqlite3.connect(path, timeout=5)
            try:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone()
                if not exists:
                    return None
                row = conn.execute(f"SELECT MAX(ts_ms) FROM {table}").fetchone()
            finally:
                if own_conn:
                    conn.close()
            return int(row[0]) if row and row[0] is not None else None
        except Exception as exc:
            print(f"[TICK_WRITER][WARN] max_ts({symbol}) failed: {exc}", flush=True)
            return None

    def max_ts_batch(self, symbols: list, kind: str = "quote", conn=None) -> dict:
        """Batched version of max_ts() for interface parity with
        PostgresTickWriter.max_ts_batch(). SQLite has no per-call
        network round-trip to save, so this is just a loop over
        max_ts() reusing conn if given — the batching win is entirely
        on the Postgres side; this exists so BackfillManager's Phase 1
        loop can call the same method name regardless of backend."""
        own_conn = conn is None
        if own_conn:
            conn = self.open_read_connection()
        try:
            return {s: self.max_ts(s, kind=kind, conn=conn) for s in symbols}
        finally:
            if own_conn:
                conn.close()

    def read_ticks_batch(self, symbol_ranges: dict, kind: str = "quote", conn=None) -> dict:
        """Batched version of read_ticks() for interface parity with
        PostgresTickWriter.read_ticks_batch(). symbol_ranges:
        {symbol: (start_ms, end_ms)}. SQLite has no network round-trip
        to save, so this is just a loop reusing conn — exists purely so
        BackfillManager's Phase 1 loop can call one method name
        regardless of backend."""
        own_conn = conn is None
        if own_conn:
            conn = self.open_read_connection()
        try:
            return {
                s: self.read_ticks(s, start_ms=start_ms, end_ms=end_ms, kind=kind, conn=conn)
                for s, (start_ms, end_ms) in symbol_ranges.items()
            }
        finally:
            if own_conn:
                conn.close()

    def read_ticks(self, symbol: str, start_ms: int = None, end_ms: int = None, kind: str = "quote", conn=None):
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
        read connection by default — not on the hot write path, and a
        local sqlite3.connect() has no network cost, so this doesn't
        need PostgresTickWriter's per-call fix. conn= exists purely
        for interface parity with PostgresTickWriter.read_ticks() so
        backend-agnostic callers can pass one uniformly; if given, it's
        reused and left open instead of opened/closed here.
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

        own_conn = conn is None
        try:
            if own_conn:
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
                if own_conn:
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

    ── Live writer: a single dedicated thread, not sharded ──────────────
    Live tick volume is naturally rate-limited by the market feed itself
    (unlike backfill's "dump months of history at once" pattern), so it
    doesn't need Postgres's MVCC-driven cross-table concurrency the way
    bulk backfill throughput does — one thread comfortably keeps up.
    Live ticks for EVERY symbol flow through one queue (self._live_queue),
    one dedicated thread (_run_live()), one connection. Ordering per
    symbol is trivially guaranteed (single thread, natural queue order) —
    simpler than the shard-hash scheme below, which now exists purely
    for bulk/backfill throughput.

    ── Bulk writer pool: still sharded ──────────────────────────────────
    Every symbol is routed to exactly ONE bulk shard for its entire
    lifetime (via a deterministic hash — see _shard_for()), so all of
    one symbol's BACKFILL rows always pass through the SAME queue,
    processed by the SAME single thread, in arrival order. Postgres's
    MVCC lets genuinely concurrent writes to DIFFERENT tables proceed
    without blocking each other, unlike SQLite's single file-level write
    lock, so a pool is a real throughput lever here — this is where it
    actually matters, unlike the live path above.

    ── Live vs. backfill tables ─────────────────────────────────────────
    Unlike SQLiteTickWriter, live and backfilled ticks are written into
    physically SEPARATE tables — quote_<symbol>_live / quote_<symbol>_backfill
    (and depth_<symbol>_live / depth_<symbol>_backfill) — see table_name().
    _flush_live() always targets the "_live" table, _flush_bulk() (fed by
    enqueue_backfill_rows(), always called via BackfillManager) always
    targets the "_backfill" table.

    This removes the ordering race hold()/release() used to guard
    against: that gate existed only because live and backfilled rows
    used to share ONE table's timeline, so a live tick landing before an
    earlier-timestamped historical row (still in flight from Postgres)
    would have corrupted max_ts()'s "what's already cached" resume-point
    logic. With separate tables there's no shared timeline to corrupt —
    live ticks can be written the instant they arrive, with zero wait on
    backfill. hold()/release() are kept as no-ops purely so this class
    stays a drop-in replacement for SQLiteTickWriter (which still needs
    them) — callers (engine_runtime.py) don't need to know or care which
    backend is active.

    Read methods (max_ts(), read_ticks(), delete_range(),
    delete_timestamps()) default to source="backfill", matching what
    BackfillManager's own cache-check / gap-detection / resume-point
    logic actually needs — it must only ever see its own backfilled
    history, never live ticks, or it could wrongly conclude a date range
    is already covered and silently skip re-fetching a real gap. Pass
    source="live" explicitly for anything that needs the live table
    instead, or read both tables and UNION them for a combined
    live+backfill view (e.g. building candles from full tick history).

    ── RAM ceiling kept from multiplying by pool size ──────────────────
    BULK_QUEUE_MAXSIZE and COMMIT_MAX_ROWS (bulk side) are divided by
    the bulk pool size (see __init__) so the AGGREGATE worst-case bulk
    backlog across every shard stays roughly where a single writer's
    ceiling was, instead of silently multiplying by 9x just because
    there are 9 bulk queues/9 open transactions now. LIVE_QUEUE_MAXSIZE
    is NOT divided — there's only one live queue now, so it gets the
    full ceiling.

    ── Cross-pool maintenance ops ────────────────────────────────────────
    flush_and_wait() fans a barrier out to every bulk shard AND the
    live queue, waiting for all of them. build_pending_indexes()/
    prune_older_than_days() are DB-wide operations (not per-symbol), so
    they call flush_and_wait() first (ensuring every shard's AND the
    live thread's pending work is durable), then run once against bulk
    shard 0's connection only — no need to run them once per
    shard/thread, any single connection can CREATE INDEX / DELETE across
    every table in the database.
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

    # Statement timeout (ms) applied to open_read_connection()'s
    # connection — see that method's docstring. This connection is only
    # used for background cache-check queries (max_ts_batch/
    # read_ticks_batch), never anything latency-sensitive, so it's set
    # generously rather than inheriting a tight server-side default
    # meant for normal query traffic.
    _BACKGROUND_STATEMENT_TIMEOUT_MS = 120_000

    def __init__(self):
        self._params = auto_setup()

        self.pool_size = max(1, int(os.getenv("PG_LOCAL_WRITER_THREADS", str(self.WRITER_POOL_SIZE))))

        # Divided by BULK pool size only — see class docstring's "RAM
        # ceiling" section. Floored so a large pool_size can't shrink
        # this to something degenerate (e.g. a queue that can't even
        # hold one bulk chunk).
        self._bulk_queue_maxsize_per_shard = max(4, self.BULK_QUEUE_MAXSIZE // self.pool_size)
        self._commit_max_rows_per_shard    = max(1_000, self.COMMIT_MAX_ROWS // self.pool_size)

        self._bulk_queues = [queue.Queue(maxsize=self._bulk_queue_maxsize_per_shard) for _ in range(self.pool_size)]
        # Single dedicated live queue/thread — not sharded, not divided
        # across pool_size. See class docstring's "Live writer" section.
        self._live_queue = queue.Queue(maxsize=self.LIVE_QUEUE_MAXSIZE)
        self._stop   = threading.Event()

        # No hold/release buffering state — see class docstring's "Live
        # vs. backfill tables" section. hold()/release() below are kept
        # as no-ops purely for drop-in compatibility with
        # SQLiteTickWriter, which still needs the gate.

        # Per-BULK-shard state — each dict is only ever mutated by its
        # OWN shard thread (inside _run_bulk(shard_idx)), so no locking
        # is needed for any of these; get_metrics() only READS them.
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

        # Live thread's own state — same shape as one bulk shard's, but
        # a single dict since there's only one live thread. Only ever
        # mutated by _run_live(); get_metrics() only reads it.
        self._live_state = {
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

        # SHARED across bulk shards (a table can only ever be created by
        # the one shard that owns its symbol, but build_pending_indexes()
        # needs to see every shard's pending tables, not just shard 0's)
        # — writes are rare (once per symbol, at first table creation),
        # so a simple lock is more than sufficient, no contention risk.
        self._pending_index_tables = set()
        self._pending_index_lock   = threading.Lock()

        # Detected ONCE, up front, via a single throwaway connection —
        # not per-shard — so every shard/thread agrees on whether this
        # is a fresh db, and so nothing independently races to query
        # pg_tables at startup.
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
            t = threading.Thread(target=self._run_bulk, name=f"pg-bulk-writer-{i}", args=(i,), daemon=True)
            t.start()
            self._threads.append(t)

        self._live_thread = threading.Thread(target=self._run_live, name="pg-live-writer", daemon=True)
        self._live_thread.start()
        self._threads.append(self._live_thread)

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
        try:
            self._live_queue.put_nowait(("live", symbol, kind, dict(snapshot)))
        except queue.Full:
            self._dropped_live_ticks += 1
            if self._dropped_live_ticks == 1 or self._dropped_live_ticks % 500 == 0:
                print(
                    f"[PG_LOCAL_WRITER][WARN] LIVE queue full — dropped live tick "
                    f"for {symbol} ({self._dropped_live_ticks} dropped total)",
                    flush=True,
                )

    def hold(self):
        """No-op — live and backfill write to separate tables now, so
        there's no ordering race left to guard against. Kept only so
        this class stays a drop-in replacement for SQLiteTickWriter
        (which still needs a real gate) — see class docstring."""
        pass

    # Back-compat alias, matching TickWriter's — nothing in this repo
    # currently calls it, kept only so PostgresTickWriter is a true
    # drop-in.
    def enqueue(self, symbol: str, snapshot: dict):
        self.enqueue_live(symbol, "depth", snapshot)

    def release(self):
        """No-op — see hold()."""
        pass

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
        Fans a barrier out to every BULK shard's queue AND the single
        live queue, and waits for all of them — guarantees everything
        queued before this call, on any of them, is committed by the
        time it returns. They run concurrently, so the real wall-clock
        cost is bounded by the slowest one, not the sum of all of them.
        """
        events = []
        for q in list(self._bulk_queues) + [self._live_queue]:
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

    def delete_range(self, symbol: str, start_ms: int, end_ms=None, kind: str = "quote", source: str = "backfill", timeout: float = 30):
        done = threading.Event()
        self._bulk_queues[self._shard_for(symbol)].put(("delete_range", symbol, start_ms, end_ms, kind, source, done))
        if not done.wait(timeout=timeout):
            print(f"[PG_LOCAL_WRITER][WARN] delete_range({symbol}) timed out", flush=True)

    def delete_timestamps(self, symbol: str, timestamps, kind: str = "quote", source: str = "backfill", wait: bool = True, timeout: float = 30):
        timestamps = [int(t) for t in timestamps]
        if not timestamps:
            return
        done = threading.Event()
        self._bulk_queues[self._shard_for(symbol)].put(("delete_timestamps", symbol, timestamps, kind, source, done))
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
        bulk_rows_written  = sum(s["rows_written_total"] for s in self._shard_state)
        bulk_commits_total = sum(s["commits_total"] for s in self._shard_state)
        bulk_commit_time   = sum(s["commit_time_total"] for s in self._shard_state)
        rows_written_total = bulk_rows_written + self._live_state["rows_written_total"]
        commits_total       = bulk_commits_total + self._live_state["commits_total"]
        commit_time_total   = bulk_commit_time + self._live_state["commit_time_total"]
        bulk_queue_depth = sum(q.qsize() for q in self._bulk_queues)
        live_queue_depth = self._live_queue.qsize()
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
            "live": {
                "queue_depth":        live_queue_depth,
                "rows_written_total": self._live_state["rows_written_total"],
                "commits_total":      self._live_state["commits_total"],
                "last_commit_secs":   self._live_state["last_commit_duration"],
                "last_commit_rows":   self._live_state["last_commit_rows"],
                "last_txn_secs":      self._live_state["last_txn_duration"],
            },
            "per_bulk_shard": [
                {
                    "bulk_queue_depth":   self._bulk_queues[i].qsize(),
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

    def table_name(self, symbol: str, kind: str, source: str = "backfill") -> str:
        """source: 'live' or 'backfill' — see class docstring's "Live vs.
        backfill tables" section for why these are physically separate."""
        return f"{kind}_{_safe_symbol(symbol)}_{source}"

    def read_ticks(self, symbol: str, start_ms=None, end_ms=None, kind: str = "quote",
                    source: str = "backfill", conn=None):
        """Same return shape as TickWriter.read_ticks(): list of dicts,
        {"timestamp","ltp","qty"} for quote, {"timestamp","ltp",
        <DEPTH_LEVEL_COLUMNS...>} for depth. source defaults to
        "backfill" — see class docstring.

        conn: optional pre-opened psycopg2 connection to reuse instead
        of opening a fresh one. Callers making many calls back-to-back
        (e.g. a per-symbol cache-check loop) should open one connection
        up front and pass it in here to avoid paying TCP/auth setup
        cost per call — see max_ts() below, same pattern. When conn is
        passed in, this method never closes it; that's the caller's
        responsibility."""
        table = self.table_name(symbol, kind, source)
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

        own_conn = conn is None
        try:
            if own_conn:
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
                if own_conn:
                    conn.close()
            out_cols = ["timestamp", "ltp"] + (list(DEPTH_LEVEL_COLUMNS) if kind == "depth" else ["qty"])
            return [dict(zip(out_cols, r)) for r in rows]
        except Exception as exc:
            print(f"[PG_LOCAL_WRITER][WARN] read_ticks({symbol}) failed: {exc}", flush=True)
            if conn is not None and not own_conn:
                # This is a shared connection the caller will keep using
                # (e.g. read_ticks_batch's bisection base case) — this
                # whole call is read-only, so a full rollback can't lose
                # any committed work, and it's required here: without
                # it, the failed statement leaves the connection's
                # transaction "aborted", and EVERY later query on it —
                # including just creating a SAVEPOINT for an unrelated
                # sibling chunk — fails too, cascading a single genuine
                # timeout into a total batch failure. Confirmed in
                # testing: one slow/large table's query timing out here
                # otherwise took down every other symbol's result in
                # the same read_ticks_batch() call.
                try:
                    conn.rollback()
                except Exception:
                    pass
            return []

    def read_ticks_combined(self, symbol: str, start_ms=None, end_ms=None, kind: str = "quote"):
        """Live + backfill tables UNIONed and re-sorted by timestamp —
        for consumers that want the full tick history regardless of
        which table a row happens to live in (e.g. building candles
        from raw ticks). BackfillManager's own cache-check should NOT
        use this — it needs read_ticks(source="backfill") specifically,
        see class docstring."""
        live     = self.read_ticks(symbol, start_ms, end_ms, kind, source="live")
        backfill = self.read_ticks(symbol, start_ms, end_ms, kind, source="backfill")
        return sorted(live + backfill, key=lambda r: r["timestamp"])

    def open_read_connection(self):
        """Open a connection suitable for passing as conn= to max_ts()/
        read_ticks() across repeated calls (e.g. a per-symbol loop),
        so the caller pays the TCP-handshake-plus-auth cost once
        instead of once per call. Caller owns it and must close it
        when done.

        Sets a generous statement_timeout on this connection —
        read_ticks_batch()'s UNION ALL queries can legitimately scan a
        lot of data across many symbols/tables at once, and this
        connection is only ever used for background cache-check work
        (never anything user-facing), so there's no reason to inherit
        whatever tight server-side default statement_timeout is
        configured for normal query traffic. Without this, a batch
        query that's merely SLOW (not actually stuck) gets cancelled by
        Postgres, which used to trigger a fallback to per-symbol
        queries for the whole chunk — recreating the exact per-symbol
        slowdown this batching was meant to fix."""
        conn = psycopg2.connect(**self._params)
        try:
            cur = conn.cursor()
            cur.execute(f"SET statement_timeout = {self._BACKGROUND_STATEMENT_TIMEOUT_MS}")
            conn.commit()
        except Exception as exc:
            print(f"[PG_LOCAL_WRITER][WARN] could not raise statement_timeout on read connection: {exc}", flush=True)
        return conn

    def max_ts(self, symbol: str, kind: str = "quote", source: str = "backfill", conn=None):
        """conn: optional pre-opened psycopg2 connection to reuse. Without
        it, every call pays a fresh TCP handshake + Postgres auth
        negotiation — fine for one-off calls, but ruinous in a tight
        per-symbol loop (e.g. BackfillManager's Phase 1 cache-check,
        which calls this once per symbol for ~199 symbols). Callers
        doing that should open one connection up front, pass it in
        here on every iteration, and close it themselves once the loop
        is done. When conn is passed in, this method never closes it."""
        table = self.table_name(symbol, kind, source)
        own_conn = conn is None
        try:
            if own_conn:
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
                if own_conn:
                    conn.close()
        except Exception as exc:
            print(f"[PG_LOCAL_WRITER][WARN] max_ts({symbol}) failed: {exc}", flush=True)
            if conn is not None and not own_conn:
                # Same reasoning as read_ticks()'s except block — a
                # shared connection must be rolled back on failure or
                # every later query on it (even in unrelated chunks)
                # fails too. Read-only call, so nothing is lost.
                try:
                    conn.rollback()
                except Exception:
                    pass
            return None

    def max_ts_batch(self, symbols: list, kind: str = "quote", source: str = "backfill", conn=None) -> dict:
        """Batched version of max_ts() for a whole symbol list — returns
        {symbol: last_ts_ms_or_None}.

        max_ts() called once per symbol in a Python loop still costs one
        network round-trip per call even with a shared connection (199
        round-trips for 199 symbols, each a real TCP send/recv even on
        localhost). This does it in ~2 round-trips total regardless of
        symbol count:

          1. One "which of these tables exist" query using
             tablename = ANY(%s) instead of 199 single-table checks.
          2. One UNION ALL query — one `SELECT %s AS symbol,
             MAX(ts_ms) AS max_ts FROM {table}` branch per EXISTING
             table — executed as a single statement, chunked at
             _BATCH_CHUNK_SIZE branches per query to keep individual
             statements from growing unreasonably large with very big
             symbol lists.

        Table names are still interpolated directly (table_name() /
        _safe_symbol() restrict to alnum+underscore already, same as
        every other method here); the symbol label in each branch is
        passed as a %s parameter, not interpolated, so it can't affect
        the query even if a symbol ever contained unexpected characters.

        ── One bad table must not blank out everyone else ──────────────
        If ANY branch in a UNION ALL chunk fails (e.g. one symbol's
        table has a corrupt/mismatched column type), Postgres aborts
        the WHOLE transaction on that connection — every later query on
        that same connection then fails too with "current transaction
        is aborted" until a rollback happens. Since this method is
        called with one shared connection reused across the whole
        cache-check loop, a single broken symbol could otherwise poison
        every OTHER symbol's result for the rest of the run (verified:
        one corrupt table among 20 healthy ones made all 20 come back
        as "no cached data", not just the bad one) — BackfillManager
        would then think it needs to re-fetch everything for symbols
        that were actually fine, and depending on timing that can leave
        a symbol's live feed running before its backfill has caught up.

        Each chunk therefore runs inside its own SAVEPOINT: if the
        chunk's UNION ALL fails, we roll back to the savepoint (which
        un-poisons the connection without needing a new one) and retry
        that chunk's symbols ONE AT A TIME via max_ts(), so only the
        genuinely broken symbol(s) get logged as failed — every healthy
        symbol in that chunk still gets its real answer.
        """
        if not symbols:
            return {}

        table_by_symbol = {s: self.table_name(s, kind, source) for s in symbols}
        result = {s: None for s in symbols}

        own_conn = conn is None
        try:
            if own_conn:
                conn = psycopg2.connect(**self._params)
            try:
                cur = conn.cursor()

                # Step 1: which tables actually exist — one round-trip.
                # Wrapped in its own savepoint too: it's low-risk (just
                # reading pg_tables, not touching per-symbol table
                # content) but costs nothing to protect the same way.
                cur.execute("SAVEPOINT sp_exist_check")
                try:
                    all_tables = list(table_by_symbol.values())
                    cur.execute(
                        "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename = ANY(%s)",
                        (all_tables,),
                    )
                    existing_tables = {row[0] for row in cur.fetchall()}
                    cur.execute("RELEASE SAVEPOINT sp_exist_check")
                except Exception as exc:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_exist_check")
                    raise

                existing_symbols = [s for s, t in table_by_symbol.items() if t in existing_tables]
                if not existing_symbols:
                    return result

                # Step 2: one UNION ALL query per chunk for MAX(ts_ms)
                # across every existing table, each chunk isolated by
                # its own savepoint so a bad table in one chunk can't
                # affect other chunks OR other symbols in the same chunk.
                _BATCH_CHUNK_SIZE = 150
                for start in range(0, len(existing_symbols), _BATCH_CHUNK_SIZE):
                    chunk = existing_symbols[start:start + _BATCH_CHUNK_SIZE]
                    branches = []
                    params = []
                    for s in chunk:
                        branches.append(f"SELECT %s AS symbol, MAX(ts_ms) AS max_ts FROM {table_by_symbol[s]}")
                        params.append(s)
                    query = " UNION ALL ".join(branches)

                    cur.execute("SAVEPOINT sp_batch_chunk")
                    try:
                        cur.execute(query, params)
                        for sym, max_ts in cur.fetchall():
                            result[sym] = max_ts
                        cur.execute("RELEASE SAVEPOINT sp_batch_chunk")
                    except Exception as exc:
                        # Un-poison the connection first, THEN fall back
                        # — falling back without this rollback is what
                        # made every symbol in the chunk look empty.
                        cur.execute("ROLLBACK TO SAVEPOINT sp_batch_chunk")
                        print(
                            f"[PG_LOCAL_WRITER][WARN] max_ts_batch chunk failed ({exc}); "
                            f"falling back to per-symbol checks for this chunk ({len(chunk)} symbols) "
                            f"so only the actually-broken symbol(s) are affected",
                            flush=True,
                        )
                        for s in chunk:
                            result[s] = self.max_ts(s, kind=kind, source=source, conn=conn)
            finally:
                if own_conn:
                    conn.close()
        except Exception as exc:
            print(f"[PG_LOCAL_WRITER][WARN] max_ts_batch failed: {exc}", flush=True)
            # Last-resort fallback if something broke before/outside the
            # per-chunk handling above (e.g. the connection itself is
            # bad) — a fresh connection per symbol here since the shared
            # one may be unusable.
            for s in symbols:
                if result[s] is None:
                    result[s] = self.max_ts(s, kind=kind, source=source, conn=None)
        return result

    def _read_ticks_chunk_with_bisection(self, cur, chunk, table_by_symbol, symbol_ranges,
                                          select_cols_sql, out_cols, result, conn, depth=0):
        """Try one UNION ALL chunk; on failure (including a statement
        timeout — a chunk that's just SLOW, not necessarily broken),
        split it in half and retry each half instead of immediately
        collapsing to a full per-symbol loop for the WHOLE chunk.

        Why this matters: falling all the way back to N individual
        queries for a merely-slow-but-otherwise-fine chunk recreates
        exactly the per-symbol round-trip cost this batching exists to
        avoid — confirmed in production, where a 150-symbol chunk hit
        statement_timeout and the resulting per-symbol fallback caused
        the same live-queue-overflow this whole change was meant to
        fix. Bisecting means only the genuinely problematic symbol(s)
        (if any — timeouts are often just "this much data takes this
        long", not a broken table) end up paying the per-symbol cost;
        everything else in the chunk still benefits from batching at a
        smaller size.

        Bottoms out at per-symbol reads once a "chunk" is down to a
        single symbol — that's the only case truly equivalent to the
        old fully-unbatched behavior, and only for whichever symbol(s)
        actually need it.
        """
        if len(chunk) == 1:
            s = chunk[0]
            start_ms, end_ms = symbol_ranges[s]
            result[s] = self.read_ticks(s, start_ms=start_ms, end_ms=end_ms, conn=conn)
            return

        branches, params = [], []
        for s in chunk:
            start_ms, end_ms = symbol_ranges[s]
            branches.append(
                f"SELECT %s AS symbol, {select_cols_sql} FROM {table_by_symbol[s]} "
                f"WHERE ts_ms >= %s AND ts_ms <= %s"
            )
            params.extend([s, int(start_ms), int(end_ms)])
        query = " UNION ALL ".join(branches) + " ORDER BY symbol, ts_ms"

        savepoint = f"sp_rt_chunk_{depth}_{len(chunk)}"
        cur.execute(f"SAVEPOINT {savepoint}")
        try:
            cur.execute(query, params)
            for row in cur.fetchall():
                sym = row[0]
                result[sym].append(dict(zip(out_cols, row[1:])))
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        except Exception as exc:
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            mid = len(chunk) // 2
            print(
                f"[PG_LOCAL_WRITER][WARN] read_ticks_batch chunk of {len(chunk)} symbol(s) "
                f"failed ({exc}); bisecting into {mid} + {len(chunk) - mid} and retrying "
                f"rather than falling back to {len(chunk)} individual queries",
                flush=True,
            )
            self._read_ticks_chunk_with_bisection(
                cur, chunk[:mid], table_by_symbol, symbol_ranges, select_cols_sql, out_cols, result, conn, depth + 1
            )
            self._read_ticks_chunk_with_bisection(
                cur, chunk[mid:], table_by_symbol, symbol_ranges, select_cols_sql, out_cols, result, conn, depth + 1
            )

    def read_ticks_batch(self, symbol_ranges: dict, kind: str = "quote", source: str = "backfill",
                          conn=None) -> dict:
        """Batched version of read_ticks() for multiple symbols at once —
        symbol_ranges: {symbol: (start_ms, end_ms)}. Returns
        {symbol: [row_dict, ...]} — same row shape read_ticks() returns.

        This is the read_ticks() twin of max_ts_batch() above, and for
        the same reason: a per-symbol loop calling read_ticks() for
        ~199 symbols does ~199 real network round-trips even with a
        shared connection — and unlike max_ts (a single MAX() scalar),
        each read_ticks() call pulls back a symbol's FULL cached tick
        range, so on a live system (most symbols already have cached
        data) this is actually the heavier of the two per-symbol costs,
        not max_ts. Collapses to ~2 round-trips total via the same
        two-step pattern: one existence check with tablename = ANY(%s),
        then one UNION ALL per chunk — each branch tagged with its own
        symbol as a bound %s parameter so results can be split back out
        per symbol after fetching.

        Same savepoint-per-chunk isolation as max_ts_batch(): one bad
        table can't blank out every other symbol's results — see that
        method's docstring for the full explanation of why that
        matters when reusing one shared connection.
        """
        if not symbol_ranges:
            return {}

        table_by_symbol = {s: self.table_name(s, kind, source) for s in symbol_ranges}
        result = {s: [] for s in symbol_ranges}

        if kind == "depth":
            select_cols = ["ts_ms", "ltp"] + list(DEPTH_LEVEL_COLUMNS)
        else:
            select_cols = ["ts_ms", "ltp", "qty"]
        out_cols = ["timestamp", "ltp"] + (list(DEPTH_LEVEL_COLUMNS) if kind == "depth" else ["qty"])
        select_cols_sql = ", ".join(select_cols)

        own_conn = conn is None
        try:
            if own_conn:
                conn = psycopg2.connect(**self._params)
            try:
                cur = conn.cursor()

                cur.execute("SAVEPOINT sp_rt_exist_check")
                try:
                    all_tables = list(table_by_symbol.values())
                    cur.execute(
                        "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename = ANY(%s)",
                        (all_tables,),
                    )
                    existing_tables = {row[0] for row in cur.fetchall()}
                    cur.execute("RELEASE SAVEPOINT sp_rt_exist_check")
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_rt_exist_check")
                    raise

                existing_symbols = [s for s, t in table_by_symbol.items() if t in existing_tables]
                if not existing_symbols:
                    return result

                # Chunk size kept smaller than max_ts_batch's — each
                # branch here pulls back a symbol's FULL cached tick
                # range (potentially thousands of rows), not a single
                # scalar, so a 150-symbol chunk can be a genuinely large
                # query. Confirmed in production: 150-symbol chunks were
                # hitting Postgres's statement_timeout and cancelling
                # the whole chunk.
                _BATCH_CHUNK_SIZE = 40
                for start in range(0, len(existing_symbols), _BATCH_CHUNK_SIZE):
                    chunk = existing_symbols[start:start + _BATCH_CHUNK_SIZE]
                    self._read_ticks_chunk_with_bisection(
                        cur, chunk, table_by_symbol, symbol_ranges, select_cols_sql, out_cols, result, conn
                    )
            finally:
                if own_conn:
                    conn.close()
        except Exception as exc:
            print(f"[PG_LOCAL_WRITER][WARN] read_ticks_batch failed: {exc}", flush=True)
            for s, (start_ms, end_ms) in symbol_ranges.items():
                if not result[s]:
                    result[s] = self.read_ticks(s, start_ms=start_ms, end_ms=end_ms, kind=kind, source=source, conn=None)
        return result

    # ─────────────────────────────────────────────
    # Writer thread internals — every method below takes an explicit
    # `state` dict (a bulk shard's own self._shard_state[shard_idx], or
    # self._live_state for the single live thread) and/or a worker
    # label for logging. No locking anywhere in here: each worker's
    # state dict, connection, and batch are touched by exactly one
    # thread for that worker's entire lifetime.
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
            table = self.table_name(symbol, kind, "live")
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
        table = self.table_name(symbol, kind, "backfill")
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

    def _delete_range(self, conn, symbol, start_ms, end_ms, kind, source):
        table = self.table_name(symbol, kind, source)
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

    def _delete_timestamps(self, conn, symbol, timestamps, kind, source):
        table = self.table_name(symbol, kind, source)
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

    def _commit_if_due(self, conn, state, worker_label, commit_max_rows: int, force: bool = False):
        """
        Returns the connection to use going forward — usually the same
        `conn` passed in, but a FRESH one if the commit failed and had
        to reconnect. Callers must always do `conn = self._commit_if_due(...)`
        and keep using the returned value, never the original variable.

        worker_label is just for log messages (a bulk shard index or
        "live"). commit_max_rows is that worker's own threshold — bulk
        shards use self._commit_max_rows_per_shard (aggregate budget
        divided across the pool), the live thread uses the full
        self.COMMIT_MAX_ROWS (it's the only live writer, no dividing).

        Without a try/except here, ANY commit failure (a transient
        network blip, a Postgres-side timeout, a dropped connection)
        would propagate straight out of the run loop and kill this
        worker's thread silently — a daemon thread with no supervisor
        to notice or restart it. From that point on this worker's
        queue never drains again: enqueue_backfill_rows()/enqueue_live()
        for every symbol mapped to it blocks forever on a full queue,
        and whatever's already queued sits in RAM permanently. That's a
        real, previously-unhandled path to exactly the "fetched data
        but RAM never comes back down" symptom.
        """
        if state["pending_commit_rows"] == 0:
            return conn
        due = (
            force
            or state["pending_commit_rows"] >= commit_max_rows
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
                f"[PG_LOCAL_WRITER][ERROR] {worker_label} commit failed "
                f"({lost_rows} row(s) in this transaction lost): {exc} — reconnecting",
                flush=True,
            )
            try:
                conn.close()
            except Exception:
                pass
            # Reconnect so this worker keeps working instead of being
            # permanently wedged — the rows in the failed transaction
            # are gone (never buffered separately for retry, same
            # trade-off SQLiteTickWriter makes on an insert failure),
            # but every symbol mapped to it keeps flowing instead of
            # hanging forever on a dead queue.
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

    def _run_bulk(self, shard_idx: int):
        """
        One of self.pool_size bulk shard threads. Handles ONLY backfill
        rows (enqueue_backfill_rows()) plus the DB-wide admin ops
        (prune/delete/barrier/build_indexes) that piggyback on shard 0's
        connection — see class docstring. Live ticks never flow through
        here anymore (see _run_live()), so there's no batch-accumulation
        or live/bulk priority juggling left to do — just drain bulk_q.
        """
        bulk_q = self._bulk_queues[shard_idx]
        state  = self._shard_state[shard_idx]
        label  = f"bulk shard {shard_idx}"

        conn = self._connect()
        try:
            while True:
                stopping = self._stop.is_set() and bulk_q.empty()
                if stopping:
                    conn = self._commit_if_due(conn, state, label, self._commit_max_rows_per_shard, force=True)
                    break

                try:
                    item = bulk_q.get(timeout=0.25)
                except queue.Empty:
                    conn = self._commit_if_due(conn, state, label, self._commit_max_rows_per_shard, force=False)
                    continue

                try:
                    kind0 = item[0]
                    if kind0 == "bulk":
                        _, symbol, rows, kind = item
                        self._note_inserted(self._flush_bulk(conn, symbol, rows, kind, state), state)
                    elif kind0 == "prune":
                        conn = self._commit_if_due(conn, state, label, self._commit_max_rows_per_shard, force=True)
                        _, n_trading_days, done_event = item
                        self._prune_tables(conn, n_trading_days)
                        done_event.set()
                    elif kind0 == "delete_range":
                        conn = self._commit_if_due(conn, state, label, self._commit_max_rows_per_shard, force=True)
                        _, symbol, start_ms, end_ms, kind, source, done_event = item
                        self._delete_range(conn, symbol, start_ms, end_ms, kind, source)
                        done_event.set()
                    elif kind0 == "delete_timestamps":
                        conn = self._commit_if_due(conn, state, label, self._commit_max_rows_per_shard, force=True)
                        _, symbol, timestamps, kind, source, done_event = item
                        self._delete_timestamps(conn, symbol, timestamps, kind, source)
                        done_event.set()
                    elif kind0 == "barrier":
                        conn = self._commit_if_due(conn, state, label, self._commit_max_rows_per_shard, force=True)
                        _, done_event = item
                        done_event.set()
                    elif kind0 == "build_indexes":
                        conn = self._commit_if_due(conn, state, label, self._commit_max_rows_per_shard, force=True)
                        self._build_pending_indexes(conn)
                        _, done_event = item
                        done_event.set()

                    conn = self._commit_if_due(conn, state, label, self._commit_max_rows_per_shard, force=False)

                except Exception as exc:
                    # Catch-all safety net — see _run_live()'s matching
                    # block for why this must never let the thread die.
                    print(
                        f"[PG_LOCAL_WRITER][ERROR] {label} hit an unexpected "
                        f"error, reconnecting and continuing: {exc}",
                        flush=True,
                    )
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = self._connect()
                    state["pending_commit_rows"] = 0
                    state["txn_started_at"] = None
        finally:
            conn.close()

    def _run_live(self):
        """
        Single dedicated live writer thread — see class docstring's
        "Live writer" section for why one thread is enough here (unlike
        bulk backfill, live volume is naturally rate-limited by the
        market feed). Every symbol's live ticks flow through the one
        queue this drains, batched the same way the old combined loop
        batched them (BATCH_SIZE / BATCH_MAX_SECS), just without any
        bulk-priority juggling since bulk never shows up here.
        """
        live_q = self._live_queue
        state  = self._live_state
        label  = "live writer"

        conn = self._connect()
        batch = []
        last_batch_flush = time.time()
        try:
            while True:
                stopping = self._stop.is_set() and live_q.empty()
                if not batch and stopping:
                    conn = self._commit_if_due(conn, state, label, self.COMMIT_MAX_ROWS, force=True)
                    break

                try:
                    item = live_q.get(timeout=0.25)
                except queue.Empty:
                    item = None

                try:
                    if item is not None and item[0] == "barrier":
                        if batch:
                            self._note_inserted(self._flush_live(conn, batch, state), state)
                            batch = []
                            last_batch_flush = time.time()
                        conn = self._commit_if_due(conn, state, label, self.COMMIT_MAX_ROWS, force=True)
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

                    conn = self._commit_if_due(conn, state, label, self.COMMIT_MAX_ROWS, force=stopping)

                except Exception as exc:
                    # Catch-all safety net: _insert()/_commit_if_due()
                    # already handle the failures they can anticipate,
                    # but this guarantees NOTHING unexpected can kill
                    # this thread outright. A dead live thread means
                    # enqueue_live() blocks/drops for EVERY symbol
                    # (there's only one live thread now, not 9 shards
                    # to fall back on) — reconnecting and dropping
                    # whatever was in-flight is a far better outcome
                    # than a permanently wedged live writer.
                    print(
                        f"[PG_LOCAL_WRITER][ERROR] {label} hit an unexpected "
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
