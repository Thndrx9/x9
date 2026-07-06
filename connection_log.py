"""
connection_log.py — SQLite-backed connection event log.

Tracks websocket connect/disconnect events so BackfillManager can derive
exact gap windows from a log instead of re-scanning quote data.

Event types:
    DAY_STARTED      — first successful connection of the trading day
    RECONNECTED      — any subsequent successful connection the same day
    DISCONNECTED     — websocket dropped
    DAY_NOT_STARTED  — retroactively marked when a past trading day has
                        zero connection events (process never came up)

Why SQLite:
    - Zero network dependency — must work even if PostgreSQL is down,
      which is often exactly when you need to know a disconnect happened
    - Fast local writes on every connect/disconnect (rare events, not hot path)
    - Matches the existing pattern already used for live quote storage
"""

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

DB_FILENAME = "connection_log.db"

ConnEvent = Tuple[str, int, Optional[str]]   # (event, ts_ms, mode)


def _db_path(base_dir: str) -> Path:
    return Path(base_dir) / DB_FILENAME


def _connect(base_dir: str) -> sqlite3.Connection:
    path = _db_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=5)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connection_events (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            event  TEXT    NOT NULL,
            ts_ms  INTEGER NOT NULL,
            day    TEXT    NOT NULL,
            mode   TEXT,
            note   TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_conn_day ON connection_events(day)"
    )
    conn.commit()
    return conn


def log_event(
    base_dir: str,
    event: str,
    when: datetime,
    mode: Optional[str] = None,
    note: Optional[str] = None,
    day: Optional[date] = None,
) -> None:
    """
    Record a connection event. `when` must be timezone-aware (IST recommended)
    and is used as the event's actual timestamp.

    `day` determines which trading day this event is filed under — defaults
    to when.date(). Pass it explicitly for retroactive events (e.g. marking
    DAY_NOT_STARTED for a *previous* day while `when` is the current
    detection time).

    Never raises — a failed log write should never crash the websocket.
    """
    try:
        bucket_day = (day or when.date()).isoformat()
        ts_ms      = int(when.astimezone(timezone.utc).timestamp() * 1000)
        conn       = _connect(base_dir)
        try:
            conn.execute(
                "INSERT INTO connection_events (event, ts_ms, day, mode, note) "
                "VALUES (?, ?, ?, ?, ?)",
                (event, ts_ms, bucket_day, mode, note),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        print(f"[CONN_LOG][WARN] failed to write '{event}': {exc}", flush=True)


def get_events_for_day(base_dir: str, day: date) -> List[ConnEvent]:
    """Return all events for `day`, sorted chronologically. Empty if none."""
    try:
        conn = _connect(base_dir)
        try:
            cur = conn.execute(
                "SELECT event, ts_ms, mode FROM connection_events "
                "WHERE day = ? ORDER BY ts_ms ASC",
                (day.isoformat(),),
            )
            return [(row[0], row[1], row[2]) for row in cur.fetchall()]
        finally:
            conn.close()
    except Exception as exc:
        print(f"[CONN_LOG][WARN] failed to read events for {day}: {exc}", flush=True)
        return []


def has_event_today(base_dir: str, event: str, when: datetime) -> bool:
    """Check whether `event` has already been logged for when.date()."""
    for logged_event, _ts_ms, _mode in get_events_for_day(base_dir, when.date()):
        if logged_event == event:
            return True
    return False


def mark_day_not_started_if_missing(base_dir: str, day: date, when: datetime) -> bool:
    """
    Retroactive watchdog — call at process startup for the previous trading
    day. If that day has ZERO connection events logged, write a
    DAY_NOT_STARTED marker so BackfillManager treats it as a full-session
    gap instead of falling back to an uncertain data scan.

    Returns True if a marker was written.
    """
    existing = get_events_for_day(base_dir, day)
    if existing:
        return False
    log_event(
        base_dir,
        event="DAY_NOT_STARTED",
        when=when,
        day=day,
        note=f"no connection events found for {day.isoformat()} at startup check",
    )
    print(
        f"[CONN_LOG] {day} had no connection activity — marked DAY_NOT_STARTED",
        flush=True,
    )
    return True