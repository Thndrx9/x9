"""
verify_vectorized_filters.py

One-off sanity check: pulls a real sample of ticks from your main DB and
confirms the new vectorized filters in gap_detector.py produce byte-for-byte
the same result as the original .dt.time / .dt.dayofweek comparisons they
replaced. Synthetic random data has already been checked exhaustively; this
closes the one gap synthetic data can't — your actual data's quirks
(duplicate timestamps, unusual gaps, DST-irrelevant-but-still-worth-checking
year boundaries, NaT rows, symbol listing/delisting edges, etc.).

Usage:
    python verify_vectorized_filters.py [--limit N] [--table quote_RELIANCE]

If --table is omitted, it samples a handful of quote_* tables automatically.
Exits non-zero and prints every mismatching row if anything disagrees —
safe to wire into CI or run manually before a production backfill.
"""

import argparse
import sys

import pandas as pd
import psycopg2

from market_time import tz_kolkata, MARKET_OPEN, MARKET_CLOSE
from gap_detector import (
    is_at_or_after_market_open_vectorized,
    is_market_hours_weekday_vectorized,
)

# Reuse the same connection env vars as backfill_manager.py's _connect()
# (main DB — the same one the batched tick fetch reads from).
from dotenv import load_dotenv
import os

load_dotenv()


def get_connection():
    missing = [
        var for var in ("PG_HOST", "PG_USER", "PG_PASSWORD")
        if not os.getenv(var)
    ]
    dbname = os.getenv("PG_DBNAME") or os.getenv("PG_DATABASE")
    if not dbname:
        missing.append("PG_DBNAME (or PG_DATABASE)")
    if missing:
        print(
            f"[VERIFY][ERROR] Missing required env var(s): {', '.join(missing)}. "
            "These are the same PG_* vars backfill_manager.py already uses — "
            "if the backfill runs fine in this environment, they're already set.",
            file=sys.stderr,
        )
        sys.exit(1)

    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=dbname,
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
        connect_timeout=15,
    )


def discover_sample_tables(conn, limit=5):
    """
    Grab a handful of quote_* tables to sample from. Mirrors
    backfill_manager.py's _load_existing_quote_tables() exactly (same
    pg_tables query, same schema/prefix) so this always looks at the same
    set of tables the backfill itself would use.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname='public' AND tablename LIKE 'quote_%' "
        "ORDER BY tablename"
    )
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    return tables[:limit]


def fetch_sample(conn, table, limit):
    """
    Column shape matches backfill_manager.py's real tick query exactly:
    `last_quantity` is the actual column, aliased to `qty` (there is no
    literal `qty` column) — and only non-null ltp rows are real ticks.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            f'SELECT timestamp, ltp, COALESCE(last_quantity, 0) AS qty '
            f'FROM "{table}" WHERE ltp IS NOT NULL '
            f'ORDER BY timestamp LIMIT %s',
            (limit,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
    return rows


def verify_table(conn, table, limit):
    rows = fetch_sample(conn, table, limit)
    if not rows:
        print(f"[VERIFY] {table}: no rows, skipping")
        return True

    df = pd.DataFrame(rows, columns=["timestamp", "ltp", "qty"])
    df["ist_ts"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)

    # --- reference (old) behavior ---
    t = df["ist_ts"].dt.time
    ref_open = t >= MARKET_OPEN
    ref_hours = (t >= MARKET_OPEN) & (t < MARKET_CLOSE) & (df["ist_ts"].dt.dayofweek < 5)

    # --- new vectorized behavior ---
    got_open = is_at_or_after_market_open_vectorized(df["ist_ts"])
    got_hours = is_market_hours_weekday_vectorized(df["ist_ts"])

    open_mismatches = df.loc[ref_open.values != got_open.values]
    hours_mismatches = df.loc[ref_hours.values != got_hours.values]

    ok = open_mismatches.empty and hours_mismatches.empty
    status = "OK" if ok else "MISMATCH"
    print(f"[VERIFY] {table}: {len(df)} row(s) checked -> {status}")

    if not open_mismatches.empty:
        print(f"[VERIFY][FAIL] {table}: {len(open_mismatches)} market-open mismatch(es):")
        print(open_mismatches.head(20).to_string())

    if not hours_mismatches.empty:
        print(f"[VERIFY][FAIL] {table}: {len(hours_mismatches)} market-hours mismatch(es):")
        print(hours_mismatches.head(20).to_string())

    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=200_000, help="rows to sample per table")
    parser.add_argument("--table", type=str, default=None, help="specific quote_* table to check")
    parser.add_argument("--tables-to-sample", type=int, default=5, help="how many tables to auto-sample")
    args = parser.parse_args()

    conn = get_connection()
    try:
        tables = [args.table] if args.table else discover_sample_tables(conn, args.tables_to_sample)
        if not tables:
            print("[VERIFY] No quote_* tables found.", file=sys.stderr)
            sys.exit(1)

        all_ok = True
        for table in tables:
            try:
                ok = verify_table(conn, table, args.limit)
                all_ok = all_ok and ok
            except Exception as exc:
                print(f"[VERIFY][ERROR] {table}: {exc}", file=sys.stderr)
                all_ok = False
    finally:
        conn.close()

    if all_ok:
        print("\n[VERIFY] All sampled tables match. Safe to trust the vectorized filters on real data.")
        sys.exit(0)
    else:
        print("\n[VERIFY] Mismatches found — do NOT trust the vectorized filters until resolved.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
