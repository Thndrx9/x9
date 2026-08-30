# candle_builder.py
#
# STANDALONE / OFFLINE candle-rebuild tool — NOT part of the live
# startup path. Do NOT call this before starting live trading.
#
# Why: save_candles_bulk() (ohlc.py) writes candles to BOTH the
# parquet files on disk AND the in-RAM ohlc_data dict that indicators/
# signal_generator read from live. Running candle building in a
# separate OS process means that RAM dict belongs to THIS process and
# is discarded the instant it exits — the live process's OHLCCollector
# would never see those candles. There is currently no code path that
# reloads parquet files back into a running OHLCCollector's RAM, so
# using this before live trading starts would leave indicators with no
# candle history.
#
# The RAM-spike fix for the live startup path lives in
# BackfillManager.run_low_memory() (backfill_manager.py), which is
# what ohlc.py's ensure_backfill() now calls — it does the same
# chunk-and-discard candle building, just in-process, so the built
# candles land in the live OHLCCollector's RAM where trading needs
# them.
#
# What THIS script is for: rebuilding/backfilling candles on disk
# only — e.g. regenerating parquet history for analysis/backtesting —
# run it manually, separately from the live system.
#
# Usage:
#   python3 candle_builder.py

import sys
from datetime import datetime

from utils import load_symbols
from market_time import tz_kolkata
from tick_writer import TickWriter, HistoryCandleStore
from ohlc import OHLCCollector
from backfill_manager import BackfillManager

CHUNK_SIZE = 20


def main():
    symbols = load_symbols("symbols.csv")
    if not symbols:
        print("[CANDLE_BUILDER][ERROR] No valid symbols found in symbols.csv", flush=True)
        sys.exit(1)

    # Same local-cache backends the live process uses — this script
    # only reads them, it never opens a Postgres/main-db connection.
    tick_writer   = TickWriter(base_dir="tickdata")
    history_store = HistoryCandleStore(base_dir="tickdata")
    ohlc          = OHLCCollector(tick_writer=tick_writer, history_store=history_store)

    backfill = BackfillManager(ohlc)
    # This process doesn't need the main Postgres tick db (that's the
    # live process's job via sync_local_cache_with_main_db /
    # _run_depth_backfill) — close it immediately.
    if backfill.conn is not None:
        try:
            backfill.conn.close()
        except Exception:
            pass
        backfill.conn = None

    start_ts = backfill._compute_lookback_start()
    now      = datetime.now(tz_kolkata)

    print(
        f"[CANDLE_BUILDER] Starting | {len(symbols)} symbol(s) | "
        f"from={start_ts.strftime('%Y-%m-%d %H:%M %Z')} | chunk_size={CHUNK_SIZE}",
        flush=True,
    )

    # Top up the local history-candle cache first (chunked, discards as
    # it goes — see sync_history_cache_with_main_db's docstring), THEN
    # build candles purely from local caches. This is the one main-db
    # connection this process opens, and it's closed again right after.
    history_conn = backfill._get_history_conn()
    if history_conn is not None:
        history_existing_tables = backfill._load_existing_quote_tables(history_conn)
        backfill.sync_history_cache_with_main_db(symbols, start_ts, now, history_existing_tables, chunk_size=CHUNK_SIZE)
        try:
            history_conn.close()
        except Exception:
            pass
        backfill.conn_history = None
    else:
        print("[CANDLE_BUILDER][WARN] no history-db connection — building candles from local cache only", flush=True)

    backfill.build_candles_from_local_cache(symbols, start_ts, now, chunk_size=CHUNK_SIZE)

    print("[CANDLE_BUILDER] Completed", flush=True)


if __name__ == "__main__":
    main()
