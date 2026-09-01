# tick_gap_detector.py

"""
TickGapDetector — TICK-CACHE GAP DETECTION, NO DB / NO I/O
--------------------------------------------------------------
Everything in this file is pure in-memory logic: given tick data
BackfillManager has already fetched/cached, tell it which parts are
missing (or suspicious). It never opens a Postgres connection and
never touches SQLite itself — BackfillManager owns all DB I/O
(fetching from Postgres, reading/writing the local tick cache) and
just hands the resulting DataFrames to this class to find out what's
missing.

This is distinct from gap_detector.py's GapDetector, which diffs
"what candle buckets should exist" against "what we actually have"
(expected-schedule vs. actual). TickGapDetector instead looks at a
symbol's own already-cached tick stream and finds silent same-session
holes in it — i.e. verifying that a cache's high-water mark
(max_ts) is trustworthy, not just present.
"""

import pandas as pd
from datetime import datetime
from market_time import tz_kolkata, MARKET_OPEN, MARKET_CLOSE
from gap_detector import gap_matches_outage


class TickGapDetector:
    """
    Scans already-fetched/cached tick (or depth) data to find silent
    gaps inside a live trading session, and classifies candle-count
    validation results. BackfillManager fetches from Postgres and
    hands the resulting DataFrames here to find out what's missing.
    """

    # ─────────────────────────────────────────────
    # Silent same-session gap detection
    # ─────────────────────────────────────────────

    def first_suspicious_gap_ms(self, df: pd.DataFrame, threshold_secs: int):
        """
        Verifies a cached tick range instead of blindly trusting
        max_ts() as proof of completeness. Scans df (must have an
        'ist_ts' column; sorted internally) for the first pair of
        consecutive ticks that are:
          - on the same calendar day, AND
          - both within market hours (MARKET_OPEN..MARKET_CLOSE), AND
          - more than threshold_secs apart.
        Overnight/weekend/holiday gaps between sessions never match
        (different day, or outside market hours) — only a genuinely
        silent stretch *inside* a live session counts as suspicious.

        Returns the ts_ms of the tick immediately before that gap (the
        last point still safe to trust), or None if no such gap exists
        (including when df has fewer than 2 rows).
        """
        if df is None or len(df) < 2:
            return None

        d = df.sort_values("timestamp").reset_index(drop=True)
        prev_ist = d["ist_ts"].iloc[:-1].reset_index(drop=True)
        next_ist = d["ist_ts"].iloc[1:].reset_index(drop=True)
        prev_ms  = d["timestamp"].iloc[:-1].reset_index(drop=True)

        gap_secs = (next_ist - prev_ist).dt.total_seconds()
        same_day = prev_ist.dt.date == next_ist.dt.date
        prev_in_hours = (prev_ist.dt.time >= MARKET_OPEN) & (prev_ist.dt.time <= MARKET_CLOSE)
        next_in_hours = (next_ist.dt.time >= MARKET_OPEN) & (next_ist.dt.time <= MARKET_CLOSE)

        suspicious = same_day & prev_in_hours & next_in_hours & (gap_secs > threshold_secs)
        hits = prev_ms[suspicious]

        return int(hits.iloc[0]) if not hits.empty else None

    def find_cache_gap(
        self,
        cached_df: pd.DataFrame,
        last_ms: int,
        threshold_secs: int,
        outage_windows: list = None,
    ) -> dict:
        """
        Given a symbol's full locally-cached tick (or depth) range
        (cached_df) and its high-water mark (last_ms), determine
        whether there's a silent same-session gap in it.

        Pure in-memory — does not touch Postgres or SQLite; the caller
        is responsible for reading cached_df and for re-fetching
        whatever this reports as untrusted.

        Returns:
            {
                "gap_ms": int or None,          # ts_ms right before the gap
                "trusted_df": DataFrame,        # portion still safe to trust
                "cached_until_ms": int or None, # gap_ms, or None if nothing left to trust
                "phantom_range": (start_ms, end_ms) or None,
                "phantom_local_ts": set[int] or None,
                "confirmed_outage": bool,       # gap overlaps a logged disconnect
            }
        """
        result = {
            "gap_ms": None,
            "trusted_df": cached_df,
            "cached_until_ms": last_ms,
            "phantom_range": None,
            "phantom_local_ts": None,
            "confirmed_outage": False,
        }

        gap_ms = self.first_suspicious_gap_ms(cached_df, threshold_secs)
        if gap_ms is None:
            return result

        tail_df = cached_df[cached_df["timestamp"] > gap_ms]
        trusted_df = cached_df[cached_df["timestamp"] <= gap_ms].reset_index(drop=True)

        result["gap_ms"] = gap_ms
        result["trusted_df"] = trusted_df
        result["cached_until_ms"] = gap_ms if not trusted_df.empty else None
        result["phantom_range"] = (gap_ms + 1, last_ms)
        result["phantom_local_ts"] = set(int(t) for t in tail_df["timestamp"])

        if outage_windows:
            gap_start_dt = datetime.fromtimestamp(gap_ms / 1000, tz=tz_kolkata)
            gap_end_dt   = datetime.fromtimestamp(last_ms / 1000, tz=tz_kolkata)
            result["confirmed_outage"] = gap_matches_outage(gap_start_dt, gap_end_dt, outage_windows)

        return result

    # ─────────────────────────────────────────────
    # Candle-count validation classification
    # ─────────────────────────────────────────────

    # ─────────────────────────────────────────────
    # Chunked local-cache scan — READ-ONLY, owns all local-cache access
    # ─────────────────────────────────────────────

    def scan_local_cache(
        self,
        symbols: list,
        tick_writer,
        start_ms: int,
        now_ms: int,
        threshold_secs: int,
        outage_windows: list = None,
        chunk_size: int = 20,
        conn=None,
        kind: str = "quote",
        progress=None,
    ) -> dict:
        """
        Owns the ENTIRE local-tick-cache read path. BackfillManager no
        longer reads ticks.db/Postgres-cache itself — it only calls
        this once and gets back what's missing.

        Reads `symbols` in chunks of `chunk_size` (default 20). Each
        chunk's raw cached rows (read_ticks_batch) live only inside
        that chunk's loop iteration — nothing from one chunk survives
        into the next, so peak memory is bounded by one chunk's worth
        of tick data, not every symbol's.

        Returns {symbol: entry} where entry is:
            {
                "fetch_start_ms": int or None,   # None = fully covered locally
                "phantom_range": (start_ms, end_ms) or None,
                "phantom_local_ts": set[int] or None,
                "confirmed_outage": bool,
            }
        No DataFrames are returned — only timestamps/sets, which are
        cheap enough to hold for every symbol at once.
        """
        out = {}
        all_names = [s["symbol"] for s in symbols]

        last_ms_by_symbol = (
            tick_writer.max_ts_batch(all_names, kind=kind, conn=conn)
            if tick_writer is not None else {}
        )

        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]

            ranges_needed = {
                inst["symbol"]: (start_ms, last_ms_by_symbol[inst["symbol"]])
                for inst in chunk
                if last_ms_by_symbol.get(inst["symbol"]) is not None
                and last_ms_by_symbol[inst["symbol"]] >= start_ms
            }
            cached_rows_chunk = (
                tick_writer.read_ticks_batch(ranges_needed, kind=kind, conn=conn)
                if tick_writer is not None and ranges_needed else {}
            )

            for j, inst in enumerate(chunk):
                symbol  = inst["symbol"]
                last_ms = last_ms_by_symbol.get(symbol)

                cached_until_ms  = None
                phantom_range    = None
                phantom_local_ts = None
                confirmed_outage = False
                fetch_end_ms     = None

                if last_ms is not None and last_ms >= start_ms:
                    cached_df = self._rows_to_df(cached_rows_chunk.get(symbol, []))
                    gap_result = self.find_cache_gap(cached_df, last_ms, threshold_secs, outage_windows)

                    if gap_result["gap_ms"] is not None:
                        cached_until_ms  = gap_result["cached_until_ms"]
                        phantom_range    = gap_result["phantom_range"]
                        phantom_local_ts = gap_result["phantom_local_ts"]
                        confirmed_outage = gap_result["confirmed_outage"]
                    else:
                        cached_until_ms = last_ms

                    # find_cache_gap() (above) only checks for gaps
                    # BETWEEN rows already in cached_df — it has no way
                    # to know whether cached_df's own earliest row goes
                    # back far enough to cover start_ms. If the lookback
                    # window was just widened (a longer TF added,
                    # MIN_CANDLES raised) and the newly-required older
                    # days were never fetched before, cached_df would
                    # look perfectly gap-free internally while still
                    # being missing an entire leading chunk — silently
                    # under-covering the window with no warning. Catch
                    # that here: if the earliest cached row starts
                    # meaningfully later than start_ms, only the leading
                    # slice before that row (start_ms .. earliest_cached_ms-1)
                    # is actually missing — everything from
                    # earliest_cached_ms onward already passed the gap
                    # check above and doesn't need touching. Fetch just
                    # that leading slice via fetch_end_ms, instead of
                    # re-fetching (and re-writing on top of) the whole
                    # already-covered range through now: the writer has
                    # no dedup-on-timestamp, so re-inserting rows that
                    # already exist would leave duplicates behind, not
                    # just cost extra time.
                    if not cached_df.empty:
                        earliest_cached_ms = int(cached_df["timestamp"].min())
                        if earliest_cached_ms > start_ms + threshold_secs * 1000:
                            cached_until_ms = None
                            fetch_end_ms = earliest_cached_ms - 1

                    del cached_df   # this symbol's raw rows are done being used

                fetch_start_ms = (cached_until_ms + 1) if cached_until_ms is not None else start_ms

                out[symbol] = {
                    "fetch_start_ms":   fetch_start_ms if fetch_start_ms < now_ms else None,
                    "fetch_end_ms":     fetch_end_ms,
                    "phantom_range":    phantom_range,
                    "phantom_local_ts": phantom_local_ts,
                    "confirmed_outage": confirmed_outage,
                }

                if progress:
                    progress(i + j + 1, len(symbols))

            # cached_rows_chunk (this chunk's raw local-cache rows) falls
            # out of scope here — nothing from it is retained beyond the
            # small dicts written into `out` above.
            del cached_rows_chunk

        return out

    @staticmethod
    def _rows_to_df(rows: list) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["timestamp", "ist_ts"])
        df = pd.DataFrame(rows)
        if "ist_ts" not in df.columns and "timestamp" in df.columns:
            df["ist_ts"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(tz_kolkata)
        return df

    def classify_symbol_tf(self, count: int, missing: int, min_candles: int) -> str:
        """
        Classifies a symbol/TF's candle-count validation result:
          - "insufficient": fewer than min_candles candles loaded
          - "gap":           enough candles, but some expected buckets
                              are still missing (unrecoverable gap)
          - "ok":             enough candles, nothing missing

        Pure classification — no printing, no I/O. The caller decides
        what to do with the verdict (e.g. print a warning).
        """
        if count < min_candles:
            return "insufficient"
        if missing > 0:
            return "gap"
        return "ok"
