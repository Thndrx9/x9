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
                "gap_window": (start_ms, end_ms) or None,
                "confirmed_outage": bool,
            }
        No DataFrames are returned — only timestamps, which are cheap
        enough to hold for every symbol at once.

        gap_window is the EXACT narrow span a detected silent gap
        covers — (tick right before it) + 1, to (tick right after it)
        - 1 — not a "everything from the gap through now" range. There
        is nothing to read locally inside that span (that's the
        definition of a gap: zero local rows there), so this method no
        longer reads or returns a "phantom" tail to diff — the caller
        (BackfillManager) is the one with a main-db connection, and
        checking the ONE authoritative source of truth (does upstream
        actually have anything in this exact window) is its job, not
        something guessable from local data alone. See
        BackfillManager.sync_local_cache_with_main_db()'s docstring for
        the full reasoning and what replaced the old phantom-tail
        design.
        """
        out = {}
        all_names = [s["symbol"] for s in symbols]

        # Two server-side capability tiers, checked in order:
        #   1. session_gap_batch_combined() — scans the UNION of a
        #      symbol's backfill AND live tables for a silent gap, not
        #      backfill alone. Without this, every tick already
        #      captured live since backfill last ran looks like a gap
        #      needing a redundant re-fetch from main db after a
        #      restart — not incorrect, just wasted network/DB time for
        #      data already on hand. See its docstring in tick_writer.py
        #      for the full reasoning, including why this is a genuine
        #      LAG-based scan of the merged timeline and NOT just "trust
        #      each table's min/max" — a live table can have its own
        #      internal gaps too.
        #   2. session_gap_batch() — backfill-only fallback, for a
        #      writer that has the single-table method but not yet the
        #      combined one.
        # SQLite has no server worth offloading to (it's a local file —
        # no network cost to save), so it gets neither and the loop
        # below falls back to the original full-row pandas scan for it
        # automatically.
        use_combined_gap    = tick_writer is not None and hasattr(tick_writer, "session_gap_batch_combined")
        use_server_side_gap = use_combined_gap or (tick_writer is not None and hasattr(tick_writer, "session_gap_batch"))

        # last_ms_by_symbol has to reflect the MORE RECENT of the two
        # tables too, for the same reason as the gap check above: after
        # a restart, live's last row is very likely newer than
        # backfill's own (backfill hasn't run since; live has been
        # writing the whole time). Using backfill's max_ts() alone here
        # would make scan_local_cache() think coverage ends earlier than
        # it actually does, even once the gap check itself is combined.
        if use_combined_gap:
            last_ms_backfill = tick_writer.max_ts_batch(all_names, kind=kind, source="backfill", conn=conn)
            last_ms_live     = tick_writer.max_ts_batch(all_names, kind=kind, source="live", conn=conn)
            last_ms_by_symbol = {}
            for name in all_names:
                candidates = [v for v in (last_ms_backfill.get(name), last_ms_live.get(name)) if v is not None]
                last_ms_by_symbol[name] = max(candidates) if candidates else None
        else:
            last_ms_by_symbol = (
                tick_writer.max_ts_batch(all_names, kind=kind, conn=conn)
                if tick_writer is not None else {}
            )

        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]

            names_in_range = [
                inst["symbol"] for inst in chunk
                if last_ms_by_symbol.get(inst["symbol"]) is not None
                and last_ms_by_symbol[inst["symbol"]] >= start_ms
            ]

            gap_info_chunk = {}
            cached_rows_chunk = {}
            if names_in_range:
                if use_combined_gap:
                    gap_info_chunk = tick_writer.session_gap_batch_combined(
                        names_in_range, kind=kind, start_ms=start_ms,
                        threshold_secs=threshold_secs, conn=conn,
                    )
                elif use_server_side_gap:
                    gap_info_chunk = tick_writer.session_gap_batch(
                        names_in_range, kind=kind, start_ms=start_ms,
                        threshold_secs=threshold_secs, conn=conn,
                    )
                else:
                    ranges_needed = {
                        name: (start_ms, last_ms_by_symbol[name]) for name in names_in_range
                    }
                    cached_rows_chunk = tick_writer.read_ticks_batch(ranges_needed, kind=kind, conn=conn)

            for j, inst in enumerate(chunk):
                symbol  = inst["symbol"]
                last_ms = last_ms_by_symbol.get(symbol)

                cached_until_ms  = None
                gap_window       = None
                confirmed_outage = False
                fetch_end_ms     = None

                if last_ms is not None and last_ms >= start_ms:
                    if use_server_side_gap:
                        info = gap_info_chunk.get(symbol, {})
                        earliest_ms = info.get("earliest_ms")
                        gap_prev_ms = info.get("gap_prev_ms")
                        gap_next_ms = info.get("gap_next_ms")

                        if gap_prev_ms is not None:
                            # Trust everything already cached, gap
                            # included — nothing local sits inside a gap
                            # by definition, so there's nothing to
                            # "un-trust" past it either. Only the exact
                            # empty span itself is in question.
                            cached_until_ms = last_ms
                            gap_window = (
                                (gap_prev_ms + 1, gap_next_ms - 1)
                                if gap_next_ms is not None
                                # Shouldn't happen — gap_next_ms comes from
                                # the same FILTER predicate as gap_prev_ms,
                                # so they're always both set or both None —
                                # but fall back to the old "through last_ms"
                                # span rather than silently dropping the
                                # gap if it ever does.
                                else (gap_prev_ms + 1, last_ms)
                            )

                            if outage_windows:
                                gap_start_dt = datetime.fromtimestamp(gap_window[0] / 1000, tz=tz_kolkata)
                                gap_end_dt   = datetime.fromtimestamp(gap_window[1] / 1000, tz=tz_kolkata)
                                confirmed_outage = gap_matches_outage(gap_start_dt, gap_end_dt, outage_windows)
                        else:
                            cached_until_ms = last_ms

                        # Same "widened lookback" leading-gap check as the
                        # legacy path below, using the earliest_ms the
                        # server already computed instead of re-deriving
                        # it from a DataFrame — see the legacy branch's
                        # comment for the full explanation.
                        if earliest_ms is not None and earliest_ms > start_ms + threshold_secs * 1000:
                            cached_until_ms = None
                            fetch_end_ms = earliest_ms - 1

                    else:
                        cached_df = self._rows_to_df(cached_rows_chunk.get(symbol, []))
                        gap_result = self.find_cache_gap(cached_df, last_ms, threshold_secs, outage_windows)

                        if gap_result["gap_ms"] is not None:
                            # Legacy/SQLite fallback only (no server-side
                            # batched gap query to give us gap_next_ms
                            # cheaply) — still uses the old, more
                            # conservative "everything after the gap
                            # through last_ms" span rather than the exact
                            # narrow window, since finding the precise
                            # next-tick boundary here would mean an extra
                            # full pandas pass. SQLite has no network cost
                            # to save the way a remote Postgres round trip
                            # does, so this stays correct, just less
                            # surgical than the server-side path above.
                            cached_until_ms  = gap_result["cached_until_ms"]
                            gap_window       = gap_result["phantom_range"]
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
                    "gap_window":       gap_window,
                    "confirmed_outage": confirmed_outage,
                }

                if progress:
                    progress(i + j + 1, len(symbols))

            # Whatever this chunk pulled (server-side gap answers, or the
            # legacy path's raw rows) falls out of scope here — nothing
            # survives beyond the small dicts written into `out` above.
            del gap_info_chunk, cached_rows_chunk

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
