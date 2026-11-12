#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = ["pandas", "pyarrow", "numpy"]
# ///
"""
parquet_verify.py - check that a parquet file has data for EVERY SECOND of market hours.

Standalone (no Streamlit).  Works on daily files (02-11-2026_quote.parquet), weekly
per-symbol files (ABB-2026-W44-quote.parquet) or any parquet with a timestamp column.

    uv run parquet_verify.py 02-11-2026/ --check-ltt
    uv run parquet_verify.py 02-11-2026_quote.parquet --open 09:15 --close 15:30 --max-ignored-gap 3

Exit code: 0 = every file passed, 1 = some file failed / errored (handy for cron / CI).

HOW IT WORKS
  1. Timestamp column: 'timestamp', 'ts', 'time', 'datetime' (else the first column whose
     name contains 'time').  Only this column is read.  Epoch numbers (s/ms/us/ns, auto-
     detected) are converted from UTC; timezone-naive datetimes are assumed UTC unless
     --naive-tz Asia/Kolkata.  Everything is evaluated in IST.
  2. Dates to check: from the file name (02-11-2026 -> that day; 2026-W44 -> Mon..Sun;
     2026-11-02 -> that day), else the span of the data; or --start/--end.
  3. For each date, expected seconds = open .. close-1 (add --include-close for close too).
     A second is "present" if >= 1 row falls in it (many rows per second is fine).
  4. Missing seconds are grouped into runs.  Runs of <= --max-ignored-gap seconds (default 3)
     are treated as "no trade that second" ("quiet"), not gaps.  Longer runs are real gaps
     and the day / file FAILS.
  5. --cross-check: with several files of the same kind and date, a second missing from one
     file but present in another is also "quiet" (the feed was alive).
  6. --check-ltt: also reads the feed's own `ltt` (a real `ltt` column, else parsed from the
     `raw_json` column).  It (a) runs the same per-second check on ltt, (b) labels every
     receive-time gap "Late arrival" (ltt has those seconds -> ticks arrived late, nothing
     lost) or "Feed gap" (ltt is missing too -> never delivered), (c) reports receive lag =
     timestamp - ltt (median / p95 / max) inside market hours.
  7. Weekdays with no rows at all are skipped as holidays (--no-skip-empty to fail them);
     Saturdays/Sundays are skipped (--no-skip-weekends to include them).
"""
import argparse
import re
import sys

from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

IST = "Asia/Kolkata"

FLAT_RE = re.compile(r"^(?P<sym>.+?)-(?P<week>\d{4}-W\d{2})-(?P<kind>quote|depth)$")
WEEK_RE = re.compile(r"(\d{4})-W(\d{2})")
DAILY_RE = re.compile(r"^(?P<day>\d{2}-\d{2}-\d{4})_(?P<kind>quote|depth)$")   # 02-11-2026_quote
DMY_RE = re.compile(r"(?<!\d)(\d{2})-(\d{2})-(\d{4})(?!\d)")                  # 02-11-2026 (DD-MM-YYYY)
YMD_RE = re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)")                  # 2026-11-02

STATUS_LABEL = {
    "PASS": "✅ PASS",
    "FAIL": "❌ FAIL",
    "ERROR": "⚠️ ERROR",
    "NO DAYS": "⚠️ NO DAYS",
}


def is_time_col(name):
    n = str(name).lower()
    return "time" in n or n == "ts"


def pick_time_column(names, override=""):
    """Choose the timestamp column used for verification."""
    if override and override in names:
        return override
    cands = [c for c in names if is_time_col(c)]
    for pref in ("timestamp", "ts", "time", "datetime"):
        for c in cands:
            if c.lower() == pref:
                return c
    return cands[0] if cands else None


def to_ist(s, naive_tz="UTC"):
    """Convert a timestamp-like Series to tz-aware IST. None if nothing usable."""
    if s.notna().sum() == 0:
        return None

    if pd.api.types.is_numeric_dtype(s) and not pd.api.types.is_bool_dtype(s):
        v = abs(float(s.dropna().iloc[0]))
        unit = "ns" if v > 1e17 else "us" if v > 1e14 else "ms" if v > 1e11 else "s"
        dt = pd.to_datetime(s, unit=unit, utc=True, errors="coerce")
    else:
        try:
            dt = pd.to_datetime(s, errors="coerce")
            if dt.dt.tz is None:
                dt = dt.dt.tz_localize(naive_tz, ambiguous="NaT", nonexistent="NaT")
        except Exception:
            dt = pd.to_datetime(s, utc=True, errors="coerce")

    return dt.dt.tz_convert(IST)


def file_dates(path):
    """
    Dates a file is expected to cover, from its name (then its folder name):
      ...2026-W44...   -> Mon..Sun of that ISO week
      02-11-2026_...   -> that single day (DD-MM-YYYY)
      2026-11-02...    -> that single day
    None if no date can be read.
    """
    path = Path(path)
    for text in (path.stem, path.parent.name):
        m = WEEK_RE.search(text)
        if m:
            try:
                monday = date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)
                return [monday + timedelta(days=i) for i in range(7)]
            except ValueError:
                pass
        m = DMY_RE.search(text)
        if m:
            try:
                return [date(int(m.group(3)), int(m.group(2)), int(m.group(1)))]
            except ValueError:
                pass
        m = YMD_RE.search(text)
        if m:
            try:
                return [date(int(m.group(1)), int(m.group(2)), int(m.group(3)))]
            except ValueError:
                pass
    return None


def _secs(t):
    return t.hour * 3600 + t.minute * 60 + t.second


def _hms(epoch_secs):
    idx = pd.to_datetime(np.asarray(epoch_secs, dtype="int64"), unit="s", utc=True)
    return list(idx.tz_convert(IST).strftime("%H:%M:%S"))


def _runs(missing):
    """Collapse sorted missing epoch-seconds into (start, end_inclusive) runs."""
    if missing.size == 0:
        return np.array([], dtype="int64"), np.array([], dtype="int64")
    br = np.flatnonzero(np.diff(missing) != 1) + 1
    return missing[np.r_[0, br]], missing[np.r_[br - 1, missing.size - 1]]


def load_seconds(path, col_override="", naive_tz="UTC"):
    """Read only the timestamp column -> sorted epoch seconds (+ unique seconds)."""
    path = Path(path)
    res = {"column": None, "rows": 0, "null_ts": 0, "epoch": None, "uniq": None, "error": None}
    try:
        names = pq.read_schema(path).names
        col = pick_time_column(names, col_override)
        if col is None:
            res["error"] = "No timestamp column found (name should contain 'time' or be 'ts')"
            return res
        res["column"] = col

        raw = pd.read_parquet(path, columns=[col])[col]
        res["rows"] = int(len(raw))
        dt = to_ist(raw, naive_tz)
        if dt is None:
            res["error"] = f"Column '{col}' has no usable timestamps"
            return res
        res["null_ts"] = int(dt.isna().sum())
        dt = dt.dropna()
        if dt.empty:
            res["error"] = f"Column '{col}' has no usable timestamps"
            return res

        epoch = np.sort(
            dt.dt.tz_convert("UTC").dt.tz_localize(None)
              .to_numpy().astype("datetime64[s]").astype("int64")
        )
        res["epoch"] = epoch
        res["uniq"] = np.unique(epoch)
    except Exception as e:
        res["error"] = f"{type(e).__name__}: {e}"
    return res


def union_seconds(loaded_list):
    """Sorted unique seconds present in ANY of the given loaded files."""
    arrs = [x["uniq"] for x in loaded_list if x and x.get("uniq") is not None]
    return np.unique(np.concatenate(arrs)) if arrs else None


LTT_RE = r'"ltt"\s*:\s*"?(\d+(?:\.\d+)?)'


LTT_BATCH = 500_000      # rows per batch: keeps memory bounded on big daily files


def _ltt_values(s, kind, naive_tz):
    """Raw ltt numbers (float, NaN = missing) for one batch; second value True if already epoch seconds."""
    if kind == "json":
        first = next((v for v in s if v is not None), "")
        if isinstance(first, (bytes, bytearray)):
            s = s.map(lambda b: b.decode("utf-8", "ignore") if b is not None else None)
        vals = pd.to_numeric(s.astype("string").str.extract(LTT_RE, expand=False), errors="coerce")
        return vals.to_numpy(dtype="float64"), False
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").to_numpy(dtype="float64"), False
    d = to_ist(s, naive_tz)                      # datetime-like ltt column
    if d is None:
        return np.full(len(s), np.nan), True
    dn = d.dt.tz_convert("UTC").dt.tz_localize(None).to_numpy().astype("datetime64[s]")
    nat = np.isnat(dn)
    v = dn.astype("int64").astype("float64")
    v[nat] = np.nan
    return v, True


def load_ltt(path, col_override="", naive_tz="UTC"):
    """
    Read the receive-timestamp column and the feed's `ltt`
    (a real `ltt` column, else extracted from the raw_json column), in batches.
    Returns sorted ltt seconds plus row-aligned (receive_sec, ltt_sec) pairs for lag stats.
    """
    path = Path(path)
    res = {"source": None, "rows_with_ltt": 0, "epoch": None, "uniq": None,
           "ts_sec": None, "ltt_sec": None, "error": None}
    try:
        pf = pq.ParquetFile(path)
        names = pf.schema_arrow.names
        ts_col = pick_time_column(names, col_override)
        if ts_col is None:
            res["error"] = "No timestamp column found"
            return res

        if "ltt" in names:
            src, kind = "ltt", "col"
        elif "raw_json" in names:
            src, kind = "raw_json", "json"
        else:
            js = [c for c in names if "json" in c.lower()]
            if not js:
                res["error"] = "No 'ltt' or 'raw_json' column in this file"
                return res
            src, kind = js[0], "json"
        res["source"] = src if kind == "col" else f"{src} → ltt"

        div = None
        all_ltt, ts_both, ltt_both = [], [], []
        for batch in pf.iter_batches(batch_size=LTT_BATCH, columns=[ts_col, src]):
            df = batch.to_pandas()
            ts_dt = to_ist(df[ts_col], naive_tz)
            if ts_dt is None:
                ts_np = np.full(len(df), np.datetime64("NaT"), dtype="datetime64[s]")
            else:
                ts_np = ts_dt.dt.tz_convert("UTC").dt.tz_localize(None).to_numpy().astype("datetime64[s]")
            ts_ok = ~np.isnat(ts_np)
            ts_sec = ts_np.astype("int64")

            v, is_secs = _ltt_values(df[src], kind, naive_tz)
            ok_v = np.isfinite(v)
            if div is None and ok_v.any():
                if is_secs:
                    div = 1.0
                else:
                    m = abs(float(v[ok_v][0]))
                    div = 1e9 if m > 1e17 else 1e6 if m > 1e14 else 1e3 if m > 1e11 else 1.0
            ltt_sec = np.zeros(len(v), dtype="int64")
            if div is not None:
                ltt_sec[ok_v] = np.floor(v[ok_v] / div).astype("int64")
            ltt_ok = ok_v & (ltt_sec > 1_000_000_000)       # ignore 0 / garbage
            both = ltt_ok & ts_ok
            all_ltt.append(ltt_sec[ltt_ok])
            ts_both.append(ts_sec[both])
            ltt_both.append(ltt_sec[both])

        all_ltt = np.concatenate(all_ltt) if all_ltt else np.array([], dtype="int64")
        if all_ltt.size == 0:
            res["error"] = f"No usable 'ltt' values found in '{src}'"
            return res

        res["rows_with_ltt"] = int(all_ltt.size)
        res["epoch"] = np.sort(all_ltt)
        res["uniq"] = np.unique(res["epoch"])
        res["ts_sec"], res["ltt_sec"] = np.concatenate(ts_both), np.concatenate(ltt_both)
    except Exception as e:
        res["error"] = f"{type(e).__name__}: {e}"
    return res


def _lag_stats(ts_sec, ltt_sec, cfg):
    """Receive-time minus ltt, in whole seconds, for rows inside market hours."""
    if ts_sec is None or ts_sec.size == 0:
        return None
    o = _secs(cfg["open_t"])
    c = _secs(cfg["close_t"]) + (1 if cfg["include_close"] else 0)
    sod = (ts_sec + 19800) % 86400          # IST has no DST
    m = (sod >= o) & (sod < c)
    lag = (ts_sec - ltt_sec)[m]
    if lag.size == 0:
        return None
    thr = int(cfg.get("lag_flag", 5))
    return {
        "n": int(lag.size), "p50": float(np.percentile(lag, 50)),
        "p95": float(np.percentile(lag, 95)), "max": int(lag.max()), "min": int(lag.min()),
        "thr": thr, "n_over": int((lag > thr).sum()),
        "pct_over": round(100.0 * float((lag > thr).sum()) / lag.size, 2),
    }


def _check_days(name, epoch, uniq, days, cfg, ref):
    """
    Per-second window check on one set of epoch seconds.
    Returns (day_records, gap_records).
    """
    out_days, out_gaps = [], []
    o, c = _secs(cfg["open_t"]), _secs(cfg["close_t"])
    close_extra = 1 if cfg["include_close"] else 0
    tol = int(cfg.get("max_ignored_gap", 0))

    for d in days:
        if cfg["skip_weekends"] and d.weekday() >= 5:
            continue

        day0 = int(pd.Timestamp(datetime.combine(d, dtime.min), tz=IST).timestamp())
        rows_day = int(np.searchsorted(epoch, day0 + 86400) - np.searchsorted(epoch, day0))

        rec = {
            "date": d.isoformat(), "day": d.strftime("%a"), "status": "",
            "expected": 0, "present": 0, "quiet": 0, "missing": 0, "coverage": None,
            "gap_ranges": 0, "longest_gap_s": 0, "first_tick": "-", "last_tick": "-",
            "rows_in_window": 0, "rows_outside": rows_day, "note": "",
        }

        if rows_day == 0 and cfg["skip_empty"]:
            rec["status"] = "SKIPPED"
            rec["note"] = "no data this day (holiday?)"
            out_days.append(rec)
            continue

        start, stop = day0 + o, day0 + c + close_extra
        present = uniq[np.searchsorted(uniq, start):np.searchsorted(uniq, stop)]
        absent = np.setdiff1d(np.arange(start, stop, dtype="int64"), present, assume_unique=True)
        if ref is not None and absent.size:
            covered = np.isin(absent, ref, assume_unique=False)
            quiet, missing = int(covered.sum()), absent[~covered]
        else:
            quiet, missing = 0, absent
        rows_win = int(np.searchsorted(epoch, stop) - np.searchsorted(epoch, start))
        expected = stop - start

        g_start, g_end = _runs(missing)
        g_len = g_end - g_start + 1

        # Short gaps (<= tolerance seconds) are just "no trade that second"
        short = g_len <= tol
        quiet += int(g_len[short].sum())
        g_start, g_end, g_len = g_start[~short], g_end[~short], g_len[~short]
        n_missing = int(g_len.sum())

        rec.update(
            status="PASS" if n_missing == 0 else "FAIL",
            expected=int(expected),
            present=int(present.size),
            quiet=quiet,
            missing=n_missing,
            coverage=round(100.0 * (expected - n_missing) / expected, 2) if expected else None,
            gap_ranges=int(g_start.size),
            longest_gap_s=int(g_len.max()) if g_len.size else 0,
            first_tick=_hms([present[0]])[0] if present.size else "-",
            last_tick=_hms([present[-1]])[0] if present.size else "-",
            rows_in_window=rows_win,
            rows_outside=rows_day - rows_win,
            note="no data this day" if rows_day == 0 else "",
        )
        out_days.append(rec)

        for a, b, s_, e_, n_s in zip(_hms(g_start), _hms(g_end), g_start, g_end, g_len):
            out_gaps.append({
                "file": name, "date": d.isoformat(), "from": a, "to": b,
                "seconds": int(n_s), "_s": int(s_), "_e": int(e_),
            })
    return out_days, out_gaps


def _status_of(days):
    checked = [x for x in days if x["status"] != "SKIPPED"]
    if not checked:
        return "NO DAYS"
    return "PASS" if all(x["status"] == "PASS" for x in checked) else "FAIL"


def verify_file(path, cfg, loaded=None, ref=None, ltt=None, ltt_ref=None):
    """
    Check each second between market open and close, for each selected date,
    using the receive timestamp.

    A second with no row in this file is a REAL gap only if no other file in
    `ref` (sorted unique seconds from the other symbols' files of the same
    type) has data for it either. If another symbol has that second, the feed
    was alive and this symbol simply had no tick -> counted as "quiet", not a gap.

    If cfg["check_ltt"]: the feed's own `ltt` is checked the same way (against
    `ltt_ref`), every receive-time gap is classified as
    "late arrival" (ltt has those seconds -> ticks arrived late, not lost) or
    "feed gap" (ltt is missing too), and the receive-lag (receive - ltt) is summarised.
    """
    path = Path(path)
    out = {
        "file": path.name, "path": str(path), "status": "ERROR", "error": None,
        "column": None, "rows": 0, "null_ts": 0, "days": [], "gaps": [], "ltt": None,
    }
    try:
        if loaded is None:
            loaded = load_seconds(path, cfg.get("col", ""), cfg["naive_tz"])
        out["column"], out["rows"], out["null_ts"] = loaded["column"], loaded["rows"], loaded["null_ts"]
        if loaded["error"]:
            out["error"] = loaded["error"]
            return out
        epoch, uniq = loaded["epoch"], loaded["uniq"]

        # Which dates to check
        if cfg["date_mode"] == "week":
            days = file_dates(path)
            if days is None:  # no date in filename -> span of the data itself
                lo = pd.Timestamp(int(epoch[0]), unit="s", tz="UTC").tz_convert(IST).date()
                hi = pd.Timestamp(int(epoch[-1]), unit="s", tz="UTC").tz_convert(IST).date()
                days = [lo + timedelta(days=i) for i in range((hi - lo).days + 1)]
        else:
            n = (cfg["end"] - cfg["start"]).days + 1
            days = [cfg["start"] + timedelta(days=i) for i in range(max(n, 0))]

        out["days"], out["gaps"] = _check_days(path.name, epoch, uniq, days, cfg, ref)
        out["status"] = _status_of(out["days"])

        # ---- optional ltt check ----
        if cfg.get("check_ltt"):
            if ltt is None:
                ltt = load_ltt(path, cfg.get("col", ""), cfg["naive_tz"])
            if ltt["error"]:
                out["ltt"] = {"status": "ERROR", "error": ltt["error"], "source": ltt["source"]}
            else:
                l_ref = ltt_ref if cfg.get("cross_check") else None
                l_days, l_gaps = _check_days(path.name, ltt["epoch"], ltt["uniq"], days, cfg, l_ref)

                # classify receive-time gaps using every ltt second seen (this + other symbols)
                all_ltt = ltt["uniq"] if ltt_ref is None else np.union1d(ltt["uniq"], ltt_ref)
                late = feed = 0
                for g in out["gaps"]:
                    rng = np.arange(g["_s"], g["_e"] + 1, dtype="int64")
                    cov = int(np.isin(rng, all_ltt).sum())
                    g["ltt_seconds"] = cov
                    if cov == g["seconds"]:
                        g["verdict"] = "Late arrival (ltt has these seconds)"
                        late += 1
                    elif cov == 0:
                        g["verdict"] = "Feed gap (ltt missing too)"
                        feed += 1
                    else:
                        g["verdict"] = f"Partly late ({cov}/{g['seconds']} s in ltt)"
                        feed += 1

                out["ltt"] = {
                    "status": _status_of(l_days), "error": None, "source": ltt["source"],
                    "rows_with_ltt": ltt["rows_with_ltt"], "days": l_days, "gaps": l_gaps,
                    "late_gaps": late, "feed_gaps": feed,
                    "lag": _lag_stats(ltt["ts_sec"], ltt["ltt_sec"], cfg),
                }
        return out

    except Exception as e:  # unreadable / corrupt file etc.
        out["status"] = "ERROR"
        out["error"] = f"{type(e).__name__}: {e}"
        return out


def summary_df(results):
    rows = []
    any_ltt = any(r.get("ltt") for r in results.values())
    for r in results.values():
        checked = [d for d in r["days"] if d["status"] != "SKIPPED"]
        exp = sum(d["expected"] for d in checked)
        miss = sum(d["missing"] for d in checked)
        failed = [d["date"] for d in checked if d["status"] == "FAIL"]
        row = {
            "File": r["file"],
            "Status": STATUS_LABEL.get(r["status"], r["status"]),
            "Timestamp col": r["column"] or "-",
            "Rows": r["rows"],
            "Days checked": len(checked),
            "Days skipped": len(r["days"]) - len(checked),
            "Expected sec": exp,
            "Quiet sec": sum(d["quiet"] for d in checked),
            "Missing sec": miss,
            "Coverage %": round(100.0 * (exp - miss) / exp, 2) if exp else None,
            "Gap ranges": sum(d["gap_ranges"] for d in checked),
        }
        if any_ltt:
            l = r.get("ltt") or {}
            lag = l.get("lag") or {}
            l_checked = [d for d in l.get("days", []) if d["status"] != "SKIPPED"]
            row.update({
                "ltt Status": STATUS_LABEL.get(l.get("status"), l.get("status") or "-"),
                "ltt missing sec": sum(d["missing"] for d in l_checked) if l else None,
                "Late-arrival gaps": l.get("late_gaps"),
                "Feed gaps": l.get("feed_gaps"),
                "Lag p50 (s)": lag.get("p50"),
                "Lag p95 (s)": lag.get("p95"),
                "Lag max (s)": lag.get("max"),
            })
        row["Failed days / error"] = ", ".join(failed) if failed else (r["error"] or "")
        rows.append(row)
    return pd.DataFrame(rows)


# =====================================================================
# Command line
# =====================================================================
def _parse_time(text):
    h, m, *rest = [int(x) for x in text.split(":")]
    return dtime(h, m, rest[0] if rest else 0)


def _parse_date(text):
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(f"bad date '{text}' (use DD-MM-YYYY or YYYY-MM-DD)")


def _group_key(p):
    kind = "quote" if "quote" in p.name.lower() else "depth" if "depth" in p.name.lower() else p.stem
    return kind, tuple(file_dates(p) or ())


def main(argv=None):
    ap = argparse.ArgumentParser(description="Verify per-second parquet completeness in market hours.")
    ap.add_argument("paths", nargs="+", help="parquet files and/or folders")
    ap.add_argument("--open", default="09:15", type=_parse_time, dest="open_t", help="market open IST (default 09:15)")
    ap.add_argument("--close", default="15:30", type=_parse_time, dest="close_t", help="market close IST (default 15:30)")
    ap.add_argument("--include-close", action="store_true", help="also require the closing second")
    ap.add_argument("--max-ignored-gap", type=int, default=3, help="ignore gaps up to N seconds (default 3)")
    ap.add_argument("--start", type=_parse_date, help="first date to check (default: from file name)")
    ap.add_argument("--end", type=_parse_date, help="last date to check (default: = --start)")
    ap.add_argument("--no-skip-weekends", action="store_true")
    ap.add_argument("--no-skip-empty", action="store_true", help="a weekday with no rows fails")
    ap.add_argument("--cross-check", action="store_true", help="a second present in another file of the same kind/date is not a gap")
    ap.add_argument("--check-ltt", action="store_true", help="also check ltt (column or raw_json)")
    ap.add_argument("--lag-flag", type=int, default=5, help="flag receive lag above N seconds (default 5)")
    ap.add_argument("--col", default="", help="timestamp column (default: auto)")
    ap.add_argument("--naive-tz", default="UTC", choices=["UTC", IST], help="timezone of tz-naive timestamps")
    ap.add_argument("--out-prefix", help="write <prefix>_summary.csv and <prefix>_gaps.csv")
    a = ap.parse_args(argv)

    if a.open_t >= a.close_t:
        ap.error("--open must be earlier than --close")

    files = []
    for x in a.paths:
        x = Path(x).expanduser()
        files += sorted(x.rglob("*.parquet")) if x.is_dir() else [x]
    if not files:
        print("No parquet files found.", file=sys.stderr)
        return 2

    cfg = {
        "open_t": a.open_t, "close_t": a.close_t, "include_close": a.include_close,
        "date_mode": "custom" if a.start else "week",
        "start": a.start, "end": a.end or a.start,
        "skip_weekends": not a.no_skip_weekends, "skip_empty": not a.no_skip_empty,
        "cross_check": a.cross_check, "max_ignored_gap": a.max_ignored_gap,
        "check_ltt": a.check_ltt, "lag_flag": a.lag_flag,
        "col": a.col, "naive_tz": a.naive_tz,
    }

    loaded = {f: load_seconds(f, a.col, a.naive_tz) for f in files}
    ltts = {f: load_ltt(f, a.col, a.naive_tz) for f in files} if a.check_ltt else {}

    groups = {}
    for f in files:
        groups.setdefault(_group_key(f), []).append(f)

    results = {}
    for f in files:
        peers = groups[_group_key(f)]
        ref = union_seconds([loaded[p] for p in peers]) if a.cross_check else None
        lref = union_seconds([ltts[p] for p in peers]) if a.check_ltt else None
        results[str(f)] = verify_file(f, cfg, loaded[f], ref, ltts.get(f), lref)

    pd.set_option("display.width", 250, "display.max_columns", 50, "display.max_colwidth", 60)
    sdf = summary_df(results)
    print(sdf.to_string(index=False))

    all_gaps = []
    for r in results.values():
        all_gaps += [{k: v for k, v in g.items() if not k.startswith("_")} for g in r["gaps"]]
        if r["ltt"] and r["ltt"].get("error"):
            print(f"\n[{r['file']}] ltt: {r['ltt']['error']}")
        elif r["ltt"] and r["ltt"].get("lag"):
            lag = r["ltt"]["lag"]
            print(f"\n[{r['file']}] ltt source {r['ltt']['source']}; lag median {lag['p50']:.0f}s, "
                  f"p95 {lag['p95']:.0f}s, max {lag['max']}s, {lag['n_over']:,} rows (> {lag['thr']}s)")
    if all_gaps:
        print("\nREAL GAPS")
        print(pd.DataFrame(all_gaps).to_string(index=False))
    for r in results.values():
        if r["error"]:
            print(f"\n[{r['file']}] ERROR: {r['error']}")

    if a.out_prefix:
        sdf.to_csv(f"{a.out_prefix}_summary.csv", index=False)
        pd.DataFrame(all_gaps).to_csv(f"{a.out_prefix}_gaps.csv", index=False)

    return 0 if all(r["status"] == "PASS" for r in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
