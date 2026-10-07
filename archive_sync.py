"""
archive_sync.py — bulk-load a COMPLETED trading day's ticks from the
upstream server's Parquet archive instead of querying its Postgres.

Upstream (x9_data_fetcher) publishes two combined files per day, all
symbols in each, with a `symbol` column:

    <ARCHIVE_BASE_PATH>/<DD-MM-YYYY>/<DD-MM-YYYY>_quote.parquet
    <ARCHIVE_BASE_PATH>/<DD-MM-YYYY>/<DD-MM-YYYY>_depth.parquet

Both come down in ONE rsync call (one SSH session). rsync's own
size/mtime quick-check makes a repeat call a near no-op, and it writes
to a temp file and renames on completion, so a half-transferred file is
never visible under its final name.

Only completed days belong here. Today's data (live session, mid-session
auto-heal, gap fills) stays on Postgres — a Parquet snapshot is only
valid once the day is over.

The readers below return per-symbol DataFrames already shaped like what
BackfillManager._fetch_ticks_batch()/_fetch_depth_batch() hand to
tick_writer.enqueue_backfill_rows(), so everything downstream (local
mirror DB, gap detection, candle building) is untouched.

Quote archive rows come in three record_types — "trade", "full",
"daily_close". Every "full" row carries every column the local schema
needs (ltp, ltt, volume, oi, circuits) with no nulls, exactly one per
broker packet; "trade" rows are the same packet's other half and
"daily_close" is an end-of-day summary, not a tick. Only "full" is read.

Environment (all optional except a reachable host):
    ARCHIVE_SYNC_ENABLED   default "1"   — "0" disables this whole path
    ARCHIVE_SSH_HOST       default $PG_HOST (same box as the main db)
    ARCHIVE_SSH_USER       default "ubuntu"
    ARCHIVE_SSH_PORT       default "22"
    ARCHIVE_SSH_KEY        default unset — path to a private key; unset
                           means ssh's normal keys/agent are used
    ARCHIVE_BASE_PATH      default "/home/ubuntu/x9_data_fetcher/archive"
    ARCHIVE_LOCAL_DIR      default "<this dir>/archive_cache"
    ARCHIVE_RSYNC_TIMEOUT  default "120" (seconds of I/O inactivity)
"""

import os
import subprocess
from datetime import date
from typing import Dict, Iterable, List, Optional

import pandas as pd
import pyarrow.parquet as pq

from tick_writer import DEPTH_LEVEL_COLUMNS, QUOTE_EXTRA_COLUMNS, _safe_symbol

KINDS = ("quote", "depth")

# Columns actually read from each file — everything else in the archive
# (open/high/low/close daily snapshots, trade-only fields, ...) is never
# used downstream, so it is never even decoded.
_QUOTE_READ_COLUMNS = ["symbol", "record_type", "timestamp", "ltp"] + [
    c for c in QUOTE_EXTRA_COLUMNS
]


def _depth_source_columns() -> Dict[str, str]:
    """{archive column -> local column}. The archive numbers levels 1..5
    and puts the level last (buy_price_1); the local schema numbers them
    0..4 with the level in the middle (buy0_price). Level 1 is the best
    price on both sides (verified against real data: bids descend and
    asks ascend from level 1), so it maps to level 0."""
    out = {}
    for side in ("buy", "sell"):
        for lvl in range(5):
            for field in ("price", "qty", "orders"):
                out[f"{side}_{field}_{lvl + 1}"] = f"{side}{lvl}_{field}"
    return out


_DEPTH_COLUMN_MAP = _depth_source_columns()
assert set(_DEPTH_COLUMN_MAP.values()) == set(DEPTH_LEVEL_COLUMNS), (
    "archive depth column mapping no longer matches tick_writer.DEPTH_LEVEL_COLUMNS"
)


def _int_if_clean(s: pd.Series) -> pd.Series:
    """Whole-number column -> int64 when nothing is missing, otherwise
    float64 with NaN (same dtypes the Postgres fetch path produces, and
    what the writers' own NULL-safe cleaning already expects)."""
    s = pd.to_numeric(s, errors="coerce")
    if s.isna().any():
        return s.astype("float64")
    return s.round().astype("int64")


class ArchiveSync:
    def __init__(self):
        self.enabled = os.getenv("ARCHIVE_SYNC_ENABLED", "1").strip() not in ("0", "false", "False", "")
        raw_host = (os.getenv("ARCHIVE_SSH_HOST") or os.getenv("PG_HOST") or "").strip().strip("'\"")
        raw_port = os.getenv("ARCHIVE_SSH_PORT", "").strip()
        # PG_HOST is written for psycopg2, which sometimes gets it as
        # "host:port" (psycopg2 itself only uses the host part), or with a
        # "postgresql://"/"ssh://" scheme, a trailing "/", or odd internal
        # whitespace from a copy-paste. ssh/rsync want a bare hostname and
        # the port separately, so all of that is stripped here rather than
        # passed through — any of it left in is what actually produces
        # rsync's "hostname contains invalid characters". An explicit
        # ARCHIVE_SSH_PORT always wins over a port found this way.
        for scheme in ("ssh://", "rsync://", "postgresql://", "postgres://"):
            if raw_host.lower().startswith(scheme):
                raw_host = raw_host[len(scheme):]
        raw_host = raw_host.split("/", 1)[0]          # drop any trailing /path
        raw_host = "".join(raw_host.split())           # drop internal whitespace
        if "@" in raw_host:                            # tolerate a user@ already in the value
            raw_host = raw_host.rsplit("@", 1)[1]
        if ":" in raw_host:
            raw_host, _, sniffed_port = raw_host.partition(":")
            raw_port = raw_port or sniffed_port
        self.host = raw_host
        self.user = os.getenv("ARCHIVE_SSH_USER", "ubuntu")
        self.port = raw_port or "22"
        self.key = os.getenv("ARCHIVE_SSH_KEY") or ""
        self.base = os.getenv("ARCHIVE_BASE_PATH", "/home/ubuntu/x9_data_fetcher/archive").rstrip("/")
        self.local_dir = os.getenv(
            "ARCHIVE_LOCAL_DIR",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "archive_cache"),
        )
        self.timeout = os.getenv("ARCHIVE_RSYNC_TIMEOUT", "120")
        if not self.host:
            self.enabled = False   # nothing to connect to
        self._symbol_maps: Dict[str, Dict[str, str]] = {}

    # ── paths ──────────────────────────────────────────────────────────
    @staticmethod
    def _d(day: date) -> str:
        return day.strftime("%d-%m-%Y")

    def remote_path(self, day: date, kind: str) -> str:
        d = self._d(day)
        return f"{self.base}/{d}/{d}_{kind}.parquet"

    def local_path(self, day: date, kind: str) -> str:
        d = self._d(day)
        return os.path.join(self.local_dir, d, f"{d}_{kind}.parquet")

    # ── download ───────────────────────────────────────────────────────
    def ensure_files(self, day: date, kinds: Iterable[str]) -> Dict[str, Optional[str]]:
        """ONE rsync call for every requested file of `day`. Returns
        {kind: local_path or None}. A file that could not be fetched but
        already exists locally from an earlier successful call is still
        returned (with a warning) — the network being down is no reason
        to ignore a good cached copy. Never raises."""
        kinds = [k for k in kinds if k in KINDS]
        result = {k: None for k in kinds}
        if not kinds or not self.enabled:
            return result

        dest_dir = os.path.join(self.local_dir, self._d(day))
        os.makedirs(dest_dir, exist_ok=True)

        ssh = ["ssh", "-p", str(self.port), "-o", "BatchMode=yes",
               "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=15"]
        if self.key:
            ssh += ["-i", self.key]

        target = f"{self.user}@{self.host}"
        # Second and later sources use rsync's ":path" shorthand (same
        # host as the first) — accepted by every rsync version.
        sources = []
        for i, k in enumerate(kinds):
            rp = self.remote_path(day, k)
            sources.append(f"{target}:{rp}" if i == 0 else f":{rp}")

        cmd = ["rsync", "-a", "--partial", f"--timeout={self.timeout}",
               "-e", " ".join(ssh), *sources, dest_dir + "/"]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=int(self.timeout) * 5)
            rc, err = proc.returncode, (proc.stderr or "").strip()
        except FileNotFoundError:
            rc, err = -1, "rsync is not installed on this machine"
        except subprocess.TimeoutExpired:
            rc, err = -2, "rsync did not finish in time"
        except Exception as exc:
            rc, err = -3, str(exc)

        for k in kinds:
            p = self.local_path(day, k)
            if os.path.isfile(p) and os.path.getsize(p) > 0:
                result[k] = p
        if rc != 0:
            missing = [k for k in kinds if result[k] is None]
            have = [k for k in kinds if result[k] is not None]
            print(
                f"[ARCHIVE][WARN] rsync for {self._d(day)} exited {rc}"
                + (f": {err.splitlines()[0]}" if err else "")
                + f" | target={self.user}@{self.host}:{self.port}"
                + (f" | not available: {', '.join(missing)}" if missing else "")
                + (f" | using earlier local copy of: {', '.join(have)}" if have else ""),
                flush=True,
            )
        return result

    def discard_local(self, day: date, kind: str):
        """Drop a local copy that turned out unreadable so the next run
        downloads a fresh one."""
        try:
            os.remove(self.local_path(day, kind))
        except OSError:
            pass
        self._symbol_maps.pop(self.local_path(day, kind), None)

    # ── reading ────────────────────────────────────────────────────────
    def _symbol_map(self, path: str) -> Dict[str, str]:
        """{lookup key -> symbol exactly as stored in the file}. Two kinds
        of key per file symbol: its UPPERCASE form (exact, case-insensitive
        match) and its _safe_symbol() form (lowercase, special characters
        dropped). The second exists because every table in this codebase is
        named with _safe_symbol() — "M&M" lives in quote_mm_* — and the
        archive spells such names the same stripped way ("MM"), so matching
        the app's "M&M" against the archive's "MM" needs the same rule on
        both sides. The two key forms can't collide (upper vs lower case);
        if two file symbols ever strip to the same key, the first one wins."""
        m = self._symbol_maps.get(path)
        if m is None:
            col = pq.read_table(path, columns=["symbol"]).column("symbol")
            m = {}
            for s in col.unique().to_pylist():
                if s is None:
                    continue
                s = str(s)
                m.setdefault(s.upper(), s)
                m.setdefault(_safe_symbol(s), s)
            self._symbol_maps[path] = m
        return m

    @staticmethod
    def _resolve(smap: Dict[str, str], name) -> Optional[str]:
        """The file's spelling of `name` (app-side), or None."""
        return smap.get(str(name).upper()) or smap.get(_safe_symbol(name))

    def unmatched(self, path: str, names: Iterable[str]) -> List[str]:
        """App-side names that have no symbol in this archive file at all."""
        smap = self._symbol_map(path)
        return [n for n in names if self._resolve(smap, n) is None]

    def read_chunk(self, kind: str, path: str, names: List[str]) -> Dict[str, pd.DataFrame]:
        """Per-symbol DataFrames for `names` (app-side spelling), shaped
        for tick_writer.enqueue_backfill_rows() but WITHOUT the symbol
        column, ordered by timestamp. Symbols the file doesn't contain
        are simply absent from the result. Reads only the needed columns
        and only these symbols' rows, so memory is bounded by one chunk."""
        smap = self._symbol_map(path)
        file_to_app = {}
        for n in names:
            actual = self._resolve(smap, n)
            if actual is not None:
                file_to_app[actual] = n
        if not file_to_app:
            return {}

        filters = [("symbol", "in", list(file_to_app))]
        if kind == "quote":
            filters.append(("record_type", "==", "full"))
            cols = _QUOTE_READ_COLUMNS
        else:
            cols = ["symbol", "timestamp", "ltp"] + list(_DEPTH_COLUMN_MAP)

        df = pq.read_table(path, columns=cols, filters=filters).to_pandas()
        if df.empty:
            return {}
        if kind == "quote":
            df = df[df["ltp"].notna() & df["timestamp"].notna()]
            df = self._shape_quote(df)
        else:
            df = df[df["timestamp"].notna()]
            df = self._shape_depth(df)

        out = {}
        for file_sym, g in df.groupby("symbol", sort=False):
            g = g.drop(columns=["symbol"]).sort_values("timestamp", kind="stable").reset_index(drop=True)
            out[file_to_app[file_sym]] = g
        return out

    @staticmethod
    def _shape_quote(df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame({
            "symbol":    df["symbol"].to_numpy(),
            "timestamp": df["timestamp"].astype("int64").to_numpy(),
            "ltp":       df["ltp"].astype("float64").to_numpy(),
            # last_quantity is gone upstream; qty is kept only as a
            # constant so consumers that expect the key keep working.
            "qty":       0,
        })
        for c in QUOTE_EXTRA_COLUMNS:
            if c not in df.columns:
                out[c] = None
            elif c in ("upper_circuit", "lower_circuit"):
                out[c] = df[c].astype("float64").to_numpy()
            else:
                out[c] = _int_if_clean(df[c]).to_numpy()
        return out

    @staticmethod
    def _shape_depth(df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame({
            "symbol":    df["symbol"].to_numpy(),
            "timestamp": df["timestamp"].astype("int64").to_numpy(),
            "ltp":       df["ltp"].astype("float64").to_numpy(),
        })
        for src, dst in _DEPTH_COLUMN_MAP.items():
            out[dst] = df[src].to_numpy() if src in df.columns else None
        # local column order, exactly as _fetch_depth_batch produces it
        return out[["symbol", "timestamp", "ltp"] + list(DEPTH_LEVEL_COLUMNS)]
