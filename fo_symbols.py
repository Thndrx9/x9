"""
fo_symbols.py — F&O-eligible underlying list, sourced from OpenAlgo.

Client-side counterpart to x9_data_fetcher's fo_symbols.py (the
upstream server repo) — same purpose, same OpenAlgo instruments-API
approach, same daily-refresh cache, adapted only for this repo's flat
(same-directory) import style and its own utils.load_symbols().

Problem this solves: NSE's Closing Auction Session (CAS, live since
August 3, 2026) only applies to stocks that have active F&O (futures &
options) contracts — those stocks stop continuous trading at 15:15,
while every other stock keeps trading normally to 15:30. This repo's
own candle-building paths (OHLCCollector.aggregate_ticks_to_candles(),
BackfillManager._build_1m_candles_from_main_db_ticks()) need to know,
per symbol, which group it's in, so they stop building continuous-
trading candles at 15:15 for F&O symbols specifically, without cutting
off real trading data for everyone else. See:
https://www.nseindia.com/static/products-services/closing-auction-session

Rather than scraping NSE's website (session/cookie handling, a daily-
changing download URL), this asks OpenAlgo directly — it already
maintains a master instrument list, including the NFO (F&O) segment,
via its own /api/v1/instruments endpoint. Every NFO row carries a
"name" field: the underlying's symbol. Collecting that field across
every NFO row gives the complete, current F&O-eligible set, in the
same OpenAlgo standard symbol format symbols.csv already uses — no
separate mapping needed.

The F&O-eligible list itself changes rarely (NSE reviews it every few
months, not daily — see SEBI's periodic eligibility criteria), so this
doesn't need to be fetched on every candle-building run. It's cached
to a local JSON file and only re-fetched once the cache is older than
FO_LIST_MAX_AGE_HOURS (default 24h — i.e. refreshed at most once a
day), matching how often OpenAlgo's own master contracts typically
refresh.

Usage:
    from fo_symbols import get_fo_underlyings
    fo_set = get_fo_underlyings(api_key)
    is_fo = "RELIANCE" in fo_set

Safe to call even if the fetch fails (network issue, OpenAlgo down,
bad API key) — falls back to whatever's in the cache file, however
stale, or an empty set if there's no cache at all yet. Never raises.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Optional, Set
from urllib import request
from urllib.error import URLError

FO_LIST_MAX_AGE_HOURS = float(os.getenv("FO_LIST_MAX_AGE_HOURS", "24").strip() or "24")

_CACHE_FILENAME = "fo_symbols_cache.json"


def _cache_path() -> Path:
    """
    Same directory as this module — so the cache lives alongside the
    code regardless of which directory the process was launched from.
    """
    return Path(__file__).resolve().parent / _CACHE_FILENAME


def _instruments_endpoint() -> str:
    override = os.getenv("OPENALGO_INSTRUMENTS_URL", "").strip()
    if override:
        return override
    host = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000").rstrip("/")
    return f"{host}/api/v1/instruments"


def _fetch_nfo_underlyings(api_key: str) -> Optional[Set[str]]:
    """
    Calls OpenAlgo's instruments API, filtered to the NFO exchange, and
    returns the set of unique underlying names (uppercased) across every
    NFO row. Returns None (not an empty set) on any failure, so the
    caller can tell "genuinely fetched zero rows" apart from "the fetch
    itself failed" — the latter should fall back to cache, not overwrite
    a good cache with an empty result.
    """
    url = f"{_instruments_endpoint()}?apikey={api_key}&exchange=NFO&format=json"
    http_request = request.Request(url, method="GET")
    try:
        with request.urlopen(http_request, timeout=30) as response:
            raw_response = response.read().decode("utf-8")
            payload = json.loads(raw_response)
    except (URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
        print(f"[FO_SYMBOLS][ERROR] instruments fetch failed: {exc}", flush=True)
        return None

    rows = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        print(f"[FO_SYMBOLS][ERROR] unexpected instruments response shape", flush=True)
        return None

    underlyings: Set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = (row.get("name") or "").strip().upper()
        if name:
            underlyings.add(name)
    return underlyings


def _load_cache() -> Optional[dict]:
    path = _cache_path()
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _save_cache(underlyings: Set[str]) -> None:
    path = _cache_path()
    payload = {
        "fetched_at": time.time(),
        "underlyings": sorted(underlyings),
    }
    try:
        path.write_text(json.dumps(payload), encoding="utf-8")
    except OSError as exc:
        print(f"[FO_SYMBOLS][ERROR] cache write failed: {exc}", flush=True)


def get_fo_underlyings(api_key: str, max_age_hours: float = FO_LIST_MAX_AGE_HOURS) -> Set[str]:
    """
    Returns the current set of F&O-eligible underlying symbols
    (uppercased, OpenAlgo standard format — matches symbols.csv).

    Re-fetches from OpenAlgo only if the cache is missing or older than
    max_age_hours; otherwise returns the cached set as-is, no network
    call. This means calling it once per candle-building run costs
    nothing extra on a normal day — it only actually hits OpenAlgo
    roughly once a day.

    Never raises. If a fresh fetch is needed but fails, falls back to
    whatever's in the cache (even if stale) rather than blocking
    candle building on a network hiccup. Returns an empty set only if
    there's no usable cache AND the fetch also failed — callers should
    treat an empty set as "unknown / not verified" rather than
    "confirmed no symbols are F&O".
    """
    cache = _load_cache()
    cache_age_hours = None
    if cache and isinstance(cache.get("fetched_at"), (int, float)):
        cache_age_hours = (time.time() - cache["fetched_at"]) / 3600.0

    if cache_age_hours is not None and cache_age_hours < max_age_hours:
        return set(cache.get("underlyings", []))

    fresh = _fetch_nfo_underlyings(api_key)
    if fresh is not None:
        _save_cache(fresh)
        print(f"[FO_SYMBOLS] refreshed — {len(fresh)} F&O-eligible underlying(s)", flush=True)
        return fresh

    # Fetch failed — fall back to whatever cache exists, however stale.
    if cache:
        print(
            f"[FO_SYMBOLS][WARN] using stale cache "
            f"({cache_age_hours:.1f}h old) — refresh failed",
            flush=True,
        )
        return set(cache.get("underlyings", []))

    print("[FO_SYMBOLS][WARN] no cache and refresh failed — treating as empty (unknown)", flush=True)
    return set()
