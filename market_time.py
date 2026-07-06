import json
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import List, Optional, Set

try:
    from zoneinfo import ZoneInfo

    tz_kolkata = ZoneInfo("Asia/Kolkata")
except ImportError:
    import pytz

    tz_kolkata = pytz.timezone("Asia/Kolkata")

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
PRE_CONNECT = dtime(9, 14)  # 1 minute before market open

# ── Trading calendar ────────────────────────────────────────────────────
_CALENDAR_FILE = Path(__file__).resolve().parent / "trading_calendar.json"

_holidays: Set[date] = set()
_special_open: Set[date] = set()


def _load_calendar() -> None:
    global _holidays, _special_open
    if not _CALENDAR_FILE.exists():
        _holidays = set()
        _special_open = set()
        return
    try:
        with open(_CALENDAR_FILE) as f:
            cal = json.load(f)
        _holidays = {date.fromisoformat(d) for d in cal.get("holidays", [])}
        _special_open = {date.fromisoformat(d) for d in cal.get("special_open", [])}
        print(
            f"[X9_FETCHER] Trading calendar loaded: "
            f"{len(_holidays)} holidays, {len(_special_open)} special open days",
            flush=True,
        )
    except Exception as exc:
        print(
            f"[X9_FETCHER][WARN] Failed to load trading calendar: {exc}", flush=True
        )
        _holidays = set()
        _special_open = set()


_load_calendar()


def reload_calendar() -> None:
    """Reload the trading calendar from disk (call after editing the JSON)."""
    _load_calendar()


def refresh_trading_calendar() -> None:
    """
    Auto-refresh the trading calendar from NSE if:
      - the JSON file is older than 7 days, OR
      - there are no future holidays on record.

    Merges freshly fetched holidays with historical ones already in the file.
    Fails silently — a stale calendar is better than a crashed startup.
    """
    global _holidays, _special_open
    today = date.today()

    # ── decide whether a refresh is needed ──────────────────────────────
    needs_refresh = not _CALENDAR_FILE.exists()

    if not needs_refresh and _CALENDAR_FILE.exists():
        age_days = (today - date.fromtimestamp(_CALENDAR_FILE.stat().st_mtime)).days
        if age_days > 7:
            needs_refresh = True

    if not needs_refresh:
        future_hols = [d for d in _holidays if d >= today]
        if not future_hols:
            needs_refresh = True

    if not needs_refresh:
        return

    print("[CALENDAR] Checking for updated NSE holiday list...", flush=True)

    try:
        from urllib import request as _ureq

        req = _ureq.Request(
            "https://www.nseindia.com/api/holiday-master?type=trading",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept":          "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer":         "https://www.nseindia.com/",
            },
        )
        with _ureq.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")

        data = json.loads(raw)
        new_dates: List[str] = []

        # NSE returns segment keys: CM (cash market), FO, CD …
        # CM is the authoritative list; FO used as fallback/supplement
        for segment in ("CM", "FO"):
            for item in data.get(segment, []):
                raw_date = (
                    item.get("tradingDate")
                    or item.get("trade_date")
                    or item.get("Date")
                    or ""
                ).strip()
                if not raw_date:
                    continue
                for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
                    try:
                        parsed = datetime.strptime(raw_date, fmt).date()
                        new_dates.append(parsed.isoformat())
                        break
                    except ValueError:
                        continue

        if not new_dates:
            print("[CALENDAR][WARN] NSE returned no holiday dates", flush=True)
            return

        # ── merge: keep past holidays, replace future ones with fresh data ──
        existing: dict = {}
        if _CALENDAR_FILE.exists():
            with open(_CALENDAR_FILE) as f:
                existing = json.load(f)

        old_hols = [
            d for d in existing.get("holidays", [])
            if date.fromisoformat(d) < today
        ]
        merged = sorted(set(old_hols + new_dates))

        existing["holidays"]      = merged
        existing["_last_updated"] = today.isoformat()

        with open(_CALENDAR_FILE, "w") as f:
            json.dump(existing, f, indent=4)

        print(
            f"[CALENDAR] Updated: {len(merged)} holidays total "
            f"({len(new_dates)} from NSE, {len(old_hols)} historical kept)",
            flush=True,
        )
        _load_calendar()

    except Exception as exc:
        print(
            f"[CALENDAR][WARN] Auto-refresh failed ({exc}) — using existing calendar",
            flush=True,
        )


# ── Core helpers ────────────────────────────────────────────────────────


def now_kolkata() -> datetime:
    return datetime.now(tz_kolkata)


def is_trading_day(dt: Optional[datetime] = None) -> bool:
    """
    True if the given date is a trading day:
    - Weekday that is NOT in the holidays list, OR
    - Any date explicitly listed in special_open (overrides weekends).
    """
    if dt is None:
        dt = now_kolkata()
    d = dt.date() if isinstance(dt, datetime) else dt

    if d in _special_open:
        return True
    if d in _holidays:
        return False
    return d.weekday() < 5  # Mon-Fri


def is_market_open(dt: Optional[datetime] = None) -> bool:
    if dt is None:
        dt = now_kolkata()
    if not is_trading_day(dt):
        return False
    return MARKET_OPEN <= dt.time() < MARKET_CLOSE


def is_connectable(dt: Optional[datetime] = None) -> bool:
    """True if within pre-connect window or market hours on a trading day."""
    if dt is None:
        dt = now_kolkata()
    if not is_trading_day(dt):
        return False
    return PRE_CONNECT <= dt.time() < MARKET_CLOSE


def seconds_until_pre_connect(dt: Optional[datetime] = None) -> float:
    """Seconds until 9:14 on the next trading day."""
    if dt is None:
        dt = now_kolkata()

    # If today is a trading day and we haven't reached pre-connect yet → target today
    if is_trading_day(dt) and dt.time() < PRE_CONNECT:
        target = dt.replace(
            hour=PRE_CONNECT.hour,
            minute=PRE_CONNECT.minute,
            second=0,
            microsecond=0,
        )
        return max(0.0, (target - dt).total_seconds())

    # Otherwise advance day-by-day to find the next trading day (max 15 days ahead)
    target = dt.replace(
        hour=PRE_CONNECT.hour, minute=PRE_CONNECT.minute, second=0, microsecond=0
    ) + timedelta(days=1)

    for _ in range(15):
        if is_trading_day(target):
            return max(0.0, (target - dt).total_seconds())
        target += timedelta(days=1)

    # Fallback (shouldn't happen unless calendar blocks 15+ consecutive days)
    return max(0.0, (target - dt).total_seconds())


def seconds_until_close(dt: Optional[datetime] = None) -> float:
    """Seconds until market close (15:30) today."""
    if dt is None:
        dt = now_kolkata()

    close_dt = dt.replace(
        hour=MARKET_CLOSE.hour, minute=MARKET_CLOSE.minute, second=0, microsecond=0
    )
    return max(0.0, (close_dt - dt).total_seconds())