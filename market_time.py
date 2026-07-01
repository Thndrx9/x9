from datetime import datetime, time as dtime
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    tz_kolkata = ZoneInfo("Asia/Kolkata")
except ImportError:
    import pytz
    tz_kolkata = pytz.timezone("Asia/Kolkata")

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

def now_kolkata() -> datetime:
    return datetime.now(tz_kolkata)

def is_market_open(dt: Optional[datetime] = None) -> bool:
    if dt is None:
        dt = now_kolkata()
    if dt.weekday() >= 5:
        return False
    return MARKET_OPEN <= dt.time() < MARKET_CLOSE