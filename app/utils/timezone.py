"""
Central IST (India Standard Time) utilities.

All date/time operations in the application must use these helpers so that
the IST timezone is defined in exactly ONE place. Changing the timezone for
the whole system means changing only the `IST` constant below.

Usage:
    from app.utils.timezone import IST, now_ist, ist_today_utc_window

    # Current IST datetime (tz-aware)
    current = now_ist()

    # Today's UTC window + date string for jobs query / sheet tab name
    start_utc, end_utc, date_str = ist_today_utc_window()
    # start_utc / end_utc are naive UTC datetimes (for Postgres TIMESTAMP WITHOUT TIME ZONE)
    # date_str  is "YYYY-MM-DD" in IST calendar
"""

import pytz
from datetime import datetime, timedelta
from typing import Tuple

# ── Single source of truth ────────────────────────────────────────────────────
IST = pytz.timezone("Asia/Kolkata")
# ──────────────────────────────────────────────────────────────────────────────


def now_ist() -> datetime:
    """Return the current datetime in IST (tz-aware)."""
    return datetime.now(IST)


def ist_today_utc_window(
    reference: datetime | None = None,
) -> Tuple[datetime, datetime, str]:
    """
    Return the UTC window that covers today's IST calendar day.

    Args:
        reference: Optional tz-aware or naive datetime to use as "now".
                   Defaults to the actual current time in IST.

    Returns:
        (start_utc, end_utc, ist_date_str) where:
        - start_utc  : naive UTC datetime for IST midnight (for Postgres queries)
        - end_utc    : naive UTC datetime for next IST midnight (exclusive boundary)
        - ist_date_str: "YYYY-MM-DD" string in IST calendar (for sheet tab names etc.)
    """
    if reference is None:
        ref_ist = now_ist()
    elif reference.tzinfo is None:
        ref_ist = IST.localize(reference)
    else:
        ref_ist = reference.astimezone(IST)

    ist_midnight = ref_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = ist_midnight.astimezone(pytz.utc).replace(tzinfo=None)  # naive UTC
    end_utc = start_utc + timedelta(days=1)
    ist_date_str = ist_midnight.strftime("%Y-%m-%d")

    return start_utc, end_utc, ist_date_str
