from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")


def jst_now() -> datetime:
    return datetime.now(JST)


def _format_event_date(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return None
    return None


def _format_event_timestamp(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return None
    return None


def _parse_daily_date(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        raw = str(int(value)).zfill(8)
        year = int(raw[:4])
        month = int(raw[4:6])
        day = int(raw[6:8])
        return datetime(year, month, day)
    except (ValueError, TypeError):
        return None
