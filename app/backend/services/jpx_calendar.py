from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

_JST = timezone(timedelta(hours=9))


@dataclass(frozen=True)
class JpxSessionInfo:
    is_trading_day: bool
    day_type: str
    close_time_jst: str
    pan_finalize_after_jst: str
    yahoo_persist_after_jst: str


def jst_now() -> datetime:
    return datetime.now(_JST)


def _env_date_set(name: str) -> set[int]:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return set()
    out: set[int] = set()
    for part in raw.split(","):
        text = part.strip().replace("-", "")
        if len(text) == 8 and text.isdigit():
            out.add(int(text))
    return out


def _normalize_date_key(value: int | str | datetime | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.astimezone(_JST).strftime("%Y%m%d"))
    try:
        raw = int(str(value).replace("-", ""))
    except (TypeError, ValueError):
        return None
    text = str(abs(raw))
    if len(text) == 8:
        return int(text)
    if raw >= 1_000_000_000_000:
        return int(datetime.fromtimestamp(raw / 1000, tz=_JST).strftime("%Y%m%d"))
    if raw >= 1_000_000_000:
        return int(datetime.fromtimestamp(raw, tz=_JST).strftime("%Y%m%d"))
    return None


def _parse_hhmm(value: str) -> int:
    try:
        hour_text, minute_text = value.split(":", 1)
        return max(0, min(23, int(hour_text))) * 60 + max(0, min(59, int(minute_text)))
    except Exception:
        return 0


def get_jpx_session_info(now: datetime | None = None) -> JpxSessionInfo:
    current = now.astimezone(_JST) if isinstance(now, datetime) else jst_now()
    date_key = int(current.strftime("%Y%m%d"))
    holidays = _env_date_set("MEEMEE_JPX_HOLIDAYS")
    half_days = _env_date_set("MEEMEE_JPX_HALF_DAYS")
    is_weekday = current.weekday() < 5
    is_trading_day = is_weekday and date_key not in holidays
    is_half_day = is_trading_day and date_key in half_days
    if not is_trading_day:
        return JpxSessionInfo(
            is_trading_day=False,
            day_type="closed",
            close_time_jst="15:30",
            pan_finalize_after_jst="16:00",
            yahoo_persist_after_jst="15:45",
        )
    if is_half_day:
        return JpxSessionInfo(
            is_trading_day=True,
            day_type="half_day",
            close_time_jst="11:30",
            pan_finalize_after_jst="12:00",
            yahoo_persist_after_jst="11:45",
        )
    return JpxSessionInfo(
        is_trading_day=True,
        day_type="full_day",
        close_time_jst="15:30",
        pan_finalize_after_jst="16:00",
        yahoo_persist_after_jst="15:45",
    )


def get_intraday_refresh_end_minute(now: datetime | None = None) -> int:
    session = get_jpx_session_info(now)
    if not session.is_trading_day:
        return 0
    return _parse_hhmm(session.pan_finalize_after_jst)


def should_pan_be_finalized_for_date(value: int | str | datetime | None, now: datetime | None = None) -> bool:
    target_key = _normalize_date_key(value)
    if target_key is None:
        return False
    current = now.astimezone(_JST) if isinstance(now, datetime) else jst_now()
    today_key = int(current.strftime("%Y%m%d"))
    if target_key < today_key:
        return True
    if target_key > today_key:
        return False
    session = get_jpx_session_info(current)
    if not session.is_trading_day:
        return True
    now_minutes = current.hour * 60 + current.minute
    return now_minutes >= _parse_hhmm(session.pan_finalize_after_jst)


__all__ = [
    "JpxSessionInfo",
    "get_intraday_refresh_end_minute",
    "get_jpx_session_info",
    "jst_now",
    "should_pan_be_finalized_for_date",
]
