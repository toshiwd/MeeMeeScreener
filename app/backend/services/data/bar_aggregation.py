from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Sequence


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_month_start(value: Any) -> datetime | None:
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None

    if raw >= 1_000_000_000_000:
        return datetime.fromtimestamp(raw / 1000, tz=timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if raw >= 1_000_000_000:
        return datetime.fromtimestamp(raw, tz=timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    text = str(abs(raw))
    if len(text) == 8:
        try:
            return datetime(int(text[:4]), int(text[4:6]), 1, tzinfo=timezone.utc)
        except ValueError:
            return None
    if len(text) == 6:
        try:
            return datetime(int(text[:4]), int(text[4:6]), 1, tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _month_bucket(value: Any) -> int | None:
    dt = _to_month_start(value)
    if dt is None:
        return None
    return dt.year * 100 + dt.month


def _month_row_key(*, daily_value: Any, reference_value: Any | None) -> int:
    month_start = _to_month_start(daily_value)
    if month_start is None:
        return int(daily_value)

    ref = reference_value
    try:
        ref_raw = int(ref) if ref is not None else None
    except (TypeError, ValueError):
        ref_raw = None

    if ref_raw is not None:
        ref_text = str(abs(ref_raw))
        if ref_raw >= 1_000_000_000_000:
            return int(month_start.timestamp() * 1000)
        if ref_raw >= 1_000_000_000:
            return int(month_start.timestamp())
        if len(ref_text) == 8:
            return month_start.year * 10000 + month_start.month * 100 + 1
        if len(ref_text) == 6:
            return month_start.year * 100 + month_start.month

    return int(month_start.timestamp())


def _sort_key(value: Any) -> tuple[int, int]:
    month = _month_bucket(value)
    if month is not None:
        return (0, month)
    try:
        return (1, int(value))
    except (TypeError, ValueError):
        return (2, 0)


def _normalize_daily_row(row: Sequence[Any]) -> tuple[Any, float, float, float, float, float] | None:
    if len(row) < 5:
        return None
    open_ = _to_float(row[1])
    high = _to_float(row[2])
    low = _to_float(row[3])
    close = _to_float(row[4])
    if open_ is None or high is None or low is None or close is None:
        return None
    volume = _to_float(row[5]) if len(row) >= 6 else 0.0
    return (row[0], open_, high, low, close, 0.0 if volume is None else volume)


def merge_monthly_rows_with_daily(
    monthly_rows: Iterable[Sequence[Any]],
    daily_rows: Iterable[Sequence[Any]],
) -> list[tuple]:
    base_monthly = [tuple(row) for row in monthly_rows if row]
    normalized_daily = [
        normalized
        for normalized in (_normalize_daily_row(row) for row in daily_rows)
        if normalized is not None
    ]
    if not normalized_daily:
        return base_monthly

    target_month = _month_bucket(normalized_daily[-1][0])
    if target_month is None:
        return base_monthly

    month_daily = [row for row in normalized_daily if _month_bucket(row[0]) == target_month]
    if not month_daily:
        return base_monthly

    existing_index: int | None = None
    reference_key: Any | None = base_monthly[-1][0] if base_monthly else None
    for idx in range(len(base_monthly) - 1, -1, -1):
        row = base_monthly[idx]
        if _month_bucket(row[0]) == target_month:
            existing_index = idx
            reference_key = row[0]
            break

    merged_row = (
        _month_row_key(daily_value=month_daily[0][0], reference_value=reference_key),
        month_daily[0][1],
        max(row[2] for row in month_daily),
        min(row[3] for row in month_daily),
        month_daily[-1][4],
        sum(row[5] for row in month_daily),
    )

    if existing_index is None:
        base_monthly.append(merged_row)
    else:
        base_monthly[existing_index] = merged_row

    base_monthly.sort(key=lambda row: _sort_key(row[0]))
    return base_monthly


__all__ = ["merge_monthly_rows_with_daily"]
