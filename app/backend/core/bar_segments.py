from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence


_RELIST_GAP_DAYS = max(180, int(os.getenv("MEEMEE_RELIST_GAP_DAYS", "540")))
_MONTHLY_STALE_DAYS = max(90, int(os.getenv("MEEMEE_MONTHLY_STALE_DAYS", "120")))
_SPARSE_SEGMENT_MIN_BARS = max(2, int(os.getenv("MEEMEE_RELIST_SPARSE_MIN_BARS", "20")))


def normalize_bar_epoch(value: Any) -> int | None:
    try:
        iv = int(value)
    except (TypeError, ValueError):
        return None
    if iv >= 1_000_000_000_000:
        return iv // 1000
    if iv >= 1_000_000_000:
        return iv
    text = str(iv)
    try:
        if len(text) == 8 and text.isdigit():
            return int(datetime(int(text[:4]), int(text[4:6]), int(text[6:8]), tzinfo=timezone.utc).timestamp())
        if len(text) == 6 and text.isdigit():
            return int(datetime(int(text[:4]), int(text[4:6]), 1, tzinfo=timezone.utc).timestamp())
    except ValueError:
        return None
    return None


def trim_to_latest_continuous_segment(
    rows: Iterable[Sequence[Any]],
    *,
    key_index: int = 0,
    gap_days: int = _RELIST_GAP_DAYS,
) -> list[tuple]:
    materialized = [tuple(row) for row in rows]
    if len(materialized) < 2:
        return materialized
    threshold_sec = max(1, int(gap_days)) * 86400
    start_index = 0
    previous_ts = normalize_bar_epoch(materialized[0][key_index])
    for index in range(1, len(materialized)):
        current_ts = normalize_bar_epoch(materialized[index][key_index])
        if previous_ts is not None and current_ts is not None and current_ts - previous_ts > threshold_sec:
            start_index = index
        previous_ts = current_ts
    return materialized[start_index:]


def build_monthly_rows_from_daily(daily_rows: Iterable[Sequence[Any]], *, limit: int = 120) -> list[tuple]:
    grouped: dict[int, list[float]] = {}
    for row in [tuple(row) for row in daily_rows]:
        if len(row) < 6:
            continue
        ts = normalize_bar_epoch(row[0])
        if ts is None:
            continue
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        month_key = int(datetime(dt.year, dt.month, 1, tzinfo=timezone.utc).timestamp())
        open_ = float(row[1])
        high = float(row[2])
        low = float(row[3])
        close = float(row[4])
        volume = float(row[5]) if row[5] is not None else 0.0
        existing = grouped.get(month_key)
        if existing is None:
            grouped[month_key] = [open_, high, low, close, volume]
        else:
            existing[1] = max(existing[1], high)
            existing[2] = min(existing[2], low)
            existing[3] = close
            existing[4] += volume
    rows = [
        (month_key, values[0], values[1], values[2], values[3], values[4])
        for month_key, values in sorted(grouped.items(), key=lambda item: item[0])
    ]
    if limit > 0 and len(rows) > limit:
        return rows[-limit:]
    return rows


def should_replace_monthly_with_daily(
    daily_rows: Iterable[Sequence[Any]],
    monthly_rows: Iterable[Sequence[Any]],
    *,
    stale_days: int = _MONTHLY_STALE_DAYS,
) -> bool:
    daily_list = [tuple(row) for row in daily_rows]
    monthly_list = [tuple(row) for row in monthly_rows]
    if not daily_list:
        return False
    if not monthly_list:
        return True
    latest_daily = normalize_bar_epoch(daily_list[-1][0])
    latest_monthly = normalize_bar_epoch(monthly_list[-1][0])
    if latest_daily is None or latest_monthly is None:
        return False
    return (latest_daily - latest_monthly) > max(1, int(stale_days)) * 86400


def prefer_richer_history(
    current_rows: Iterable[Sequence[Any]],
    candidate_rows: Iterable[Sequence[Any]],
    *,
    key_index: int = 0,
    min_sparse_bars: int = _SPARSE_SEGMENT_MIN_BARS,
) -> list[tuple]:
    current = [tuple(row) for row in current_rows]
    candidate = trim_to_latest_continuous_segment(candidate_rows, key_index=key_index)
    if not candidate:
        return current
    if len(current) >= max(1, int(min_sparse_bars)):
        return current
    if not current:
        return candidate
    current_latest = normalize_bar_epoch(current[-1][key_index])
    candidate_latest = normalize_bar_epoch(candidate[-1][key_index])
    if candidate_latest is None:
        return current
    if current_latest is not None and candidate_latest < current_latest:
        return current
    if len(candidate) <= len(current):
        return current
    return candidate


def needs_history_backfill(
    rows: Iterable[Sequence[Any]],
    *,
    min_sparse_bars: int = _SPARSE_SEGMENT_MIN_BARS,
) -> bool:
    return len([tuple(row) for row in rows]) < max(1, int(min_sparse_bars))
