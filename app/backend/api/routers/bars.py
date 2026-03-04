from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.backend.api.dependencies import get_stock_repo
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.services.yahoo_provisional import (
    apply_split_gap_adjustment,
    get_provisional_daily_rows_from_spark,
    merge_daily_rows_with_provisional,
)
from app.services.box_detector import detect_boxes


router = APIRouter(prefix="/api", tags=["bars"])
logger = logging.getLogger(__name__)
_SUPPORTED_TIMEFRAMES = {"daily", "weekly", "monthly"}


class BatchBarsRequest(BaseModel):
    timeframe: str = Field(..., description="daily or monthly")
    codes: List[str] = Field(default_factory=list)
    limit: int = Field(..., ge=1, le=2000)


class BatchBarsV3Request(BaseModel):
    codes: List[str] = Field(default_factory=list)
    timeframes: List[str] = Field(default_factory=list, description="daily/weekly/monthly")
    limit: int = Field(..., ge=1, le=2000)
    includeProvisional: bool = True


def _normalize_bar_time(value: Any) -> int | None:
    try:
        iv = int(value)
    except (TypeError, ValueError):
        return None
    if iv >= 1_000_000_000_000:
        return iv // 1000
    if iv >= 1_000_000_000:
        return iv
    text = str(iv)
    if len(text) == 8 and text.isdigit():
        try:
            dt = datetime(int(text[:4]), int(text[4:6]), int(text[6:8]), tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return None
    if len(text) == 6 and text.isdigit():
        try:
            dt = datetime(int(text[:4]), int(text[4:6]), 1, tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            return None
    return None


def _build_weekly_bars_from_daily(rows: List[tuple]) -> List[tuple]:
    grouped: dict[int, list[float]] = {}
    for row in rows:
        if not row or len(row) < 5:
            continue
        ts = _normalize_bar_time(row[0])
        if ts is None:
            continue
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        week_start = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc) - timedelta(days=dt.weekday())
        key = int(week_start.timestamp())
        open_ = float(row[1])
        high = float(row[2])
        low = float(row[3])
        close = float(row[4])
        volume = float(row[5]) if len(row) >= 6 and row[5] is not None else 0.0
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = [open_, high, low, close, volume]
        else:
            existing[1] = max(existing[1], high)
            existing[2] = min(existing[2], low)
            existing[3] = close
            existing[4] += volume
    return [
        (week_key, values[0], values[1], values[2], values[3], values[4])
        for week_key, values in sorted(grouped.items(), key=lambda item: item[0])
    ]


def _to_payload_rows(rows: List[tuple], *, boxes_enabled: bool) -> Dict[str, Any]:
    return {
        "bars": [list(row) for row in rows],
        "ma": {"ma7": [], "ma20": [], "ma60": []},
        "boxes": detect_boxes(rows, range_basis="body", max_range_pct=0.2) if boxes_enabled and rows else [],
    }


def _normalize_codes(codes: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for code in codes:
        if not isinstance(code, str):
            continue
        normalized = code.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _normalize_requested_frames(raw_frames: List[str]) -> List[str]:
    seen: set[str] = set()
    requested: List[str] = []
    for frame in raw_frames:
        normalized = str(frame).strip().lower()
        if not normalized:
            continue
        if normalized not in _SUPPORTED_TIMEFRAMES:
            raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {normalized}")
        if normalized in seen:
            continue
        seen.add(normalized)
        requested.append(normalized)
    if not requested:
        return ["daily"]
    return requested


def _fetch_multi_timeframe_items(
    *,
    repo: StockRepository,
    codes: List[str],
    requested_frames: List[str],
    limit: int,
    include_provisional: bool,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    items: Dict[str, Dict[str, Dict[str, Any]]] = {code: {} for code in codes}
    if not codes:
        return items

    provisional_map: Dict[str, tuple] = {}
    if include_provisional and ("daily" in requested_frames or "weekly" in requested_frames):
        try:
            provisional_map = get_provisional_daily_rows_from_spark(codes)
        except Exception as exc:
            logger.debug("Yahoo provisional fetch skipped in batch bars: %s", exc)

    daily_rows_by_code: Dict[str, List[tuple]] | None = None
    if "daily" in requested_frames or "weekly" in requested_frames:
        raw_daily = repo.get_daily_bars_batch(codes, limit)
        daily_rows_by_code = {}
        for code in codes:
            merged = merge_daily_rows_with_provisional(
                raw_daily.get(code, []),
                provisional_map.get(code) if include_provisional else None,
            )
            merged = apply_split_gap_adjustment(merged)
            daily_rows_by_code[code] = merged
            if "daily" in requested_frames:
                items[code]["daily"] = _to_payload_rows(merged, boxes_enabled=False)

    if "weekly" in requested_frames:
        if daily_rows_by_code is None:
            daily_rows_by_code = {code: [] for code in codes}
        for code in codes:
            weekly_rows = _build_weekly_bars_from_daily(daily_rows_by_code.get(code, []))
            if limit > 0 and len(weekly_rows) > limit:
                weekly_rows = weekly_rows[-limit:]
            items[code]["weekly"] = _to_payload_rows(weekly_rows, boxes_enabled=False)

    if "monthly" in requested_frames:
        monthly_rows_by_code = repo.get_monthly_bars_batch(codes, limit)
        for code in codes:
            monthly_rows = monthly_rows_by_code.get(code, [])
            monthly_rows = apply_split_gap_adjustment(monthly_rows)
            items[code]["monthly"] = _to_payload_rows(monthly_rows, boxes_enabled=True)

    return items


@router.post("/batch_bars")
def batch_bars(
    payload: BatchBarsRequest,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Dict]:
    timeframe = str(payload.timeframe).strip().lower()
    if timeframe not in {"daily", "monthly"}:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")

    valid_codes = _normalize_codes(payload.codes)
    if not valid_codes:
        return {"items": {}}

    multi_items = _fetch_multi_timeframe_items(
        repo=repo,
        codes=valid_codes,
        requested_frames=[timeframe],
        limit=int(payload.limit),
        include_provisional=True,
    )
    items: Dict[str, Dict] = {}
    for code in valid_codes:
        code_items = multi_items.get(code, {})
        frame_payload = code_items.get(timeframe)
        if frame_payload is None:
            frame_payload = _to_payload_rows([], boxes_enabled=timeframe == "monthly")
        items[code] = frame_payload
    return {"items": items}


@router.post("/batch_bars_v3")
def batch_bars_v3(
    payload: BatchBarsV3Request,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Dict]:
    requested_frames = _normalize_requested_frames(payload.timeframes)
    valid_codes = _normalize_codes(payload.codes)
    if not valid_codes:
        return {"items": {}}
    items = _fetch_multi_timeframe_items(
        repo=repo,
        codes=valid_codes,
        requested_frames=requested_frames,
        limit=int(payload.limit),
        include_provisional=bool(payload.includeProvisional),
    )
    return {"items": items}
