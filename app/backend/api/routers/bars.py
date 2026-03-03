from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.backend.api.dependencies import get_stock_repo
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.services.yahoo_provisional import (
    get_provisional_daily_rows_from_spark,
    merge_daily_rows_with_provisional,
)
from app.services.box_detector import detect_boxes


router = APIRouter(prefix="/api", tags=["bars"])
logger = logging.getLogger(__name__)
_BATCH_BARS_V2 = os.getenv("BATCH_BARS_V2", "1").lower() in {"1", "true", "yes", "on"}


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


@router.post("/batch_bars")
def batch_bars(
    payload: BatchBarsRequest,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Dict]:
    timeframe = payload.timeframe
    if timeframe not in {"daily", "monthly"}:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")

    items: Dict[str, Dict] = {}
    valid_codes = [code.strip() for code in payload.codes if isinstance(code, str) and code.strip()]
    if not valid_codes:
        return {"items": items}

    if _BATCH_BARS_V2:
        if timeframe == "daily":
            rows_by_code = repo.get_daily_bars_batch(valid_codes, payload.limit)
            provisional_map: Dict[str, tuple] = {}
            try:
                provisional_map = get_provisional_daily_rows_from_spark(valid_codes)
            except Exception as exc:
                logger.debug("Yahoo provisional batch fetch skipped: %s", exc)
            for code in valid_codes:
                rows = merge_daily_rows_with_provisional(
                    rows_by_code.get(code, []),
                    provisional_map.get(code),
                )
                items[code] = {
                    "bars": [list(row) for row in rows],
                    "ma": {"ma7": [], "ma20": [], "ma60": []},
                    "boxes": [],
                }
        else:
            rows_by_code = repo.get_monthly_bars_batch(valid_codes, payload.limit)
            for code in valid_codes:
                rows = rows_by_code.get(code, [])
                items[code] = {
                    "bars": [list(row) for row in rows],
                    "ma": {"ma7": [], "ma20": [], "ma60": []},
                    "boxes": detect_boxes(rows, range_basis="body", max_range_pct=0.2) if rows else [],
                }
        return {"items": items}

    legacy_provisional_map: Dict[str, tuple] = {}
    if timeframe == "daily":
        try:
            legacy_provisional_map = get_provisional_daily_rows_from_spark(valid_codes)
        except Exception as exc:
            logger.debug("Yahoo provisional legacy batch fetch skipped: %s", exc)

    for code in valid_codes:
        if timeframe == "daily":
            rows = repo.get_daily_bars(code, payload.limit)
            provisional_row = legacy_provisional_map.get(code)
            rows = merge_daily_rows_with_provisional(rows, provisional_row)
            boxes: list[dict] = []
        else:
            rows = repo.get_monthly_bars(code, payload.limit)
            boxes = detect_boxes(rows, range_basis="body", max_range_pct=0.2)

        items[code] = {
            "bars": [list(row) for row in rows],
            "ma": {"ma7": [], "ma20": [], "ma60": []},
            "boxes": boxes,
        }

    return {"items": items}


@router.post("/batch_bars_v3")
def batch_bars_v3(
    payload: BatchBarsV3Request,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Dict]:
    requested_frames = [str(frame).strip().lower() for frame in payload.timeframes if str(frame).strip()]
    if not requested_frames:
        requested_frames = ["daily"]
    for frame in requested_frames:
        if frame not in {"daily", "weekly", "monthly"}:
            raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {frame}")

    valid_codes = [code.strip() for code in payload.codes if isinstance(code, str) and code.strip()]
    if not valid_codes:
        return {"items": {}}

    include_provisional = bool(payload.includeProvisional)
    provisional_map: Dict[str, tuple] = {}
    if include_provisional and ("daily" in requested_frames or "weekly" in requested_frames):
        try:
            provisional_map = get_provisional_daily_rows_from_spark(valid_codes)
        except Exception as exc:
            logger.debug("Yahoo provisional fetch skipped in batch_bars_v3: %s", exc)

    items: Dict[str, Dict[str, Dict[str, Any]]] = {code: {} for code in valid_codes}
    daily_rows_by_code: Dict[str, List[tuple]] | None = None

    if "daily" in requested_frames or "weekly" in requested_frames:
        raw_daily = repo.get_daily_bars_batch(valid_codes, payload.limit)
        daily_rows_by_code = {}
        for code in valid_codes:
            merged = merge_daily_rows_with_provisional(
                raw_daily.get(code, []),
                provisional_map.get(code) if include_provisional else None,
            )
            daily_rows_by_code[code] = merged
            if "daily" in requested_frames:
                items[code]["daily"] = _to_payload_rows(merged, boxes_enabled=False)

    if "weekly" in requested_frames:
        if daily_rows_by_code is None:
            daily_rows_by_code = {code: [] for code in valid_codes}
        for code in valid_codes:
            weekly_rows = _build_weekly_bars_from_daily(daily_rows_by_code.get(code, []))
            if payload.limit > 0 and len(weekly_rows) > payload.limit:
                weekly_rows = weekly_rows[-payload.limit:]
            items[code]["weekly"] = _to_payload_rows(weekly_rows, boxes_enabled=False)

    if "monthly" in requested_frames:
        monthly_rows_by_code = repo.get_monthly_bars_batch(valid_codes, payload.limit)
        for code in valid_codes:
            monthly_rows = monthly_rows_by_code.get(code, [])
            items[code]["monthly"] = _to_payload_rows(monthly_rows, boxes_enabled=True)

    return {"items": items}
