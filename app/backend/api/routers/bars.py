from __future__ import annotations

import copy
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import OrderedDict
from threading import Event, Lock
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.backend.api.dependencies import get_stock_repo
from app.core.config import config
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.services.data.yahoo_provisional import (
    apply_split_gap_adjustment,
    get_provisional_daily_rows_from_spark,
    merge_daily_rows_with_provisional,
    normalize_date_key,
)
from app.services.box_detector import detect_boxes


router = APIRouter(prefix="/api", tags=["bars"])
logger = logging.getLogger(__name__)
_SUPPORTED_TIMEFRAMES = {"daily", "weekly", "monthly"}
_BATCH_V3_CACHE_TTL_SEC = 300.0
_BATCH_V3_CACHE_MAX_ENTRIES = 512
_batch_v3_cache_lock = Lock()
_batch_v3_cache: "OrderedDict[tuple[Any, ...], tuple[float, Dict[str, Dict[str, Dict[str, Any]]]]]" = OrderedDict()
_batch_v3_inflight: dict[tuple[Any, ...], Event] = {}


class BatchBarsRequest(BaseModel):
    timeframe: str = Field(..., description="daily or monthly")
    codes: List[str] = Field(default_factory=list)
    limit: int = Field(..., ge=1, le=10000)


class BatchBarsV3Request(BaseModel):
    codes: List[str] = Field(default_factory=list)
    timeframes: List[str] = Field(default_factory=list, description="daily/weekly/monthly")
    limit: int = Field(..., ge=1, le=10000)
    timeframeLimits: Dict[str, int] = Field(default_factory=dict)
    includeProvisional: bool = True
    includeBoxes: bool = False
    asof: str | int | None = None


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


def _parse_asof(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        raw = str(value)
    else:
        raw = str(value).strip()
    if not raw:
        return None
    if raw.isdigit() and len(raw) == 8:
        parsed = datetime.strptime(raw, "%Y%m%d").replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            continue
    if raw.isdigit():
        value_int = int(raw)
        if value_int > 1_000_000_000_000:
            return int(value_int / 1000)
        return value_int
    return None


def _to_payload_rows(rows: List[tuple], *, boxes_enabled: bool) -> Dict[str, Any]:
    return {
        "bars": [list(row) for row in rows],
        "ma": {"ma7": [], "ma20": [], "ma60": []},
        "boxes": detect_boxes(rows, range_basis="body", max_range_pct=0.2) if boxes_enabled and rows else [],
    }


def _build_batch_meta(*, include_provisional: bool = False, asof_dt: int | None = None) -> Dict[str, str | None]:
    db_path = getattr(config, "DB_PATH", None)
    data_version: str | None = None
    if db_path:
        try:
            mtime = Path(str(db_path)).stat().st_mtime
            data_version = f"duckdb-mtime:{mtime:.6f}"
        except OSError:
            data_version = None
    if include_provisional and asof_dt is None:
        # Yahoo provisional rows can change intraday without touching DuckDB.
        # Add a short JST minute bucket so frontend caches refresh while market data is live.
        live_bucket = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=9))).strftime("%Y%m%d%H%M")
        data_version = f"{data_version}|yf-live:{live_bucket}" if data_version else f"yf-live:{live_bucket}"
    return {
        "data_version": data_version,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
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


def _normalize_timeframe_limits(raw_limits: Dict[str, int]) -> Dict[str, int]:
    normalized: Dict[str, int] = {}
    for raw_frame, raw_limit in raw_limits.items():
        frame = str(raw_frame).strip().lower()
        if not frame:
            continue
        if frame not in _SUPPORTED_TIMEFRAMES:
            raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {frame}")
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail=f"Invalid timeframe limit for {frame}") from None
        if limit < 1:
            raise HTTPException(status_code=400, detail=f"Invalid timeframe limit for {frame}")
        normalized[frame] = limit
    return normalized


def _make_batch_v3_cache_key(
    *,
    codes: List[str],
    requested_frames: List[str],
    limit: int,
    timeframe_limits: Dict[str, int],
    include_provisional: bool,
    include_boxes: bool,
    asof_dt: int | None,
    data_version: str | None,
) -> tuple[Any, ...]:
    normalized_limits = tuple(sorted((frame, int(value)) for frame, value in timeframe_limits.items()))
    return (
        tuple(codes),
        tuple(requested_frames),
        int(limit),
        normalized_limits,
        bool(include_provisional),
        bool(include_boxes),
        int(asof_dt) if asof_dt is not None else None,
        data_version,
    )


def _get_cached_batch_v3_items(cache_key: tuple[Any, ...]) -> Dict[str, Dict[str, Dict[str, Any]]] | None:
    now = datetime.now(timezone.utc).timestamp()
    with _batch_v3_cache_lock:
        expired_keys = [key for key, (expires_at, _) in _batch_v3_cache.items() if expires_at <= now]
        for key in expired_keys:
            _batch_v3_cache.pop(key, None)
        cached = _batch_v3_cache.get(cache_key)
        if cached is None:
            return None
        expires_at, items = cached
        if expires_at <= now:
            _batch_v3_cache.pop(cache_key, None)
            return None
        _batch_v3_cache.move_to_end(cache_key)
        return copy.deepcopy(items)


def _store_cached_batch_v3_items(
    cache_key: tuple[Any, ...],
    items: Dict[str, Dict[str, Dict[str, Any]]],
) -> None:
    expires_at = datetime.now(timezone.utc).timestamp() + _BATCH_V3_CACHE_TTL_SEC
    with _batch_v3_cache_lock:
        _batch_v3_cache[cache_key] = (expires_at, copy.deepcopy(items))
        _batch_v3_cache.move_to_end(cache_key)
        while len(_batch_v3_cache) > _BATCH_V3_CACHE_MAX_ENTRIES:
            _batch_v3_cache.popitem(last=False)


def _claim_batch_v3_inflight(cache_key: tuple[Any, ...]) -> tuple[Event, bool]:
    with _batch_v3_cache_lock:
        event = _batch_v3_inflight.get(cache_key)
        if event is not None:
            return event, False
        event = Event()
        _batch_v3_inflight[cache_key] = event
        return event, True


def _finish_batch_v3_inflight(cache_key: tuple[Any, ...]) -> None:
    with _batch_v3_cache_lock:
        event = _batch_v3_inflight.pop(cache_key, None)
    if event is not None:
        event.set()


def _fetch_multi_timeframe_items(
    *,
    repo: StockRepository,
    codes: List[str],
    requested_frames: List[str],
    limit: int,
    timeframe_limits: Dict[str, int] | None,
    include_provisional: bool,
    include_boxes: bool,
    asof_dt: int | None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    items: Dict[str, Dict[str, Dict[str, Any]]] = {code: {} for code in codes}
    if not codes:
        return items

    frame_limits = {
        frame: int(timeframe_limits.get(frame, limit)) if timeframe_limits else int(limit)
        for frame in _SUPPORTED_TIMEFRAMES
    }
    daily_fetch_limit = max(
        frame_limits["daily"] if "daily" in requested_frames else 0,
        frame_limits["weekly"] if "weekly" in requested_frames else 0,
        max(frame_limits["monthly"] * 25, 260) if "monthly" in requested_frames else 0,
    )

    provisional_map: Dict[str, tuple] = {}
    if include_provisional and asof_dt is None and (
        "daily" in requested_frames or "weekly" in requested_frames or "monthly" in requested_frames
    ):
        try:
            provisional_map_raw = get_provisional_daily_rows_from_spark(
                codes,
                prefer_chart_ohlc=True,
            )
            today_key_jst = int((datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y%m%d"))
            provisional_map = {
                code: row
                for code, row in provisional_map_raw.items()
                if row and normalize_date_key(row[0]) == today_key_jst
            }
        except Exception as exc:
            logger.debug("Yahoo provisional fetch skipped in batch bars: %s", exc)

    daily_rows_by_code: Dict[str, List[tuple]] | None = None
    raw_daily_rows_by_code: Dict[str, List[tuple]] | None = None
    if "daily" in requested_frames or "weekly" in requested_frames:
        raw_daily = repo.get_daily_bars_batch(codes, daily_fetch_limit, asof_dt=asof_dt)
        raw_daily_rows_by_code = {}
        daily_rows_by_code = {}
        for code in codes:
            raw_rows = raw_daily.get(code, [])
            raw_daily_rows_by_code[code] = raw_rows
            merged = merge_daily_rows_with_provisional(
                raw_rows,
                provisional_map.get(code) if include_provisional else None,
            )
            merged = apply_split_gap_adjustment(merged)
            daily_rows_by_code[code] = merged
            if "daily" in requested_frames:
                daily_rows = merged[-frame_limits["daily"] :] if frame_limits["daily"] > 0 else merged
                items[code]["daily"] = _to_payload_rows(daily_rows, boxes_enabled=False)

    if "weekly" in requested_frames:
        if daily_rows_by_code is None:
            daily_rows_by_code = {code: [] for code in codes}
        for code in codes:
            weekly_rows = _build_weekly_bars_from_daily(daily_rows_by_code.get(code, []))
            weekly_limit = frame_limits["weekly"]
            if weekly_limit > 0 and len(weekly_rows) > weekly_limit:
                weekly_rows = weekly_rows[-weekly_limit:]
            items[code]["weekly"] = _to_payload_rows(weekly_rows, boxes_enabled=False)

    if "monthly" in requested_frames:
        monthly_rows_by_code = repo.get_monthly_bars_batch(
            codes,
            frame_limits["monthly"],
            asof_dt=asof_dt,
            recent_daily_rows_by_code=raw_daily_rows_by_code,
        )
        for code in codes:
            monthly_rows = monthly_rows_by_code.get(code, [])
            items[code]["monthly"] = _to_payload_rows(
                monthly_rows,
                boxes_enabled=include_boxes,
            )

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
        timeframe_limits=None,
        include_provisional=True,
        include_boxes=True,
        asof_dt=None,
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
    asof_dt = _parse_asof(payload.asof)
    meta = _build_batch_meta(
        include_provisional=bool(payload.includeProvisional),
        asof_dt=asof_dt,
    )
    if not valid_codes:
        return {"items": {}, "meta": meta}
    timeframe_limits = _normalize_timeframe_limits(payload.timeframeLimits)
    cache_key = _make_batch_v3_cache_key(
        codes=valid_codes,
        requested_frames=requested_frames,
        limit=int(payload.limit),
        timeframe_limits=timeframe_limits,
        include_provisional=bool(payload.includeProvisional),
        include_boxes=bool(payload.includeBoxes),
        asof_dt=asof_dt,
        data_version=meta.get("data_version"),
    )
    cached_items = _get_cached_batch_v3_items(cache_key)
    if cached_items is not None:
        return {"items": cached_items, "meta": meta}

    inflight_event, is_owner = _claim_batch_v3_inflight(cache_key)
    if not is_owner:
        inflight_event.wait(timeout=15.0)
        cached_items = _get_cached_batch_v3_items(cache_key)
        if cached_items is not None:
            return {"items": cached_items, "meta": meta}

    try:
        items = _fetch_multi_timeframe_items(
            repo=repo,
            codes=valid_codes,
            requested_frames=requested_frames,
            limit=int(payload.limit),
            timeframe_limits=timeframe_limits,
            include_provisional=bool(payload.includeProvisional),
            include_boxes=bool(payload.includeBoxes),
            asof_dt=asof_dt,
        )
        _store_cached_batch_v3_items(cache_key, items)
    finally:
        if is_owner:
            _finish_batch_v3_inflight(cache_key)
    return {"items": items, "meta": meta}
