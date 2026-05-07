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
from app.backend.services.data.bar_aggregation import merge_weekly_rows_with_daily
from app.backend.services.data.yahoo_provisional import (
    apply_split_gap_adjustment,
    get_provisional_daily_rows_from_spark,
    merge_daily_rows_with_provisional,
    normalize_date_key,
)
from shared.chart_data_provenance import build_chart_data_provenance
from app.services.box_detector import detect_boxes


router = APIRouter(prefix="/api", tags=["bars"])
logger = logging.getLogger(__name__)
_SUPPORTED_TIMEFRAMES = {"daily", "weekly", "monthly"}
_BATCH_V3_CACHE_TTL_SEC = 300.0
_BATCH_V3_CACHE_MAX_ENTRIES = 512
_WEEKLY_PATCH_DAILY_LIMIT = 10
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
    forceRefresh: bool = False


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


def _latest_row_date(rows: List[tuple] | None) -> int | None:
    latest: int | None = None
    for row in rows or []:
        if not row:
            continue
        key = normalize_date_key(row[0])
        if key is None:
            continue
        if latest is None or key > latest:
            latest = key
    return latest


def _should_apply_provisional_overlay(
    *,
    confirmed_last: int | None,
    provisional_row: tuple | None,
    asof_dt: int | None,
) -> bool:
    if provisional_row is None:
        return False
    provisional_last = normalize_date_key(provisional_row[0])
    if provisional_last is None:
        return False
    if asof_dt is not None:
        return False
    if confirmed_last is None:
        return True
    return provisional_last > confirmed_last


def _merge_confirmed_daily_rows(
    raw_rows: List[tuple],
    *,
    provisional_row: tuple | None,
    include_provisional: bool,
    asof_dt: int | None,
) -> tuple[List[tuple], bool]:
    provisional_applied = _should_apply_provisional_overlay(
        confirmed_last=_latest_row_date(raw_rows),
        provisional_row=provisional_row if include_provisional else None,
        asof_dt=asof_dt,
    )
    merged = (
        merge_daily_rows_with_provisional(
            raw_rows,
            provisional_row,
            asof_dt=asof_dt,
        )
        if provisional_applied
        else list(raw_rows)
    )
    return apply_split_gap_adjustment(merged), provisional_applied


def _build_frame_provenance(
    *,
    code: str,
    timeframe: str,
    runtime_db_path: str | None,
    rendered_rows: List[tuple],
    confirmed_rows: List[tuple],
    provisional_row: tuple | None,
    provisional_applied: bool,
    include_provisional: bool,
) -> Dict[str, Any]:
    confirmed_last = _latest_row_date(confirmed_rows)
    provisional_last = normalize_date_key(provisional_row[0]) if provisional_row else None
    requested_date = _latest_row_date(rendered_rows)
    if requested_date is None:
        requested_date = provisional_last if provisional_last is not None else confirmed_last

    confirmed_judgment_available = bool(
        requested_date is not None
        and confirmed_last is not None
        and requested_date <= confirmed_last
    )
    provisional_judgment_available = bool(provisional_applied and provisional_last is not None)

    if provisional_judgment_available and not confirmed_judgment_available:
        chart_date_match_status = "lagged_provisional"
        chart_source_freshness_status = "lagged"
    elif requested_date is None or confirmed_last is None:
        chart_date_match_status = "blocked"
        chart_source_freshness_status = "stale_blocking"
    elif confirmed_judgment_available:
        chart_date_match_status = "exact"
        chart_source_freshness_status = "exact"
    else:
        chart_date_match_status = "blocked"
        chart_source_freshness_status = "stale_blocking"

    db_prefix = str(runtime_db_path or "runtime_stock_db").strip() or "runtime_stock_db"
    yahoo_symbol = f"{code}.T"
    provisional_identifier = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}?interval=1d&range=10d"
    confirmed_chart_source_provider = "chart_gallery_confirmed_source"
    provisional_chart_source_provider = "yahoo_intraday_unconfirmed_source" if provisional_last is not None else None
    confirmed_judgment_basis = "chart_gallery_confirmed_source_only" if confirmed_judgment_available else None
    provisional_judgment_basis = (
        "yahoo_intraday_unconfirmed_source_only" if provisional_judgment_available else None
    )
    if provisional_applied and confirmed_last is not None and provisional_last is not None and provisional_last <= confirmed_last:
        overwrite_status = "provisional_replaced_by_confirmed"
    elif provisional_applied:
        overwrite_status = "provisional_only"
    elif provisional_last is not None and confirmed_last is not None and provisional_last <= confirmed_last:
        overwrite_status = "provisional_replaced_by_confirmed"
    elif confirmed_last is not None:
        overwrite_status = "authoritative_confirmed"
    elif provisional_last is not None:
        overwrite_status = "provisional_only"
    else:
        overwrite_status = None

    if provisional_applied and confirmed_last is not None:
        display_basis_classification: str | None = "mixed"
    elif provisional_applied:
        display_basis_classification = "provisional"
    elif confirmed_last is not None:
        display_basis_classification = "confirmed"
    else:
        display_basis_classification = None

    if confirmed_judgment_available and provisional_judgment_available:
        judgment_basis_classification: str | None = "dual"
    elif confirmed_judgment_available:
        judgment_basis_classification = "confirmed"
    elif provisional_judgment_available:
        judgment_basis_classification = "provisional"
    else:
        judgment_basis_classification = None

    if timeframe == "daily":
        chart_source_provider = "runtime_stock_db.daily_bars+yahoo_chart_overlay" if provisional_applied else "runtime_stock_db.daily_bars"
        chart_source_type = "mixed" if provisional_applied and confirmed_last is not None else ("provisional" if provisional_applied else "confirmed")
        chart_source_path = f"{db_prefix}#daily_bars"
        if provisional_applied:
            chart_source_path = f"{chart_source_path} + {provisional_identifier}"
        chart_aggregation_source = "mixed" if provisional_applied and confirmed_last is not None else "direct"
        chart_data_classification = "mixed" if provisional_applied and confirmed_last is not None else ("provisional" if provisional_applied else "confirmed")
    elif timeframe == "weekly":
        chart_source_provider = "derived_from_runtime_stock_db.daily_bars+yahoo_chart_overlay" if provisional_applied else "derived_from_runtime_stock_db.daily_bars"
        chart_source_type = "mixed" if provisional_applied and confirmed_last is not None else ("provisional" if provisional_applied else "confirmed")
        chart_source_path = f"derived_from({db_prefix}#daily_bars)"
        if provisional_applied:
            chart_source_path = f"{chart_source_path} + {provisional_identifier}"
        chart_aggregation_source = "derived"
        chart_data_classification = "mixed" if provisional_applied and confirmed_last is not None else ("provisional" if provisional_applied else "confirmed")
    else:
        chart_source_provider = "runtime_stock_db.monthly_bars+runtime_stock_db.daily_bars+yahoo_chart_overlay" if provisional_applied else "runtime_stock_db.monthly_bars+runtime_stock_db.daily_bars"
        chart_source_type = "mixed" if provisional_applied and confirmed_last is not None else ("provisional" if provisional_applied else "confirmed")
        chart_source_path = f"{db_prefix}#monthly_bars + {db_prefix}#daily_bars"
        if provisional_applied:
            chart_source_path = f"{chart_source_path} + {provisional_identifier}"
        chart_aggregation_source = "mixed"
        chart_data_classification = "mixed" if provisional_applied and confirmed_last is not None else ("provisional" if provisional_applied else "confirmed")

    return build_chart_data_provenance(
        chart_source_provider=chart_source_provider,
        chart_source_type=chart_source_type,  # type: ignore[arg-type]
        chart_source_path_or_identifier=chart_source_path,
        chart_requested_date=requested_date,
        chart_last_confirmed_date=confirmed_last,
        chart_last_provisional_date=provisional_last if include_provisional else None,
        chart_date_match_status=chart_date_match_status,  # type: ignore[arg-type]
        chart_source_freshness_status=chart_source_freshness_status,  # type: ignore[arg-type]
        chart_data_classification=chart_data_classification,  # type: ignore[arg-type]
        chart_aggregation_source=chart_aggregation_source,  # type: ignore[arg-type]
        confirmed_chart_source_provider=confirmed_chart_source_provider,
        provisional_chart_source_provider=provisional_chart_source_provider,
        confirmed_judgment_basis=confirmed_judgment_basis,
        provisional_judgment_basis=provisional_judgment_basis,
        confirmed_judgment_available=confirmed_judgment_available,
        provisional_judgment_available=provisional_judgment_available,
        display_basis_classification=display_basis_classification,
        judgment_basis_classification=judgment_basis_classification,
        confirmed_last_available_date=confirmed_last,
        provisional_last_available_date=provisional_last if include_provisional else None,
        overwrite_status=overwrite_status,
    )


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
    runtime_db_path: str | None,
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
        runtime_db_path,
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
    force_refresh: bool = False,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    items: Dict[str, Dict[str, Dict[str, Any]]] = {code: {} for code in codes}
    if not codes:
        return items

    frame_limits = {
        frame: int(timeframe_limits.get(frame, limit)) if timeframe_limits else int(limit)
        for frame in _SUPPORTED_TIMEFRAMES
    }

    provisional_map: Dict[str, tuple] = {}
    if include_provisional and asof_dt is None and (
        "daily" in requested_frames or "weekly" in requested_frames or "monthly" in requested_frames
    ):
        try:
            provisional_map_raw = get_provisional_daily_rows_from_spark(
                codes,
                prefer_chart_ohlc=True,
                force_refresh=force_refresh,
            )
            today_key_jst = int((datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y%m%d"))
            provisional_map = {
                code: row
                for code, row in provisional_map_raw.items()
                if row and normalize_date_key(row[0]) == today_key_jst
            }
        except Exception as exc:
            logger.debug("Yahoo provisional fetch skipped in batch bars: %s", exc)

    raw_daily_rows_by_code: Dict[str, List[tuple]] | None = None
    if "daily" in requested_frames:
        raw_daily = repo.get_daily_bars_batch(codes, frame_limits["daily"], asof_dt=asof_dt)
        raw_daily_rows_by_code = {}
        for code in codes:
            raw_rows = raw_daily.get(code, [])
            provisional_row = provisional_map.get(code) if include_provisional else None
            raw_daily_rows_by_code[code] = raw_rows
            merged, provisional_applied = _merge_confirmed_daily_rows(
                raw_rows,
                provisional_row=provisional_row,
                include_provisional=include_provisional,
                asof_dt=asof_dt,
            )
            daily_rows = merged[-frame_limits["daily"] :] if frame_limits["daily"] > 0 else merged
            payload = _to_payload_rows(daily_rows, boxes_enabled=False)
            payload["provenance"] = _build_frame_provenance(
                code=code,
                timeframe="daily",
                runtime_db_path=str(config.DB_PATH) if getattr(config, "DB_PATH", None) else None,
                rendered_rows=daily_rows,
                confirmed_rows=raw_rows,
                provisional_row=provisional_row,
                provisional_applied=provisional_applied,
                include_provisional=include_provisional,
            )
            items[code]["daily"] = payload

    if "weekly" in requested_frames:
        weekly_rows_by_code = repo.get_weekly_bars_batch(
            codes,
            frame_limits["weekly"],
            asof_dt=asof_dt,
        )
        weekly_confirmed_daily_rows_by_code: Dict[str, List[tuple]] | None = None
        weekly_patch_daily_rows_by_code: Dict[str, List[tuple]] | None = None
        if include_provisional and asof_dt is None:
            if raw_daily_rows_by_code is not None:
                weekly_confirmed_daily_rows_by_code = {
                    code: list((raw_daily_rows_by_code.get(code, []) or [])[-_WEEKLY_PATCH_DAILY_LIMIT :])
                    for code in codes
                }
            else:
                weekly_confirmed_daily_rows_by_code = repo.get_daily_bars_batch(
                    codes,
                    _WEEKLY_PATCH_DAILY_LIMIT,
                    asof_dt=asof_dt,
                )
            weekly_patch_daily_rows_by_code = {}
        for code in codes:
            confirmed_weekly_rows = weekly_rows_by_code.get(code, [])
            weekly_rows = list(confirmed_weekly_rows)
            provisional_row = provisional_map.get(code) if include_provisional else None
            confirmed_rows_for_provenance: List[tuple] = confirmed_weekly_rows
            provisional_applied = False
            if weekly_confirmed_daily_rows_by_code is not None and weekly_patch_daily_rows_by_code is not None:
                confirmed_daily_rows = weekly_confirmed_daily_rows_by_code.get(code, [])
                patched_daily_rows, provisional_applied = _merge_confirmed_daily_rows(
                    confirmed_daily_rows,
                    provisional_row=provisional_row,
                    include_provisional=include_provisional,
                    asof_dt=asof_dt,
                )
                weekly_patch_daily_rows_by_code[code] = patched_daily_rows
                weekly_rows = merge_weekly_rows_with_daily(confirmed_weekly_rows, patched_daily_rows)
                confirmed_rows_for_provenance = confirmed_daily_rows
            payload = _to_payload_rows(weekly_rows, boxes_enabled=False)
            payload["provenance"] = _build_frame_provenance(
                code=code,
                timeframe="weekly",
                runtime_db_path=str(config.DB_PATH) if getattr(config, "DB_PATH", None) else None,
                rendered_rows=weekly_rows,
                confirmed_rows=confirmed_rows_for_provenance,
                provisional_row=provisional_row,
                provisional_applied=provisional_applied,
                include_provisional=include_provisional,
            )
            items[code]["weekly"] = payload

    if "monthly" in requested_frames:
        monthly_rows_by_code = repo.get_monthly_bars_batch(
            codes,
            frame_limits["monthly"],
            asof_dt=asof_dt,
            recent_daily_rows_by_code=raw_daily_rows_by_code,
        )
        for code in codes:
            monthly_rows = monthly_rows_by_code.get(code, [])
            confirmed_rows = raw_daily_rows_by_code.get(code, []) if raw_daily_rows_by_code else monthly_rows
            payload = _to_payload_rows(
                monthly_rows,
                boxes_enabled=include_boxes,
            )
            payload["provenance"] = _build_frame_provenance(
                code=code,
                timeframe="monthly",
                runtime_db_path=str(config.DB_PATH) if getattr(config, "DB_PATH", None) else None,
                rendered_rows=monthly_rows,
                confirmed_rows=confirmed_rows,
                provisional_row=provisional_map.get(code) if include_provisional else None,
                provisional_applied=_should_apply_provisional_overlay(
                    confirmed_last=_latest_row_date(confirmed_rows),
                    provisional_row=provisional_map.get(code) if include_provisional else None,
                    asof_dt=asof_dt,
                )
                if include_provisional
                else False,
                include_provisional=include_provisional,
            )
            items[code]["monthly"] = payload

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
    force_refresh = bool(payload.forceRefresh) and bool(payload.includeProvisional) and asof_dt is None
    meta = _build_batch_meta(
        include_provisional=bool(payload.includeProvisional),
        asof_dt=asof_dt,
    )
    meta["force_refresh"] = "true" if force_refresh else "false"
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
        runtime_db_path=str(getattr(config, "DB_PATH", None)) if getattr(config, "DB_PATH", None) else None,
        data_version=meta.get("data_version"),
    )
    cached_items = None if force_refresh else _get_cached_batch_v3_items(cache_key)
    if cached_items is not None:
        return {"items": cached_items, "meta": meta}

    inflight_event: Event | None = None
    is_owner = True
    if not force_refresh:
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
            force_refresh=force_refresh,
        )
        if not force_refresh:
            _store_cached_batch_v3_items(cache_key, items)
    finally:
        if not force_refresh and is_owner:
            _finish_batch_v3_inflight(cache_key)
    return {"items": items, "meta": meta}
