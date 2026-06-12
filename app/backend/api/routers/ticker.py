from __future__ import annotations

import json
import math
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Any
from threading import Lock

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool

from app.backend.api.dependencies import get_stock_repo
from app.backend.core.edinet_auto_start_job import get_active_edinet_bootstrap_state
from app.backend.edinetdb.repository import EdinetdbRepository
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.domain.screening import ranking
from app.backend.tdnetdb.repository import TdnetdbRepository
from app.backend.services import rankings_cache
from app.backend.services.data.bar_aggregation import merge_monthly_rows_with_daily
from app.backend.services.ml.edinet_rank_features import load_edinet_rank_features
from app.backend.services.data.taisyaku_import import load_taisyaku_snapshot
from app.backend.services.jpx_calendar import get_jpx_session_info, should_pan_be_finalized_for_date
from app.backend.services.analysis.analysis_decision import (
    DETAIL_ANALYSIS_DISPLAY_LABEL,
    DETAIL_ANALYSIS_LOGIC_FAMILY,
    DETAIL_ANALYSIS_SOURCE,
    TRADEX_ANALYSIS_DISPLAY_LABEL,
    TRADEX_ANALYSIS_SOURCE,
    build_analysis_decision,
)
from app.backend.services import swing_expectancy_service, swing_plan_service
from app.backend.services.tradex_analysis_service import (
    build_tradex_detail_analysis_snapshot,
    is_tradex_detail_analysis_enabled,
)
from app.backend.services.tradex_list_summary_service import (
    build_tradex_list_summary_snapshot,
    is_tradex_list_summary_enabled,
)
from app.backend.services.data.yahoo_provisional import (
    apply_split_gap_adjustment,
    get_provisional_daily_row_from_chart,
    merge_daily_rows_with_provisional,
    normalize_date_key,
)
from app.backend.services.chart_shape_service import (
    classify_daily_chart_shape,
    classify_daily_chart_shapes_by_window,
    get_chart_shape_pattern_catalog,
)
from app.db.session import get_conn
from app.core.config import config as app_config
from app.services.box_detector import detect_boxes


router = APIRouter(prefix="/api/ticker", tags=["ticker"])
logger = logging.getLogger(__name__)
_VALID_RISK_MODES = {"defensive", "balanced", "aggressive"}
_EDINET_SUMMARY_CACHE: dict[tuple[str, int | None], tuple[float, Dict[str, Any] | None]] = {}
_EDINET_SUMMARY_CACHE_LOCK = Lock()
_EDINET_FINANCIALS_CACHE: dict[str, tuple[float, Dict[str, Any] | None]] = {}
_EDINET_FINANCIALS_CACHE_LOCK = Lock()
_ANALYSIS_SERIES_CACHE: dict[tuple[Any, ...], tuple[float, tuple[List[tuple], List[tuple]]]] = {}
_ANALYSIS_SERIES_CACHE_LOCK = Lock()
_TIMELINE_RANKING_CACHE: dict[tuple[Any, ...], tuple[float, float | None]] = {}
_TIMELINE_RANKING_CACHE_LOCK = Lock()
_DETAIL_REQUEST_STATS_LOCK = Lock()
_DETAIL_REQUEST_STATS: dict[str, Any] = {
    "request_count": 0,
    "cancelled_count": 0,
    "exception_count": 0,
    "inflight_request_count": 0,
    "max_inflight_request_count": 0,
    "same_code_repeated_request_count": 0,
    "near_code_repeated_request_count": 0,
    "disconnect_query_count": 0,
    "disconnect_transform_count": 0,
    "rss_bytes": None,
    "rss_peak_bytes": None,
    "recent_requests": [],
}
_SWING_EXPECTANCY_REFRESH_LOCK = Lock()
_LAST_SWING_EXPECTANCY_REFRESH_TS = 0.0
try:
    _DETAIL_CACHE_TTL_SEC = max(5.0, float(os.getenv("MEEMEE_DETAIL_CACHE_TTL_SEC", "20")))
except (TypeError, ValueError):
    _DETAIL_CACHE_TTL_SEC = 20.0
try:
    _SWING_EXPECTANCY_REFRESH_TTL_SEC = max(
        30.0,
        float(os.getenv("MEEMEE_SWING_EXPECTANCY_REFRESH_TTL_SEC", "300")),
    )
except (TypeError, ValueError):
    _SWING_EXPECTANCY_REFRESH_TTL_SEC = 300.0
try:
    import psutil as _psutil
except Exception:  # pragma: no cover - optional dependency
    _psutil = None
_PROCESS = _psutil.Process(os.getpid()) if _psutil is not None else None
_TDNET_REPO: TdnetdbRepository | None = None
try:
    _EDINET_SUMMARY_CACHE_TTL_SEC = max(
        30.0,
        float(os.getenv("MEEMEE_EDINET_SUMMARY_CACHE_TTL_SEC", "300")),
    )
except (TypeError, ValueError):
    _EDINET_SUMMARY_CACHE_TTL_SEC = 300.0


def _get_tdnet_repo() -> TdnetdbRepository:
    global _TDNET_REPO
    if _TDNET_REPO is None:
        _TDNET_REPO = TdnetdbRepository(app_config.DB_PATH)
    return _TDNET_REPO


def _is_tdnet_fetch_configured() -> bool:
    return bool(str(os.getenv("TDNET_MCP_FETCH_COMMAND") or "").strip())


def _tdnet_isoformat(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _build_tdnet_meta(
    state: dict[str, Any],
    *,
    source_configured: bool,
    query_failed: bool = False,
    status_detail: str | None = None,
) -> dict[str, Any]:
    missing_tables = [str(item) for item in state.get("missing_tables") or [] if str(item).strip()]
    total_count = _to_int_or_none(state.get("total_count")) or 0
    matched_count = _to_int_or_none(state.get("matched_count")) or 0
    if query_failed:
        status = "error"
        detail = status_detail or "query_failed"
    elif missing_tables:
        status = "missing_tables"
        detail = "tdnet_tables_missing"
    elif total_count <= 0 and not source_configured:
        status = "unconfigured"
        detail = "TDNET_MCP_FETCH_COMMAND is not set"
    elif total_count <= 0:
        status = "empty"
        detail = "no_tdnet_rows"
    elif matched_count <= 0:
        status = "no_symbol_rows"
        detail = "code_has_no_rows"
    else:
        status = "ok"
        detail = None
    return {
        "status": status,
        "statusDetail": detail,
        "sourceConfigured": bool(source_configured),
        "missingTables": missing_tables,
        "totalCount": total_count,
        "matchedCount": matched_count,
        "latestPublishedAt": _tdnet_isoformat(state.get("latest_published_at")),
        "latestFetchedAt": _tdnet_isoformat(state.get("latest_fetched_at")),
    }


def _sample_rss_bytes() -> int | None:
    if _PROCESS is None:
        return None
    try:
        return int(_PROCESS.memory_info().rss)
    except Exception:
        return None


def inspect_detail_request_stats(*, reset: bool = False) -> dict[str, Any]:
    with _DETAIL_REQUEST_STATS_LOCK:
        recent = list(_DETAIL_REQUEST_STATS.get("recent_requests") or [])
        payload = {
            key: value
            for key, value in _DETAIL_REQUEST_STATS.items()
            if key != "recent_requests"
        }
        payload["recent_requests"] = recent
        if reset:
            _DETAIL_REQUEST_STATS.update(
                {
                    "request_count": 0,
                    "cancelled_count": 0,
                    "exception_count": 0,
                    "inflight_request_count": 0,
                    "max_inflight_request_count": 0,
                    "same_code_repeated_request_count": 0,
                    "near_code_repeated_request_count": 0,
                    "disconnect_query_count": 0,
                    "disconnect_transform_count": 0,
                    "rss_bytes": None,
                    "rss_peak_bytes": None,
                    "recent_requests": [],
                }
            )
    return payload


def _begin_detail_request(endpoint: str, code: str) -> dict[str, Any]:
    now = time.time()
    rss_bytes = _sample_rss_bytes()
    token = {
        "endpoint": endpoint,
        "code": str(code or "").strip(),
        "query_steps": 0,
        "transform_steps": 0,
    }
    with _DETAIL_REQUEST_STATS_LOCK:
        recent = [
            item
            for item in list(_DETAIL_REQUEST_STATS.get("recent_requests") or [])
            if now - float(item.get("ts") or 0.0) <= 30.0
        ]
        code_text = token["code"]
        for item in recent:
            if item.get("endpoint") != endpoint:
                continue
            prev_code = str(item.get("code") or "")
            if prev_code == code_text:
                _DETAIL_REQUEST_STATS["same_code_repeated_request_count"] += 1
            elif prev_code.isdigit() and code_text.isdigit() and abs(int(prev_code) - int(code_text)) <= 5:
                _DETAIL_REQUEST_STATS["near_code_repeated_request_count"] += 1
        recent.append({"ts": now, "endpoint": endpoint, "code": code_text})
        _DETAIL_REQUEST_STATS["recent_requests"] = recent[-200:]
        _DETAIL_REQUEST_STATS["request_count"] += 1
        _DETAIL_REQUEST_STATS["inflight_request_count"] += 1
        _DETAIL_REQUEST_STATS["max_inflight_request_count"] = max(
            int(_DETAIL_REQUEST_STATS["max_inflight_request_count"]),
            int(_DETAIL_REQUEST_STATS["inflight_request_count"]),
        )
        _DETAIL_REQUEST_STATS["rss_bytes"] = rss_bytes
        if rss_bytes is not None:
            peak = _DETAIL_REQUEST_STATS.get("rss_peak_bytes")
            _DETAIL_REQUEST_STATS["rss_peak_bytes"] = max(int(peak or 0), int(rss_bytes))
    return token


def _finish_detail_request(token: dict[str, Any], exc: Exception | None = None) -> None:
    rss_bytes = _sample_rss_bytes()
    with _DETAIL_REQUEST_STATS_LOCK:
        _DETAIL_REQUEST_STATS["inflight_request_count"] = max(
            0,
            int(_DETAIL_REQUEST_STATS["inflight_request_count"]) - 1,
        )
        _DETAIL_REQUEST_STATS["rss_bytes"] = rss_bytes
        if rss_bytes is not None:
            peak = _DETAIL_REQUEST_STATS.get("rss_peak_bytes")
            _DETAIL_REQUEST_STATS["rss_peak_bytes"] = max(int(peak or 0), int(rss_bytes))
        if exc is not None and not (isinstance(exc, HTTPException) and int(exc.status_code) == 499):
            _DETAIL_REQUEST_STATS["exception_count"] += 1


def _record_detail_query_step(token: dict[str, Any], count: int = 1) -> None:
    token["query_steps"] = int(token.get("query_steps") or 0) + int(count)


def _record_detail_transform_step(token: dict[str, Any], count: int = 1) -> None:
    token["transform_steps"] = int(token.get("transform_steps") or 0) + int(count)


async def _raise_if_client_disconnected(
    request: Request | None,
    token: dict[str, Any],
    *,
    phase: str,
) -> None:
    if request is None:
        return
    try:
        disconnected = await request.is_disconnected()
    except Exception:
        return
    if not disconnected:
        return
    with _DETAIL_REQUEST_STATS_LOCK:
        _DETAIL_REQUEST_STATS["cancelled_count"] += 1
        if phase == "query":
            _DETAIL_REQUEST_STATS["disconnect_query_count"] += int(token.get("query_steps") or 0)
        else:
            _DETAIL_REQUEST_STATS["disconnect_transform_count"] += int(token.get("transform_steps") or 0)
    raise HTTPException(status_code=499, detail="client_closed")


def _runtime_db_cache_marker() -> tuple[str | None, float | None]:
    db_path = getattr(app_config, "DB_PATH", None)
    if not db_path:
        return None, None
    try:
        resolved = str(Path(str(db_path)).expanduser().resolve(strict=False))
    except Exception:
        resolved = str(db_path)
    try:
        mtime = Path(resolved).stat().st_mtime
    except OSError:
        mtime = None
    return resolved, mtime


def _get_cached_analysis_series(
    code: str,
    *,
    asof_dt: int | None,
    daily_limit: int,
    monthly_limit: int,
) -> tuple[List[tuple], List[tuple]] | None:
    key = (str(code), asof_dt, int(daily_limit), int(monthly_limit), _runtime_db_cache_marker())
    now = time.monotonic()
    with _ANALYSIS_SERIES_CACHE_LOCK:
        cached = _ANALYSIS_SERIES_CACHE.get(key)
        if not cached or now - float(cached[0]) > _DETAIL_CACHE_TTL_SEC:
            if cached:
                _ANALYSIS_SERIES_CACHE.pop(key, None)
            return None
        return cached[1]


def _put_cached_analysis_series(
    code: str,
    *,
    asof_dt: int | None,
    daily_limit: int,
    monthly_limit: int,
    daily_rows: List[tuple],
    monthly_rows: List[tuple],
) -> None:
    key = (str(code), asof_dt, int(daily_limit), int(monthly_limit), _runtime_db_cache_marker())
    with _ANALYSIS_SERIES_CACHE_LOCK:
        _ANALYSIS_SERIES_CACHE[key] = (
            time.monotonic(),
            (list(daily_rows), list(monthly_rows)),
        )
        if len(_ANALYSIS_SERIES_CACHE) > 256:
            oldest_key = min(_ANALYSIS_SERIES_CACHE, key=lambda item: _ANALYSIS_SERIES_CACHE[item][0])
            _ANALYSIS_SERIES_CACHE.pop(oldest_key, None)


def _load_analysis_series(
    repo: StockRepository,
    code: str,
    *,
    asof_dt: int | None,
    daily_limit: int,
    monthly_limit: int,
) -> tuple[List[tuple], List[tuple]]:
    cached = _get_cached_analysis_series(
        code,
        asof_dt=asof_dt,
        daily_limit=daily_limit,
        monthly_limit=monthly_limit,
    )
    if cached is not None:
        return cached
    daily_rows = repo.get_daily_bars(code, limit=daily_limit, asof_dt=asof_dt)
    monthly_rows = repo.get_monthly_bars(
        code,
        limit=monthly_limit,
        asof_dt=asof_dt,
        recent_daily_rows=daily_rows,
    )
    _put_cached_analysis_series(
        code,
        asof_dt=asof_dt,
        daily_limit=daily_limit,
        monthly_limit=monthly_limit,
        daily_rows=daily_rows,
        monthly_rows=monthly_rows,
    )
    return daily_rows, monthly_rows


def _get_cached_timeline_ranking_score(code: str, asof_dt: int | None) -> float | None | object:
    key = (str(code), asof_dt, _runtime_db_cache_marker())
    now = time.monotonic()
    with _TIMELINE_RANKING_CACHE_LOCK:
        cached = _TIMELINE_RANKING_CACHE.get(key)
        if not cached or now - float(cached[0]) > _DETAIL_CACHE_TTL_SEC:
            if cached:
                _TIMELINE_RANKING_CACHE.pop(key, None)
            return Ellipsis
        return cached[1]


def _put_cached_timeline_ranking_score(code: str, asof_dt: int | None, score: float | None) -> None:
    key = (str(code), asof_dt, _runtime_db_cache_marker())
    with _TIMELINE_RANKING_CACHE_LOCK:
        _TIMELINE_RANKING_CACHE[key] = (time.monotonic(), score)
        if len(_TIMELINE_RANKING_CACHE) > 256:
            oldest_key = min(_TIMELINE_RANKING_CACHE, key=lambda item: _TIMELINE_RANKING_CACHE[item][0])
            _TIMELINE_RANKING_CACHE.pop(oldest_key, None)


def _get_timeline_ranking_score(repo: StockRepository, code: str, asof_dt: int | None) -> float | None:
    cached = _get_cached_timeline_ranking_score(code, asof_dt)
    if cached is not Ellipsis:
        return cached
    daily_rows = repo.get_daily_bars(code, limit=500, asof_dt=asof_dt)
    if not daily_rows:
        _put_cached_timeline_ranking_score(code, asof_dt, None)
        return None
    daily_rows_asc = list(reversed(daily_rows))
    config = {
        "common": {"min_daily_bars": 80},
        "weekly": {
            "weights": {"ma_alignment": 10},
            "thresholds": {"volume_ratio": 1.5},
        },
    }
    up, _, _ = ranking.score_weekly_candidate(code, "", daily_rows_asc, config, None)
    latest_score = _to_float_or_none((up or {}).get("total_score")) if up else None
    _put_cached_timeline_ranking_score(code, asof_dt, latest_score)
    return latest_score


def _ensure_latest_swing_setup_stats_once() -> None:
    global _LAST_SWING_EXPECTANCY_REFRESH_TS
    now = time.monotonic()
    with _SWING_EXPECTANCY_REFRESH_LOCK:
        if now - _LAST_SWING_EXPECTANCY_REFRESH_TS < _SWING_EXPECTANCY_REFRESH_TTL_SEC:
            return
        _LAST_SWING_EXPECTANCY_REFRESH_TS = now
    swing_expectancy_service.ensure_latest_swing_setup_stats()


def _normalize_rows(rows: Iterable[Sequence], *, fill_volume: bool) -> List[List[float]]:
    normalized: List[List[float]] = []
    for row in rows:
        if len(row) < 5:
            continue
        time_value, open_, high, low, close = row[:5]
        if time_value is None or open_ is None or high is None or low is None or close is None:
            continue
        volume = 0.0
        if len(row) >= 6 and row[5] is not None and fill_volume:
            try:
                volume = float(row[5])
            except (TypeError, ValueError):
                volume = 0.0
        normalized.append(
            [
                float(time_value),
                float(open_),
                float(high),
                float(low),
                float(close),
                volume,
            ]
        )
    return normalized


def _today_jst_key() -> int:
    return int((datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y%m%d"))


def _date_key_sql_expr(column: str) -> str:
    return (
        f"CASE "
        f"WHEN {column} >= 1000000000000 THEN CAST(strftime(to_timestamp({column} / 1000.0), '%Y%m%d') AS BIGINT) "
        f"WHEN {column} BETWEEN 19000101 AND 29991231 THEN CAST({column} AS BIGINT) "
        f"WHEN {column} > 0 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS BIGINT) "
        f"ELSE CAST({column} AS BIGINT) END"
    )


def _format_date_key(date_key: int | None) -> str | None:
    if date_key is None:
        return None
    text = str(int(date_key))
    if len(text) != 8 or not text.isdigit():
        return None
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _format_jst_timestamp(ts: int | float | None) -> str | None:
    if ts is None:
        return None
    try:
        jst = timezone(timedelta(hours=9))
        return datetime.fromtimestamp(float(ts), tz=jst).strftime("%Y-%m-%d %H:%M JST")
    except Exception:
        return None


def _latest_row_date(rows: list[tuple] | None) -> int | None:
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


def _build_market_data_status_message(
    *,
    has_provisional: bool,
    pan_delayed: bool,
    delayed_pending_date: int | None,
    pending_yahoo_date: int | None,
    session: Any,
    provisional_fetched_at_text: str | None = None,
) -> str | None:
    if not has_provisional:
        return None
    delayed_date_text = _format_date_key(delayed_pending_date)
    pending_date_text = _format_date_key(pending_yahoo_date)
    fetched_suffix = f"（最終取得 {provisional_fetched_at_text}）" if provisional_fetched_at_text else ""
    if pan_delayed and delayed_date_text:
        return f"PAN取込遅延中: {delayed_date_text} は Yahoo 仮データ{fetched_suffix}を表示しています。"
    suffix = "（半日取引）" if session.day_type == "half_day" else ""
    pending_suffix = f" [{pending_date_text}]" if pending_date_text else ""
    return (
        f"Yahoo 仮データ{fetched_suffix}を表示しています{pending_suffix}{suffix}。"
        " PAN取込完了後に正データへ切り替わります。"
    )


def _should_apply_provisional_overlay(
    *,
    confirmed_last: int | None,
    provisional_key: int | None,
    asof_dt: int | None,
) -> bool:
    if provisional_key is None:
        return False
    if asof_dt is not None:
        return False
    if confirmed_last is None:
        return True
    return provisional_key > confirmed_last


def _load_market_data_meta(
    code: str,
    *,
    requested_date: int | None,
    intraday_provisional_key: int | None,
    provisional_fetched_at_ts: int | None,
    asof_dt: int | None,
    provisional_applied: bool,
) -> dict[str, Any] | None:
    if asof_dt is not None or not code:
        return None

    date_key_expr = _date_key_sql_expr("date")
    with get_conn() as conn:
        row = conn.execute(
            f"""
            SELECT
                MAX(CASE WHEN COALESCE(source, 'pan') <> 'yahoo' THEN {date_key_expr} END) AS latest_pan_date,
                MAX(CASE WHEN COALESCE(source, 'pan') = 'yahoo' THEN {date_key_expr} END) AS latest_yahoo_date
            FROM daily_bars
            WHERE code = ?
            """,
            [code],
        ).fetchone()
        pending_rows = conn.execute(
            f"""
            SELECT DISTINCT {date_key_expr} AS yahoo_date
            FROM daily_bars
            WHERE code = ?
              AND COALESCE(source, 'pan') = 'yahoo'
            ORDER BY yahoo_date DESC
            LIMIT 16
            """,
            [code],
        ).fetchall()

    latest_pan_date = normalize_date_key(row[0]) if row and row[0] is not None else None
    latest_yahoo_date = normalize_date_key(row[1]) if row and row[1] is not None else None
    effective_provisional_last = intraday_provisional_key if intraday_provisional_key is not None else latest_yahoo_date
    pending_yahoo_dates = [
        value
        for value in (normalize_date_key(item[0]) for item in pending_rows)
        if value is not None and (latest_pan_date is None or value > latest_pan_date)
    ]
    latest_resolved_date = max(
        [
            value
            for value in (
                latest_pan_date,
                effective_provisional_last if provisional_applied else None,
            )
            if value is not None
        ],
        default=None,
    )
    if intraday_provisional_key is not None and provisional_applied and (
        latest_pan_date is None or intraday_provisional_key > latest_pan_date
    ):
        pending_yahoo_dates.append(intraday_provisional_key)
    pending_yahoo_date = max(pending_yahoo_dates, default=None)

    session = get_jpx_session_info()
    delayed_pending_date = max(
        [value for value in pending_yahoo_dates if should_pan_be_finalized_for_date(value)],
        default=None,
    )
    pan_delayed = delayed_pending_date is not None
    has_provisional = provisional_applied and pending_yahoo_date is not None
    message = _build_market_data_status_message(
        has_provisional=has_provisional,
        pan_delayed=pan_delayed,
        delayed_pending_date=delayed_pending_date,
        pending_yahoo_date=pending_yahoo_date,
        session=session,
        provisional_fetched_at_text=_format_jst_timestamp(provisional_fetched_at_ts),
    )
    return {
        "hasProvisional": has_provisional,
        "panDelayed": pan_delayed,
        "latestPanDate": latest_pan_date,
        "latestYahooDate": latest_yahoo_date,
        "latestResolvedDate": latest_resolved_date,
        "pendingYahooDate": pending_yahoo_date,
        "delayedPendingDate": delayed_pending_date,
        "todayDayType": session.day_type,
        "todayIsTradingDay": session.is_trading_day,
        "closeTimeJst": session.close_time_jst,
        "panFinalizeAfterJst": session.pan_finalize_after_jst,
        "message": message,
        "confirmedChartSourceProvider": "chart_gallery_confirmed_source",
        "provisionalChartSourceProvider": "yahoo_intraday_unconfirmed_source" if effective_provisional_last is not None else None,
        "confirmedJudgmentBasis": "chart_gallery_confirmed_source_only"
        if latest_pan_date is not None and requested_date is not None and requested_date <= latest_pan_date
        else None,
        "provisionalJudgmentBasis": "yahoo_intraday_unconfirmed_source_only" if has_provisional else None,
        "confirmedJudgmentAvailable": bool(
            latest_pan_date is not None and requested_date is not None and requested_date <= latest_pan_date
        ),
        "provisionalJudgmentAvailable": bool(has_provisional),
        "displayBasisClassification": (
            "mixed"
            if has_provisional and latest_pan_date is not None
            else "provisional"
            if has_provisional
            else "confirmed"
            if latest_pan_date is not None
            else None
        ),
        "judgmentBasisClassification": (
            "dual"
            if latest_pan_date is not None
            and requested_date is not None
            and requested_date <= latest_pan_date
            and has_provisional
            else "confirmed"
            if latest_pan_date is not None and requested_date is not None and requested_date <= latest_pan_date
            else "provisional"
            if has_provisional
            else None
        ),
        "confirmedLastAvailableDate": latest_pan_date,
        "provisionalLastAvailableDate": effective_provisional_last,
        "overwriteStatus": (
            "provisional_replaced_by_confirmed"
            if effective_provisional_last is not None
            and latest_pan_date is not None
            and effective_provisional_last <= latest_pan_date
            else "authoritative_confirmed"
            if latest_pan_date is not None and not has_provisional
            else "provisional_only"
            if has_provisional
            else None
        ),
        "confirmed_chart_source_provider": "chart_gallery_confirmed_source",
        "provisional_chart_source_provider": "yahoo_intraday_unconfirmed_source" if effective_provisional_last is not None else None,
        "confirmed_judgment_basis": "chart_gallery_confirmed_source_only"
        if latest_pan_date is not None and requested_date is not None and requested_date <= latest_pan_date
        else None,
        "provisional_judgment_basis": "yahoo_intraday_unconfirmed_source_only" if has_provisional else None,
        "confirmed_judgment_available": bool(
            latest_pan_date is not None and requested_date is not None and requested_date <= latest_pan_date
        ),
        "provisional_judgment_available": bool(has_provisional),
        "display_basis_classification": (
            "mixed"
            if has_provisional and latest_pan_date is not None
            else "provisional"
            if has_provisional
            else "confirmed"
            if latest_pan_date is not None
            else None
        ),
        "judgment_basis_classification": (
            "dual"
            if latest_pan_date is not None
            and requested_date is not None
            and requested_date <= latest_pan_date
            and has_provisional
            else "confirmed"
            if latest_pan_date is not None and requested_date is not None and requested_date <= latest_pan_date
            else "provisional"
            if has_provisional
            else None
        ),
        "confirmed_last_available_date": latest_pan_date,
        "provisional_last_available_date": effective_provisional_last,
        "overwrite_status": (
            "provisional_replaced_by_confirmed"
            if effective_provisional_last is not None
            and latest_pan_date is not None
            and effective_provisional_last <= latest_pan_date
            else "authoritative_confirmed"
            if latest_pan_date is not None and not has_provisional
            else "provisional_only"
            if has_provisional
            else None
        ),
    }
    message: str | None = None
    delayed_date_text = _format_date_key(delayed_pending_date)
    pending_date_text = _format_date_key(pending_yahoo_date)
    if has_provisional and pan_delayed and delayed_date_text:
        message = f"PAN取込遅延中: {delayed_date_text} は Yahoo 仮データを表示しています。"
    elif has_provisional:
        suffix = "（半日立会）" if session.day_type == "half_day" else ""
        message = (
            f"Yahoo 仮データを表示しています{suffix}。"
            f" PAN 取込完了後に正式データへ切り替わります。"
        )

    return {
        "hasProvisional": has_provisional,
        "panDelayed": pan_delayed,
        "latestPanDate": latest_pan_date,
        "latestYahooDate": latest_yahoo_date,
        "latestResolvedDate": latest_resolved_date,
        "pendingYahooDate": pending_yahoo_date,
        "delayedPendingDate": delayed_pending_date,
        "todayDayType": session.day_type,
        "todayIsTradingDay": session.is_trading_day,
        "closeTimeJst": session.close_time_jst,
        "panFinalizeAfterJst": session.pan_finalize_after_jst,
        "message": message,
    }


def _load_monthly_rows_with_provisional(
    repo: StockRepository,
    code: str,
    *,
    limit: int,
    asof_dt: int | None,
) -> tuple[List[tuple], int | None, int | None, bool]:
    patch_daily_rows = repo.get_daily_bars(code, 62, asof_dt)
    rows = repo.get_monthly_bars(code, limit, asof_dt, recent_daily_rows=patch_daily_rows)
    intraday_provisional_key: int | None = None
    provisional_fetched_at_ts: int | None = None
    provisional_applied = False
    if asof_dt is None:
        try:
            provisional_row = get_provisional_daily_row_from_chart(code)
            provisional_key = normalize_date_key(provisional_row[0]) if provisional_row else None
            confirmed_last = _latest_row_date(patch_daily_rows)
            provisional_applied = _should_apply_provisional_overlay(
                confirmed_last=confirmed_last,
                provisional_key=provisional_key,
                asof_dt=asof_dt,
            )
            if provisional_applied:
                patch_daily_rows = merge_daily_rows_with_provisional(patch_daily_rows, provisional_row)
                intraday_provisional_key = provisional_key
                provisional_fetched_at_ts = int(time.time())
        except Exception as exc:
            logger.debug("Yahoo provisional monthly merge skipped for code=%s: %s", code, exc)
    patch_daily_rows = apply_split_gap_adjustment(patch_daily_rows)
    rows = merge_monthly_rows_with_daily(rows, patch_daily_rows)
    rows = apply_split_gap_adjustment(rows)
    return rows, intraday_provisional_key, provisional_fetched_at_ts, provisional_applied


@router.get("/daily", response_model=None)
def get_daily_bars(
    code: str,
    limit: int = 400,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    rows = repo.get_daily_bars(code, limit, asof_dt)
    intraday_provisional_key: int | None = None
    provisional_fetched_at_ts: int | None = None
    provisional_applied = False
    try:
        provisional_row = get_provisional_daily_row_from_chart(code)
        provisional_key = normalize_date_key(provisional_row[0]) if provisional_row else None
        confirmed_last = _latest_row_date(rows)
        provisional_applied = _should_apply_provisional_overlay(
            confirmed_last=confirmed_last,
            provisional_key=provisional_key,
            asof_dt=asof_dt,
        )
        if provisional_applied:
            rows = merge_daily_rows_with_provisional(rows, provisional_row, asof_dt=asof_dt)
            intraday_provisional_key = provisional_key
            provisional_fetched_at_ts = int(time.time())
    except Exception as exc:
        logger.debug("Yahoo provisional merge skipped for code=%s: %s", code, exc)
    rows = apply_split_gap_adjustment(rows)
    requested_date = _latest_row_date(rows)
    meta = _load_market_data_meta(
        code,
        requested_date=requested_date,
        intraday_provisional_key=intraday_provisional_key,
        provisional_fetched_at_ts=provisional_fetched_at_ts,
        asof_dt=asof_dt,
        provisional_applied=provisional_applied,
    )
    return {"data": _normalize_rows(rows, fill_volume=True), "errors": [], "meta": meta}


@router.get("/daily/shape", response_model=None)
def get_daily_chart_shape(
    code: str,
    window: int = Query(10, ge=3, le=120),
    windows: str | None = Query(None),
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    requested_windows: list[int] = []
    if windows:
        for part in str(windows).split(","):
            part = part.strip()
            if not part:
                continue
            try:
                requested_windows.append(max(3, min(120, int(part))))
            except ValueError:
                continue
    if not requested_windows:
        requested_windows = [int(window)]
    max_window = max(requested_windows)
    # Pull one extra bar so gap detection can compare the first in-window bar with its previous close.
    rows = repo.get_daily_bars(code, max_window + 1, asof_dt)
    rows = apply_split_gap_adjustment(rows)
    shape = classify_daily_chart_shape(rows, requested_window=int(window))
    multi_window = classify_daily_chart_shapes_by_window(rows, requested_windows=requested_windows)
    return {
        "code": str(code),
        "timeframe": "D",
        "asof": asof,
        "shape": shape,
        "multi_window": multi_window,
        "item": {"shape": shape, "multi_window": multi_window},
    }


@router.get("/daily/shape/patterns", response_model=None)
def get_daily_chart_shape_patterns() -> Dict[str, Any]:
    return {
        "timeframe": "D",
        "patterns": get_chart_shape_pattern_catalog(),
        "contract": {
            "scope": "display_confirmation_only",
            "ranking_changed": False,
            "tradex_research_changed": False,
            "expectancy_validated": False,
        },
    }


@router.get("/monthly", response_model=None)
async def get_monthly_bars(
    code: str,
    limit: int = 120,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
    *,
    request: Request,
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    token = _begin_detail_request("ticker.monthly", code)
    asof_dt = _parse_dt(asof)
    error: Exception | None = None
    try:
        rows, intraday_provisional_key, provisional_fetched_at_ts, provisional_applied = await run_in_threadpool(
            _load_monthly_rows_with_provisional,
            repo,
            code,
            limit=limit,
            asof_dt=asof_dt,
        )
        _record_detail_query_step(token, 2)
        await _raise_if_client_disconnected(request, token, phase="query")
        requested_date = _latest_row_date(rows)
        meta = await run_in_threadpool(
            _load_market_data_meta,
            code,
            requested_date=requested_date,
            intraday_provisional_key=intraday_provisional_key,
            provisional_fetched_at_ts=provisional_fetched_at_ts,
            asof_dt=asof_dt,
            provisional_applied=provisional_applied,
        )
        _record_detail_query_step(token, 1)
        payload = {"data": _normalize_rows(rows, fill_volume=True), "errors": [], "meta": meta}
        _record_detail_transform_step(token, 1)
        await _raise_if_client_disconnected(request, token, phase="transform")
        return payload
    except Exception as exc:
        error = exc
        raise
    finally:
        _finish_detail_request(token, error)


@router.get("/boxes", response_model=None)
def get_boxes(
    code: str,
    limit: int = 120,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> List[Dict]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    rows, _, _ = _load_monthly_rows_with_provisional(
        repo,
        code,
        limit=limit,
        asof_dt=asof_dt,
    )
    return detect_boxes(rows, range_basis="body", max_range_pct=0.2)


def _parse_dt(value: str | int | None) -> int | None:
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


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _build_sell_context_from_row(row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "pDown": _to_float_or_none(row[3]) if len(row) > 3 else None,
        "pTurnDown": _to_float_or_none(row[4]) if len(row) > 4 else None,
        "shortScore": _to_float_or_none(row[11]) if len(row) > 11 else None,
        "aScore": _to_float_or_none(row[12]) if len(row) > 12 else None,
        "bScore": _to_float_or_none(row[13]) if len(row) > 13 else None,
        "distMa20Signed": _to_float_or_none(row[18]) if len(row) > 18 else None,
        "ma20Slope": _to_float_or_none(row[16]) if len(row) > 16 else None,
        "ma60Slope": _to_float_or_none(row[17]) if len(row) > 17 else None,
        "trendDown": bool(row[20]) if len(row) > 20 and row[20] is not None else None,
        "trendDownStrict": bool(row[21]) if len(row) > 21 and row[21] is not None else None,
    }


def _build_research_prior_summary(code: str) -> Dict[str, Any] | None:
    code_key = str(code or "").strip()
    if not code_key:
        return None
    try:
        snapshot = rankings_cache._load_research_prior_snapshot()
    except Exception:
        return None
    if not isinstance(snapshot, dict):
        return None

    run_id = str(snapshot.get("run_id") or "").strip() or None
    if run_id is None:
        return None

    summary: Dict[str, Any] = {"runId": run_id}
    for side in ("up", "down"):
        probe: Dict[str, Any] = {}
        rankings_cache._calc_research_prior_bonus(
            item=probe,
            direction=side,  # type: ignore[arg-type]
            code=code_key,
            prior_snapshot=snapshot,
        )
        summary[side] = {
            "aligned": bool(probe.get("researchPriorAligned")),
            "rank": _to_int_or_none(probe.get("researchPriorRank")),
            "universe": _to_int_or_none(probe.get("researchPriorUniverse")),
            "bonus": _to_float_or_none(probe.get("researchPriorBonus")),
            "asOf": str(probe.get("researchPriorAsOf") or "").strip() or None,
            "patternTag": str(probe.get("researchPatternTag") or "").strip() or None,
            "signalStrength": _to_float_or_none(probe.get("researchSignalStrength")),
            "promotionStage": str(probe.get("researchPromotionStage") or "").strip() or None,
            "decisionReasons": (
                [str(reason).strip() for reason in probe.get("researchDecisionReasons") if str(reason).strip()]
                if isinstance(probe.get("researchDecisionReasons"), list)
                else None
            ),
            "riskWatch": (
                [str(reason).strip() for reason in probe.get("researchRiskWatch") if str(reason).strip()]
                if isinstance(probe.get("researchRiskWatch"), list)
                else None
            ),
            "provisional": bool(probe.get("researchProvisional")) if probe.get("researchPriorAligned") else False,
            "hypothesisFamily": str(probe.get("researchHypothesisFamily") or "").strip() or None,
            "fitScore": _to_float_or_none(probe.get("reboundOnsetFitScore")),
            "adoptionReasons": (
                [str(reason).strip() for reason in probe.get("reboundOnsetAdoptionReasons") if str(reason).strip()]
                if isinstance(probe.get("reboundOnsetAdoptionReasons"), list)
                else None
            ),
        }
    return summary


def _asof_dt_to_ymd(asof_dt: int | None) -> int | None:
    if asof_dt is None:
        return None
    try:
        return int(datetime.fromtimestamp(int(asof_dt), tz=timezone.utc).strftime("%Y%m%d"))
    except Exception:
        return None


def _normalize_date_key(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        iv = int(value)
        if iv >= 1_000_000_000:
            try:
                return int(datetime.fromtimestamp(iv, tz=timezone.utc).strftime("%Y%m%d"))
            except Exception:
                return None
        if 19_000_101 <= iv <= 21_001_231:
            return iv
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return int(datetime.strptime(text, fmt).strftime("%Y%m%d"))
        except ValueError:
            continue
    return None


def _normalize_month_key(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        iv = int(value)
        if iv >= 1_000_000_000:
            try:
                return int(datetime.fromtimestamp(iv, tz=timezone.utc).strftime("%Y%m"))
            except Exception:
                return None
        if 190001 <= iv <= 210012:
            return iv
        if 19_000_101 <= iv <= 21_001_231:
            return int(iv / 100)
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y%m"):
        try:
            return int(datetime.strptime(text, fmt).strftime("%Y%m"))
        except ValueError:
            continue
    return None


def _build_exact_analysis_decision(
    *,
    analysis_point: Dict[str, Any],
    daily_rows: list[tuple],
    monthly_rows: list[tuple],
    sell_row: tuple[Any, ...] | None,
    risk_mode: str,
) -> Dict[str, Any]:
    p_up = _to_float_or_none(analysis_point.get("pUp"))
    p_down = _to_float_or_none(analysis_point.get("pDown"))
    if p_down is None and p_up is not None:
        p_down = 1.0 - p_up
    p_turn_up = _to_float_or_none(analysis_point.get("pTurnUp"))
    p_turn_down = _to_float_or_none(analysis_point.get("pTurnDown"))
    ev20_net = _to_float_or_none(analysis_point.get("ev20Net"))

    additive_signals = None
    entry_policy = None
    try:
        additive_signals = _build_additive_signal_summary(daily_rows, monthly_rows)
        entry_policy = _build_entry_policy_summary(
            daily_rows=daily_rows,
            monthly_rows=monthly_rows,
            risk_mode=risk_mode,
        )
    except Exception:
        additive_signals = None
        entry_policy = None

    sell_context = _build_sell_context_from_row(sell_row)
    return build_analysis_decision(
        analysis_p_up=p_up,
        analysis_p_down=p_down,
        analysis_p_turn_up=p_turn_up,
        analysis_p_turn_down=p_turn_down,
        analysis_ev_net=ev20_net,
        playbook_up_score_bonus=_to_float_or_none((entry_policy or {}).get("up", {}).get("playbookScoreBonus"))
        if isinstance(entry_policy, dict)
        else None,
        playbook_down_score_bonus=_to_float_or_none((entry_policy or {}).get("down", {}).get("playbookScoreBonus"))
        if isinstance(entry_policy, dict)
        else None,
        additive_signals=additive_signals if isinstance(additive_signals, dict) else None,
        sell_analysis=sell_context if isinstance(sell_context, dict) else None,
    )


def _build_cached_analysis_decision(
    *,
    analysis_point: Dict[str, Any],
) -> Dict[str, Any]:
    p_up = _to_float_or_none(analysis_point.get("pUp"))
    p_down = _to_float_or_none(analysis_point.get("pDown"))
    if p_down is None and p_up is not None:
        p_down = 1.0 - p_up
    p_turn_up = _to_float_or_none(analysis_point.get("pTurnUp"))
    p_turn_down = _to_float_or_none(analysis_point.get("pTurnDown"))
    ev20_net = _to_float_or_none(analysis_point.get("ev20Net"))
    sell_context = {
        "pDown": _to_float_or_none(analysis_point.get("sellPDown")),
        "pTurnDown": _to_float_or_none(analysis_point.get("sellPTurnDown")),
        "trendDown": analysis_point.get("trendDown"),
        "trendDownStrict": analysis_point.get("trendDownStrict"),
        "shortRet5": _to_float_or_none(analysis_point.get("shortRet5")),
        "shortRet10": _to_float_or_none(analysis_point.get("shortRet10")),
        "shortRet20": _to_float_or_none(analysis_point.get("shortRet20")),
        "shortWin5": analysis_point.get("shortWin5"),
        "shortWin10": analysis_point.get("shortWin10"),
        "shortWin20": analysis_point.get("shortWin20"),
    }
    return build_analysis_decision(
        analysis_p_up=p_up,
        analysis_p_down=p_down,
        analysis_p_turn_up=p_turn_up,
        analysis_p_turn_down=p_turn_down,
        analysis_ev_net=ev20_net,
        playbook_up_score_bonus=None,
        playbook_down_score_bonus=None,
        additive_signals=None,
        sell_analysis=sell_context,
    )


def _edinet_repo() -> EdinetdbRepository:
    return EdinetdbRepository(app_config.DB_PATH)


def _edinet_isoformat(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _resolve_edinet_runtime_status(
    *,
    base_status: str | None,
    bootstrap_state: dict[str, Any] | None,
    missing_tables: bool = False,
    empty_tables: bool = False,
) -> tuple[str, str | None]:
    bootstrap_active = bool((bootstrap_state or {}).get("active"))
    status = str(base_status or "").strip() or None
    if missing_tables:
        return "error", "missing_tables"
    if empty_tables:
        return ("loading", "bootstrap_active") if bootstrap_active else ("empty_tables", "empty_tables")
    if bootstrap_active and status == "no_payload":
        return "loading", "bootstrap_active"
    if status == "ok":
        return "ok", None
    if status == "unmapped":
        return "unmapped", "not_mapped"
    if status == "no_payload":
        return "no_payload", "no_usable_payload"
    if status == "empty_tables":
        return ("loading", "bootstrap_active") if bootstrap_active else ("empty_tables", "empty_tables")
    if status == "missing_tables":
        return "error", "missing_tables"
    if status == "loading":
        return "loading", "bootstrap_active"
    return "error", "load_failed"


def _apply_runtime_status_to_edinet_summary(
    summary: Dict[str, Any],
    *,
    bootstrap_state: dict[str, Any] | None,
) -> Dict[str, Any]:
    resolved = dict(summary)
    status, _ = _resolve_edinet_runtime_status(
        base_status=str(resolved.get("status") or "").strip() or None,
        bootstrap_state=bootstrap_state,
    )
    resolved["status"] = status
    if status in {"error", "empty_tables", "loading"} and resolved.get("mapped") is False:
        return resolved
    if str(summary.get("status") or "").strip() == "missing_tables":
        resolved["mapped"] = None
    return resolved


def _build_edinet_summary(code: str, asof_dt: int | None) -> Dict[str, Any] | None:
    code_key = str(code or "").strip()
    if not code_key:
        return None
    asof_ymd = _asof_dt_to_ymd(asof_dt)
    cache_key = (code_key, asof_ymd)
    now_ts = time.time()
    bootstrap_state = get_active_edinet_bootstrap_state()
    with _EDINET_SUMMARY_CACHE_LOCK:
        cached = _EDINET_SUMMARY_CACHE.get(cache_key)
        if cached and now_ts - cached[0] <= _EDINET_SUMMARY_CACHE_TTL_SEC:
            payload = cached[1]
            return (
                _apply_runtime_status_to_edinet_summary(dict(payload), bootstrap_state=bootstrap_state)
                if isinstance(payload, dict)
                else None
            )

    try:
        with get_conn() as conn:
            feature_map = load_edinet_rank_features(conn, [code_key], asof_ymd)
    except Exception:
        return {
            "status": "loading" if bool((bootstrap_state or {}).get("active")) else "error",
            "mapped": None,
            "freshnessDays": None,
            "metricCount": None,
            "qualityScore": None,
            "dataScore": None,
            "scoreBonus": None,
            "featureFlagApplied": None,
            "ebitdaMetric": None,
            "roe": None,
            "equityRatio": None,
            "debtRatio": None,
            "operatingCfMargin": None,
            "revenueGrowthYoy": None,
        }
    if not isinstance(feature_map, dict):
        return {
            "status": "error",
            "mapped": None,
            "freshnessDays": None,
            "metricCount": None,
            "qualityScore": None,
            "dataScore": None,
            "scoreBonus": None,
            "featureFlagApplied": None,
            "ebitdaMetric": None,
            "roe": None,
            "equityRatio": None,
            "debtRatio": None,
            "operatingCfMargin": None,
            "revenueGrowthYoy": None,
        }
    feature = feature_map.get(code_key)
    if not isinstance(feature, dict):
        return {
            "status": "error",
            "mapped": None,
            "freshnessDays": None,
            "metricCount": None,
            "qualityScore": None,
            "dataScore": None,
            "scoreBonus": None,
            "featureFlagApplied": None,
            "ebitdaMetric": None,
            "roe": None,
            "equityRatio": None,
            "debtRatio": None,
            "operatingCfMargin": None,
            "revenueGrowthYoy": None,
        }

    metric_count = _to_int_or_none(feature.get("edinetMetricCount"))
    data_score = _to_float_or_none(feature.get("edinetDataScore"))
    coverage = float(max(0.0, min(1.0, float(metric_count or 0) / 3.0)))
    feature_flag_applied = bool(rankings_cache._is_edinet_bonus_enabled())
    bonus_core = (
        float((float(data_score) - 0.5) * rankings_cache._EDINET_SCORE_BONUS_SCALE * coverage)
        if data_score is not None and coverage > 0
        else 0.0
    )
    score_bonus = bonus_core if feature_flag_applied else 0.0
    summary: Dict[str, Any] = {
        "status": str(feature.get("edinetStatus") or "").strip() or None,
        "mapped": bool(feature.get("edinetMapped")) if feature.get("edinetMapped") is not None else None,
        "freshnessDays": _to_int_or_none(feature.get("edinetFreshnessDays")),
        "metricCount": metric_count,
        "qualityScore": _to_float_or_none(feature.get("edinetQualityScore")),
        "dataScore": data_score,
        "scoreBonus": score_bonus,
        "featureFlagApplied": feature_flag_applied,
        "ebitdaMetric": _to_float_or_none(feature.get("edinetEbitdaMetric")),
        "roe": _to_float_or_none(feature.get("edinetRoe")),
        "equityRatio": _to_float_or_none(feature.get("edinetEquityRatio")),
        "debtRatio": _to_float_or_none(feature.get("edinetDebtRatio")),
        "operatingCfMargin": _to_float_or_none(feature.get("edinetOperatingCfMargin")),
        "revenueGrowthYoy": _to_float_or_none(feature.get("edinetRevenueGrowthYoy")),
    }
    with _EDINET_SUMMARY_CACHE_LOCK:
        _EDINET_SUMMARY_CACHE[cache_key] = (now_ts, dict(summary))
        if len(_EDINET_SUMMARY_CACHE) > 2048:
            oldest_key = min(_EDINET_SUMMARY_CACHE, key=lambda key: _EDINET_SUMMARY_CACHE[key][0])
            _EDINET_SUMMARY_CACHE.pop(oldest_key, None)
    return _apply_runtime_status_to_edinet_summary(summary, bootstrap_state=bootstrap_state)


_EDINET_ALIAS_SPLIT_RE = re.compile(r"[\s_\-./()%\[\]{}:%・,+]")
_EDINET_ALIAS_REVENUE = (
    "revenue",
    "sales",
    "netsales",
    "netsales",
    "売上高",
    "売上収益",
    "営業収益",
)
_EDINET_ALIAS_GROSS_PROFIT = (
    "grossprofit",
    "売上総利益",
    "売総益",
)
_EDINET_ALIAS_OPERATING_INCOME = (
    "operatingincome",
    "operatingprofit",
    "営業利益",
    "事業利益",
)
_EDINET_ALIAS_NET_INCOME = (
    "netincome",
    "profitattributabletoownersofparent",
    "profitattributabletoownersofparent",
    "当期純利益",
    "純利益",
    "親会社株主に帰属する当期純利益",
)
_EDINET_ALIAS_EPS = (
    "eps",
    "earningspershare",
    "basiceps",
    "1株当たり当期純利益",
    "1株当たり純利益",
)
_EDINET_ALIAS_BPS = (
    "bps",
    "bookvaluepershare",
    "netassetvaluepershare",
    "1株当たり純資産",
    "1株純資産",
)
_EDINET_ALIAS_DIVIDEND = (
    "dividendpershare",
    "annualdividendpershare",
    "cashdividendpershare",
    "1株当たり配当",
    "年間配当金",
)
_EDINET_ALIAS_EQUITY_RATIO_DETAIL = (
    "equityratio",
    "自己資本比率",
    "自己資本率",
)
_EDINET_ALIAS_ROE_DETAIL = (
    "roe",
    "returnonequity",
    "自己資本利益率",
)
_EDINET_ALIAS_ROA_DETAIL = (
    "roa",
    "returnonassets",
    "総資産利益率",
    "総資産経常利益率",
)
_EDINET_ALIAS_NET_INTEREST_BEARING_DEBT = (
    "netinterestbearingdebt",
    "netdebt",
    "純有利子負債",
)


_EDINET_ALIAS_SPLIT_RE = re.compile(r"[\s_\-./()%\[\]{}:,+]")
_EDINET_ALIAS_REVENUE = ("revenue", "sales", "netsales", "operatingrevenue")
_EDINET_ALIAS_GROSS_PROFIT = ("grossprofit",)
_EDINET_ALIAS_OPERATING_INCOME = ("operatingincome", "operatingprofit")
_EDINET_ALIAS_NET_INCOME = ("netincome", "profitattributabletoownersofparent", "profitloss")
_EDINET_ALIAS_EPS = ("eps", "earningspershare", "basiceps")
_EDINET_ALIAS_BPS = ("bps", "bookvaluepershare", "netassetvaluepershare")
_EDINET_ALIAS_DIVIDEND = ("dividendpershare", "annualdividendpershare", "cashdividendpershare")
_EDINET_ALIAS_EQUITY_RATIO_DETAIL = ("equityratio", "equitycapitalratio")
_EDINET_ALIAS_ROE_DETAIL = ("roe", "returnonequity")
_EDINET_ALIAS_ROA_DETAIL = ("roa", "returnonassets")
_EDINET_ALIAS_NET_INTEREST_BEARING_DEBT = ("netinterestbearingdebt", "netdebt")
_EDINET_AMOUNT_DISQUALIFIERS = (
    "pershare",
    "perstock",
    "margin",
    "ratio",
    "rate",
    "growth",
    "yoy",
    "forecast",
    "estimate",
    "plan",
)

def _edinet_normalize_key(text: object) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    return _EDINET_ALIAS_SPLIT_RE.sub("", raw).lower()


def _edinet_normalize_segments(text: object) -> list[str]:
    parts = re.split(r"[.\[\]]+", str(text or ""))
    return [part for part in (_edinet_normalize_key(item) for item in parts) if part]


def _edinet_json_load(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _edinet_parse_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    text = str(value).strip()
    if not text:
        return None
    cleaned = (
        text.replace(",", "")
        .replace("△", "-")
        .replace("▲", "-")
        .replace("%", "")
        .replace("倍", "")
        .replace("円", "")
    )
    cleaned = cleaned.replace("△", "-").replace("▲", "-").replace("倍", "").replace("円", "")
    try:
        numeric = float(cleaned)
    except ValueError:
        return None
    return numeric if math.isfinite(numeric) else None


def _edinet_collect_numeric_pairs(payload: Any) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    stack: list[tuple[str, Any]] = [("", payload)]
    while stack:
        prefix, node = stack.pop()
        if isinstance(node, dict):
            for key, value in reversed(list(node.items())):
                path = f"{prefix}.{key}" if prefix else str(key)
                stack.append((path, value))
            continue
        if isinstance(node, list):
            for idx, value in reversed(list(enumerate(node))):
                path = f"{prefix}[{idx}]"
                stack.append((path, value))
            continue
        numeric = _edinet_parse_float(node)
        if numeric is None or not prefix:
            continue
        out.append((prefix, numeric))
    return out


def _edinet_find_first_metric(
    *pairs_groups: list[tuple[str, float]],
    aliases: Sequence[str],
    disqualifiers: Sequence[str] = (),
) -> float | None:
    alias_norm = [_edinet_normalize_key(alias) for alias in aliases if _edinet_normalize_key(alias)]
    if not alias_norm:
        return None
    disqualifier_norm = [
        _edinet_normalize_key(alias)
        for alias in disqualifiers
        if _edinet_normalize_key(alias)
    ]
    best_score: int | None = None
    best_value: float | None = None
    for pairs in pairs_groups:
        for path, value in pairs:
            normalized = _edinet_normalize_key(path)
            if not normalized:
                continue
            segments = _edinet_normalize_segments(path)
            terminal = segments[-1] if segments else normalized
            penalty = sum(35 for token in disqualifier_norm if token in terminal)
            for alias in alias_norm:
                score: int | None = None
                if terminal == alias:
                    score = 120
                elif normalized.endswith(alias):
                    score = 84
                elif terminal.startswith(alias):
                    score = 48
                elif alias in terminal:
                    score = 28
                elif alias in normalized:
                    score = 12
                if score is None:
                    continue
                score -= penalty
                if best_score is None or score > best_score:
                    best_score = score
                    best_value = float(value)
    return best_value


def _edinet_normalize_ratio_metric(value: float | None, *, max_abs: float) -> float | None:
    if value is None:
        return None
    numeric = float(value)
    if not math.isfinite(numeric):
        return None
    if abs(numeric) > max_abs:
        if abs(numeric) <= max_abs * 100:
            numeric = numeric / 100.0
        else:
            return None
    return numeric if math.isfinite(numeric) and abs(numeric) <= max_abs else None


def _edinet_resolve_margin(
    numerator: float | None,
    denominator: float | None,
    fallback_ratio: float | None,
    *,
    max_abs: float,
) -> float | None:
    if numerator is not None and denominator not in (None, 0):
        try:
            computed = float(numerator) / float(denominator)
        except (TypeError, ValueError, ZeroDivisionError):
            computed = None
        normalized = _edinet_normalize_ratio_metric(computed, max_abs=max_abs)
        if normalized is not None:
            return normalized
    return _edinet_normalize_ratio_metric(fallback_ratio, max_abs=max_abs)


def _edinet_parse_fiscal_year(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"(19|20)\d{2}", text)
    if not match:
        return None
    try:
        year = int(match.group(0))
    except ValueError:
        return None
    return year if 1900 <= year <= 2100 else None


def _edinet_normalize_excerpt(value: Any, *, limit: int) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text:
        return None
    return text if len(text) <= limit else f"{text[: max(0, limit - 1)].rstrip()}…"


def _edinet_collect_text_pairs(payload: Any) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    stack: list[tuple[str, Any]] = [("", payload)]
    while stack:
        path, node = stack.pop()
        if isinstance(node, dict):
            for key, value in reversed(list(node.items())):
                next_path = f"{path}.{key}" if path else str(key)
                stack.append((next_path, value))
            continue
        if isinstance(node, list):
            for idx, value in reversed(list(enumerate(node))):
                next_path = f"{path}[{idx}]"
                stack.append((next_path, value))
            continue
        normalized = _edinet_normalize_excerpt(node, limit=400)
        if not normalized or len(normalized) < 12:
            continue
        pairs.append((path, normalized))
    return pairs


def _edinet_text_path_score(path: str, aliases: Sequence[str]) -> int | None:
    normalized = _edinet_normalize_key(path)
    if not normalized:
        return None
    segments = _edinet_normalize_segments(path)
    terminal = segments[-1] if segments else normalized
    best: int | None = None
    for alias in aliases:
        alias_key = _edinet_normalize_key(alias)
        if not alias_key:
            continue
        score: int | None = None
        if terminal == alias_key:
            score = 120
        elif normalized.endswith(alias_key):
            score = 84
        elif alias_key in terminal:
            score = 36
        elif alias_key in normalized:
            score = 14
        if score is None:
            continue
        if best is None or score > best:
            best = score
    return best


def _edinet_display_label_from_path(path: str, *, fallback: str) -> str:
    text = str(path or "").strip()
    if not text:
        return fallback
    segment = re.split(r"[.\[\]]+", text)[-1].strip(" _-")
    if not segment:
        return fallback
    if len(segment) <= 32:
        return segment
    return fallback


def _build_edinet_analysis_summary(repo: EdinetdbRepository, edinet_code: str) -> dict[str, Any] | None:
    latest = repo.get_latest_analysis(edinet_code)
    if not isinstance(latest, dict):
        return None
    pairs = _edinet_collect_text_pairs(latest.get("payload"))
    if not pairs:
        return None

    groups: list[tuple[str, tuple[str, ...]]] = [
        ("要約", ("summary", "overview", "executive_summary", "investment_summary")),
        ("強み", ("strengths", "strength", "pros", "bull_case")),
        ("リスク", ("risks", "risk", "cons", "bear_case")),
        ("見通し", ("outlook", "guidance", "prospects", "forecast")),
        ("バリュエーション", ("valuation", "fair_value", "multiple", "price_target")),
    ]
    used_paths: set[str] = set()
    used_values: set[str] = set()
    items: list[dict[str, str]] = []
    for label, aliases in groups:
        scored: list[tuple[int, int, str, str]] = []
        for index, (path, value) in enumerate(pairs):
            if path in used_paths or value in used_values:
                continue
            score = _edinet_text_path_score(path, aliases)
            if score is None:
                continue
            scored.append((score, -index, path, value))
        if not scored:
            continue
        scored.sort(reverse=True)
        _, _, path, value = scored[0]
        used_paths.add(path)
        used_values.add(value)
        items.append({"label": label, "value": _edinet_normalize_excerpt(value, limit=220) or value})

    if not items:
        for path, value in pairs:
            if value in used_values:
                continue
            items.append(
                {
                    "label": _edinet_display_label_from_path(path, fallback=f"項目{len(items) + 1}"),
                    "value": _edinet_normalize_excerpt(value, limit=220) or value,
                }
            )
            used_values.add(value)
            if len(items) >= 4:
                break

    if not items:
        return None
    return {
        "asOf": str(latest.get("asof_date") or "").strip() or _edinet_isoformat(latest.get("fetched_at")),
        "items": items,
    }


def _build_edinet_text_highlights(repo: EdinetdbRepository, edinet_code: str) -> list[dict[str, Any]]:
    rows = repo.list_text_blocks(edinet_code, limit=64)
    if not rows:
        return []

    groups: list[tuple[str, tuple[str, ...]]] = [
        ("business", ("business", "overview", "description", "profile", "operations")),
        ("strategy", ("strategy", "management", "plan", "growth", "vision")),
        ("mda", ("mda", "md&a", "managementdiscussion", "analysis", "operatingresults")),
        ("risk", ("risk", "riskfactor", "businessrisk")),
    ]
    used_indices: set[int] = set()
    highlights: list[dict[str, Any]] = []
    for _, aliases in groups:
        scored: list[tuple[int, int, dict[str, Any], int]] = []
        for index, row in enumerate(rows):
            if index in used_indices:
                continue
            block_name = str(row.get("block_name") or "").strip()
            if not block_name:
                continue
            score = _edinet_text_path_score(block_name, aliases)
            if score is None:
                continue
            fetched_at = row.get("fetched_at")
            sort_key = int(fetched_at.timestamp()) if isinstance(fetched_at, datetime) else 0
            scored.append((score, sort_key, row, index))
        if not scored:
            continue
        scored.sort(reverse=True)
        _, _, row, index = scored[0]
        excerpt = _edinet_normalize_excerpt(row.get("text"), limit=220)
        if not excerpt:
            continue
        used_indices.add(index)
        highlights.append(
            {
                "blockName": str(row.get("block_name") or "").strip() or "block",
                "fiscalYear": str(row.get("fiscal_year") or "").strip() or None,
                "excerpt": excerpt,
            }
        )

    if not highlights:
        for row in rows:
            excerpt = _edinet_normalize_excerpt(row.get("text"), limit=220)
            if not excerpt:
                continue
            highlights.append(
                {
                    "blockName": str(row.get("block_name") or "").strip() or "block",
                    "fiscalYear": str(row.get("fiscal_year") or "").strip() or None,
                    "excerpt": excerpt,
                }
            )
            if len(highlights) >= 4:
                break
    return highlights[:4]


def _build_edinet_error_payload(
    *,
    status: str,
    status_detail: str | None,
    bootstrap_state: dict[str, Any] | None,
    mapped: bool | None = None,
    official_filings: list[dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    return {
        "status": status,
        "statusDetail": status_detail,
        "mapped": mapped,
        "fetchedAt": None,
        "lastCheckedAt": None,
        "bootstrapState": bootstrap_state,
        "summary": None,
        "series": [],
        "analysisSummary": None,
        "textHighlights": [],
        "officialFilings": list(official_filings or []),
    }


def _build_official_edinet_filings(
    repo: EdinetdbRepository,
    *,
    sec_code: str,
    edinet_code: str | None,
    limit: int = 6,
) -> list[dict[str, Any]]:
    rows = repo.list_official_documents(sec_code=sec_code, edinet_code=edinet_code, limit=limit)
    filings: list[dict[str, Any]] = []
    seen_doc_ids: set[str] = set()
    for row in rows:
        doc_id = str(row.get("doc_id") or "").strip()
        if not doc_id or doc_id in seen_doc_ids:
            continue
        seen_doc_ids.add(doc_id)
        period_start = str(row.get("period_start") or "").strip() or None
        period_end = str(row.get("period_end") or "").strip() or None
        if period_start and period_end:
            period_label = f"{period_start} - {period_end}"
        else:
            period_label = period_end or period_start
        filings.append(
            {
                "docId": doc_id,
                "submitDateTime": str(row.get("submit_datetime") or "").strip() or None,
                "docDescription": str(row.get("doc_description") or "").strip() or None,
                "formCode": str(row.get("form_code") or "").strip() or None,
                "periodLabel": period_label,
                "filerName": str(row.get("filer_name") or "").strip() or None,
                "hasCsv": bool(row.get("csv_flag")),
                "hasPdf": bool(row.get("pdf_flag")),
                "hasXbrl": bool(row.get("xbrl_flag")),
                "searchUrl": f"https://disclosure2.edinet-fsa.go.jp/WEEK0010.aspx?code={sec_code}",
            }
        )
    return filings


def _build_edinet_financials_base_payload(code: str) -> Dict[str, Any] | None:
    code_key = str(code or "").strip()
    if not code_key:
        return None
    now_ts = time.time()
    with _EDINET_FINANCIALS_CACHE_LOCK:
        cached = _EDINET_FINANCIALS_CACHE.get(code_key)
        if cached and now_ts - cached[0] <= _EDINET_SUMMARY_CACHE_TTL_SEC:
            payload = cached[1]
            return dict(payload) if isinstance(payload, dict) else None

    try:
        with get_conn() as conn:
            map_row = conn.execute(
                """
                SELECT edinet_code
                FROM edinetdb_company_map
                WHERE sec_code = ?
                LIMIT 1
                """,
                [code_key],
            ).fetchone()
            if not map_row or not map_row[0]:
                result = {"status": "unmapped", "mapped": False, "summary": None, "series": []}
            else:
                edinet_code = str(map_row[0]).strip()
                fin_rows = conn.execute(
                    """
                    SELECT fiscal_year, payload_json, fetched_at
                    FROM edinetdb_financials
                    WHERE edinet_code = ?
                    ORDER BY fetched_at DESC NULLS LAST, fiscal_year DESC NULLS LAST
                    """,
                    [edinet_code],
                ).fetchall()
                ratio_rows = conn.execute(
                    """
                    SELECT fiscal_year, payload_json, fetched_at
                    FROM edinetdb_ratios
                    WHERE edinet_code = ?
                    ORDER BY fetched_at DESC NULLS LAST, fiscal_year DESC NULLS LAST
                    """,
                    [edinet_code],
                ).fetchall()
                by_year: dict[int, dict[str, Any]] = {}
                latest_fetched: datetime | None = None
                for fiscal_year, payload_json, fetched_at in fin_rows:
                    year = _edinet_parse_fiscal_year(fiscal_year)
                    if year is None or year in by_year and by_year[year].get("financial") is not None:
                        continue
                    bucket = by_year.setdefault(year, {"financial": None, "ratio": None})
                    bucket["financial"] = _edinet_json_load(payload_json)
                    if isinstance(fetched_at, datetime) and (latest_fetched is None or fetched_at > latest_fetched):
                        latest_fetched = fetched_at
                for fiscal_year, payload_json, fetched_at in ratio_rows:
                    year = _edinet_parse_fiscal_year(fiscal_year)
                    if year is None or year in by_year and by_year[year].get("ratio") is not None:
                        continue
                    bucket = by_year.setdefault(year, {"financial": None, "ratio": None})
                    bucket["ratio"] = _edinet_json_load(payload_json)
                    if isinstance(fetched_at, datetime) and (latest_fetched is None or fetched_at > latest_fetched):
                        latest_fetched = fetched_at

                series: list[dict[str, Any]] = []
                for year in sorted(by_year):
                    financial_payload = by_year[year].get("financial")
                    ratio_payload = by_year[year].get("ratio")
                    fin_pairs = _edinet_collect_numeric_pairs(financial_payload)
                    ratio_pairs = _edinet_collect_numeric_pairs(ratio_payload)
                    revenue = _edinet_find_first_metric(
                        fin_pairs,
                        ratio_pairs,
                        aliases=_EDINET_ALIAS_REVENUE,
                        disqualifiers=_EDINET_AMOUNT_DISQUALIFIERS,
                    )
                    gross_profit = _edinet_find_first_metric(
                        fin_pairs,
                        ratio_pairs,
                        aliases=_EDINET_ALIAS_GROSS_PROFIT,
                        disqualifiers=_EDINET_AMOUNT_DISQUALIFIERS,
                    )
                    operating_income = _edinet_find_first_metric(
                        fin_pairs,
                        ratio_pairs,
                        aliases=_EDINET_ALIAS_OPERATING_INCOME,
                        disqualifiers=_EDINET_AMOUNT_DISQUALIFIERS,
                    )
                    net_income = _edinet_find_first_metric(
                        fin_pairs,
                        ratio_pairs,
                        aliases=_EDINET_ALIAS_NET_INCOME,
                        disqualifiers=_EDINET_AMOUNT_DISQUALIFIERS,
                    )
                    gross_margin = _edinet_resolve_margin(
                        gross_profit,
                        revenue,
                        _edinet_find_first_metric(ratio_pairs, fin_pairs, aliases=("grossmargin", "売上総利益率")),
                        max_abs=1.5,
                    )
                    operating_margin = _edinet_resolve_margin(
                        operating_income,
                        revenue,
                        _edinet_find_first_metric(ratio_pairs, fin_pairs, aliases=("operatingmargin", "営業利益率")),
                        max_abs=1.5,
                    )
                    net_margin = _edinet_resolve_margin(
                        net_income,
                        revenue,
                        _edinet_find_first_metric(ratio_pairs, fin_pairs, aliases=("netmargin", "純利益率")),
                        max_abs=2.5,
                    )
                    roe = _edinet_normalize_ratio_metric(
                        _edinet_find_first_metric(ratio_pairs, fin_pairs, aliases=_EDINET_ALIAS_ROE_DETAIL),
                        max_abs=3.0,
                    )
                    roa = _edinet_normalize_ratio_metric(
                        _edinet_find_first_metric(ratio_pairs, fin_pairs, aliases=_EDINET_ALIAS_ROA_DETAIL),
                        max_abs=2.0,
                    )
                    eps = _edinet_find_first_metric(fin_pairs, ratio_pairs, aliases=_EDINET_ALIAS_EPS)
                    bps = _edinet_find_first_metric(fin_pairs, ratio_pairs, aliases=_EDINET_ALIAS_BPS)
                    dividend_per_share = _edinet_find_first_metric(fin_pairs, ratio_pairs, aliases=_EDINET_ALIAS_DIVIDEND)
                    equity_ratio = _edinet_normalize_ratio_metric(
                        _edinet_find_first_metric(ratio_pairs, fin_pairs, aliases=_EDINET_ALIAS_EQUITY_RATIO_DETAIL),
                        max_abs=1.2,
                    )
                    net_interest_bearing_debt = _edinet_find_first_metric(
                        fin_pairs,
                        ratio_pairs,
                        aliases=_EDINET_ALIAS_NET_INTEREST_BEARING_DEBT,
                    )
                    series.append(
                        {
                            "fiscalYear": year,
                            "label": str(year),
                            "revenue": revenue,
                            "grossProfit": gross_profit,
                            "operatingIncome": operating_income,
                            "netIncome": net_income,
                            "grossMargin": gross_margin,
                            "operatingMargin": operating_margin,
                            "netMargin": net_margin,
                            "roe": roe,
                            "roa": roa,
                            "eps": eps,
                            "bps": bps,
                            "dividendPerShare": dividend_per_share,
                            "equityRatio": equity_ratio,
                            "netInterestBearingDebt": net_interest_bearing_debt,
                        }
                    )

                latest = series[-1] if series else None
                result = {
                    "status": "ok" if series else "no_payload",
                    "mapped": True,
                    "fetchedAt": latest_fetched.isoformat() if latest_fetched else None,
                    "summary": {
                        "latestFiscalYear": latest.get("fiscalYear") if latest else None,
                        "equityRatio": latest.get("equityRatio") if latest else None,
                        "eps": latest.get("eps") if latest else None,
                        "bps": latest.get("bps") if latest else None,
                        "dividendPerShare": latest.get("dividendPerShare") if latest else None,
                        "netInterestBearingDebt": latest.get("netInterestBearingDebt") if latest else None,
                    } if latest else None,
                    "series": series,
                }
    except Exception:
        return None

    with _EDINET_FINANCIALS_CACHE_LOCK:
        _EDINET_FINANCIALS_CACHE[code_key] = (now_ts, dict(result))
        if len(_EDINET_FINANCIALS_CACHE) > 1024:
            oldest_key = min(_EDINET_FINANCIALS_CACHE, key=lambda key: _EDINET_FINANCIALS_CACHE[key][0])
            _EDINET_FINANCIALS_CACHE.pop(oldest_key, None)
    return result


def _build_edinet_financials_payload(code: str) -> Dict[str, Any] | None:
    code_key = str(code or "").strip()
    if not code_key:
        return None
    bootstrap_state = get_active_edinet_bootstrap_state()
    repo = _edinet_repo()
    try:
        edinet_code = str(repo.lookup_edinet_codes([code_key]).get(code_key) or "").strip()
    except Exception:
        edinet_code = ""
    try:
        official_filings = _build_official_edinet_filings(
            repo,
            sec_code=code_key,
            edinet_code=edinet_code or None,
        )
    except Exception:
        official_filings = []
    try:
        table_state = repo.get_seed_table_state()
    except Exception:
        return _build_edinet_error_payload(
            status="loading" if bool((bootstrap_state or {}).get("active")) else "error",
            status_detail="seed_table_state_failed",
            bootstrap_state=bootstrap_state,
            mapped=bool(edinet_code) if edinet_code else None,
            official_filings=official_filings,
        )

    missing_tables = bool(table_state.get("missing_tables"))
    all_empty = bool(table_state.get("all_empty"))
    if missing_tables or all_empty:
        status, status_detail = _resolve_edinet_runtime_status(
            base_status="empty_tables" if all_empty else "missing_tables",
            bootstrap_state=bootstrap_state,
            missing_tables=missing_tables,
            empty_tables=all_empty,
        )
        return _build_edinet_error_payload(
            status=status,
            status_detail=status_detail,
            bootstrap_state=bootstrap_state,
            mapped=bool(edinet_code) if edinet_code else None,
            official_filings=official_filings,
        )

    base = _build_edinet_financials_base_payload(code_key)
    if not isinstance(base, dict):
        return _build_edinet_error_payload(
            status="loading" if bool((bootstrap_state or {}).get("active")) else "error",
            status_detail="financial_payload_build_failed",
            bootstrap_state=bootstrap_state,
            mapped=bool(edinet_code) if edinet_code else None,
            official_filings=official_filings,
        )

    status, status_detail = _resolve_edinet_runtime_status(
        base_status=str(base.get("status") or "").strip() or None,
        bootstrap_state=bootstrap_state,
    )
    company_latest = repo.get_company_latest(edinet_code) if edinet_code else None
    return {
        "status": status,
        "statusDetail": status_detail,
        "mapped": base.get("mapped"),
        "fetchedAt": base.get("fetchedAt"),
        "lastCheckedAt": _edinet_isoformat((company_latest or {}).get("last_checked_at"))
        or _edinet_isoformat((company_latest or {}).get("fetched_at"))
        or (base.get("fetchedAt") if isinstance(base.get("fetchedAt"), str) else None),
        "bootstrapState": bootstrap_state,
        "summary": base.get("summary"),
        "series": base.get("series") if isinstance(base.get("series"), list) else [],
        "analysisSummary": _build_edinet_analysis_summary(repo, edinet_code) if edinet_code else None,
        "textHighlights": _build_edinet_text_highlights(repo, edinet_code) if edinet_code else [],
        "officialFilings": official_filings,
    }


def _normalize_risk_mode(value: str | None) -> str:
    resolved = str(value or "balanced").strip().lower()
    if resolved not in _VALID_RISK_MODES:
        raise HTTPException(status_code=400, detail="risk_mode must be defensive/balanced/aggressive")
    return resolved


def _infer_playbook_setup_type(
    *,
    direction: str,
    shape_patterns: dict[str, bool],
    trend_up_strict: bool,
    trend_down_strict: bool,
    monthly_box_state: str | None,
) -> str:
    box_state = str(monthly_box_state or "")
    if direction == "up":
        if bool(shape_patterns.get("a3CapitulationRebound")):
            return "rebound"
        if bool(shape_patterns.get("a1MaturedBreakout")):
            return "breakout"
        if bool(shape_patterns.get("a2BoxTrend")):
            return "accumulation"
        if trend_up_strict and box_state in {"box_mid", "box_upper", "breakout_up"}:
            return "continuation"
        return "watch"

    if (
        bool(shape_patterns.get("d1ShortBreakdown"))
        or bool(shape_patterns.get("d2ShortMixedFar"))
        or bool(shape_patterns.get("d3ShortNaBelow"))
    ):
        return "breakdown"
    if trend_down_strict and box_state in {"below_box", "box_lower"}:
        return "continuation"
    return "watch"


def _build_playbook_policy_side(
    *,
    direction: str,
    risk_mode: str,
    trend_up_strict: bool,
    trend_down_strict: bool,
    monthly_box_state: str | None,
    monthly_box_months: float | None,
    dist_ma20_signed: float | None,
    cnt60_up: float | None,
    cnt100_up: float | None,
) -> Dict[str, Any]:
    shape_patterns = rankings_cache._calc_shape_pattern_flags(
        direction=direction,  # type: ignore[arg-type]
        trend_up_strict=trend_up_strict,
        trend_down_strict=trend_down_strict,
        monthly_box_state=monthly_box_state,
        monthly_box_months=monthly_box_months,
        dist_ma20_signed=dist_ma20_signed,
        cnt60_up=cnt60_up,
        cnt100_up=cnt100_up,
    )
    setup_type = _infer_playbook_setup_type(
        direction=direction,
        shape_patterns=shape_patterns,
        trend_up_strict=trend_up_strict,
        trend_down_strict=trend_down_strict,
        monthly_box_state=monthly_box_state,
    )
    side: Dict[str, Any] = {}
    rankings_cache._apply_entry_playbook_fields(
        side,
        direction=direction,  # type: ignore[arg-type]
        setup_type=setup_type,
        shape_patterns=shape_patterns,
        risk_mode=risk_mode,  # type: ignore[arg-type]
    )
    side["setupType"] = setup_type
    side["shapePatterns"] = shape_patterns
    side["playbookScoreBonus"] = float(
        rankings_cache._calc_playbook_entry_bonus(
            direction=direction,  # type: ignore[arg-type]
            shape_patterns=shape_patterns,
        )
    )
    return side


def _build_entry_policy_summary(
    *,
    daily_rows: list[tuple],
    monthly_rows: list[tuple],
    risk_mode: str,
) -> Dict[str, Any] | None:
    if not daily_rows:
        return None

    daily_closes: list[float] = []
    for row in daily_rows:
        if len(row) < 5 or row[4] is None:
            continue
        close_val = _to_float_or_none(row[4])
        if close_val is None:
            continue
        daily_closes.append(float(close_val))
    if not daily_closes:
        return None

    ma20 = _rolling_sma(daily_closes, 20)
    ma60 = _rolling_sma(daily_closes, 60)
    last_idx = len(daily_closes) - 1
    close_now = daily_closes[last_idx]
    ma20_now = ma20[last_idx] if last_idx >= 0 else None
    ma60_now = ma60[last_idx] if last_idx >= 0 else None
    ma20_prev = ma20[last_idx - 1] if last_idx - 1 >= 0 else None
    ma60_prev = ma60[last_idx - 1] if last_idx - 1 >= 0 else None

    trend_up = bool(
        ma20_now is not None
        and ma60_now is not None
        and close_now > ma20_now > ma60_now
    )
    trend_down = bool(
        ma20_now is not None
        and ma60_now is not None
        and close_now < ma20_now < ma60_now
    )
    ma20_slope = (
        float(ma20_now - ma20_prev)
        if ma20_now is not None and ma20_prev is not None and math.isfinite(ma20_now) and math.isfinite(ma20_prev)
        else None
    )
    ma60_slope = (
        float(ma60_now - ma60_prev)
        if ma60_now is not None and ma60_prev is not None and math.isfinite(ma60_now) and math.isfinite(ma60_prev)
        else None
    )
    dist_ma20_signed = (
        float((close_now - ma20_now) / ma20_now)
        if ma20_now is not None and ma20_now != 0 and math.isfinite(ma20_now)
        else None
    )
    trend_up_strict = bool(
        trend_up
        and isinstance(ma20_slope, (int, float))
        and isinstance(ma60_slope, (int, float))
        and float(ma20_slope) > 0
        and float(ma60_slope) > 0
        and isinstance(dist_ma20_signed, (int, float))
        and float(dist_ma20_signed) >= 0.005
    )
    trend_down_strict = bool(
        trend_down
        and isinstance(ma20_slope, (int, float))
        and isinstance(ma60_slope, (int, float))
        and float(ma20_slope) < 0
        and float(ma60_slope) < 0
        and isinstance(dist_ma20_signed, (int, float))
        and float(dist_ma20_signed) <= -0.005
    )

    v60_signals = rankings_cache._calc_60v_signals(daily_rows)
    cnt60_up = _to_float_or_none(v60_signals.get("cnt60Up"))
    cnt100_up = _to_float_or_none(v60_signals.get("cnt100Up"))

    monthly_box = rankings_cache._detect_monthly_body_box(monthly_rows)
    monthly_box_state, _ = rankings_cache._calc_monthly_box_state(
        entry_close=close_now,
        box=monthly_box,
    )
    monthly_box_months = (
        _to_float_or_none(monthly_box.get("months"))
        if isinstance(monthly_box, dict)
        else None
    )

    up_side = _build_playbook_policy_side(
        direction="up",
        risk_mode=risk_mode,
        trend_up_strict=trend_up_strict,
        trend_down_strict=trend_down_strict,
        monthly_box_state=monthly_box_state,
        monthly_box_months=monthly_box_months,
        dist_ma20_signed=dist_ma20_signed,
        cnt60_up=cnt60_up,
        cnt100_up=cnt100_up,
    )
    down_side = _build_playbook_policy_side(
        direction="down",
        risk_mode=risk_mode,
        trend_up_strict=trend_up_strict,
        trend_down_strict=trend_down_strict,
        monthly_box_state=monthly_box_state,
        monthly_box_months=monthly_box_months,
        dist_ma20_signed=dist_ma20_signed,
        cnt60_up=cnt60_up,
        cnt100_up=cnt100_up,
    )
    return {
        "riskMode": risk_mode,
        "up": up_side,
        "down": down_side,
    }


def _clip_probability(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return min(1.0, max(0.0, float(value)))


def _scale_probability_by_horizon(
    base_prob: float | None,
    source_horizon: int,
    target_horizon: int,
) -> float | None:
    clipped = _clip_probability(base_prob)
    if clipped is None:
        return None
    if source_horizon <= 0 or target_horizon <= 0:
        return clipped
    eps = 1.0e-6
    p = min(1.0 - eps, max(eps, clipped))
    ratio = float(target_horizon) / float(source_horizon)
    if ratio <= 0:
        return clipped
    scale = math.sqrt(ratio)
    logit = math.log(p / (1.0 - p))
    scaled = logit * scale
    prob = 1.0 / (1.0 + math.exp(-scaled))
    return _clip_probability(prob)


def _scale_ev_by_horizon(
    base_ev: float | None,
    source_horizon: int,
    target_horizon: int,
) -> float | None:
    if base_ev is None or not math.isfinite(base_ev):
        return None
    if source_horizon <= 0 or target_horizon <= 0:
        return float(base_ev)
    return float(base_ev) * (float(target_horizon) / float(source_horizon))


def _build_horizon_analysis(
    p_up_20d: float | None,
    ev_net_20d: float | None,
    p_turn_down_10d: float | None,
    *,
    p_up_5d: float | None = None,
    p_up_10d: float | None = None,
    ev_net_5d: float | None = None,
    ev_net_10d: float | None = None,
    p_turn_down_5d: float | None = None,
    p_turn_down_20d: float | None = None,
) -> Dict[str, Any]:
    horizon_values: Dict[str, Dict[str, Any]] = {}
    base_turn = p_turn_down_10d
    if base_turn is None:
        base_turn = 1.0 - p_up_20d if p_up_20d is not None else None
    for horizon in (5, 10, 20):
        direct_p_up = (
            _clip_probability(p_up_5d)
            if horizon == 5
            else _clip_probability(p_up_10d)
            if horizon == 10
            else _clip_probability(p_up_20d)
        )
        if direct_p_up is not None:
            p_up = direct_p_up
            p_up_projected = False
        elif horizon == 20:
            p_up = _clip_probability(p_up_20d)
            p_up_projected = False
        else:
            p_up = _scale_probability_by_horizon(p_up_20d, source_horizon=20, target_horizon=horizon)
            p_up_projected = True
        p_down = (1.0 - p_up) if p_up is not None else None
        direct_ev = (
            _to_float_or_none(ev_net_5d)
            if horizon == 5
            else _to_float_or_none(ev_net_10d)
            if horizon == 10
            else _to_float_or_none(ev_net_20d)
        )
        if direct_ev is not None:
            ev_net = direct_ev
            ev_projected = False
        else:
            ev_net = _scale_ev_by_horizon(ev_net_20d, source_horizon=20, target_horizon=horizon)
            ev_projected = horizon != 20
        direct_turn = (
            _clip_probability(p_turn_down_5d)
            if horizon == 5
            else _clip_probability(p_turn_down_10d)
            if horizon == 10
            else _clip_probability(p_turn_down_20d)
        )
        if direct_turn is not None:
            p_turn_down = direct_turn
            turn_projected = False
        elif horizon == 10:
            p_turn_down = _clip_probability(1.0 - p_up) if p_up is not None else None
            turn_projected = True
        else:
            p_turn_down = _scale_probability_by_horizon(
                base_turn,
                source_horizon=10,
                target_horizon=horizon,
            )
            turn_projected = True
        horizon_values[str(horizon)] = {
            "horizon": horizon,
            "pUp": p_up,
            "pDown": p_down,
            "evNet": ev_net,
            "pTurnDown": p_turn_down,
            "pTurnUp": (1.0 - p_turn_down) if p_turn_down is not None else None,
            "pUpProjected": p_up_projected,
            "evProjected": ev_projected,
            "turnProjected": turn_projected,
        }
    return {
        "defaultHorizon": 20,
        "turnBaseHorizon": 10,
        "projectionMethod": "logit_sqrt_time",
        "items": horizon_values,
    }


def _rolling_sma(values: list[float], period: int) -> list[float | None]:
    if period <= 0:
        return [None for _ in values]
    out: list[float | None] = [None for _ in values]
    running = 0.0
    for idx, value in enumerate(values):
        running += float(value)
        if idx >= period:
            running -= float(values[idx - period])
        if idx >= period - 1:
            out[idx] = float(running / period)
    return out


def _build_additive_signal_summary(
    daily_rows: list[tuple],
    monthly_rows: list[tuple],
) -> Dict[str, Any] | None:
    if not daily_rows:
        return None

    daily_closes: list[float] = []
    for row in daily_rows:
        if len(row) < 5 or row[4] is None:
            continue
        try:
            daily_closes.append(float(row[4]))
        except (TypeError, ValueError):
            continue
    if not daily_closes:
        return None

    ma20 = _rolling_sma(daily_closes, 20)
    ma60 = _rolling_sma(daily_closes, 60)
    last_idx = len(daily_closes) - 1
    close_now = daily_closes[last_idx]
    ma20_now = ma20[last_idx] if last_idx >= 0 else None
    ma60_now = ma60[last_idx] if last_idx >= 0 else None
    ma20_prev = ma20[last_idx - 1] if last_idx - 1 >= 0 else None
    ma60_prev = ma60[last_idx - 1] if last_idx - 1 >= 0 else None
    trend_up = bool(
        ma20_now is not None
        and ma60_now is not None
        and close_now > ma20_now > ma60_now
    )
    ma20_slope = (
        float(ma20_now - ma20_prev)
        if ma20_now is not None and ma20_prev is not None and math.isfinite(ma20_now) and math.isfinite(ma20_prev)
        else None
    )
    ma60_slope = (
        float(ma60_now - ma60_prev)
        if ma60_now is not None and ma60_prev is not None and math.isfinite(ma60_now) and math.isfinite(ma60_prev)
        else None
    )
    dist_ma20_signed = (
        float((close_now - ma20_now) / ma20_now)
        if ma20_now is not None and ma20_now != 0 and math.isfinite(ma20_now)
        else None
    )
    trend_up_strict = bool(
        trend_up
        and isinstance(ma20_slope, (int, float))
        and isinstance(ma60_slope, (int, float))
        and float(ma20_slope) > 0
        and float(ma60_slope) > 0
        and isinstance(dist_ma20_signed, (int, float))
        and float(dist_ma20_signed) >= 0.005
    )

    weekly = rankings_cache._build_weekly_bars(daily_rows)
    last_daily_dt = rankings_cache._parse_date_value(daily_rows[-1][0]) if daily_rows else None
    weekly = rankings_cache._drop_incomplete_weekly(weekly, last_daily_dt)
    weekly_closes = [float(item["c"]) for item in weekly if isinstance(item.get("c"), (int, float))]
    monthly_closes = [
        float(row[4])
        for row in monthly_rows
        if len(row) >= 5 and isinstance(row[4], (int, float))
    ]
    weekly_regime = rankings_cache._calc_regime_probs(weekly_closes, lookback=20)
    monthly_regime = rankings_cache._calc_regime_probs(monthly_closes, lookback=12)
    weekly_breakout_up_prob = _to_float_or_none(weekly_regime.get("breakoutUpProb"))
    monthly_breakout_up_prob = _to_float_or_none(monthly_regime.get("breakoutUpProb"))
    monthly_range_prob = _to_float_or_none(monthly_regime.get("rangeProb"))
    monthly_range_pos = _to_float_or_none(monthly_regime.get("rangePos"))

    candle_signals = rankings_cache._calc_triplet_candle_signals(daily_rows)
    shooting_star_like = bool((_to_float_or_none(candle_signals.get("shootingStarLike")) or 0.0) >= 0.5)
    bear_marubozu = bool((_to_float_or_none(candle_signals.get("bearMarubozu")) or 0.0) >= 0.5)
    three_white_soldiers = bool((_to_float_or_none(candle_signals.get("threeWhiteSoldiers")) or 0.0) >= 0.5)
    three_black_crows = bool((_to_float_or_none(candle_signals.get("threeBlackCrows")) or 0.0) >= 0.5)
    morning_star = bool((_to_float_or_none(candle_signals.get("morningStar")) or 0.0) >= 0.5)
    bull_engulfing = bool((_to_float_or_none(candle_signals.get("bullEngulfing")) or 0.0) >= 0.5)

    v60_signals = rankings_cache._calc_60v_signals(daily_rows)
    reclaim60 = bool((_to_float_or_none(v60_signals.get("reclaim60")) or 0.0) >= 0.5)
    v60_core = bool((_to_float_or_none(v60_signals.get("v60Core")) or 0.0) >= 0.5)
    v60_strong = bool((_to_float_or_none(v60_signals.get("v60Strong")) or 0.0) >= 0.5)

    mtf_strong_aligned = bool(
        trend_up_strict
        and weekly_breakout_up_prob is not None
        and weekly_breakout_up_prob >= 0.56
        and monthly_breakout_up_prob is not None
        and monthly_breakout_up_prob >= 0.60
    )
    box_bottom_aligned = bool(
        monthly_range_prob is not None
        and monthly_range_pos is not None
        and monthly_range_prob >= 0.62
        and monthly_range_pos <= 0.38
    )

    candlestick_pattern_bonus, candlestick_pattern_bonus_details = rankings_cache._calc_candlestick_pattern_bonus(
        candle_signals,
        direction="up",
    )
    v60_strong_penalty = bool(v60_strong)
    bonus_estimate = (
        (0.02 if trend_up_strict else 0.0)
        + (0.02 if mtf_strong_aligned else 0.0)
        + (0.03 if box_bottom_aligned else 0.0)
        + candlestick_pattern_bonus
        - (0.01 if v60_strong_penalty else 0.0)
    )

    return {
        "trendUpStrict": trend_up_strict,
        "mtfStrongAligned": mtf_strong_aligned,
        "boxBottomAligned": box_bottom_aligned,
        "shootingStarLike": shooting_star_like,
        "bearMarubozu": bear_marubozu,
        "threeWhiteSoldiers": three_white_soldiers,
        "threeBlackCrows": three_black_crows,
        "morningStar": morning_star,
        "bullEngulfing": bull_engulfing,
        "reclaim60": reclaim60,
        "v60Core": v60_core,
        "v60Strong": v60_strong,
        "v60StrongPenalty": v60_strong_penalty,
        "candlestickPatternBonus": candlestick_pattern_bonus,
        "candlestickPatternBonusDetails": candlestick_pattern_bonus_details,
        "bonusEstimate": bonus_estimate,
        "weeklyBreakoutUpProb": weekly_breakout_up_prob,
        "monthlyBreakoutUpProb": monthly_breakout_up_prob,
        "monthlyRangeProb": monthly_range_prob,
        "monthlyRangePos": monthly_range_pos,
    }


@router.get("/phase", response_model=None)
def get_phase_pred(
    code: str,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    row = repo.get_phase_pred(code, asof_dt)
    if not row:
        return {"item": None}
    return {
        "item": {
            "dt": row[0],
            "earlyScore": row[1],
            "lateScore": row[2],
            "bodyScore": row[3],
            "n": row[4],
            "reasonsTop3": row[5],
        }
    }


@router.get("/analysis", response_model=None)
async def get_analysis_pred(
    code: str,
    asof: str | int | None = None,
    risk_mode: str = Query("balanced"),
    repo: StockRepository = Depends(get_stock_repo),
    *,
    request: Request,
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    token = _begin_detail_request("ticker.analysis", code)
    resolved_risk_mode = _normalize_risk_mode(risk_mode)
    asof_dt = _parse_dt(asof)
    error: Exception | None = None
    try:
        row = await run_in_threadpool(repo.get_ml_analysis_pred, code, asof_dt)
        _record_detail_query_step(token, 1)
        if not row:
            return {"item": None}
        await _raise_if_client_disconnected(request, token, phase="query")
        p_up = _to_float_or_none(row[1])
        p_down = _to_float_or_none(row[2]) if len(row) > 2 else None
        if p_down is None and p_up is not None:
            p_down = 1.0 - p_up
        p_up_5 = _to_float_or_none(row[3]) if len(row) > 3 else None
        p_up_10 = _to_float_or_none(row[4]) if len(row) > 4 else None
        p_turn_up = _to_float_or_none(row[5]) if len(row) > 5 else None
        p_turn_down = _to_float_or_none(row[6]) if len(row) > 6 else None
        p_turn_down_5 = _to_float_or_none(row[7]) if len(row) > 7 else None
        p_turn_down_10 = _to_float_or_none(row[8]) if len(row) > 8 else None
        p_turn_down_20 = _to_float_or_none(row[9]) if len(row) > 9 else None
        ret_pred20 = _to_float_or_none(row[12]) if len(row) > 12 else None
        ev20 = _to_float_or_none(row[13]) if len(row) > 13 else None
        ev20_net_raw = _to_float_or_none(row[14]) if len(row) > 14 else None
        ev5_net = _to_float_or_none(row[15]) if len(row) > 15 else None
        ev10_net = _to_float_or_none(row[16]) if len(row) > 16 else None
        ev20_net = ev20_net_raw if ev20_net_raw is not None else (ev20 - 0.002 if ev20 is not None else None)
        horizon_analysis = _build_horizon_analysis(
            p_up,
            ev20_net,
            p_turn_down_10 if p_turn_down_10 is not None else p_turn_down,
            p_up_5d=p_up_5,
            p_up_10d=p_up_10,
            ev_net_5d=ev5_net,
            ev_net_10d=ev10_net,
            p_turn_down_5d=p_turn_down_5,
            p_turn_down_20d=p_turn_down_20,
        )
        model_version = row[17] if len(row) > 17 else None
        additive_signals = None
        buy_stage_precision = None
        entry_policy = None
        daily_rows: list[tuple] = []
        monthly_rows: list[tuple] = []
        try:
            daily_rows, monthly_rows = await run_in_threadpool(
                _load_analysis_series,
                repo,
                code,
                asof_dt=asof_dt,
                daily_limit=1260,
                monthly_limit=60,
            )
            _record_detail_query_step(token, 2)
            await _raise_if_client_disconnected(request, token, phase="query")
            additive_signals = _build_additive_signal_summary(daily_rows, monthly_rows)
            entry_policy = _build_entry_policy_summary(
                daily_rows=daily_rows,
                monthly_rows=monthly_rows,
                risk_mode=resolved_risk_mode,
            )
        except Exception:
            additive_signals = None
            entry_policy = None
        try:
            buy_stage_precision = await run_in_threadpool(
                repo.get_buy_stage_precision,
                code,
                asof_dt,
                360,
                20,
            )
            _record_detail_query_step(token, 1)
        except Exception:
            buy_stage_precision = None
        research_prior = _build_research_prior_summary(code)
        edinet_summary = _build_edinet_summary(code, asof_dt)
        sell_context = None
        try:
            sell_row = await run_in_threadpool(repo.get_sell_analysis_snapshot, code, asof_dt)
            _record_detail_query_step(token, 1)
            sell_context = _build_sell_context_from_row(sell_row)
        except Exception:
            sell_context = None
        atr_pct, liquidity20d = swing_expectancy_service.compute_atr_pct_and_liquidity20d(daily_rows)
        as_of_ymd = _asof_dt_to_ymd(asof_dt)
        if as_of_ymd is None:
            as_of_ymd = _to_int_or_none(row[0])
        try:
            await run_in_threadpool(_ensure_latest_swing_setup_stats_once)
        except Exception:
            pass
        decision = build_analysis_decision(
            analysis_p_up=p_up,
            analysis_p_down=p_down,
            analysis_p_turn_up=p_turn_up,
            analysis_p_turn_down=p_turn_down,
            analysis_ev_net=ev20_net,
            playbook_up_score_bonus=_to_float_or_none((entry_policy or {}).get("up", {}).get("playbookScoreBonus"))
            if isinstance(entry_policy, dict)
            else None,
            playbook_down_score_bonus=_to_float_or_none((entry_policy or {}).get("down", {}).get("playbookScoreBonus"))
            if isinstance(entry_policy, dict)
            else None,
            additive_signals=additive_signals if isinstance(additive_signals, dict) else None,
            sell_analysis=sell_context if isinstance(sell_context, dict) else None,
        )
        swing_eval = swing_plan_service.build_swing_plan(
            code=code,
            as_of_ymd=None,
            close=_to_float_or_none(daily_rows[-1][4]) if daily_rows else None,
            p_up=p_up,
            p_down=p_down,
            p_turn_up=p_turn_up,
            p_turn_down=p_turn_down,
            ev20_net=ev20_net,
            long_setup_type=(entry_policy or {}).get("up", {}).get("setupType")
            if isinstance(entry_policy, dict)
            else None,
            short_setup_type=(entry_policy or {}).get("down", {}).get("setupType")
            if isinstance(entry_policy, dict)
            else None,
            playbook_bonus_long=_to_float_or_none((entry_policy or {}).get("up", {}).get("playbookScoreBonus"))
            if isinstance(entry_policy, dict)
            else None,
            playbook_bonus_short=_to_float_or_none((entry_policy or {}).get("down", {}).get("playbookScoreBonus"))
            if isinstance(entry_policy, dict)
            else None,
            short_score=_to_float_or_none((sell_context or {}).get("shortScore"))
            if isinstance(sell_context, dict)
            else None,
            atr_pct=atr_pct,
            liquidity20d=liquidity20d,
            decision_tone=str(decision.get("tone")) if isinstance(decision, dict) else None,
            hold_days_long=_to_int_or_none((entry_policy or {}).get("up", {}).get("recommendedHoldDays"))
            if isinstance(entry_policy, dict)
            else None,
            hold_days_short=_to_int_or_none((entry_policy or {}).get("down", {}).get("recommendedHoldDays"))
            if isinstance(entry_policy, dict)
            else None,
        )
        item = {
            "dt": row[0],
            "pUp": p_up,
            "pDown": p_down,
            "pTurnUp": p_turn_up,
            "pTurnDown": p_turn_down,
            "pTurnDownHorizon": 10,
            "retPred20": ret_pred20,
            "ev20": ev20,
            "ev20Net": ev20_net,
            "horizonAnalysis": horizon_analysis,
            "additiveSignals": additive_signals,
            "entryPolicy": entry_policy,
            "riskMode": resolved_risk_mode,
            "buyStagePrecision": buy_stage_precision,
            "researchPrior": research_prior,
            "edinetSummary": edinet_summary,
            "modelVersion": str(model_version) if model_version is not None else None,
            "source": decision.get("source") if isinstance(decision, dict) else DETAIL_ANALYSIS_SOURCE,
            "logic_family": decision.get("logic_family") if isinstance(decision, dict) else DETAIL_ANALYSIS_LOGIC_FAMILY,
            "display_label": decision.get("display_label") if isinstance(decision, dict) else DETAIL_ANALYSIS_DISPLAY_LABEL,
            "decision": decision,
            "swingPlan": swing_eval.get("plan") if isinstance(swing_eval, dict) else None,
            "swingDiagnostics": swing_eval.get("diagnostics") if isinstance(swing_eval, dict) else None,
        }
        _record_detail_transform_step(token, 1)
        await _raise_if_client_disconnected(request, token, phase="transform")
        succeeded = True
        return {
            "item": item,
            "source": item["source"],
            "logic_family": item["logic_family"],
            "display_label": item["display_label"],
        }
    except Exception as exc:
        error = exc
        raise
    finally:
        _finish_detail_request(token, error)


@router.get("/tradex/analysis", response_model=None)
def get_tradex_detail_analysis_snapshot(
    code: str,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    snapshot = build_tradex_detail_analysis_snapshot(
        code=code,
        asof_dt=asof_dt,
        repo=repo,
        enabled=is_tradex_detail_analysis_enabled(),
    )
    if not isinstance(snapshot, dict):
        return snapshot
    analysis = snapshot.get("analysis")
    if isinstance(analysis, dict):
        analysis = dict(analysis)
        analysis.setdefault("source", TRADEX_ANALYSIS_SOURCE)
        analysis.setdefault("display_label", TRADEX_ANALYSIS_DISPLAY_LABEL)
        snapshot["analysis"] = analysis
    snapshot.setdefault("source", TRADEX_ANALYSIS_SOURCE)
    snapshot.setdefault("display_label", TRADEX_ANALYSIS_DISPLAY_LABEL)
    return snapshot


@router.post("/tradex/summary", response_model=None)
def post_tradex_list_summary_snapshot(
    payload: Dict[str, Any] | list[Dict[str, Any]] = Body(...),
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    items = payload.get("items") if isinstance(payload, dict) else payload
    scope = payload.get("scope") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items must be a list")
    return build_tradex_list_summary_snapshot(
        items=[item for item in items if isinstance(item, dict)],
        repo=repo,
        enabled=is_tradex_list_summary_enabled(),
        detail_enabled=is_tradex_detail_analysis_enabled(),
        scope=str(scope).strip() if isinstance(scope, str) and scope.strip() else None,
    )


@router.get("/edinet/financials", response_model=None)
def get_edinet_financials(code: str) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    payload = _build_edinet_financials_payload(code)
    return {"item": payload}


@router.get("/tdnet/disclosures", response_model=None)
def get_tdnet_disclosures(code: str, limit: int = Query(10, ge=1, le=100)) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    repo = _get_tdnet_repo()
    source_configured = _is_tdnet_fetch_configured()
    try:
        state = repo.get_disclosure_state(code)
    except Exception as exc:
        logger.warning("TDNET disclosure state query failed for %s: %s", code, exc)
        return {
            "items": [],
            "meta": _build_tdnet_meta(
                {},
                source_configured=source_configured,
                query_failed=True,
                status_detail="state_query_failed",
            ),
        }
    meta = _build_tdnet_meta(state, source_configured=source_configured)
    if meta.get("status") == "missing_tables":
        return {"items": [], "meta": meta}
    try:
        items = repo.list_disclosures_by_code(code, limit=limit)
    except Exception as exc:
        logger.warning("TDNET disclosure list query failed for %s: %s", code, exc)
        return {
            "items": [],
            "meta": _build_tdnet_meta(
                state,
                source_configured=source_configured,
                query_failed=True,
                status_detail="list_query_failed",
            ),
        }
    return {"items": items, "meta": meta}


@router.get("/taisyaku/snapshot", response_model=None)
def get_taisyaku_snapshot(
    code: str,
    history_limit: int = Query(10, ge=1, le=60),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    return {"item": load_taisyaku_snapshot(code, history_limit=history_limit)}


@router.post("/tdnet/disclosures/import", response_model=None)
def import_tdnet_disclosures(
    payload: Dict[str, Any] | list[Dict[str, Any]] = Body(...),
) -> Dict[str, Any]:
    repo = _get_tdnet_repo()
    items = payload.get("items") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items must be a list")
    saved = repo.upsert_disclosures([item for item in items if isinstance(item, dict)])
    return {"ok": True, "saved": saved}


@router.get("/analysis/timeline", response_model=None)
async def get_analysis_timeline(
    code: str,
    limit: int = Query(400, ge=1, le=2000),
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
    *,
    request: Request,
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    token = _begin_detail_request("ticker.analysis_timeline", code)
    asof_dt = _parse_dt(asof)
    error: Exception | None = None
    try:
        items = await run_in_threadpool(repo.get_analysis_timeline, code, asof_dt, limit=limit)
        _record_detail_query_step(token, 1)
        await _raise_if_client_disconnected(request, token, phase="query")

        if items:
            try:
                latest_score = await run_in_threadpool(_get_timeline_ranking_score, repo, code, asof_dt)
                _record_detail_query_step(token, 1)
                if latest_score is not None:
                    for item in items:
                        item["rankingScore"] = latest_score
                    _record_detail_transform_step(token, 1)
            except Exception as exc:
                logger.warning("timeline ranking score attach failed code=%s reason=%s", code, exc)
        await _raise_if_client_disconnected(request, token, phase="transform")
        return {"items": items}
    except Exception as exc:
        error = exc
        raise
    finally:
        _finish_detail_request(token, error)


@router.get("/analysis/decisions", response_model=None)
def get_exact_analysis_decisions(
    code: str,
    start_dt: str | int,
    end_dt: str | int,
    risk_mode: str = Query("balanced"),
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    start_asof = _parse_dt(start_dt)
    end_asof = _parse_dt(end_dt)
    if start_asof is None or end_asof is None:
        raise HTTPException(status_code=400, detail="start_dt and end_dt are required")
    if start_asof > end_asof:
        start_asof, end_asof = end_asof, start_asof

    resolved_risk_mode = _normalize_risk_mode(risk_mode)
    start_key = _asof_dt_to_ymd(start_asof)
    end_key = _asof_dt_to_ymd(end_asof)
    if start_key is None or end_key is None:
        return {"items": []}

    timeline_limit = 400
    timeline_items = repo.get_analysis_timeline(code, end_asof, limit=timeline_limit)
    items: list[Dict[str, Any]] = []
    for analysis_point in timeline_items:
        if not isinstance(analysis_point, dict):
            continue
        dt_key = _normalize_date_key(analysis_point.get("dt"))
        if dt_key is None or dt_key < start_key or dt_key > end_key:
            continue
        # Detail markers only need decision tone. Use the warmed timeline cache
        # instead of rebuilding entry policy/additive signals for every bar.
        decision = _build_cached_analysis_decision(analysis_point=analysis_point)
        items.append({"dt": dt_key, "decision": decision})
    return {"items": items}


@router.get("/analysis/sell", response_model=None)
def get_sell_analysis_snapshot(
    code: str,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    row = repo.get_sell_analysis_snapshot(code, asof_dt)
    if not row:
        return {"item": None}
    return {
        "item": {
            "dt": row[0],
            "close": _to_float_or_none(row[1]),
            "dayChangePct": _to_float_or_none(row[2]),
            "pDown": _to_float_or_none(row[3]),
            "pTurnDown": _to_float_or_none(row[4]),
            "ev20Net": _to_float_or_none(row[5]),
            "rankDown20": _to_float_or_none(row[6]),
            "predDt": row[7],
            "pUp5": _to_float_or_none(row[8]),
            "pUp10": _to_float_or_none(row[9]),
            "pUp20": _to_float_or_none(row[10]),
            "shortScore": _to_float_or_none(row[11]),
            "aScore": _to_float_or_none(row[12]),
            "bScore": _to_float_or_none(row[13]),
            "ma20": _to_float_or_none(row[14]),
            "ma60": _to_float_or_none(row[15]),
            "ma20Slope": _to_float_or_none(row[16]),
            "ma60Slope": _to_float_or_none(row[17]),
            "distMa20Signed": _to_float_or_none(row[18]),
            "distMa60Signed": _to_float_or_none(row[19]),
            "trendDown": bool(row[20]) if row[20] is not None else None,
            "trendDownStrict": bool(row[21]) if row[21] is not None else None,
            "fwdClose5": _to_float_or_none(row[22]),
            "fwdClose10": _to_float_or_none(row[23]),
            "fwdClose20": _to_float_or_none(row[24]),
            "shortRet5": _to_float_or_none(row[25]),
            "shortRet10": _to_float_or_none(row[26]),
            "shortRet20": _to_float_or_none(row[27]),
            "shortWin5": bool(row[28]) if row[28] is not None else None,
            "shortWin10": bool(row[29]) if row[29] is not None else None,
            "shortWin20": bool(row[30]) if row[30] is not None else None,
        }
    }
