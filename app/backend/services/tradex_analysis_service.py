from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from copy import deepcopy
from threading import Lock
from time import perf_counter, time
from typing import Any

from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.services.analysis_bridge.contracts import LATEST_POINTER_NAME
from external_analysis.contracts.analysis_input import AnalysisInputContract
from external_analysis.contracts.paths import resolve_result_db_path
from external_analysis.results.result_schema import connect_result_db
from external_analysis.runtime.orchestrator import run_tradex_analysis

_logger = logging.getLogger(__name__)
_TRADEX_DETAIL_ANALYSIS_FLAG = "MEEMEE_ENABLE_TRADEX_DETAIL_ANALYSIS"
try:
    _TRADEX_DETAIL_ANALYSIS_CACHE_TTL_SEC = max(
        30.0,
        min(60.0, float(os.getenv("MEEMEE_TRADEX_DETAIL_ANALYSIS_CACHE_TTL_SEC", "45"))),
    )
except (TypeError, ValueError):
    _TRADEX_DETAIL_ANALYSIS_CACHE_TTL_SEC = 45.0

_CACHE_LOCK = Lock()
_CACHE: dict[tuple[str, int | None], tuple[float, dict[str, Any]]] = {}
_OBSERVABILITY_LOCK = Lock()
_OBSERVABILITY: dict[str, Any] = {
    "success_count": 0,
    "failure_count": 0,
    "cache_hit_count": 0,
    "cache_miss_count": 0,
    "unavailable_reason_counts": {},
    "latency_ms_last": None,
    "latency_ms_avg": None,
    "latency_ms_max": None,
    "last_reason": None,
    "last_reason_at": None,
}


def is_tradex_detail_analysis_enabled(flag: str | None = None) -> bool:
    raw = str(flag if flag is not None else os.getenv(_TRADEX_DETAIL_ANALYSIS_FLAG, "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def _format_asof_label(value: Any) -> str:
    if value is None:
        return "unknown"
    text = str(value).strip()
    if not text:
        return "unknown"
    if text.isdigit() and len(text) == 8:
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) >= 10 and text[4] in {"-", "/"}:
        return text.replace("/", "-")[:10]
    return text


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


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_schema) = 'main' AND lower(table_name) = lower(?)
        LIMIT 1
        """,
        [str(table_name)],
    ).fetchone()
    return bool(row)


def _normalize_reason(reason: Any) -> str | None:
    text = str(reason or "").strip()
    return text or None


def _json_load(value: Any, default: Any) -> Any:
    if value is None:
        return default
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return parsed if parsed is not None else default


def _get_cache_key(code: str, asof_dt: int | None) -> tuple[str, int | None]:
    return (str(code).strip(), asof_dt)


def _get_cached_snapshot(cache_key: tuple[str, int | None]) -> dict[str, Any] | None:
    now = time()
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            del _CACHE[cache_key]
            return None
        return deepcopy(payload)


def _store_cached_snapshot(cache_key: tuple[str, int | None], payload: dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _CACHE[cache_key] = (time() + float(_TRADEX_DETAIL_ANALYSIS_CACHE_TTL_SEC), deepcopy(payload))


def _record_observation(*, available: bool, reason: str | None, latency_ms: float, cache_hit: bool) -> None:
    normalized_reason = _normalize_reason(reason)
    with _OBSERVABILITY_LOCK:
        if cache_hit:
            _OBSERVABILITY["cache_hit_count"] = int(_OBSERVABILITY.get("cache_hit_count") or 0) + 1
        else:
            _OBSERVABILITY["cache_miss_count"] = int(_OBSERVABILITY.get("cache_miss_count") or 0) + 1
        if available:
            _OBSERVABILITY["success_count"] = int(_OBSERVABILITY.get("success_count") or 0) + 1
        else:
            _OBSERVABILITY["failure_count"] = int(_OBSERVABILITY.get("failure_count") or 0) + 1
            if normalized_reason:
                reason_counts = _OBSERVABILITY.setdefault("unavailable_reason_counts", {})
                if not isinstance(reason_counts, dict):
                    reason_counts = {}
                    _OBSERVABILITY["unavailable_reason_counts"] = reason_counts
                reason_counts[normalized_reason] = int(reason_counts.get(normalized_reason) or 0) + 1
                _OBSERVABILITY["last_reason"] = normalized_reason
                _OBSERVABILITY["last_reason_at"] = datetime.now(timezone.utc).isoformat()
        prev_max = _OBSERVABILITY.get("latency_ms_max")
        prev_total = float(_OBSERVABILITY.get("latency_ms_total") or 0.0)
        prev_count = int(_OBSERVABILITY.get("latency_count") or 0)
        _OBSERVABILITY["latency_ms_last"] = round(float(latency_ms), 3)
        _OBSERVABILITY["latency_count"] = prev_count + 1
        _OBSERVABILITY["latency_ms_total"] = prev_total + float(latency_ms)
        _OBSERVABILITY["latency_ms_avg"] = round((_OBSERVABILITY["latency_ms_total"] / _OBSERVABILITY["latency_count"]), 3)
        _OBSERVABILITY["latency_ms_max"] = round(max(float(prev_max or 0.0), float(latency_ms)), 3)


def get_tradex_detail_analysis_observability() -> dict[str, Any]:
    with _OBSERVABILITY_LOCK:
        reason_counts = _OBSERVABILITY.get("unavailable_reason_counts")
        if isinstance(reason_counts, dict):
            reason_counts_payload = dict(reason_counts)
        else:
            reason_counts_payload = {}
        return {
            "success_count": int(_OBSERVABILITY.get("success_count") or 0),
            "failure_count": int(_OBSERVABILITY.get("failure_count") or 0),
            "cache_hit_count": int(_OBSERVABILITY.get("cache_hit_count") or 0),
            "cache_miss_count": int(_OBSERVABILITY.get("cache_miss_count") or 0),
            "unavailable_reason_counts": reason_counts_payload,
            "latency_ms_last": _OBSERVABILITY.get("latency_ms_last"),
            "latency_ms_avg": _OBSERVABILITY.get("latency_ms_avg"),
            "latency_ms_max": _OBSERVABILITY.get("latency_ms_max"),
            "last_reason": _OBSERVABILITY.get("last_reason"),
            "last_reason_at": _OBSERVABILITY.get("last_reason_at"),
            "cache_ttl_sec": float(_TRADEX_DETAIL_ANALYSIS_CACHE_TTL_SEC),
        }


def reset_tradex_detail_analysis_observability() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
    with _OBSERVABILITY_LOCK:
        _OBSERVABILITY.clear()
        _OBSERVABILITY.update(
            {
                "success_count": 0,
                "failure_count": 0,
                "cache_hit_count": 0,
                "cache_miss_count": 0,
                "unavailable_reason_counts": {},
                "latency_ms_last": None,
                "latency_ms_avg": None,
                "latency_ms_max": None,
                "last_reason": None,
                "last_reason_at": None,
                "latency_ms_total": 0.0,
                "latency_count": 0,
            }
        )


def _build_tradex_detail_analysis_snapshot_uncached(
    *,
    code: str,
    asof_dt: int | None,
    repo: StockRepository,
    enabled: bool | None = None,
) -> tuple[dict[str, Any], bool]:
    if not str(code or "").strip():
        return ({"available": False, "reason": "code required", "analysis": None}, False)

    if enabled is None:
        enabled = is_tradex_detail_analysis_enabled()
    if not enabled:
        return ({"available": False, "reason": "feature flag disabled", "analysis": None}, False)

    row = repo.get_ml_analysis_pred(code, asof_dt)
    if not row:
        return ({"available": False, "reason": "analysis unavailable", "analysis": None}, True)

    sell_row = None
    try:
        sell_row = repo.get_sell_analysis_snapshot(code, asof_dt)
    except Exception:
        sell_row = None

    input_contract = AnalysisInputContract(
        symbol=str(code),
        asof=_format_asof_label(row[0] if len(row) > 0 else asof_dt),
        analysis_p_up=_to_float_or_none(row[1]) if len(row) > 1 else None,
        analysis_p_down=_to_float_or_none(row[2]) if len(row) > 2 else None,
        analysis_p_turn_up=_to_float_or_none(row[5]) if len(row) > 5 else None,
        analysis_p_turn_down=_to_float_or_none(row[6]) if len(row) > 6 else None,
        analysis_ev_net=_to_float_or_none(row[14]) if len(row) > 14 else None,
        sell_analysis=_build_sell_context_from_row(sell_row),
    )
    output = run_tradex_analysis(input_contract)
    return ({"available": True, "reason": None, "analysis": output.to_dict()}, True)


def _build_forecast_surface_detail_snapshot(
    *,
    code: str,
    asof_dt: int | None,
) -> dict[str, Any]:
    if not str(code or "").strip():
        return {"available": False, "reason": "code required", "rows": []}
    db_path = resolve_result_db_path()
    if not db_path.exists():
        return {"available": False, "reason": "result db missing", "rows": []}
    conn = connect_result_db(str(db_path), read_only=True)
    try:
        if not _table_exists(conn, "publish_pointer") or not _table_exists(conn, "forecast_surface_daily"):
            return {"available": False, "reason": "forecast surface unavailable", "rows": []}
        pointer_row = conn.execute(
            """
            SELECT publish_id, CAST(as_of_date AS VARCHAR), freshness_state
            FROM publish_pointer
            WHERE pointer_name = ?
            LIMIT 1
            """,
            [LATEST_POINTER_NAME],
        ).fetchone()
        if not pointer_row:
            return {"available": False, "reason": "forecast surface unavailable", "rows": []}
        publish_id = str(pointer_row[0])
        rows = conn.execute(
            """
            SELECT
                publish_id, CAST(as_of_date AS VARCHAR), code, side, action_state, direction_prob,
                expected_ret_5, expected_ret_10, expected_ret_20, expected_mfe_20, expected_mae_20,
                invalidation_price, setup_tags, reason_codes, opportunity_score, freshness_state, CAST(created_at AS VARCHAR)
            FROM forecast_surface_daily
            WHERE publish_id = ? AND code = ?
            ORDER BY CASE WHEN side = 'long' THEN 0 ELSE 1 END, side ASC
            """,
            [publish_id, code],
        ).fetchall()
    except Exception:
        return {"available": False, "reason": "forecast surface unavailable", "rows": []}
    finally:
        conn.close()
    if not rows:
        return {"available": False, "reason": "forecast surface unavailable", "rows": []}
    columns = (
        "publish_id",
        "as_of_date",
        "code",
        "side",
        "action_state",
        "direction_prob",
        "expected_ret_5",
        "expected_ret_10",
        "expected_ret_20",
        "expected_mfe_20",
        "expected_mae_20",
        "invalidation_price",
        "setup_tags",
        "reason_codes",
        "opportunity_score",
        "freshness_state",
        "created_at",
    )
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        item = dict(zip(columns, row, strict=True))
        item["direction_prob"] = _to_float_or_none(item.get("direction_prob"))
        item["expected_ret_5"] = _to_float_or_none(item.get("expected_ret_5"))
        item["expected_ret_10"] = _to_float_or_none(item.get("expected_ret_10"))
        item["expected_ret_20"] = _to_float_or_none(item.get("expected_ret_20"))
        item["expected_mfe_20"] = _to_float_or_none(item.get("expected_mfe_20"))
        item["expected_mae_20"] = _to_float_or_none(item.get("expected_mae_20"))
        item["invalidation_price"] = _to_float_or_none(item.get("invalidation_price"))
        item["setup_tags"] = _json_load(item.get("setup_tags"), [])
        item["reason_codes"] = _json_load(item.get("reason_codes"), [])
        item["opportunity_score"] = _to_float_or_none(item.get("opportunity_score"))
        side = str(item.get("side") or "").strip().lower()
        expected_mfe_20 = _to_float_or_none(item.get("expected_mfe_20"))
        expected_mae_20 = _to_float_or_none(item.get("expected_mae_20"))
        if side == "short":
            item["expected_upside"] = None if expected_mae_20 is None else max(float(expected_mae_20), 0.0)
            item["expected_downside"] = None if expected_mfe_20 is None else max(float(-expected_mfe_20), 0.0)
        else:
            item["expected_upside"] = None if expected_mfe_20 is None else max(float(expected_mfe_20), 0.0)
            item["expected_downside"] = None if expected_mae_20 is None else max(float(-expected_mae_20), 0.0)
        normalized_rows.append(item)
    return {
        "available": True,
        "reason": None,
        "publish_id": str(pointer_row[0]),
        "as_of_date": str(pointer_row[1]) if pointer_row[1] is not None else None,
        "freshness_state": str(pointer_row[2]) if pointer_row[2] is not None else None,
        "rows": normalized_rows,
    }


def build_tradex_detail_analysis_snapshot(
    *,
    code: str,
    asof_dt: int | None,
    repo: StockRepository,
    enabled: bool | None = None,
) -> dict[str, Any]:
    started_at = perf_counter()
    cache_key = _get_cache_key(code, asof_dt)

    if enabled is None:
        enabled = is_tradex_detail_analysis_enabled()

    if not enabled or not str(code or "").strip():
        result, cacheable = _build_tradex_detail_analysis_snapshot_uncached(
            code=code,
            asof_dt=asof_dt,
            repo=repo,
            enabled=enabled,
        )
        _record_observation(
            available=bool(result.get("available")),
            reason=result.get("reason"),
            latency_ms=(perf_counter() - started_at) * 1000.0,
            cache_hit=False,
        )
        return result

    cached = _get_cached_snapshot(cache_key)
    if cached is not None:
        _record_observation(
            available=bool(cached.get("available")),
            reason=cached.get("reason"),
            latency_ms=(perf_counter() - started_at) * 1000.0,
            cache_hit=True,
        )
        return cached

    try:
        result, cacheable = _build_tradex_detail_analysis_snapshot_uncached(
            code=code,
            asof_dt=asof_dt,
            repo=repo,
            enabled=enabled,
        )
        if bool(result.get("available")):
            forecast_surface = _build_forecast_surface_detail_snapshot(code=code, asof_dt=asof_dt)
            if not bool(forecast_surface.get("available")):
                analysis = result.get("analysis") if isinstance(result.get("analysis"), dict) else {}
                forecast_surface["fallback"] = {
                    "candidate_comparisons": list(analysis.get("candidate_comparisons") or []),
                    "publish_readiness": dict(analysis.get("publish_readiness") or {})
                    if isinstance(analysis.get("publish_readiness"), dict)
                    else None,
                }
            result["forecast_surface"] = forecast_surface
            analysis = result.get("analysis")
            if isinstance(analysis, dict):
                try:
                    from app.backend.services.tradex_research_bridge_service import get_internal_state_eval_promotion_review

                    promotion_review = get_internal_state_eval_promotion_review()
                    review = promotion_review.get("review") if isinstance(promotion_review, dict) else None
                    analysis["promotion_review"] = review if isinstance(review, dict) else None
                except Exception:
                    analysis["promotion_review"] = None
    except Exception as exc:
        _logger.warning("tradex detail analysis unavailable code=%s reason=%s", code, exc)
        result = {"available": False, "reason": "analysis unavailable", "analysis": None}
        cacheable = False

    if cacheable:
        _store_cached_snapshot(cache_key, result)

    _record_observation(
        available=bool(result.get("available")),
        reason=result.get("reason"),
        latency_ms=(perf_counter() - started_at) * 1000.0,
        cache_hit=False,
    )
    return result
