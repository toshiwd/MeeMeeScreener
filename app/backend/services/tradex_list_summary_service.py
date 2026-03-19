from __future__ import annotations

import os
from copy import deepcopy
from datetime import datetime
from threading import Lock
from time import time
from typing import Any, Iterable

from app.backend.infra.duckdb.stock_repo import StockRepository

from app.backend.services.tradex_analysis_service import (
    build_tradex_detail_analysis_snapshot,
    is_tradex_detail_analysis_enabled,
)

_LIST_SUMMARY_FLAG = "MEEMEE_ENABLE_TRADEX_LIST_SUMMARY"
try:
    _LIST_SUMMARY_CACHE_TTL_SEC = max(
        30.0,
        min(60.0, float(os.getenv("MEEMEE_TRADEX_LIST_SUMMARY_CACHE_TTL_SEC", "45"))),
    )
except (TypeError, ValueError):
    _LIST_SUMMARY_CACHE_TTL_SEC = 45.0

_ITEM_CACHE_LOCK = Lock()
_ITEM_CACHE: dict[tuple[str, int | None], tuple[float, dict[str, Any]]] = {}


def is_tradex_list_summary_enabled(flag: str | None = None, detail_flag: str | None = None) -> bool:
    raw = str(flag if flag is not None else os.getenv(_LIST_SUMMARY_FLAG, "")).strip().lower()
    if raw not in {"1", "true", "yes", "on"}:
        return False
    return is_tradex_detail_analysis_enabled(detail_flag)


def _normalize_code(value: Any) -> str:
    return str(value or "").strip()


def _parse_asof_dt(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        parsed = int(value)
        return parsed if parsed > 0 else None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        parsed = int(text)
        return parsed if parsed > 0 else None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            candidate = text[:10] if fmt in {"%Y-%m-%d", "%Y/%m/%d"} else text[:8]
            dt = datetime.strptime(candidate, fmt)
            return int(dt.strftime("%Y%m%d"))
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int(dt.strftime("%Y%m%d"))
    except ValueError:
        return None


def _format_asof_dt(value: int | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text or None


def _normalize_request_items(items: Iterable[dict[str, Any]] | None) -> tuple[tuple[str, int | None], ...]:
    normalized: list[tuple[str, int | None]] = []
    seen: set[tuple[str, int | None]] = set()
    if items is None:
        return ()
    for item in items:
        if not isinstance(item, dict):
            continue
        code = _normalize_code(item.get("code") or item.get("symbol"))
        if not code:
            continue
        asof_dt = _parse_asof_dt(item.get("asof"))
        key = (code, asof_dt)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return tuple(normalized)


def _cache_key(code: str, asof_dt: int | None) -> tuple[str, int | None]:
    return (code, asof_dt)


def _get_cached_item(cache_key: tuple[str, int | None]) -> dict[str, Any] | None:
    now = time()
    with _ITEM_CACHE_LOCK:
        cached = _ITEM_CACHE.get(cache_key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            del _ITEM_CACHE[cache_key]
            return None
        return deepcopy(payload)


def _store_cached_item(cache_key: tuple[str, int | None], payload: dict[str, Any]) -> None:
    with _ITEM_CACHE_LOCK:
        _ITEM_CACHE[cache_key] = (time() + float(_LIST_SUMMARY_CACHE_TTL_SEC), deepcopy(payload))


def _resolve_dominant_tone(side_ratios: dict[str, Any] | None) -> str | None:
    if not isinstance(side_ratios, dict):
        return None
    try:
        buy = float(side_ratios.get("buy") or 0.0)
        neutral = float(side_ratios.get("neutral") or 0.0)
        sell = float(side_ratios.get("sell") or 0.0)
    except (TypeError, ValueError):
        return None
    if buy >= neutral and buy > sell:
        return "buy"
    if sell > buy and sell >= neutral:
        return "sell"
    return "neutral"


def _build_list_summary_item(
    *,
    code: str,
    asof_dt: int | None,
    detail_result: dict[str, Any],
) -> dict[str, Any]:
    available = bool(detail_result.get("available"))
    reason = detail_result.get("reason")
    analysis = detail_result.get("analysis") if isinstance(detail_result.get("analysis"), dict) else None
    if not available or not analysis:
        return {
            "code": code,
            "asof": _format_asof_dt(asof_dt),
            "available": False,
            "reason": str(reason or "analysis unavailable"),
            "dominant_tone": None,
            "confidence": None,
            "publish_readiness": None,
            "reasons": [],
        }

    reasons = analysis.get("reasons")
    if isinstance(reasons, list):
        reasons = [str(item).strip() for item in reasons if str(item).strip()][:2]
    else:
        reasons = []
    publish_readiness = analysis.get("publishReadiness") or analysis.get("publish_readiness")
    if not isinstance(publish_readiness, dict):
        publish_readiness = None
    return {
        "code": str(analysis.get("symbol") or code),
        "asof": str(analysis.get("asof") or _format_asof_dt(asof_dt) or "unknown"),
        "available": True,
        "reason": None,
        "dominant_tone": _resolve_dominant_tone(analysis.get("sideRatios") or analysis.get("side_ratios")),
        "confidence": analysis.get("confidence"),
        "publish_readiness": publish_readiness,
        "reasons": reasons,
    }


def build_tradex_list_summary_snapshot(
    *,
    items: Iterable[dict[str, Any]] | None,
    repo: StockRepository,
    enabled: bool | None = None,
    scope: str | None = None,
) -> dict[str, Any]:
    normalized_items = _normalize_request_items(items)
    if enabled is None:
        enabled = is_tradex_list_summary_enabled()
    if not enabled:
        return {"available": False, "reason": "feature flag disabled", "scope": scope or "list", "items": []}
    if not normalized_items:
        return {"available": False, "reason": "items required", "scope": scope or "list", "items": []}

    summary_items: list[dict[str, Any]] = []
    available_count = 0
    for code, asof_dt in normalized_items:
        cache_key = _cache_key(code, asof_dt)
        cached = _get_cached_item(cache_key)
        if cached is not None:
            summary_items.append(cached)
            if cached.get("available"):
                available_count += 1
            continue

        detail_result = build_tradex_detail_analysis_snapshot(
            code=code,
            asof_dt=asof_dt,
            repo=repo,
            enabled=enabled,
        )
        summary_item = _build_list_summary_item(code=code, asof_dt=asof_dt, detail_result=detail_result)
        summary_items.append(summary_item)
        if summary_item.get("available"):
            available_count += 1
        if summary_item.get("available") or summary_item.get("reason") in {"analysis unavailable"}:
            _store_cached_item(cache_key, summary_item)

    top_reason = None
    if not available_count:
        for item in summary_items:
            reason = item.get("reason")
            if reason:
                top_reason = str(reason)
                break

    return {
        "available": available_count > 0,
        "reason": top_reason,
        "scope": scope or "list",
        "items": summary_items,
    }


def reset_tradex_list_summary_cache() -> None:
    with _ITEM_CACHE_LOCK:
        _ITEM_CACHE.clear()
