from __future__ import annotations

import os
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from time import time
from typing import Any, Iterable

from app.backend.infra.duckdb.stock_repo import StockRepository

from app.backend.services.tradex_analysis_service import (
    build_tradex_detail_analysis_snapshot,
    is_tradex_detail_analysis_enabled,
)

_LIST_SUMMARY_FLAG = "MEEMEE_ENABLE_TRADEX_LIST_SUMMARY"
_SHORT_LIFECYCLE_OVERLAY_FLAG = "MEEMEE_ENABLE_TRADEX_SHORT_LIFECYCLE_OVERLAY"
_SHORT_LIFECYCLE_ROOT = Path(
    os.getenv("TRADEX_SHORT_LIFECYCLE_BOARD_ROOT", r"G:\Tradex\current_short_lifecycle_rank_board_v1")
)
_SHORT_LIFECYCLE_MAX_AGE_DAYS = 7
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
    lifecycle_raw = str(os.getenv(_SHORT_LIFECYCLE_OVERLAY_FLAG, "")).strip().lower()
    return (
        raw in {"1", "true", "yes", "on"} and is_tradex_detail_analysis_enabled(detail_flag)
    ) or lifecycle_raw in {"1", "true", "yes", "on"}


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


def _latest_short_lifecycle_board_path() -> Path | None:
    if not _SHORT_LIFECYCLE_ROOT.exists() or not _SHORT_LIFECYCLE_ROOT.is_dir():
        return None
    candidates = [
        directory / "current_short_lifecycle_rank_board.json"
        for directory in _SHORT_LIFECYCLE_ROOT.iterdir()
        if directory.is_dir() and (directory / "current_short_lifecycle_rank_board.json").exists()
    ]
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.parent.name)) if candidates else None


def _load_short_lifecycle_by_code() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    path = _latest_short_lifecycle_board_path()
    if path is None:
        return {}, {"available": False, "reason": "short_lifecycle_board_not_found"}
    age_days = max(0.0, (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400.0)
    if age_days > _SHORT_LIFECYCLE_MAX_AGE_DAYS:
        return {}, {
            "available": False,
            "reason": "short_lifecycle_board_stale",
            "artifact_path": str(path),
            "age_days": age_days,
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}, {"available": False, "reason": "short_lifecycle_board_read_failed", "artifact_path": str(path)}
    rows = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    by_code: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = _normalize_code(row.get("code"))
        if not code:
            continue
        current = by_code.get(code)
        row_rank = row.get("lifecycle_rank") if isinstance(row.get("lifecycle_rank"), int) else 999999
        current_rank = current.get("lifecycle_rank") if current and isinstance(current.get("lifecycle_rank"), int) else 999999
        if current is None or row_rank < current_rank:
            by_code[code] = row
    return by_code, {
        "available": True,
        "reason": None,
        "artifact_path": str(path),
        "run_id": payload.get("run_id"),
        "created_at": payload.get("created_at"),
        "authoritative_decision": payload.get("authoritative_decision"),
        "age_days": age_days,
    }


def _build_short_lifecycle_overlay(row: dict[str, Any] | None, board_meta: dict[str, Any]) -> dict[str, Any] | None:
    if not row or not board_meta.get("available"):
        return None
    return {
        "state": row.get("lifecycle_state"),
        "rank": row.get("lifecycle_rank"),
        "signal_ymd": row.get("signal_ymd"),
        "expected_downside_pct": row.get("expected_downside_pct"),
        "risk_reward_to_sl8": row.get("risk_reward_to_sl8"),
        "setup_state": row.get("setup_state"),
        "continuation_status": row.get("continuation_status"),
        "final_review_status": row.get("final_review_status"),
        "reasons": list(row.get("lifecycle_reasons") or [])[:3],
        "review_only": True,
        "artifact_created_at": board_meta.get("created_at"),
        "artifact_path": board_meta.get("artifact_path"),
    }


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
    short_lifecycle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    available = bool(detail_result.get("available"))
    reason = detail_result.get("reason")
    analysis = detail_result.get("analysis") if isinstance(detail_result.get("analysis"), dict) else None
    if not available or not analysis:
        return {
            "code": code,
            "asof": _format_asof_dt(asof_dt),
            "available": short_lifecycle is not None,
            "reason": None if short_lifecycle is not None else str(reason or "analysis unavailable"),
            "dominant_tone": None,
            "confidence": None,
            "publish_readiness": None,
            "reasons": [],
            "short_lifecycle": short_lifecycle,
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
        "short_lifecycle": short_lifecycle,
    }


def build_tradex_list_summary_snapshot(
    *,
    items: Iterable[dict[str, Any]] | None,
    repo: StockRepository,
    enabled: bool | None = None,
    detail_enabled: bool | None = None,
    scope: str | None = None,
) -> dict[str, Any]:
    normalized_items = _normalize_request_items(items)
    if enabled is None:
        enabled = is_tradex_list_summary_enabled()
    if not enabled:
        return {"available": False, "reason": "feature flag disabled", "scope": scope or "list", "items": []}
    if not normalized_items:
        return {"available": False, "reason": "items required", "scope": scope or "list", "items": []}
    if detail_enabled is None:
        detail_enabled = enabled
    lifecycle_by_code, lifecycle_board = _load_short_lifecycle_by_code()

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

        detail_result = (
            build_tradex_detail_analysis_snapshot(code=code, asof_dt=asof_dt, repo=repo, enabled=True)
            if detail_enabled
            else {"available": False, "reason": "analysis unavailable", "analysis": None}
        )
        summary_item = _build_list_summary_item(
            code=code,
            asof_dt=asof_dt,
            detail_result=detail_result,
            short_lifecycle=_build_short_lifecycle_overlay(lifecycle_by_code.get(code), lifecycle_board),
        )
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
        "short_lifecycle_board": lifecycle_board,
        "items": summary_items,
    }


def reset_tradex_list_summary_cache() -> None:
    with _ITEM_CACHE_LOCK:
        _ITEM_CACHE.clear()
