from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


CONTRACT_VERSION = "meemee_data_freshness_v1"
RANKING_SOURCE_PATH = "app/backend/services/ml/rankings_cache.py"
CHART_SOURCE_PATH = "app/backend/api/routers/bars.py:batch_bars_v3"

_TIMEFRAMES = ("daily", "weekly", "monthly")
_CLASSIFICATION_ORDER = {
    "confirmed": 0,
    "provisional": 1,
    "mixed": 2,
    "research-only": 3,
}
_STATUS_ORDER = {
    "ready": 0,
    "empty": 1,
    "missing": 2,
    "loading": 3,
    "error": 4,
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _date_key_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    try:
        raw = int(value)
    except (TypeError, ValueError):
        text = _normalize_text(value)
        if text and len(text) == 10 and text[4] == "-" and text[7] == "-":
            return text
        return None
    text = str(raw)
    if len(text) == 8:
        try:
            return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
        except Exception:
            return None
    if len(text) == 6:
        try:
            return f"{text[:4]}-{text[4:6]}-01"
        except Exception:
            return None
    if raw >= 1_000_000_000:
        try:
            return datetime.fromtimestamp(raw / 1000 if raw >= 1_000_000_000_000 else raw, tz=timezone.utc).date().isoformat()
        except Exception:
            return None
    return None


def _max_iso_date(values: list[str | None]) -> str | None:
    candidates = [value for value in values if value]
    return max(candidates) if candidates else None


def _normalize_freshness_state(value: Any, *, has_data: bool, error: bool = False) -> str:
    if error:
        return "error"
    text = str(value or "").strip().lower()
    if text in {"fresh", "exact"}:
        return "fresh"
    if text in {"stale", "lagged", "stale_blocking", "partial", "incomplete"}:
        return "stale"
    if not has_data:
        return "missing"
    return "fresh"


def _normalize_classification(value: Any, *, has_data: bool) -> str:
    text = str(value or "").strip().lower()
    if text in _CLASSIFICATION_ORDER:
        return text
    return "confirmed" if has_data else "missing"


def _worst_classification(values: list[str]) -> str:
    usable = [value for value in values if value in _CLASSIFICATION_ORDER]
    if not usable:
        return "missing"
    return max(usable, key=lambda item: _CLASSIFICATION_ORDER[item])


def _worst_status(values: list[str]) -> str:
    usable = [value for value in values if value in _STATUS_ORDER]
    if not usable:
        return "missing"
    return max(usable, key=lambda item: _STATUS_ORDER[item])


def empty_data_freshness_contract(*, generated_at: str | None = None) -> dict[str, Any]:
    chart_empty = {
        "source": None,
        "source_path_or_adapter": None,
        "right_edge_date": None,
        "freshness_state": "missing",
        "classification": "missing",
        "status": "missing",
    }
    return {
        "contract_version": CONTRACT_VERSION,
        "generated_at": generated_at or _utc_now_iso(),
        "ranking": {
            "snapshot_as_of": None,
            "snapshot_id": None,
            "freshness_state": "missing",
            "source": None,
            "source_path_or_adapter": None,
            "classification": "missing",
            "status": "missing",
        },
        "detail": {
            "source": None,
            "source_path_or_adapter": None,
            "classification": "missing",
            "status": "missing",
        },
        "charts": {timeframe: dict(chart_empty) for timeframe in _TIMEFRAMES},
        "research": {
            "normal_ui_exposure_allowed": False,
            "classification": "research-only",
        },
    }


def build_ranking_data_freshness_contract(
    payload: dict[str, Any],
    *,
    source: str,
    source_path_or_adapter: str = RANKING_SOURCE_PATH,
    generated_at: str | None = None,
) -> dict[str, Any]:
    contract = empty_data_freshness_contract(generated_at=generated_at)
    snapshot_as_of = (
        _normalize_text(payload.get("snapshot_as_of"))
        or _normalize_text(payload.get("confirmed_snapshot_as_of"))
        or _normalize_text(payload.get("provisional_snapshot_as_of"))
    )
    errors = payload.get("errors")
    has_error = bool(errors) if isinstance(errors, list) else bool(payload.get("error"))
    has_data = bool(snapshot_as_of)
    tf = _normalize_text(payload.get("tf")) or _normalize_text(payload.get("timeframe"))
    which = _normalize_text(payload.get("which"))
    direction = _normalize_text(payload.get("dir"))
    mode = _normalize_text(payload.get("mode"))
    risk_mode = _normalize_text(payload.get("risk_mode"))
    identity_parts = [
        "ranking",
        tf,
        which,
        direction,
        mode,
        risk_mode,
        snapshot_as_of,
    ]
    snapshot_id = ":".join(part for part in identity_parts if part)
    contract["ranking"] = {
        "snapshot_as_of": snapshot_as_of,
        "snapshot_id": snapshot_id or None,
        "freshness_state": _normalize_freshness_state(
            payload.get("freshness_state"),
            has_data=has_data,
            error=has_error,
        ),
        "source": source,
        "source_path_or_adapter": source_path_or_adapter,
        "classification": "confirmed" if has_data else "missing",
        "status": "error" if has_error else ("ready" if has_data else "missing"),
    }
    return contract


def _chart_frame_contract(timeframe: str, frame_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(frame_payload, dict):
        return empty_data_freshness_contract()["charts"][timeframe]
    bars = frame_payload.get("bars")
    has_data = bool(isinstance(bars, list) and bars)
    provenance = frame_payload.get("provenance")
    provenance = provenance if isinstance(provenance, dict) else {}
    right_edge = _date_key_to_iso(
        provenance.get("chart_requested_date")
        or provenance.get("chart_last_provisional_date")
        or provenance.get("chart_last_confirmed_date")
    )
    classification = _normalize_classification(
        provenance.get("display_basis_classification")
        or provenance.get("chart_data_classification")
        or provenance.get("chart_source_type"),
        has_data=has_data,
    )
    return {
        "source": provenance.get("chart_source_provider"),
        "source_path_or_adapter": provenance.get("chart_source_path_or_identifier") or CHART_SOURCE_PATH,
        "right_edge_date": right_edge,
        "freshness_state": _normalize_freshness_state(
            provenance.get("chart_source_freshness_status"),
            has_data=has_data,
        ),
        "classification": classification,
        "status": "ready" if has_data else "empty",
    }


def build_chart_data_freshness_contract(
    *,
    items: dict[str, Any],
    requested_timeframes: list[str] | tuple[str, ...] | set[str] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    contract = empty_data_freshness_contract(generated_at=generated_at)
    requested = set(_TIMEFRAMES if requested_timeframes is None else requested_timeframes)
    per_timeframe: dict[str, list[dict[str, Any]]] = {timeframe: [] for timeframe in _TIMEFRAMES}
    symbol_count = 0
    for code_payload in items.values():
        if not isinstance(code_payload, dict):
            continue
        symbol_count += 1
        for timeframe in _TIMEFRAMES:
            if timeframe not in requested:
                continue
            per_timeframe[timeframe].append(_chart_frame_contract(timeframe, code_payload.get(timeframe)))

    chart_statuses: list[str] = []
    chart_classes: list[str] = []
    for timeframe in _TIMEFRAMES:
        frames = per_timeframe[timeframe]
        if not frames:
            continue
        status = _worst_status([str(frame.get("status") or "missing") for frame in frames])
        classification = _worst_classification([str(frame.get("classification") or "missing") for frame in frames])
        freshness = _normalize_freshness_state(
            "stale" if any(frame.get("freshness_state") == "stale" for frame in frames) else "fresh",
            has_data=any(frame.get("status") == "ready" for frame in frames),
            error=any(frame.get("freshness_state") == "error" for frame in frames),
        )
        source = next((frame.get("source") for frame in frames if frame.get("source")), None)
        source_path = next((frame.get("source_path_or_adapter") for frame in frames if frame.get("source_path_or_adapter")), CHART_SOURCE_PATH)
        contract["charts"][timeframe] = {
            "source": source,
            "source_path_or_adapter": source_path,
            "right_edge_date": _max_iso_date([frame.get("right_edge_date") for frame in frames]),
            "freshness_state": freshness,
            "classification": classification,
            "status": status,
        }
        chart_statuses.append(status)
        chart_classes.append(classification)

    contract["detail"] = {
        "source": "batch_bars_v3",
        "source_path_or_adapter": CHART_SOURCE_PATH,
        "classification": _worst_classification(chart_classes),
        "status": _worst_status(chart_statuses) if symbol_count else "missing",
    }
    return contract
