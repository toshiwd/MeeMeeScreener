from __future__ import annotations

from typing import Any, Iterable


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def _to_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _as_str_tuple(values: Iterable[Any] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    items: list[str] = []
    for value in values:
        text = _to_text(value)
        if text:
            items.append(text)
    return tuple(items)


def build_candidate_comparison_payloads(
    scenarios: Iterable[dict[str, Any]] | None,
    *,
    comparison_scope: str,
    baseline_key: str | None = None,
) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    if scenarios is None:
        return ()
    scenario_list = list(scenarios)
    if not scenario_list:
        return ()
    selected_score = _to_float(next((item.get("score") for item in scenario_list if bool(item.get("selected"))), None))
    if selected_score is None:
        selected_score = _to_float(scenario_list[0].get("score"))
    for index, scenario in enumerate(scenario_list, start=1):
        score = _to_float(scenario.get("score"))
        reasons: list[str] = []
        if scenario.get("key") is not None:
            reasons.append(f"key={_to_text(scenario.get('key'))}")
        if scenario.get("label") is not None:
            reasons.append(f"label={_to_text(scenario.get('label'))}")
        if scenario.get("tone") is not None:
            reasons.append(f"tone={_to_text(scenario.get('tone'))}")
        # Unknown fields are intentionally dropped here so the typed boundary stays stable.
        rows.append(
            {
                "candidate_key": _to_text(scenario.get("key"), fallback=f"candidate_{index}"),
                "baseline_key": _to_text(baseline_key) or None,
                "comparison_scope": _to_text(comparison_scope, fallback="decision_scenarios"),
                "score": score,
                "score_delta": (score - selected_score) if score is not None and selected_score is not None else None,
                "rank": index,
                "reasons": reasons,
                "publish_ready": bool(scenario.get("publish_ready")) if scenario.get("publish_ready") is not None else None,
            }
        )
    return tuple(rows)


def normalize_candidate_comparison_payloads(
    payloads: Iterable[Any] | None,
    *,
    default_scope: str = "external_result",
) -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []
    if payloads is None:
        return ()
    for item in payloads:
        payload = item if isinstance(item, dict) else {}
        # Unknown fields are intentionally dropped here so the typed boundary stays stable.
        rows.append(
            {
                "candidate_key": _to_text(payload.get("candidate_key") or payload.get("key"), fallback="candidate"),
                "baseline_key": _to_text(payload.get("baseline_key")) or None,
                "comparison_scope": _to_text(payload.get("comparison_scope"), fallback=default_scope),
                "score": _to_float(payload.get("score")),
                "score_delta": _to_float(payload.get("score_delta")),
                "rank": int(float(payload.get("rank"))) if payload.get("rank") is not None else None,
                "reasons": list(_as_str_tuple(payload.get("reasons"))),
                "publish_ready": bool(payload.get("publish_ready")) if payload.get("publish_ready") is not None else None,
            }
        )
    return tuple(rows)


def normalize_publish_readiness_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
        value = value.to_dict()
    if not isinstance(value, dict):
        return None
    return {
        "ready": bool(value.get("ready")),
        "status": _to_text(value.get("status"), fallback="unknown"),
        "reasons": list(_as_str_tuple(value.get("reasons"))),
        "candidate_key": _to_text(value.get("candidate_key")) or None,
        "approved": value.get("approved") if value.get("approved") is not None else None,
    }


def normalize_override_state_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
        value = value.to_dict()
    if not isinstance(value, dict):
        return None
    return {
        "present": bool(value.get("present")),
        "source": _to_text(value.get("source")) or None,
        "logic_key": _to_text(value.get("logic_key")) or None,
        "logic_version": _to_text(value.get("logic_version")) or None,
        "reason": _to_text(value.get("reason")) or None,
    }
