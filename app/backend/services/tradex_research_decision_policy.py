from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Final

from app.backend.services import tradex_research_os_contracts as os_contracts
from app.backend.services import tradex_research_os_store as os_store


TRADEX_RESEARCH_DECISION_POLICY_SCHEMA_VERSION: Final[str] = "tradex_research_decision_policy_v1"
TRADEX_RESEARCH_DECISION_POLICY_VERSION: Final[str] = "v1"
TRADEX_RESEARCH_DECISION_POLICY_FILE_NAME: Final[str] = "decision_policy_v1.json"


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value


def decision_policy_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    return root / "config" / "tradex" / TRADEX_RESEARCH_DECISION_POLICY_FILE_NAME


def load_decision_policy() -> dict[str, Any]:
    path = decision_policy_path()
    payload = os_store.read_json_object_strict(path, artifact_name="decision policy")
    if _text(payload.get("schema_version")) != TRADEX_RESEARCH_DECISION_POLICY_SCHEMA_VERSION:
        raise ValueError("decision policy schema_version mismatch")
    if _text(payload.get("decision_policy_version")) != TRADEX_RESEARCH_DECISION_POLICY_VERSION:
        raise ValueError("decision policy version mismatch")
    if not isinstance(payload.get("decision_order"), list) or not payload.get("decision_order"):
        raise ValueError("decision policy decision_order must be a non-empty list")
    if not isinstance(payload.get("signals"), dict):
        raise ValueError("decision policy signals must be an object")
    return payload


def _summary_field(summary_metrics: dict[str, Any], field_name: str, fallback: Any = None) -> Any:
    value = summary_metrics.get(field_name)
    if value is None:
        return fallback
    return value


def _blocking_reasons_from_context(
    *,
    policy: dict[str, Any],
    authoritative_context: dict[str, Any],
    provisional_decision: dict[str, Any],
    judge_input: dict[str, Any],
) -> list[str]:
    signals = policy.get("signals") if isinstance(policy.get("signals"), dict) else {}
    hard_blocker_codes = {str(item).strip() for item in signals.get("hard_blocker_reason_codes") or [] if str(item).strip()}
    blocking_reasons: list[str] = []
    for item in provisional_decision.get("blocking_unknowns") or []:
        reason = _text(item)
        if reason and reason in hard_blocker_codes:
            blocking_reasons.append(reason)
    topk_block_reason = _text(authoritative_context.get("topk_branching_block_reason"))
    if topk_block_reason and topk_block_reason in hard_blocker_codes:
        blocking_reasons.append(topk_block_reason)
    family_compare_state = _text(authoritative_context.get("family_compare_state"))
    if family_compare_state in {"missing", "ambiguous"}:
        blocking_reasons.append(f"family_compare_{family_compare_state}")
    if not _text(judge_input.get("experiment_id")):
        blocking_reasons.append("missing_experiment_id")
    return list(dict.fromkeys(blocking_reasons))


def _branching_exists(policy: dict[str, Any], judge_input: dict[str, Any], authoritative_context: dict[str, Any]) -> bool:
    signals = policy.get("signals") if isinstance(policy.get("signals"), dict) else {}
    min_changed = int(signals.get("branching_positive_changed_count_min") or 1)
    meaningful = bool(authoritative_context.get("meaningful_topk_branching_possible"))
    if meaningful:
        return True
    return any(
        int(judge_input.get(field) or 0) >= min_changed
        for field in ("changed_top5_members_count", "changed_top10_members_count", "changed_rank_count")
    )


def _compare_signal(authoritative_context: dict[str, Any]) -> str:
    family_decision = _text(authoritative_context.get("family_decision"))
    authoritative_rollup_decision = _text(authoritative_context.get("authoritative_rollup_decision"), fallback=family_decision)
    best_candidate_decision = _text(authoritative_context.get("best_candidate_decision"), fallback=authoritative_rollup_decision)
    candidate_local_decision = _text(authoritative_context.get("candidate_local_decision"), fallback=best_candidate_decision)
    decisions = {family_decision, authoritative_rollup_decision, best_candidate_decision, candidate_local_decision}
    if "keep" in decisions and "drop" in decisions:
        return "unclear"
    if "keep" in decisions:
        return "affirmative"
    if "drop" in decisions:
        return "negative"
    return "unclear"


def _missing_or_ambiguous_metrics(judge_input: dict[str, Any], authoritative_context: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    summary_metrics = judge_input.get("summary_metrics") if isinstance(judge_input.get("summary_metrics"), dict) else {}
    for field in ("family_decision", "authoritative_rollup_decision", "candidate_local_decision"):
        if not _text(summary_metrics.get(field) or authoritative_context.get(field)):
            missing.append(field)
    for field in ("changed_top5_members_count", "changed_top10_members_count", "changed_rank_count", "top5_boundary_score_gap", "top10_boundary_score_gap"):
        if field not in judge_input or judge_input.get(field) is None:
            missing.append(field)
    if not _text(judge_input.get("selection_divergence_reason")):
        missing.append("selection_divergence_reason")
    return list(dict.fromkeys(missing))


def build_authoritative_decision_inputs(
    *,
    judge_input: dict[str, Any],
    authoritative_context: dict[str, Any],
    provisional_decision: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    summary_metrics = judge_input.get("summary_metrics") if isinstance(judge_input.get("summary_metrics"), dict) else {}
    return {
        "policy": {
            "schema_version": _text(policy.get("schema_version")),
            "decision_policy_version": _text(policy.get("decision_policy_version")),
            "decision_order": list(policy.get("decision_order") or []),
        },
        "judge_input": {
            "experiment_id": _text(judge_input.get("experiment_id")),
            "comparison_scope": _json_ready(judge_input.get("comparison_scope") or {}),
            "changed_top5_members_count": int(judge_input.get("changed_top5_members_count") or 0),
            "changed_top10_members_count": int(judge_input.get("changed_top10_members_count") or 0),
            "changed_rank_count": int(judge_input.get("changed_rank_count") or 0),
            "top5_boundary_score_gap": float(judge_input.get("top5_boundary_score_gap") or 0.0),
            "top10_boundary_score_gap": float(judge_input.get("top10_boundary_score_gap") or 0.0),
            "selection_divergence_reason": _text(judge_input.get("selection_divergence_reason")),
            "available_sample_count": int(judge_input.get("available_sample_count") or 0),
            "available_session_count": int(judge_input.get("available_session_count") or 0),
            "summary_metrics": {
                "family_decision": _text(summary_metrics.get("family_decision")),
                "authoritative_rollup_decision": _text(summary_metrics.get("authoritative_rollup_decision")),
                "candidate_local_decision": _text(summary_metrics.get("candidate_local_decision")),
                "session_aggregate_decision": _text(summary_metrics.get("session_aggregate_decision")),
                "promote_ready": bool(summary_metrics.get("promote_ready")),
                "insufficient_samples": bool(summary_metrics.get("insufficient_samples")),
                "topk_branching_block_reason": _text(summary_metrics.get("topk_branching_block_reason")),
                "meaningful_topk_branching_possible": bool(summary_metrics.get("meaningful_topk_branching_possible")),
            },
        },
        "authoritative_context": _json_ready(authoritative_context),
        "provisional_decision": _json_ready(provisional_decision),
    }


def evaluate_authoritative_decision(
    *,
    experiment_id: str,
    hypothesis_id: str,
    method_family: str,
    judge_input: dict[str, Any],
    authoritative_context: dict[str, Any],
    provisional_decision: dict[str, Any],
    decided_at: str,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    loaded_policy = policy or load_decision_policy()
    decision_inputs = build_authoritative_decision_inputs(
        judge_input=judge_input,
        authoritative_context=authoritative_context,
        provisional_decision=provisional_decision,
        policy=loaded_policy,
    )
    summary_metrics = judge_input.get("summary_metrics") if isinstance(judge_input.get("summary_metrics"), dict) else {}
    hard_blocking_reasons = _blocking_reasons_from_context(
        policy=loaded_policy,
        authoritative_context=authoritative_context,
        provisional_decision=provisional_decision,
        judge_input=judge_input,
    )
    insufficient_sample = int(judge_input.get("available_sample_count") or 0) <= 0 or bool(summary_metrics.get("insufficient_samples"))
    branching_exists = _branching_exists(loaded_policy, judge_input, authoritative_context)
    compare_signal = _compare_signal(authoritative_context)
    missing_metrics = _missing_or_ambiguous_metrics(judge_input, authoritative_context)
    compare_is_affirmative = compare_signal == "affirmative" and not missing_metrics
    compare_is_negative = compare_signal == "negative" and not missing_metrics
    weak_or_unclear = (
        compare_signal == "unclear"
        or bool(summary_metrics.get("topk_branching_block_reason"))
        or not bool(summary_metrics.get("promote_ready"))
        or bool(missing_metrics)
    )

    if hard_blocking_reasons:
        decision = "drop"
        blocking_reasons = hard_blocking_reasons
    elif insufficient_sample:
        decision = "hold"
        blocking_reasons = ["insufficient_sample"]
    elif not branching_exists:
        decision = "hold"
        blocking_reasons = [reason for reason in [
            _text(authoritative_context.get("topk_branching_block_reason"), fallback="no_branching")
        ] if reason]
    elif compare_is_affirmative and not hard_blocking_reasons:
        decision = "keep"
        blocking_reasons = []
    elif compare_is_negative:
        decision = "drop"
        blocking_reasons = ["family_compare_negative"]
    elif weak_or_unclear or missing_metrics:
        decision = "hold"
        blocking_reasons = missing_metrics or ["weak_or_unclear_evaluation"]
    else:
        decision = "hold"
        blocking_reasons = ["weak_or_unclear_evaluation"]

    evidence_summary = {
        "hard_blocker": bool(hard_blocking_reasons),
        "hard_blocker_reasons": hard_blocking_reasons,
        "insufficient_sample": insufficient_sample,
        "branching_exists": branching_exists,
        "compare_signal": compare_signal,
        "compare_affirmative": compare_is_affirmative,
        "compare_negative": compare_is_negative,
        "weak_or_unclear": weak_or_unclear,
        "missing_or_ambiguous_metrics": missing_metrics,
        "available_sample_count": int(judge_input.get("available_sample_count") or 0),
        "available_session_count": int(judge_input.get("available_session_count") or 0),
        "changed_top5_members_count": int(judge_input.get("changed_top5_members_count") or 0),
        "changed_top10_members_count": int(judge_input.get("changed_top10_members_count") or 0),
        "changed_rank_count": int(judge_input.get("changed_rank_count") or 0),
        "top5_boundary_score_gap": float(judge_input.get("top5_boundary_score_gap") or 0.0),
        "top10_boundary_score_gap": float(judge_input.get("top10_boundary_score_gap") or 0.0),
        "selection_divergence_reason": _text(judge_input.get("selection_divergence_reason")),
        "family_decision": _text(authoritative_context.get("family_decision")),
        "authoritative_rollup_decision": _text(authoritative_context.get("authoritative_rollup_decision")),
        "best_candidate_decision": _text(authoritative_context.get("best_candidate_decision")),
        "candidate_local_decision": _text(authoritative_context.get("candidate_local_decision")),
        "provisional_decision": _text(provisional_decision.get("decision")),
    }

    return os_contracts.build_authoritative_decision(
        experiment_id=experiment_id,
        hypothesis_id=hypothesis_id,
        method_family=method_family,
        decision=decision,
        decision_policy_version=_text(loaded_policy.get("decision_policy_version"), fallback=TRADEX_RESEARCH_DECISION_POLICY_VERSION),
        decision_inputs=decision_inputs,
        blocking_reasons=blocking_reasons,
        evidence_summary=evidence_summary,
        decided_at=decided_at,
    )
