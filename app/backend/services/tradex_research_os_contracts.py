from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Final


TRADEX_RESEARCH_OS_HYPOTHESIS_SCHEMA_VERSION: Final[str] = "tradex_research_os_hypothesis_v1"
TRADEX_RESEARCH_OS_EXPERIMENT_MANIFEST_SCHEMA_VERSION: Final[str] = "tradex_research_os_experiment_manifest_v1"
TRADEX_RESEARCH_OS_JUDGE_INPUT_SCHEMA_VERSION: Final[str] = "tradex_research_os_judge_input_v1"
TRADEX_RESEARCH_OS_JUDGE_DECISION_SCHEMA_VERSION: Final[str] = "tradex_research_os_judge_decision_v1"
TRADEX_RESEARCH_OS_AUTHORITATIVE_DECISION_SCHEMA_VERSION: Final[str] = "tradex_research_os_authoritative_decision_v1"
TRADEX_RESEARCH_OS_OBSERVATION_SNAPSHOT_SCHEMA_VERSION: Final[str] = "tradex_research_os_observation_snapshot_v1"
TRADEX_RESEARCH_OS_STRATEGY_JUDGEMENT_SCHEMA_VERSION: Final[str] = "tradex_research_os_strategy_judgement_v1"
TRADEX_RESEARCH_OS_TEACHER_EVALUATION_ROW_SCHEMA_VERSION: Final[str] = "tradex_research_os_teacher_evaluation_row_v1"
TRADEX_RESEARCH_OS_PREFLIGHT_REPORT_SCHEMA_VERSION: Final[str] = "tradex_research_os_preflight_report_v1"
TRADEX_RESEARCH_OS_MEMORY_SCHEMA_VERSION: Final[str] = "tradex_research_os_memory_v1"
TRADEX_RESEARCH_OS_TRADER_BENCHMARK_ROW_SCHEMA_VERSION: Final[str] = "tradex_research_os_trader_benchmark_row_v1"
TRADEX_RESEARCH_OS_TRADER_ADAPTER_SCOREBOARD_SCHEMA_VERSION: Final[str] = "tradex_research_os_trader_adapter_scoreboard_v1"
TRADEX_RESEARCH_OS_TRADER_BENCHMARK_MANIFEST_SCHEMA_VERSION: Final[str] = "tradex_research_os_trader_benchmark_manifest_v1"
TRADEX_RESEARCH_OS_DECISIONS: Final[tuple[str, ...]] = ("keep", "drop", "hold")
TRADEX_RESEARCH_OS_RUNNER_VERSION: Final[str] = "tradex_research_os_runner_v1"
TRADEX_TRADER_MACHINE_ACTION_STATES: Final[tuple[str, ...]] = ("enter", "wait", "skip")
TRADEX_TRADER_HUMAN_JUDGEMENTS: Final[tuple[str, ...]] = ("buy", "hold", "reject")
TRADEX_TRADER_JUDGEMENT_OUTCOME_CLASSES: Final[tuple[str, ...]] = ("good", "mixed", "bad", "incomplete")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _require_fields(payload: dict[str, Any], *, required_fields: tuple[str, ...], artifact_name: str) -> None:
    missing = [field for field in required_fields if field not in payload]
    _require(not missing, f"{artifact_name} missing required fields: {', '.join(missing)}")


def _require_non_empty_text(payload: dict[str, Any], field_name: str) -> str:
    value = _text(payload.get(field_name))
    _require(value != "", f"{field_name} is required")
    return value


def _require_list_of_text(payload: dict[str, Any], field_name: str) -> list[str]:
    value = payload.get(field_name)
    _require(isinstance(value, list), f"{field_name} must be a list")
    result = [_text(item) for item in value if _text(item)]
    _require(bool(result), f"{field_name} must be a non-empty list")
    return result


def _require_dict(payload: dict[str, Any], field_name: str) -> dict[str, Any]:
    value = payload.get(field_name)
    _require(isinstance(value, dict), f"{field_name} must be an object")
    return dict(value)


def validate_hypothesis(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "hypothesis_id",
            "hypothesis_type",
            "changed_axis",
            "fixed_contracts",
            "expected_effect",
            "metrics_to_watch",
            "acceptance_gate",
            "rejection_gate",
            "notes",
            "status",
            "target_method_family",
            "execution",
        ),
        artifact_name="hypothesis",
    )
    _require(_text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_HYPOTHESIS_SCHEMA_VERSION, "hypothesis schema_version mismatch")
    _require_non_empty_text(payload, "hypothesis_id")
    _require_non_empty_text(payload, "hypothesis_type")
    _require_non_empty_text(payload, "changed_axis")
    _require_non_empty_text(payload, "expected_effect")
    _require_non_empty_text(payload, "notes")
    _require_non_empty_text(payload, "status")
    _require_non_empty_text(payload, "target_method_family")
    _require_list_of_text(payload, "fixed_contracts")
    _require_list_of_text(payload, "metrics_to_watch")
    _require_dict(payload, "acceptance_gate")
    _require_dict(payload, "rejection_gate")
    execution = _require_dict(payload, "execution")
    _require(_text(execution.get("runner")) == "tradex_research_session", "execution.runner must be tradex_research_session")
    for field in ("session_id", "random_seed", "session_scope_id", "universe_size", "max_candidates_per_family", "ret20_source_mode"):
        _require(field in execution, f"execution.{field} is required")
        _require(_text(execution.get(field)) != "", f"execution.{field} is required")
    if "strategy_target" in payload:
        strategy_target = _require_dict(payload, "strategy_target")
        for field in ("code", "as_of_date", "side", "judgement_type"):
            _require(field in strategy_target, f"strategy_target.{field} is required")
            _require(_text(strategy_target.get(field)) != "", f"strategy_target.{field} is required")
    if "strategy_judgement" in payload:
        _require("strategy_target" in payload, "strategy_judgement requires strategy_target")
        strategy_judgement = _require_dict(payload, "strategy_judgement")
        if "primary_adapter_id" in strategy_judgement:
            _require(_text(strategy_judgement.get("primary_adapter_id")) != "", "strategy_judgement.primary_adapter_id is required")
        if "adapter_ids" in strategy_judgement:
            adapter_ids = _require_list_of_text(strategy_judgement, "adapter_ids")
            if "primary_adapter_id" in strategy_judgement:
                _require(
                    _text(strategy_judgement.get("primary_adapter_id")) in adapter_ids,
                    "strategy_judgement.primary_adapter_id must be in adapter_ids",
                )
        for field in ("observation_lookback_bars", "teacher_horizon_bars"):
            if field in strategy_judgement:
                value = strategy_judgement.get(field)
                _require(isinstance(value, int), f"strategy_judgement.{field} must be an integer")
                _require(int(value) > 0, f"strategy_judgement.{field} must be > 0")


def build_hypothesis_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    validate_hypothesis(payload)
    normalized = {
        "schema_version": TRADEX_RESEARCH_OS_HYPOTHESIS_SCHEMA_VERSION,
        "hypothesis_id": _text(payload.get("hypothesis_id")),
        "hypothesis_type": _text(payload.get("hypothesis_type")),
        "changed_axis": _text(payload.get("changed_axis")),
        "fixed_contracts": [_text(item) for item in payload.get("fixed_contracts") or [] if _text(item)],
        "expected_effect": _text(payload.get("expected_effect")),
        "metrics_to_watch": [_text(item) for item in payload.get("metrics_to_watch") or [] if _text(item)],
        "acceptance_gate": dict(payload.get("acceptance_gate") or {}),
        "rejection_gate": dict(payload.get("rejection_gate") or {}),
        "notes": _text(payload.get("notes")),
        "status": _text(payload.get("status")),
        "target_method_family": _text(payload.get("target_method_family")),
        "execution": dict(payload.get("execution") or {}),
    }
    if isinstance(payload.get("strategy_target"), dict):
        normalized["strategy_target"] = dict(payload.get("strategy_target") or {})
    if isinstance(payload.get("strategy_judgement"), dict):
        normalized["strategy_judgement"] = dict(payload.get("strategy_judgement") or {})
    normalized["hypothesis_hash"] = _stable_hash(normalized)
    return normalized


def validate_experiment_manifest(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "experiment_id",
            "hypothesis_id",
            "repo_commit",
            "runner_version",
            "config_fingerprint",
            "scope_fingerprint",
            "seed",
            "started_at",
            "finished_at",
            "generated_artifacts",
        ),
        artifact_name="experiment manifest",
    )
    _require(_text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_EXPERIMENT_MANIFEST_SCHEMA_VERSION, "experiment manifest schema_version mismatch")
    for field in ("experiment_id", "hypothesis_id", "repo_commit", "runner_version", "config_fingerprint", "scope_fingerprint", "started_at", "finished_at"):
        _require_non_empty_text(payload, field)
    _require(isinstance(payload.get("seed"), int), "seed must be an integer")
    _require(isinstance(payload.get("generated_artifacts"), list), "generated_artifacts must be a list")


def build_experiment_manifest(
    *,
    experiment_id: str,
    hypothesis_id: str,
    repo_commit: str,
    runner_version: str,
    config_fingerprint: str,
    scope_fingerprint: str,
    seed: int,
    started_at: str,
    finished_at: str,
    generated_artifacts: list[dict[str, Any]],
    parent_experiment_id: str | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_EXPERIMENT_MANIFEST_SCHEMA_VERSION,
        "experiment_id": _text(experiment_id),
        "hypothesis_id": _text(hypothesis_id),
        "parent_experiment_id": _text(parent_experiment_id) or None,
        "repo_commit": _text(repo_commit),
        "runner_version": _text(runner_version),
        "config_fingerprint": _text(config_fingerprint),
        "scope_fingerprint": _text(scope_fingerprint),
        "seed": int(seed),
        "started_at": _text(started_at),
        "finished_at": _text(finished_at),
        "generated_artifacts": [dict(item) for item in generated_artifacts if isinstance(item, dict)],
    }
    payload["experiment_manifest_hash"] = _stable_hash(payload)
    validate_experiment_manifest(payload)
    return payload


def validate_judge_input(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "experiment_id",
            "comparison_scope",
            "changed_top5_members_count",
            "changed_top10_members_count",
            "changed_rank_count",
            "top5_boundary_score_gap",
            "top10_boundary_score_gap",
            "selection_divergence_reason",
            "available_sample_count",
            "available_session_count",
            "summary_metrics",
        ),
        artifact_name="judge input",
    )
    _require(_text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_JUDGE_INPUT_SCHEMA_VERSION, "judge input schema_version mismatch")
    _require_non_empty_text(payload, "experiment_id")
    _require(isinstance(payload.get("comparison_scope"), dict), "comparison_scope must be an object")
    for field in ("changed_top5_members_count", "changed_top10_members_count", "changed_rank_count", "available_sample_count", "available_session_count"):
        _require(isinstance(payload.get(field), int), f"{field} must be an integer")
    for field in ("top5_boundary_score_gap", "top10_boundary_score_gap"):
        _require(isinstance(payload.get(field), (int, float)), f"{field} must be numeric")
    _require_non_empty_text(payload, "selection_divergence_reason")
    _require(isinstance(payload.get("summary_metrics"), dict), "summary_metrics must be an object")


def build_judge_input(
    *,
    experiment_id: str,
    comparison_scope: dict[str, Any],
    changed_top5_members_count: int,
    changed_top10_members_count: int,
    changed_rank_count: int,
    top5_boundary_score_gap: float,
    top10_boundary_score_gap: float,
    selection_divergence_reason: str,
    available_sample_count: int,
    available_session_count: int,
    summary_metrics: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_JUDGE_INPUT_SCHEMA_VERSION,
        "experiment_id": _text(experiment_id),
        "comparison_scope": dict(comparison_scope),
        "changed_top5_members_count": int(changed_top5_members_count),
        "changed_top10_members_count": int(changed_top10_members_count),
        "changed_rank_count": int(changed_rank_count),
        "top5_boundary_score_gap": float(top5_boundary_score_gap),
        "top10_boundary_score_gap": float(top10_boundary_score_gap),
        "selection_divergence_reason": _text(selection_divergence_reason, fallback="no_meaningful_branching"),
        "available_sample_count": int(available_sample_count),
        "available_session_count": int(available_session_count),
        "summary_metrics": dict(summary_metrics),
    }
    payload["judge_input_hash"] = _stable_hash(payload)
    validate_judge_input(payload)
    return payload


def validate_judge_decision(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "experiment_id",
            "decision",
            "typed_reason",
            "confidence",
            "next_action",
            "blocking_unknowns",
            "decided_at",
        ),
        artifact_name="judge decision",
    )
    _require(_text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_JUDGE_DECISION_SCHEMA_VERSION, "judge decision schema_version mismatch")
    _require_non_empty_text(payload, "experiment_id")
    _require(_text(payload.get("decision")) in TRADEX_RESEARCH_OS_DECISIONS, "decision must be keep, drop, or hold")
    _require(isinstance(payload.get("typed_reason"), dict), "typed_reason must be an object")
    _require(isinstance(payload.get("confidence"), (int, float)), "confidence must be numeric")
    _require_non_empty_text(payload, "next_action")
    _require(isinstance(payload.get("blocking_unknowns"), list), "blocking_unknowns must be a list")
    _require_non_empty_text(payload, "decided_at")


def build_judge_decision(
    *,
    experiment_id: str,
    decision: str,
    typed_reason: dict[str, Any],
    confidence: float,
    next_action: str,
    blocking_unknowns: list[str],
    decided_at: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_JUDGE_DECISION_SCHEMA_VERSION,
        "experiment_id": _text(experiment_id),
        "decision": _text(decision),
        "typed_reason": dict(typed_reason),
        "confidence": float(confidence),
        "next_action": _text(next_action),
        "blocking_unknowns": [_text(item) for item in blocking_unknowns if _text(item)],
        "decided_at": _text(decided_at),
    }
    payload["judge_decision_hash"] = _stable_hash(payload)
    validate_judge_decision(payload)
    return payload


def validate_authoritative_decision(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "experiment_id",
            "hypothesis_id",
            "method_family",
            "decision",
            "decision_policy_version",
            "decision_inputs",
            "blocking_reasons",
            "evidence_summary",
            "decided_at",
        ),
        artifact_name="authoritative decision",
    )
    _require(_text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_AUTHORITATIVE_DECISION_SCHEMA_VERSION, "authoritative decision schema_version mismatch")
    _require_non_empty_text(payload, "experiment_id")
    _require_non_empty_text(payload, "hypothesis_id")
    _require_non_empty_text(payload, "method_family")
    _require(_text(payload.get("decision")) in TRADEX_RESEARCH_OS_DECISIONS, "decision must be keep, drop, or hold")
    _require_non_empty_text(payload, "decision_policy_version")
    _require(isinstance(payload.get("decision_inputs"), dict), "decision_inputs must be an object")
    _require(isinstance(payload.get("blocking_reasons"), list), "blocking_reasons must be a list")
    _require(isinstance(payload.get("evidence_summary"), dict), "evidence_summary must be an object")
    _require_non_empty_text(payload, "decided_at")


def build_authoritative_decision(
    *,
    experiment_id: str,
    hypothesis_id: str,
    method_family: str,
    decision: str,
    decision_policy_version: str,
    decision_inputs: dict[str, Any],
    blocking_reasons: list[str],
    evidence_summary: dict[str, Any],
    decided_at: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_AUTHORITATIVE_DECISION_SCHEMA_VERSION,
        "experiment_id": _text(experiment_id),
        "hypothesis_id": _text(hypothesis_id),
        "method_family": _text(method_family),
        "decision": _text(decision),
        "decision_policy_version": _text(decision_policy_version),
        "decision_inputs": dict(decision_inputs),
        "blocking_reasons": [_text(item) for item in blocking_reasons if _text(item)],
        "evidence_summary": dict(evidence_summary),
        "decided_at": _text(decided_at),
    }
    payload["authoritative_decision_hash"] = _stable_hash(payload)
    validate_authoritative_decision(payload)
    return payload


def validate_observation_snapshot(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "experiment_id",
            "hypothesis_id",
            "target",
            "observation_contract_version",
            "confirmed_bar",
            "recent_bars",
            "derived_features",
            "market_context",
            "lineage",
            "generated_at",
        ),
        artifact_name="observation snapshot",
    )
    _require(
        _text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_OBSERVATION_SNAPSHOT_SCHEMA_VERSION,
        "observation snapshot schema_version mismatch",
    )
    for field in ("experiment_id", "hypothesis_id", "observation_contract_version", "generated_at"):
        _require_non_empty_text(payload, field)
    for field in ("target", "confirmed_bar", "derived_features", "market_context", "lineage"):
        _require(isinstance(payload.get(field), dict), f"{field} must be an object")
    _require(isinstance(payload.get("recent_bars"), list), "recent_bars must be a list")
    target = dict(payload.get("target") or {})
    for field in ("code", "as_of_date", "side", "judgement_type"):
        _require(_text(target.get(field)) != "", f"target.{field} is required")


def build_observation_snapshot(
    *,
    experiment_id: str,
    hypothesis_id: str,
    target: dict[str, Any],
    observation_contract_version: str,
    confirmed_bar: dict[str, Any],
    recent_bars: list[dict[str, Any]],
    derived_features: dict[str, Any],
    market_context: dict[str, Any],
    lineage: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_OBSERVATION_SNAPSHOT_SCHEMA_VERSION,
        "experiment_id": _text(experiment_id),
        "hypothesis_id": _text(hypothesis_id),
        "target": dict(target),
        "observation_contract_version": _text(observation_contract_version),
        "confirmed_bar": dict(confirmed_bar),
        "recent_bars": [dict(item) for item in recent_bars if isinstance(item, dict)],
        "derived_features": dict(derived_features),
        "market_context": dict(market_context),
        "lineage": dict(lineage),
        "generated_at": _text(generated_at),
    }
    payload["observation_snapshot_hash"] = _stable_hash(payload)
    validate_observation_snapshot(payload)
    return payload


def validate_strategy_judgement(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "experiment_id",
            "hypothesis_id",
            "target",
            "primary_adapter_id",
            "machine_action_state",
            "human_readable_judgement",
            "buy_score",
            "environment_score",
            "trend_score",
            "trigger_score",
            "risk_score",
            "invalidation_price",
            "invalidation_reason_code",
            "reason_codes",
            "adapter_outputs",
            "observation_snapshot_hash",
            "generated_at",
        ),
        artifact_name="strategy judgement",
    )
    _require(
        _text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_STRATEGY_JUDGEMENT_SCHEMA_VERSION,
        "strategy judgement schema_version mismatch",
    )
    for field in ("experiment_id", "hypothesis_id", "primary_adapter_id", "invalidation_reason_code", "observation_snapshot_hash", "generated_at"):
        _require_non_empty_text(payload, field)
    _require(isinstance(payload.get("target"), dict), "target must be an object")
    _require(
        _text(payload.get("machine_action_state")) in TRADEX_TRADER_MACHINE_ACTION_STATES,
        "machine_action_state must be enter, wait, or skip",
    )
    _require(
        _text(payload.get("human_readable_judgement")) in TRADEX_TRADER_HUMAN_JUDGEMENTS,
        "human_readable_judgement must be buy, hold, or reject",
    )
    for field in ("buy_score", "environment_score", "trend_score", "trigger_score", "risk_score", "invalidation_price"):
        _require(isinstance(payload.get(field), (int, float)), f"{field} must be numeric")
    _require(isinstance(payload.get("reason_codes"), list), "reason_codes must be a list")
    _require(isinstance(payload.get("adapter_outputs"), list), "adapter_outputs must be a list")


def build_strategy_judgement(
    *,
    experiment_id: str,
    hypothesis_id: str,
    target: dict[str, Any],
    primary_adapter_id: str,
    machine_action_state: str,
    human_readable_judgement: str,
    buy_score: float,
    environment_score: float,
    trend_score: float,
    trigger_score: float,
    risk_score: float,
    invalidation_price: float,
    invalidation_reason_code: str,
    reason_codes: list[str],
    adapter_outputs: list[dict[str, Any]],
    observation_snapshot_hash: str,
    generated_at: str,
    explanation: str = "",
    adapter_agreement: bool | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_STRATEGY_JUDGEMENT_SCHEMA_VERSION,
        "experiment_id": _text(experiment_id),
        "hypothesis_id": _text(hypothesis_id),
        "target": dict(target),
        "primary_adapter_id": _text(primary_adapter_id),
        "machine_action_state": _text(machine_action_state),
        "human_readable_judgement": _text(human_readable_judgement),
        "buy_score": float(buy_score),
        "environment_score": float(environment_score),
        "trend_score": float(trend_score),
        "trigger_score": float(trigger_score),
        "risk_score": float(risk_score),
        "invalidation_price": float(invalidation_price),
        "invalidation_reason_code": _text(invalidation_reason_code),
        "reason_codes": [_text(item) for item in reason_codes if _text(item)],
        "adapter_outputs": [dict(item) for item in adapter_outputs if isinstance(item, dict)],
        "observation_snapshot_hash": _text(observation_snapshot_hash),
        "generated_at": _text(generated_at),
        "explanation": _text(explanation),
    }
    if adapter_agreement is not None:
        payload["adapter_agreement"] = bool(adapter_agreement)
    payload["strategy_judgement_hash"] = _stable_hash(payload)
    validate_strategy_judgement(payload)
    return payload


def validate_teacher_evaluation_row(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "experiment_id",
            "hypothesis_id",
            "target",
            "observation_snapshot_hash",
            "strategy_judgement_hash",
            "realized_outcome_window",
            "lineage",
            "generated_at",
        ),
        artifact_name="teacher evaluation row",
    )
    _require(
        _text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_TEACHER_EVALUATION_ROW_SCHEMA_VERSION,
        "teacher evaluation row schema_version mismatch",
    )
    for field in ("experiment_id", "hypothesis_id", "observation_snapshot_hash", "strategy_judgement_hash", "generated_at"):
        _require_non_empty_text(payload, field)
    for field in ("target", "realized_outcome_window", "lineage"):
        _require(isinstance(payload.get(field), dict), f"{field} must be an object")


def build_teacher_evaluation_row(
    *,
    experiment_id: str,
    hypothesis_id: str,
    target: dict[str, Any],
    observation_snapshot_hash: str,
    strategy_judgement_hash: str,
    realized_outcome_window: dict[str, Any],
    lineage: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_TEACHER_EVALUATION_ROW_SCHEMA_VERSION,
        "experiment_id": _text(experiment_id),
        "hypothesis_id": _text(hypothesis_id),
        "target": dict(target),
        "observation_snapshot_hash": _text(observation_snapshot_hash),
        "strategy_judgement_hash": _text(strategy_judgement_hash),
        "realized_outcome_window": dict(realized_outcome_window),
        "lineage": dict(lineage),
        "generated_at": _text(generated_at),
    }
    payload["teacher_evaluation_row_hash"] = _stable_hash(payload)
    validate_teacher_evaluation_row(payload)
    return payload


def validate_preflight_report(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "experiment_id",
            "hypothesis_id",
            "runner",
            "status",
            "passed",
            "failure_code",
            "failure_detail",
            "checked_inputs",
            "normalization_applied",
            "checked_at",
        ),
        artifact_name="preflight report",
    )
    _require(_text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_PREFLIGHT_REPORT_SCHEMA_VERSION, "preflight report schema_version mismatch")
    _require_non_empty_text(payload, "experiment_id")
    _require_non_empty_text(payload, "hypothesis_id")
    _require_non_empty_text(payload, "runner")
    _require_non_empty_text(payload, "status")
    _require(isinstance(payload.get("passed"), bool), "passed must be a boolean")
    _require(isinstance(payload.get("failure_detail"), dict), "failure_detail must be an object")
    _require(isinstance(payload.get("checked_inputs"), dict), "checked_inputs must be an object")
    _require(isinstance(payload.get("normalization_applied"), list), "normalization_applied must be a list")
    _require_non_empty_text(payload, "checked_at")
    if "cause_class" in payload:
        _require_non_empty_text(payload, "cause_class")
    if "cause_source" in payload:
        _require_non_empty_text(payload, "cause_source")
    if "remediation_hint" in payload:
        _require_non_empty_text(payload, "remediation_hint")
    if "readiness_checks" in payload:
        _require(isinstance(payload.get("readiness_checks"), list), "readiness_checks must be a list")
    if "readiness_summary" in payload:
        _require(isinstance(payload.get("readiness_summary"), dict), "readiness_summary must be an object")


def build_preflight_report(
    *,
    experiment_id: str,
    hypothesis_id: str,
    runner: str,
    status: str,
    passed: bool,
    failure_code: str,
    failure_detail: dict[str, Any],
    checked_inputs: dict[str, Any],
    normalization_applied: list[str],
    checked_at: str,
    cause_class: str = "",
    cause_source: str = "",
    remediation_hint: str = "",
    readiness_checks: list[dict[str, Any]] | None = None,
    readiness_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_PREFLIGHT_REPORT_SCHEMA_VERSION,
        "experiment_id": _text(experiment_id),
        "hypothesis_id": _text(hypothesis_id),
        "runner": _text(runner),
        "status": _text(status),
        "passed": bool(passed),
        "failure_code": _text(failure_code),
        "failure_detail": dict(failure_detail),
        "checked_inputs": dict(checked_inputs),
        "normalization_applied": [_text(item) for item in normalization_applied if _text(item)],
        "checked_at": _text(checked_at),
    }
    if _text(cause_class):
        payload["cause_class"] = _text(cause_class)
    if _text(cause_source):
        payload["cause_source"] = _text(cause_source)
    if _text(remediation_hint):
        payload["remediation_hint"] = _text(remediation_hint)
    if readiness_checks is not None:
        payload["readiness_checks"] = [dict(item) for item in readiness_checks if isinstance(item, dict)]
    if readiness_summary is not None:
        payload["readiness_summary"] = dict(readiness_summary)
    payload["preflight_report_hash"] = _stable_hash(payload)
    validate_preflight_report(payload)
    return payload


def validate_research_memory(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "hypothesis_id",
            "latest_decision",
            "decision_history",
            "retry_blockers",
            "related_hypotheses",
            "notes_for_next_iteration",
        ),
        artifact_name="research memory",
    )
    _require(_text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_MEMORY_SCHEMA_VERSION, "research memory schema_version mismatch")
    _require_non_empty_text(payload, "hypothesis_id")
    _require(_text(payload.get("latest_decision")) in TRADEX_RESEARCH_OS_DECISIONS, "latest_decision must be keep, drop, or hold")
    _require(isinstance(payload.get("decision_history"), list), "decision_history must be a list")
    _require(isinstance(payload.get("retry_blockers"), list), "retry_blockers must be a list")
    _require(isinstance(payload.get("related_hypotheses"), list), "related_hypotheses must be a list")
    _require_non_empty_text(payload, "notes_for_next_iteration")


def build_research_memory(
    *,
    hypothesis_id: str,
    latest_decision: str,
    decision_history: list[dict[str, Any]],
    retry_blockers: list[str],
    related_hypotheses: list[str],
    notes_for_next_iteration: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_MEMORY_SCHEMA_VERSION,
        "hypothesis_id": _text(hypothesis_id),
        "latest_decision": _text(latest_decision),
        "decision_history": [dict(item) for item in decision_history if isinstance(item, dict)],
        "retry_blockers": [_text(item) for item in retry_blockers if _text(item)],
        "related_hypotheses": [_text(item) for item in related_hypotheses if _text(item)],
        "notes_for_next_iteration": _text(notes_for_next_iteration),
        "updated_at": now_utc_iso(),
    }
    payload["research_memory_hash"] = _stable_hash(payload)
    validate_research_memory(payload)
    return payload


def validate_trader_benchmark_row(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "benchmark_version",
            "experiment_id",
            "hypothesis_id",
            "family_id",
            "method_family",
            "as_of_date",
            "code",
            "adapter_id",
            "machine_action_state",
            "human_readable_judgement",
            "buy_score",
            "environment_score",
            "trend_score",
            "trigger_score",
            "risk_score",
            "invalidation_price",
            "invalidation_reason_code",
            "reason_codes",
            "confidence",
            "is_primary_adapter",
            "teacher_horizon_bars",
            "future_bar_count",
            "complete_horizon",
            "anchor_close_price",
            "next_open_price",
            "final_close_price",
            "return_close_basis",
            "return_next_open_basis",
            "max_favorable_excursion_close_basis",
            "max_adverse_excursion_close_basis",
            "close_positive_20",
            "next_open_positive_20",
            "mfe_ge_10pct_20",
            "mae_worse_than_7pct_20",
            "judgement_outcome_class",
            "label_policy_version",
            "observation_snapshot_hash",
            "strategy_judgement_hash",
            "teacher_evaluation_row_hash",
        ),
        artifact_name="trader benchmark row",
    )
    _require(
        _text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_TRADER_BENCHMARK_ROW_SCHEMA_VERSION,
        "trader benchmark row schema_version mismatch",
    )
    _require(_text(payload.get("benchmark_version")) == "v1", "trader benchmark row benchmark_version mismatch")
    for field in (
        "experiment_id",
        "hypothesis_id",
        "family_id",
        "method_family",
        "adapter_id",
        "invalidation_reason_code",
        "observation_snapshot_hash",
        "strategy_judgement_hash",
        "teacher_evaluation_row_hash",
    ):
        _require_non_empty_text(payload, field)
    _require(isinstance(payload.get("as_of_date"), int), "as_of_date must be an integer")
    _require_non_empty_text(payload, "code")
    _require(_text(payload.get("machine_action_state")) in TRADEX_TRADER_MACHINE_ACTION_STATES, "machine_action_state must be enter, wait, or skip")
    _require(_text(payload.get("human_readable_judgement")) in TRADEX_TRADER_HUMAN_JUDGEMENTS, "human_readable_judgement must be buy, hold, or reject")
    for field in (
        "buy_score",
        "environment_score",
        "trend_score",
        "trigger_score",
        "risk_score",
        "invalidation_price",
        "confidence",
        "anchor_close_price",
    ):
        _require(isinstance(payload.get(field), (int, float)), f"{field} must be numeric")
    for field in ("teacher_horizon_bars", "future_bar_count"):
        _require(isinstance(payload.get(field), int), f"{field} must be an integer")
    for field in ("is_primary_adapter", "complete_horizon"):
        _require(isinstance(payload.get(field), bool), f"{field} must be a boolean")
    for field in ("next_open_price", "final_close_price", "return_close_basis", "return_next_open_basis", "max_favorable_excursion_close_basis", "max_adverse_excursion_close_basis"):
        _require(payload.get(field) is None or isinstance(payload.get(field), (int, float)), f"{field} must be numeric or null")
    for field in ("close_positive_20", "next_open_positive_20", "mfe_ge_10pct_20", "mae_worse_than_7pct_20"):
        _require(payload.get(field) is None or isinstance(payload.get(field), bool), f"{field} must be boolean or null")
    _require(_text(payload.get("judgement_outcome_class")) in TRADEX_TRADER_JUDGEMENT_OUTCOME_CLASSES, "judgement_outcome_class is invalid")
    _require_non_empty_text(payload, "label_policy_version")
    _require(isinstance(payload.get("reason_codes"), list), "reason_codes must be a list")


def build_trader_benchmark_row(
    *,
    experiment_id: str,
    hypothesis_id: str,
    family_id: str,
    method_family: str,
    as_of_date: int,
    code: str,
    adapter_id: str,
    machine_action_state: str,
    human_readable_judgement: str,
    buy_score: float,
    environment_score: float,
    trend_score: float,
    trigger_score: float,
    risk_score: float,
    invalidation_price: float,
    invalidation_reason_code: str,
    reason_codes: list[str],
    confidence: float,
    is_primary_adapter: bool,
    teacher_horizon_bars: int,
    future_bar_count: int,
    complete_horizon: bool,
    anchor_close_price: float,
    next_open_price: float | None,
    final_close_price: float | None,
    return_close_basis: float | None,
    return_next_open_basis: float | None,
    max_favorable_excursion_close_basis: float | None,
    max_adverse_excursion_close_basis: float | None,
    close_positive_20: bool | None,
    next_open_positive_20: bool | None,
    mfe_ge_10pct_20: bool | None,
    mae_worse_than_7pct_20: bool | None,
    judgement_outcome_class: str,
    label_policy_version: str,
    observation_snapshot_hash: str,
    strategy_judgement_hash: str,
    teacher_evaluation_row_hash: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_TRADER_BENCHMARK_ROW_SCHEMA_VERSION,
        "benchmark_version": "v1",
        "experiment_id": _text(experiment_id),
        "hypothesis_id": _text(hypothesis_id),
        "family_id": _text(family_id),
        "method_family": _text(method_family),
        "as_of_date": int(as_of_date),
        "code": _text(code),
        "adapter_id": _text(adapter_id),
        "machine_action_state": _text(machine_action_state),
        "human_readable_judgement": _text(human_readable_judgement),
        "buy_score": float(buy_score),
        "environment_score": float(environment_score),
        "trend_score": float(trend_score),
        "trigger_score": float(trigger_score),
        "risk_score": float(risk_score),
        "invalidation_price": float(invalidation_price),
        "invalidation_reason_code": _text(invalidation_reason_code),
        "reason_codes": [_text(item) for item in reason_codes if _text(item)],
        "confidence": float(confidence),
        "is_primary_adapter": bool(is_primary_adapter),
        "teacher_horizon_bars": int(teacher_horizon_bars),
        "future_bar_count": int(future_bar_count),
        "complete_horizon": bool(complete_horizon),
        "anchor_close_price": float(anchor_close_price),
        "next_open_price": None if next_open_price is None else float(next_open_price),
        "final_close_price": None if final_close_price is None else float(final_close_price),
        "return_close_basis": None if return_close_basis is None else float(return_close_basis),
        "return_next_open_basis": None if return_next_open_basis is None else float(return_next_open_basis),
        "max_favorable_excursion_close_basis": None if max_favorable_excursion_close_basis is None else float(max_favorable_excursion_close_basis),
        "max_adverse_excursion_close_basis": None if max_adverse_excursion_close_basis is None else float(max_adverse_excursion_close_basis),
        "close_positive_20": None if close_positive_20 is None else bool(close_positive_20),
        "next_open_positive_20": None if next_open_positive_20 is None else bool(next_open_positive_20),
        "mfe_ge_10pct_20": None if mfe_ge_10pct_20 is None else bool(mfe_ge_10pct_20),
        "mae_worse_than_7pct_20": None if mae_worse_than_7pct_20 is None else bool(mae_worse_than_7pct_20),
        "judgement_outcome_class": _text(judgement_outcome_class),
        "label_policy_version": _text(label_policy_version),
        "observation_snapshot_hash": _text(observation_snapshot_hash),
        "strategy_judgement_hash": _text(strategy_judgement_hash),
        "teacher_evaluation_row_hash": _text(teacher_evaluation_row_hash),
    }
    payload["trader_benchmark_row_hash"] = _stable_hash(payload)
    validate_trader_benchmark_row(payload)
    return payload


def validate_trader_adapter_scoreboard(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=("schema_version", "benchmark_version", "generated_at", "adapters"),
        artifact_name="trader adapter scoreboard",
    )
    _require(
        _text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_TRADER_ADAPTER_SCOREBOARD_SCHEMA_VERSION,
        "trader adapter scoreboard schema_version mismatch",
    )
    _require(_text(payload.get("benchmark_version")) == "v1", "trader adapter scoreboard benchmark_version mismatch")
    _require_non_empty_text(payload, "generated_at")
    _require(isinstance(payload.get("adapters"), list), "adapters must be a list")
    for row in payload.get("adapters") or []:
        _require(isinstance(row, dict), "adapters entries must be objects")
        _require_non_empty_text(row, "adapter_id")
        for field in (
            "sample_count",
            "complete_horizon_count",
            "labeled_sample_count",
            "primary_count",
            "enter_count",
            "wait_count",
            "skip_count",
        ):
            _require(isinstance(row.get(field), int), f"{field} must be an integer")
        for field in (
            "avg_buy_score",
            "avg_confidence",
            "avg_return_close_basis_all",
            "avg_return_close_basis_enter",
            "median_return_close_basis_enter",
            "avg_return_next_open_basis_enter",
            "avg_mfe_enter",
            "avg_mae_enter",
            "close_positive_rate_all",
            "close_positive_rate_enter",
            "next_open_positive_rate_enter",
            "mfe_ge_10pct_rate_enter",
            "mae_worse_than_7pct_rate_enter",
            "good_outcome_rate_enter",
            "bad_outcome_rate_enter",
        ):
            _require(row.get(field) is None or isinstance(row.get(field), (int, float)), f"{field} must be numeric or null")


def build_trader_adapter_scoreboard(*, generated_at: str, adapters: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_TRADER_ADAPTER_SCOREBOARD_SCHEMA_VERSION,
        "benchmark_version": "v1",
        "generated_at": _text(generated_at),
        "adapters": [dict(item) for item in adapters if isinstance(item, dict)],
    }
    payload["trader_adapter_scoreboard_hash"] = _stable_hash(payload)
    validate_trader_adapter_scoreboard(payload)
    return payload


def validate_trader_benchmark_manifest(payload: dict[str, Any]) -> None:
    _require_fields(
        payload,
        required_fields=(
            "schema_version",
            "benchmark_version",
            "generated_at",
            "source_experiment_count",
            "materialized_row_count",
            "scoreboard_adapter_count",
            "skipped_experiments",
            "output_files",
        ),
        artifact_name="trader benchmark manifest",
    )
    _require(
        _text(payload.get("schema_version")) == TRADEX_RESEARCH_OS_TRADER_BENCHMARK_MANIFEST_SCHEMA_VERSION,
        "trader benchmark manifest schema_version mismatch",
    )
    _require(_text(payload.get("benchmark_version")) == "v1", "trader benchmark manifest benchmark_version mismatch")
    _require_non_empty_text(payload, "generated_at")
    for field in ("source_experiment_count", "materialized_row_count", "scoreboard_adapter_count"):
        _require(isinstance(payload.get(field), int), f"{field} must be an integer")
    _require(isinstance(payload.get("skipped_experiments"), list), "skipped_experiments must be a list")
    _require(isinstance(payload.get("output_files"), dict), "output_files must be an object")


def build_trader_benchmark_manifest(
    *,
    generated_at: str,
    source_experiment_count: int,
    materialized_row_count: int,
    scoreboard_adapter_count: int,
    skipped_experiments: list[dict[str, Any]],
    output_files: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RESEARCH_OS_TRADER_BENCHMARK_MANIFEST_SCHEMA_VERSION,
        "benchmark_version": "v1",
        "generated_at": _text(generated_at),
        "source_experiment_count": int(source_experiment_count),
        "materialized_row_count": int(materialized_row_count),
        "scoreboard_adapter_count": int(scoreboard_adapter_count),
        "skipped_experiments": [dict(item) for item in skipped_experiments if isinstance(item, dict)],
        "output_files": dict(output_files),
    }
    payload["trader_benchmark_manifest_hash"] = _stable_hash(payload)
    validate_trader_benchmark_manifest(payload)
    return payload
