"""Create a watch-only logging plan for inactive teppan shadow."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


AXIS_ID = "teppan_shadow_watch_mode_logging_plan_v1"
DEFAULT_RUN_ID = "20260514T120000Z-teppan-shadow-watch-mode-logging-plan-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\shadow_watch_mode_logging_plans\teppan_shadow_watch_mode_logging_plan_v1")
DEFAULT_RECENT_DAYS_REPLAY_ROOT = Path(
    r"G:\Tradex\shadow_recent_days_replays\teppan_shadow_recent_days_replay_v1"
    r"\20260514T110000Z-teppan-shadow-recent-days-replay-v1"
)
REQUIRED_INPUTS = [
    "replay_result.json",
    "replay_date_coverage_report.json",
    "no_mutation_audit.json",
    "next_axis_recommendation.json",
    "_ARTIFACT_COMPLETE.json",
]
REQUIRED_OUTPUTS = [
    "watch_mode_logging_plan.json",
    "watch_trigger_conditions.json",
    "daily_watch_artifact_contract.json",
    "human_review_trigger_contract.json",
    "no_activation_policy.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]
WATCH_METRICS = [
    "ranking_date",
    "top20_teppan_pattern_match_count",
    "top50_teppan_pattern_match_count",
    "top100_teppan_pattern_match_count",
    "top20_teppan_guard_pass_count",
    "top50_teppan_guard_pass_count",
    "top100_teppan_guard_pass_count",
    "boost_eligible_count",
    "loss_guard_blocked_count",
    "added_by_shadow_top5",
    "added_by_shadow_top10",
    "added_by_shadow_top20",
    "nearest_shadow_candidate_to_top5",
    "nearest_shadow_candidate_to_top10",
    "human_review_candidate_count",
    "no_mutation_pass",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recent-days-replay-root", type=Path, default=DEFAULT_RECENT_DAYS_REPLAY_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = parser.parse_args()
    run_teppan_shadow_watch_mode_logging_plan_v1(
        recent_days_replay_root=args.recent_days_replay_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
    )
    return 0


def run_teppan_shadow_watch_mode_logging_plan_v1(
    *,
    recent_days_replay_root: Path = DEFAULT_RECENT_DAYS_REPLAY_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    input_audit = _input_audit(recent_days_replay_root)
    replay_result = _read_json(recent_days_replay_root / "replay_result.json") if input_audit["required_inputs_present"] else {}
    coverage_report = _read_json(recent_days_replay_root / "replay_date_coverage_report.json") if input_audit["required_inputs_present"] else {}
    no_mutation = _read_json(recent_days_replay_root / "no_mutation_audit.json") if input_audit["required_inputs_present"] else {}
    source_complete = _read_json(recent_days_replay_root / "_ARTIFACT_COMPLETE.json") if input_audit["required_inputs_present"] else {}

    plan = _watch_mode_logging_plan(recent_days_replay_root, replay_result, coverage_report)
    triggers = _watch_trigger_conditions()
    daily_contract = _daily_watch_artifact_contract()
    review_contract = _human_review_trigger_contract()
    no_activation = _no_activation_policy()
    decision, decision_reason = _decision(input_audit, replay_result, no_mutation, source_complete)
    next_axis = _next_axis_recommendation(decision)
    research_decision = {
        "schema_version": "teppan_shadow_watch_mode_research_decision_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": decision_reason,
        "source_recent_days_replay_root": str(recent_days_replay_root),
        "source_replay_decision": replay_result.get("decision"),
        "source_no_mutation_pass": no_mutation.get("no_mutation_pass"),
        "activation_allowed": False,
        "watch_mode_ready": decision == "watch_mode_ready",
        "manual_review_triggered_now": False,
        "generated_at_utc": _utc_now(),
    }

    _write_json(output_root / "watch_mode_logging_plan.json", plan)
    _write_json(output_root / "watch_trigger_conditions.json", triggers)
    _write_json(output_root / "daily_watch_artifact_contract.json", daily_contract)
    _write_json(output_root / "human_review_trigger_contract.json", review_contract)
    _write_json(output_root / "no_activation_policy.json", no_activation)
    _write_json(output_root / "next_axis_recommendation.json", next_axis)
    _write_json(output_root / "research_decision.json", research_decision)
    complete = _artifact_complete(output_root, research_decision)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "research_decision": research_decision,
        "watch_mode_logging_plan": plan,
        "artifact_complete": complete,
    }


def _watch_mode_logging_plan(
    recent_days_replay_root: Path,
    replay_result: Mapping[str, Any],
    coverage_report: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = coverage_report.get("metrics") if isinstance(coverage_report.get("metrics"), Mapping) else {}
    return {
        "schema_version": "teppan_shadow_watch_mode_logging_plan_v1",
        "axis_id": AXIS_ID,
        "mode": "watch_only_inactive_shadow",
        "purpose": "record_lightweight_teppan_shadow_opportunities_after_runtime_ranking_updates",
        "source_recent_days_replay_root": str(recent_days_replay_root),
        "source_replay_decision": replay_result.get("decision"),
        "source_replay_reason": replay_result.get("decision_reason"),
        "source_metrics": dict(metrics),
        "watch_metrics": WATCH_METRICS,
        "run_cadence": "daily_or_on_demand_after_runtime_ranking_update",
        "artifact_only": True,
        "runtime_duckdb_write_allowed": False,
        "production_publish_registration_allowed": False,
        "active_runtime_ranking_change_allowed": False,
        "frontend_backend_api_exposure_allowed": False,
        "boost_value_change_allowed": False,
        "loss_guard_semantics_change_allowed": False,
        "pattern_definition_change_allowed": False,
        "no_silent_fallback": True,
        "not_changed": _not_changed(),
    }


def _watch_trigger_conditions() -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_watch_trigger_conditions_v1",
        "watch_mode_ready_if": [
            "daily_or_on_demand_lightweight_artifact_can_be_written",
            "no_mutation_audit_passes",
            "human_review_trigger_is_explicit",
            "activation_forbidden_policy_is_present",
        ],
        "human_review_trigger_if_any": [
            {"metric": "added_by_shadow_top5", "operator": ">", "threshold": 0},
            {"metric": "added_by_shadow_top10", "operator": ">", "threshold": 0},
            {
                "metric": "boost_eligible_near_top20_count",
                "operator": ">=",
                "threshold": 2,
                "required_explanation": "teppan_pattern_match_and_teppan_guard_pass_confirmed",
            },
        ],
        "continue_watch_only_if_all": [
            "teppan_coverage_exists_or_absent_but_no_shadow_topK_delta",
            "human_review_candidate_count_equals_0",
            "no_mutation_pass_true",
        ],
        "freeze_shadow_if_any": [
            "configured_watch_window_has_near_zero_top100_boost_eligible_count",
            "configured_watch_window_has_zero_topK_candidate_value",
        ],
    }


def _daily_watch_artifact_contract() -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_daily_watch_artifact_contract_v1",
        "artifact_kind": "json_only_daily_watch_snapshot",
        "required_fields": WATCH_METRICS,
        "required_candidate_fields": [
            "symbol",
            "name",
            "ranking_date",
            "active_rank",
            "display_score",
            "shadow_adjusted_score",
            "shadow_adjusted_rank",
            "baseline_no_boost_shadow_rank",
            "teppan_pattern_match",
            "teppan_guard_pass",
            "loss_guard_blocked",
            "best_pattern_family",
            "best_pattern_key",
            "guard_block_reason",
            "shadow_decision_reason",
        ],
        "forbidden_fields": [
            "ret20_fwd",
            "ret40_fwd",
            "mfe20",
            "mae20",
            "severe_loss20",
            "future_return_labels",
        ],
        "write_location_policy": "G:\\Tradex\\shadow_watch_logs\\teppan_shadow_watch_mode_logging_v1\\<run_id>",
        "runtime_duckdb_write_allowed": False,
        "silent_fallback_allowed": False,
    }


def _human_review_trigger_contract() -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_human_review_trigger_contract_v1",
        "max_candidates_per_watch_snapshot": 3,
        "trigger_decision": "human_review_trigger",
        "trigger_if_any": [
            "added_by_shadow_top5 > 0",
            "added_by_shadow_top10 > 0",
            "boost_eligible_near_top20_count >= 2",
        ],
        "candidate_must_explain": [
            "teppan_pattern_match",
            "teppan_guard_pass",
            "loss_guard_blocked_or_loss_guard_pass",
            "best_pattern_family",
            "best_pattern_key",
            "shadow_adjusted_score",
            "shadow_adjusted_rank",
        ],
        "activation_allowed_after_trigger": False,
        "next_after_trigger": "teppan_shadow_candidate_manual_review_v1",
    }


def _no_activation_policy() -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_no_activation_policy_v1",
        "activation_allowed": False,
        "reason": "recent_days_replay_has_watch_mode_hold_without_human_review_candidates",
        "forbidden_actions": [
            "activate_teppan_challenger",
            "change_active_runtime_ranking",
            "change_display_score",
            "write_runtime_duckdb",
            "register_production_publish",
            "modify_frontend_backend_ui_or_api",
            "change_boost_value",
            "change_loss_guard_semantics",
            "change_pattern_definitions",
            "force_teppan_into_topK",
            "additional_tuning_from_watch_plan",
        ],
        "allowed_actions": [
            "read_runtime_ranking_after_update",
            "compute_live_safe_teppan_features_read_only",
            "compute_inactive_shadow_deltas_read_only",
            "write_G_Tradex_watch_artifacts",
            "escalate_to_manual_review_only_when_trigger_conditions_pass",
        ],
    }


def _decision(
    input_audit: Mapping[str, Any],
    replay_result: Mapping[str, Any],
    no_mutation: Mapping[str, Any],
    source_complete: Mapping[str, Any],
) -> tuple[str, str]:
    if input_audit.get("required_inputs_present") is not True:
        return "watch_mode_plan_blocked", "missing_required_recent_days_replay_inputs"
    if source_complete.get("complete") is not True:
        return "watch_mode_plan_blocked", "source_recent_days_replay_artifact_incomplete"
    if no_mutation.get("no_mutation_pass") is not True:
        return "watch_mode_plan_blocked", "source_recent_days_replay_no_mutation_failed"
    if replay_result.get("decision") != "hold_for_watch_mode":
        return "watch_mode_plan_blocked", "source_recent_days_replay_not_watch_mode_hold"
    return "watch_mode_ready", "recent_days_replay_supports_watch_only_logging_with_activation_blocked"


def _next_axis_recommendation(decision: str) -> dict[str, Any]:
    if decision == "watch_mode_ready":
        next_axis = "teppan_shadow_watch_mode_logging_v1"
    else:
        next_axis = "repair_teppan_shadow_watch_mode_plan_inputs_v1"
    return {
        "schema_version": "teppan_shadow_watch_mode_next_axis_recommendation_v1",
        "decision": decision,
        "next": next_axis,
        "activation_allowed": False,
        "manual_review_next_if_triggered": "teppan_shadow_candidate_manual_review_v1",
    }


def _input_audit(root: Path) -> dict[str, Any]:
    presence = {name: (root / name).exists() for name in REQUIRED_INPUTS}
    return {
        "schema_version": "teppan_shadow_watch_mode_input_audit_v1",
        "source_recent_days_replay_root": str(root),
        "required_inputs": REQUIRED_INPUTS,
        "present_inputs": presence,
        "required_inputs_present": all(presence.values()),
    }


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "teppan_shadow_watch_mode_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": decision.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _not_changed() -> list[str]:
    return [
        "active_rank",
        "display_score",
        "runtime_duckdb",
        "production_publish_registry",
        "frontend_ui",
        "backend_api_response",
        "boost_value",
        "loss_guard_semantics",
        "pattern_definitions",
    ]


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
