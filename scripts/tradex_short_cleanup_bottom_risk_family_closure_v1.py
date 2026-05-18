from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCHEMA_PREFIX = "tradex_short_cleanup_bottom_risk_family_closure_v1"
VARIANT_ID = "short_cleanup_bottom_risk_v1"

DEFAULT_SOURCE_ROOT = Path(
    r"G:\Tradex\entry_precision_short_bottom_risk_exposure_guard_v1"
    r"\short_bottom_risk_exposure_guard_v1-guard-20260517T053547Z"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_cleanup_bottom_risk_family_closure_v1")

REQUIRED_OUTPUTS = [
    "short_cleanup_bottom_risk_family_closure_contract.json",
    "short_cleanup_bottom_risk_lineage_summary.json",
    "short_cleanup_bottom_risk_metric_rollup.json",
    "short_cleanup_bottom_risk_failure_diagnosis.json",
    "short_cleanup_bottom_risk_final_decision.json",
    "no_lookahead_audit_rollup.json",
    "_ARTIFACT_COMPLETE.json",
]

LINEAGE_STAGES: list[dict[str, Any]] = [
    {
        "stage_name": "entry_precision_short_audit",
        "authoritative_artifact_root": Path(r"G:\Tradex\entry_precision_short_audit_v1"),
        "decision_artifact": "entry_precision_short_decision.json",
        "supporting_artifacts": [
            "entry_precision_short_challenger_compare.json",
            "entry_precision_short_error_taxonomy.json",
            "entry_precision_short_feature_map.json",
            "entry_precision_short_report.md",
        ],
        "decision_field": "overall_decision",
        "no_lookahead_artifact": None,
    },
    {
        "stage_name": "entry_precision_short_broad_down_closepos",
        "authoritative_artifact_root": Path(r"G:\Tradex\entry_precision_short_broad_down_closepos_audit_v1"),
        "decision_artifact": "entry_precision_short_broad_down_closepos_fix_decision.json",
        "supporting_artifacts": [
            "entry_precision_short_broad_down_closepos_ablation_compare.json",
            "entry_precision_short_broad_down_closepos_quality_compare.json",
            "entry_precision_short_broad_down_closepos_reentry_audit.json",
            "entry_precision_short_broad_down_closepos_contract.json",
            "entry_precision_short_broad_down_closepos_fix_report.md",
        ],
        "decision_field": "overall_decision",
        "no_lookahead_artifact": None,
    },
    {
        "stage_name": "entry_precision_short_broad_down_monthly_fix",
        "authoritative_artifact_root": Path(r"G:\Tradex\entry_precision_short_broad_down_monthly_fix_audit_v1"),
        "decision_artifact": "entry_precision_short_broad_down_monthly_fix_decision.json",
        "supporting_artifacts": [
            "entry_precision_short_broad_down_monthly_ablation_compare.json",
            "entry_precision_short_broad_down_monthly_quality_compare.json",
            "entry_precision_short_broad_down_monthly_reentry_audit.json",
            "entry_precision_short_broad_down_fix_contract.json",
            "entry_precision_short_broad_down_monthly_fix_report.md",
        ],
        "decision_field": "overall_decision",
        "no_lookahead_artifact": None,
    },
    {
        "stage_name": "entry_precision_short_bottom_risk_diagnostic",
        "authoritative_artifact_root": Path(
            r"G:\Tradex\entry_precision_short_bottom_risk_diagnostic_v1"
            r"\20260517T024751Z-entry-short-bottom-risk-diagnostic-v1"
        ),
        "decision_artifact": "short_bottom_risk_failure_diagnosis.json",
        "supporting_artifacts": [
            "short_bottom_risk_next_axis_decision.json",
            "short_bottom_risk_feature_comparison.json",
            "short_bottom_risk_confusion_groups.csv",
            "short_bottom_risk_removed_good_shorts.csv",
            "short_bottom_risk_retained_bad_shorts.csv",
            "short_bottom_risk_diagnostic_contract.json",
        ],
        "decision_field": "decision",
        "no_lookahead_artifact": "no_lookahead_audit.json",
    },
    {
        "stage_name": "entry_precision_short_bottom_risk_closed_horizon_stability",
        "authoritative_artifact_root": Path(
            r"G:\Tradex\entry_precision_short_bottom_risk_closed_horizon_stability_v1"
            r"\20260517T030047Z-entry-short-bottom-risk-closed-horizon-stability-v1"
        ),
        "decision_artifact": "short_bottom_risk_stability_decision.json",
        "supporting_artifacts": [
            "short_bottom_risk_closed_horizon_compare.json",
            "short_bottom_risk_monthly_stability.json",
            "short_bottom_risk_unknown_impact.json",
            "short_bottom_risk_closed_horizon_contract.json",
        ],
        "decision_field": "decision",
        "no_lookahead_artifact": "no_lookahead_audit.json",
    },
    {
        "stage_name": "entry_precision_short_bottom_risk_maturity_gate",
        "authoritative_artifact_root": Path(
            r"G:\Tradex\entry_precision_short_bottom_risk_maturity_gate_v1"
            r"\20260517T032317Z-entry-short-bottom-risk-maturity-gate-v1"
        ),
        "decision_artifact": "short_bottom_risk_frozen_watch_decision.json",
        "supporting_artifacts": [
            "short_bottom_risk_maturity_calendar.json",
            "short_bottom_risk_recheck_plan.json",
            "short_bottom_risk_recheck_acceptance_gate.json",
            "short_bottom_risk_maturity_gate_contract.json",
            "short_bottom_risk_unknown_rows.csv",
        ],
        "decision_field": "decision",
        "no_lookahead_artifact": "no_lookahead_audit.json",
    },
    {
        "stage_name": "entry_precision_short_bottom_risk_full_recheck",
        "authoritative_artifact_root": Path(
            r"G:\Tradex\entry_precision_short_bottom_risk_full_recheck_v1"
            r"\20260517T034734Z-entry-short-bottom-risk-full-recheck-v1"
        ),
        "decision_artifact": "short_bottom_risk_full_recheck_decision.json",
        "supporting_artifacts": [
            "short_bottom_risk_full_recheck_compare.json",
            "short_bottom_risk_full_recheck_monthly_stability.json",
            "short_bottom_risk_full_recheck_unknown_resolution.json",
            "short_bottom_risk_full_recheck_confusion_groups.csv",
            "short_bottom_risk_full_recheck_contract.json",
        ],
        "decision_field": "decision",
        "no_lookahead_artifact": "no_lookahead_audit.json",
    },
    {
        "stage_name": "entry_precision_short_bottom_risk_stability_replay",
        "authoritative_artifact_root": Path(
            r"G:\Tradex\entry_precision_short_bottom_risk_stability_replay_v1"
            r"\20260517T041737Z-entry-short-bottom-risk-stability-replay-v1"
        ),
        "decision_artifact": "short_bottom_risk_stability_replay_decision.json",
        "supporting_artifacts": [
            "short_bottom_risk_snapshot_stability.json",
            "short_bottom_risk_monthly_stability_replay.json",
            "short_bottom_risk_regime_stability.json",
            "short_bottom_risk_borrow_proxy_report.json",
            "short_bottom_risk_stability_replay_contract.json",
        ],
        "decision_field": "decision",
        "no_lookahead_artifact": "no_lookahead_audit.json",
    },
    {
        "stage_name": "entry_precision_short_bottom_risk_borrow_decomposition",
        "authoritative_artifact_root": Path(
            r"G:\Tradex\entry_precision_short_bottom_risk_borrow_decomposition_v1"
            r"\short_cleanup_bottom_risk_v1-borrow-decomposition-20260517T051125Z"
        ),
        "decision_artifact": "short_bottom_risk_borrow_decomposition_decision.json",
        "supporting_artifacts": [
            "short_bottom_risk_borrow_bucket_events.csv",
            "short_bottom_risk_borrow_bucket_summary.json",
            "short_bottom_risk_soft_cost_concentration.json",
            "short_bottom_risk_borrow_adjusted_compare.json",
            "short_bottom_risk_borrow_decomposition_contract.json",
        ],
        "decision_field": "decision",
        "no_lookahead_artifact": "no_lookahead_audit.json",
    },
    {
        "stage_name": "entry_precision_short_bottom_risk_exposure_guard",
        "authoritative_artifact_root": Path(
            r"G:\Tradex\entry_precision_short_bottom_risk_exposure_guard_v1"
            r"\short_bottom_risk_exposure_guard_v1-guard-20260517T053547Z"
        ),
        "decision_artifact": "short_bottom_risk_exposure_guard_decision.json",
        "supporting_artifacts": [
            "short_bottom_risk_exposure_guard_compare.json",
            "short_bottom_risk_size_reduction_compare.json",
            "short_bottom_risk_borrow_caveat_compare.json",
            "short_bottom_risk_bad_exposure_reduction.json",
            "short_bottom_risk_exposure_guard_contract.json",
        ],
        "decision_field": "decision",
        "no_lookahead_artifact": "no_lookahead_audit.json",
    },
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _optional_load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _decision_value(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if value is None:
        raise KeyError(f"missing decision field {field!r} in payload keys={sorted(payload.keys())}")
    return str(value)


def _stage_record(spec: Mapping[str, Any]) -> dict[str, Any]:
    root = Path(spec["authoritative_artifact_root"])
    decision_path = root / str(spec["decision_artifact"])
    decision_payload = _load_json(decision_path)
    no_lookahead_path = spec.get("no_lookahead_artifact")
    no_lookahead_payload = None if no_lookahead_path is None else _optional_load_json(root / str(no_lookahead_path))
    supporting_artifacts = [str(root / str(name)) for name in spec.get("supporting_artifacts") or []]
    record = {
        "stage_name": str(spec["stage_name"]),
        "authoritative_artifact_root": str(root),
        "decision_artifact": str(decision_path),
        "supporting_artifacts": supporting_artifacts,
        "decision": _decision_value(decision_payload, str(spec["decision_field"])),
        "no_lookahead_artifact": None if no_lookahead_path is None else str(root / str(no_lookahead_path)),
        "no_lookahead_pass": None,
        "production_ranking_changed": bool(decision_payload.get("production_ranking_changed")) if "production_ranking_changed" in decision_payload else None,
        "active_champion_changed": bool(decision_payload.get("active_champion_changed")) if "active_champion_changed" in decision_payload else None,
        "publish_run": bool(decision_payload.get("publish_run")) if "publish_run" in decision_payload else None,
        "live_sell_signal_added": bool(decision_payload.get("live_sell_signal_added")) if "live_sell_signal_added" in decision_payload else None,
        "research_fallback": bool(decision_payload.get("research_fallback")) if "research_fallback" in decision_payload else None,
        "silent_fallback_used": bool(decision_payload.get("silent_fallback_used")) if "silent_fallback_used" in decision_payload else None,
        "decision_payload": decision_payload,
    }
    if no_lookahead_payload is not None:
        record["no_lookahead_pass"] = bool(no_lookahead_payload.get("no_lookahead_pass"))
        record["no_lookahead_artifact_payload"] = no_lookahead_payload
    else:
        record["no_lookahead_artifact_payload"] = None
    return record


def _build_lineage_summary(stage_records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_lineage_summary_v1",
        "generated_at": _utc_now(),
        "family_name": VARIANT_ID,
        "final_decision": "closed_as_research_drop",
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "production_state_changed": False,
        "meemee_changed": False,
        "lineage_stage_count": len(stage_records),
        "authoritative_artifact_roots": [record["authoritative_artifact_root"] for record in stage_records],
        "stages": [
            {
                "stage_name": record["stage_name"],
                "authoritative_artifact_root": record["authoritative_artifact_root"],
                "decision_artifact": record["decision_artifact"],
                "decision": record["decision"],
                "no_lookahead_artifact": record["no_lookahead_artifact"],
                "no_lookahead_pass": record["no_lookahead_pass"],
                "production_ranking_changed": record["production_ranking_changed"],
                "active_champion_changed": record["active_champion_changed"],
                "publish_run": record["publish_run"],
                "live_sell_signal_added": record["live_sell_signal_added"],
            }
            for record in stage_records
        ],
    }


def _stage_summary(stage_records: list[dict[str, Any]], stage_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
    for record in stage_records:
        if record["stage_name"] == stage_name:
            return record, record["decision_payload"]
    raise KeyError(stage_name)


def _build_metric_rollup(stage_records: list[dict[str, Any]]) -> dict[str, Any]:
    stage_map = {record["stage_name"]: record for record in stage_records}
    stage_payloads = {name: record["decision_payload"] for name, record in stage_map.items()}
    exposure_root = Path(stage_map["entry_precision_short_bottom_risk_exposure_guard"]["authoritative_artifact_root"])
    bad_exposure_reduction = _load_json(exposure_root / "short_bottom_risk_bad_exposure_reduction.json")
    bad_exposure_summary = bad_exposure_reduction.get("summary") or {}

    return {
        "schema_version": f"{SCHEMA_PREFIX}_metric_rollup_v1",
        "generated_at": _utc_now(),
        "family_name": VARIANT_ID,
        "source_lineage_decisions": {name: record["decision"] for name, record in stage_map.items()},
        "source_lineage_roots": [record["authoritative_artifact_root"] for record in stage_records],
        "final_chain_status": "closed_as_research_drop",
        "entry_precision_audit": {
            "decision": stage_map["entry_precision_short_audit"]["decision"],
        },
        "close_pos_follow_up": {
            "decision": stage_map["entry_precision_short_broad_down_closepos"]["decision"],
        },
        "monthly_alignment_follow_up": {
            "decision": stage_map["entry_precision_short_broad_down_monthly_fix"]["decision"],
        },
        "bottom_risk_diagnostic": {
            "decision": stage_map["entry_precision_short_bottom_risk_diagnostic"]["decision"],
            "known_outcome_rows": stage_payloads["entry_precision_short_bottom_risk_diagnostic"].get("known_outcome_rows"),
            "known_evidence_rows": stage_payloads["entry_precision_short_bottom_risk_diagnostic"].get("known_evidence_rows"),
            "total_selected_rows": stage_payloads["entry_precision_short_bottom_risk_diagnostic"].get("total_selected_rows"),
            "sample_shrinkage_only": stage_payloads["entry_precision_short_bottom_risk_diagnostic"].get("sample_shrinkage_only"),
            "true_bad_pick_removal_visible": stage_payloads["entry_precision_short_bottom_risk_diagnostic"].get("true_bad_pick_removal_visible"),
        },
        "closed_horizon_stability": {
            "decision": stage_map["entry_precision_short_bottom_risk_closed_horizon_stability"]["decision"],
            "closed_horizon_gain_persists": stage_payloads["entry_precision_short_bottom_risk_closed_horizon_stability"].get("closed_horizon_gain_persists"),
            "monthly_stability": stage_payloads["entry_precision_short_bottom_risk_closed_horizon_stability"].get("monthly_stability"),
            "unknown_impact": stage_payloads["entry_precision_short_bottom_risk_closed_horizon_stability"].get("unknown_impact"),
        },
        "maturity_gate": {
            "decision": stage_map["entry_precision_short_bottom_risk_maturity_gate"]["decision"],
            "current_unknown_row_count": stage_payloads["entry_precision_short_bottom_risk_maturity_gate"].get("current_unknown_row_count"),
            "current_partial_recheck_ready_now": stage_payloads["entry_precision_short_bottom_risk_maturity_gate"].get("current_partial_recheck_ready_now"),
            "current_full_recheck_ready_now": stage_payloads["entry_precision_short_bottom_risk_maturity_gate"].get("current_full_recheck_ready_now"),
            "source_unknown_materiality": stage_payloads["entry_precision_short_bottom_risk_maturity_gate"].get("source_unknown_materiality"),
        },
        "full_recheck": {
            "decision": stage_map["entry_precision_short_bottom_risk_full_recheck"]["decision"],
            "full_recheck_gain_persists": stage_payloads["entry_precision_short_bottom_risk_full_recheck"].get("full_recheck_gain_persists"),
            "full_recheck_monthly_not_worse": stage_payloads["entry_precision_short_bottom_risk_full_recheck"].get("full_recheck_monthly_not_worse"),
            "unknown_rows_fully_resolved": stage_payloads["entry_precision_short_bottom_risk_full_recheck"].get("unknown_rows_fully_resolved"),
            "resolution_summary": stage_payloads["entry_precision_short_bottom_risk_full_recheck"].get("resolution_summary"),
        },
        "stability_replay": {
            "decision": stage_map["entry_precision_short_bottom_risk_stability_replay"]["decision"],
            "paper_replay_ready": stage_payloads["entry_precision_short_bottom_risk_stability_replay"].get("paper_replay_ready"),
            "shadow_paper_replay_candidate": stage_payloads["entry_precision_short_bottom_risk_stability_replay"].get("shadow_paper_replay_candidate"),
            "edge_survives_outside_source_snapshot": stage_payloads["entry_precision_short_bottom_risk_stability_replay"].get("edge_survives_outside_source_snapshot"),
            "no_contrary_snapshot": stage_payloads["entry_precision_short_bottom_risk_stability_replay"].get("no_contrary_snapshot"),
            "snapshot_stability": stage_payloads["entry_precision_short_bottom_risk_stability_replay"].get("snapshot_stability"),
            "monthly_stability": stage_payloads["entry_precision_short_bottom_risk_stability_replay"].get("monthly_stability"),
            "regime_support_summary": stage_payloads["entry_precision_short_bottom_risk_stability_replay"].get("regime_support_summary"),
            "borrow_proxy_summary": stage_payloads["entry_precision_short_bottom_risk_stability_replay"].get("borrow_proxy_summary"),
        },
        "borrow_decomposition": {
            "decision": stage_map["entry_precision_short_bottom_risk_borrow_decomposition"]["decision"],
            "paper_replay_ready": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("paper_replay_ready"),
            "borrow_caveated_paper_replay_candidate": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_caveated_paper_replay_candidate"),
            "selected_event_count": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_summary", {}).get("selected_event_count"),
            "selected_code_count": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_summary", {}).get("selected_code_count"),
            "hard_borrow_gap_event_count": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_summary", {}).get("hard_borrow_gap_event_count"),
            "soft_borrow_cost_event_count": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_summary", {}).get("soft_borrow_cost_event_count"),
            "clean_borrowable_event_count": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_summary", {}).get("clean_borrowable_event_count"),
            "clean_sample_too_small": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_adjusted_compare", {}).get("dependency_readout", {}).get("clean_sample_too_small"),
            "edge_depends_on_soft_cost_names": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_adjusted_compare", {}).get("dependency_readout", {}).get("edge_depends_on_soft_cost_names"),
            "borrow_summary": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_summary"),
            "borrow_adjusted_compare": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("borrow_adjusted_compare"),
            "soft_cost_concentration": stage_payloads["entry_precision_short_bottom_risk_borrow_decomposition"].get("soft_cost_concentration"),
        },
        "exposure_guard": {
            "decision": stage_map["entry_precision_short_bottom_risk_exposure_guard"]["decision"],
            "borrow_caveated_guard_candidate": stage_payloads["entry_precision_short_bottom_risk_exposure_guard"].get("borrow_caveated_guard_candidate"),
            "short_exposure_reduction_guard_candidate": stage_payloads["entry_precision_short_bottom_risk_exposure_guard"].get("short_exposure_reduction_guard_candidate"),
            "harmful_short_exposure_reduced": stage_payloads["entry_precision_short_bottom_risk_exposure_guard"].get("authoritative_metrics", {}).get("harmful_short_exposure_reduced"),
            "edge_depends_on_soft_cost_names": stage_payloads["entry_precision_short_bottom_risk_exposure_guard"].get("authoritative_metrics", {}).get("edge_depends_on_soft_cost_names"),
            "clean_sample_too_small": stage_payloads["entry_precision_short_bottom_risk_exposure_guard"].get("authoritative_metrics", {}).get("clean_sample_too_small"),
            "production_state_changed": False,
            "meeMee_changed": False,
            "authoritative_metrics": stage_payloads["entry_precision_short_bottom_risk_exposure_guard"].get("authoritative_metrics"),
            "baseline_summary": stage_payloads["entry_precision_short_bottom_risk_exposure_guard"].get("baseline_summary"),
            "criteria_state": stage_payloads["entry_precision_short_bottom_risk_exposure_guard"].get("criteria_state"),
            "decision_reasons": stage_payloads["entry_precision_short_bottom_risk_exposure_guard"].get("decision_reasons"),
            "bad_exposure_reduction": bad_exposure_reduction,
            "harmful_short_exposure_reduced": bad_exposure_summary.get("harmful_short_exposure_reduced"),
            "edge_depends_on_soft_cost_names": bad_exposure_summary.get("edge_depends_on_soft_cost_names"),
            "clean_sample_too_small": bad_exposure_summary.get("clean_sample_too_small"),
            "good_short_overblocked": bad_exposure_summary.get("good_short_overblocked"),
            "flagged_subset_mean_ret20": bad_exposure_summary.get("flagged_subset_mean_ret20"),
            "unguarded_subset_mean_ret20": bad_exposure_summary.get("unguarded_subset_mean_ret20"),
        },
    }


def _build_no_lookahead_rollup(stage_records: list[dict[str, Any]]) -> dict[str, Any]:
    stages: list[dict[str, Any]] = []
    available_count = 0
    passed_count = 0
    for record in stage_records:
        available = record["no_lookahead_pass"] is not None
        if available:
            available_count += 1
            if record["no_lookahead_pass"]:
                passed_count += 1
        stages.append(
            {
                "stage_name": record["stage_name"],
                "authoritative_artifact_root": record["authoritative_artifact_root"],
                "decision_artifact": record["decision_artifact"],
                "no_lookahead_available": available,
                "no_lookahead_pass": record["no_lookahead_pass"],
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_rollup_v1",
        "generated_at": _utc_now(),
        "family_name": VARIANT_ID,
        "overall_no_lookahead_pass": available_count == passed_count,
        "available_stage_count": available_count,
        "passed_stage_count": passed_count,
        "stages": stages,
    }


def _build_failure_diagnosis(stage_records: list[dict[str, Any]], metric_rollup: Mapping[str, Any], no_lookahead_rollup: Mapping[str, Any]) -> dict[str, Any]:
    borrow = metric_rollup["borrow_decomposition"]
    exposure = metric_rollup["exposure_guard"]
    full_recheck = metric_rollup["full_recheck"]
    stability = metric_rollup["stability_replay"]
    closed = metric_rollup["closed_horizon_stability"]
    diagnostic = metric_rollup["bottom_risk_diagnostic"]
    borrow_summary = borrow.get("borrow_summary") or {}
    borrow_dependency = (borrow.get("borrow_adjusted_compare") or {}).get("dependency_readout") or {}
    exposure_metrics = exposure.get("authoritative_metrics") or {}
    exposure_criteria = exposure.get("criteria_state") or {}

    evidence = [
        {
            "reason": "soft_cost_dependency_too_high",
            "evidence": {
                "edge_depends_on_soft_cost_names": bool(borrow_dependency.get("edge_depends_on_soft_cost_names")),
                "soft_borrow_cost_event_count": borrow_summary.get("soft_borrow_cost_event_count"),
                "clean_borrowable_event_count": borrow_summary.get("clean_borrowable_event_count"),
                "soft_cost_event_share": borrow_summary.get("soft_borrow_cost_event_share"),
                "clean_borrowable_event_share": borrow_summary.get("clean_borrowable_event_share"),
                "flagged_subset_mean_ret20": exposure_metrics.get("flagged_subset_mean_ret20"),
                "flagged_good_count": exposure_metrics.get("flagged_good_count"),
                "flagged_bad_count": exposure_metrics.get("flagged_bad_count"),
                "exposure_guard_decision": stage_records[-1]["decision"],
            },
        },
        {
            "reason": "insufficient_clean_borrowable_sample",
            "evidence": {
                "clean_borrowable_event_count": borrow_summary.get("clean_borrowable_event_count"),
                "clean_sample_too_small": bool(borrow.get("clean_sample_too_small")),
                "clean_only_gate_breadth_ok": False,
                "paper_replay_ready": borrow.get("paper_replay_ready"),
            },
        },
        {
            "reason": "paper_replay_blocked",
            "evidence": {
                "borrow_decomposition_decision": stage_records[-2]["decision"],
                "stability_replay_decision": stage_records[-3]["decision"],
                "paper_replay_ready": stability.get("paper_replay_ready"),
                "shadow_paper_replay_candidate": stability.get("shadow_paper_replay_candidate"),
            },
        },
        {
            "reason": "exposure_guard_damages_edge",
            "evidence": {
                "baseline_mean_ret20": exposure_metrics.get("baseline_mean_ret20"),
                "baseline_median_ret20": exposure_metrics.get("baseline_median_ret20"),
                "full_veto_mean_ret20": exposure_metrics.get("full_veto_mean_ret20"),
                "full_veto_median_ret20": exposure_metrics.get("full_veto_median_ret20"),
                "size_reducer_mean_ret20": exposure_metrics.get("size_reducer_mean_ret20"),
                "size_reducer_median_ret20": exposure_metrics.get("size_reducer_median_ret20"),
                "flagged_subset_mean_ret20": exposure_metrics.get("flagged_subset_mean_ret20"),
                "unflagged_subset_mean_ret20": exposure_metrics.get("unflagged_subset_mean_ret20"),
                "size_reducer_selected_short_count_impact": None if exposure_metrics.get("size_reducer_effective_selected_short_count") is None else float((exposure_metrics.get("size_reducer_effective_selected_short_count") or 0) - (exposure_metrics.get("baseline_effective_selected_short_count") or 0)),
                "hard_borrow_gap_event_count": borrow.get("hard_borrow_gap_event_count"),
                "soft_borrow_cost_event_count": borrow.get("soft_borrow_cost_event_count"),
                "criteria_state": exposure_criteria,
            },
        },
        {
            "reason": "not_production_candidate",
            "evidence": {
                "production_candidate": False,
                "production_state_changed": False,
                "publish_allowed": False,
                "live_sell_signal_allowed": False,
            },
        },
        {
            "reason": "meemee_reflectable_false",
            "evidence": {
                "meemee_reflectable": False,
                "meeMee_changed": False,
                "family_status": "closed",
            },
        },
    ]

    return {
        "schema_version": f"{SCHEMA_PREFIX}_failure_diagnosis_v1",
        "generated_at": _utc_now(),
        "family_name": VARIANT_ID,
        "decision": "closed_as_research_drop",
        "typed_reasons": [
            "soft_cost_dependency_too_high",
            "insufficient_clean_borrowable_sample",
            "paper_replay_blocked",
            "exposure_guard_damages_edge",
            "not_production_candidate",
            "meemee_reflectable_false",
        ],
        "root_cause": "operational_conversion_failed_even_though_research_edge_survived",
        "status": "closed",
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "production_state_changed": False,
        "meeMee_changed": False,
        "no_lookahead_pass": bool(no_lookahead_rollup.get("overall_no_lookahead_pass")),
        "blockers": evidence,
        "supporting_metrics": {
            "closed_horizon_decision": closed.get("decision"),
            "full_recheck_decision": full_recheck.get("decision"),
            "stability_replay_decision": stability.get("decision"),
            "borrow_decomposition_decision": borrow.get("decision"),
            "exposure_guard_decision": stage_records[-1]["decision"],
            "borrow_clean_sample_too_small": borrow.get("clean_sample_too_small"),
            "borrow_edge_depends_on_soft_cost_names": borrow.get("edge_depends_on_soft_cost_names"),
            "exposure_guard_harmful_short_exposure_reduced": exposure.get("harmful_short_exposure_reduced"),
            "exposure_guard_edge_depends_on_soft_cost_names": exposure.get("edge_depends_on_soft_cost_names"),
            "diagnostic_next_axis_justified": diagnostic.get("next_axis_justified") if isinstance(diagnostic, Mapping) else None,
            "closed_horizon_gain_persists": closed.get("closed_horizon_gain_persists"),
            "full_recheck_gain_persists": full_recheck.get("full_recheck_gain_persists"),
            "edge_survives_outside_source_snapshot": stability.get("edge_survives_outside_source_snapshot"),
        },
        "lineage_roots": [record["authoritative_artifact_root"] for record in stage_records],
        "production_state_changed": False,
        "meeMee_changed": False,
    }


def _build_final_decision(failure_diagnosis: Mapping[str, Any], stage_records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_final_decision_v1",
        "generated_at": _utc_now(),
        "family_name": VARIANT_ID,
        "decision": "closed_as_research_drop",
        "final_decision": "closed_as_research_drop",
        "decision_reasons": list(failure_diagnosis["typed_reasons"]),
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "production_state_changed": False,
        "meeMee_changed": False,
        "no_lookahead_pass": bool(failure_diagnosis.get("no_lookahead_pass")),
        "authoritative_artifact_root": str(DEFAULT_SOURCE_ROOT),
        "lineage_roots": [record["authoritative_artifact_root"] for record in stage_records],
        "lineage_stage_count": len(stage_records),
        "final_source_decision": stage_records[-1]["decision"],
        "final_source_artifact_root": stage_records[-1]["authoritative_artifact_root"],
        "final_source_decision_artifact": stage_records[-1]["decision_artifact"],
        "production_blocking_reasons": list(failure_diagnosis["typed_reasons"]),
    }


def _artifact_complete(output_root: Path) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "complete": True,
        "status": "complete",
        "family_name": VARIANT_ID,
        "artifact_refs": {
            "short_cleanup_bottom_risk_family_closure_contract": str(output_root / "short_cleanup_bottom_risk_family_closure_contract.json"),
            "short_cleanup_bottom_risk_lineage_summary": str(output_root / "short_cleanup_bottom_risk_lineage_summary.json"),
            "short_cleanup_bottom_risk_metric_rollup": str(output_root / "short_cleanup_bottom_risk_metric_rollup.json"),
            "short_cleanup_bottom_risk_failure_diagnosis": str(output_root / "short_cleanup_bottom_risk_failure_diagnosis.json"),
            "short_cleanup_bottom_risk_final_decision": str(output_root / "short_cleanup_bottom_risk_final_decision.json"),
            "no_lookahead_audit_rollup": str(output_root / "no_lookahead_audit_rollup.json"),
        },
        "required_outputs": REQUIRED_OUTPUTS,
    }


def run(*, source_root: Path = DEFAULT_SOURCE_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    if Path(source_root).resolve(strict=False) != DEFAULT_SOURCE_ROOT.resolve(strict=False):
        raise ValueError(f"unexpected source_root: {source_root}")
    stage_records = [_stage_record(spec) for spec in LINEAGE_STAGES]
    lineage_summary = _build_lineage_summary(stage_records)
    metric_rollup = _build_metric_rollup(stage_records)
    no_lookahead_rollup = _build_no_lookahead_rollup(stage_records)
    failure_diagnosis = _build_failure_diagnosis(stage_records, metric_rollup, no_lookahead_rollup)
    final_decision = _build_final_decision(failure_diagnosis, stage_records)
    run_dir = Path(output_root).expanduser().resolve(strict=False) / f"{_utc_stamp()}-short-cleanup-bottom-risk-family-closure-v1"
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json(
        run_dir / "short_cleanup_bottom_risk_family_closure_contract.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
            "generated_at": _utc_now(),
            "family_name": VARIANT_ID,
            "source_root": str(DEFAULT_SOURCE_ROOT),
            "final_decision_expected": "closed_as_research_drop",
            "decision_labels": ["closed_as_research_drop"],
            "typed_reasons": failure_diagnosis["typed_reasons"],
            "fixed_constraints": {
                "traDEX_only": True,
                "no_new_short_rule": True,
                "no_threshold_tuning": True,
                "no_borrow_proxy_changes": True,
                "no_close_pos_changes": True,
                "no_monthly_alignment_changes": True,
                "no_long_logic_changes": True,
                "no_cost_model_changes": True,
                "no_paper_replay": True,
                "no_meemee_changes": True,
                "no_production_ranking_changes": True,
                "no_active_champion_changes": True,
                "no_publish": True,
                "no_live_sell_signal": True,
            },
            "input_artifacts": {
                "exposure_guard_root": str(DEFAULT_SOURCE_ROOT),
                "lineage_roots": [record["authoritative_artifact_root"] for record in stage_records],
            },
            "required_outputs": REQUIRED_OUTPUTS,
        },
    )
    _write_json(run_dir / "short_cleanup_bottom_risk_lineage_summary.json", lineage_summary)
    _write_json(run_dir / "short_cleanup_bottom_risk_metric_rollup.json", metric_rollup)
    _write_json(run_dir / "short_cleanup_bottom_risk_failure_diagnosis.json", failure_diagnosis)
    _write_json(run_dir / "short_cleanup_bottom_risk_final_decision.json", final_decision)
    _write_json(run_dir / "no_lookahead_audit_rollup.json", no_lookahead_rollup)
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", _artifact_complete(run_dir))

    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": final_decision["decision"],
        "family_name": VARIANT_ID,
        "artifact_refs": {
            "short_cleanup_bottom_risk_family_closure_contract": str(run_dir / "short_cleanup_bottom_risk_family_closure_contract.json"),
            "short_cleanup_bottom_risk_lineage_summary": str(run_dir / "short_cleanup_bottom_risk_lineage_summary.json"),
            "short_cleanup_bottom_risk_metric_rollup": str(run_dir / "short_cleanup_bottom_risk_metric_rollup.json"),
            "short_cleanup_bottom_risk_failure_diagnosis": str(run_dir / "short_cleanup_bottom_risk_failure_diagnosis.json"),
            "short_cleanup_bottom_risk_final_decision": str(run_dir / "short_cleanup_bottom_risk_final_decision.json"),
            "no_lookahead_audit_rollup": str(run_dir / "no_lookahead_audit_rollup.json"),
            "_ARTIFACT_COMPLETE": str(run_dir / "_ARTIFACT_COMPLETE.json"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Close the short_cleanup_bottom_risk_v1 research axis as a TRADEX-only research drop.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(source_root=Path(args.source_root), output_root=Path(args.output_root))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
