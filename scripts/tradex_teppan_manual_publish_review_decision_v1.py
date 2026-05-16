from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "teppan_ranking_branching_probe_v1"
SCHEMA_PREFIX = "tradex_teppan_manual_publish_review_decision_v1"

DEFAULT_PATTERN_ROOT = Path(
    r"G:\Tradex\teppan_chart_pattern_discovery_v1\20260514T000000Z-current-runtime-teppan-discovery-v1-teppan_chart_pattern_discovery_v1"
)
DEFAULT_GUARD_ROOT = Path(
    r"G:\Tradex\teppan_loss_guard_v1\20260514T000000Z-current-runtime-teppan-loss-guard-v1-teppan_loss_guard_v1"
)
DEFAULT_BRANCHING_PROBE_ROOT = Path(
    r"G:\Tradex\teppan_ranking_branching_probe_v1\20260514T010000Z-runtime-ranking-teppan-branching-probe-v1-teppan_ranking_branching_probe_v1"
)
DEFAULT_PUBLISH_REVIEW_GATE_ROOT = Path(
    r"G:\Tradex\publish_review_gates\teppan_ranking_branching_probe_v1\20260514T020000Z-teppan-ranking-branching-publish-review-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\manual_publish_reviews\teppan_ranking_branching_probe_v1")

DECISION_APPROVE = "approve_publish_implementation_plan"
DECISION_HOLD_VALIDATION = "hold_for_additional_validation"
DECISION_HOLD_PORTABILITY = "hold_for_feature_portability_gap"
DECISION_REJECT = "reject_publish_review"

REQUIRED_OUTPUTS = (
    "manual_publish_review_decision.json",
    "implementation_readiness_report.json",
    "runtime_reflection_gap_report.json",
    "feature_portability_report.json",
    "blocker_or_approval_report.json",
    "next_axis_recommendation.json",
    "_ARTIFACT_COMPLETE.json",
)

SHADOW_BUNDLE_REQUIRED_FILES = (
    "published_logic_artifact.json",
    "published_logic_manifest.json",
    "validation_summary.json",
    "source_artifact_refs.json",
    "ranking_adjustment_contract.json",
    "meemee_exposure_assessment.json",
    "bundle_manifest.json",
)

IMPLEMENTATION_PLANNING_TARGETS = (
    "app/backend/services/signal_tracking_service.py",
    "app/backend/services/runtime_selection_service.py",
    "shared/contracts/publish_registry.py",
    "app/backend/services/publish_registry_sync_service.py",
    "app/backend/services/publish_promotion_service.py",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _json_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    try:
        if pd.isna(value):
            return float(default)
    except (TypeError, ValueError):
        pass
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    try:
        if pd.isna(value):
            return int(default)
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _load_required(root: Path, names: tuple[str, ...]) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    payloads: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    for name in names:
        path = root / name
        if not path.exists():
            errors[name] = "missing"
            continue
        try:
            payloads[name] = _load_json(path)
        except Exception as exc:
            errors[name] = f"parse_error:{exc}"
    return payloads, errors


def _topk_delta(compare: dict[str, Any], topk: str, metric: str) -> float:
    return _safe_float((((compare.get("ranking_compare") or {}).get(topk) or {}).get("delta") or {}).get(metric), 0.0)


def _shadow_bundle_root(gate_root: Path, gate_decision: dict[str, Any], gate_complete: dict[str, Any]) -> Path:
    raw = gate_decision.get("shadow_bundle_root") or gate_complete.get("shadow_bundle_root") or ""
    return Path(str(raw)).expanduser().resolve() if str(raw).strip() else gate_root / "shadow_publish_bundle"


def _shadow_bundle_review(shadow_bundle_root: Path) -> dict[str, Any]:
    payloads, errors = _load_required(shadow_bundle_root, SHADOW_BUNDLE_REQUIRED_FILES)
    bundle_manifest = payloads.get("bundle_manifest.json") or {}
    logic_artifact = payloads.get("published_logic_artifact.json") or {}
    validation = payloads.get("validation_summary.json") or {}
    missing = [name for name in SHADOW_BUNDLE_REQUIRED_FILES if name in errors]
    forbidden_inputs = list(logic_artifact.get("forbidden_inputs") or [])
    required_inputs = list(logic_artifact.get("required_inputs") or [])
    forbidden_future_inputs_present = all(
        item in forbidden_inputs
        for item in ("forward_ret_20d", "future_return_labels", "realized_topk_membership_labels")
    )
    required_runtime_inputs_present = all(
        item in required_inputs
        for item in (
            "champion_rank",
            "champion_score",
            "runtime_ohlcv_history_up_to_anchor_date",
            "teppan_pattern_artifact",
            "teppan_loss_guard_artifact",
        )
    )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_shadow_bundle_review_v1",
        "generated_at": _utc_now(),
        "shadow_bundle_root": str(shadow_bundle_root),
        "required_files": list(SHADOW_BUNDLE_REQUIRED_FILES),
        "missing_files": missing,
        "parse_errors": errors,
        "bundle_status": bundle_manifest.get("bundle_status"),
        "bundle_manifest_required_files_present": bool(bundle_manifest.get("required_files_present")),
        "logic_id": logic_artifact.get("logic_id"),
        "logic_version": logic_artifact.get("logic_version"),
        "scorer_type": logic_artifact.get("scorer_type"),
        "required_inputs": required_inputs,
        "forbidden_inputs": forbidden_inputs,
        "required_runtime_inputs_present": required_runtime_inputs_present,
        "forbidden_future_inputs_present": forbidden_future_inputs_present,
        "validation_decision": validation.get("decision"),
        "metrics": validation.get("metrics") or {},
        "pass": (
            not missing
            and not errors
            and bundle_manifest.get("bundle_status") == "complete"
            and bool(bundle_manifest.get("required_files_present"))
            and logic_artifact.get("logic_version") == "static_teppan_guarded_soft_boost_v1"
            and required_runtime_inputs_present
            and forbidden_future_inputs_present
        ),
    }


def _feature_portability_report(feature_audit: dict[str, Any], ranking_contract: dict[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    hard_blockers: list[str] = []
    planning_gaps: list[str] = []
    for feature in feature_audit.get("features") or []:
        if not isinstance(feature, dict):
            continue
        name = str(feature.get("feature") or "")
        native = bool(feature.get("available_in_current_meemee_runtime_ranking_generation"))
        contract_available = bool(feature.get("available_for_publish_review_contract"))
        decision_time_safe = bool(feature.get("decision_time_safe"))
        depends_future = bool(feature.get("depends_on_future_label"))
        depends_research_label = bool(feature.get("depends_on_research_only_mining_labels"))
        missing_fields = list(feature.get("missing_fields") or [])
        if native and contract_available and decision_time_safe and not depends_future and not depends_research_label and not missing_fields:
            portability = "native_runtime_input"
        elif contract_available and decision_time_safe and not depends_future and not depends_research_label and not missing_fields:
            portability = "portable_with_shadow_integration_plan"
            planning_gaps.append(name)
        else:
            portability = "blocked"
            hard_blockers.append(name)
        rows.append(
            {
                "feature": name,
                "portability": portability,
                "available_for_publish_review_contract": contract_available,
                "available_in_current_meemee_runtime_ranking_generation": native,
                "decision_time_safe": decision_time_safe,
                "depends_on_future_label": depends_future,
                "depends_on_research_only_mining_labels": depends_research_label,
                "missing_fields": missing_fields,
                "source_file_or_artifact": feature.get("source_file_or_artifact"),
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_portability_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "pass": not hard_blockers,
        "runtime_native_features": [row["feature"] for row in rows if row["portability"] == "native_runtime_input"],
        "portable_with_shadow_integration_plan": [
            row["feature"] for row in rows if row["portability"] == "portable_with_shadow_integration_plan"
        ],
        "hard_blockers": hard_blockers,
        "planning_gaps": planning_gaps,
        "ranking_contract_required_inputs": list(ranking_contract.get("required_inputs") or []),
        "features": rows,
        "portability_judgment": "bounded_planning_gap" if planning_gaps and not hard_blockers else "native_or_ready" if not hard_blockers else "blocked",
    }


def _runtime_reflection_gap_report(feature_report: dict[str, Any], exposure: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_runtime_reflection_gap_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "direct_runtime_reflection_allowed_now": False,
        "runtime_reflection_implementation_required": True,
        "gap_state": "bounded_implementation_planning_gap" if feature_report.get("pass") else "feature_portability_blocked",
        "native_runtime_features": feature_report.get("runtime_native_features", []),
        "review_only_features_to_port": feature_report.get("portable_with_shadow_integration_plan", []),
        "allowed_future_meemee_exposure": exposure.get("allowed_future_meemee_exposure", []),
        "forbidden_meemee_exposure": exposure.get("forbidden_meemee_exposure", []),
        "what_must_remain_unchanged": [
            "MeeMee runtime ranking",
            "runtime DuckDB",
            "frontend",
            "backend API",
            "production publish registry",
            "boost value",
            "loss guard definition",
            "pattern definitions",
        ],
    }


def _strict_teppan_assessment(pattern_decision: dict[str, Any], policy: str) -> dict[str, Any]:
    teppan_count = _safe_int(pattern_decision.get("teppan_count"), 0)
    high_return = _safe_int(pattern_decision.get("high_return_count"), 0)
    high_win = _safe_int(pattern_decision.get("high_win_rate_count"), 0)
    if teppan_count > 0:
        classification = "strict_teppan_present"
        blocker = False
    elif policy == "validation_blocker":
        classification = "validation_confidence_blocker"
        blocker = True
    else:
        classification = "naming_threshold_limitation"
        blocker = False
    return {
        "strict_teppan_count": teppan_count,
        "high_return_count": high_return,
        "high_win_rate_count": high_win,
        "classification": classification,
        "is_blocker": blocker,
        "policy": policy,
        "usable_evidence_basis": "high-return / high-win pattern candidates + guard" if teppan_count == 0 else "strict teppan candidates + guard",
    }


def _implementation_readiness_report(
    *,
    decision: str,
    strict_teppan: dict[str, Any],
    feature_report: dict[str, Any],
    shadow_review: dict[str, Any],
    probe_decision: dict[str, Any],
    probe_compare: dict[str, Any],
) -> dict[str, Any]:
    ready = decision == DECISION_APPROVE
    return {
        "schema_version": f"{SCHEMA_PREFIX}_implementation_readiness_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "readiness_state": "ready_for_shadow_integration_plan" if ready else "not_ready_for_shadow_integration_plan",
        "approved_for_runtime_mutation": False,
        "approved_for_production_registration": False,
        "approved_for_publish_implementation_plan": ready,
        "candidate_logic_version": shadow_review.get("logic_version"),
        "implementation_scope": {
            "allowed_next_step": "write a shadow integration implementation plan only" if ready else "resolve manual review blockers first",
            "candidate_change_targets_for_next_plan": list(IMPLEMENTATION_PLANNING_TARGETS),
            "must_remain_shadow_or_planning_only": True,
        },
        "evidence_summary": {
            "candidate_local_decision": probe_decision.get("candidate_local_decision"),
            "authoritative_research_decision": probe_decision.get("authoritative_research_decision"),
            "changed_top5_members_count": probe_decision.get("changed_top5_members_count"),
            "changed_top10_members_count": probe_decision.get("changed_top10_members_count"),
            "changed_rank_count": probe_decision.get("changed_rank_count"),
            "top5_avg_ret20_delta": _topk_delta(probe_compare, "top5", "avg_ret20"),
            "top10_avg_ret20_delta": _topk_delta(probe_compare, "top10", "avg_ret20"),
            "top10_severe_loss_rate20_delta": _topk_delta(probe_compare, "top10", "severe_loss_rate20"),
            "strict_teppan_assessment": strict_teppan,
            "feature_portability_judgment": feature_report.get("portability_judgment"),
        },
    }


def _choose_decision(
    *,
    gate_decision: dict[str, Any],
    gate_complete: dict[str, Any],
    pattern_decision: dict[str, Any],
    guard_decision: dict[str, Any],
    probe_decision: dict[str, Any],
    probe_compare: dict[str, Any],
    probe_coverage: dict[str, Any],
    feature_report: dict[str, Any],
    shadow_review: dict[str, Any],
    strict_teppan: dict[str, Any],
    load_errors: dict[str, Any],
) -> tuple[str, str, list[str], list[str]]:
    reject_blockers: list[str] = []
    validation_holds: list[str] = []
    portability_holds: list[str] = []
    warnings: list[str] = []

    for label, errors in load_errors.items():
        if errors:
            reject_blockers.append(f"{label}_artifact_missing_or_unparseable")
    if gate_decision.get("decision") != "pass_to_manual_review":
        reject_blockers.append("publish_review_gate_not_passed")
    if gate_decision.get("blockers"):
        reject_blockers.append("publish_review_gate_has_blockers")
    for field in ("reproducibility_pass", "anti_leakage_pass", "source_artifact_integrity_pass", "feature_availability_pass"):
        if gate_decision.get(field) is not True:
            reject_blockers.append(f"gate_{field}_false")
    if gate_decision.get("no_meemee_mutation") is not True:
        reject_blockers.append("gate_meemee_mutation_not_excluded")
    if bool(gate_decision.get("production_ranking_changed")):
        reject_blockers.append("production_ranking_changed")
    if bool(gate_decision.get("meemee_reflectable_now")):
        reject_blockers.append("unexpected_direct_meemee_reflectable_state")
    if gate_complete.get("complete") is not True:
        reject_blockers.append("publish_review_gate_artifact_complete_false")
    if shadow_review.get("pass") is not True:
        reject_blockers.append("shadow_bundle_incomplete_or_invalid")
    if guard_decision.get("authoritative_research_decision") != "keep":
        validation_holds.append("loss_guard_not_keep")
    if probe_decision.get("decision") != "keep":
        validation_holds.append("branching_probe_not_keep")
    if pattern_decision.get("authoritative_research_decision") not in {"promising_patterns_found", "keep"}:
        validation_holds.append("pattern_discovery_not_promising")
    if _safe_int(probe_decision.get("changed_top5_members_count"), 0) <= 0 or _safe_int(probe_decision.get("changed_top10_members_count"), 0) <= 0:
        validation_holds.append("topk_branching_not_material")
    if _topk_delta(probe_compare, "top5", "avg_ret20") < 0.0 or _topk_delta(probe_compare, "top10", "avg_ret20") < 0.0:
        validation_holds.append("topk_expectancy_delta_negative")
    if _topk_delta(probe_compare, "top10", "severe_loss_rate20") > 0.0:
        validation_holds.append("top10_severe_loss_worse")
    if probe_coverage.get("complete_champion_ranking_available") is not True:
        validation_holds.append("champion_ranking_coverage_incomplete")
    if strict_teppan.get("is_blocker"):
        validation_holds.append("strict_teppan_count_zero_escalated_to_validation_blocker")
    elif strict_teppan.get("classification") == "naming_threshold_limitation":
        warnings.append("strict_teppan_count_zero_recorded_as_naming_threshold_limitation")
    if not feature_report.get("pass"):
        portability_holds.append("feature_portability_hard_blocker")

    if reject_blockers:
        return DECISION_REJECT, "review_rejected_by_integrity_or_safety_failure", reject_blockers, warnings
    if portability_holds:
        return DECISION_HOLD_PORTABILITY, "feature_portability_gap_blocks_publish_implementation_plan", portability_holds, warnings
    if validation_holds:
        return DECISION_HOLD_VALIDATION, "additional_validation_required_before_publish_implementation_plan", validation_holds, warnings
    return DECISION_APPROVE, "review_gates_passed_with_feature_portability_planning_required", [], warnings


def run_manual_publish_review_decision_v1(
    *,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    guard_root: str | Path = DEFAULT_GUARD_ROOT,
    branching_probe_root: str | Path = DEFAULT_BRANCHING_PROBE_ROOT,
    publish_review_gate_root: str | Path = DEFAULT_PUBLISH_REVIEW_GATE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    strict_teppan_zero_policy: str = "naming_threshold_limitation",
) -> dict[str, Any]:
    pattern_dir = _safe_path(pattern_root, DEFAULT_PATTERN_ROOT)
    guard_dir = _safe_path(guard_root, DEFAULT_GUARD_ROOT)
    probe_dir = _safe_path(branching_probe_root, DEFAULT_BRANCHING_PROBE_ROOT)
    gate_dir = _safe_path(publish_review_gate_root, DEFAULT_PUBLISH_REVIEW_GATE_ROOT)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    output_dir = output_base / (run_id or _run_id())
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern_payloads, pattern_errors = _load_required(pattern_dir, ("research_decision.json", "teppan_candidates.json"))
    guard_payloads, guard_errors = _load_required(guard_dir, ("research_decision.json",))
    probe_payloads, probe_errors = _load_required(
        probe_dir,
        ("research_decision.json", "compare.json", "ranking_coverage_audit.json", "branching_probe.json", "_ARTIFACT_COMPLETE.json"),
    )
    gate_payloads, gate_errors = _load_required(
        gate_dir,
        (
            "publish_review_decision.json",
            "feature_availability_audit.json",
            "ranking_adjustment_contract.json",
            "reproducibility_audit.json",
            "anti_leakage_recheck.json",
            "meemee_exposure_assessment.json",
            "shadow_publish_bundle_manifest.json",
            "_ARTIFACT_COMPLETE.json",
        ),
    )
    pattern_decision = pattern_payloads.get("research_decision.json") or {}
    guard_decision = guard_payloads.get("research_decision.json") or {}
    probe_decision = probe_payloads.get("research_decision.json") or {}
    probe_compare = probe_payloads.get("compare.json") or {}
    probe_coverage = probe_payloads.get("ranking_coverage_audit.json") or {}
    gate_decision = gate_payloads.get("publish_review_decision.json") or {}
    gate_complete = gate_payloads.get("_ARTIFACT_COMPLETE.json") or {}
    feature_audit = gate_payloads.get("feature_availability_audit.json") or {}
    ranking_contract = gate_payloads.get("ranking_adjustment_contract.json") or {}
    exposure = gate_payloads.get("meemee_exposure_assessment.json") or {}
    shadow_root = _shadow_bundle_root(gate_dir, gate_decision, gate_complete)
    shadow_review = _shadow_bundle_review(shadow_root)
    feature_report = _feature_portability_report(feature_audit, ranking_contract)
    runtime_gap = _runtime_reflection_gap_report(feature_report, exposure)
    strict_teppan = _strict_teppan_assessment(pattern_decision, strict_teppan_zero_policy)
    load_errors = {
        "pattern": pattern_errors,
        "guard": guard_errors,
        "branching_probe": probe_errors,
        "publish_review_gate": gate_errors,
    }
    decision, reason, blockers, warnings = _choose_decision(
        gate_decision=gate_decision,
        gate_complete=gate_complete,
        pattern_decision=pattern_decision,
        guard_decision=guard_decision,
        probe_decision=probe_decision,
        probe_compare=probe_compare,
        probe_coverage=probe_coverage,
        feature_report=feature_report,
        shadow_review=shadow_review,
        strict_teppan=strict_teppan,
        load_errors=load_errors,
    )
    implementation = _implementation_readiness_report(
        decision=decision,
        strict_teppan=strict_teppan,
        feature_report=feature_report,
        shadow_review=shadow_review,
        probe_decision=probe_decision,
        probe_compare=probe_compare,
    )
    blocker_or_approval = {
        "schema_version": f"{SCHEMA_PREFIX}_blocker_or_approval_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": reason,
        "blockers": blockers,
        "warnings": warnings,
        "approval_conditions": {
            "publish_review_gate_passed": gate_decision.get("decision") == "pass_to_manual_review",
            "shadow_bundle_complete": shadow_review.get("pass") is True,
            "reproducibility_pass": gate_decision.get("reproducibility_pass") is True,
            "anti_leakage_pass": gate_decision.get("anti_leakage_pass") is True,
            "source_artifact_integrity_pass": gate_decision.get("source_artifact_integrity_pass") is True,
            "feature_portability_pass": feature_report.get("pass") is True,
            "strict_teppan_zero_documented": strict_teppan.get("classification") in {"naming_threshold_limitation", "strict_teppan_present"},
            "no_meemee_mutation": gate_decision.get("no_meemee_mutation") is True,
            "production_ranking_changed": False,
        },
        "reject_only_conditions_checked": [
            "leakage",
            "artifact inconsistency",
            "non-reproducibility",
            "hidden MeeMee mutation",
            "production ranking mutation",
            "missing shadow bundle",
            "failed publish-review gate",
        ],
    }
    if decision == DECISION_APPROVE:
        next_action = "teppan_ranking_meemee_shadow_integration_plan_v1"
    elif decision == DECISION_HOLD_VALIDATION:
        next_action = "teppan_branching_stability_validation_v1"
    elif decision == DECISION_HOLD_PORTABILITY:
        next_action = "teppan_feature_portability_audit_v1"
    else:
        next_action = "freeze_teppan_ranking_branching_probe_v1_as_research_keep_only"
    next_axis = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "decision": decision,
        "next": next_action,
        "recommended_next_action": next_action,
        "still_not_meemee_implementation": True,
        "still_not_runtime_reflection": True,
        "what_not_to_do_next": [
            "do not change MeeMee runtime ranking from this review artifact",
            "do not write runtime DuckDB",
            "do not register production publish",
            "do not tune boost value",
            "do not change pattern or guard definitions",
        ],
    }
    manual_decision = {
        "schema_version": f"{SCHEMA_PREFIX}_manual_publish_review_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": AXIS_ID,
        "decision": decision,
        "decision_reason": reason,
        "blockers": blockers,
        "warnings": warnings,
        "source_roots": {
            "pattern_discovery_root": str(pattern_dir),
            "loss_guard_root": str(guard_dir),
            "branching_probe_root": str(probe_dir),
            "publish_review_gate_root": str(gate_dir),
            "shadow_bundle_root": str(shadow_root),
        },
        "source_decisions": {
            "pattern_discovery": pattern_decision.get("authoritative_research_decision"),
            "loss_guard": guard_decision.get("authoritative_research_decision"),
            "branching_probe": probe_decision.get("authoritative_research_decision"),
            "publish_review_gate": gate_decision.get("decision"),
        },
        "strict_teppan_assessment": strict_teppan,
        "next": next_action,
        "no_meemee_mutation": True,
        "no_runtime_duckdb_write": True,
        "no_production_registration": True,
        "production_ranking_changed": False,
        "meemee_runtime_ranking_changed": False,
        "approved_for_runtime_mutation": False,
    }
    artifact_paths = {
        "manual_publish_review_decision.json": _write_json(output_dir / "manual_publish_review_decision.json", manual_decision),
        "implementation_readiness_report.json": _write_json(output_dir / "implementation_readiness_report.json", implementation),
        "runtime_reflection_gap_report.json": _write_json(output_dir / "runtime_reflection_gap_report.json", runtime_gap),
        "feature_portability_report.json": _write_json(output_dir / "feature_portability_report.json", feature_report),
        "blocker_or_approval_report.json": _write_json(output_dir / "blocker_or_approval_report.json", blocker_or_approval),
        "next_axis_recommendation.json": _write_json(output_dir / "next_axis_recommendation.json", next_axis),
    }
    existing = {name: True for name in REQUIRED_OUTPUTS}
    complete_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "required_artifacts": list(REQUIRED_OUTPUTS),
        "existing_artifacts": existing,
        "complete": True,
        "manual_publish_review_decision": decision,
        "decision_reason": reason,
        "no_meemee_mutation": True,
        "no_runtime_duckdb_write": True,
        "no_production_registration": True,
        "production_ranking_changed": False,
        "artifact_hashes": {name: _json_hash(_load_json(path)) for name, path in artifact_paths.items()},
    }
    artifact_paths["_ARTIFACT_COMPLETE.json"] = _write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete_payload)
    return {
        "ok": decision == DECISION_APPROVE,
        "decision": decision,
        "decision_reason": reason,
        "output_root": str(output_dir),
        "artifact_paths": {name: str(path) for name, path in artifact_paths.items()},
        "manual_publish_review_decision": manual_decision,
        "implementation_readiness_report": implementation,
        "runtime_reflection_gap_report": runtime_gap,
        "feature_portability_report": feature_report,
        "blocker_or_approval_report": blocker_or_approval,
        "next_axis_recommendation": next_axis,
        "artifact_complete": complete_payload,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manual publish review decision for teppan ranking branching probe.")
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--guard-root", default=str(DEFAULT_GUARD_ROOT))
    parser.add_argument("--branching-probe-root", default=str(DEFAULT_BRANCHING_PROBE_ROOT))
    parser.add_argument("--publish-review-gate-root", default=str(DEFAULT_PUBLISH_REVIEW_GATE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument(
        "--strict-teppan-zero-policy",
        choices=("naming_threshold_limitation", "validation_blocker"),
        default="naming_threshold_limitation",
    )
    args = parser.parse_args(argv)
    payload = run_manual_publish_review_decision_v1(
        pattern_root=args.pattern_root,
        guard_root=args.guard_root,
        branching_probe_root=args.branching_probe_root,
        publish_review_gate_root=args.publish_review_gate_root,
        output_root=args.output_root,
        run_id=args.run_id or _run_id(),
        strict_teppan_zero_policy=args.strict_teppan_zero_policy,
    )
    print(json.dumps({"manual_review_root": payload["output_root"], "decision": payload["decision"]}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
