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


AXIS_ID = "teppan_ranking_meemee_shadow_integration_plan_v1"
CANDIDATE_ID = "teppan_ranking_branching_probe_v1"
SCHEMA_PREFIX = "tradex_teppan_meemee_shadow_integration_plan_v1"

DEFAULT_MANUAL_REVIEW_ROOT = Path(
    r"G:\Tradex\manual_publish_reviews\teppan_ranking_branching_probe_v1\20260514T030000Z-teppan-manual-publish-review-decision-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\shadow_integration_plans\teppan_ranking_meemee_shadow_integration_plan_v1")

DECISION_APPROVE = "approve_shadow_integration_implementation"
DECISION_HOLD_CONTRACT = "hold_for_contract_gap"
DECISION_HOLD_FEATURE = "hold_for_feature_materialization_gap"
DECISION_REJECT = "reject_shadow_integration"

REQUIRED_OUTPUTS = (
    "shadow_integration_plan.json",
    "runtime_contract_gap_report.json",
    "rank_storage_contract.json",
    "feature_materialization_plan.json",
    "rollback_plan.json",
    "acceptance_criteria.json",
    "implementation_change_list.json",
    "_ARTIFACT_COMPLETE.json",
)

MANUAL_REVIEW_REQUIRED = (
    "manual_publish_review_decision.json",
    "implementation_readiness_report.json",
    "runtime_reflection_gap_report.json",
    "feature_portability_report.json",
    "blocker_or_approval_report.json",
    "next_axis_recommendation.json",
    "_ARTIFACT_COMPLETE.json",
)

RUNTIME_CONTRACT_FILES = (
    "app/backend/services/signal_tracking_service.py",
    "app/backend/services/runtime_selection_service.py",
    "app/backend/services/publish_registry_sync_service.py",
    "app/backend/services/publish_promotion_service.py",
    "shared/contracts/publish_registry.py",
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


def _root_from_manual(manual_decision: dict[str, Any], key: str) -> Path:
    roots = manual_decision.get("source_roots") if isinstance(manual_decision.get("source_roots"), dict) else {}
    return Path(str(roots.get(key) or "")).expanduser().resolve()


def _feature_materialization_plan(feature_portability: dict[str, Any], manual_decision: dict[str, Any]) -> dict[str, Any]:
    source_roots = manual_decision.get("source_roots") if isinstance(manual_decision.get("source_roots"), dict) else {}
    portability_features = feature_portability.get("features") if isinstance(feature_portability.get("features"), list) else []
    materialized_features: list[dict[str, Any]] = []
    blockers: list[str] = []
    for row in portability_features:
        if not isinstance(row, dict):
            continue
        feature = str(row.get("feature") or "")
        portability = str(row.get("portability") or "")
        if portability == "blocked":
            blockers.append(feature)
        if feature in {"teppan_pattern_match", "teppan_guard_pass"}:
            materialized_features.append(
                {
                    "feature": feature,
                    "materialization_mode": "shadow_plan_runtime_reconstruction",
                    "source_artifact": row.get("source_file_or_artifact"),
                    "decision_time_safe": bool(row.get("decision_time_safe")),
                    "runtime_native_now": bool(row.get("available_in_current_meemee_runtime_ranking_generation")),
                    "requires_implementation": True,
                    "fallback_policy": "preserve champion order and emit reason code when unavailable",
                }
            )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_materialization_plan_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": CANDIDATE_ID,
        "decision": "ready" if not blockers else "blocked",
        "blockers": blockers,
        "materialization_scope": "shadow_only",
        "source_roots": {
            "pattern_discovery_root": source_roots.get("pattern_discovery_root"),
            "loss_guard_root": source_roots.get("loss_guard_root"),
            "publish_review_gate_root": source_roots.get("publish_review_gate_root"),
            "shadow_bundle_root": source_roots.get("shadow_bundle_root"),
        },
        "materialized_features": materialized_features,
        "runtime_native_features_reused": feature_portability.get("runtime_native_features", []),
        "forbidden_materialization_inputs": [
            "forward_ret_20d",
            "future_return_labels",
            "realized_topk_membership_labels",
            "selected_event_ledger.jsonl as runtime input",
            "research-only performance diagnostics",
        ],
        "missing_feature_policy": {
            "teppan_pattern_match": "no_boost_preserve_champion_order_reason_no_teppan_pattern_feature",
            "teppan_guard_pass": "no_boost_preserve_champion_order_reason_teppan_guard_unavailable",
            "silent_fallback_allowed": False,
            "fallback_to_future_labels": False,
        },
    }


def _runtime_contract_gap_report(
    *,
    runtime_gap: dict[str, Any],
    feature_plan: dict[str, Any],
    existing_contract_paths: dict[str, bool],
) -> dict[str, Any]:
    missing_contract_files = [path for path, exists in existing_contract_paths.items() if not exists]
    feature_blockers = list(feature_plan.get("blockers") or [])
    blockers = []
    if missing_contract_files:
        blockers.append("runtime_contract_file_missing")
    if feature_blockers:
        blockers.append("feature_materialization_blocked")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_runtime_contract_gap_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": CANDIDATE_ID,
        "decision": "compatible_with_shadow_adapter_plan" if not blockers else "blocked",
        "blockers": blockers,
        "missing_contract_files": missing_contract_files,
        "existing_contract_paths": existing_contract_paths,
        "direct_runtime_reflection_allowed_now": False,
        "runtime_reflection_implementation_required": True,
        "gap_state_from_manual_review": runtime_gap.get("gap_state"),
        "required_shadow_boundaries": [
            "shadow plan must not alter active ranking selection",
            "adjusted rank must be stored separately from original rank",
            "runtime DB writes are forbidden at planning stage",
            "publish registry activation is forbidden at planning stage",
            "MeeMee exposure must use allowlisted fields only",
        ],
        "expected_adapter_surface": {
            "candidate_metadata": "inactive shadow integration candidate",
            "active_selection": "unchanged",
            "ranking_materialization": "not_run",
            "publish_registry": "not_mutated",
        },
    }


def _rank_storage_contract(manual_decision: dict[str, Any], feature_plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_rank_storage_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": CANDIDATE_ID,
        "storage_mode": "shadow_integration_plan_only",
        "adjusted_ranking_materialization": "not_run",
        "adjusted_ranking_reason": "planning_only",
        "record_shape": {
            "source_candidate_id": "string",
            "anchor_date": "date|string",
            "symbol": "string",
            "side": "string",
            "original_rank": "integer",
            "original_rank_is_recoverable": True,
            "adjusted_rank": "integer",
            "adjusted_rank_is_separate": True,
            "original_score": "number",
            "adjusted_score": "number",
            "teppan_guarded_boost_applied": "boolean",
            "teppan_pattern_reason_code": "string",
            "teppan_guard_reason_code": "string",
            "boost_value": "number",
        },
        "persistence_contract": {
            "store_original_and_adjusted_ranks_separately": True,
            "store_original_and_adjusted_scores_separately": True,
            "store_audit_reason_codes": True,
            "store_source_candidate_id": True,
            "allow_rollback_without_recomputation": True,
            "rollback_restore_source": "stored original champion ranking trace",
        },
        "source_roots": manual_decision.get("source_roots") or {},
        "feature_materialization_dependencies": [
            row.get("feature") for row in feature_plan.get("materialized_features", []) if isinstance(row, dict)
        ],
        "rollback_contract": {
            "rollback_method": "ignore or remove shadow adjusted-ranking records and restore original champion ranking trace",
            "recompute_required": False,
            "active_selection_preserved": True,
        },
    }


def _rollback_plan() -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_rollback_plan_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": CANDIDATE_ID,
        "rollback_scope": "shadow_plan_or_shadow_candidate_only",
        "rollback_action": "remove_or_ignore_shadow_integration_candidate_artifacts",
        "runtime_db_rollback_required": False,
        "production_registry_rollback_required": False,
        "active_selection_rollback_required": False,
        "safe_restore_source": "unchanged runtime champion ranking",
        "rollback_steps": [
            "Do not consult the shadow candidate from runtime selection.",
            "Ignore or remove shadow integration metadata.",
            "Preserve original champion rank and score as the active ranking source.",
            "Verify runtime selection snapshot fields are unchanged before and after.",
        ],
        "verification": [
            "production_ranking_changed remains false",
            "no runtime DuckDB writes are observed",
            "selected_logic_key and active publish registry are unchanged",
            "adjusted ranking materialization remains not_run unless a later implementation gate approves it",
        ],
    }


def _acceptance_criteria(decision: str, blockers: list[str]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_acceptance_criteria_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": CANDIDATE_ID,
        "decision": "pass" if decision == DECISION_APPROVE else "blocked",
        "blockers": blockers,
        "acceptance_tests_for_later_implementation": [
            "manual publish review decision is approve_publish_implementation_plan",
            "shadow integration plan artifacts are complete",
            "feature materialization uses only decision-time runtime OHLCV and frozen TRADEX artifacts",
            "missing teppan features preserve champion order and emit explicit reason codes",
            "original rank and adjusted rank are stored separately",
            "forward_ret_20d and future labels are not runtime inputs",
            "runtime selection snapshot is unchanged by shadow integration",
            "production publish registry is not activated by shadow integration",
            "rollback can restore or ignore shadow adjusted ranking without recomputation",
        ],
        "acceptance_reject_conditions": [
            "runtime DB write required",
            "active production ranking changes",
            "feature materialization needs future labels",
            "original champion rank cannot be recovered",
            "silent fallback would be required",
        ],
    }


def _implementation_change_list(decision: str, contract_gap: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_implementation_change_list_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": CANDIDATE_ID,
        "decision": "ready_for_implementation_ticket" if decision == DECISION_APPROVE else "not_ready",
        "candidate_files_or_contracts_for_later_implementation": list(RUNTIME_CONTRACT_FILES),
        "required_changes_for_later_implementation": [
            "add a shadow-only adjusted ranking adapter that reads champion rank and score",
            "materialize teppan_pattern_match and teppan_guard_pass from decision-time inputs",
            "store adjusted rank and score separately from original rank and score",
            "emit allowlisted reason codes only",
            "add invariance checks proving active runtime ranking and publish registry are unchanged",
        ],
        "explicit_non_changes": [
            "do not change runtime ranking output in this planning step",
            "do not write runtime DuckDB in this planning step",
            "do not change frontend or backend API in this planning step",
            "do not activate a publish registry challenger in this planning step",
            "do not tune boost, pattern definitions, or loss guard",
        ],
        "runtime_contract_gap_decision": contract_gap.get("decision"),
    }


def _choose_decision(
    *,
    manual_decision: dict[str, Any],
    manual_complete: dict[str, Any],
    feature_plan: dict[str, Any],
    contract_gap: dict[str, Any],
    load_errors: dict[str, str],
) -> tuple[str, str, list[str]]:
    blockers: list[str] = []
    if load_errors:
        blockers.append("manual_review_artifacts_missing_or_unparseable")
    if manual_decision.get("decision") != "approve_publish_implementation_plan":
        blockers.append("manual_publish_review_not_approved_for_implementation_plan")
    if manual_decision.get("approved_for_runtime_mutation") is not False:
        blockers.append("manual_review_runtime_mutation_flag_not_false")
    if manual_decision.get("no_meemee_mutation") is not True:
        blockers.append("manual_review_meemee_mutation_not_excluded")
    if manual_decision.get("no_runtime_duckdb_write") is not True:
        blockers.append("manual_review_runtime_duckdb_write_not_excluded")
    if manual_decision.get("no_production_registration") is not True:
        blockers.append("manual_review_production_registration_not_excluded")
    if manual_complete.get("complete") is not True:
        blockers.append("manual_review_artifact_complete_false")
    if feature_plan.get("decision") != "ready":
        return DECISION_HOLD_FEATURE, "feature_materialization_gap_blocks_shadow_integration_plan", list(feature_plan.get("blockers") or ["feature_materialization_not_ready"])
    if contract_gap.get("decision") != "compatible_with_shadow_adapter_plan":
        return DECISION_HOLD_CONTRACT, "runtime_contract_gap_blocks_shadow_integration_plan", list(contract_gap.get("blockers") or ["runtime_contract_gap"])
    if blockers:
        if any("mutation" in blocker or "registration" in blocker or "duckdb" in blocker for blocker in blockers):
            return DECISION_REJECT, "manual_review_boundary_flags_invalid", blockers
        return DECISION_HOLD_CONTRACT, "manual_review_contract_not_ready", blockers
    return DECISION_APPROVE, "manual_review_approved_and_shadow_contracts_are_bounded", []


def run_teppan_ranking_meemee_shadow_integration_plan_v1(
    *,
    manual_review_root: str | Path = DEFAULT_MANUAL_REVIEW_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    manual_root = _safe_path(manual_review_root, DEFAULT_MANUAL_REVIEW_ROOT)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    output_dir = output_base / (run_id or _run_id())
    output_dir.mkdir(parents=True, exist_ok=True)

    manual_payloads, load_errors = _load_required(manual_root, MANUAL_REVIEW_REQUIRED)
    manual_decision = manual_payloads.get("manual_publish_review_decision.json") or {}
    manual_complete = manual_payloads.get("_ARTIFACT_COMPLETE.json") or {}
    runtime_gap = manual_payloads.get("runtime_reflection_gap_report.json") or {}
    feature_portability = manual_payloads.get("feature_portability_report.json") or {}
    implementation_readiness = manual_payloads.get("implementation_readiness_report.json") or {}

    existing_contract_paths = {path: Path(path).exists() for path in RUNTIME_CONTRACT_FILES}
    feature_plan = _feature_materialization_plan(feature_portability, manual_decision)
    contract_gap = _runtime_contract_gap_report(
        runtime_gap=runtime_gap,
        feature_plan=feature_plan,
        existing_contract_paths=existing_contract_paths,
    )
    rank_storage = _rank_storage_contract(manual_decision, feature_plan)
    rollback = _rollback_plan()
    decision, decision_reason, blockers = _choose_decision(
        manual_decision=manual_decision,
        manual_complete=manual_complete,
        feature_plan=feature_plan,
        contract_gap=contract_gap,
        load_errors=load_errors,
    )
    acceptance = _acceptance_criteria(decision, blockers)
    implementation_changes = _implementation_change_list(decision, contract_gap)
    shadow_plan = {
        "schema_version": f"{SCHEMA_PREFIX}_shadow_integration_plan_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": CANDIDATE_ID,
        "decision": decision,
        "decision_reason": decision_reason,
        "blockers": blockers,
        "manual_review_root": str(manual_root),
        "manual_review_decision": manual_decision.get("decision"),
        "target_state": "ready_for_shadow_integration_implementation" if decision == DECISION_APPROVE else "hold",
        "integration_mode": "shadow_only",
        "active_runtime_ranking_change_allowed": False,
        "runtime_duckdb_write_allowed": False,
        "production_publish_registration_allowed": False,
        "frontend_or_backend_ui_change_allowed": False,
        "source_roots": manual_decision.get("source_roots") or {},
        "implementation_readiness_state": implementation_readiness.get("readiness_state"),
        "feature_materialization_plan_ref": str(output_dir / "feature_materialization_plan.json"),
        "rank_storage_contract_ref": str(output_dir / "rank_storage_contract.json"),
        "rollback_plan_ref": str(output_dir / "rollback_plan.json"),
        "acceptance_criteria_ref": str(output_dir / "acceptance_criteria.json"),
        "next_allowed_step": "implement_shadow_integration_candidate_inactive_only" if decision == DECISION_APPROVE else "resolve_shadow_integration_plan_blockers",
    }

    artifact_paths = {
        "shadow_integration_plan.json": _write_json(output_dir / "shadow_integration_plan.json", shadow_plan),
        "runtime_contract_gap_report.json": _write_json(output_dir / "runtime_contract_gap_report.json", contract_gap),
        "rank_storage_contract.json": _write_json(output_dir / "rank_storage_contract.json", rank_storage),
        "feature_materialization_plan.json": _write_json(output_dir / "feature_materialization_plan.json", feature_plan),
        "rollback_plan.json": _write_json(output_dir / "rollback_plan.json", rollback),
        "acceptance_criteria.json": _write_json(output_dir / "acceptance_criteria.json", acceptance),
        "implementation_change_list.json": _write_json(output_dir / "implementation_change_list.json", implementation_changes),
    }
    existing = {name: True for name in REQUIRED_OUTPUTS}
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_id": CANDIDATE_ID,
        "artifact_root": str(output_dir),
        "required_artifacts": list(REQUIRED_OUTPUTS),
        "existing_artifacts": existing,
        "complete": True,
        "decision": decision,
        "decision_reason": decision_reason,
        "no_meemee_mutation": True,
        "no_runtime_duckdb_write": True,
        "no_production_registration": True,
        "active_runtime_ranking_changed": False,
        "artifact_hashes": {name: _json_hash(_load_json(path)) for name, path in artifact_paths.items()},
    }
    artifact_paths["_ARTIFACT_COMPLETE.json"] = _write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "ok": decision == DECISION_APPROVE,
        "decision": decision,
        "decision_reason": decision_reason,
        "output_root": str(output_dir),
        "artifact_paths": {name: str(path) for name, path in artifact_paths.items()},
        "shadow_integration_plan": shadow_plan,
        "runtime_contract_gap_report": contract_gap,
        "rank_storage_contract": rank_storage,
        "feature_materialization_plan": feature_plan,
        "rollback_plan": rollback,
        "acceptance_criteria": acceptance,
        "implementation_change_list": implementation_changes,
        "artifact_complete": complete,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create teppan MeeMee shadow integration planning artifacts.")
    parser.add_argument("--manual-review-root", default=str(DEFAULT_MANUAL_REVIEW_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    payload = run_teppan_ranking_meemee_shadow_integration_plan_v1(
        manual_review_root=args.manual_review_root,
        output_root=args.output_root,
        run_id=args.run_id or _run_id(),
    )
    print(json.dumps({"shadow_integration_plan_root": payload["output_root"], "decision": payload["decision"]}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
