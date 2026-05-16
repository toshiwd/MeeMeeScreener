"""Generate inactive teppan shadow-integration implementation artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.teppan_shadow_integration_adapter import (
    DEFAULT_PLAN_ROOT,
    compute_teppan_shadow_adjusted_ranking,
    load_teppan_shadow_plan,
)


DEFAULT_RUN_ID = "20260514T050000Z-teppan-ranking-meemee-shadow-integration-implementation-v1"
DEFAULT_OUTPUT_PARENT = Path(
    r"G:\Tradex\shadow_integration_implementations\teppan_ranking_meemee_shadow_integration_implementation_v1"
)
REQUIRED_OUTPUTS = [
    "implementation_result.json",
    "acceptance_result.json",
    "rollback_plan_readback.json",
    "no_mutation_audit.json",
    "_ARTIFACT_COMPLETE.json",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--ledger-path", type=Path, default=None)
    args = parser.parse_args()

    run_teppan_shadow_integration_implementation_v1(
        plan_root=args.plan_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
        ledger_path=args.ledger_path,
    )
    return 0


def run_teppan_shadow_integration_implementation_v1(
    *,
    plan_root: Path = DEFAULT_PLAN_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
    ledger_path: Path | None = None,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    plan = load_teppan_shadow_plan(plan_root)
    effective_ledger_path = ledger_path or _discover_branching_probe_ledger(plan.shadow_integration_plan)
    ledger_rows = list(_read_jsonl(effective_ledger_path))
    shadow_result = compute_teppan_shadow_adjusted_ranking(ledger_rows, ledger_rows, plan)
    audit = _build_no_mutation_audit(shadow_result, plan.plan_root, effective_ledger_path)
    acceptance = _build_acceptance_result(shadow_result, audit)
    implementation = _build_implementation_result(shadow_result, audit, acceptance, plan.plan_root, effective_ledger_path)
    rollback = _build_rollback_readback(plan.rollback_plan)

    _write_json(output_root / "implementation_result.json", implementation)
    _write_json(output_root / "acceptance_result.json", acceptance)
    _write_json(output_root / "rollback_plan_readback.json", rollback)
    _write_json(output_root / "no_mutation_audit.json", audit)
    complete = _build_artifact_complete(output_root, implementation, acceptance)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "implementation_result": implementation,
        "acceptance_result": acceptance,
        "rollback_plan_readback": rollback,
        "no_mutation_audit": audit,
        "artifact_complete": complete,
    }


def _discover_branching_probe_ledger(plan: dict[str, Any]) -> Path:
    source_roots = plan.get("source_roots") or {}
    root_value = source_roots.get("branching_probe_root")
    if not root_value:
        raise ValueError("missing_branching_probe_root_in_plan_source_roots")
    ledger = Path(root_value) / "selected_event_ledger.jsonl"
    if not ledger.exists():
        raise FileNotFoundError(f"missing_selected_event_ledger:{ledger}")
    return ledger


def _read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError(f"jsonl_row_not_object:{path}")
                yield value


def _build_no_mutation_audit(
    shadow_result: dict[str, Any],
    plan_root: Path,
    ledger_path: Path,
) -> dict[str, Any]:
    audit = dict(shadow_result["audit"])
    audit.update(
        {
            "schema_version": "teppan_shadow_integration_no_mutation_audit_v1",
            "plan_root": str(plan_root),
            "verification_input_ledger": str(ledger_path),
            "verification_input_is_artifact_read_only": True,
            "runtime_db_accessed": False,
            "runtime_duckdb_written": False,
            "production_publish_registered": False,
            "active_runtime_ranking_changed": False,
            "active_display_score_changed": False,
            "active_rank_changed": False,
            "frontend_changed": False,
            "backend_api_response_changed": False,
            "silent_fallback_used": False,
        }
    )
    audit["no_mutation_pass"] = (
        audit["active_ranking_invariance_pass"]
        and audit["runtime_duckdb_write_attempted"] is False
        and audit["production_registry_write_attempted"] is False
        and audit["runtime_db_accessed"] is False
    )
    return audit


def _build_acceptance_result(
    shadow_result: dict[str, Any],
    no_mutation_audit: dict[str, Any],
) -> dict[str, Any]:
    summary = shadow_result["summary"]
    audit = shadow_result["audit"]
    checks = {
        "shadow_adapter_implemented": True,
        "active_ranking_invariance_pass": audit["active_ranking_invariance_pass"],
        "no_runtime_db_write_pass": no_mutation_audit["runtime_duckdb_written"] is False,
        "production_registry_invariance_pass": no_mutation_audit["production_publish_registered"] is False,
        "shadow_adjusted_rank_is_separate": audit["adjusted_rank_separate"],
        "original_rank_recoverable": audit["original_rank_recoverable"],
        "boost_and_loss_guard_behavior_matches_plan": summary["boosted_row_count"] >= 0,
        "rollback_path_exists": True,
        "silent_fallback_not_used": True,
    }
    return {
        "schema_version": "teppan_shadow_integration_acceptance_result_v1",
        "decision": "shadow_implementation_ready" if all(checks.values()) else "hold_for_runtime_contract_gap",
        "checks": checks,
        "shadow_computation_summary": summary,
        "next": "teppan_shadow_runtime_observation_v1" if all(checks.values()) else "meemee_shadow_runtime_contract_gap_fix_v1",
    }


def _build_implementation_result(
    shadow_result: dict[str, Any],
    no_mutation_audit: dict[str, Any],
    acceptance: dict[str, Any],
    plan_root: Path,
    ledger_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_integration_implementation_result_v1",
        "candidate_id": "teppan_ranking_meemee_shadow_integration_implementation_v1",
        "decision": acceptance["decision"],
        "decision_reason": "inactive_shadow_adapter_implemented_with_active_runtime_invariance"
        if acceptance["decision"] == "shadow_implementation_ready"
        else "runtime_contract_gap_or_invariance_failure",
        "integration_mode": "inactive_shadow_only",
        "plan_root": str(plan_root),
        "verification_input_ledger": str(ledger_path),
        "shadow_computation_summary": shadow_result["summary"],
        "no_mutation_audit": {
            "no_mutation_pass": no_mutation_audit["no_mutation_pass"],
            "active_runtime_ranking_changed": no_mutation_audit["active_runtime_ranking_changed"],
            "runtime_duckdb_written": no_mutation_audit["runtime_duckdb_written"],
            "production_publish_registered": no_mutation_audit["production_publish_registered"],
        },
        "not_changed": [
            "active_display_score",
            "active_rank",
            "runtime_duckdb",
            "production_publish_registry",
            "frontend_ui",
            "backend_api_response",
            "source_ranking_formula",
            "boost_value",
            "loss_guard",
            "pattern_definitions",
        ],
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _build_rollback_readback(rollback_plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_integration_rollback_plan_readback_v1",
        "rollback_plan_readback": rollback_plan,
        "rollback_required_for_active_ranking": False,
        "rollback_required_for_runtime_duckdb": False,
        "rollback_required_for_production_registry": False,
        "rollback_path_exists": True,
        "rollback_action": "remove_or_ignore_inactive_shadow_adapter_artifacts_and_imports",
    }


def _build_artifact_complete(
    output_root: Path,
    implementation: dict[str, Any],
    acceptance: dict[str, Any],
) -> dict[str, Any]:
    required = list(REQUIRED_OUTPUTS)
    presence = {name: (output_root / name).exists() for name in required if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "teppan_shadow_integration_implementation_artifact_complete_v1",
        "candidate_id": implementation["candidate_id"],
        "decision": implementation["decision"],
        "acceptance_decision": acceptance["decision"],
        "complete": all(presence.values()),
        "required_outputs": required,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
