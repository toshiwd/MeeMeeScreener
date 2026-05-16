from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


AXIS_ID = "monthly_drawdown_guarded_momentum_active_replacement_plan_v1"
SCHEMA_PREFIX = "tradex_monthly_drawdown_guarded_momentum_active_replacement_plan"

DEFAULT_TOP5_GATE_ROOT = Path(
    "G:/Tradex/monthly_drawdown_guarded_momentum_top5_gate_v1/"
    "20260515T000000Z-monthly-drawdown-guarded-momentum-top5-gate-v1"
)
DEFAULT_STARTER_PRETEST_ROOT = Path(
    "G:/Tradex/monthly_drawdown_guarded_momentum_starter_entry_pretest_v1/"
    "20260515T003000Z-monthly-drawdown-guarded-momentum-starter-entry-pretest-v1"
)
DEFAULT_MANUAL_REVIEW_PACK_ROOT = Path(
    "G:/Tradex/monthly_drawdown_guarded_momentum_manual_candidate_review_pack_v1/"
    "20260515T010000Z-monthly-drawdown-guarded-momentum-manual-candidate-review-pack-v1"
)
DEFAULT_ENTRY_TIMING_ROOT = Path(
    "G:/Tradex/monthly_drawdown_guarded_momentum_entry_timing_confirmation_v1/"
    "20260515T013000Z-monthly-drawdown-guarded-momentum-entry-timing-confirmation-v1"
)
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/monthly_drawdown_guarded_momentum_active_replacement_plan_v1")
DEFAULT_RUN_ID = "20260515T020000Z-monthly-drawdown-guarded-momentum-active-replacement-plan-v1"

RUNTIME_CONTRACT_FILES = [
    Path("app/backend/api/routers/rankings.py"),
    Path("app/backend/services/ml/rankings_cache.py"),
    Path("shared/contracts/ranking_output.py"),
    Path("app/db/schema.py"),
]

REQUIRED_ARTIFACTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "source_decision_readback.json",
    "active_replacement_plan.json",
    "runtime_contract_impact_report.json",
    "live_dry_run_contract.json",
    "implementation_change_list.json",
    "acceptance_criteria.json",
    "rollback_plan.json",
    "blocked_or_approval_report.json",
    "no_mutation_audit.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def _sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_status(path: Path) -> dict[str, Any]:
    return {"path": str(path), "exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}


def _require_json(root: Path, name: str) -> dict[str, Any]:
    path = root / name
    if not path.exists():
        raise FileNotFoundError(f"required artifact missing: {path}")
    return _read_json(path)


def _runtime_file_audit(repo_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rel in RUNTIME_CONTRACT_FILES:
        path = repo_root / rel
        rows.append(
            {
                "relative_path": str(rel).replace("\\", "/"),
                "exists": path.exists(),
                "bytes": path.stat().st_size if path.exists() else 0,
                "sha256": _sha256(path),
            }
        )
    return rows


def _all_required_top5_gates_pass(gate_report: Mapping[str, Any]) -> bool:
    results = gate_report.get("best_variant_gate_results", {})
    mandatory = gate_report.get("mandatory_gates", [])
    return bool(mandatory) and all(bool(results.get(name)) for name in mandatory)


def _classify(
    top5_decision: Mapping[str, Any],
    gate_report: Mapping[str, Any],
    pretest_decision: Mapping[str, Any],
    pretest_leaderboard: Mapping[str, Any],
    manual_decision: Mapping[str, Any],
    entry_decision: Mapping[str, Any],
) -> tuple[str, str, list[str], bool, bool]:
    top5_ready = (
        top5_decision.get("decision") == "keep_candidate"
        and top5_decision.get("top5_candidate_pool_clearly_better_than_baseline") is True
        and _all_required_top5_gates_pass(gate_report)
    )
    pretest_ready = (
        pretest_decision.get("authoritative_research_decision") == "starter_entry_pretest_keep"
        and pretest_leaderboard.get("all_pretest_gates_pass") is True
    )
    manual_ready = manual_decision.get("authoritative_research_decision") == "manual_review_pack_ready"
    entry_timing_ready = entry_decision.get("authoritative_research_decision") == "entry_timing_confirmation_keep"
    entry_timing_hold = entry_decision.get("authoritative_research_decision") == "entry_timing_confirmation_hold"

    if top5_ready and pretest_ready and manual_ready:
        if entry_timing_ready:
            return (
                "keep_candidate",
                "active_replacement_implementation_plan_ready",
                ["all_source_gates_passed_entry_timing_ready"],
                True,
                True,
            )
        if entry_timing_hold:
            return (
                "keep_candidate",
                "active_replacement_plan_ready_for_live_dry_run",
                ["source_gates_passed_entry_timing_hold_blocks_immediate_activation"],
                True,
                False,
            )
        return (
            "hold",
            "active_replacement_plan_hold",
            ["entry_timing_confirmation_not_ready"],
            False,
            False,
        )
    return (
        "hold",
        "active_replacement_plan_hold",
        ["upstream_keep_chain_incomplete"],
        False,
        False,
    )


def _complete_artifact(output_root: Path, run_id: str) -> None:
    complete: dict[str, Any] = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "run_id": run_id,
        "artifact_root": str(output_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "artifacts": {},
        "complete": False,
    }
    for name in REQUIRED_ARTIFACTS:
        path = output_root / name
        complete["artifacts"][name] = {"exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}
    complete["complete"] = all(
        item["exists"] and item["bytes"] > 0
        for name, item in complete["artifacts"].items()
        if name != "_ARTIFACT_COMPLETE.json"
    )
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    complete["artifacts"]["_ARTIFACT_COMPLETE.json"] = _artifact_status(output_root / "_ARTIFACT_COMPLETE.json")
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for item in complete["artifacts"].values())
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)


def run(args: argparse.Namespace) -> Path:
    output_root = args.output_parent / args.run_id
    output_root.mkdir(parents=True, exist_ok=True)
    generated_at = _utc_now()

    top5_decision = _require_json(args.top5_gate_root, "research_decision.json")
    gate_report = _require_json(args.top5_gate_root, "gate_pass_fail_report.json")
    top5_leaderboard = _require_json(args.top5_gate_root, "strict_gate_leaderboard.json")
    pretest_decision = _require_json(args.starter_pretest_root, "research_decision.json")
    pretest_leaderboard = _require_json(args.starter_pretest_root, "starter_entry_leaderboard.json")
    manual_decision = _require_json(args.manual_review_pack_root, "research_decision.json")
    entry_decision = _require_json(args.entry_timing_root, "research_decision.json")
    entry_metrics = _require_json(args.entry_timing_root, "confirmed_candidate_metrics.json")

    decision, authoritative, typed_reasons, replacement_direction_approved, immediate_activation_allowed = _classify(
        top5_decision,
        gate_report,
        pretest_decision,
        pretest_leaderboard,
        manual_decision,
        entry_decision,
    )
    live_dry_run_required = replacement_direction_approved and not immediate_activation_allowed
    repo_root = Path(__file__).resolve().parents[1]
    runtime_files = _runtime_file_audit(repo_root)
    runtime_contract_files_exist = all(row["exists"] for row in runtime_files)

    best_variant = top5_leaderboard.get("best_variant", {})
    starter_variant = pretest_leaderboard.get("starter_entry_variant", {})
    source_summary = {
        "best_variant_id": top5_decision.get("best_variant_id") or best_variant.get("variant_id"),
        "top5_gate_decision": top5_decision.get("authoritative_research_decision"),
        "starter_pretest_decision": pretest_decision.get("authoritative_research_decision"),
        "manual_review_pack_decision": manual_decision.get("authoritative_research_decision"),
        "entry_timing_decision": entry_decision.get("authoritative_research_decision"),
        "all_top5_gates_pass": _all_required_top5_gates_pass(gate_report),
        "all_pretest_gates_pass": pretest_leaderboard.get("all_pretest_gates_pass") is True,
        "top5_deltas_vs_baseline": starter_variant.get("deltas_vs_baseline", {}),
        "top5_metrics": starter_variant.get("metrics", {}),
        "top3_guardrail": starter_variant.get("guardrail", {}),
        "candidate_source_mix": starter_variant.get("candidate_source_mix", {}),
        "entry_timing_metrics": entry_metrics,
    }

    payloads: dict[str, Mapping[str, Any]] = {
        "evaluation_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
            "axis_id": AXIS_ID,
            "boundary": "TRADEX-only",
            "purpose": "prepare active replacement plan and live dry-run gate without mutating MeeMee runtime",
            "json_artifacts_authoritative": True,
            "markdown_summary_only": True,
            "fixed_conditions_preserved_from_source_chain": True,
            "no_silent_fallback": True,
            "no_automatic_meemee_reflection": True,
            "do_not_change": [
                "active_runtime_ranking",
                "display_score",
                "runtime_duckdb",
                "production_publish_registry",
                "frontend_backend_ui_api",
                "boost_values",
                "loss_guard_semantics",
                "pattern_definitions",
            ],
        },
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_PREFIX}_run_manifest_v1",
            "axis_id": AXIS_ID,
            "run_id": args.run_id,
            "generated_at_utc": generated_at,
            "output_root": str(output_root),
            "top5_gate_root": str(args.top5_gate_root),
            "starter_pretest_root": str(args.starter_pretest_root),
            "manual_review_pack_root": str(args.manual_review_pack_root),
            "entry_timing_root": str(args.entry_timing_root),
        },
        "source_artifact_refs.json": {
            "schema_version": f"{SCHEMA_PREFIX}_source_refs_v1",
            "top5_gate_root": str(args.top5_gate_root),
            "starter_pretest_root": str(args.starter_pretest_root),
            "manual_review_pack_root": str(args.manual_review_pack_root),
            "entry_timing_root": str(args.entry_timing_root),
            "required_source_artifacts": [
                "top5_gate/research_decision.json",
                "top5_gate/gate_pass_fail_report.json",
                "top5_gate/strict_gate_leaderboard.json",
                "starter_pretest/research_decision.json",
                "starter_pretest/starter_entry_leaderboard.json",
                "manual_review_pack/research_decision.json",
                "entry_timing/research_decision.json",
                "entry_timing/confirmed_candidate_metrics.json",
            ],
        },
        "source_decision_readback.json": {
            "schema_version": f"{SCHEMA_PREFIX}_source_decision_readback_v1",
            **source_summary,
        },
        "active_replacement_plan.json": {
            "schema_version": f"{SCHEMA_PREFIX}_active_replacement_plan_v1",
            "replacement_direction_approved": replacement_direction_approved,
            "immediate_active_replacement_allowed": immediate_activation_allowed,
            "live_dry_run_required_before_activation": live_dry_run_required,
            "best_variant": best_variant,
            "starter_entry_variant_id": starter_variant.get("variant_id"),
            "target_runtime_surface": {
                "ranking_api_router": "app/backend/api/routers/rankings.py",
                "ranking_service": "app/backend/services/ml/rankings_cache.py",
                "ranking_output_contract": "shared/contracts/ranking_output.py",
                "runtime_schema": "app/db/schema.py",
            },
            "implementation_policy": {
                "first_step": "implement opt-in dry-run replacement adapter that emits comparison artifacts only",
                "activation_step": "separate approval after live dry-run acceptance",
                "rollback_required": True,
                "feature_flag_or_explicit_mode_required": True,
                "default_runtime_behavior_must_remain_current_until_activation_approval": True,
            },
        },
        "runtime_contract_impact_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_runtime_contract_impact_report_v1",
            "runtime_contract_files_exist": runtime_contract_files_exist,
            "runtime_contract_files": runtime_files,
            "planned_runtime_impact": {
                "active_rank_changes_only_after_separate_activation": True,
                "display_score_changes_only_after_separate_activation": True,
                "api_response_shape_change_required": False,
                "frontend_change_required_for_dry_run": False,
                "runtime_duckdb_write_required": False,
                "production_registry_change_required": False,
            },
        },
        "live_dry_run_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_live_dry_run_contract_v1",
            "required_before_active_replacement": True,
            "dry_run_boundary": "MeeMee-read TRADEX-write-artifacts-only",
            "must_compare": [
                "active_top5",
                "replacement_top5",
                "added_by_replacement_top5",
                "removed_from_active_top5",
                "changed_rank_count",
                "display_score_invariance_before_activation",
                "runtime_duckdb_no_write",
            ],
            "minimum_acceptance": {
                "runtime_input_materialization_available": True,
                "active_ranking_snapshot_reproducible": True,
                "replacement_candidate_scores_reproducible": True,
                "rollback_path_verified": True,
                "manual_review_candidate_examples_present": True,
            },
            "blocked_from_live_activation_until": [
                "live_dry_run_passes",
                "entry_timing_hold_is_resolved_or_explicitly_accepted_as_non_blocking",
                "rollback_is_verified",
            ],
        },
        "implementation_change_list.json": {
            "schema_version": f"{SCHEMA_PREFIX}_implementation_change_list_v1",
            "this_run_changes_runtime_code": False,
            "next_implementation_files_to_touch": [
                {
                    "path": "app/backend/services/ml/rankings_cache.py",
                    "purpose": "add opt-in replacement scoring path after dry-run approval",
                    "allowed_now": False,
                },
                {
                    "path": "app/backend/api/routers/rankings.py",
                    "purpose": "expose explicit dry-run mode only if needed",
                    "allowed_now": False,
                },
            ],
            "files_that_must_remain_unchanged_in_this_run": [str(path).replace("\\", "/") for path in RUNTIME_CONTRACT_FILES],
        },
        "acceptance_criteria.json": {
            "schema_version": f"{SCHEMA_PREFIX}_acceptance_criteria_v1",
            "active_replacement_acceptance_requires": [
                "top5_candidate_pool_clearly_better_than_baseline remains true",
                "top5_avg_ret20 improves",
                "top5_big_winner_capture improves",
                "top5_future_top10_capture improves",
                "top5_severe_loss_rate does not worsen",
                "top5_bad_pick_count does not increase",
                "human_selectable_day_rate improves or is maintained",
                "time block majority remains positive",
                "family concentration is not excessive",
                "top3 guardrail is not fatal",
                "live dry-run shows expected branch behavior",
                "rollback path is verified",
            ],
            "currently_satisfied_by_source_chain": {
                "top5_gate": top5_decision.get("top5_candidate_pool_clearly_better_than_baseline") is True,
                "starter_entry_pretest": pretest_leaderboard.get("all_pretest_gates_pass") is True,
                "manual_review_pack": manual_decision.get("authoritative_research_decision") == "manual_review_pack_ready",
                "entry_timing_confirmation": entry_decision.get("authoritative_research_decision") == "entry_timing_confirmation_keep",
            },
        },
        "rollback_plan.json": {
            "schema_version": f"{SCHEMA_PREFIX}_rollback_plan_v1",
            "rollback_required": True,
            "rollback_strategy": "keep current ranking path as default and gate replacement behind explicit mode/config until activation approval",
            "rollback_checks": [
                "current active top5 restored",
                "display_score restored",
                "ranking_appearance_daily unchanged by dry-run",
                "production registry unchanged",
                "API default mode unchanged",
            ],
        },
        "blocked_or_approval_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_blocked_or_approval_report_v1",
            "replacement_direction_approved": replacement_direction_approved,
            "immediate_active_replacement_allowed": immediate_activation_allowed,
            "live_dry_run_required": live_dry_run_required,
            "blocking_items": [
                item
                for item, blocked in {
                    "entry_timing_confirmation_hold": entry_decision.get("authoritative_research_decision")
                    == "entry_timing_confirmation_hold",
                    "live_dry_run_not_yet_run": live_dry_run_required,
                    "rollback_not_yet_verified_in_runtime": live_dry_run_required,
                }.items()
                if blocked
            ],
            "approval_items": [
                "top5_gate_keep_candidate",
                "starter_entry_pretest_keep",
                "manual_review_pack_ready",
            ],
            "typed_reasons": typed_reasons,
        },
        "no_mutation_audit.json": {
            "schema_version": f"{SCHEMA_PREFIX}_no_mutation_audit_v1",
            "axis_id": AXIS_ID,
            "production_ranking_changed": False,
            "runtime_duckdb_written": False,
            "display_score_changed": False,
            "publish_bundle_created": False,
            "production_publish_registered": False,
            "meemee_runtime_changed": False,
            "frontend_backend_changed": False,
            "no_mutation_pass": True,
        },
        "next_axis_recommendation.json": {
            "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
            "axis_id": AXIS_ID,
            "decision": authoritative,
            "next": "monthly_drawdown_guarded_momentum_live_replacement_dry_run_v1"
            if live_dry_run_required
            else "monthly_drawdown_guarded_momentum_active_replacement_implementation_v1",
            "reason": "replacement_direction_is_supported_but_runtime_activation_requires_dry_run_and_rollback_gate"
            if live_dry_run_required
            else "source_chain_supports_active_replacement_implementation_plan",
        },
        "research_decision.json": {
            "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
            "generated_at_utc": generated_at,
            "research_phase": "active_replacement_plan",
            "boundary": "TRADEX-only",
            "axis_moved": "monthly_drawdown_guarded_momentum_active_replacement_plan",
            "source_top5_gate_decision": top5_decision.get("authoritative_research_decision"),
            "source_starter_pretest_decision": pretest_decision.get("authoritative_research_decision"),
            "source_manual_review_pack_decision": manual_decision.get("authoritative_research_decision"),
            "source_entry_timing_decision": entry_decision.get("authoritative_research_decision"),
            "replacement_direction_approved": replacement_direction_approved,
            "immediate_active_replacement_allowed": immediate_activation_allowed,
            "live_dry_run_required_before_activation": live_dry_run_required,
            "candidate_scoring_created": False,
            "candidate_generation_challenger_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "production_ranking_changed": False,
            "publish_bundle_created": False,
            "meemee_reflectable": False,
            "future_labels_used_for_evaluation_only": False,
            "future_labels_used_in_candidate_construction": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "decision": decision,
            "authoritative_research_decision": authoritative,
            "typed_reasons": typed_reasons,
        },
    }

    for name, payload in payloads.items():
        _write_json(output_root / name, payload)
    _complete_artifact(output_root, args.run_id)
    return output_root


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top5-gate-root", type=Path, default=DEFAULT_TOP5_GATE_ROOT)
    parser.add_argument("--starter-pretest-root", type=Path, default=DEFAULT_STARTER_PRETEST_ROOT)
    parser.add_argument("--manual-review-pack-root", type=Path, default=DEFAULT_MANUAL_REVIEW_PACK_ROOT)
    parser.add_argument("--entry-timing-root", type=Path, default=DEFAULT_ENTRY_TIMING_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    return parser


def main() -> None:
    output_root = run(_parser().parse_args())
    print(json.dumps({"axis_id": AXIS_ID, "artifact_root": str(output_root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
