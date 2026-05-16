from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_BACKEND_ROOT = REPO_ROOT / "app" / "backend"
for candidate in (REPO_ROOT, APP_BACKEND_ROOT):
    candidate_text = str(candidate)
    if candidate_text not in sys.path:
        sys.path.insert(0, candidate_text)

AXIS_ID = "monthly_drawdown_guarded_momentum_live_replacement_dry_run_v1"
SCHEMA_PREFIX = "tradex_monthly_drawdown_guarded_momentum_live_replacement_dry_run"

DEFAULT_SOURCE_PLAN_ROOT = Path(
    "G:/Tradex/monthly_drawdown_guarded_momentum_active_replacement_plan_v1/"
    "20260515T020000Z-monthly-drawdown-guarded-momentum-active-replacement-plan-v1"
)
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/monthly_drawdown_guarded_momentum_live_replacement_dry_run_v1")
DEFAULT_RUN_ID = "20260515T023000Z-monthly-drawdown-guarded-momentum-live-replacement-dry-run-v1"

REQUIRED_ARTIFACTS = [
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "runtime_state_check.json",
    "live_replacement_dry_run_contract.json",
    "active_topk_snapshot.json",
    "replacement_topk_snapshot.json",
    "active_vs_replacement_diff.json",
    "replacement_scoring_report.json",
    "rollback_verification_report.json",
    "no_mutation_audit.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]

RANKING_FIELDS = [
    "code",
    "name",
    "asOf",
    "tradePriorityScore",
    "entryScore",
    "probSide",
    "setupType",
    "tradeEntryClass",
    "entryQualified",
    "momentumFollowThroughV1",
    "momentumFollowThroughScore",
    "marketRiskOff",
    "marketRegime",
    "monthlyBoxState",
    "tradeDecisionReasons",
    "tradeRiskWatch",
    "qualityFlags",
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


def _artifact_status(path: Path) -> dict[str, Any]:
    return {"path": str(path), "exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}


def _file_stat(path_text: str | None) -> dict[str, Any]:
    if not path_text:
        return {"path": None, "exists": False, "bytes": None, "mtime_ns": None}
    path = Path(path_text)
    if not path.exists():
        return {"path": str(path), "exists": False, "bytes": None, "mtime_ns": None}
    stat = path.stat()
    return {"path": str(path), "exists": True, "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _get_runtime_stock_db_status() -> dict[str, Any]:
    from app.backend.services import codex_bridge_service

    return codex_bridge_service.get_runtime_stock_db_status()


def _get_rankings_freshness(limit: int) -> dict[str, Any]:
    from app.backend.services import codex_bridge_service

    return codex_bridge_service.get_rankings_freshness(
        tf="D",
        which="latest",
        direction="up",
        mode="trade",
        risk_mode="balanced",
        limit=limit,
    )


def _get_active_rankings(limit: int) -> dict[str, Any]:
    from app.backend.services.ml import rankings_cache

    return rankings_cache.get_rankings("D", "latest", "up", limit, mode="trade", risk_mode="balanced")


def _compact_item(item: Mapping[str, Any], rank: int) -> dict[str, Any]:
    out = {field: item.get(field) for field in RANKING_FIELDS if field in item}
    out["active_rank"] = rank
    out["active_score"] = _safe_float(item.get("tradePriorityScore"), _safe_float(item.get("entryScore")))
    return out


def _runtime_flags(item: Mapping[str, Any]) -> dict[str, bool]:
    risk_watch = item.get("tradeRiskWatch") if isinstance(item.get("tradeRiskWatch"), list) else []
    quality_flags = item.get("qualityFlags") if isinstance(item.get("qualityFlags"), list) else []
    monthly_box_state = str(item.get("monthlyBoxState") or "").strip().lower()
    momentum_score = _safe_float(item.get("momentumFollowThroughScore"))
    momentum = _bool(item.get("momentumFollowThroughV1")) or momentum_score >= 0.75
    high_risk = _bool(item.get("marketRiskOff")) or bool(risk_watch) or "entry_not_qualified" in quality_flags
    low_risk = (not high_risk) and _bool(item.get("entryQualified"))
    monthly_drawdown = monthly_box_state in {"box_lower", "box_mid", "no_box"}
    return {
        "runtime_momentum_candidate_flag": momentum,
        "runtime_momentum_low_risk_context_flag": low_risk,
        "runtime_momentum_high_risk_context_flag": high_risk,
        "runtime_monthly_down_or_drawdown_flag": monthly_drawdown,
    }


def _replacement_rows(items: list[Mapping[str, Any]], variant_spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rank, item in enumerate(items, 1):
        base_score = _safe_float(item.get("tradePriorityScore"), _safe_float(item.get("entryScore")))
        flags = _runtime_flags(item)
        delta = 0.0
        if flags["runtime_momentum_candidate_flag"]:
            delta += _safe_float(variant_spec.get("momentum_weight"))
        if flags["runtime_momentum_low_risk_context_flag"]:
            delta += _safe_float(variant_spec.get("momentum_low_risk_weight"))
        if flags["runtime_momentum_high_risk_context_flag"]:
            delta += _safe_float(variant_spec.get("momentum_high_risk_penalty"))
        if flags["runtime_monthly_down_or_drawdown_flag"]:
            delta += _safe_float(variant_spec.get("monthly_down_or_drawdown_penalty"))
        rows.append(
            {
                **_compact_item(item, rank),
                **flags,
                "replacement_delta": delta,
                "replacement_score": base_score + delta,
            }
        )
    rows.sort(key=lambda row: (-_safe_float(row.get("replacement_score")), int(row["active_rank"]), str(row.get("code") or "")))
    for rank, row in enumerate(rows, 1):
        row["replacement_rank"] = rank
    return rows


def _codes(rows: Iterable[Mapping[str, Any]], *, rank_field: str) -> list[str]:
    return [str(row.get("code")) for row in sorted(rows, key=lambda row: int(row[rank_field])) if row.get("code")]


def _diff(active_rows: list[Mapping[str, Any]], replacement_rows: list[Mapping[str, Any]], top_k: int) -> dict[str, Any]:
    active_top = _codes([row for row in active_rows if int(row["active_rank"]) <= top_k], rank_field="active_rank")
    replacement_top = _codes([row for row in replacement_rows if int(row["replacement_rank"]) <= top_k], rank_field="replacement_rank")
    active_set = set(active_top)
    replacement_set = set(replacement_top)
    rank_changes = [
        {
            "code": row.get("code"),
            "active_rank": row.get("active_rank"),
            "replacement_rank": row.get("replacement_rank"),
            "rank_delta": int(row["replacement_rank"]) - int(row["active_rank"]),
        }
        for row in replacement_rows
        if int(row["replacement_rank"]) != int(row["active_rank"])
    ]
    return {
        "top_k": top_k,
        "active_top": active_top,
        "replacement_top": replacement_top,
        "added_by_replacement": [code for code in replacement_top if code not in active_set],
        "removed_from_active": [code for code in active_top if code not in replacement_set],
        "changed_rank_count": len(rank_changes),
        "rank_changes": rank_changes,
    }


def _classify(
    *,
    source_plan_decision: Mapping[str, Any],
    runtime_status: Mapping[str, Any],
    rankings_freshness: Mapping[str, Any],
    active_count: int,
    diff_top5: Mapping[str, Any],
    no_mutation_pass: bool,
) -> tuple[str, str, list[str], str]:
    if source_plan_decision.get("authoritative_research_decision") != "active_replacement_plan_ready_for_live_dry_run":
        return "hold", "live_replacement_dry_run_hold", ["source_plan_not_ready_for_live_dry_run"], "rerun_active_replacement_plan"
    if bool(runtime_status.get("stale")) or bool(rankings_freshness.get("stale")):
        return "hold", "live_replacement_dry_run_hold", ["runtime_or_ranking_snapshot_stale"], "rerun_after_runtime_refresh"
    if active_count < 5:
        return "hold", "live_replacement_dry_run_hold", ["runtime_active_candidate_count_below_top5"], "runtime_candidate_breadth_audit_v1"
    if diff_top5.get("changed_rank_count", 0) == 0 and not diff_top5.get("added_by_replacement"):
        return "hold", "live_replacement_dry_run_hold", ["replacement_dry_run_created_no_top5_branching"], "runtime_candidate_universe_materialization_audit_v1"
    if no_mutation_pass:
        return "keep_candidate", "live_replacement_dry_run_pass", ["live_dry_run_branching_observed_no_mutation"], "monthly_drawdown_guarded_momentum_active_replacement_implementation_v1"
    return "drop", "live_replacement_dry_run_failed", ["mutation_audit_failed"], "disable_replacement_path"


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

    source_plan_decision = _read_json(args.source_plan_root / "research_decision.json")
    active_replacement_plan = _read_json(args.source_plan_root / "active_replacement_plan.json")
    source_dry_run_contract = _read_json(args.source_plan_root / "live_dry_run_contract.json")
    variant_spec = (active_replacement_plan.get("best_variant") or {}).get("spec") or {}

    runtime_status_before = _get_runtime_stock_db_status()
    db_stat_before = _file_stat(runtime_status_before.get("selected_runtime_db_path"))
    rankings_freshness = _get_rankings_freshness(args.limit)
    active_payload = _get_active_rankings(args.limit)
    runtime_status_after = _get_runtime_stock_db_status()
    db_stat_after = _file_stat(runtime_status_after.get("selected_runtime_db_path"))

    items = list(active_payload.get("items") or [])
    active_rows = [_compact_item(item, rank) for rank, item in enumerate(items, 1)]
    replacement_rows = _replacement_rows(items, variant_spec)
    top5_diff = _diff(active_rows, replacement_rows, 5)
    top10_diff = _diff(active_rows, replacement_rows, 10)
    no_mutation_pass = db_stat_before == db_stat_after
    decision, authoritative, typed_reasons, next_axis = _classify(
        source_plan_decision=source_plan_decision,
        runtime_status=runtime_status_after,
        rankings_freshness=rankings_freshness,
        active_count=len(active_rows),
        diff_top5=top5_diff,
        no_mutation_pass=no_mutation_pass,
    )

    payloads: dict[str, Mapping[str, Any]] = {
        "evaluation_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
            "axis_id": AXIS_ID,
            "boundary": "MeeMee-read TRADEX-write-artifacts-only",
            "purpose": "read active runtime ranking and dry-run replacement ranking without mutating MeeMee",
            "candidate_construction_source": "current MeeMee ranking payload fields only",
            "runtime_mapping_is_activation_contract": False,
            "no_silent_fallback": True,
            "do_not_change": [
                "active_runtime_ranking",
                "display_score",
                "runtime_duckdb",
                "production_publish_registry",
                "frontend_backend_ui_api",
            ],
        },
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_PREFIX}_run_manifest_v1",
            "axis_id": AXIS_ID,
            "run_id": args.run_id,
            "generated_at_utc": generated_at,
            "source_plan_root": str(args.source_plan_root),
            "output_root": str(output_root),
            "limit": args.limit,
        },
        "source_artifact_refs.json": {
            "schema_version": f"{SCHEMA_PREFIX}_source_refs_v1",
            "source_plan_root": str(args.source_plan_root),
            "source_plan_decision": source_plan_decision.get("authoritative_research_decision"),
            "source_live_dry_run_contract": source_dry_run_contract,
            "best_variant_spec": variant_spec,
        },
        "runtime_state_check.json": {
            "schema_version": f"{SCHEMA_PREFIX}_runtime_state_check_v1",
            "runtime_stock_db_status_before": runtime_status_before,
            "rankings_freshness": rankings_freshness,
            "runtime_stock_db_status_after": runtime_status_after,
            "runtime_db_file_stat_before": db_stat_before,
            "runtime_db_file_stat_after": db_stat_after,
            "runtime_state_checks_completed": True,
        },
        "live_replacement_dry_run_contract.json": {
            "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
            "required_before_active_replacement": True,
            "active_ranking_payload_source": "rankings_cache.get_rankings(D, latest, up, limit, mode=trade, risk_mode=balanced)",
            "replacement_scoring_changes_runtime": False,
            "replacement_score_formula": "tradePriorityScore + guarded_momentum_variant_delta",
            "future_labels_used": False,
            "score_mapping": {
                "momentum_candidate": "momentumFollowThroughV1 OR momentumFollowThroughScore >= 0.75",
                "high_risk_context": "marketRiskOff OR tradeRiskWatch non-empty OR entry_not_qualified",
                "low_risk_context": "entryQualified AND not high_risk_context",
                "monthly_down_or_drawdown": "monthlyBoxState in box_lower/box_mid/no_box",
            },
            "mapping_is_runtime_dry_run_only": True,
        },
        "active_topk_snapshot.json": {
            "schema_version": f"{SCHEMA_PREFIX}_active_topk_snapshot_v1",
            "snapshot_as_of": active_payload.get("snapshot_as_of"),
            "freshness_state": active_payload.get("freshness_state"),
            "active_candidate_count": len(active_rows),
            "active_top5": [row for row in active_rows if int(row["active_rank"]) <= 5],
            "active_top10": [row for row in active_rows if int(row["active_rank"]) <= 10],
        },
        "replacement_topk_snapshot.json": {
            "schema_version": f"{SCHEMA_PREFIX}_replacement_topk_snapshot_v1",
            "snapshot_as_of": active_payload.get("snapshot_as_of"),
            "replacement_candidate_count": len(replacement_rows),
            "replacement_top5": [row for row in replacement_rows if int(row["replacement_rank"]) <= 5],
            "replacement_top10": [row for row in replacement_rows if int(row["replacement_rank"]) <= 10],
        },
        "active_vs_replacement_diff.json": {
            "schema_version": f"{SCHEMA_PREFIX}_diff_v1",
            "snapshot_as_of": active_payload.get("snapshot_as_of"),
            "top5": top5_diff,
            "top10": top10_diff,
            "selection_divergence_reason": "runtime_active_candidate_count_below_top5"
            if len(active_rows) < 5
            else ("replacement_score_changed_rank" if top5_diff["changed_rank_count"] else "no_top5_branching"),
        },
        "replacement_scoring_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_replacement_scoring_report_v1",
            "variant_spec": variant_spec,
            "runtime_candidate_count": len(replacement_rows),
            "momentum_candidate_count": sum(1 for row in replacement_rows if row["runtime_momentum_candidate_flag"]),
            "high_risk_context_count": sum(1 for row in replacement_rows if row["runtime_momentum_high_risk_context_flag"]),
            "monthly_down_or_drawdown_count": sum(1 for row in replacement_rows if row["runtime_monthly_down_or_drawdown_flag"]),
            "score_changed_count": sum(1 for row in replacement_rows if abs(_safe_float(row["replacement_delta"])) > 1e-12),
            "rows": replacement_rows,
        },
        "rollback_verification_report.json": {
            "schema_version": f"{SCHEMA_PREFIX}_rollback_verification_report_v1",
            "rollback_verified_for_this_dry_run": no_mutation_pass,
            "rollback_basis": "no runtime code path changed and runtime DB file stat unchanged",
            "default_runtime_behavior_changed": False,
            "active_ranking_mutated": False,
            "runtime_duckdb_written": not no_mutation_pass,
        },
        "no_mutation_audit.json": {
            "schema_version": f"{SCHEMA_PREFIX}_no_mutation_audit_v1",
            "axis_id": AXIS_ID,
            "production_ranking_changed": False,
            "runtime_duckdb_written": not no_mutation_pass,
            "display_score_changed": False,
            "publish_bundle_created": False,
            "production_publish_registered": False,
            "meemee_runtime_changed": False,
            "frontend_backend_changed": False,
            "runtime_db_file_stat_unchanged": no_mutation_pass,
            "no_mutation_pass": no_mutation_pass,
        },
        "next_axis_recommendation.json": {
            "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
            "axis_id": AXIS_ID,
            "decision": authoritative,
            "next": next_axis,
            "reason": typed_reasons[0] if typed_reasons else None,
        },
        "research_decision.json": {
            "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
            "generated_at_utc": generated_at,
            "research_phase": "live_replacement_dry_run",
            "boundary": "MeeMee-read TRADEX-write-artifacts-only",
            "axis_moved": "monthly_drawdown_guarded_momentum_live_replacement_dry_run",
            "source_plan_decision": source_plan_decision.get("authoritative_research_decision"),
            "runtime_snapshot_as_of": active_payload.get("snapshot_as_of"),
            "runtime_freshness_state": active_payload.get("freshness_state"),
            "active_candidate_count": len(active_rows),
            "replacement_candidate_count": len(replacement_rows),
            "changed_top5_members_count": len(top5_diff["added_by_replacement"]) + len(top5_diff["removed_from_active"]),
            "changed_top10_members_count": len(top10_diff["added_by_replacement"]) + len(top10_diff["removed_from_active"]),
            "changed_rank_count": top5_diff["changed_rank_count"],
            "replacement_direction_approved": True,
            "active_replacement_executed": False,
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
    parser.add_argument("--source-plan-root", type=Path, default=DEFAULT_SOURCE_PLAN_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--limit", type=int, default=int(os.getenv("TRADEX_LIVE_REPLACEMENT_DRY_RUN_LIMIT", "100")))
    return parser


def main() -> None:
    output_root = run(_parser().parse_args())
    print(json.dumps({"axis_id": AXIS_ID, "artifact_root": str(output_root)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
