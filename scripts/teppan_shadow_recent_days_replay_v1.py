"""Read-only recent-days replay for live-safe teppan shadow candidates."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from app.backend.services.teppan_live_safe_materialization import (
    build_input_dependency_audit,
    load_recent_runtime_ranking_rows,
    materialize_teppan_features,
)
from app.backend.services.teppan_shadow_integration_adapter import (
    DEFAULT_PLAN_ROOT,
    compute_teppan_shadow_adjusted_ranking,
    load_teppan_shadow_plan,
)
from scripts.teppan_shadow_runtime_coverage_observation_v2 import (
    MATERIALIZATION_FIX_ROOT,
    DEFAULT_PATTERN_ROOT,
    _baseline_no_boost_features,
    _baseline_shadow_rank_lookup,
    _enrich_candidate_rows,
    _file_stat,
    _topk_comparison,
    _topk_payload,
)


AXIS_ID = "teppan_shadow_recent_days_replay_v1"
DEFAULT_RUN_ID = "20260514T110000Z-teppan-shadow-recent-days-replay-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\shadow_recent_days_replays\teppan_shadow_recent_days_replay_v1")
TOP_KS = (5, 10, 20)
REQUIRED_OUTPUTS = [
    "recent_days_replay_contract.json",
    "replay_date_coverage_report.json",
    "active_shadow_topk_by_date.jsonl",
    "shadow_diff_by_date.jsonl",
    "teppan_coverage_by_date.jsonl",
    "human_review_candidate_list.json",
    "best_shadow_candidate_examples.json",
    "no_mutation_audit.json",
    "next_axis_recommendation.json",
    "replay_result.json",
    "_ARTIFACT_COMPLETE.json",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    parser.add_argument("--pattern-root", type=Path, default=DEFAULT_PATTERN_ROOT)
    parser.add_argument("--materialization-fix-root", type=Path, default=MATERIALIZATION_FIX_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--direction", default="up")
    parser.add_argument("--risk-mode", default="balanced")
    parser.add_argument("--recent-dates", type=int, default=10)
    parser.add_argument("--rank-limit", type=int, default=100)
    args = parser.parse_args()
    run_teppan_shadow_recent_days_replay_v1(
        plan_root=args.plan_root,
        pattern_root=args.pattern_root,
        materialization_fix_root=args.materialization_fix_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
        direction=args.direction,
        risk_mode=args.risk_mode,
        recent_dates=args.recent_dates,
        rank_limit=args.rank_limit,
    )
    return 0


def run_teppan_shadow_recent_days_replay_v1(
    *,
    plan_root: Path = DEFAULT_PLAN_ROOT,
    pattern_root: Path = DEFAULT_PATTERN_ROOT,
    materialization_fix_root: Path = MATERIALIZATION_FIX_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
    direction: str = "up",
    risk_mode: str = "balanced",
    recent_dates: int = 10,
    rank_limit: int = 100,
    runtime_status: Mapping[str, Any] | None = None,
    rankings_freshness: Mapping[str, Any] | None = None,
    active_rows: Sequence[Mapping[str, Any]] | None = None,
    materialized_rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    plan = load_teppan_shadow_plan(plan_root)
    effective_runtime_status = dict(runtime_status or get_runtime_stock_db_status())
    effective_rankings_freshness = dict(
        rankings_freshness
        or get_rankings_freshness(tf="D", which="latest", direction=direction, mode="trade", risk_mode=risk_mode, limit=20)
    )
    db_path = Path(str(effective_runtime_status.get("selected_runtime_db_path") or ""))
    db_stat_before = _file_stat(db_path)

    source_rows = list(active_rows) if active_rows is not None else load_recent_runtime_ranking_rows(
        db_path,
        direction=direction,
        recent_dates=recent_dates,
        rank_limit=rank_limit,
    )
    materialization = (
        {
            "rows": list(materialized_rows),
            "summary": _materialization_summary(materialized_rows or []),
            "input_dependency_audit": build_input_dependency_audit(),
        }
        if materialized_rows is not None
        else materialize_teppan_features(source_rows, db_path=db_path, pattern_root=pattern_root)
    )
    feature_rows = list(materialization["rows"])
    baseline_shadow = compute_teppan_shadow_adjusted_ranking(source_rows, _baseline_no_boost_features(source_rows), plan)
    baseline_candidate_rows = _enrich_candidate_rows(
        baseline_shadow["shadow_rows"],
        source_rows,
        _baseline_no_boost_features(source_rows),
        _baseline_shadow_rank_lookup(baseline_shadow["shadow_rows"]),
    )
    shadow_result = compute_teppan_shadow_adjusted_ranking(source_rows, feature_rows, plan)
    candidate_rows = _enrich_candidate_rows(
        shadow_result["shadow_rows"],
        source_rows,
        feature_rows,
        _baseline_shadow_rank_lookup(baseline_shadow["shadow_rows"]),
    )
    by_date = _group_by_date(candidate_rows)
    baseline_by_date = _group_by_date(baseline_candidate_rows)
    active_shadow_topk_by_date, shadow_diff_by_date, coverage_by_date = _date_replay_rows(by_date, baseline_by_date)
    coverage_report = _coverage_report(coverage_by_date, shadow_diff_by_date)
    human_review = _human_review_candidate_list(candidate_rows, shadow_diff_by_date)
    examples = _best_shadow_candidate_examples(candidate_rows, shadow_diff_by_date)

    db_stat_after = _file_stat(db_path)
    no_mutation = _no_mutation_audit(
        db_path=db_path,
        db_stat_before=db_stat_before,
        db_stat_after=db_stat_after,
        adapter_audit=shadow_result["audit"],
    )
    decision, decision_reason = _decision(coverage_report, human_review, no_mutation)
    next_axis = _next_axis_recommendation(decision)
    contract = _contract(
        plan_root=plan.plan_root,
        pattern_root=pattern_root,
        materialization_fix_root=materialization_fix_root,
        recent_dates=recent_dates,
        rank_limit=rank_limit,
    )
    result = {
        "schema_version": "teppan_shadow_recent_days_replay_result_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": decision_reason,
        "generated_at_utc": _utc_now(),
        "integration_mode": "read_only_recent_days_shadow_replay",
        "runtime_stock_db_status": effective_runtime_status,
        "rankings_freshness": effective_rankings_freshness,
        "contract": contract,
        "required_metrics": coverage_report["metrics"],
        "human_review_candidate_count": len(human_review),
        "best_shadow_candidate_example_count": len(examples["examples"]),
        "no_mutation_audit": no_mutation,
        "next_axis_recommendation": next_axis,
        "not_changed": contract["not_changed"],
    }

    _write_json(output_root / "recent_days_replay_contract.json", contract)
    _write_json(output_root / "replay_date_coverage_report.json", coverage_report)
    _write_jsonl(output_root / "active_shadow_topk_by_date.jsonl", active_shadow_topk_by_date)
    _write_jsonl(output_root / "shadow_diff_by_date.jsonl", shadow_diff_by_date)
    _write_jsonl(output_root / "teppan_coverage_by_date.jsonl", coverage_by_date)
    _write_json(output_root / "human_review_candidate_list.json", {"human_review_candidate_list": human_review, "max_user_selection": 3})
    _write_json(output_root / "best_shadow_candidate_examples.json", examples)
    _write_json(output_root / "no_mutation_audit.json", no_mutation)
    _write_json(output_root / "next_axis_recommendation.json", next_axis)
    _write_json(output_root / "replay_result.json", result)
    complete = _artifact_complete(output_root, result)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "replay_result": result,
        "replay_date_coverage_report": coverage_report,
        "artifact_complete": complete,
    }


def _date_replay_rows(
    by_date: Mapping[str, Sequence[Mapping[str, Any]]],
    baseline_by_date: Mapping[str, Sequence[Mapping[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    topk_rows = []
    diff_rows = []
    coverage_rows = []
    for date, rows in sorted(by_date.items()):
        topk = _topk_payload(rows)
        baseline_topk = _topk_payload(baseline_by_date.get(date, []))
        active_comparison = _topk_comparison(topk["active"], topk["shadow"])
        comparison = _topk_comparison(baseline_topk["shadow"], topk["shadow"])
        topk_rows.append(
            {
                "schema_version": "teppan_shadow_recent_days_active_shadow_topk_by_date_v1",
                "anchor_date": date,
                "active_top5": topk["active"]["top5"],
                "active_top10": topk["active"]["top10"],
                "active_top20": topk["active"]["top20"],
                "shadow_top5": topk["shadow"]["top5"],
                "shadow_top10": topk["shadow"]["top10"],
                "shadow_top20": topk["shadow"]["top20"],
                "baseline_no_boost_shadow_top5": baseline_topk["shadow"]["top5"],
                "baseline_no_boost_shadow_top10": baseline_topk["shadow"]["top10"],
                "baseline_no_boost_shadow_top20": baseline_topk["shadow"]["top20"],
            }
        )
        diff_rows.append(
            {
                "schema_version": "teppan_shadow_recent_days_shadow_diff_by_date_v1",
                "anchor_date": date,
                "diff_basis": "baseline_no_boost_shadow_vs_teppan_shadow",
                "added_by_shadow_top5": comparison["top5"]["added_by_shadow"],
                "removed_from_active_top5": comparison["top5"]["removed_from_active"],
                "added_by_shadow_top10": comparison["top10"]["added_by_shadow"],
                "removed_from_active_top10": comparison["top10"]["removed_from_active"],
                "added_by_shadow_top20": comparison["top20"]["added_by_shadow"],
                "removed_from_active_top20": comparison["top20"]["removed_from_active"],
                "added_by_shadow_top5_count": len(comparison["top5"]["added_by_shadow"]),
                "added_by_shadow_top10_count": len(comparison["top10"]["added_by_shadow"]),
                "added_by_shadow_top20_count": len(comparison["top20"]["added_by_shadow"]),
                "changed_member_count_top5": comparison["top5"]["changed_member_count"],
                "changed_member_count_top10": comparison["top10"]["changed_member_count"],
                "changed_member_count_top20": comparison["top20"]["changed_member_count"],
                "changed_rank_count_by_teppan_boost": sum(1 for row in rows if row.get("shadow_rank_changed_by_teppan_boost") is True),
                "active_vs_shadow_changed_member_count_top5": active_comparison["top5"]["changed_member_count"],
                "active_vs_shadow_changed_member_count_top10": active_comparison["top10"]["changed_member_count"],
                "active_vs_shadow_changed_member_count_top20": active_comparison["top20"]["changed_member_count"],
            }
        )
        coverage_rows.append(_coverage_by_date(date, rows))
    return topk_rows, diff_rows, coverage_rows


def _coverage_by_date(date: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": "teppan_shadow_recent_days_teppan_coverage_by_date_v1", "anchor_date": date}
    for k in (5, 10, 20, 50, 100):
        out[f"top{k}"] = _coverage_bucket([row for row in rows if int(row["active_rank"]) <= k])
    out["all_observed"] = _coverage_bucket(rows)
    out["shadow_decision_reason_counts"] = dict(Counter(str(row.get("shadow_decision_reason")) for row in rows))
    return out


def _coverage_report(
    coverage_by_date: Sequence[Mapping[str, Any]],
    shadow_diff_by_date: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    metrics = {
        "replay_date_count": len(coverage_by_date),
        "dates_with_teppan_pattern_match_top20": _dates_with_count(coverage_by_date, "top20", "teppan_pattern_match_count"),
        "dates_with_teppan_pattern_match_top50": _dates_with_count(coverage_by_date, "top50", "teppan_pattern_match_count"),
        "dates_with_teppan_pattern_match_top100": _dates_with_count(coverage_by_date, "top100", "teppan_pattern_match_count"),
        "dates_with_shadow_top5_additions": _dates_with_diff(shadow_diff_by_date, "added_by_shadow_top5_count"),
        "dates_with_shadow_top10_additions": _dates_with_diff(shadow_diff_by_date, "added_by_shadow_top10_count"),
        "dates_with_shadow_top20_additions": _dates_with_diff(shadow_diff_by_date, "added_by_shadow_top20_count"),
        "total_boosted_candidate_count": sum(int(row["all_observed"]["boosted_candidate_count"]) for row in coverage_by_date),
        "total_loss_guard_blocked_count": sum(int(row["all_observed"]["loss_guard_blocked_count"]) for row in coverage_by_date),
        "added_by_shadow_top5_count": sum(int(row["added_by_shadow_top5_count"]) for row in shadow_diff_by_date),
        "added_by_shadow_top10_count": sum(int(row["added_by_shadow_top10_count"]) for row in shadow_diff_by_date),
        "added_by_shadow_top20_count": sum(int(row["added_by_shadow_top20_count"]) for row in shadow_diff_by_date),
        "human_review_candidate_count": 0,
    }
    return {
        "schema_version": "teppan_shadow_recent_days_replay_date_coverage_report_v1",
        "metrics": metrics,
        "active_vs_shadow_topK_delta_by_date": [
            {
                "anchor_date": row["anchor_date"],
                "top5_added": row["added_by_shadow_top5_count"],
                "top10_added": row["added_by_shadow_top10_count"],
                "top20_added": row["added_by_shadow_top20_count"],
                "changed_rank_count_by_teppan_boost": row["changed_rank_count_by_teppan_boost"],
            }
            for row in shadow_diff_by_date
        ],
        "teppan_coverage_by_date": [
            {
                "anchor_date": row["anchor_date"],
                "top20_teppan_pattern_match_count": row["top20"]["teppan_pattern_match_count"],
                "top50_teppan_pattern_match_count": row["top50"]["teppan_pattern_match_count"],
                "top100_teppan_pattern_match_count": row["top100"]["teppan_pattern_match_count"],
                "top100_teppan_guard_pass_count": row["top100"]["teppan_guard_pass_count"],
                "top100_boosted_candidate_count": row["top100"]["boosted_candidate_count"],
                "top100_loss_guard_blocked_count": row["top100"]["loss_guard_blocked_count"],
            }
            for row in coverage_by_date
        ],
    }


def _human_review_candidate_list(
    candidate_rows: Sequence[Mapping[str, Any]],
    shadow_diff_by_date: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    picked: dict[tuple[str, str], dict[str, Any]] = {}
    for diff in shadow_diff_by_date:
        for topk in ("top5", "top10", "top20"):
            for row in diff[f"added_by_shadow_{topk}"]:
                candidate = dict(row)
                candidate["human_review_reason"] = f"added_by_shadow_{topk}"
                picked[(str(candidate["anchor_date"]), str(candidate["symbol"]))] = candidate
    for row in candidate_rows:
        if row.get("teppan_guarded_boost_applied") is True:
            candidate = dict(row)
            candidate["human_review_reason"] = "boosted_teppan_guard_pass"
            picked.setdefault((str(candidate["anchor_date"]), str(candidate["symbol"])), candidate)
    return _candidate_sort(picked.values())[:30]


def _best_shadow_candidate_examples(
    candidate_rows: Sequence[Mapping[str, Any]],
    shadow_diff_by_date: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    added_keys = {
        (str(row["anchor_date"]), str(row["symbol"]))
        for diff in shadow_diff_by_date
        for key in ("added_by_shadow_top5", "added_by_shadow_top10", "added_by_shadow_top20")
        for row in diff[key]
    }
    examples = []
    for row in candidate_rows:
        if row.get("teppan_pattern_match") is not True:
            continue
        candidate = dict(row)
        candidate["example_reason"] = "added_by_shadow" if (str(row["anchor_date"]), str(row["symbol"])) in added_keys else (
            "boosted_teppan_guard_pass" if row.get("teppan_guarded_boost_applied") is True else "teppan_pattern_match_observed"
        )
        examples.append(candidate)
    return {
        "schema_version": "teppan_shadow_recent_days_best_shadow_candidate_examples_v1",
        "examples": _candidate_sort(examples)[:20],
    }


def _candidate_sort(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    reason_order = {"added_by_shadow_top5": 0, "added_by_shadow_top10": 1, "added_by_shadow_top20": 2, "added_by_shadow": 3, "boosted_teppan_guard_pass": 4}
    return [
        _compact(row)
        for row in sorted(
            rows,
            key=lambda row: (
                reason_order.get(str(row.get("human_review_reason") or row.get("example_reason")), 9),
                str(row.get("anchor_date")),
                int(row.get("shadow_adjusted_rank") or 999),
                int(row.get("active_rank") or 999),
                str(row.get("symbol")),
            ),
        )
    ]


def _decision(
    coverage_report: Mapping[str, Any],
    human_review: Sequence[Mapping[str, Any]],
    no_mutation: Mapping[str, Any],
) -> tuple[str, str]:
    if no_mutation.get("no_mutation_pass") is not True:
        return "drop_shadow_live_value", "no_mutation_audit_failed"
    metrics = coverage_report["metrics"]
    top_addition_dates = set(metrics["dates_with_shadow_top5_additions"]) | set(metrics["dates_with_shadow_top10_additions"]) | set(
        metrics["dates_with_shadow_top20_additions"]
    )
    top100_dates = set(metrics["dates_with_teppan_pattern_match_top100"])
    if len(top_addition_dates) >= 2 and human_review:
        return "recent_days_replay_pass", "multiple_recent_dates_created_shadow_topk_additions"
    if top_addition_dates and human_review:
        return "hold_for_candidate_manual_review", "recent_replay_created_concrete_shadow_candidates_for_review"
    if top100_dates or int(metrics["total_boosted_candidate_count"]) > 0:
        return "hold_for_watch_mode", "teppan_coverage_exists_but_shadow_topk_additions_are_sparse_or_absent"
    return "drop_shadow_live_value", "recent_replay_no_meaningful_topk_additions_or_review_candidates"


def _next_axis_recommendation(decision: str) -> dict[str, Any]:
    next_by_decision = {
        "recent_days_replay_pass": "teppan_shadow_candidate_manual_review_v1",
        "hold_for_candidate_manual_review": "teppan_shadow_candidate_manual_review_v1",
        "hold_for_watch_mode": "teppan_shadow_watch_mode_logging_plan_v1",
        "drop_shadow_live_value": "keep_teppan_shadow_inactive_tradex_research_keep_only",
    }
    return {
        "schema_version": "teppan_shadow_recent_days_next_axis_recommendation_v1",
        "decision": decision,
        "next": next_by_decision.get(decision, "keep_teppan_shadow_inactive_tradex_research_keep_only"),
        "activation_allowed": False,
    }


def _contract(
    *,
    plan_root: Path,
    pattern_root: Path,
    materialization_fix_root: Path,
    recent_dates: int,
    rank_limit: int,
) -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_recent_days_replay_contract_v1",
        "axis_id": AXIS_ID,
        "purpose": "read_only_recent_runtime_ranking_dates_shadow_replay",
        "plan_root": str(plan_root),
        "pattern_root": str(pattern_root),
        "materialization_fix_root": str(materialization_fix_root),
        "recent_dates": int(recent_dates),
        "rank_limit": int(rank_limit),
        "uses_live_safe_materialization": True,
        "future_labels_allowed": False,
        "historical_evaluation_filter_allowed": False,
        "activation_allowed": False,
        "not_changed": [
            "active_ranking",
            "display_score",
            "runtime_duckdb",
            "production_publish_registry",
            "frontend_ui",
            "backend_api_response",
            "boost_value",
            "loss_guard_semantics",
            "pattern_definitions",
        ],
    }


def _no_mutation_audit(
    *,
    db_path: Path,
    db_stat_before: Mapping[str, Any],
    db_stat_after: Mapping[str, Any],
    adapter_audit: Mapping[str, Any],
) -> dict[str, Any]:
    unchanged = db_stat_before == db_stat_after
    return {
        "schema_version": "teppan_shadow_recent_days_replay_no_mutation_audit_v1",
        "runtime_duckdb_path": str(db_path),
        "runtime_duckdb_stat_before": dict(db_stat_before),
        "runtime_duckdb_stat_after": dict(db_stat_after),
        "runtime_duckdb_unchanged": unchanged,
        "runtime_duckdb_written": not unchanged,
        "active_ranking_invariance_pass": bool(adapter_audit.get("active_ranking_invariance_pass")),
        "active_rank_unchanged": bool(adapter_audit.get("active_rank_unchanged")),
        "display_score_unchanged": bool(adapter_audit.get("active_display_score_unchanged")),
        "production_publish_registered": False,
        "frontend_changed": False,
        "backend_api_response_changed": False,
        "no_mutation_pass": unchanged and bool(adapter_audit.get("active_ranking_invariance_pass")),
        "silent_fallback_used": False,
    }


def _coverage_bucket(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    pattern = sum(1 for row in rows if row.get("teppan_pattern_match") is True)
    guard = sum(1 for row in rows if row.get("teppan_guard_pass") is True)
    blocked = sum(1 for row in rows if row.get("loss_guard_blocked") is True)
    boosted = sum(1 for row in rows if row.get("teppan_guarded_boost_applied") is True)
    changed_rank = sum(1 for row in rows if row.get("shadow_rank_changed_by_teppan_boost") is True)
    return {
        "row_count": count,
        "teppan_pattern_match_count": pattern,
        "teppan_pattern_match_rate": _rate(pattern, count),
        "teppan_guard_pass_count": guard,
        "teppan_guard_pass_rate": _rate(guard, count),
        "loss_guard_blocked_count": blocked,
        "loss_guard_blocked_rate": _rate(blocked, count),
        "boosted_candidate_count": boosted,
        "boosted_candidate_rate": _rate(boosted, count),
        "changed_rank_count_by_teppan_boost": changed_rank,
    }


def _dates_with_count(rows: Sequence[Mapping[str, Any]], bucket: str, metric: str) -> list[str]:
    return [str(row["anchor_date"]) for row in rows if int((row.get(bucket) or {}).get(metric) or 0) > 0]


def _dates_with_diff(rows: Sequence[Mapping[str, Any]], metric: str) -> list[str]:
    return [str(row["anchor_date"]) for row in rows if int(row.get(metric) or 0) > 0]


def _materialization_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "row_count": len(rows),
        "teppan_pattern_match_count": sum(1 for row in rows if row.get("teppan_pattern_match") is True),
        "teppan_guard_pass_count": sum(1 for row in rows if row.get("teppan_guard_pass") is True),
        "loss_guard_blocked_count": sum(1 for row in rows if row.get("loss_guard_blocked") is True),
        "future_label_inputs_used": False,
    }


def _group_by_date(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["anchor_date"])].append(row)
    return grouped


def _artifact_complete(output_root: Path, result: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "teppan_shadow_recent_days_replay_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": result.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _rate(num: int, denom: int) -> float | None:
    return None if denom <= 0 else float(num) / float(denom)


def _compact(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _json_ready(value) for key, value in row.items() if value is not None}


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


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_json_ready(dict(row)), ensure_ascii=False, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
