"""Read-only watch-mode logger for inactive teppan shadow."""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from app.backend.services.teppan_live_safe_materialization import load_recent_runtime_ranking_rows, materialize_teppan_features
from app.backend.services.teppan_shadow_integration_adapter import (
    DEFAULT_PLAN_ROOT,
    compute_teppan_shadow_adjusted_ranking,
    load_teppan_shadow_plan,
)
from scripts.teppan_shadow_recent_days_replay_v1 import (
    DEFAULT_PATTERN_ROOT,
    _baseline_no_boost_features,
    _baseline_shadow_rank_lookup,
    _coverage_bucket,
    _file_stat,
    _no_mutation_audit,
    _topk_comparison,
    _topk_payload,
)
from scripts.teppan_shadow_runtime_coverage_observation_v2 import _enrich_candidate_rows


AXIS_ID = "teppan_shadow_watch_mode_logging_v1"
DEFAULT_RUN_ID = "20260514T130000Z-teppan-shadow-watch-mode-logging-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\shadow_watch_logs\teppan_shadow_watch_mode_logging_v1")
DEFAULT_WATCH_PLAN_ROOT = Path(
    r"G:\Tradex\shadow_watch_mode_logging_plans\teppan_shadow_watch_mode_logging_plan_v1"
    r"\20260514T120000Z-teppan-shadow-watch-mode-logging-plan-v1"
)
TOP_KS = (5, 10, 20, 50, 100)
REQUIRED_OUTPUTS = [
    "watch_run_result.json",
    "teppan_watch_metrics.json",
    "active_shadow_topk_snapshot.json",
    "boost_opportunity_report.json",
    "human_review_trigger_report.json",
    "no_mutation_audit.json",
    "_ARTIFACT_COMPLETE.json",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch-plan-root", type=Path, default=DEFAULT_WATCH_PLAN_ROOT)
    parser.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    parser.add_argument("--pattern-root", type=Path, default=DEFAULT_PATTERN_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--direction", default="up")
    parser.add_argument("--risk-mode", default="balanced")
    parser.add_argument("--rank-limit", type=int, default=100)
    args = parser.parse_args()
    run_teppan_shadow_watch_mode_logging_v1(
        watch_plan_root=args.watch_plan_root,
        plan_root=args.plan_root,
        pattern_root=args.pattern_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
        direction=args.direction,
        risk_mode=args.risk_mode,
        rank_limit=args.rank_limit,
    )
    return 0


def run_teppan_shadow_watch_mode_logging_v1(
    *,
    watch_plan_root: Path = DEFAULT_WATCH_PLAN_ROOT,
    plan_root: Path = DEFAULT_PLAN_ROOT,
    pattern_root: Path = DEFAULT_PATTERN_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
    direction: str = "up",
    risk_mode: str = "balanced",
    rank_limit: int = 100,
    runtime_status: Mapping[str, Any] | None = None,
    rankings_freshness: Mapping[str, Any] | None = None,
    active_rows: Sequence[Mapping[str, Any]] | None = None,
    materialized_rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    watch_plan = _load_watch_plan(watch_plan_root)
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
        recent_dates=1,
        rank_limit=rank_limit,
    )
    if not source_rows:
        raise ValueError("watch_logger_runtime_candidate_rows_empty")
    ranking_date = max(str(row["anchor_date"]) for row in source_rows)
    source_rows = [dict(row) for row in source_rows if str(row.get("anchor_date")) == ranking_date]
    materialization = (
        {"rows": list(materialized_rows), "summary": _materialization_summary(materialized_rows or [])}
        if materialized_rows is not None
        else materialize_teppan_features(source_rows, db_path=db_path, pattern_root=pattern_root)
    )
    feature_rows = list(materialization["rows"])
    baseline_shadow = compute_teppan_shadow_adjusted_ranking(source_rows, _baseline_no_boost_features(source_rows), plan)
    baseline_lookup = _baseline_shadow_rank_lookup(baseline_shadow["shadow_rows"])
    baseline_candidate_rows = _enrich_candidate_rows(
        baseline_shadow["shadow_rows"],
        source_rows,
        _baseline_no_boost_features(source_rows),
        baseline_lookup,
    )
    shadow_result = compute_teppan_shadow_adjusted_ranking(source_rows, feature_rows, plan)
    candidate_rows = _enrich_candidate_rows(shadow_result["shadow_rows"], source_rows, feature_rows, baseline_lookup)

    topk = _topk_payload(candidate_rows)
    baseline_topk = _topk_payload(baseline_candidate_rows)
    active_comparison = _topk_comparison(topk["active"], topk["shadow"])
    baseline_comparison = _topk_comparison(baseline_topk["shadow"], topk["shadow"])
    coverage = _coverage_by_topk(candidate_rows)
    boost_report = _boost_opportunity_report(candidate_rows, baseline_comparison)
    db_stat_after = _file_stat(db_path)
    no_mutation = _no_mutation_audit(
        db_path=db_path,
        db_stat_before=db_stat_before,
        db_stat_after=db_stat_after,
        adapter_audit=shadow_result["audit"],
    )
    trigger_report = _human_review_trigger_report(boost_report, baseline_comparison, no_mutation)
    metrics = _watch_metrics(
        ranking_date=ranking_date,
        candidate_rows=candidate_rows,
        coverage=coverage,
        boost_report=boost_report,
        comparison=baseline_comparison,
        trigger_report=trigger_report,
        no_mutation=no_mutation,
    )
    decision, decision_reason = _decision(trigger_report, no_mutation)
    result = {
        "schema_version": "teppan_shadow_watch_run_result_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": decision_reason,
        "logger_status": "watch_logger_ready" if no_mutation.get("no_mutation_pass") is True else "logger_failed",
        "ranking_date": ranking_date,
        "generated_at_utc": _utc_now(),
        "integration_mode": "read_only_watch_mode_inactive_shadow",
        "watch_plan_root": str(watch_plan_root),
        "watch_plan_decision": watch_plan["research_decision"].get("decision"),
        "runtime_stock_db_status": effective_runtime_status,
        "rankings_freshness": effective_rankings_freshness,
        "activation_allowed": False,
        "manual_review_triggered": trigger_report["manual_review_triggered"],
        "trigger_reason": trigger_report["trigger_reason"],
        "metrics": metrics,
        "not_changed": [
            "active_rank",
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

    _write_json(output_root / "watch_run_result.json", result)
    _write_json(output_root / "teppan_watch_metrics.json", metrics)
    _write_json(
        output_root / "active_shadow_topk_snapshot.json",
        {
            "schema_version": "teppan_shadow_active_shadow_topk_snapshot_v1",
            "ranking_date": ranking_date,
            "active": topk["active"],
            "baseline_no_boost_shadow": baseline_topk["shadow"],
            "shadow": topk["shadow"],
            "active_vs_shadow_topk_comparison": active_comparison,
            "baseline_no_boost_vs_teppan_shadow_topk_comparison": baseline_comparison,
        },
    )
    _write_json(output_root / "boost_opportunity_report.json", boost_report)
    _write_json(output_root / "human_review_trigger_report.json", trigger_report)
    _write_json(output_root / "no_mutation_audit.json", no_mutation)
    complete = _artifact_complete(output_root, result)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "watch_run_result": result,
        "teppan_watch_metrics": metrics,
        "human_review_trigger_report": trigger_report,
        "artifact_complete": complete,
    }


def _load_watch_plan(root: Path) -> dict[str, Any]:
    required = {
        "research_decision": root / "research_decision.json",
        "watch_mode_logging_plan": root / "watch_mode_logging_plan.json",
        "watch_trigger_conditions": root / "watch_trigger_conditions.json",
        "human_review_trigger_contract": root / "human_review_trigger_contract.json",
        "no_activation_policy": root / "no_activation_policy.json",
        "artifact_complete": root / "_ARTIFACT_COMPLETE.json",
    }
    payload = {key: _read_json(path) for key, path in required.items()}
    issues = []
    if payload["research_decision"].get("decision") != "watch_mode_ready":
        issues.append("watch_plan_not_ready")
    if payload["research_decision"].get("activation_allowed") is not False:
        issues.append("watch_plan_activation_not_forbidden")
    if payload["no_activation_policy"].get("activation_allowed") is not False:
        issues.append("no_activation_policy_activation_not_forbidden")
    if payload["artifact_complete"].get("complete") is not True:
        issues.append("watch_plan_artifact_incomplete")
    if issues:
        raise ValueError(";".join(issues))
    return payload


def _coverage_by_topk(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_watch_coverage_by_topk_v1",
        **{f"top{k}": _coverage_bucket([row for row in rows if int(row["active_rank"]) <= k]) for k in TOP_KS},
        "all_observed": _coverage_bucket(rows),
    }


def _boost_opportunity_report(
    rows: Sequence[Mapping[str, Any]],
    comparison: Mapping[str, Any],
) -> dict[str, Any]:
    boost_eligible = [row for row in rows if row.get("teppan_guarded_boost_applied") is True]
    pattern_rows = [row for row in rows if row.get("teppan_pattern_match") is True]
    near_top20 = [row for row in boost_eligible if int(row.get("shadow_adjusted_rank") or 999) <= 25]
    return {
        "schema_version": "teppan_shadow_boost_opportunity_report_v1",
        "boost_eligible_count": len(boost_eligible),
        "boost_eligible_near_top20_count": len(near_top20),
        "loss_guard_blocked_count": sum(1 for row in rows if row.get("loss_guard_blocked") is True),
        "added_by_shadow_top5": comparison["top5"]["added_by_shadow"],
        "added_by_shadow_top10": comparison["top10"]["added_by_shadow"],
        "added_by_shadow_top20": comparison["top20"]["added_by_shadow"],
        "added_by_shadow_top5_count": len(comparison["top5"]["added_by_shadow"]),
        "added_by_shadow_top10_count": len(comparison["top10"]["added_by_shadow"]),
        "added_by_shadow_top20_count": len(comparison["top20"]["added_by_shadow"]),
        "nearest_shadow_candidate_to_top5": _nearest_candidate(boost_eligible or pattern_rows, target_rank=5),
        "nearest_shadow_candidate_to_top10": _nearest_candidate(boost_eligible or pattern_rows, target_rank=10),
        "boost_eligible_candidates": _candidate_sort(boost_eligible)[:10],
        "pattern_match_candidates": _candidate_sort(pattern_rows)[:10],
    }


def _human_review_trigger_report(
    boost_report: Mapping[str, Any],
    comparison: Mapping[str, Any],
    no_mutation: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_pool: list[dict[str, Any]] = []
    reason = "continue_watch_only_no_trigger"
    if len(comparison["top5"]["added_by_shadow"]) > 0:
        reason = "added_by_shadow_top5"
        candidate_pool.extend(dict(row, human_review_reason="added_by_shadow_top5") for row in comparison["top5"]["added_by_shadow"])
    elif len(comparison["top10"]["added_by_shadow"]) > 0:
        reason = "added_by_shadow_top10"
        candidate_pool.extend(dict(row, human_review_reason="added_by_shadow_top10") for row in comparison["top10"]["added_by_shadow"])
    elif int(boost_report.get("boost_eligible_near_top20_count") or 0) >= 2:
        reason = "boost_eligible_near_top20_count_gte_2"
        candidate_pool.extend(dict(row, human_review_reason="boost_eligible_near_top20") for row in boost_report.get("boost_eligible_candidates") or [])

    no_mutation_pass = no_mutation.get("no_mutation_pass") is True
    candidates = _candidate_sort(candidate_pool)[:3] if no_mutation_pass else []
    triggered = bool(candidates) and no_mutation_pass
    return {
        "schema_version": "teppan_shadow_human_review_trigger_report_v1",
        "manual_review_triggered": triggered,
        "trigger_reason": reason if triggered else "continue_watch_only",
        "human_review_candidate_count": len(candidates),
        "human_review_candidate_list": candidates,
        "activation_allowed_after_trigger": False,
        "next_if_triggered": "teppan_shadow_candidate_manual_review_v1",
        "next_if_not_triggered": "keep_inactive_watch_mode",
        "no_mutation_pass": no_mutation_pass,
    }


def _watch_metrics(
    *,
    ranking_date: str,
    candidate_rows: Sequence[Mapping[str, Any]],
    coverage: Mapping[str, Any],
    boost_report: Mapping[str, Any],
    comparison: Mapping[str, Any],
    trigger_report: Mapping[str, Any],
    no_mutation: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_watch_metrics_v1",
        "ranking_date": ranking_date,
        "runtime_candidate_count": len(candidate_rows),
        "top20_teppan_pattern_match_count": coverage["top20"]["teppan_pattern_match_count"],
        "top50_teppan_pattern_match_count": coverage["top50"]["teppan_pattern_match_count"],
        "top100_teppan_pattern_match_count": coverage["top100"]["teppan_pattern_match_count"],
        "top20_teppan_guard_pass_count": coverage["top20"]["teppan_guard_pass_count"],
        "top50_teppan_guard_pass_count": coverage["top50"]["teppan_guard_pass_count"],
        "top100_teppan_guard_pass_count": coverage["top100"]["teppan_guard_pass_count"],
        "boost_eligible_count": boost_report["boost_eligible_count"],
        "loss_guard_blocked_count": boost_report["loss_guard_blocked_count"],
        "added_by_shadow_top5": boost_report["added_by_shadow_top5_count"],
        "added_by_shadow_top10": boost_report["added_by_shadow_top10_count"],
        "added_by_shadow_top20": boost_report["added_by_shadow_top20_count"],
        "nearest_shadow_candidate_to_top5": boost_report["nearest_shadow_candidate_to_top5"],
        "nearest_shadow_candidate_to_top10": boost_report["nearest_shadow_candidate_to_top10"],
        "human_review_candidate_count": trigger_report["human_review_candidate_count"],
        "manual_review_triggered": trigger_report["manual_review_triggered"],
        "trigger_reason": trigger_report["trigger_reason"],
        "no_mutation_pass": no_mutation["no_mutation_pass"],
        "topk_diff_basis": "baseline_no_boost_shadow_vs_teppan_shadow",
        "active_vs_shadow_changed_member_count_top5": comparison["top5"]["changed_member_count"],
        "active_vs_shadow_changed_member_count_top10": comparison["top10"]["changed_member_count"],
        "active_vs_shadow_changed_member_count_top20": comparison["top20"]["changed_member_count"],
    }


def _decision(trigger_report: Mapping[str, Any], no_mutation: Mapping[str, Any]) -> tuple[str, str]:
    if no_mutation.get("no_mutation_pass") is not True:
        return "logger_failed", "no_mutation_audit_failed"
    if trigger_report.get("manual_review_triggered") is True:
        return "manual_review_triggered", str(trigger_report.get("trigger_reason"))
    return "continue_watch_only", "watch_logger_ready_without_manual_review_trigger"


def _nearest_candidate(rows: Sequence[Mapping[str, Any]], *, target_rank: int) -> dict[str, Any] | None:
    if not rows:
        return None
    ordered = sorted(
        rows,
        key=lambda row: (
            max(0, int(row.get("shadow_adjusted_rank") or 999) - target_rank),
            abs(int(row.get("shadow_adjusted_rank") or 999) - target_rank),
            int(row.get("active_rank") or 999),
            str(row.get("symbol")),
        ),
    )
    candidate = dict(ordered[0])
    candidate["distance_to_target_shadow_rank"] = int(candidate.get("shadow_adjusted_rank") or 999) - target_rank
    return _compact(candidate)


def _candidate_sort(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        _compact(row)
        for row in sorted(
            rows,
            key=lambda row: (
                int(row.get("shadow_adjusted_rank") or 999),
                int(row.get("active_rank") or 999),
                str(row.get("symbol")),
            ),
        )
    ]


def _materialization_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "row_count": len(rows),
        "teppan_pattern_match_count": sum(1 for row in rows if row.get("teppan_pattern_match") is True),
        "teppan_guard_pass_count": sum(1 for row in rows if row.get("teppan_guard_pass") is True),
        "loss_guard_blocked_count": sum(1 for row in rows if row.get("loss_guard_blocked") is True),
    }


def _artifact_complete(output_root: Path, result: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "teppan_shadow_watch_logger_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": result.get("decision"),
        "logger_status": result.get("logger_status"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ValueError(f"missing_required_watch_plan_artifact:{path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"watch_plan_artifact_not_object:{path}")
    return data


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


if __name__ == "__main__":
    raise SystemExit(main())
