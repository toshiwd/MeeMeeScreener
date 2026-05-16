"""Read-only teppan shadow coverage observation after live-safe materialization."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from app.backend.services.teppan_live_safe_materialization import (
    materialize_teppan_features,
    build_input_dependency_audit,
    load_recent_runtime_ranking_rows,
)
from app.backend.services.teppan_shadow_integration_adapter import (
    DEFAULT_PLAN_ROOT,
    compute_teppan_shadow_adjusted_ranking,
    load_teppan_shadow_plan,
)


AXIS_ID = "teppan_shadow_runtime_coverage_observation_v2"
DEFAULT_RUN_ID = "20260514T100000Z-teppan-shadow-runtime-coverage-observation-v2"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\shadow_runtime_coverage_observations\teppan_shadow_runtime_coverage_observation_v2")
DEFAULT_PATTERN_ROOT = Path(
    r"G:\Tradex\teppan_chart_pattern_discovery_v1"
    r"\20260514T000000Z-current-runtime-teppan-discovery-v1-teppan_chart_pattern_discovery_v1"
)
MATERIALIZATION_FIX_ROOT = Path(
    r"G:\Tradex\runtime_feature_materialization_fixes\teppan_runtime_feature_materialization_fix_v1"
    r"\20260514T090000Z-teppan-runtime-feature-materialization-fix-v1"
)
TOP_KS = (5, 10, 20)
REQUIRED_OUTPUTS = [
    "coverage_observation_v2_result.json",
    "active_shadow_topk.json",
    "topk_comparison.json",
    "coverage_summary.json",
    "candidate_rows.json",
    "human_review_candidate_list.json",
    "materialization_readback.json",
    "no_mutation_audit.json",
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
    run_teppan_shadow_runtime_coverage_observation_v2(
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


def run_teppan_shadow_runtime_coverage_observation_v2(
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
    source_rows = _use_runtime_rank_as_active_rank(source_rows)
    materialization = (
        {"rows": list(materialized_rows), "summary": _materialization_summary(materialized_rows or []), "input_dependency_audit": build_input_dependency_audit()}
        if materialized_rows is not None
        else materialize_teppan_features(source_rows, db_path=db_path, pattern_root=pattern_root)
    )
    feature_rows = list(materialization["rows"])
    baseline_features = _baseline_no_boost_features(source_rows)
    baseline_shadow_result = compute_teppan_shadow_adjusted_ranking(source_rows, baseline_features, plan)
    baseline_rank_lookup = _baseline_shadow_rank_lookup(baseline_shadow_result["shadow_rows"])
    shadow_result = compute_teppan_shadow_adjusted_ranking(source_rows, feature_rows, plan)
    candidate_rows = _enrich_candidate_rows(shadow_result["shadow_rows"], source_rows, feature_rows, baseline_rank_lookup)
    latest_date = max(str(row["anchor_date"]) for row in candidate_rows)
    latest_rows = [row for row in candidate_rows if str(row["anchor_date"]) == latest_date]

    topk = _topk_payload(latest_rows)
    topk_comparison = _topk_comparison(topk["active"], topk["shadow"])
    coverage_summary = _coverage_summary(candidate_rows, latest_date=latest_date)
    human_review = _human_review_candidate_list(latest_rows, topk_comparison)
    materialization_readback = _materialization_readback(materialization_fix_root, materialization, candidate_rows)
    changed_rank_count = sum(1 for row in candidate_rows if row.get("shadow_rank_changed_by_teppan_boost") is True)

    db_stat_after = _file_stat(db_path)
    no_mutation = _no_mutation_audit(
        db_path=db_path,
        db_stat_before=db_stat_before,
        db_stat_after=db_stat_after,
        adapter_audit=shadow_result["audit"],
    )
    decision, decision_reason = _decision(
        topk_comparison=topk_comparison,
        coverage_summary=coverage_summary,
        human_review=human_review,
        no_mutation=no_mutation,
    )
    result = {
        "schema_version": "teppan_shadow_runtime_coverage_observation_v2_result_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": decision_reason,
        "integration_mode": "read_only_shadow_coverage_observation_after_live_safe_materialization_fix",
        "generated_at_utc": _utc_now(),
        "source_surface": "runtime_duckdb.ranking_appearance_daily",
        "latest_runtime_date": latest_date,
        "recent_dates": int(recent_dates),
        "rank_limit": int(rank_limit),
        "plan_root": str(plan.plan_root),
        "pattern_root": str(pattern_root),
        "materialization_fix_root": str(materialization_fix_root),
        "runtime_stock_db_status": effective_runtime_status,
        "rankings_freshness": effective_rankings_freshness,
        "active_top5": topk["active"]["top5"],
        "active_top10": topk["active"]["top10"],
        "active_top20": topk["active"]["top20"],
        "shadow_top5": topk["shadow"]["top5"],
        "shadow_top10": topk["shadow"]["top10"],
        "shadow_top20": topk["shadow"]["top20"],
        "added_by_shadow_top5": topk_comparison["top5"]["added_by_shadow"],
        "added_by_shadow_top10": topk_comparison["top10"]["added_by_shadow"],
        "added_by_shadow_top20": topk_comparison["top20"]["added_by_shadow"],
        "removed_from_active_top5": topk_comparison["top5"]["removed_from_active"],
        "removed_from_active_top10": topk_comparison["top10"]["removed_from_active"],
        "removed_from_active_top20": topk_comparison["top20"]["removed_from_active"],
        "boosted_candidate_count": coverage_summary["all_observed"]["boosted_candidate_count"],
        "loss_guard_blocked_count": coverage_summary["all_observed"]["loss_guard_blocked_count"],
        "changed_rank_count_shadow_vs_active": changed_rank_count,
        "coverage_summary": coverage_summary,
        "topk_comparison": topk_comparison,
        "candidate_rows": candidate_rows,
        "human_review_candidate_list": human_review,
        "materialization_readback": materialization_readback,
        "no_mutation_audit": no_mutation,
        "not_changed": [
            "active_rank",
            "display_score",
            "runtime_duckdb",
            "production_publish_registry",
            "frontend_ui",
            "backend_api_response",
        ],
    }

    _write_json(output_root / "coverage_observation_v2_result.json", result)
    _write_json(output_root / "active_shadow_topk.json", {"active": topk["active"], "shadow": topk["shadow"]})
    _write_json(output_root / "topk_comparison.json", topk_comparison)
    _write_json(output_root / "coverage_summary.json", coverage_summary)
    _write_json(output_root / "candidate_rows.json", {"candidate_rows": candidate_rows})
    _write_json(output_root / "human_review_candidate_list.json", {"human_review_candidate_list": human_review, "max_user_selection": 3})
    _write_json(output_root / "materialization_readback.json", materialization_readback)
    _write_json(output_root / "no_mutation_audit.json", no_mutation)
    complete = _artifact_complete(output_root, result)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_root": str(output_root), "coverage_observation_v2_result": result, "artifact_complete": complete}


def _enrich_candidate_rows(
    shadow_rows: Sequence[Mapping[str, Any]],
    active_rows: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]],
    baseline_rank_lookup: Mapping[tuple[str, str, str], int],
) -> list[dict[str, Any]]:
    active_index = {(str(row["symbol"]), str(row["anchor_date"])): row for row in active_rows}
    feature_index = {(str(row["symbol"]), str(row["anchor_date"])): row for row in feature_rows}
    out = []
    for shadow in shadow_rows:
        key = (str(shadow["symbol"]), str(shadow["anchor_date"]))
        rank_key = (str(shadow["anchor_date"]), str(shadow["symbol"]), str(shadow["side"]))
        active = active_index.get(key, {})
        feature = feature_index.get(key, {})
        baseline_rank = baseline_rank_lookup.get(rank_key)
        out.append(
            _compact(
                {
                    "symbol": shadow.get("symbol"),
                    "name": active.get("name"),
                    "anchor_date": shadow.get("anchor_date"),
                    "side": shadow.get("side"),
                    "active_rank": shadow.get("active_rank"),
                    "runtime_rank": active.get("runtime_rank"),
                    "display_score": shadow.get("active_display_score"),
                    "original_rank": shadow.get("original_rank"),
                    "original_score": shadow.get("original_score"),
                    "shadow_adjusted_rank": shadow.get("shadow_adjusted_rank"),
                    "baseline_no_boost_shadow_rank": baseline_rank,
                    "shadow_rank_changed_by_teppan_boost": baseline_rank is not None
                    and int(baseline_rank) != int(shadow.get("shadow_adjusted_rank")),
                    "shadow_adjusted_score": shadow.get("shadow_adjusted_score"),
                    "teppan_guarded_boost_applied": shadow.get("teppan_guarded_boost_applied"),
                    "teppan_shadow_boost_value": shadow.get("teppan_shadow_boost_value"),
                    "teppan_pattern_match": shadow.get("teppan_pattern_match"),
                    "teppan_guard_pass": shadow.get("teppan_guard_pass"),
                    "loss_guard_blocked": feature.get("loss_guard_blocked"),
                    "loss_guard_pass": feature.get("loss_guard_pass"),
                    "shadow_decision_reason": shadow.get("shadow_decision_reason"),
                    "best_pattern_family": feature.get("best_pattern_family"),
                    "best_pattern_key": feature.get("best_pattern_key"),
                    "best_pattern_decision": feature.get("best_pattern_decision"),
                    "best_teppan_score": _optional_float(feature.get("best_teppan_score")),
                    "matched_pattern_count": int(feature.get("matched_pattern_count") or 0),
                    "guard_block_reason": feature.get("guard_block_reason"),
                    "signal_features": feature.get("signal_features"),
                    "future_label_inputs_used": feature.get("future_label_inputs_used"),
                    "signal_state": active.get("signal_state"),
                    "entry_qualified": active.get("entry_qualified"),
                    "setup_type": active.get("setup_type"),
                    "status": active.get("status"),
                }
            )
        )
    ordered = sorted(out, key=lambda row: (str(row["anchor_date"]), str(row["side"]), int(row["active_rank"]), str(row["symbol"])))
    current_group: tuple[str, str] | None = None
    position = 0
    for row in ordered:
        group = (str(row["anchor_date"]), str(row["side"]))
        if group != current_group:
            current_group = group
            position = 0
        position += 1
        row["active_position_rank"] = position
    return ordered


def _use_runtime_rank_as_active_rank(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        item = dict(row)
        if item.get("runtime_rank") is not None:
            item["champion_rank"] = int(item["runtime_rank"])
        normalized.append(item)
    return normalized


def _baseline_no_boost_features(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        out.append(
            {
                "anchor_date": row.get("anchor_date"),
                "symbol": row.get("symbol"),
                "side": row.get("side", "long"),
                "teppan_pattern_match": False,
                "teppan_guard_pass": False,
            }
        )
    return out


def _baseline_shadow_rank_lookup(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str, str], int]:
    return {
        (str(row.get("anchor_date")), str(row.get("symbol")), str(row.get("side"))): int(row["shadow_adjusted_rank"])
        for row in rows
    }


def _topk_payload(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    return {
        "active": {f"top{k}": _topk(rows, "active_rank", k) for k in TOP_KS},
        "shadow": {f"top{k}": _topk(rows, "shadow_adjusted_rank", k) for k in TOP_KS},
    }


def _topk(rows: Sequence[Mapping[str, Any]], rank_field: str, k: int) -> list[dict[str, Any]]:
    return [_compact(row) for row in sorted(rows, key=lambda row: (int(row[rank_field]), str(row["symbol"])))[:k]]


def _topk_comparison(
    active_topk: Mapping[str, Sequence[Mapping[str, Any]]],
    shadow_topk: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": "teppan_shadow_runtime_coverage_v2_topk_comparison_v1"}
    for key, active_rows in active_topk.items():
        shadow_rows = shadow_topk[key]
        active_symbols = {str(row["symbol"]) for row in active_rows}
        shadow_symbols = {str(row["symbol"]) for row in shadow_rows}
        out[key] = {
            "active_symbols": [row["symbol"] for row in active_rows],
            "shadow_symbols": [row["symbol"] for row in shadow_rows],
            "added_by_shadow": [row for row in shadow_rows if str(row["symbol"]) not in active_symbols],
            "removed_from_active": [row for row in active_rows if str(row["symbol"]) not in shadow_symbols],
            "changed_member_count": len(active_symbols ^ shadow_symbols),
        }
    return out


def _coverage_summary(rows: Sequence[Mapping[str, Any]], *, latest_date: str) -> dict[str, Any]:
    latest_rows = [row for row in rows if str(row["anchor_date"]) == latest_date]
    summary: dict[str, Any] = {
        "schema_version": "teppan_shadow_runtime_coverage_v2_summary_v1",
        "latest_runtime_date": latest_date,
        "latest": {},
        "recent": {},
    }
    for k in TOP_KS:
        summary["latest"][f"top{k}"] = _coverage_bucket([row for row in latest_rows if int(row["active_rank"]) <= k])
        summary["recent"][f"top{k}"] = _coverage_bucket([row for row in rows if int(row["active_rank"]) <= k])
    summary["latest"]["all_observed"] = _coverage_bucket(latest_rows)
    summary["recent"]["all_observed"] = _coverage_bucket(rows)
    summary["all_observed"] = summary["recent"]["all_observed"]
    summary["latest_shadow_decision_reason_counts"] = dict(Counter(str(row.get("shadow_decision_reason")) for row in latest_rows))
    summary["recent_shadow_decision_reason_counts"] = dict(Counter(str(row.get("shadow_decision_reason")) for row in rows))
    return summary


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
        "changed_rank_count_shadow_vs_active": changed_rank,
    }


def _human_review_candidate_list(
    latest_rows: Sequence[Mapping[str, Any]],
    topk_comparison: Mapping[str, Any],
) -> list[dict[str, Any]]:
    picked: dict[tuple[str, str], dict[str, Any]] = {}
    for topk in ("top5", "top10", "top20"):
        for row in topk_comparison[topk]["added_by_shadow"]:
            candidate = dict(row)
            candidate["human_review_reason"] = f"added_by_shadow_{topk}"
            picked[(str(candidate["anchor_date"]), str(candidate["symbol"]))] = candidate
    for row in latest_rows:
        if row.get("teppan_guarded_boost_applied") is True:
            candidate = dict(row)
            candidate["human_review_reason"] = "boosted_teppan_guard_pass"
            picked.setdefault((str(candidate["anchor_date"]), str(candidate["symbol"])), candidate)
    ordered = sorted(
        picked.values(),
        key=lambda row: (
            0 if str(row.get("human_review_reason", "")).startswith("added_by_shadow") else 1,
            int(row.get("shadow_adjusted_rank") or 999),
            int(row.get("active_rank") or 999),
            str(row.get("symbol")),
        ),
    )
    return [_compact(row) for row in ordered[:3]]


def _materialization_readback(
    materialization_fix_root: Path,
    materialization: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    decision_path = Path(materialization_fix_root) / "research_decision.json"
    decision_payload = _read_json(decision_path) if decision_path.exists() else {}
    dependency = materialization.get("input_dependency_audit") or {}
    future_label_rows = sum(1 for row in candidate_rows if row.get("future_label_inputs_used") is True)
    return {
        "schema_version": "teppan_shadow_runtime_coverage_v2_materialization_readback_v1",
        "materialization_fix_root": str(materialization_fix_root),
        "materialization_fix_decision": decision_payload.get("decision"),
        "materialization_summary": materialization.get("summary"),
        "future_labels_used": bool(dependency.get("future_labels_used")) or future_label_rows > 0,
        "historical_evaluation_filter_used": bool(dependency.get("historical_evaluation_filter_used")),
        "future_label_row_count": future_label_rows,
        "input_dependency_audit": dependency,
    }


def _materialization_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    match = sum(1 for row in rows if row.get("teppan_pattern_match") is True)
    guard = sum(1 for row in rows if row.get("teppan_guard_pass") is True)
    blocked = sum(1 for row in rows if row.get("loss_guard_blocked") is True)
    return {
        "row_count": count,
        "teppan_pattern_match_count": match,
        "teppan_guard_pass_count": guard,
        "loss_guard_blocked_count": blocked,
        "future_label_inputs_used": False,
    }


def _decision(
    *,
    topk_comparison: Mapping[str, Any],
    coverage_summary: Mapping[str, Any],
    human_review: Sequence[Mapping[str, Any]],
    no_mutation: Mapping[str, Any],
) -> tuple[str, str]:
    if no_mutation.get("no_mutation_pass") is not True:
        return "drop_shadow_live_value", "no_mutation_audit_failed"
    latest = coverage_summary["latest"]
    recent = coverage_summary["recent"]
    latest_topk_patterns = sum(int(latest[f"top{k}"]["teppan_pattern_match_count"]) for k in TOP_KS)
    recent_top100_patterns = int(recent["all_observed"]["teppan_pattern_match_count"])
    topk_changed = any(topk_comparison[f"top{k}"]["changed_member_count"] > 0 for k in TOP_KS)
    if latest_topk_patterns > 0 and topk_changed and human_review:
        return "shadow_coverage_v2_pass", "materialized_candidates_created_latest_topk_shadow_differences"
    if topk_changed and human_review:
        return "hold_for_shadow_candidate_review", "shadow_created_differences_requiring_human_review"
    if recent_top100_patterns > 0:
        return "hold_for_sparse_live_coverage", "recent_top100_coverage_exists_but_latest_topk_differences_are_sparse"
    return "drop_shadow_live_value", "no_recent_top100_teppan_candidate_value_after_materialization_fix"


def _no_mutation_audit(
    *,
    db_path: Path,
    db_stat_before: Mapping[str, Any],
    db_stat_after: Mapping[str, Any],
    adapter_audit: Mapping[str, Any],
) -> dict[str, Any]:
    unchanged = db_stat_before == db_stat_after
    return {
        "schema_version": "teppan_shadow_runtime_coverage_v2_no_mutation_audit_v1",
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


def _artifact_complete(output_root: Path, result: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "teppan_shadow_runtime_coverage_v2_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": result.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def _rate(num: int, denom: int) -> float | None:
    return None if denom <= 0 else float(num) / float(denom)


def _optional_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _file_stat(path: Path) -> dict[str, Any]:
    if not path or not str(path):
        return {"exists": False}
    if not path.exists():
        return {"exists": False, "path": str(path)}
    stat = path.stat()
    return {"exists": True, "path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


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
