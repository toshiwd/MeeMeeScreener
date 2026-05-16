"""Generate artifacts for the live-safe teppan materialization fix."""

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

from app.backend.services.codex_bridge_service import get_runtime_stock_db_status
from app.backend.services.teppan_live_safe_materialization import (
    FUTURE_LABEL_COLUMNS,
    build_input_dependency_audit,
    independent_exact_match_rows,
    load_recent_runtime_ranking_rows,
    load_teppan_candidates,
    materialize_teppan_features,
    materialize_teppan_features_from_anchors,
)
from app.backend.services.teppan_shadow_integration_adapter import DEFAULT_PLAN_ROOT


AXIS_ID = "teppan_runtime_feature_materialization_fix_v1"
DEFAULT_RUN_ID = "20260514T090000Z-teppan-runtime-feature-materialization-fix-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\runtime_feature_materialization_fixes\teppan_runtime_feature_materialization_fix_v1")
DEFAULT_PATTERN_ROOT = Path(
    r"G:\Tradex\teppan_chart_pattern_discovery_v1"
    r"\20260514T000000Z-current-runtime-teppan-discovery-v1-teppan_chart_pattern_discovery_v1"
)
TOP_KS = (20, 50, 100)
REQUIRED_OUTPUTS = [
    "live_safe_materialization_contract.json",
    "feature_input_dependency_audit.json",
    "future_label_independence_audit.json",
    "runtime_materialized_teppan_rows.jsonl",
    "materialization_parity_report.json",
    "runtime_topk_teppan_coverage_after_fix.json",
    "no_mutation_audit.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern-root", type=Path, default=DEFAULT_PATTERN_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--direction", default="up")
    parser.add_argument("--recent-dates", type=int, default=10)
    parser.add_argument("--rank-limit", type=int, default=100)
    args = parser.parse_args()
    run_teppan_runtime_feature_materialization_fix_v1(
        pattern_root=args.pattern_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
        direction=args.direction,
        recent_dates=args.recent_dates,
        rank_limit=args.rank_limit,
    )
    return 0


def run_teppan_runtime_feature_materialization_fix_v1(
    *,
    pattern_root: Path = DEFAULT_PATTERN_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
    direction: str = "up",
    recent_dates: int = 10,
    rank_limit: int = 100,
    runtime_status: Mapping[str, Any] | None = None,
    active_rows: Sequence[Mapping[str, Any]] | None = None,
    anchor_rows: Sequence[Mapping[str, Any]] | None = None,
    candidates: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    effective_runtime_status = dict(runtime_status or get_runtime_stock_db_status())
    db_path = Path(str(effective_runtime_status.get("selected_runtime_db_path") or ""))
    db_stat_before = _file_stat(db_path)
    effective_candidates = [dict(row) for row in (candidates or load_teppan_candidates(pattern_root))]
    effective_active_rows = list(active_rows) if active_rows is not None else load_recent_runtime_ranking_rows(
        db_path,
        direction=direction,
        recent_dates=recent_dates,
        rank_limit=rank_limit,
    )
    if anchor_rows is not None:
        materialized = materialize_teppan_features_from_anchors(effective_active_rows, anchor_rows, effective_candidates)
    else:
        materialized = materialize_teppan_features(effective_active_rows, db_path=db_path, pattern_root=pattern_root)
    rows = [_compact(row) for row in materialized["rows"]]
    dependency_audit = materialized["input_dependency_audit"]
    future_label_audit = _future_label_independence_audit(rows, dependency_audit)
    parity = _parity_report(rows, independent_exact_match_rows(rows, effective_candidates))
    coverage = _topk_coverage(rows)
    db_stat_after = _file_stat(db_path)
    no_mutation = _no_mutation_audit(db_path=db_path, before=db_stat_before, after=db_stat_after)
    contract = _contract(pattern_root, direction, recent_dates, rank_limit)
    decision = _decision(dependency_audit, future_label_audit, parity, coverage, no_mutation)
    research_decision = {
        "schema_version": "teppan_runtime_feature_materialization_fix_research_decision_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_reason": _decision_reason(decision),
        "runtime_top100_teppan_pattern_match_count": coverage["by_topk"]["top100"]["teppan_pattern_match_count"],
        "runtime_top100_teppan_guard_pass_count": coverage["by_topk"]["top100"]["teppan_guard_pass_count"],
        "future_labels_used": future_label_audit["future_labels_used"],
        "parity_pass": parity["parity_pass"],
        "no_mutation_pass": no_mutation["no_mutation_pass"],
        "production_publish_registered": False,
        "active_rank_changed": False,
        "display_score_changed": False,
        "runtime_duckdb_written": False,
    }

    _write_json(output_root / "live_safe_materialization_contract.json", contract)
    _write_json(output_root / "feature_input_dependency_audit.json", dependency_audit)
    _write_json(output_root / "future_label_independence_audit.json", future_label_audit)
    _write_jsonl(output_root / "runtime_materialized_teppan_rows.jsonl", rows)
    _write_json(output_root / "materialization_parity_report.json", parity)
    _write_json(output_root / "runtime_topk_teppan_coverage_after_fix.json", coverage)
    _write_json(output_root / "no_mutation_audit.json", no_mutation)
    _write_json(output_root / "research_decision.json", research_decision)
    complete = _artifact_complete(output_root, research_decision)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_root": str(output_root),
        "research_decision": research_decision,
        "coverage": coverage,
        "parity": parity,
        "artifact_complete": complete,
    }


def _contract(pattern_root: Path, direction: str, recent_dates: int, rank_limit: int) -> dict[str, Any]:
    return {
        "schema_version": "teppan_live_safe_materialization_contract_v1",
        "axis_id": AXIS_ID,
        "pattern_root": str(pattern_root),
        "plan_root": str(DEFAULT_PLAN_ROOT),
        "runtime_scope": {
            "direction": direction,
            "recent_dates": recent_dates,
            "rank_limit": rank_limit,
        },
        "materialized_features": ["teppan_pattern_match", "teppan_guard_pass", "loss_guard_pass", "loss_guard_blocked"],
        "allowed_inputs": "decision_time_ohlc_ma_volume_and_runtime_ranking_fields",
        "forbidden_inputs": sorted(FUTURE_LABEL_COLUMNS),
        "historical_evaluation_filter_used": False,
        "runtime_duckdb_write_allowed": False,
        "production_publish_registration_allowed": False,
        "active_runtime_ranking_change_allowed": False,
    }


def _future_label_independence_audit(rows: Sequence[Mapping[str, Any]], dependency_audit: Mapping[str, Any]) -> dict[str, Any]:
    row_future_flags = [row for row in rows if row.get("future_label_inputs_used") is True]
    return {
        "schema_version": "teppan_future_label_independence_audit_v1",
        "forbidden_future_label_inputs": sorted(FUTURE_LABEL_COLUMNS),
        "dependency_future_label_overlap": dependency_audit.get("future_label_overlap") or [],
        "row_future_label_flag_count": len(row_future_flags),
        "future_labels_used": bool(dependency_audit.get("future_labels_used") or row_future_flags),
        "historical_evaluation_filter_used": bool(dependency_audit.get("historical_evaluation_filter_used")),
        "pass": not bool(dependency_audit.get("future_labels_used") or row_future_flags or dependency_audit.get("historical_evaluation_filter_used")),
    }


def _parity_report(rows: Sequence[Mapping[str, Any]], independent_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    independent_index = {(str(row["symbol"]), str(row["anchor_date"])): row for row in independent_rows}
    mismatches = []
    for row in rows:
        key = (str(row["symbol"]), str(row["anchor_date"]))
        independent = independent_index.get(key, {})
        if bool(row.get("teppan_pattern_match")) != bool(independent.get("independent_teppan_pattern_match")):
            mismatches.append(
                {
                    "symbol": row.get("symbol"),
                    "anchor_date": row.get("anchor_date"),
                    "materialized": row.get("teppan_pattern_match"),
                    "independent": independent.get("independent_teppan_pattern_match"),
                }
            )
    return {
        "schema_version": "teppan_materialization_parity_report_v1",
        "row_count": len(rows),
        "independent_row_count": len(independent_rows),
        "mismatch_count": len(mismatches),
        "mismatch_examples": mismatches[:20],
        "parity_pass": len(mismatches) == 0 and len(rows) == len(independent_rows),
    }


def _topk_coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_topk = {}
    by_date = {}
    for k in TOP_KS:
        by_topk[f"top{k}"] = _coverage_bucket([row for row in rows if int(row.get("active_rank") or 9999) <= k])
    for date in sorted({str(row.get("anchor_date")) for row in rows}):
        date_rows = [row for row in rows if str(row.get("anchor_date")) == date]
        by_date[date] = {f"top{k}": _coverage_bucket([row for row in date_rows if int(row.get("active_rank") or 9999) <= k]) for k in TOP_KS}
    return {
        "schema_version": "teppan_runtime_topk_coverage_after_fix_v1",
        "by_topk": by_topk,
        "by_date": by_date,
    }


def _coverage_bucket(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    match = sum(1 for row in rows if row.get("teppan_pattern_match") is True)
    guard = sum(1 for row in rows if row.get("teppan_guard_pass") is True)
    blocked = sum(1 for row in rows if row.get("loss_guard_blocked") is True)
    return {
        "row_count": total,
        "teppan_pattern_match_count": match,
        "teppan_pattern_match_rate": _rate(match, total),
        "teppan_guard_pass_count": guard,
        "teppan_guard_pass_rate": _rate(guard, total),
        "loss_guard_blocked_count": blocked,
        "loss_guard_blocked_rate": _rate(blocked, total),
    }


def _decision(
    dependency_audit: Mapping[str, Any],
    future_label_audit: Mapping[str, Any],
    parity: Mapping[str, Any],
    coverage: Mapping[str, Any],
    no_mutation: Mapping[str, Any],
) -> str:
    if not no_mutation.get("no_mutation_pass"):
        return "materialization_fix_failed"
    if dependency_audit.get("missing_required_runtime_fields"):
        return "hold_for_runtime_field_gap"
    if not future_label_audit.get("pass"):
        return "materialization_fix_failed"
    if not parity.get("parity_pass"):
        return "hold_for_parity_gap"
    if int((coverage["by_topk"]["top100"]).get("teppan_pattern_match_count") or 0) <= 0:
        return "materialization_fix_failed"
    return "live_safe_materialization_ready"


def _decision_reason(decision: str) -> str:
    return {
        "live_safe_materialization_ready": "future_label_free_materialization_matches_independent_matcher_and_restores_top100_coverage",
        "hold_for_runtime_field_gap": "runtime_fields_missing_for_live_safe_materialization",
        "hold_for_parity_gap": "materialized_rows_do_not_match_independent_matcher",
        "materialization_fix_failed": "materialization_fix_contract_or_coverage_failed",
    }[decision]


def _no_mutation_audit(*, db_path: Path, before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    unchanged = before == after
    return {
        "schema_version": "teppan_runtime_feature_materialization_no_mutation_audit_v1",
        "runtime_duckdb_path": str(db_path),
        "runtime_duckdb_stat_before": dict(before),
        "runtime_duckdb_stat_after": dict(after),
        "runtime_duckdb_unchanged": unchanged,
        "runtime_duckdb_written": not unchanged,
        "active_rank_changed": False,
        "display_score_changed": False,
        "production_publish_registered": False,
        "frontend_changed": False,
        "backend_api_response_changed": False,
        "no_mutation_pass": unchanged,
        "silent_fallback_used": False,
    }


def _artifact_complete(output_root: Path, research_decision: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "teppan_runtime_feature_materialization_fix_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": research_decision.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _file_stat(path: Path) -> dict[str, Any]:
    if not path or not str(path):
        return {"exists": False}
    if not path.exists():
        return {"exists": False, "path": str(path)}
    stat = path.stat()
    return {"exists": True, "path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


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


def _compact(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _json_ready(value) for key, value in row.items() if value is not None}


def _rate(num: int, denom: int) -> float | None:
    return None if denom <= 0 else float(num) / float(denom)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {**dict(payload), "generated_at_utc": dict(payload).get("generated_at_utc") or _utc_now()}
    path.write_text(json.dumps(_json_ready(data), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(_json_ready(dict(row)), ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
