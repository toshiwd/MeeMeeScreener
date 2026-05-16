from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_v1 as base
from scripts import tradex_point_in_time_candidate_pool_phase2c2_v1 as phase2c2
from scripts import tradex_side_aware_min_pool_feasibility_v1 as min_pool

SCRIPT_NAME = "tradex_point_in_time_candidate_pool_phase2c3_v1"
SCHEMA_VERSION = "tradex_point_in_time_candidate_pool_phase2c3_v1"
DEFAULT_PHASE2C_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1_phase2c\20260509T064219Z-989246")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1_phase2c3")
CONTRACT_PATH = REPO_ROOT / "docs" / "contracts" / "tradex_point_in_time_candidate_pool_contract_v1.json"
GENERATION_PATH = "scripts/tradex_side_aware_min_pool_feasibility_v1.py::build_artifacts"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "lineage_source_discovery.json",
    "instrumentation_change_summary.json",
    "full_candidate_pool_rows_context_lineage.parquet",
    "full_pool_no_lookahead_coverage.json",
    "point_in_time_contract_validation.json",
    "phase2c3_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    base._write_parquet(path, frame)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _load_contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _key(frame: pd.DataFrame) -> pd.Series:
    return frame["anchor_date"].astype(str) + "|" + frame["side"].astype(str) + "|" + frame["symbol"].astype(str)


def _preservation_against_phase2c(reference: pd.DataFrame, current: pd.DataFrame) -> dict[str, Any]:
    ref = reference.copy()
    cur = current.copy()
    ref["__cmp_key__"] = _key(ref)
    cur["__cmp_key__"] = _key(cur)
    merged = ref.merge(cur, on="__cmp_key__", how="outer", suffixes=("_ref", "_cur"), indicator=True)
    changed_counts: dict[str, int] = {}
    for column in phase2c2.PRESERVE_COLUMNS:
        left = f"{column}_ref"
        right = f"{column}_cur"
        if left in merged.columns and right in merged.columns:
            changed_counts[column] = int((merged[left].fillna("__NA__").astype(str) != merged[right].fillna("__NA__").astype(str)).sum())
    return {
        "reference_phase2c_row_count": int(len(reference)),
        "current_row_count": int(len(current)),
        "row_count_preserved": int(len(reference)) == int(len(current)),
        "key_added_count": int((merged["_merge"] == "right_only").sum()),
        "key_removed_count": int((merged["_merge"] == "left_only").sum()),
        "preserved_column_changed_counts": changed_counts,
        "candidate_membership_changed": bool(changed_counts.get("candidate_pool_membership", 0) != 0),
        "champion_score_changed": bool(changed_counts.get("champion_score", 0) != 0),
        "champion_rank_changed": bool(changed_counts.get("champion_rank", 0) != 0),
        "topk_flags_changed": bool(any(changed_counts.get(c, 0) != 0 for c in ["top5_membership", "top10_membership", "top20_membership", "top50_membership"])),
    }


def _coverage(rows: pd.DataFrame, preservation: dict[str, Any]) -> dict[str, Any]:
    valid = rows["context_no_lookahead_valid"].fillna(False).astype(bool)
    invalid = rows["context_no_lookahead_status"].astype(str).eq("invalid")
    unverifiable = rows["context_no_lookahead_status"].astype(str).eq("unverifiable")
    return {
        "schema_version": f"{SCHEMA_VERSION}_coverage_v1",
        "generated_at_utc": _utc_now(),
        "total_rows": int(len(rows)),
        "daily_valid_count": int(rows["daily_no_lookahead_valid"].fillna(False).astype(bool).sum()),
        "weekly_valid_count": int(rows["weekly_no_lookahead_valid"].fillna(False).astype(bool).sum()),
        "monthly_valid_count": int(rows["monthly_no_lookahead_valid"].fillna(False).astype(bool).sum()),
        "complete_context_source_rows": int(
            (
                rows["daily_context_source_date"].notna()
                & rows["weekly_context_source_date"].notna()
                & rows["monthly_context_source_date"].notna()
            ).sum()
        ),
        "verified_no_lookahead_rows": int(valid.sum()),
        "unverifiable_rows": int(unverifiable.sum()),
        "invalid_rows": int(invalid.sum()),
        "invalid_reason_counts": rows.loc[invalid, "context_no_lookahead_failure_reason"].fillna("none").value_counts().to_dict(),
        "unverifiable_reason_counts": rows.loc[unverifiable, "context_no_lookahead_failure_reason"].fillna("none").value_counts().to_dict(),
        "full_no_lookahead_verified": bool(len(rows) > 0 and valid.all()),
        "preservation_audit": preservation,
    }


def _validate_contract(rows: pd.DataFrame, coverage: dict[str, Any]) -> dict[str, Any]:
    validation = phase2c2._validate_contract(rows, coverage)
    validation["schema_version"] = f"{SCHEMA_VERSION}_contract_validation_v1"
    return validation


def run_phase2c3(*, phase2c_root: Path, output_root: Path) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    contract = _load_contract()
    reference_path = phase2c_root / "full_candidate_pool_rows.parquet"
    reference = pd.read_parquet(reference_path)
    payload = min_pool.build_artifacts()
    rows = payload["selected_pool"].copy()
    preservation = _preservation_against_phase2c(reference, rows)
    coverage = _coverage(rows, preservation)
    validation = _validate_contract(rows, coverage)

    if (
        validation["validation_pass"]
        and coverage["full_no_lookahead_verified"]
        and preservation["row_count_preserved"]
        and preservation["key_added_count"] == 0
        and preservation["key_removed_count"] == 0
        and not preservation["candidate_membership_changed"]
        and not preservation["champion_score_changed"]
        and not preservation["champion_rank_changed"]
        and not preservation["topk_flags_changed"]
    ):
        decision = "ready"
        reason = "full_selected_pool_has_complete_daily_weekly_monthly_lineage"
    elif preservation["candidate_membership_changed"] or preservation["champion_score_changed"] or preservation["champion_rank_changed"]:
        decision = "blocked_requires_scoring_change"
        reason = "instrumentation_changed_candidate_membership_or_score_rank"
    elif coverage["invalid_rows"] > 0:
        decision = "blocked_future_label_risk"
        reason = "future_context_source_date_detected"
    elif coverage["unverifiable_rows"] > 0:
        decision = "blocked_unverifiable_source_dates"
        reason = "weekly_or_monthly_source_dates_still_unverifiable"
    elif not validation["validation_pass"]:
        decision = "reconstructable_with_contract_gap"
        reason = "lineage_improved_but_contract_validation_has_gap"
    else:
        decision = "blocked_generation_path_not_identified"
        reason = "unexpected_generation_path_gap"

    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "authoritative_decision": decision,
        "decision_reason": reason,
        "selected_instrumented_path": GENERATION_PATH,
        "total_rows": coverage["total_rows"],
        "daily_valid_count": coverage["daily_valid_count"],
        "weekly_valid_count": coverage["weekly_valid_count"],
        "monthly_valid_count": coverage["monthly_valid_count"],
        "complete_context_source_rows": coverage["complete_context_source_rows"],
        "verified_no_lookahead_rows": coverage["verified_no_lookahead_rows"],
        "unverifiable_rows": coverage["unverifiable_rows"],
        "invalid_rows": coverage["invalid_rows"],
        "full_no_lookahead_verified": coverage["full_no_lookahead_verified"],
        "contract_validation_pass": validation["validation_pass"],
        "future_phase2d_ranking_exposure_retest_allowed": decision == "ready",
        "scoring_behavior_changed": False,
        "candidate_membership_changed": preservation["candidate_membership_changed"],
        "champion_score_changed": preservation["champion_score_changed"],
        "champion_rank_changed": preservation["champion_rank_changed"],
        "topk_flags_changed": preservation["topk_flags_changed"],
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "ranking_challenger_created": False,
        "ranking_pretest_ran": False,
    }

    artifacts = {
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
            "generated_at_utc": _utc_now(),
            "script_name": SCRIPT_NAME,
            "session_root": str(session_root),
            "boundary": "TRADEX-only",
            "meemee_changed": False,
            "production_ranking_changed": False,
            "publish_changed": False,
            "ranking_challenger_created": False,
            "ranking_pretest_ran": False,
        },
        "input_resolution.json": {
            "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
            "generated_at_utc": _utc_now(),
            "phase2c_reference_path": str(reference_path),
            "phase2c_reference_exists": reference_path.exists(),
            "context_shape_source": str(min_pool.CONTEXT_SHAPE_SESSION / "conditional_shape_rows.parquet"),
            "selected_generation_path": GENERATION_PATH,
            "source_resolution": payload["generation_summary"].get("source_resolution"),
        },
        "lineage_source_discovery.json": {
            "schema_version": f"{SCHEMA_VERSION}_lineage_source_discovery_v1",
            "generated_at_utc": _utc_now(),
            "earliest_safe_source_identified": True,
            "source_function_script": "scripts/tradex_side_aware_min_pool_feasibility_v1.py::_load_shape_context_lineage",
            "source_artifact": str(min_pool.CONTEXT_SHAPE_SESSION / "conditional_shape_rows.parquet"),
            "input_columns": [
                "code",
                "trade_date",
                "monthly_context_date",
                "monthly_context_source",
                "monthly_context_no_lookahead",
                "weekly_context_date",
                "weekly_context_source",
                "weekly_context_no_lookahead",
            ],
            "join_keys": ["anchor_date/trade_date", "symbol/code"],
            "available_before_scoring": True,
            "source_dates_confirm_completed_bars": True,
            "adding_lineage_changes_scoring_behavior": False,
        },
        "instrumentation_change_summary.json": {
            "schema_version": f"{SCHEMA_VERSION}_instrumentation_summary_v1",
            "generated_at_utc": _utc_now(),
            "modified_generation_path": GENERATION_PATH,
            "lineage_fields_added": [
                "daily_context_source_date",
                "weekly_context_source_date",
                "monthly_context_source_date",
                "daily_feature_cutoff_date",
                "weekly_feature_cutoff_date",
                "monthly_feature_cutoff_date",
                "daily_no_lookahead_valid",
                "weekly_no_lookahead_valid",
                "monthly_no_lookahead_valid",
                "context_no_lookahead_valid",
                "context_no_lookahead_status",
                "context_no_lookahead_failure_reason",
                "context_lineage_source",
            ],
            "score_rank_candidate_membership_preserved": not (
                preservation["candidate_membership_changed"]
                or preservation["champion_score_changed"]
                or preservation["champion_rank_changed"]
                or preservation["topk_flags_changed"]
            ),
            "preservation_audit": preservation,
        },
        "full_pool_no_lookahead_coverage.json": coverage,
        "point_in_time_contract_validation.json": validation,
        "phase2c3_decision.json": decision_payload,
    }
    for name, body in artifacts.items():
        _write_json(session_root / name, body)
    _write_json(session_root / "point_in_time_candidate_pool_contract_snapshot.json", contract)
    _write_parquet(session_root / "full_candidate_pool_rows_context_lineage.parquet", rows)
    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "session_root": str(session_root),
        "decision": decision,
        "total_rows": coverage["total_rows"],
        "full_no_lookahead_verified": coverage["full_no_lookahead_verified"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Phase 2c3 full-pool context lineage instrumentation")
    parser.add_argument("--phase2c-root", default=str(DEFAULT_PHASE2C_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run_phase2c3(
        phase2c_root=_safe_path(args.phase2c_root, DEFAULT_PHASE2C_ROOT),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
    )
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
