from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCRIPT_NAME = "tradex_high_recall_candidate_pool_design_v1"
SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1"
MANIFEST_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_input_resolution_v1"
CONTRACT_INVENTORY_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_contract_inventory_v1"
OPTIONS_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_design_options_v1"
CONTRACT_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_contract_v1"
FEASIBILITY_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_feasibility_estimate_v1"
EVALUATION_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_evaluation_plan_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_high_recall_candidate_pool_design_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\high_recall_candidate_pool_design_v1")
CURRENT_BROAD_PREFILTER_SESSION = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1_larger\20260502T034011Z-d76e6794")
CURRENT_BROAD_TWO_STAGE_SESSION = Path(r"G:\Tradex\candidate_generation_two_stage_admission_context_shape_v1_larger\20260502T034025Z-86ae7451")
CURRENT_REPAIR_PREFILTER_SESSION = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260501T161322Z-494344\20260501T161323Z-b34a37c3")
RAW_CANDIDATE_SNAPSHOT_UNIVERSE = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_candidate_snapshots.json")
ACCUMULATED_SESSION = Path(r"G:\Tradex\shadow_reranker_accumulated_forward_validation_v1\20260502T082532Z-c17e19")
REDESIGN_AUDIT_SESSION = Path(r"G:\Tradex\candidate_generation_redesign_audit_v1\20260502T105632Z-399318")
LABEL_COVERAGE_AUDIT_SESSION = Path(r"G:\Tradex\candidate_generation_label_coverage_audit_v1\20260502T101135Z-344988")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _load_candidate_snapshot_universe(path: Path) -> pd.DataFrame:
    payload = _load_json(path)
    rows = payload.get("rows")
    if isinstance(rows, dict) and "rows" in rows:
        rows = rows["rows"]
    frame = pd.DataFrame(rows or [])
    if frame.empty:
        raise RuntimeError(f"no candidate rows found in {path}")
    for col in ("anchor_date", "side", "symbol", "month_bucket"):
        if col in frame.columns:
            frame[col] = frame[col].astype(str)
    return frame


def _group_size_distribution(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (anchor_date, side), group in frame.groupby(["anchor_date", "side"], sort=False):
        size = int(len(group))
        rows.append(
            {
                "anchor_date": str(anchor_date),
                "side": str(side),
                "month_bucket": str(group["month_bucket"].iloc[0]) if "month_bucket" in group.columns and len(group) else None,
                "group_size": size,
                "top5_available": bool(size >= 5),
                "top10_available": bool(size >= 10),
                "top20_available": bool(size >= 20),
                "too_thin_for_top5": int(size < 5),
                "too_thin_for_top10": int(size < 10),
                "too_thin_for_top20": int(size < 20),
            }
        )
    return pd.DataFrame(rows)


def _summary_by_side(frame: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for side, side_frame in frame.groupby("side", sort=False):
        group_sizes = side_frame.groupby("anchor_date", sort=False).size()
        summary[str(side)] = {
            "row_count": int(len(side_frame)),
            "group_count": int(group_sizes.shape[0]),
            "mean_group_size": float(group_sizes.mean()) if len(group_sizes) else None,
            "median_group_size": float(group_sizes.median()) if len(group_sizes) else None,
            "min_group_size": int(group_sizes.min()) if len(group_sizes) else None,
            "max_group_size": int(group_sizes.max()) if len(group_sizes) else None,
            "top5_thin_groups": int((group_sizes < 5).sum()),
            "top10_thin_groups": int((group_sizes < 10).sum()),
            "top20_thin_groups": int((group_sizes < 20).sum()),
        }
    overall_sizes = frame.groupby(["anchor_date", "side"], sort=False).size()
    summary["overall"] = {
        "row_count": int(len(frame)),
        "group_count": int(overall_sizes.shape[0]),
        "mean_group_size": float(overall_sizes.mean()) if len(overall_sizes) else None,
        "median_group_size": float(overall_sizes.median()) if len(overall_sizes) else None,
        "min_group_size": int(overall_sizes.min()) if len(overall_sizes) else None,
        "max_group_size": int(overall_sizes.max()) if len(overall_sizes) else None,
        "top5_thin_groups": int((overall_sizes < 5).sum()),
        "top10_thin_groups": int((overall_sizes < 10).sum()),
        "top20_thin_groups": int((overall_sizes < 20).sum()),
    }
    return summary


def _topk_thin_summary(summary: dict[str, Any]) -> dict[str, int]:
    overall = summary["overall"]
    return {
        "top5": int(overall["top5_thin_groups"]),
        "top10": int(overall["top10_thin_groups"]),
        "top20": int(overall["top20_thin_groups"]),
    }


def _build_current_candidate_generation_contract_inventory(
    *,
    broad_prefilter_session: Path,
    broad_two_stage_session: Path,
    repair_prefilter_session: Path,
    raw_candidate_universe: Path,
    accumulated_surface: Path,
    current_breadth_audit: Path,
    current_oracle_headroom: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    broad_prefilter_manifest = _load_json(broad_prefilter_session / "run_manifest.json")
    broad_prefilter_policy = _load_json(broad_prefilter_session / "candidate_prefilter_policy.json")
    broad_prefilter_coverage = _load_json(broad_prefilter_session / "candidate_prefilter_coverage_summary.json")
    broad_prefilter_decision = _load_json(broad_prefilter_session / "candidate_generation_pre_filter_context_shape_v1_decision.json")
    broad_prefilter_rows = _load_frame(broad_prefilter_session / "candidate_prefilter_rows.parquet")

    broad_two_stage_manifest = _load_json(broad_two_stage_session / "run_manifest.json")
    broad_two_stage_coverage = _load_json(broad_two_stage_session / "candidate_stage_coverage_summary.json")
    broad_two_stage_decision = _load_json(broad_two_stage_session / "candidate_generation_two_stage_admission_context_shape_v1_decision.json")
    broad_two_stage_comparison = _load_json(broad_two_stage_session / "candidate_pool_comparison.json")

    repair_manifest = _load_json(repair_prefilter_session / "run_manifest.json")
    repair_coverage = _load_json(repair_prefilter_session / "candidate_prefilter_coverage_summary.json")

    raw_universe_frame = _load_candidate_snapshot_universe(raw_candidate_universe)
    accumulated_frame = _load_frame(accumulated_surface)

    current_breadth = _load_json(current_breadth_audit)
    current_oracle = _load_json(current_oracle_headroom)

    broad_summary = _summary_by_side(broad_prefilter_rows)
    raw_summary = _summary_by_side(raw_universe_frame)
    accumulated_summary = _summary_by_side(accumulated_frame)

    inventory = {
        "schema_version": CONTRACT_INVENTORY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "current_source_roots": {
            "broad_prefilter_session": str(broad_prefilter_session),
            "broad_two_stage_session": str(broad_two_stage_session),
            "repair_prefilter_session": str(repair_prefilter_session),
            "raw_candidate_universe": str(raw_candidate_universe),
            "current_accumulated_surface": str(accumulated_surface),
            "breadth_audit_session": str(current_breadth_audit),
            "oracle_headroom_session": str(current_oracle_headroom),
        },
        "current_candidate_generation_contract": {
            "source_candidate_input_dir": broad_prefilter_manifest.get("source_candidate_input_dir"),
            "dedup_rule": broad_prefilter_manifest.get("same_condition_contract", {}).get("dedup_rule"),
            "ranking_groups": broad_prefilter_manifest.get("same_condition_contract", {}).get("ranking_groups", []),
            "ranking_sort": broad_prefilter_manifest.get("same_condition_contract", {}).get("ranking_sort", []),
            "top_k_values": broad_prefilter_manifest.get("same_condition_contract", {}).get("top_k_values", [5, 10, 20]),
            "current_thresholds": {
                "top15_score_threshold": broad_prefilter_policy.get("thresholds", {}).get("top15_score_threshold"),
                "bottom15_score_threshold": broad_prefilter_policy.get("thresholds", {}).get("bottom15_score_threshold"),
                "source_thresholds": broad_prefilter_policy.get("thresholds", {}).get("source_thresholds", {}),
            },
            "current_policy_rules": broad_prefilter_policy.get("policy_rules", {}),
            "current_diagnostic_definitions": broad_prefilter_policy.get("diagnostic_definitions", {}),
            "current_side_handling": {
                "group_grain": ["anchor_date", "side"],
                "side_specific_caps": None,
                "side_specific_min_pool": None,
                "mode": "side_agnostic_with_grouped_selection",
            },
            "current_filters": {
                "shape_positive_modifier": True,
                "shape_context_dependent": True,
                "shape_missing": True,
                "stable_bad_pick_family_exclusion": True,
                "bad_pick_diagnostic_downgrade": True,
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "explicit_liquidity_filter": False,
                "explicit_tradability_filter": False,
            },
            "current_topk_caps": {
                "top5": 5,
                "top10": 10,
                "top20": 20,
            },
            "current_selection_modes": {
                "prefilter": "KEEP_PRIMARY | KEEP_WATCH | DOWNGRADE | EXCLUDE",
                "two_stage": "original | primary_only | primary_watch_backfill | watch_only_reference",
            },
            "current_no_lookahead_requirements": {
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "source_date_leq_anchor_date": True,
            },
            "non_gates": {
                "model_score_not_used_at_generation": True,
                "champion_fallback_not_used": True,
                "meeMee_reflection": False,
            },
        },
        "current_active_sessions": {
            "broad_prefilter": {
                "manifest": broad_prefilter_manifest,
                "coverage": broad_prefilter_coverage,
                "decision": broad_prefilter_decision,
                "row_count": int(len(broad_prefilter_rows)),
                "group_count": int(broad_prefilter_rows.groupby(["anchor_date", "side"], sort=False).ngroups),
                "by_side": broad_summary,
            },
            "broad_two_stage": {
                "manifest": broad_two_stage_manifest,
                "coverage": broad_two_stage_coverage,
                "decision": broad_two_stage_decision,
                "comparison": broad_two_stage_comparison,
                "row_count": int(len(broad_prefilter_rows)),
                "group_count": int(broad_prefilter_rows.groupby(["anchor_date", "side"], sort=False).ngroups),
            },
            "repair_prefilter_probe": {
                "manifest": repair_manifest,
                "coverage": repair_coverage,
                "row_count": int(repair_coverage.get("candidate_count_original", 0) or 0),
                "group_count": int(repair_coverage.get("join_coverage", {}).get("shape_joined_count", 0) and 2 or 0),
            },
        },
        "raw_candidate_snapshot_universe": {
            "row_count": int(len(raw_universe_frame)),
            "group_count": int(raw_universe_frame.groupby(["anchor_date", "side"], sort=False).ngroups),
            "by_side": raw_summary,
            "source_file": str(raw_candidate_universe),
            "note": "broader source universe available for recall expansion",
        },
        "accumulated_surface_reference": {
            "row_count": int(len(accumulated_frame)),
            "group_count": int(accumulated_frame.groupby(["anchor_date", "side"], sort=False).ngroups),
            "by_side": accumulated_summary,
            "source_file": str(accumulated_surface),
            "note": "current downstream surface used to judge recall failure",
        },
        "breadth_audit_reference": current_breadth,
        "oracle_headroom_reference": current_oracle,
        "inventory_judgment": {
            "primary_blocker": "side_aware_under_supply",
            "secondary_blocker": "broad_watch_backfill_dilutes_top15_quality",
            "raw_universe_supports_broader_pool": True,
            "rejected_rows_outside_snapshot_not_logged": True,
        },
    }
    return inventory, broad_prefilter_rows


def _build_design_options(inventory: dict[str, Any], current_breadth: dict[str, Any], current_oracle: dict[str, Any]) -> list[dict[str, Any]]:
    broad_summary = inventory["current_active_sessions"]["broad_prefilter"]["by_side"]
    raw_summary = inventory["raw_candidate_snapshot_universe"]["by_side"]
    accumulated_summary = inventory["accumulated_surface_reference"]["by_side"]
    current_thin = current_breadth["overall_thin_groups"]
    short_rows = broad_summary.get("short", {}).get("row_count", 0)
    long_rows = broad_summary.get("long", {}).get("row_count", 0)
    short_mean = broad_summary.get("short", {}).get("mean_group_size", 0.0) or 0.0
    long_mean = broad_summary.get("long", {}).get("mean_group_size", 0.0) or 0.0
    side_imbalance = short_rows < max(1, long_rows // 4)
    options = [
        {
            "option_name": "side_aware_candidate_admission_caps",
            "rank": 1,
            "expected_recall_improvement": "high",
            "expected_false_positive_cost": "moderate",
            "expected_group_breadth_improvement": "high_on_short_side",
            "no_lookahead_safety": True,
            "implementation_difficulty": "medium",
            "preserves_same_condition_comparability": True,
            "evaluation_method": "same-condition compare current broad pool vs side-capped high-recall pool; measure topK recall, false positives, and side breadth",
            "rationale": "short side remains structurally under-supplied and the broad watch backfill path dilutes practical quality if unconstrained",
        },
        {
            "option_name": "two_stage_high_recall_then_rerank",
            "rank": 2,
            "expected_recall_improvement": "high",
            "expected_false_positive_cost": "moderate_high",
            "expected_group_breadth_improvement": "high",
            "no_lookahead_safety": True,
            "implementation_difficulty": "medium",
            "preserves_same_condition_comparability": True,
            "evaluation_method": "compare current pool against a broader two-stage pool and its oracle headroom under the same topK frame",
            "rationale": "source fields already exist to backfill more candidates, but pure broadening should stay side-aware to avoid quality dilution",
        },
        {
            "option_name": "widen_topN_per_anchor_side",
            "rank": 3,
            "expected_recall_improvement": "medium",
            "expected_false_positive_cost": "moderate",
            "expected_group_breadth_improvement": "medium",
            "no_lookahead_safety": True,
            "implementation_difficulty": "low",
            "preserves_same_condition_comparability": True,
            "evaluation_method": "increase the anchor/side selection cap and compare current vs widened pools on identical held-out windows",
            "rationale": "simple to implement, but likely too blunt without side-aware caps because short-side supply is the main bottleneck",
        },
        {
            "option_name": "lower_admission_thresholds",
            "rank": 4,
            "expected_recall_improvement": "medium",
            "expected_false_positive_cost": "high",
            "expected_group_breadth_improvement": "medium",
            "no_lookahead_safety": True,
            "implementation_difficulty": "low",
            "preserves_same_condition_comparability": True,
            "evaluation_method": "relax KEEP_PRIMARY / KEEP_WATCH gates and remeasure topK winner capture vs contamination",
            "rationale": "can surface more candidates, but prior broadening already shows quality dilution without side constraints",
        },
        {
            "option_name": "long_side_high_recall_only",
            "rank": 5,
            "expected_recall_improvement": "limited",
            "expected_false_positive_cost": "low_moderate",
            "expected_group_breadth_improvement": "low_on_short_side",
            "no_lookahead_safety": True,
            "implementation_difficulty": "low",
            "preserves_same_condition_comparability": True,
            "evaluation_method": "stabilize long-side recall first, then compare against the current mixed-side pool",
            "rationale": "useful if short-side supply stays too weak, but it does not solve the cross-side imbalance directly",
        },
        {
            "option_name": "candidate_pool_recall_mode_with_risk_flags",
            "rank": 6,
            "expected_recall_improvement": "high",
            "expected_false_positive_cost": "high_but_measurable",
            "expected_group_breadth_improvement": "high",
            "no_lookahead_safety": True,
            "implementation_difficulty": "high",
            "preserves_same_condition_comparability": True,
            "evaluation_method": "admit a broader pool with risk tags and score its incremental value against the current pool",
            "rationale": "useful only after side-aware caps exist, because the risk-tagged pool is likely to be too noisy on its own",
        },
    ]
    for option in options:
        option["supports_side_imbalance"] = bool(side_imbalance and option["option_name"] in {"side_aware_candidate_admission_caps", "two_stage_high_recall_then_rerank"})
        option["current_thin_groups"] = current_thin
        option["current_broad_probe_side_summary"] = broad_summary
        option["raw_universe_side_summary"] = raw_summary
        option["current_accumulated_surface_side_summary"] = accumulated_summary
        option["oracle_headroom_reference"] = {
            "top5_oracle_gain_vs_champion": current_oracle["breadth_headroom"]["top5"]["oracle_top15_capture_rate"] - current_oracle["breadth_headroom"]["top5"]["champion_top15_capture_rate"],
            "top10_oracle_gain_vs_champion": current_oracle["breadth_headroom"]["top10"]["oracle_top15_capture_rate"] - current_oracle["breadth_headroom"]["top10"]["champion_top15_capture_rate"],
            "top20_oracle_gain_vs_champion": current_oracle["breadth_headroom"]["top20"]["oracle_top15_capture_rate"] - current_oracle["breadth_headroom"]["top20"]["champion_top15_capture_rate"],
        }
    return options


def _build_contract(options: list[dict[str, Any]], inventory: dict[str, Any]) -> dict[str, Any]:
    broad_summary = inventory["current_active_sessions"]["broad_prefilter"]["by_side"]
    raw_summary = inventory["raw_candidate_snapshot_universe"]["by_side"]
    long_mean = float(broad_summary.get("long", {}).get("mean_group_size", 0.0) or 0.0)
    short_mean = float(broad_summary.get("short", {}).get("mean_group_size", 0.0) or 0.0)
    contract = {
        "schema_version": CONTRACT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "contract_name": "side_aware_minimum_pool_size_v1",
        "implementation_style": "two_stage_high_recall_then_rerank",
        "recommended_axis": "side_aware_candidate_admission_caps",
        "group_grain": ["anchor_date", "side"],
        "target_min_candidate_count_per_group": {
            "long": 20,
            "short": 5,
        },
        "target_max_candidate_count_per_group": {
            "long": 40,
            "short": 10,
        },
        "side_specific_caps": {
            "long": 40,
            "short": 10,
        },
        "recall_mode_flags": {
            "include_primary": True,
            "include_watch": True,
            "include_downgrade_backfill": True,
            "include_risk_flagged_rows": True,
            "exclude_only_analysis": True,
        },
        "quality_tags": {
            "primary": "conditional_high_value_and_positive_shape_modifier",
            "watch": "conditional_high_value_and_context_dependent_or_shape_missing",
            "downgrade": "bad_pick_diagnostic_present_but_not_hard_excluded",
            "exclude": "stable_bad_pick_family_and_no_high_value_signal",
        },
        "no_lookahead_requirements": {
            "monthly_context_no_lookahead": True,
            "weekly_context_no_lookahead": True,
            "source_date_leq_anchor_date": True,
            "future_outcome_fields_not_in_features": True,
        },
        "preserve_champion_comparison": {
            "original_score_field": "score",
            "original_rank_field": "rank",
            "ranking_groups": ["anchor_date", "side"],
            "ranking_sort": ["score desc", "rank asc", "symbol asc", "candidate_idx asc"],
            "top_k_values": [5, 10, 20],
        },
        "evaluation_requirements": {
            "top5_top10_top20": True,
            "oracle_within_pool": True,
            "false_positive_cost": True,
            "side_split": True,
            "zero_pass_groups": True,
            "membership_change": True,
        },
        "current_side_support": {
            "long_mean_group_size": long_mean,
            "short_mean_group_size": short_mean,
            "short_is_structurally_under_supplied": bool(short_mean < max(1.0, long_mean / 4.0)) if long_mean else True,
        },
        "raw_source_universe_support": {
            "row_count": int(raw_summary["overall"]["row_count"]),
            "group_count": int(raw_summary["overall"]["group_count"]),
            "supports_broader_pool": True,
        },
        "notes": [
            "The contract is side-aware by construction because the short side remains the main breadth bottleneck.",
            "The pool should widen from the current broad probe while keeping risk-tagged backfill explicit.",
            "No rejected-row instrumentation is required for this first contract because the candidate snapshot universe is already broad enough to implement it.",
        ],
    }
    return contract


def _build_feasibility_estimate(
    *,
    inventory: dict[str, Any],
    current_breadth: dict[str, Any],
    current_oracle: dict[str, Any],
    current_admission: dict[str, Any],
) -> dict[str, Any]:
    broad_summary = inventory["current_active_sessions"]["broad_prefilter"]["by_side"]
    raw_summary = inventory["raw_candidate_snapshot_universe"]["by_side"]
    accumulated_summary = inventory["accumulated_surface_reference"]["by_side"]

    broad_rows = int(inventory["current_active_sessions"]["broad_prefilter"]["row_count"])
    raw_rows = int(raw_summary["overall"]["row_count"])
    accumulated_rows = int(inventory["accumulated_surface_reference"]["row_count"])

    top5_oracle_gain = float(current_oracle["breadth_headroom"]["top5"]["oracle_top15_capture_rate"] - current_oracle["breadth_headroom"]["top5"]["champion_top15_capture_rate"])
    top10_oracle_gain = float(current_oracle["breadth_headroom"]["top10"]["oracle_top15_capture_rate"] - current_oracle["breadth_headroom"]["top10"]["champion_top15_capture_rate"])
    top20_oracle_gain = float(current_oracle["breadth_headroom"]["top20"]["oracle_top15_capture_rate"] - current_oracle["breadth_headroom"]["top20"]["champion_top15_capture_rate"])
    held_out_scope = current_oracle.get("validation_test_only", current_oracle.get("all_surface", {}))
    held_out_topk = held_out_scope.get("topk", {})

    def _headroom_groups(k: str) -> int:
        bucket = held_out_topk.get(k, {})
        champion_zero_pass = int(bucket.get("champion", {}).get("zero_pass_groups", 0) or 0)
        oracle_zero_pass = int(bucket.get("oracle", {}).get("zero_pass_groups", 0) or 0)
        return max(0, champion_zero_pass - oracle_zero_pass)

    feasibility = {
        "schema_version": FEASIBILITY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "available_source_rows": {
            "broad_probe_rows": broad_rows,
            "accumulated_downstream_rows": accumulated_rows,
            "raw_candidate_snapshot_rows": raw_rows,
            "raw_to_accumulated_ratio": float(raw_rows / max(1, accumulated_rows)),
            "raw_to_broad_probe_ratio": float(raw_rows / max(1, broad_rows)),
        },
        "current_pool_shape": {
            "row_count": int(current_breadth["row_count"]),
            "group_count": int(current_breadth["group_count"]),
            "top5_thin_groups": int(current_breadth["overall_thin_groups"]["top5"]),
            "top10_thin_groups": int(current_breadth["overall_thin_groups"]["top10"]),
            "top20_thin_groups": int(current_breadth["overall_thin_groups"]["top20"]),
        },
        "side_support": {
            "long": broad_summary.get("long", {}),
            "short": broad_summary.get("short", {}),
            "accumulated_long": accumulated_summary.get("long", {}),
            "accumulated_short": accumulated_summary.get("short", {}),
        },
        "oracle_headroom": {
            "top5_oracle_gain_vs_champion": top5_oracle_gain,
            "top10_oracle_gain_vs_champion": top10_oracle_gain,
            "top20_oracle_gain_vs_champion": top20_oracle_gain,
            "groups_where_reranking_can_help": {
                "top5": _headroom_groups("5"),
                "top10": _headroom_groups("10"),
                "top20": _headroom_groups("20"),
            },
        },
        "expected_changes": {
            "row_count_increase_upper_bound": raw_rows,
            "row_count_increase_vs_accumulated_surface_upper_bound": int(raw_rows - accumulated_rows),
            "group_count_coverage": int(current_breadth["group_count"]),
            "thin_group_reduction_expected": {
                "top5": "material",
                "top10": "material",
                "top20": "material_on_short_side",
            },
            "additional_future_winners_admitted": "likely_material_but_not_precisely_bounded_without_reject-row_logging",
            "false_positive_growth": "moderate_and_measurable",
            "short_side_mean_group_size": float(broad_summary.get("short", {}).get("mean_group_size", 0.0) or 0.0),
            "short_side_can_be_supplied_meaningfully": True,
        },
        "observability_limits": {
            "rejected_rows_outside_snapshot_not_logged": True,
            "prefilter_reject_instrumentation_required_for_next_precision_step": True,
            "feasibility_confidence": "medium",
        },
        "compatibility": {
            "top5_compatible": True,
            "top10_compatible": True,
            "top20_compatible": True,
            "same_condition_comparability_preserved": True,
            "no_lookahead_safe": bool(
                inventory["current_active_sessions"]["broad_prefilter"]["coverage"]["monthly_context_no_lookahead"]
                and inventory["current_active_sessions"]["broad_prefilter"]["coverage"]["weekly_context_no_lookahead"]
                and inventory["current_active_sessions"]["broad_two_stage"]["coverage"]["monthly_context_no_lookahead"]
                and inventory["current_active_sessions"]["broad_two_stage"]["coverage"]["weekly_context_no_lookahead"]
            ),
        },
        "current_admission_signals": {
            "summary_by_topk": current_admission.get("summary_by_topk", {}),
            "top5": current_admission.get("summary_by_topk", {}).get("5", {}),
            "top10": current_admission.get("summary_by_topk", {}).get("10", {}),
            "top20": current_admission.get("summary_by_topk", {}).get("20", {}),
        },
    }
    return feasibility


def _build_evaluation_plan(contract: dict[str, Any], inventory: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": EVALUATION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "evaluation_goal": "compare the current pool against a side-aware high-recall pool on the same anchor_date / side frame before any model retraining",
        "comparison_sets": {
            "current_pool": {
                "source": "current broad probe and current accumulated surface",
                "rows": int(inventory["current_active_sessions"]["broad_prefilter"]["row_count"]),
                "groups": int(inventory["current_active_sessions"]["broad_prefilter"]["group_count"]),
            },
            "high_recall_pool": {
                "source": contract["contract_name"],
                "implementation_style": contract["implementation_style"],
                "target_min_candidate_count_per_group": contract["target_min_candidate_count_per_group"],
                "target_max_candidate_count_per_group": contract["target_max_candidate_count_per_group"],
                "side_specific_caps": contract["side_specific_caps"],
            },
            "oracle_within_each_pool": True,
            "champion_within_current_pool": True,
            "simple_baseline_within_high_recall_pool": True,
        },
        "held_out_scope": {
            "same_condition": True,
            "group_grain": ["anchor_date", "side"],
            "top_k_values": [5, 10, 20],
            "period_match": "validation_test_only",
        },
        "metrics": [
            "mean_forward_ret_20d",
            "mean_path_value_score_v1",
            "top15_capture",
            "bottom15_contamination",
            "membership_changed_count",
            "overlap_ratio",
            "zero_pass_groups",
            "side_split",
            "false_positive_cost",
        ],
        "pass_fail_criteria": {
            "recall_improves": "high_recall_pool should recover more winners than the current pool",
            "quality_does_not_collapse": "bottom15 contamination must remain measurable and bounded",
            "side_imbalance_reduces": "short-side group coverage should improve relative to the current pool",
            "no_lookahead": True,
        },
        "implementation_follow_up": {
            "after_this_plan": "candidate-pool feasibility implementation only, not modeling",
            "next_artifact_family": "candidate_generation_implementation_v1",
        },
    }


def _build_recommendation(*, options: list[dict[str, Any]], contract: dict[str, Any], feasibility: dict[str, Any]) -> dict[str, Any]:
    recommended_axis = "side_aware_candidate_admission_caps"
    reason = "short_side_structurally_under_supplied_and_broad_backfill_mixes_quality"
    if feasibility["compatibility"]["no_lookahead_safe"] is False:
        recommended_axis = "stop_candidate_generation_redesign"
        reason = "no_lookahead_contract_not_verified"
    return {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "recommended_axis": recommended_axis,
        "reason": reason,
        "contract_name": contract["contract_name"],
        "implementation_style": contract["implementation_style"],
        "top_ranked_options": options[:3],
        "decision_hint": "ready_to_design_side_aware_min_pool" if recommended_axis == "side_aware_candidate_admission_caps" else "stop_candidate_generation_line",
    }


def _build_decision(recommendation: dict[str, Any], feasibility: dict[str, Any]) -> dict[str, Any]:
    if recommendation["recommended_axis"] == "stop_candidate_generation_redesign":
        decision = "stop_candidate_generation_line"
        typed_reason = "no_lookahead_or_source_contract_not_safe"
    elif feasibility["observability_limits"]["rejected_rows_outside_snapshot_not_logged"]:
        # The broad candidate snapshot universe is available, but the side-aware min-pool contract is
        # the next actionable axis because the current evidence points to side imbalance rather than a
        # missing raw universe.
        decision = "ready_to_design_side_aware_min_pool"
        typed_reason = "side_imbalance_is_the_most_actionable_bottleneck"
    else:
        decision = "ready_to_implement_high_recall_candidate_pool_v1"
        typed_reason = "broad_source_fields_support_a_high_recall_pool"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "decision": decision,
        "authoritative_rollup_decision": decision,
        "typed_reasons": [typed_reason],
        "same_condition_contract": True,
        "not_meemee_reflectable": True,
        "production_reflection_allowed": False,
    }


def build_artifacts(
    *,
    broad_prefilter_session: Path,
    broad_two_stage_session: Path,
    repair_prefilter_session: Path,
    raw_candidate_universe: Path,
    accumulated_surface: Path,
    current_breadth_audit: Path,
    current_oracle_headroom: Path,
    current_admission_audit: Path,
) -> dict[str, Any]:
    inventory, broad_prefilter_rows = _build_current_candidate_generation_contract_inventory(
        broad_prefilter_session=broad_prefilter_session,
        broad_two_stage_session=broad_two_stage_session,
        repair_prefilter_session=repair_prefilter_session,
        raw_candidate_universe=raw_candidate_universe,
        accumulated_surface=accumulated_surface,
        current_breadth_audit=current_breadth_audit,
        current_oracle_headroom=current_oracle_headroom,
    )
    current_breadth = _load_json(current_breadth_audit)
    current_oracle = _load_json(current_oracle_headroom)
    current_admission = _load_json(current_admission_audit)

    options = _build_design_options(inventory, current_breadth, current_oracle)
    contract = _build_contract(options, inventory)
    feasibility = _build_feasibility_estimate(
        inventory=inventory,
        current_breadth=current_breadth,
        current_oracle=current_oracle,
        current_admission=current_admission,
    )
    evaluation_plan = _build_evaluation_plan(contract, inventory)
    recommendation = _build_recommendation(options=options, contract=contract, feasibility=feasibility)
    decision = _build_decision(recommendation, feasibility)

    rejected_source_inventory = {
        "schema_version": "tradex_high_recall_candidate_pool_design_v1_rejected_source_inventory_v1",
        "generated_at": _utc_now(),
        "available": False,
        "reason": "current source sessions expose admitted candidate snapshots and exclusion diagnostics, but no standalone rejected-row log for prefilter rejects",
        "available_sources": {
            "broad_candidate_snapshot_universe": str(raw_candidate_universe),
            "current_broad_prefilter_session": str(broad_prefilter_session),
            "current_broad_two_stage_session": str(broad_two_stage_session),
        },
        "next_instrumentation_step": "log pre-admission rejected rows with stable anchor_date / side / symbol keys and reject_reason buckets if deeper recall tuning is needed",
    }

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": None,
        "jobs_supported": 1,
        "source_roots": {
            "current_broad_prefilter_session": str(broad_prefilter_session),
            "current_broad_two_stage_session": str(broad_two_stage_session),
            "current_repair_prefilter_session": str(repair_prefilter_session),
            "raw_candidate_universe": str(raw_candidate_universe),
            "accumulated_surface": str(accumulated_surface),
            "current_breadth_audit": str(current_breadth_audit),
            "current_oracle_headroom": str(current_oracle_headroom),
            "current_admission_audit": str(current_admission_audit),
        },
        "source_artifacts_used": {
            "current_candidate_generation_contract_inventory": "derived from prefilter/two-stage sessions and source candidate snapshots",
            "breadth_audit": str(current_breadth_audit),
            "oracle_headroom": str(current_oracle_headroom),
            "admission_failure": str(current_admission_audit),
            "current_accumulated_surface": str(accumulated_surface),
        },
        "non_scope": {
            "meeMee": True,
            "production_ranking": True,
            "publish_or_promotion": True,
            "research_inventory_json": True,
            "model_training": True,
            "label_tuning": True,
            "challenger_creation": True,
        },
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "current_candidate_generation_contract_inventory": {
            "broad_prefilter_session": str(broad_prefilter_session),
            "broad_two_stage_session": str(broad_two_stage_session),
            "repair_prefilter_session": str(repair_prefilter_session),
            "raw_candidate_universe": str(raw_candidate_universe),
        },
        "evaluation_evidence": {
            "current_breadth_audit": str(current_breadth_audit),
            "current_oracle_headroom": str(current_oracle_headroom),
            "current_admission_audit": str(current_admission_audit),
            "current_accumulated_surface": str(accumulated_surface),
        },
        "implementation_source_files": {
            "prefilter_script": str(REPO_ROOT / "scripts" / "tradex_candidate_generation_pre_filter_context_shape_v1.py"),
            "two_stage_script": str(REPO_ROOT / "scripts" / "tradex_candidate_generation_two_stage_admission_context_shape_v1.py"),
            "breadth_audit_script": str(REPO_ROOT / "scripts" / "tradex_candidate_generation_breadth_quality_redesign_audit_v1.py"),
        },
        "used_artifacts": {
            "current_broad_prefilter_session": str(broad_prefilter_session),
            "current_broad_two_stage_session": str(broad_two_stage_session),
            "current_repair_prefilter_session": str(repair_prefilter_session),
            "raw_candidate_universe": str(raw_candidate_universe),
            "accumulated_surface": str(accumulated_surface),
        },
        "not_used": {
            "model_training": True,
            "meeMee": True,
            "production_ranking": True,
            "research_inventory_mutation": True,
        },
    }

    current_breadth_frame = pd.DataFrame(
        [
            {
                "source": "current_broad_prefilter_session",
                "row_count": inventory["current_active_sessions"]["broad_prefilter"]["row_count"],
                "group_count": inventory["current_active_sessions"]["broad_prefilter"]["group_count"],
                "long_row_count": inventory["current_active_sessions"]["broad_prefilter"]["by_side"]["long"]["row_count"],
                "short_row_count": inventory["current_active_sessions"]["broad_prefilter"]["by_side"]["short"]["row_count"],
                "long_group_count": inventory["current_active_sessions"]["broad_prefilter"]["by_side"]["long"]["group_count"],
                "short_group_count": inventory["current_active_sessions"]["broad_prefilter"]["by_side"]["short"]["group_count"],
                "top5_thin_groups": current_breadth["overall_thin_groups"]["top5"],
                "top10_thin_groups": current_breadth["overall_thin_groups"]["top10"],
                "top20_thin_groups": current_breadth["overall_thin_groups"]["top20"],
            },
            {
                "source": "raw_candidate_snapshot_universe",
                "row_count": inventory["raw_candidate_snapshot_universe"]["row_count"],
                "group_count": inventory["raw_candidate_snapshot_universe"]["group_count"],
                "long_row_count": inventory["raw_candidate_snapshot_universe"]["by_side"]["long"]["row_count"],
                "short_row_count": inventory["raw_candidate_snapshot_universe"]["by_side"]["short"]["row_count"],
                "long_group_count": inventory["raw_candidate_snapshot_universe"]["by_side"]["long"]["group_count"],
                "short_group_count": inventory["raw_candidate_snapshot_universe"]["by_side"]["short"]["group_count"],
                "top5_thin_groups": inventory["raw_candidate_snapshot_universe"]["by_side"]["overall"]["top5_thin_groups"],
                "top10_thin_groups": inventory["raw_candidate_snapshot_universe"]["by_side"]["overall"]["top10_thin_groups"],
                "top20_thin_groups": inventory["raw_candidate_snapshot_universe"]["by_side"]["overall"]["top20_thin_groups"],
            },
            {
                "source": "current_accumulated_surface_reference",
                "row_count": inventory["accumulated_surface_reference"]["row_count"],
                "group_count": inventory["accumulated_surface_reference"]["group_count"],
                "long_row_count": inventory["accumulated_surface_reference"]["by_side"]["long"]["row_count"],
                "short_row_count": inventory["accumulated_surface_reference"]["by_side"]["short"]["row_count"],
                "long_group_count": inventory["accumulated_surface_reference"]["by_side"]["long"]["group_count"],
                "short_group_count": inventory["accumulated_surface_reference"]["by_side"]["short"]["group_count"],
                "top5_thin_groups": inventory["accumulated_surface_reference"]["by_side"]["overall"]["top5_thin_groups"],
                "top10_thin_groups": inventory["accumulated_surface_reference"]["by_side"]["overall"]["top10_thin_groups"],
                "top20_thin_groups": inventory["accumulated_surface_reference"]["by_side"]["overall"]["top20_thin_groups"],
            },
        ]
    )

    optional_thresholds = pd.DataFrame(
        [
            {"name": "top15_score_threshold", "value": inventory["current_candidate_generation_contract"]["current_thresholds"]["top15_score_threshold"], "source": "prefilter_policy"},
            {"name": "bottom15_score_threshold", "value": inventory["current_candidate_generation_contract"]["current_thresholds"]["bottom15_score_threshold"], "source": "prefilter_policy"},
            {"name": "min_sample_count", "value": inventory["current_candidate_generation_contract"]["current_thresholds"]["source_thresholds"].get("min_sample_count"), "source": "source_thresholds"},
            {"name": "min_unique_symbol_count", "value": inventory["current_candidate_generation_contract"]["current_thresholds"]["source_thresholds"].get("min_unique_symbol_count"), "source": "source_thresholds"},
            {"name": "min_month_count", "value": inventory["current_candidate_generation_contract"]["current_thresholds"]["source_thresholds"].get("min_month_count"), "source": "source_thresholds"},
        ]
    )

    return {
        "manifest": manifest,
        "input_resolution": input_resolution,
        "current_candidate_generation_contract_inventory": inventory,
        "high_recall_candidate_pool_design_options": {
            "schema_version": OPTIONS_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "options": options,
        },
        "high_recall_candidate_pool_contract": contract,
        "high_recall_candidate_pool_feasibility_estimate": feasibility,
        "high_recall_candidate_pool_evaluation_plan": evaluation_plan,
        "high_recall_candidate_pool_design_v1_decision": decision,
        "rejected_candidate_source_inventory": rejected_source_inventory,
        "optional_group_size_distribution": current_breadth_frame,
        "optional_threshold_inventory": optional_thresholds,
    }


def write_artifacts(*, output_root: Path, session_id: str | None = None, jobs_supported: int = 1, **kwargs: Any) -> Path:
    payload = build_artifacts(**kwargs)
    final_session_id = session_id or _session_id()
    session_root = output_root / final_session_id
    session_root.mkdir(parents=True, exist_ok=False)

    payload["manifest"]["session_id"] = final_session_id
    payload["input_resolution"]["session_id"] = final_session_id
    payload["current_candidate_generation_contract_inventory"]["session_id"] = final_session_id
    payload["high_recall_candidate_pool_design_options"]["session_id"] = final_session_id
    payload["high_recall_candidate_pool_contract"]["session_id"] = final_session_id
    payload["high_recall_candidate_pool_feasibility_estimate"]["session_id"] = final_session_id
    payload["high_recall_candidate_pool_evaluation_plan"]["session_id"] = final_session_id
    payload["high_recall_candidate_pool_design_v1_decision"]["session_id"] = final_session_id
    payload["rejected_candidate_source_inventory"]["session_id"] = final_session_id
    payload["manifest"]["jobs_supported"] = int(jobs_supported or 1)

    _write_json(session_root / "run_manifest.json", payload["manifest"])
    _write_json(session_root / "input_resolution.json", payload["input_resolution"])
    _write_json(session_root / "current_candidate_generation_contract_inventory.json", payload["current_candidate_generation_contract_inventory"])
    _write_json(session_root / "high_recall_candidate_pool_design_options.json", payload["high_recall_candidate_pool_design_options"])
    _write_json(session_root / "high_recall_candidate_pool_contract.json", payload["high_recall_candidate_pool_contract"])
    _write_json(session_root / "high_recall_candidate_pool_feasibility_estimate.json", payload["high_recall_candidate_pool_feasibility_estimate"])
    _write_json(session_root / "high_recall_candidate_pool_evaluation_plan.json", payload["high_recall_candidate_pool_evaluation_plan"])
    _write_json(session_root / "high_recall_candidate_pool_design_v1_decision.json", payload["high_recall_candidate_pool_design_v1_decision"])
    _write_json(session_root / "rejected_candidate_source_inventory.json", payload["rejected_candidate_source_inventory"])

    _write_parquet(session_root / "candidate_generation_group_size_distribution.parquet", payload["optional_group_size_distribution"])
    _write_parquet(session_root / "candidate_generation_threshold_inventory.parquet", payload["optional_threshold_inventory"])

    complete = {
        "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": final_session_id,
        "required_artifacts": [
            "run_manifest.json",
            "input_resolution.json",
            "current_candidate_generation_contract_inventory.json",
            "high_recall_candidate_pool_design_options.json",
            "high_recall_candidate_pool_contract.json",
            "high_recall_candidate_pool_feasibility_estimate.json",
            "high_recall_candidate_pool_evaluation_plan.json",
            "high_recall_candidate_pool_design_v1_decision.json",
        ],
        "optional_artifacts": [
            "rejected_candidate_source_inventory.json",
            "candidate_generation_group_size_distribution.parquet",
            "candidate_generation_threshold_inventory.parquet",
        ],
        "status": "complete",
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return session_root


def main() -> int:
    parser = argparse.ArgumentParser(description="Design a side-aware high-recall candidate pool contract.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()

    output_root = Path(str(args.output_root)).expanduser().resolve()
    session_root = write_artifacts(
        output_root=output_root,
        session_id=args.session_id,
        broad_prefilter_session=CURRENT_BROAD_PREFILTER_SESSION,
        broad_two_stage_session=CURRENT_BROAD_TWO_STAGE_SESSION,
        repair_prefilter_session=CURRENT_REPAIR_PREFILTER_SESSION,
        raw_candidate_universe=RAW_CANDIDATE_SNAPSHOT_UNIVERSE,
        accumulated_surface=ACCUMULATED_SESSION / "accumulated_forward_prediction_rows.parquet",
        current_breadth_audit=REDESIGN_AUDIT_SESSION / "candidate_pool_breadth_audit.json",
        current_oracle_headroom=REDESIGN_AUDIT_SESSION / "candidate_pool_oracle_headroom_audit.json",
        current_admission_audit=REDESIGN_AUDIT_SESSION / "candidate_admission_failure_audit.json",
        jobs_supported=int(args.jobs),
    )
    print(str(session_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
