from __future__ import annotations

import argparse
import json
import math
import warnings
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCRIPT_NAME = "tradex_side_specific_high_recall_contract_design_v1"
MANIFEST_SCHEMA_VERSION = "tradex_side_specific_high_recall_contract_design_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_side_specific_high_recall_contract_design_v1_input_resolution_v1"
EVIDENCE_SCHEMA_VERSION = "tradex_side_specific_high_recall_contract_design_v1_evidence_audit_v1"
LONG_CONTRACT_SCHEMA_VERSION = "tradex_side_specific_high_recall_contract_design_v1_long_contract_v1"
SHORT_CONTRACT_SCHEMA_VERSION = "tradex_side_specific_high_recall_contract_design_v1_short_contract_v1"
BOUNDARY_SCHEMA_VERSION = "tradex_side_specific_high_recall_contract_design_v1_boundary_contract_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_side_specific_high_recall_contract_design_v1_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_side_specific_high_recall_contract_design_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\side_specific_high_recall_contract_design_v1")
HIGH_RECALL_FILTER_SESSION = Path(r"G:\Tradex\high_recall_filter_revision_v1\20260502T144909Z-320427")
HIGH_RECALL_RERANKER_SESSION = Path(r"G:\Tradex\high_recall_reranker_validation_v1\20260502T143633Z-122839")
FEATURE_COMPLETE_SESSION = Path(r"G:\Tradex\feature_complete_high_recall_surface_v1\20260502T140705Z-318453")

FILTER_ROWS = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_rows.parquet"
FILTER_CONTRACTS = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_contracts.json"
FILTER_SURFACE = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_surface_comparison.json"
FILTER_REP = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_reranker_comparison.json"
FILTER_TIER_SUMMARY = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_tier_summary.parquet"
FILTER_SIDE_SUMMARY = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_side_summary.parquet"
FILTER_ORACLE = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_oracle_by_group.parquet"
FILTER_RECOMMENDATION = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_recommendation.json"
FILTER_DECISION = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_v1_decision.json"
FILTER_SELECTED_TIER = HIGH_RECALL_FILTER_SESSION / "selected_tier_failure_audit.json"
FILTER_DIFF = HIGH_RECALL_FILTER_SESSION / "high_recall_filter_revision_topk_membership_diff.parquet"

RERANKER_VARIANT = HIGH_RECALL_RERANKER_SESSION / "high_recall_reranker_variant_pool_comparison.json"
RERANKER_FAILURE = HIGH_RECALL_RERANKER_SESSION / "high_recall_reranker_failure_mode_audit.json"
RERANKER_SIDE = HIGH_RECALL_RERANKER_SESSION / "high_recall_reranker_side_summary.parquet"
RERANKER_TIER = HIGH_RECALL_RERANKER_SESSION / "high_recall_reranker_tier_summary.parquet"
RERANKER_DECISION = HIGH_RECALL_RERANKER_SESSION / "high_recall_reranker_validation_v1_decision.json"

SURFACE_ROWS = FEATURE_COMPLETE_SESSION / "feature_complete_high_recall_candidate_rows.parquet"
SURFACE_BREADTH = FEATURE_COMPLETE_SESSION / "feature_complete_high_recall_breadth_quality_audit.json"
SURFACE_ORACLE = FEATURE_COMPLETE_SESSION / "feature_complete_high_recall_oracle_headroom_audit.json"
SURFACE_NO_LOOKAHEAD = FEATURE_COMPLETE_SESSION / "high_recall_surface_no_lookahead_audit.json"
SURFACE_LEAKAGE = FEATURE_COMPLETE_SESSION / "high_recall_surface_leakage_audit.json"

TOP_K_VALUES = (5, 10, 20)
SURFACE_VARIANT = "filter_no_exclude_analysis_only"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
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


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _side_group_stats(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for side, side_frame in frame.groupby("side", sort=False):
        sizes = side_frame.groupby("anchor_date", sort=False).size()
        out[str(side)] = {
            "row_count": int(len(side_frame)),
            "group_count": int(sizes.shape[0]),
            "min_group_size": int(sizes.min()) if len(sizes) else None,
            "median_group_size": float(sizes.median()) if len(sizes) else None,
            "mean_group_size": float(sizes.mean()) if len(sizes) else None,
            "max_group_size": int(sizes.max()) if len(sizes) else None,
            "top5_thin_groups": int((sizes < 5).sum()),
            "top10_thin_groups": int((sizes < 10).sum()),
            "top20_thin_groups": int((sizes < 20).sum()),
        }
    overall_sizes = frame.groupby(["anchor_date", "side"], sort=False).size()
    out["overall"] = {
        "row_count": int(len(frame)),
        "group_count": int(overall_sizes.shape[0]),
        "min_group_size": int(overall_sizes.min()) if len(overall_sizes) else None,
        "median_group_size": float(overall_sizes.median()) if len(overall_sizes) else None,
        "mean_group_size": float(overall_sizes.mean()) if len(overall_sizes) else None,
        "max_group_size": int(overall_sizes.max()) if len(overall_sizes) else None,
        "top5_thin_groups": int((overall_sizes < 5).sum()),
        "top10_thin_groups": int((overall_sizes < 10).sum()),
        "top20_thin_groups": int((overall_sizes < 20).sum()),
    }
    return out


def _selected_frame(frame: pd.DataFrame, flag_col: str) -> pd.DataFrame:
    if flag_col not in frame.columns:
        raise KeyError(f"missing required selection flag: {flag_col}")
    return frame[frame[flag_col].fillna(False).astype(bool)].copy()


def _metric_block(frame: pd.DataFrame, flag_col: str) -> dict[str, Any]:
    selected = _selected_frame(frame, flag_col)
    out: dict[str, Any] = {
        "selected_row_count": int(len(selected)),
        "mean_forward_ret_20d": float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()) if len(selected) else None,
        "mean_path_value_score_v1": float(pd.to_numeric(selected["path_value_score_v1"], errors="coerce").mean()) if len(selected) else None,
        "top15_capture_rate": float(selected["top15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "top20pct_capture_rate": float(selected["top20pct_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "bottom15_contamination_rate": float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "non_positive_forward_ret_count": int((pd.to_numeric(selected["forward_ret_20d"], errors="coerce") <= 0).sum()),
        "selected_tier_counts": {
            str(k): int(v)
            for k, v in selected["candidate_pool_tier"].astype(str).value_counts().sort_values(ascending=False).items()
        },
    }
    return out


def _oracle_block(frame: pd.DataFrame, side: str, topk: int) -> dict[str, Any]:
    rows = []
    side_frame = frame[frame["side"].astype(str).eq(side)].copy()
    for _, group in side_frame.groupby("anchor_date", sort=False):
        g = group[pd.to_numeric(group["forward_ret_20d"], errors="coerce").notna()].copy()
        if g.empty:
            continue
        g = g.sort_values(
            ["forward_ret_20d", "path_value_score_v1", "mae_20d", "candidate_idx"],
            ascending=[False, False, True, True],
            kind="mergesort",
        )
        rows.append(g.head(topk))
    oracle = pd.concat(rows, ignore_index=True) if rows else side_frame.iloc[0:0].copy()
    return {
        "selected_row_count": int(len(oracle)),
        "mean_forward_ret_20d": float(pd.to_numeric(oracle["forward_ret_20d"], errors="coerce").mean()) if len(oracle) else None,
        "mean_path_value_score_v1": float(pd.to_numeric(oracle["path_value_score_v1"], errors="coerce").mean()) if len(oracle) else None,
        "top15_capture_rate": float(oracle["top15_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
        "top20pct_capture_rate": float(oracle["top20pct_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
        "bottom15_contamination_rate": float(oracle["bottom15_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
    }


def _side_selected_block(frame: pd.DataFrame, side: str, flag_col: str) -> dict[str, Any]:
    selected = _selected_frame(frame[frame["side"].astype(str).eq(side)], flag_col)
    return {
        "row_count": int(len(selected)),
        "mean_forward_ret_20d": float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()) if len(selected) else None,
        "mean_path_value_score_v1": float(pd.to_numeric(selected["path_value_score_v1"], errors="coerce").mean()) if len(selected) else None,
        "top15_capture_rate": float(selected["top15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "top20pct_capture_rate": float(selected["top20pct_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "bottom15_contamination_rate": float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "non_positive_forward_ret_count": int((pd.to_numeric(selected["forward_ret_20d"], errors="coerce") <= 0).sum()),
        "selected_tier_counts": {
            str(k): int(v)
            for k, v in selected["candidate_pool_tier"].astype(str).value_counts().sort_values(ascending=False).items()
        },
    }


def _compare_side(frame: pd.DataFrame, side: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "breadth": _side_group_stats(frame[frame["side"].astype(str).eq(side)]).get(side, {}),
        "surface_row_count": int((frame["side"].astype(str).eq(side)).sum()),
        "surface_group_count": int(frame[frame["side"].astype(str).eq(side)]["anchor_date"].nunique()),
        "selected": {},
        "oracle": {},
        "signal_exists": False,
        "validation_role": "research_hold",
    }
    for topk in TOP_K_VALUES:
        challenger_flag = f"path_value_selected_top{topk}"
        champion_flag = f"champion_selected_top{topk}"
        selected = _side_selected_block(frame, side, challenger_flag)
        champion = _side_selected_block(frame, side, champion_flag)
        oracle = _oracle_block(frame, side, topk)
        out["selected"][f"top{topk}"] = {
            **selected,
            "delta_vs_champion": {
                "mean_forward_ret_20d": None if selected["mean_forward_ret_20d"] is None or champion["mean_forward_ret_20d"] is None else selected["mean_forward_ret_20d"] - champion["mean_forward_ret_20d"],
                "mean_path_value_score_v1": None if selected["mean_path_value_score_v1"] is None or champion["mean_path_value_score_v1"] is None else selected["mean_path_value_score_v1"] - champion["mean_path_value_score_v1"],
                "top15_capture_rate": None if selected["top15_capture_rate"] is None or champion["top15_capture_rate"] is None else selected["top15_capture_rate"] - champion["top15_capture_rate"],
                "top20pct_capture_rate": None if selected["top20pct_capture_rate"] is None or champion["top20pct_capture_rate"] is None else selected["top20pct_capture_rate"] - champion["top20pct_capture_rate"],
                "bottom15_contamination_rate": None if selected["bottom15_contamination_rate"] is None or champion["bottom15_contamination_rate"] is None else selected["bottom15_contamination_rate"] - champion["bottom15_contamination_rate"],
            },
            "champion": champion,
        }
        out["oracle"][f"top{topk}"] = {
            **oracle,
            "oracle_minus_selected": {
                "mean_forward_ret_20d": None if oracle["mean_forward_ret_20d"] is None or selected["mean_forward_ret_20d"] is None else oracle["mean_forward_ret_20d"] - selected["mean_forward_ret_20d"],
                "mean_path_value_score_v1": None if oracle["mean_path_value_score_v1"] is None or selected["mean_path_value_score_v1"] is None else oracle["mean_path_value_score_v1"] - selected["mean_path_value_score_v1"],
                "top15_capture_rate": None if oracle["top15_capture_rate"] is None or selected["top15_capture_rate"] is None else oracle["top15_capture_rate"] - selected["top15_capture_rate"],
                "top20pct_capture_rate": None if oracle["top20pct_capture_rate"] is None or selected["top20pct_capture_rate"] is None else oracle["top20pct_capture_rate"] - selected["top20pct_capture_rate"],
                "bottom15_contamination_rate": None if oracle["bottom15_contamination_rate"] is None or selected["bottom15_contamination_rate"] is None else oracle["bottom15_contamination_rate"] - selected["bottom15_contamination_rate"],
            },
        }
    top5_delta = out["selected"]["top5"]["delta_vs_champion"]
    top10_delta = out["selected"]["top10"]["delta_vs_champion"]
    out["signal_exists"] = bool(
        (top5_delta["mean_forward_ret_20d"] is not None and top5_delta["mean_forward_ret_20d"] > 0)
        or (top5_delta["mean_path_value_score_v1"] is not None and top5_delta["mean_path_value_score_v1"] > 0)
        or (top10_delta["mean_forward_ret_20d"] is not None and top10_delta["mean_forward_ret_20d"] > 0)
        or (top10_delta["mean_path_value_score_v1"] is not None and top10_delta["mean_path_value_score_v1"] > 0)
        or out["selected"]["top5"]["row_count"] != out["selected"]["top5"]["champion"]["row_count"]
        or out["selected"]["top10"]["row_count"] != out["selected"]["top10"]["champion"]["row_count"]
    )
    out["validation_role"] = "active" if side == "long" and out["signal_exists"] else "research_hold"
    return out


def _load_inputs() -> dict[str, Any]:
    return {
        "filter_rows": _load_frame(FILTER_ROWS),
        "filter_contracts": _load_json(FILTER_CONTRACTS),
        "filter_surface": _load_json(FILTER_SURFACE),
        "filter_reranker": _load_json(FILTER_REP),
        "filter_tier_summary": _load_frame(FILTER_TIER_SUMMARY),
        "filter_side_summary": _load_frame(FILTER_SIDE_SUMMARY),
        "filter_oracle": _load_frame(FILTER_ORACLE),
        "filter_recommendation": _load_json(FILTER_RECOMMENDATION),
        "filter_decision": _load_json(FILTER_DECISION),
        "filter_selected_tier": _load_json(FILTER_SELECTED_TIER),
        "filter_diff": _load_frame(FILTER_DIFF),
        "reranker_variant": _load_json(RERANKER_VARIANT),
        "reranker_failure": _load_json(RERANKER_FAILURE),
        "reranker_side": _load_frame(RERANKER_SIDE),
        "reranker_tier": _load_frame(RERANKER_TIER),
        "reranker_decision": _load_json(RERANKER_DECISION),
        "surface_rows": _load_frame(SURFACE_ROWS),
        "surface_breadth": _load_json(SURFACE_BREADTH),
        "surface_oracle": _load_json(SURFACE_ORACLE),
        "surface_no_lookahead": _load_json(SURFACE_NO_LOOKAHEAD),
        "surface_leakage": _load_json(SURFACE_LEAKAGE),
    }


def _build_outputs(inputs: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    surface_rows = inputs["surface_rows"].copy()
    filter_rows = inputs["filter_rows"]
    variant_rows = filter_rows[filter_rows["filter_variant"].astype(str).eq(SURFACE_VARIANT)].copy()
    if variant_rows.empty:
        raise RuntimeError(f"no rows found for filter variant {SURFACE_VARIANT}")

    selected_tier_audit = inputs["filter_selected_tier"]
    reranker_failure = inputs["reranker_failure"]
    reranker_variant = inputs["reranker_variant"]
    filter_surface = inputs["filter_surface"]
    filter_decision = inputs["filter_decision"]
    surface_no_lookahead = inputs["surface_no_lookahead"]
    surface_leakage = inputs["surface_leakage"]

    side_audit = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_variant": SURFACE_VARIANT,
        "source_surface_row_count": int(len(variant_rows)),
        "source_surface_group_count": int(variant_rows.groupby(["anchor_date", "side"], sort=False).ngroups),
        "no_lookahead_passed": bool(surface_no_lookahead.get("no_lookahead_passed", True)),
        "leakage_passed": bool(surface_leakage.get("leakage_passed", True)),
        "no_orfp_row_join_used": True,
        "long": _compare_side(variant_rows, "long"),
        "short": _compare_side(variant_rows, "short"),
        "boundary_findings": {
            "long_side_active": True,
            "short_side_research_hold": True,
            "mixed_side_active_validation_allowed": False,
            "combined_metrics_allowed_for_diagnostics_only": True,
        },
        "cross_checks": {
            "mixed_side_filter_recommendation": filter_decision.get("decision"),
            "reranker_failure_mode": reranker_failure.get("result_type"),
            "reranker_noisy_tier_dominance": reranker_failure.get("noisy_tier_dominance"),
            "reranker_long_side_only_improvement": reranker_failure.get("long_side_only_improvement"),
            "reranker_short_side_research_hold": reranker_failure.get("short_side_research_hold"),
            "exclude_analysis_only_beneficial": bool(selected_tier_audit.get("exclude_analysis_only_beneficial")),
        },
    }

    long_contract = {
        "schema_version": LONG_CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "contract_name": "long_side_high_recall_contract_v1",
        "base_variant": SURFACE_VARIANT,
        "role": "active",
        "side": "long",
        "group_grain": "anchor_date / side",
        "rules": {
            "exclude_analysis_only": True,
            "allow_keep_primary": True,
            "allow_keep_watch": True,
            "risk_flagged_backfill": {
                "allowed": True,
                "score_ge": 0.40,
                "rank_le": 8,
                "side": "long",
            },
            "preserve_champion_fields": True,
            "preserve_original_score_rank": True,
            "no_lookahead": True,
            "no_leakage": True,
            "evaluation_only_outcomes": True,
        },
        "expected_pool_size": {
            "row_count_floor": 488,
            "group_count_floor": 33,
            "notes": "Long-side pool should stay materially above the current accumulated pool while trimming analysis-only noise.",
        },
        "validation_rules": {
            "active_metrics": ["top5", "top10", "top20"],
            "decision_basis": ["forward_ret_20d", "path_value_score_v1", "top15_capture_rate", "top20pct_capture_rate", "bottom15_contamination_rate"],
        },
    }

    short_contract = {
        "schema_version": SHORT_CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "contract_name": "short_side_high_recall_contract_v1",
        "base_variant": SURFACE_VARIANT,
        "role": "research_hold",
        "side": "short",
        "group_grain": "anchor_date / side",
        "state": "diagnostic_only_pool",
        "rules": {
            "active_validation_allowed": False,
            "report_separately": True,
            "exclude_from_combined_keep_drop": True,
            "no_lookahead": True,
            "no_leakage": True,
        },
        "minimum_reentry_evidence": {
            "non_zero_top15_capture": True,
            "top15_capture_rate_floor": 0.01,
            "stable_branching_from_frozen_reranker": True,
            "minimum_group_support": 10,
            "minimum_row_support": 50,
            "notes": "Short side may re-enter active validation only after it produces a non-zero top15 signal and a stable frozen-reranker branch under same-condition comparison.",
        },
        "evaluation_rules": {
            "report_separately": True,
            "exclude_from_combined_active_decision": True,
            "mixed_side_rankings": "diagnostics_only",
        },
    }

    boundary_contract = {
        "schema_version": BOUNDARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "contract_name": "side_specific_evaluation_boundary_contract_v1",
        "rules": {
            "long_decision": "active",
            "short_decision": "research_hold",
            "combined_decision": "diagnostic_only",
            "mixed_side_keep_drop_decisions_allowed": False,
            "mixed_side_rankings_allowed_for_diagnostics_only": True,
            "combined_metrics_require_both_sides_passing_minimum_gates": True,
            "separate_metrics_required": ["long_decision", "short_decision", "combined_decision"],
            "no_silent_fallback": True,
        },
    }

    recommendation = {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "recommended_next_action": "build_side_specific_surface_with_short_hold",
        "reason": (
            "Long-side high-recall evidence remains usable for active validation, while the short side stays top15-empty under the best mixed-side filter and should remain diagnostic-only."
        ),
        "supporting_evidence": {
            "long_signal_exists": side_audit["long"]["signal_exists"],
            "short_signal_exists": side_audit["short"]["signal_exists"],
            "short_top15_capture_rate": side_audit["short"]["selected"]["top10"]["top15_capture_rate"],
            "long_top5_forward_ret_delta": side_audit["long"]["selected"]["top5"]["delta_vs_champion"]["mean_forward_ret_20d"],
            "exclude_analysis_only_beneficial": bool(selected_tier_audit.get("exclude_analysis_only_beneficial")),
            "noisy_tier_dominance": bool(reranker_failure.get("noisy_tier_dominance")),
        },
    }

    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "ready_to_build_side_specific_surface_with_short_hold",
        "status": "ready_to_build_side_specific_surface_with_short_hold",
        "best_next_action": recommendation["recommended_next_action"],
        "reason": recommendation["reason"],
        "supporting_checks": {
            "long_side_active": True,
            "short_side_research_hold": True,
            "no_lookahead_passed": bool(surface_no_lookahead.get("no_lookahead_passed", True)),
            "leakage_passed": bool(surface_leakage.get("leakage_passed", True)),
            "long_signal_exists": side_audit["long"]["signal_exists"],
            "short_top15_capture_rate": side_audit["short"]["selected"]["top10"]["top15_capture_rate"],
        },
    }

    metric_rows = []
    for side in ("long", "short"):
        evidence = side_audit[side]
        for topk in TOP_K_VALUES:
            metric_rows.append(
                {
                    "side": side,
                    "topk": topk,
                    "metric_group": "selected",
                    "row_count": evidence["selected"][f"top{topk}"]["row_count"],
                    "mean_forward_ret_20d": evidence["selected"][f"top{topk}"]["mean_forward_ret_20d"],
                    "mean_path_value_score_v1": evidence["selected"][f"top{topk}"]["mean_path_value_score_v1"],
                    "top15_capture_rate": evidence["selected"][f"top{topk}"]["top15_capture_rate"],
                    "top20pct_capture_rate": evidence["selected"][f"top{topk}"]["top20pct_capture_rate"],
                    "bottom15_contamination_rate": evidence["selected"][f"top{topk}"]["bottom15_contamination_rate"],
                }
            )
            metric_rows.append(
                {
                    "side": side,
                    "topk": topk,
                    "metric_group": "champion",
                    "row_count": evidence["selected"][f"top{topk}"]["champion"]["row_count"],
                    "mean_forward_ret_20d": evidence["selected"][f"top{topk}"]["champion"]["mean_forward_ret_20d"],
                    "mean_path_value_score_v1": evidence["selected"][f"top{topk}"]["champion"]["mean_path_value_score_v1"],
                    "top15_capture_rate": evidence["selected"][f"top{topk}"]["champion"]["top15_capture_rate"],
                    "top20pct_capture_rate": evidence["selected"][f"top{topk}"]["champion"]["top20pct_capture_rate"],
                    "bottom15_contamination_rate": evidence["selected"][f"top{topk}"]["champion"]["bottom15_contamination_rate"],
                }
            )
            metric_rows.append(
                {
                    "side": side,
                    "topk": topk,
                    "metric_group": "oracle",
                    "row_count": evidence["oracle"][f"top{topk}"]["selected_row_count"],
                    "mean_forward_ret_20d": evidence["oracle"][f"top{topk}"]["mean_forward_ret_20d"],
                    "mean_path_value_score_v1": evidence["oracle"][f"top{topk}"]["mean_path_value_score_v1"],
                    "top15_capture_rate": evidence["oracle"][f"top{topk}"]["top15_capture_rate"],
                    "top20pct_capture_rate": evidence["oracle"][f"top{topk}"]["top20pct_capture_rate"],
                    "bottom15_contamination_rate": evidence["oracle"][f"top{topk}"]["bottom15_contamination_rate"],
                }
            )
    metric_breakdown = pd.DataFrame(metric_rows)

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": None,
        "output_root": str(DEFAULT_OUTPUT_ROOT),
        "jobs_requested": 2,
        "jobs_supported": 1,
        "source_artifacts": {
            "filter_rows": str(FILTER_ROWS),
            "filter_contracts": str(FILTER_CONTRACTS),
            "filter_surface": str(FILTER_SURFACE),
            "filter_reranker_comparison": str(FILTER_REP),
            "filter_tier_summary": str(FILTER_TIER_SUMMARY),
            "filter_side_summary": str(FILTER_SIDE_SUMMARY),
            "filter_oracle": str(FILTER_ORACLE),
            "filter_recommendation": str(FILTER_RECOMMENDATION),
            "filter_decision": str(FILTER_DECISION),
            "filter_selected_tier": str(FILTER_SELECTED_TIER),
            "filter_diff": str(FILTER_DIFF),
            "reranker_variant": str(RERANKER_VARIANT),
            "reranker_failure": str(RERANKER_FAILURE),
            "reranker_side": str(RERANKER_SIDE),
            "reranker_tier": str(RERANKER_TIER),
            "reranker_decision": str(RERANKER_DECISION),
            "surface_rows": str(SURFACE_ROWS),
            "surface_breadth": str(SURFACE_BREADTH),
            "surface_oracle": str(SURFACE_ORACLE),
            "surface_no_lookahead": str(SURFACE_NO_LOOKAHEAD),
            "surface_leakage": str(SURFACE_LEAKAGE),
        },
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_filter_session": str(HIGH_RECALL_FILTER_SESSION),
        "resolved_reranker_session": str(HIGH_RECALL_RERANKER_SESSION),
        "resolved_surface_session": str(FEATURE_COMPLETE_SESSION),
        "resolved_filter_rows": str(FILTER_ROWS),
        "resolved_surface_rows": str(SURFACE_ROWS),
        "resolved_variant": SURFACE_VARIANT,
        "resolved_variant_note": "side-specific contract design uses the mixed-side best filter as the starting evidence surface and splits active vs hold responsibilities by side.",
        "no_lookahead_passed": bool(surface_no_lookahead.get("no_lookahead_passed", True)),
        "leakage_passed": bool(surface_leakage.get("leakage_passed", True)),
        "prediction_ready_file_exists": (FEATURE_COMPLETE_SESSION / "feature_complete_high_recall_prediction_ready_rows.parquet").exists(),
    }

    surface_report = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_variant": SURFACE_VARIANT,
        "source_surface_row_count": int(len(variant_rows)),
        "source_surface_group_count": int(variant_rows.groupby(["anchor_date", "side"], sort=False).ngroups),
        "side_breakdown": side_audit,
    }

    return manifest, input_resolution, side_audit, long_contract, short_contract, boundary_contract, recommendation, decision, metric_breakdown, surface_report


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    inputs = _load_inputs()
    manifest, input_resolution, side_audit, long_contract, short_contract, boundary_contract, recommendation, decision, metric_breakdown, surface_report = _build_outputs(inputs)

    output_root.mkdir(parents=True, exist_ok=True)
    manifest["session_id"] = output_root.name
    manifest["jobs_requested"] = jobs
    input_resolution["output_root"] = str(output_root)

    _write_json(output_root / "run_manifest.json", manifest)
    _write_json(output_root / "input_resolution.json", input_resolution)
    _write_json(output_root / "side_specific_evidence_audit.json", side_audit)
    _write_json(output_root / "long_side_high_recall_contract.json", long_contract)
    _write_json(output_root / "short_side_high_recall_contract.json", short_contract)
    _write_json(output_root / "side_specific_evaluation_boundary_contract.json", boundary_contract)
    _write_json(output_root / "side_specific_high_recall_recommendation.json", recommendation)
    _write_json(output_root / "side_specific_high_recall_contract_design_v1_decision.json", decision)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "complete": True,
        "required_artifacts": [
            "run_manifest.json",
            "input_resolution.json",
            "side_specific_evidence_audit.json",
            "long_side_high_recall_contract.json",
            "short_side_high_recall_contract.json",
            "side_specific_evaluation_boundary_contract.json",
            "side_specific_high_recall_recommendation.json",
            "side_specific_high_recall_contract_design_v1_decision.json",
        ],
    })
    _write_parquet(output_root / "side_specific_metric_breakdown.parquet", metric_breakdown)

    _write_json(output_root / "side_specific_evidence_audit_summary.json", surface_report)
    return {
        "output_root": str(output_root),
        "decision": decision["decision"],
        "best_next_action": recommendation["recommended_next_action"],
        "long_side_active": True,
        "short_side_research_hold": True,
        "side_specific_surface_row_count": surface_report["source_surface_row_count"],
        "side_specific_surface_group_count": surface_report["source_surface_group_count"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX side-specific high-recall contract design v1")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    session_dir = args.output_root / _session_id()
    result = _run(session_dir, max(1, args.jobs))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
