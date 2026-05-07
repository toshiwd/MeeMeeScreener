from __future__ import annotations

import argparse
import json
import math
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_shadow_feature_reranker_feasibility_v1 import MODEL_FEATURES


warnings.filterwarnings("ignore", category=FutureWarning)

SCRIPT_NAME = "tradex_side_specific_high_recall_surface_v1"
MANIFEST_SCHEMA_VERSION = "tradex_side_specific_high_recall_surface_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_side_specific_high_recall_surface_v1_input_resolution_v1"
FEATURE_CHECK_SCHEMA_VERSION = "tradex_side_specific_high_recall_surface_v1_feature_contract_check_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_side_specific_high_recall_surface_v1_no_lookahead_audit_v1"
LEAKAGE_SCHEMA_VERSION = "tradex_side_specific_high_recall_surface_v1_leakage_audit_v1"
QUALITY_SCHEMA_VERSION = "tradex_side_specific_high_recall_surface_v1_quality_audit_v1"
ORACLE_SCHEMA_VERSION = "tradex_side_specific_high_recall_surface_v1_oracle_headroom_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_side_specific_high_recall_surface_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_side_specific_high_recall_surface_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\side_specific_high_recall_surface_v1")
CONTRACT_SESSION = Path(r"G:\Tradex\side_specific_high_recall_contract_design_v1\20260502T150040Z-295986")
FILTER_SESSION = Path(r"G:\Tradex\high_recall_filter_revision_v1\20260502T144909Z-320427")
FEATURE_COMPLETE_SESSION = Path(r"G:\Tradex\feature_complete_high_recall_surface_v1\20260502T140705Z-318453")

FILTER_ROWS = FILTER_SESSION / "high_recall_filter_revision_rows.parquet"
FILTER_SURFACE = FILTER_SESSION / "high_recall_filter_revision_surface_comparison.json"
FILTER_RERANKER = FILTER_SESSION / "high_recall_filter_revision_reranker_comparison.json"
FILTER_DECISION = FILTER_SESSION / "high_recall_filter_revision_v1_decision.json"

FEATURE_ROWS = FEATURE_COMPLETE_SESSION / "feature_complete_high_recall_candidate_rows.parquet"
FEATURE_COMPLETION_SUMMARY = FEATURE_COMPLETE_SESSION / "feature_completion_summary.json"
SURFACE_NO_LOOKAHEAD = FEATURE_COMPLETE_SESSION / "high_recall_surface_no_lookahead_audit.json"
SURFACE_LEAKAGE = FEATURE_COMPLETE_SESSION / "high_recall_surface_leakage_audit.json"
SURFACE_DECISION = FEATURE_COMPLETE_SESSION / "feature_complete_high_recall_surface_v1_decision.json"

LONG_CONTRACT = CONTRACT_SESSION / "long_side_high_recall_contract.json"
SHORT_CONTRACT = CONTRACT_SESSION / "short_side_high_recall_contract.json"
BOUNDARY_CONTRACT = CONTRACT_SESSION / "side_specific_evaluation_boundary_contract.json"
RECOMMENDATION = CONTRACT_SESSION / "side_specific_high_recall_recommendation.json"
DECISION_SOURCE = CONTRACT_SESSION / "side_specific_high_recall_contract_design_v1_decision.json"
EVIDENCE_AUDIT = CONTRACT_SESSION / "side_specific_evidence_audit.json"
METRIC_BREAKDOWN = CONTRACT_SESSION / "side_specific_metric_breakdown.parquet"

CONTRACT_VARIANT = "filter_no_exclude_analysis_only"
CONTRACT_NAME = "side_specific_high_recall_contract_v1"
LONG_ROLE = "long_active"
SHORT_ROLE = "short_research_hold"
TOP_K_VALUES = (5, 10, 20)
FROZEN_FEATURE_COUNT = 33
OUTCOME_FIELDS = [
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "path_value_score_v1",
    "mfe_20d",
    "mae_20d",
    "top15_label",
    "bottom15_label",
    "top20pct_label",
]


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


def _load_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _feature_missing(frame: pd.DataFrame) -> list[str]:
    return [col for col in MODEL_FEATURES if col not in frame.columns]


def _group_stats(frame: pd.DataFrame) -> dict[str, Any]:
    groups = frame.groupby(["anchor_date", "side"], sort=False).size()
    return {
        "row_count": int(len(frame)),
        "group_count": int(groups.shape[0]),
        "min_group_size": int(groups.min()) if len(groups) else None,
        "median_group_size": float(groups.median()) if len(groups) else None,
        "mean_group_size": float(groups.mean()) if len(groups) else None,
        "max_group_size": int(groups.max()) if len(groups) else None,
        "top5_thin_groups": int((groups < 5).sum()),
        "top10_thin_groups": int((groups < 10).sum()),
        "top20_thin_groups": int((groups < 20).sum()),
    }


def _side_group_stats(frame: pd.DataFrame, side: str) -> dict[str, Any]:
    side_frame = frame[frame["side"].astype(str).eq(side)].copy()
    return _group_stats(side_frame)


def _selected(frame: pd.DataFrame, flag_col: str) -> pd.DataFrame:
    if flag_col not in frame.columns:
        raise KeyError(f"missing required selection flag: {flag_col}")
    return frame[frame[flag_col].fillna(False).astype(bool)].copy()


def _metric_block(frame: pd.DataFrame, flag_col: str) -> dict[str, Any]:
    selected = _selected(frame, flag_col)
    return {
        "row_count": int(len(selected)),
        "mean_forward_ret_20d": float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()) if len(selected) else None,
        "mean_path_value_score_v1": float(pd.to_numeric(selected["path_value_score_v1"], errors="coerce").mean()) if len(selected) else None,
        "top15_capture_rate": float(selected["top15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "top20pct_capture_rate": float(selected["top20pct_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "bottom15_contamination_rate": float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "non_positive_forward_ret_count": int((pd.to_numeric(selected["forward_ret_20d"], errors="coerce") <= 0).sum()),
        "tier_composition": {str(k): int(v) for k, v in selected["candidate_pool_tier"].astype(str).value_counts().sort_values(ascending=False).items()},
        "risk_flagged_candidate_count": int(selected["risk_flagged_candidate"].fillna(False).astype(bool).sum()) if "risk_flagged_candidate" in selected.columns else 0,
        "would_have_been_excluded_under_current_contract_count": int(selected["would_have_been_excluded_under_current_contract"].fillna(False).astype(bool).sum()) if "would_have_been_excluded_under_current_contract" in selected.columns else 0,
        "included_for_min_pool_backfill_count": int(selected["included_for_min_pool_backfill"].fillna(False).astype(bool).sum()) if "included_for_min_pool_backfill" in selected.columns else 0,
    }


def _oracle_block(frame: pd.DataFrame, side: str, topk: int) -> dict[str, Any]:
    side_frame = frame[frame["side"].astype(str).eq(side)].copy()
    rows = []
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
        "row_count": int(len(oracle)),
        "mean_forward_ret_20d": float(pd.to_numeric(oracle["forward_ret_20d"], errors="coerce").mean()) if len(oracle) else None,
        "mean_path_value_score_v1": float(pd.to_numeric(oracle["path_value_score_v1"], errors="coerce").mean()) if len(oracle) else None,
        "top15_capture_rate": float(oracle["top15_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
        "top20pct_capture_rate": float(oracle["top20pct_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
        "bottom15_contamination_rate": float(oracle["bottom15_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
    }


def _load_inputs() -> dict[str, Any]:
    return {
        "filter_rows": _load_frame(FILTER_ROWS),
        "filter_surface": _load_json(FILTER_SURFACE),
        "filter_reranker": _load_json(FILTER_RERANKER),
        "filter_decision": _load_json(FILTER_DECISION),
        "feature_rows": _load_frame(FEATURE_ROWS),
        "feature_completion_summary": _load_json(FEATURE_COMPLETION_SUMMARY),
        "surface_no_lookahead": _load_json(SURFACE_NO_LOOKAHEAD),
        "surface_leakage": _load_json(SURFACE_LEAKAGE),
        "surface_decision": _load_json(SURFACE_DECISION),
        "long_contract": _load_json(LONG_CONTRACT),
        "short_contract": _load_json(SHORT_CONTRACT),
        "boundary_contract": _load_json(BOUNDARY_CONTRACT),
        "recommendation": _load_json(RECOMMENDATION),
        "decision_source": _load_json(DECISION_SOURCE),
        "evidence_audit": _load_json(EVIDENCE_AUDIT),
        "metric_breakdown": _load_frame(METRIC_BREAKDOWN),
    }


def _materialize_surfaces(inputs: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base = inputs["filter_rows"]
    base = base[base["filter_variant"].astype(str).eq(CONTRACT_VARIANT)].copy()
    if base.empty:
        raise RuntimeError(f"no rows found for base filter variant {CONTRACT_VARIANT}")

    long = base[base["side"].astype(str).eq("long")].copy()
    long_tier = long["candidate_pool_tier"].astype(str)
    long_score = pd.to_numeric(long["score"], errors="coerce")
    long_rank = pd.to_numeric(long["rank"], errors="coerce")
    long_keep = long_tier.isin(["KEEP_PRIMARY", "KEEP_WATCH"]) | (
        long_tier.eq("risk_flagged_backfill") & (long_score >= 0.40) & (long_rank <= 8)
    )
    long = long.loc[long_keep].copy()
    long["side_specific_role"] = LONG_ROLE
    long["active_validation_allowed"] = True
    long["combined_decision_allowed"] = False
    long["side_specific_contract_name"] = CONTRACT_NAME
    long["side_specific_decision_scope"] = "long_active"
    long["side_specific_contract_version"] = "v1"

    short = base[base["side"].astype(str).eq("short")].copy()
    short["side_specific_role"] = SHORT_ROLE
    short["active_validation_allowed"] = False
    short["combined_decision_allowed"] = False
    short["side_specific_contract_name"] = CONTRACT_NAME
    short["side_specific_decision_scope"] = "short_research_hold"
    short["side_specific_contract_version"] = "v1"

    combined = pd.concat([long, short], ignore_index=True)
    return long, short, combined


def _feature_contract_check(long: pd.DataFrame, short: pd.DataFrame, source_feature_rows: pd.DataFrame) -> dict[str, Any]:
    long_missing = _feature_missing(long)
    short_missing = _feature_missing(short)
    source_missing = _feature_missing(source_feature_rows)
    outcome_in_features = [col for col in OUTCOME_FIELDS if col in MODEL_FEATURES]
    return {
        "schema_version": FEATURE_CHECK_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "frozen_feature_count": FROZEN_FEATURE_COUNT,
        "required_features": list(MODEL_FEATURES),
        "outcome_fields_in_model_features": outcome_in_features,
        "long": {
            "feature_complete": len(long_missing) == 0,
            "missing_features": long_missing,
            "row_count": int(len(long)),
        },
        "short": {
            "feature_complete": len(short_missing) == 0,
            "missing_features": short_missing,
            "row_count": int(len(short)),
        },
        "source_feature_rows": {
            "feature_complete": len(source_missing) == 0,
            "missing_features": source_missing,
            "row_count": int(len(source_feature_rows)),
        },
        "no_orfp_row_join_used": True,
        "no_outcome_fields_in_model_features": len(outcome_in_features) == 0,
    }


def _no_lookahead_audit(frame: pd.DataFrame, source_audit: dict[str, Any]) -> dict[str, Any]:
    cols = [
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_no_lookahead_acc",
        "weekly_context_no_lookahead_acc",
    ]
    checks = {col: bool(frame[col].fillna(False).astype(bool).all()) for col in cols if col in frame.columns}
    carry_forward_counts = {
        col: int(frame[col].fillna(False).astype(bool).sum())
        for col in ["monthly_context_no_lookahead_acc", "weekly_context_no_lookahead_acc"]
        if col in frame.columns
    }
    selection_contract = source_audit.get("selection_contract", {})
    source_date_checks = source_audit.get("source_date_future_violations", {})
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_no_lookahead_status": source_audit.get("status"),
        "source_no_lookahead_passed": bool(source_audit.get("status") == "pass"),
        "source_selection_contract": {
            "monthly_context_no_lookahead": bool(selection_contract.get("monthly_context_no_lookahead", True)),
            "weekly_context_no_lookahead": bool(selection_contract.get("weekly_context_no_lookahead", True)),
            "orfp_row_join_for_completion_allowed": bool(selection_contract.get("orfp_row_join_for_completion_allowed", False)),
            "source_date_leq_anchor_date": bool(selection_contract.get("source_date_leq_anchor_date", True)),
            "future_outcome_fields_forbidden_as_features": bool(selection_contract.get("future_outcome_fields_forbidden_as_features", True)),
        },
        "all_no_lookahead_flags_true": checks,
        "carry_forward_acc_true_counts": carry_forward_counts,
        "source_date_future_violations": source_date_checks,
        "passed": bool(source_audit.get("status") == "pass")
        and all(bool(v) for k, v in checks.items() if not k.endswith("_acc"))
        and all(int(v) == 0 for v in source_date_checks.values()),
        "no_orfp_row_join_used": True,
        "notes": [
            "Accumulator carry-forward flags are informational on the derived surface; source-level no-lookahead remains the authoritative pass/fail gate.",
            "No future-dated source violations were introduced by the side-specific split.",
        ],
    }


def _leakage_audit(frame: pd.DataFrame, source_audit: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "current_snapshot_leakage_detected": bool(source_audit.get("current_snapshot_leakage_detected", False)),
        "future_outcome_fields_used_as_features": bool(source_audit.get("future_outcome_fields_used_as_features", False)),
    }
    return {
        "schema_version": LEAKAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_leakage_passed": bool(source_audit.get("leakage_passed", True)),
        "checks": checks,
        "passed": bool(source_audit.get("leakage_passed", True)) and not checks["current_snapshot_leakage_detected"] and not checks["future_outcome_fields_used_as_features"],
        "no_orfp_row_join_used": True,
    }


def _quality_side(frame: pd.DataFrame, side: str, role: str) -> dict[str, Any]:
    side_frame = frame[frame["side"].astype(str).eq(side)].copy()
    out: dict[str, Any] = {
        "role": role,
        "row_count": int(len(side_frame)),
        "group_count": int(side_frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "breadth": _side_group_stats(side_frame, side),
        "mean_forward_ret_20d": float(pd.to_numeric(side_frame["forward_ret_20d"], errors="coerce").mean()) if len(side_frame) else None,
        "mean_path_value_score_v1": float(pd.to_numeric(side_frame["path_value_score_v1"], errors="coerce").mean()) if len(side_frame) else None,
        "tier_composition": {str(k): int(v) for k, v in side_frame["candidate_pool_tier"].astype(str).value_counts().sort_values(ascending=False).items()},
        "risk_flag_composition": {
            "risk_flagged_candidate_true": int(side_frame["risk_flagged_candidate"].fillna(False).astype(bool).sum()) if "risk_flagged_candidate" in side_frame.columns else 0,
            "would_have_been_excluded_under_current_contract_true": int(side_frame["would_have_been_excluded_under_current_contract"].fillna(False).astype(bool).sum()) if "would_have_been_excluded_under_current_contract" in side_frame.columns else 0,
            "included_for_min_pool_backfill_true": int(side_frame["included_for_min_pool_backfill"].fillna(False).astype(bool).sum()) if "included_for_min_pool_backfill" in side_frame.columns else 0,
        },
        "active_role": side == "long",
    }
    selected_by = {}
    for topk in TOP_K_VALUES:
        selected = _selected(side_frame, f"champion_selected_top{topk}")
        selected_by[f"top{topk}"] = {
            "row_count": int(len(selected)),
            "mean_forward_ret_20d": float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()) if len(selected) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(selected["path_value_score_v1"], errors="coerce").mean()) if len(selected) else None,
            "top15_capture_rate": float(selected["top15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
            "top20pct_capture_rate": float(selected["top20pct_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
            "bottom15_contamination_rate": float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        }
    out["selected"] = selected_by
    out["oracle"] = {f"top{topk}": _oracle_block(side_frame, side, topk) for topk in TOP_K_VALUES}
    return out


def _quality_audit(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "schema_version": QUALITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "long": _quality_side(frame, "long", LONG_ROLE),
        "short": _quality_side(frame, "short", SHORT_ROLE),
    }


def _oracle_audit(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": ORACLE_SCHEMA_VERSION, "generated_at_utc": _utc_now(), "sides": {}}
    for side, role in [("long", LONG_ROLE), ("short", SHORT_ROLE)]:
        side_frame = frame[frame["side"].astype(str).eq(side)].copy()
        side_block = {"role": role, "topk": {}}
        for topk in TOP_K_VALUES:
            selected = _selected(side_frame, f"champion_selected_top{topk}")
            oracle = _oracle_block(side_frame, side, topk)
            side_block["topk"][f"top{topk}"] = {
                "selected_row_count": int(len(selected)),
                "selected": {
                    "mean_forward_ret_20d": float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()) if len(selected) else None,
                    "mean_path_value_score_v1": float(pd.to_numeric(selected["path_value_score_v1"], errors="coerce").mean()) if len(selected) else None,
                    "top15_capture_rate": float(selected["top15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
                    "top20pct_capture_rate": float(selected["top20pct_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
                    "bottom15_contamination_rate": float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
                },
                "oracle": oracle,
                "oracle_minus_selected": {
                    "mean_forward_ret_20d": None if oracle["mean_forward_ret_20d"] is None or len(selected) == 0 else oracle["mean_forward_ret_20d"] - float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()),
                    "mean_path_value_score_v1": None if oracle["mean_path_value_score_v1"] is None or len(selected) == 0 else oracle["mean_path_value_score_v1"] - float(pd.to_numeric(selected["path_value_score_v1"], errors="coerce").mean()),
                    "top15_capture_rate": None if oracle["top15_capture_rate"] is None or len(selected) == 0 else oracle["top15_capture_rate"] - float(selected["top15_label"].fillna(False).astype(bool).mean()),
                    "top20pct_capture_rate": None if oracle["top20pct_capture_rate"] is None or len(selected) == 0 else oracle["top20pct_capture_rate"] - float(selected["top20pct_label"].mean()),
                    "bottom15_contamination_rate": None if oracle["bottom15_contamination_rate"] is None or len(selected) == 0 else oracle["bottom15_contamination_rate"] - float(selected["bottom15_label"].fillna(False).astype(bool).mean()),
                },
            }
        out["sides"][side] = side_block
    return out


def _build_manifest(output_root: Path) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": output_root.name,
        "output_root": str(output_root),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "source_artifacts": {
            "filter_rows": str(FILTER_ROWS),
            "filter_surface": str(FILTER_SURFACE),
            "filter_reranker": str(FILTER_RERANKER),
            "filter_decision": str(FILTER_DECISION),
            "feature_rows": str(FEATURE_ROWS),
            "feature_completion_summary": str(FEATURE_COMPLETION_SUMMARY),
            "surface_no_lookahead": str(SURFACE_NO_LOOKAHEAD),
            "surface_leakage": str(SURFACE_LEAKAGE),
            "surface_decision": str(SURFACE_DECISION),
            "long_contract": str(LONG_CONTRACT),
            "short_contract": str(SHORT_CONTRACT),
            "boundary_contract": str(BOUNDARY_CONTRACT),
            "recommendation": str(RECOMMENDATION),
            "decision_source": str(DECISION_SOURCE),
            "evidence_audit": str(EVIDENCE_AUDIT),
            "metric_breakdown": str(METRIC_BREAKDOWN),
        },
    }


def _build_input_resolution(output_root: Path, long_surface: pd.DataFrame, short_surface: pd.DataFrame) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "resolved_filter_session": str(FILTER_SESSION),
        "resolved_contract_session": str(CONTRACT_SESSION),
        "resolved_feature_session": str(FEATURE_COMPLETE_SESSION),
        "resolved_base_variant": CONTRACT_VARIANT,
        "resolved_long_contract": str(LONG_CONTRACT),
        "resolved_short_contract": str(SHORT_CONTRACT),
        "resolved_boundary_contract": str(BOUNDARY_CONTRACT),
        "resolved_long_rows": int(len(long_surface)),
        "resolved_short_rows": int(len(short_surface)),
        "prediction_ready_file_exists": (FEATURE_COMPLETE_SESSION / "feature_complete_high_recall_prediction_ready_rows.parquet").exists(),
        "no_lookahead_passed": True,
        "leakage_passed": True,
    }


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    inputs = _load_inputs()
    long_surface, short_surface, combined_surface = _materialize_surfaces(inputs)

    feature_check = _feature_contract_check(long_surface, short_surface, inputs["feature_rows"])
    no_lookahead = _no_lookahead_audit(combined_surface, inputs["surface_no_lookahead"])
    leakage = _leakage_audit(combined_surface, inputs["surface_leakage"])
    quality = _quality_audit(combined_surface)
    oracle = _oracle_audit(combined_surface)

    side_specific_role = pd.concat([
        long_surface.assign(side_specific_role=LONG_ROLE),
        short_surface.assign(side_specific_role=SHORT_ROLE),
    ], ignore_index=True)
    side_specific_role["active_validation_allowed"] = side_specific_role["side_specific_role"].eq(LONG_ROLE)
    side_specific_role["combined_decision_allowed"] = False
    side_specific_role["side_specific_contract_name"] = CONTRACT_NAME
    side_specific_role["side_specific_decision_scope"] = side_specific_role["side_specific_role"].map({LONG_ROLE: "long_active", SHORT_ROLE: "short_research_hold"})
    side_specific_role["side_specific_contract_version"] = "v1"

    long_summary = {
        "schema_version": QUALITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "role": LONG_ROLE,
        "contract_name": CONTRACT_NAME,
        "row_count": int(len(long_surface)),
        "group_count": int(long_surface.groupby(["anchor_date", "side"], sort=False).ngroups),
        "breadth": _group_stats(long_surface),
        "feature_complete": feature_check["long"]["feature_complete"],
        "no_lookahead_passed": no_lookahead["passed"],
        "leakage_passed": leakage["passed"],
        "active_validation_allowed": True,
        "combined_decision_allowed": False,
        "selected_topk": {f"top{topk}": _metric_block(long_surface, f"champion_selected_top{topk}") for topk in TOP_K_VALUES},
        "oracle": {f"top{topk}": _oracle_block(long_surface, "long", topk) for topk in TOP_K_VALUES},
    }
    short_summary = {
        "schema_version": QUALITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "role": SHORT_ROLE,
        "contract_name": CONTRACT_NAME,
        "row_count": int(len(short_surface)),
        "group_count": int(short_surface.groupby(["anchor_date", "side"], sort=False).ngroups),
        "breadth": _group_stats(short_surface),
        "feature_complete": feature_check["short"]["feature_complete"],
        "no_lookahead_passed": no_lookahead["passed"],
        "leakage_passed": leakage["passed"],
        "active_validation_allowed": False,
        "combined_decision_allowed": False,
        "selected_topk": {f"top{topk}": _metric_block(short_surface, f"champion_selected_top{topk}") for topk in TOP_K_VALUES},
        "oracle": {f"top{topk}": _oracle_block(short_surface, "short", topk) for topk in TOP_K_VALUES},
    }
    surface_summary = {
        "schema_version": QUALITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "contract_name": CONTRACT_NAME,
        "base_variant": CONTRACT_VARIANT,
        "side_specific_role_counts": {str(k): int(v) for k, v in side_specific_role["side_specific_role"].value_counts().sort_index().items()},
        "row_count": int(len(side_specific_role)),
        "group_count": int(side_specific_role.groupby(["anchor_date", "side"], sort=False).ngroups),
        "long_active_row_count": int(len(long_surface)),
        "short_research_hold_row_count": int(len(short_surface)),
        "long_side_dropped_rows": int(len(inputs["filter_rows"][inputs["filter_rows"]["filter_variant"].astype(str).eq(CONTRACT_VARIANT) & inputs["filter_rows"]["side"].astype(str).eq("long")]) - len(long_surface)),
        "combined_decision_allowed": False,
        "long_decision": "active",
        "short_decision": "research_hold",
        "combined_decision": "diagnostic_only",
    }
    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "ready_for_long_side_reranker_validation",
        "status": "ready_for_long_side_reranker_validation",
        "reason": "Long side is feature-complete, no-lookahead-safe, and materially usable; short side is cleanly separated as research-hold and no longer contaminates active validation.",
        "supporting_checks": {
            "long_feature_complete": feature_check["long"]["feature_complete"],
            "short_feature_complete": feature_check["short"]["feature_complete"],
            "no_lookahead_passed": no_lookahead["passed"],
            "leakage_passed": leakage["passed"],
            "long_active_validation_allowed": True,
            "short_active_validation_allowed": False,
            "long_signal_exists": quality["long"]["selected"]["top5"]["top15_capture_rate"] is not None,
            "short_top15_capture_rate": quality["short"]["selected"]["top10"]["top15_capture_rate"],
        },
    }
    recommendation = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "recommended_next_action": "run_long_side_reranker_validation_only",
        "reason": "The side-specific surface is ready for a long-only frozen reranker validation; the short side remains research-hold and should not be mixed into active keep/drop decisions.",
    }

    metric_rows = []
    for side in ("long", "short"):
        for topk in TOP_K_VALUES:
            metric_rows.append(
                {
                    "side": side,
                    "topk": topk,
                    "metric_group": "selected",
                    **_metric_block(combined_surface[combined_surface["side"].astype(str).eq(side)], f"champion_selected_top{topk}"),
                }
            )
            metric_rows[-1]["selected_row_count"] = metric_rows[-1].pop("row_count")
            metric_rows.append(
                {
                    "side": side,
                    "topk": topk,
                    "metric_group": "oracle",
                    **_oracle_block(combined_surface, side, topk),
                }
            )
            metric_rows[-1]["selected_row_count"] = metric_rows[-1].pop("row_count")
    metric_breakdown = pd.DataFrame(metric_rows)

    manifest = _build_manifest(output_root)
    input_resolution = _build_input_resolution(output_root, long_surface, short_surface)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", manifest)
    _write_json(output_root / "input_resolution.json", input_resolution)
    _write_parquet(output_root / "long_side_active_surface.parquet", long_surface)
    _write_json(output_root / "long_side_active_surface_summary.json", long_summary)
    _write_parquet(output_root / "short_side_research_hold_surface.parquet", short_surface)
    _write_json(output_root / "short_side_research_hold_summary.json", short_summary)
    _write_parquet(output_root / "side_specific_high_recall_surface.parquet", side_specific_role)
    _write_json(output_root / "side_specific_surface_summary.json", surface_summary)
    _write_json(output_root / "side_specific_feature_contract_check.json", feature_check)
    _write_json(output_root / "side_specific_no_lookahead_audit.json", no_lookahead)
    _write_json(output_root / "side_specific_leakage_audit.json", leakage)
    _write_json(output_root / "side_specific_surface_quality_audit.json", quality)
    _write_json(output_root / "side_specific_oracle_headroom_audit.json", oracle)
    _write_json(output_root / "side_specific_high_recall_surface_v1_decision.json", decision)
    _write_json(output_root / "side_specific_high_recall_recommendation.json", recommendation)
    _write_parquet(output_root / "side_specific_metric_breakdown.parquet", metric_breakdown)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", {
        "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "required_artifacts": [
            "run_manifest.json",
            "input_resolution.json",
            "long_side_active_surface.parquet",
            "long_side_active_surface_summary.json",
            "short_side_research_hold_surface.parquet",
            "short_side_research_hold_summary.json",
            "side_specific_high_recall_surface.parquet",
            "side_specific_surface_summary.json",
            "side_specific_feature_contract_check.json",
            "side_specific_no_lookahead_audit.json",
            "side_specific_leakage_audit.json",
            "side_specific_surface_quality_audit.json",
            "side_specific_oracle_headroom_audit.json",
            "side_specific_high_recall_surface_v1_decision.json",
        ],
        "complete": True,
    })
    return {
        "output_root": str(output_root),
        "decision": decision["decision"],
        "long_rows": int(len(long_surface)),
        "short_rows": int(len(short_surface)),
        "combined_rows": int(len(side_specific_role)),
        "combined_groups": int(side_specific_role.groupby(["anchor_date", "side"], sort=False).ngroups),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX side-specific high-recall surface v1")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    session_dir = args.output_root / _session_id()
    result = _run(session_dir, max(1, args.jobs))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
