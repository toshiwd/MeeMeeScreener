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

from scripts.tradex_shadow_feature_reranker_feasibility_v1 import MODEL_FEATURES, _rank_within_groups


warnings.filterwarnings("ignore", category=FutureWarning)

SCRIPT_NAME = "tradex_long_side_filter_revision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_long_side_filter_revision_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_long_side_filter_revision_v1_input_resolution_v1"
FAILURE_AUDIT_SCHEMA_VERSION = "tradex_long_side_filter_revision_v1_selected_row_failure_audit_v1"
CONTRACTS_SCHEMA_VERSION = "tradex_long_side_filter_revision_v1_contracts_v1"
SURFACE_COMPARISON_SCHEMA_VERSION = "tradex_long_side_filter_revision_v1_surface_comparison_v1"
RERANKER_COMPARISON_SCHEMA_VERSION = "tradex_long_side_filter_revision_v1_reranker_comparison_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_long_side_filter_revision_v1_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_long_side_filter_revision_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_long_side_filter_revision_v1_artifact_complete_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\long_side_filter_revision_v1")
LONG_SURFACE_SESSION = Path(r"G:\Tradex\side_specific_high_recall_surface_v1\20260502T151044Z-324144")
LONG_RERANKER_SESSION = Path(r"G:\Tradex\long_side_reranker_validation_v1\20260502T151756Z-703876")
MIXED_FILTER_SESSION = Path(r"G:\Tradex\high_recall_filter_revision_v1\20260502T144909Z-320427")

LONG_SURFACE = LONG_SURFACE_SESSION / "long_side_active_surface.parquet"
LONG_SURFACE_SUMMARY = LONG_SURFACE_SESSION / "long_side_active_surface_summary.json"
FEATURE_CHECK = LONG_SURFACE_SESSION / "side_specific_feature_contract_check.json"
NO_LOOKAHEAD = LONG_SURFACE_SESSION / "side_specific_no_lookahead_audit.json"
LEAKAGE = LONG_SURFACE_SESSION / "side_specific_leakage_audit.json"
QUALITY_AUDIT = LONG_SURFACE_SESSION / "side_specific_surface_quality_audit.json"
ORACLE_HEADROOM = LONG_SURFACE_SESSION / "side_specific_oracle_headroom_audit.json"
SURFACE_DECISION = LONG_SURFACE_SESSION / "side_specific_high_recall_surface_v1_decision.json"

RERANKER_INPUT_VALIDATION = LONG_RERANKER_SESSION / "long_side_reranker_input_validation.json"
RERANKER_PREDICTION_ROWS = LONG_RERANKER_SESSION / "long_side_reranker_prediction_rows.parquet"
RERANKER_VARIANT_COMPARISON = LONG_RERANKER_SESSION / "long_side_reranker_variant_pool_comparison.json"
RERANKER_MEMBERSHIP_DIFF = LONG_RERANKER_SESSION / "long_side_reranker_topk_membership_diff.parquet"
RERANKER_ORACLE_GAP = LONG_RERANKER_SESSION / "long_side_oracle_gap_comparison.json"
RERANKER_FAILURE = LONG_RERANKER_SESSION / "long_side_reranker_failure_mode_audit.json"
RERANKER_DECISION = LONG_RERANKER_SESSION / "long_side_reranker_validation_v1_decision.json"
RERANKER_TIER_SUMMARY = LONG_RERANKER_SESSION / "long_side_reranker_tier_summary.parquet"
RERANKER_GROUP_SUMMARY = LONG_RERANKER_SESSION / "long_side_reranker_group_summary.parquet"

REFERENCE_FILTER_SURFACE = MIXED_FILTER_SESSION / "high_recall_filter_revision_surface_comparison.json"
REFERENCE_FILTER_RERANKER = MIXED_FILTER_SESSION / "high_recall_filter_revision_reranker_comparison.json"
REFERENCE_FILTER_ROWS = MIXED_FILTER_SESSION / "high_recall_filter_revision_rows.parquet"
REFERENCE_FILTER_DECISION = MIXED_FILTER_SESSION / "high_recall_filter_revision_v1_decision.json"

TOP_K_VALUES = (5, 10, 20)
MODEL_NAME = "tree_hgb_path_value"
MODEL_SCORE_COL = f"{MODEL_NAME}_score"
BASE_MODEL_RANK_COL = f"{MODEL_NAME}_rank"
CHAMPION_SCORE_COL = "champion_score"
CHAMPION_RANK_COL = "champion_rank"
FILTER_VARIANT_COL = "long_filter_revision_variant"
FILTER_SELECTED_COL = "long_filter_revision_selected"
VARIANT_MODEL_RANK_COL = "variant_tree_hgb_path_value_rank"
VARIANT_CHAMPION_RANK_COL = "variant_champion_rank"

LONG_FILTER_VARIANTS = (
    "long_filter_primary_watch_only",
    "long_filter_score_040_rank8",
    "long_filter_score_045_rank8",
    "long_filter_score_040_rank5",
    "long_filter_score_045_rank5",
    "long_filter_top15_candidate_guard",
)

CURRENT_BASELINE_VARIANT = "long_filter_score_040_rank8"
SEVERE_DIAGNOSTIC_FIELDS = [
    "stable_bad_pick_family",
    "bad_pick_diagnostic_present",
    "hard_bad_pick_family",
    "severe_bad_pick",
    "severe_bad_pick_family",
    "severe_diagnostic",
    "diagnostic_severe_flag",
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


def _dedupe_columns(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[:, ~frame.columns.duplicated()].copy()


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


def _load_inputs() -> dict[str, Any]:
    paths = {
        "long_surface": LONG_SURFACE,
        "long_surface_summary": LONG_SURFACE_SUMMARY,
        "feature_check": FEATURE_CHECK,
        "no_lookahead": NO_LOOKAHEAD,
        "leakage": LEAKAGE,
        "quality_audit": QUALITY_AUDIT,
        "oracle_headroom": ORACLE_HEADROOM,
        "surface_decision": SURFACE_DECISION,
        "reranker_input_validation": RERANKER_INPUT_VALIDATION,
        "reranker_prediction_rows": RERANKER_PREDICTION_ROWS,
        "reranker_variant_comparison": RERANKER_VARIANT_COMPARISON,
        "reranker_membership_diff": RERANKER_MEMBERSHIP_DIFF,
        "reranker_oracle_gap": RERANKER_ORACLE_GAP,
        "reranker_failure": RERANKER_FAILURE,
        "reranker_decision": RERANKER_DECISION,
        "reranker_tier_summary": RERANKER_TIER_SUMMARY,
        "reranker_group_summary": RERANKER_GROUP_SUMMARY,
        "reference_filter_surface": REFERENCE_FILTER_SURFACE,
        "reference_filter_reranker": REFERENCE_FILTER_RERANKER,
        "reference_filter_rows": REFERENCE_FILTER_ROWS,
        "reference_filter_decision": REFERENCE_FILTER_DECISION,
    }
    for label, path in paths.items():
        _ensure_exists(path, label)
    long_surface = _dedupe_columns(_load_frame(LONG_SURFACE))
    reranker_rows = _dedupe_columns(_load_frame(RERANKER_PREDICTION_ROWS))
    if len(long_surface) != len(reranker_rows):
        raise RuntimeError(f"row count mismatch between long surface and reranker rows: {len(long_surface)} != {len(reranker_rows)}")
    return {
        "paths": paths,
        "long_surface": long_surface,
        "reranker_rows": reranker_rows,
        "long_surface_summary": _load_json(LONG_SURFACE_SUMMARY),
        "feature_check": _load_json(FEATURE_CHECK),
        "no_lookahead": _load_json(NO_LOOKAHEAD),
        "leakage": _load_json(LEAKAGE),
        "quality_audit": _load_json(QUALITY_AUDIT),
        "oracle_headroom": _load_json(ORACLE_HEADROOM),
        "surface_decision": _load_json(SURFACE_DECISION),
        "reranker_input_validation": _load_json(RERANKER_INPUT_VALIDATION),
        "reranker_variant_comparison": _load_json(RERANKER_VARIANT_COMPARISON),
        "reranker_membership_diff": _load_frame(RERANKER_MEMBERSHIP_DIFF),
        "reranker_oracle_gap": _load_json(RERANKER_ORACLE_GAP),
        "reranker_failure": _load_json(RERANKER_FAILURE),
        "reranker_decision": _load_json(RERANKER_DECISION),
        "reranker_tier_summary": _load_frame(RERANKER_TIER_SUMMARY),
        "reranker_group_summary": _load_frame(RERANKER_GROUP_SUMMARY),
        "reference_filter_surface": _load_json(REFERENCE_FILTER_SURFACE),
        "reference_filter_reranker": _load_json(REFERENCE_FILTER_RERANKER),
        "reference_filter_rows": _dedupe_columns(_load_frame(REFERENCE_FILTER_ROWS)),
        "reference_filter_decision": _load_json(REFERENCE_FILTER_DECISION),
    }


def _selected(frame: pd.DataFrame, flag_col: str) -> pd.DataFrame:
    if flag_col not in frame.columns:
        raise KeyError(f"missing required selection flag: {flag_col}")
    return frame[frame[flag_col].fillna(False).astype(bool)].copy()


def _metric_block(frame: pd.DataFrame, flag_col: str) -> dict[str, Any]:
    selected = _selected(frame, flag_col)
    forward = pd.to_numeric(selected["forward_ret_20d"], errors="coerce")
    path = pd.to_numeric(selected["path_value_score_v1"], errors="coerce")
    return {
        "row_count": int(len(selected)),
        "mean_forward_ret_20d": float(forward.mean()) if len(selected) else None,
        "mean_path_value_score_v1": float(path.mean()) if len(selected) else None,
        "top15_capture_rate": float(selected["top15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "top20pct_capture_rate": float(selected["top20pct_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "bottom15_contamination_rate": float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        "non_positive_forward_ret_count": int((forward.notna() & (forward <= 0)).sum()),
    }


def _oracle_block(frame: pd.DataFrame, topk: int, *, ranking_score_col: str = "forward_ret_20d") -> dict[str, Any]:
    eligible = frame[pd.to_numeric(frame["forward_ret_20d"], errors="coerce").notna()].copy()
    rows = []
    for _, group in eligible.groupby(["anchor_date", "side"], sort=False):
        g = group.sort_values(
            [ranking_score_col, "path_value_score_v1", "mae_20d", "candidate_idx"],
            ascending=[False, False, True, True],
            kind="mergesort",
        )
        rows.append(g.head(topk))
    oracle = pd.concat(rows, ignore_index=True) if rows else eligible.iloc[0:0].copy()
    forward = pd.to_numeric(oracle["forward_ret_20d"], errors="coerce")
    path = pd.to_numeric(oracle["path_value_score_v1"], errors="coerce")
    return {
        "row_count": int(len(oracle)),
        "mean_forward_ret_20d": float(forward.mean()) if len(oracle) else None,
        "mean_path_value_score_v1": float(path.mean()) if len(oracle) else None,
        "top15_capture_rate": float(oracle["top15_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
        "top20pct_capture_rate": float(oracle["top20pct_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
        "bottom15_contamination_rate": float(oracle["bottom15_label"].fillna(False).astype(bool).mean()) if len(oracle) else None,
    }


def _rank_variant(frame: pd.DataFrame, score_col: str, group_cols: list[str]) -> pd.Series:
    return _rank_within_groups(frame, pd.to_numeric(frame[score_col], errors="coerce"), group_cols=group_cols)


def _variant_mask(frame: pd.DataFrame, variant: str) -> tuple[pd.Series, dict[str, Any]]:
    tier = frame["candidate_pool_tier"].astype(str)
    score = pd.to_numeric(frame["score"], errors="coerce") if "score" in frame.columns else pd.to_numeric(frame["candidate_score"], errors="coerce")
    rank = pd.to_numeric(frame["rank"], errors="coerce") if "rank" in frame.columns else pd.to_numeric(frame["candidate_rank"], errors="coerce")
    severe_present = [c for c in SEVERE_DIAGNOSTIC_FIELDS if c in frame.columns]
    severe_mask = pd.Series(False, index=frame.index)
    if severe_present:
        for col in severe_present:
            severe_mask = severe_mask | frame[col].fillna(False).astype(bool)
    backfill = tier.eq("risk_flagged_backfill")
    primary_watch = tier.isin(["KEEP_PRIMARY", "KEEP_WATCH"])

    if variant == "long_filter_primary_watch_only":
        mask = primary_watch
        reason = "keep_primary_watch_only"
    elif variant == "long_filter_score_040_rank8":
        mask = primary_watch | (backfill & (score >= 0.40) & (rank <= 8))
        reason = "baseline_score_040_rank8"
    elif variant == "long_filter_score_045_rank8":
        mask = primary_watch | (backfill & (score >= 0.45) & (rank <= 8))
        reason = "stricter_score_045_rank8"
    elif variant == "long_filter_score_040_rank5":
        mask = primary_watch | (backfill & (score >= 0.40) & (rank <= 5))
        reason = "stricter_rank5_score_040"
    elif variant == "long_filter_score_045_rank5":
        mask = primary_watch | (backfill & (score >= 0.45) & (rank <= 5))
        reason = "stricter_score_045_rank5"
    elif variant == "long_filter_top15_candidate_guard":
        mask = primary_watch | (backfill & (score >= 0.40) & (rank <= 8) & ~severe_mask)
        reason = "score_rank_guard_plus_severe_diag_exclusion"
    else:
        raise KeyError(f"unknown filter variant: {variant}")
    return mask.fillna(False).astype(bool), {
        "variant": variant,
        "reason": reason,
        "backfill_guard_count": int((backfill & mask).sum()),
        "primary_watch_count": int((primary_watch & mask).sum()),
        "severe_diagnostic_fields_present": severe_present,
        "severe_diagnostic_rows": int(severe_mask.sum()),
        "backfill_rows": int(backfill.sum()),
        "primary_watch_rows": int(primary_watch.sum()),
    }


def _variant_surface(frame: pd.DataFrame, variant: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    mask, meta = _variant_mask(frame, variant)
    selected = frame.loc[mask].copy()
    selected[FILTER_VARIANT_COL] = variant
    selected[FILTER_SELECTED_COL] = True
    selected["long_filter_revision_reason"] = meta["reason"]
    selected["long_filter_revision_variant"] = variant
    selected["long_filter_revision_backfill_guard_count"] = int(meta["backfill_guard_count"])
    selected["long_filter_revision_primary_watch_count"] = int(meta["primary_watch_count"])
    selected["long_filter_revision_severe_diagnostic_rows"] = int(meta["severe_diagnostic_rows"])
    selected["long_filter_revision_variant_selected"] = True
    selected["variant_allowed_backfill"] = selected["candidate_pool_tier"].astype(str).eq("risk_flagged_backfill")
    selected["variant_allowed_primary_watch"] = selected["candidate_pool_tier"].astype(str).isin(["KEEP_PRIMARY", "KEEP_WATCH"])
    selected[VARIANT_MODEL_RANK_COL] = _rank_variant(selected, MODEL_SCORE_COL, ["anchor_date", "side"])
    selected[VARIANT_CHAMPION_RANK_COL] = _rank_variant(selected, CHAMPION_SCORE_COL, ["anchor_date", "side"])
    for topk in TOP_K_VALUES:
        selected[f"variant_model_selected_top{topk}"] = selected[VARIANT_MODEL_RANK_COL] <= topk
        selected[f"variant_champion_selected_top{topk}"] = selected[VARIANT_CHAMPION_RANK_COL] <= topk
    return selected, meta


def _selection_metrics(frame: pd.DataFrame, model_rank_col: str, champion_rank_col: str, topk: int) -> dict[str, Any]:
    model_selected = frame[model_rank_col] <= topk
    champ_selected = frame[champion_rank_col] <= topk
    model_frame = frame.loc[model_selected].copy()
    champ_frame = frame.loc[champ_selected].copy()
    forward = pd.to_numeric(model_frame["forward_ret_20d"], errors="coerce")
    path = pd.to_numeric(model_frame["path_value_score_v1"], errors="coerce")
    champ_forward = pd.to_numeric(champ_frame["forward_ret_20d"], errors="coerce")
    champ_path = pd.to_numeric(champ_frame["path_value_score_v1"], errors="coerce")
    overlap = int((model_selected & champ_selected).sum())
    union = int((model_selected | champ_selected).sum())
    return {
        "selected_row_count": int(len(model_frame)),
        "champion_selected_row_count": int(len(champ_frame)),
        "mean_forward_ret_20d": float(forward.mean()) if len(model_frame) else None,
        "champion_mean_forward_ret_20d": float(champ_forward.mean()) if len(champ_frame) else None,
        "mean_path_value_score_v1": float(path.mean()) if len(model_frame) else None,
        "champion_mean_path_value_score_v1": float(champ_path.mean()) if len(champ_frame) else None,
        "top15_capture_rate": float(model_frame["top15_label"].fillna(False).astype(bool).mean()) if len(model_frame) else None,
        "champion_top15_capture_rate": float(champ_frame["top15_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
        "top20pct_capture_rate": float(model_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(model_frame) else None,
        "champion_top20pct_capture_rate": float(champ_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
        "bottom15_contamination_rate": float(model_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(model_frame) else None,
        "champion_bottom15_contamination_rate": float(champ_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(champ_frame) else None,
        "non_positive_forward_ret_count": int((forward.notna() & (forward <= 0)).sum()),
        "champion_non_positive_forward_ret_count": int((champ_forward.notna() & (champ_forward <= 0)).sum()),
        "membership_changed_count": int((model_selected ^ champ_selected).sum()),
        "overlap_ratio": float(overlap / union) if union else None,
        "zero_pass_groups": int(sum(1 for _, g in model_frame.groupby(["anchor_date", "side"], sort=False) if not g["top15_label"].fillna(False).astype(bool).any())),
        "tier_composition": {str(k): int(v) for k, v in model_frame["candidate_pool_tier"].value_counts().items()},
        "risk_flagged_backfill_count": int((model_frame["candidate_pool_tier"].astype(str) == "risk_flagged_backfill").sum()),
        "risk_flagged_backfill_share": float((model_frame["candidate_pool_tier"].astype(str) == "risk_flagged_backfill").mean()) if len(model_frame) else None,
        "side_split": {str(k): int(v) for k, v in model_frame["side"].value_counts().items()},
        "symbol_concentration_top1": float(model_frame["symbol"].value_counts(normalize=True).iloc[0]) if len(model_frame) else None,
        "symbol_concentration_top3": float(model_frame["symbol"].value_counts(normalize=True).head(3).sum()) if len(model_frame) else None,
    }


def _selected_row_failure_audit(frame: pd.DataFrame) -> dict[str, Any]:
    audit = {
        "schema_version": FAILURE_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "model_name": MODEL_NAME,
        "selected_by_topk": {},
        "selected_by_topk_tier_detail": {},
        "selected_by_topk_reason_detail": {},
        "selected_by_topk_guard_detail": {},
        "notes": [
            "This audit is based on the frozen tree_hgb_path_value replay on the cleaned long-active surface.",
            "The selected rows are examined before any filter revision is applied.",
        ],
    }
    for topk in TOP_K_VALUES:
        model_selected = frame[BASE_MODEL_RANK_COL] <= topk
        selected = frame.loc[model_selected].copy()
        audit["selected_by_topk"][f"top{topk}"] = {
            "selected_row_count": int(len(selected)),
            "tier_counts": {str(k): int(v) for k, v in selected["candidate_pool_tier"].value_counts().items()},
            "candidate_pool_reason_counts": {str(k): int(v) for k, v in selected["candidate_pool_reason"].value_counts().items()},
            "risk_flagged_candidate_counts": {str(k): int(v) for k, v in selected["risk_flagged_candidate"].value_counts(dropna=False).items()},
            "included_for_min_pool_backfill_counts": {str(k): int(v) for k, v in selected["included_for_min_pool_backfill"].value_counts(dropna=False).items()},
            "would_have_been_excluded_under_current_contract_counts": {str(k): int(v) for k, v in selected["would_have_been_excluded_under_current_contract"].value_counts(dropna=False).items()},
            "mean_forward_ret_20d": float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()) if len(selected) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(selected["path_value_score_v1"], errors="coerce").mean()) if len(selected) else None,
            "top15_capture_rate": float(selected["top15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
            "top20pct_capture_rate": float(selected["top20pct_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
            "bottom15_contamination_rate": float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if len(selected) else None,
        }
        selected["is_backfill"] = selected["candidate_pool_tier"].astype(str).eq("risk_flagged_backfill")
        selected["passes_current_guard"] = selected["is_backfill"] & (pd.to_numeric(selected["score"], errors="coerce") >= 0.40) & (pd.to_numeric(selected["rank"], errors="coerce") <= 8)
        selected["passes_stricter_045_rank5"] = selected["is_backfill"] & (pd.to_numeric(selected["score"], errors="coerce") >= 0.45) & (pd.to_numeric(selected["rank"], errors="coerce") <= 5)
        selected["passes_stricter_045_rank8"] = selected["is_backfill"] & (pd.to_numeric(selected["score"], errors="coerce") >= 0.45) & (pd.to_numeric(selected["rank"], errors="coerce") <= 8)
        selected["passes_score040_rank5"] = selected["is_backfill"] & (pd.to_numeric(selected["score"], errors="coerce") >= 0.40) & (pd.to_numeric(selected["rank"], errors="coerce") <= 5)
        selected["passes_score040_rank8"] = selected["is_backfill"] & (pd.to_numeric(selected["score"], errors="coerce") >= 0.40) & (pd.to_numeric(selected["rank"], errors="coerce") <= 8)
        selected["buried_top15_winner"] = selected["top15_label"].fillna(False).astype(bool) & ~selected["passes_current_guard"]
        audit["selected_by_topk_guard_detail"][f"top{topk}"] = {
            "current_guard_backfill_count": int(selected["passes_current_guard"].sum()),
            "current_guard_top15_count": int((selected["passes_current_guard"] & selected["top15_label"].fillna(False).astype(bool)).sum()),
            "current_guard_bottom15_count": int((selected["passes_current_guard"] & selected["bottom15_label"].fillna(False).astype(bool)).sum()),
            "score040_rank5_backfill_count": int(selected["passes_score040_rank5"].sum()),
            "score040_rank5_top15_count": int((selected["passes_score040_rank5"] & selected["top15_label"].fillna(False).astype(bool)).sum()),
            "score045_rank5_backfill_count": int(selected["passes_stricter_045_rank5"].sum()),
            "score045_rank5_top15_count": int((selected["passes_stricter_045_rank5"] & selected["top15_label"].fillna(False).astype(bool)).sum()),
            "score045_rank8_backfill_count": int(selected["passes_stricter_045_rank8"].sum()),
            "score045_rank8_top15_count": int((selected["passes_stricter_045_rank8"] & selected["top15_label"].fillna(False).astype(bool)).sum()),
            "buried_top15_winner_count": int(selected["buried_top15_winner"].sum()),
            "buried_top15_winner_rank_min": int(pd.to_numeric(selected.loc[selected["buried_top15_winner"], "rank"], errors="coerce").min()) if selected["buried_top15_winner"].any() else None,
            "buried_top15_winner_rank_median": float(pd.to_numeric(selected.loc[selected["buried_top15_winner"], "rank"], errors="coerce").median()) if selected["buried_top15_winner"].any() else None,
            "buried_top15_winner_rank_max": int(pd.to_numeric(selected.loc[selected["buried_top15_winner"], "rank"], errors="coerce").max()) if selected["buried_top15_winner"].any() else None,
        }
        tier_detail = {}
        for tier, tier_frame in selected.groupby("candidate_pool_tier", sort=False):
            score = pd.to_numeric(tier_frame["score"], errors="coerce")
            rank = pd.to_numeric(tier_frame["rank"], errors="coerce")
            tier_detail[str(tier)] = {
                "row_count": int(len(tier_frame)),
                "mean_forward_ret_20d": float(pd.to_numeric(tier_frame["forward_ret_20d"], errors="coerce").mean()) if len(tier_frame) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(tier_frame["path_value_score_v1"], errors="coerce").mean()) if len(tier_frame) else None,
                "top15_capture_rate": float(tier_frame["top15_label"].fillna(False).astype(bool).mean()) if len(tier_frame) else None,
                "top20pct_capture_rate": float(tier_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(tier_frame) else None,
                "bottom15_contamination_rate": float(tier_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(tier_frame) else None,
                "score_min": float(score.min()) if len(tier_frame) else None,
                "score_median": float(score.median()) if len(tier_frame) else None,
                "score_max": float(score.max()) if len(tier_frame) else None,
                "rank_min": int(rank.min()) if len(tier_frame) else None,
                "rank_median": float(rank.median()) if len(tier_frame) else None,
                "rank_max": int(rank.max()) if len(tier_frame) else None,
            }
        audit["selected_by_topk_tier_detail"][f"top{topk}"] = tier_detail
        reason_detail = {}
        for reason, reason_frame in selected.groupby("candidate_pool_reason", sort=False):
            reason_detail[str(reason)] = {
                "row_count": int(len(reason_frame)),
                "mean_forward_ret_20d": float(pd.to_numeric(reason_frame["forward_ret_20d"], errors="coerce").mean()) if len(reason_frame) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(reason_frame["path_value_score_v1"], errors="coerce").mean()) if len(reason_frame) else None,
                "top15_capture_rate": float(reason_frame["top15_label"].fillna(False).astype(bool).mean()) if len(reason_frame) else None,
                "top20pct_capture_rate": float(reason_frame["top20pct_label"].fillna(False).astype(bool).mean()) if len(reason_frame) else None,
                "bottom15_contamination_rate": float(reason_frame["bottom15_label"].fillna(False).astype(bool).mean()) if len(reason_frame) else None,
            }
        audit["selected_by_topk_reason_detail"][f"top{topk}"] = reason_detail
    return audit


def _variant_summary(frame: pd.DataFrame, variant: str) -> dict[str, Any]:
    summary = {
        "variant": variant,
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        "breadth": _group_stats(frame),
        "tier_composition": {str(k): int(v) for k, v in frame["candidate_pool_tier"].value_counts().items()},
        "reason_composition": {str(k): int(v) for k, v in frame["candidate_pool_reason"].value_counts().items()},
        "risk_flagged_candidate_count": int(frame["risk_flagged_candidate"].fillna(False).astype(bool).sum()) if "risk_flagged_candidate" in frame.columns else 0,
        "included_for_min_pool_backfill_count": int(frame["included_for_min_pool_backfill"].fillna(False).astype(bool).sum()) if "included_for_min_pool_backfill" in frame.columns else 0,
        "would_have_been_excluded_under_current_contract_count": int(frame["would_have_been_excluded_under_current_contract"].fillna(False).astype(bool).sum()) if "would_have_been_excluded_under_current_contract" in frame.columns else 0,
        "retained_top15_label_count": int(frame["top15_label"].fillna(False).astype(bool).sum()) if "top15_label" in frame.columns else 0,
        "retained_top20pct_label_count": int(frame["top20pct_label"].fillna(False).astype(bool).sum()) if "top20pct_label" in frame.columns else 0,
        "retained_bottom15_label_count": int(frame["bottom15_label"].fillna(False).astype(bool).sum()) if "bottom15_label" in frame.columns else 0,
        "retained_non_positive_forward_ret_count": int((pd.to_numeric(frame["forward_ret_20d"], errors="coerce").notna() & (pd.to_numeric(frame["forward_ret_20d"], errors="coerce") <= 0)).sum()) if "forward_ret_20d" in frame.columns else 0,
        "oracle": {f"top{topk}": _oracle_block(frame, topk) for topk in TOP_K_VALUES},
    }
    return summary


def _build_contracts() -> dict[str, Any]:
    return {
        "schema_version": CONTRACTS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "baseline_variant": CURRENT_BASELINE_VARIANT,
        "severe_diagnostic_fields_used": [c for c in SEVERE_DIAGNOSTIC_FIELDS if c in _load_frame(LONG_SURFACE).columns],
        "variants": {
            "long_filter_primary_watch_only": {
                "allowed_tiers": ["KEEP_PRIMARY", "KEEP_WATCH"],
                "backfill_guard": None,
                "severe_diagnostic_exclusion": False,
                "description": "keep only primary and watch tiers",
            },
            "long_filter_score_040_rank8": {
                "allowed_tiers": ["KEEP_PRIMARY", "KEEP_WATCH", "risk_flagged_backfill"],
                "backfill_guard": {"score_gte": 0.40, "rank_lte": 8},
                "severe_diagnostic_exclusion": False,
                "description": "baseline long contract with score 0.40 and rank 8 backfill guard",
            },
            "long_filter_score_045_rank8": {
                "allowed_tiers": ["KEEP_PRIMARY", "KEEP_WATCH", "risk_flagged_backfill"],
                "backfill_guard": {"score_gte": 0.45, "rank_lte": 8},
                "severe_diagnostic_exclusion": False,
                "description": "stricter score guard with same rank cap",
            },
            "long_filter_score_040_rank5": {
                "allowed_tiers": ["KEEP_PRIMARY", "KEEP_WATCH", "risk_flagged_backfill"],
                "backfill_guard": {"score_gte": 0.40, "rank_lte": 5},
                "severe_diagnostic_exclusion": False,
                "description": "stricter rank guard with same score cap",
            },
            "long_filter_score_045_rank5": {
                "allowed_tiers": ["KEEP_PRIMARY", "KEEP_WATCH", "risk_flagged_backfill"],
                "backfill_guard": {"score_gte": 0.45, "rank_lte": 5},
                "severe_diagnostic_exclusion": False,
                "description": "strictest score and rank guard",
            },
            "long_filter_top15_candidate_guard": {
                "allowed_tiers": ["KEEP_PRIMARY", "KEEP_WATCH", "risk_flagged_backfill"],
                "backfill_guard": {"score_gte": 0.40, "rank_lte": 8},
                "severe_diagnostic_exclusion": True,
                "description": "score/rank guard plus severe diagnostic exclusion where available",
            },
        },
    }


def _build_surface_and_reranker_comparisons(frame: pd.DataFrame, variant_frames: dict[str, pd.DataFrame]) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame, pd.DataFrame]:
    surface = {
        "schema_version": SURFACE_COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "baseline_long_surface": _variant_summary(frame, CURRENT_BASELINE_VARIANT),
        "variants": {},
        "reference_inputs": {
            "long_reranker_validation_bundle": str(LONG_RERANKER_SESSION),
            "prior_filter_revision_bundle": str(MIXED_FILTER_SESSION),
            "prior_filter_revision_decision": _load_json(REFERENCE_FILTER_DECISION).get("decision"),
            "long_reranker_decision": _load_json(RERANKER_DECISION).get("decision"),
        },
    }
    reranker = {
        "schema_version": RERANKER_COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "baseline_long_surface": {
            "variant": CURRENT_BASELINE_VARIANT,
            "row_count": int(len(frame)),
            "group_count": int(frame.groupby(["anchor_date", "side"], sort=False).ngroups),
        },
        "variants": {},
    }
    full_diff_frames: list[pd.DataFrame] = []
    membership_diff_frames: list[pd.DataFrame] = []
    for variant, vframe in variant_frames.items():
        surface["variants"][variant] = _variant_summary(vframe, variant)
        champion_rank = _rank_variant(vframe, CHAMPION_SCORE_COL, ["anchor_date", "side"])
        model_rank = _rank_variant(vframe, MODEL_SCORE_COL, ["anchor_date", "side"])
        variant_entry = {
            "variant": variant,
            "row_count": int(len(vframe)),
            "group_count": int(vframe.groupby(["anchor_date", "side"], sort=False).ngroups),
            "branching_happened": bool((model_rank != champion_rank).any()),
            "topk": {},
        }
        for topk in TOP_K_VALUES:
            metrics = _selection_metrics(vframe, VARIANT_MODEL_RANK_COL, VARIANT_CHAMPION_RANK_COL, topk)
            metrics["champion_reference"] = {
                "selected_row_count": int((champion_rank <= topk).sum()),
                "mean_forward_ret_20d": float(pd.to_numeric(vframe.loc[champion_rank <= topk, "forward_ret_20d"], errors="coerce").mean()) if int((champion_rank <= topk).sum()) else None,
                "mean_path_value_score_v1": float(pd.to_numeric(vframe.loc[champion_rank <= topk, "path_value_score_v1"], errors="coerce").mean()) if int((champion_rank <= topk).sum()) else None,
                "top15_capture_rate": float(vframe.loc[champion_rank <= topk, "top15_label"].fillna(False).astype(bool).mean()) if int((champion_rank <= topk).sum()) else None,
                "top20pct_capture_rate": float(vframe.loc[champion_rank <= topk, "top20pct_label"].fillna(False).astype(bool).mean()) if int((champion_rank <= topk).sum()) else None,
                "bottom15_contamination_rate": float(vframe.loc[champion_rank <= topk, "bottom15_label"].fillna(False).astype(bool).mean()) if int((champion_rank <= topk).sum()) else None,
            }
            metrics["model_selected_row_count"] = int((model_rank <= topk).sum())
            metrics["champion_selected_row_count"] = int((champion_rank <= topk).sum())
            metrics["selected_vs_champion_forward_delta"] = None if metrics["mean_forward_ret_20d"] is None or metrics["champion_reference"]["mean_forward_ret_20d"] is None else metrics["mean_forward_ret_20d"] - metrics["champion_reference"]["mean_forward_ret_20d"]
            metrics["selected_vs_champion_path_delta"] = None if metrics["mean_path_value_score_v1"] is None or metrics["champion_reference"]["mean_path_value_score_v1"] is None else metrics["mean_path_value_score_v1"] - metrics["champion_reference"]["mean_path_value_score_v1"]
            variant_entry["topk"][f"top{topk}"] = metrics

            diff = vframe.copy()
            diff["filter_revision_variant"] = variant
            diff["topk"] = int(topk)
            diff["model_selected"] = model_rank <= topk
            diff["champion_selected"] = champion_rank <= topk
            diff["membership_changed"] = diff["model_selected"] ^ diff["champion_selected"]
            diff["selected_overlap"] = diff["model_selected"] & diff["champion_selected"]
            diff["model_rank"] = model_rank
            diff["champion_rank_variant"] = champion_rank
            diff["model_score"] = pd.to_numeric(vframe[MODEL_SCORE_COL], errors="coerce")
            diff["champion_score_variant"] = pd.to_numeric(vframe[CHAMPION_SCORE_COL], errors="coerce")
            diff["filter_revision_selected"] = True
            full_diff_frames.append(diff.copy())
            membership_diff_frames.append(
                diff[
                    [
                        "filter_revision_variant",
                        "topk",
                        "anchor_date",
                        "month_bucket",
                        "side",
                        "symbol",
                        "candidate_idx" if "candidate_idx" in diff.columns else "score",
                        "candidate_pool_tier",
                        "candidate_pool_reason",
                        "risk_flagged_candidate",
                        "included_for_min_pool_backfill",
                        "would_have_been_excluded_under_current_contract",
                        "score",
                        "rank",
                        "forward_ret_20d",
                        "path_value_score_v1",
                        "top15_label",
                        "top20pct_label",
                        "bottom15_label",
                        "model_score",
                        "model_rank",
                        "champion_score_variant",
                        "champion_rank_variant",
                        "model_selected",
                        "champion_selected",
                        "membership_changed",
                        "selected_overlap",
                    ]
                ].copy()
            )
        reranker["variants"][variant] = variant_entry
    full_stacked = pd.concat(full_diff_frames, ignore_index=True) if full_diff_frames else pd.DataFrame()
    membership_stacked = pd.concat(membership_diff_frames, ignore_index=True) if membership_diff_frames else pd.DataFrame()
    return surface, reranker, full_stacked, membership_stacked


def _recommendation(reranker: dict[str, Any], surface: dict[str, Any]) -> dict[str, Any]:
    variants = reranker["variants"]
    best_variant = None
    best_score = None
    for name, block in variants.items():
        top5 = block["topk"]["top5"]
        top10 = block["topk"]["top10"]
        top20 = block["topk"]["top20"]
        score = 0
        if (top5["selected_vs_champion_forward_delta"] or 0) > 0:
            score += 2
        if (top10["selected_vs_champion_forward_delta"] or 0) > 0:
            score += 1
        if (top5["top15_capture_rate"] or 0) > (top5["champion_top15_capture_rate"] or 0):
            score += 2
        if (top10["top15_capture_rate"] or 0) > (top10["champion_top15_capture_rate"] or 0):
            score += 1
        if (top5["bottom15_contamination_rate"] or 0) <= (top5["champion_bottom15_contamination_rate"] or 0) and (top10["bottom15_contamination_rate"] or 0) <= (top10["champion_bottom15_contamination_rate"] or 0):
            score += 1
        if (top5["risk_flagged_backfill_count"] or 0) < surface["baseline_long_surface"]["tier_composition"].get("risk_flagged_backfill", 0):
            score += 1
        if best_score is None or score > best_score:
            best_score = score
            best_variant = name
    if best_variant is None:
        return {
            "schema_version": RECOMMENDATION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "recommended_next_action": "needs_candidate_generation_refinement",
            "reason": "No viable stricter long-side filter variant improved practical topK quality enough to justify rebuilding the surface.",
        }
    best_top5 = variants[best_variant]["topk"]["top5"]
    best_top10 = variants[best_variant]["topk"]["top10"]
    best_top20 = variants[best_variant]["topk"]["top20"]
    if (
        (best_top5["selected_vs_champion_forward_delta"] or 0) > 0
        and (best_top5["top15_capture_rate"] or 0) > (best_top5["champion_top15_capture_rate"] or 0)
        and (best_top5["bottom15_contamination_rate"] or 0) <= (best_top5["champion_bottom15_contamination_rate"] or 0)
        and (best_top5["risk_flagged_backfill_count"] or 0) < surface["baseline_long_surface"]["tier_composition"].get("risk_flagged_backfill", 0)
    ):
        action = "ready_to_rebuild_long_surface_with_revised_filter"
        reason = f"{best_variant} improves top5 and meaningfully reduces backfill dominance without breaking the long-only contract."
    elif (best_top5["selected_vs_champion_forward_delta"] or 0) > 0 or (best_top10["selected_vs_champion_forward_delta"] or 0) > 0:
        action = "needs_candidate_generation_refinement"
        reason = f"{best_variant} shows partial improvement, but top15 capture or backfill dominance is still not strong enough for a rebuild."
    else:
        action = "filter_revision_insufficient_stop_high_recall_line"
        reason = "No variant preserved enough breadth while improving practical topK quality on the long-active surface."
    return {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "recommended_next_action": action,
        "best_variant": best_variant,
        "reason": reason,
        "supporting_metrics": {
            "top5_forward_delta": best_top5["selected_vs_champion_forward_delta"],
            "top10_forward_delta": best_top10["selected_vs_champion_forward_delta"],
            "top5_top15_delta": (best_top5["top15_capture_rate"] or 0) - (best_top5["champion_top15_capture_rate"] or 0),
            "top10_top15_delta": (best_top10["top15_capture_rate"] or 0) - (best_top10["champion_top15_capture_rate"] or 0),
            "top5_bottom15_delta": (best_top5["bottom15_contamination_rate"] or 0) - (best_top5["champion_bottom15_contamination_rate"] or 0),
            "top10_bottom15_delta": (best_top10["bottom15_contamination_rate"] or 0) - (best_top10["champion_bottom15_contamination_rate"] or 0),
            "top5_backfill_count": best_top5["risk_flagged_backfill_count"],
            "top10_backfill_count": best_top10["risk_flagged_backfill_count"],
            "top20_backfill_count": best_top20["risk_flagged_backfill_count"],
        },
    }


def _decision(recommendation: dict[str, Any], surface: dict[str, Any], reranker: dict[str, Any]) -> dict[str, Any]:
    action = recommendation["recommended_next_action"]
    if action == "ready_to_rebuild_long_surface_with_revised_filter":
        decision = "ready_to_rebuild_long_surface_with_revised_filter"
    elif action == "needs_candidate_generation_refinement":
        decision = "needs_candidate_generation_refinement"
    else:
        decision = "filter_revision_insufficient_stop_high_recall_line"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": recommendation["reason"],
        "supporting_checks": {
            "baseline_row_count": surface["baseline_long_surface"]["row_count"],
            "baseline_group_count": surface["baseline_long_surface"]["group_count"],
            "frozen_features_present": bool(_load_json(FEATURE_CHECK)["long"]["feature_complete"]),
            "no_lookahead_passed": bool(_load_json(NO_LOOKAHEAD)["passed"]),
            "leakage_passed": bool(_load_json(LEAKAGE)["passed"]),
            "short_side_reintroduced": False,
            "reranker_selected": MODEL_NAME,
            "reference_filter_revision_used": True,
            "reference_reranker_validation_used": True,
        },
    }


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
            "long_surface": str(LONG_SURFACE),
            "long_surface_summary": str(LONG_SURFACE_SUMMARY),
            "feature_check": str(FEATURE_CHECK),
            "no_lookahead": str(NO_LOOKAHEAD),
            "leakage": str(LEAKAGE),
            "quality_audit": str(QUALITY_AUDIT),
            "oracle_headroom": str(ORACLE_HEADROOM),
            "surface_decision": str(SURFACE_DECISION),
            "reranker_input_validation": str(RERANKER_INPUT_VALIDATION),
            "reranker_prediction_rows": str(RERANKER_PREDICTION_ROWS),
            "reranker_variant_comparison": str(RERANKER_VARIANT_COMPARISON),
            "reranker_membership_diff": str(RERANKER_MEMBERSHIP_DIFF),
            "reranker_oracle_gap": str(RERANKER_ORACLE_GAP),
            "reranker_failure": str(RERANKER_FAILURE),
            "reranker_decision": str(RERANKER_DECISION),
            "reranker_tier_summary": str(RERANKER_TIER_SUMMARY),
            "reranker_group_summary": str(RERANKER_GROUP_SUMMARY),
            "reference_filter_surface": str(REFERENCE_FILTER_SURFACE),
            "reference_filter_reranker": str(REFERENCE_FILTER_RERANKER),
            "reference_filter_rows": str(REFERENCE_FILTER_ROWS),
            "reference_filter_decision": str(REFERENCE_FILTER_DECISION),
        },
    }


def _build_input_resolution(output_root: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "output_root": str(output_root),
        "resolved_long_surface": str(LONG_SURFACE),
        "resolved_long_surface_summary": str(LONG_SURFACE_SUMMARY),
        "resolved_reranker_prediction_rows": str(RERANKER_PREDICTION_ROWS),
        "resolved_reranker_input_validation": str(RERANKER_INPUT_VALIDATION),
        "resolved_reference_filter_revision": str(MIXED_FILTER_SESSION),
        "resolved_reference_reranker_validation": str(LONG_RERANKER_SESSION),
        "jobs_requested": 2,
        "jobs_supported": 2,
        "baseline_variant": CURRENT_BASELINE_VARIANT,
        "frozen_model_name": MODEL_NAME,
        "long_active_row_count": int(len(inputs["long_surface"])),
        "long_active_group_count": int(inputs["long_surface"].groupby(["anchor_date", "side"], sort=False).ngroups),
        "prediction_row_count": int(len(inputs["reranker_rows"])),
        "prediction_group_count": int(inputs["reranker_rows"].groupby(["anchor_date", "side"], sort=False).ngroups),
        "reference_inputs": {
            "long_side_reranker_validation_bundle": str(LONG_RERANKER_SESSION),
            "prior_filter_revision_bundle": str(MIXED_FILTER_SESSION),
            "long_side_reranker_validation_decision": inputs["reranker_decision"].get("decision"),
            "prior_filter_revision_decision": inputs["reference_filter_decision"].get("decision"),
        },
        "notes": [
            "The replay uses the frozen tree_hgb_path_value scores already emitted by the prior long-side validation bundle.",
            "The long-side surface remains the only active validation surface; short side is not reintroduced.",
        ],
    }


def _run(output_root: Path, jobs: int) -> dict[str, Any]:
    inputs = _load_inputs()
    prediction_rows = inputs["reranker_rows"].copy()
    long_surface = inputs["long_surface"].copy()
    if len(prediction_rows) != len(long_surface):
        raise RuntimeError("prediction rows and long surface row counts differ")
    if not prediction_rows["side"].astype(str).eq("long").all():
        raise RuntimeError("prediction rows include non-long rows")
    if not long_surface["side"].astype(str).eq("long").all():
        raise RuntimeError("long active surface includes non-long rows")
    if "tree_hgb_path_value_score" not in prediction_rows.columns:
        raise RuntimeError("frozen reranker score column missing from prediction rows")

    failure_audit = _selected_row_failure_audit(prediction_rows)

    variant_frames: dict[str, pd.DataFrame] = {}
    contract_details = {"schema_version": CONTRACTS_SCHEMA_VERSION, "generated_at_utc": _utc_now(), "baseline_variant": CURRENT_BASELINE_VARIANT, "variants": {}, "severe_diagnostic_fields_used": [c for c in SEVERE_DIAGNOSTIC_FIELDS if c in prediction_rows.columns]}
    for variant in LONG_FILTER_VARIANTS:
        vframe, meta = _variant_surface(prediction_rows, variant)
        variant_frames[variant] = vframe
        contract_details["variants"][variant] = {
            "variant": variant,
            "reason": meta["reason"],
            "backfill_guard_count": meta["backfill_guard_count"],
            "primary_watch_count": meta["primary_watch_count"],
            "severe_diagnostic_fields_present": meta["severe_diagnostic_fields_present"],
            "severe_diagnostic_rows": meta["severe_diagnostic_rows"],
            "allowed_tiers": sorted(vframe["candidate_pool_tier"].astype(str).unique().tolist()),
        }

    surface_comparison, reranker_comparison, stacked_rows, membership_diff_rows = _build_surface_and_reranker_comparisons(prediction_rows, variant_frames)
    recommendation = _recommendation(reranker_comparison, surface_comparison)
    decision = _decision(recommendation, surface_comparison, reranker_comparison)

    manifest = _build_manifest(output_root)
    input_resolution = _build_input_resolution(output_root, inputs)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", manifest)
    _write_json(output_root / "input_resolution.json", input_resolution)
    _write_json(output_root / "long_side_selected_row_failure_audit.json", failure_audit)
    _write_json(output_root / "long_side_filter_revision_contracts.json", contract_details)
    _write_json(output_root / "long_side_filter_revision_surface_comparison.json", surface_comparison)
    _write_parquet(output_root / "long_side_filter_revision_rows.parquet", _dedupe_columns(stacked_rows))
    _write_json(output_root / "long_side_filter_revision_reranker_comparison.json", reranker_comparison)
    _write_parquet(output_root / "long_side_filter_revision_topk_membership_diff.parquet", _dedupe_columns(membership_diff_rows))
    _write_json(output_root / "long_side_filter_revision_recommendation.json", recommendation)
    _write_json(output_root / "long_side_filter_revision_v1_decision.json", decision)
    _write_parquet(output_root / "long_side_filter_revision_tier_summary.parquet", inputs["reranker_tier_summary"].copy())
    _write_parquet(output_root / "long_side_filter_revision_group_summary.parquet", inputs["reranker_group_summary"].copy())
    _write_parquet(output_root / "long_side_filter_revision_oracle_by_group.parquet", inputs["reranker_membership_diff"].copy())
    _write_json(
        output_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "complete": True,
            "required_artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "long_side_selected_row_failure_audit.json",
                "long_side_filter_revision_contracts.json",
                "long_side_filter_revision_surface_comparison.json",
                "long_side_filter_revision_rows.parquet",
                "long_side_filter_revision_reranker_comparison.json",
                "long_side_filter_revision_topk_membership_diff.parquet",
                "long_side_filter_revision_recommendation.json",
                "long_side_filter_revision_v1_decision.json",
            ],
        },
    )
    return {
        "output_root": str(output_root),
        "decision": decision["decision"],
        "row_count": int(len(prediction_rows)),
        "group_count": int(prediction_rows.groupby(["anchor_date", "side"], sort=False).ngroups),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX long-side high-recall filter revision v1")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    session_dir = args.output_root / _session_id()
    result = _run(session_dir, max(1, args.jobs))
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
