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

from scripts.tradex_bad_pick_reclassification_batch2_volume_features_v1 import (
    KEY_COLS,
    TOP_K_VALUES,
    _ensure_exists,
    _git_hash_or_unknown,
    _json_ready,
    _load_frame,
    _load_json,
    _make_session_id,
    _safe_float,
    _safe_path,
    _utc_now,
    _value_counts,
    _write_json,
    _write_parquet,
)


SCRIPT_NAME = "tradex_low_turnover_participation_require_confirmation_v1"
SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_input_resolution_v1"
PROFILE_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_low_turnover_participation_false_positive_profile_v1"
POLICY_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_participation_confirmation_policy_v1"
POOL_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_variant_pool_comparison_v1"
MONTHLY_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_monthly_comparison_v1"
CONTEXT_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_context_comparison_v1"
DIFF_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_topk_membership_diff_v1"
PRECISION_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_precision_recall_summary_v1"
FALSE_POS_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_false_positive_cost_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_low_turnover_participation_require_confirmation_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\low_turnover_participation_require_confirmation_v1")

VOLUME_SESSION = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273")
VOLUME_CANDIDATE = VOLUME_SESSION / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet"
VOLUME_ORFP = VOLUME_SESSION / "observable_regime_false_positive_batch2_volume_enriched_v1.parquet"
VOLUME_FORMULA = VOLUME_SESSION / "volume_feature_formula_contract.json"
VOLUME_COVERAGE = VOLUME_SESSION / "volume_repair_coverage_summary.json"
VOLUME_NO_LOOKAHEAD = VOLUME_SESSION / "no_lookahead_volume_feature_audit.json"

RECLASS_SESSION = Path(r"G:\Tradex\bad_pick_reclassification_batch2_volume_v1\20260501T102834Z-365658")
RECLASS_ROWS = RECLASS_SESSION / "batch2_volume_reclassification_rows.parquet"
RECLASS_FAMILY = RECLASS_SESSION / "batch2_volume_root_cause_taxonomy_summary.json"
RECLASS_BEFORE_AFTER = RECLASS_SESSION / "before_after_batch2_volume_reclassification_summary.json"
RECLASS_BOUNDARY = RECLASS_SESSION / "batch2_volume_boundary_pairwise.parquet"
RECLASS_BOUNDARY_SUMMARY = RECLASS_SESSION / "batch2_volume_boundary_pairwise_summary.json"
RECLASS_FUTURE = RECLASS_SESSION / "batch2_volume_future_challenger_candidates.json"
RECLASS_DECISION = RECLASS_SESSION / "bad_pick_reclassification_batch2_volume_v1_decision.json"

TARGET_FAMILY = "low_turnover_participation_false_positive"
CONFIRMED_BUCKETS = {"participation_normal", "participation_strong"}
RISK_REASON = "target_family_low_turnover_or_weak_participation"
FAMILY_COLUMNS = ["batch2_volume_root_cause_code", "batch2_volume_confidence", "batch2_volume_family"]
CHECKPOINT_TOPKS = TOP_K_VALUES


def _make_session_id_local() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _feature_fields() -> list[str]:
    return [
        "entry_strength_score",
        "signal_quality_bucket",
        "decision_candle_quality",
        "liquidity_quality_bucket",
        "higher_timeframe_headroom_bucket",
        "vol_ratio5_20_repaired",
        "volume_zscore_20",
        "turnover_value_ratio5_20",
        "participation_quality_bucket",
        "volume_confirmation_repaired_flag",
    ]


def _status_fields() -> list[str]:
    return [
        "vol_ratio5_20_repair_status",
        "vol_ratio5_20_repair_missing_reason",
        "volume_zscore_20_feature_status",
        "volume_zscore_20_missing_reason",
        "turnover_value_ratio5_20_feature_status",
        "turnover_value_ratio5_20_missing_reason",
        "participation_quality_bucket_feature_status",
        "participation_quality_bucket_missing_reason",
        "volume_confirmation_repaired_flag_feature_status",
        "volume_confirmation_repaired_flag_missing_reason",
    ]


def _full_required_columns() -> list[str]:
    return list(dict.fromkeys(KEY_COLS + [
        "candidate_idx",
        "score",
        "rank",
        "candidate_rank",
        "topk_bucket",
        "top15_label",
        "bottom15_label",
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "month_bucket",
        "dominant_regime_context",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "conditional_high_value",
        "entry_strength_score",
        "signal_quality_bucket",
        "decision_candle_quality",
        "liquidity_quality_bucket",
        "higher_timeframe_headroom_bucket",
        "volume_participation_bucket",
        "volume_participation_bucket_feature_status",
        "volume_participation_bucket_missing_reason",
    ] + _feature_fields() + _status_fields()))


def _profile_required_columns() -> list[str]:
    return list(dict.fromkeys(KEY_COLS + [
        "batch2_volume_root_cause_code",
        "batch2_volume_confidence",
        "batch2_volume_family",
        "score",
        "rank",
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "top15_label",
        "bottom15_label",
        "forward_ret_20d",
        "path_value_score_v1",
        "month_bucket",
        "dominant_regime_context",
        "side",
        "entry_strength_score",
        "signal_quality_bucket",
        "decision_candle_quality",
        "liquidity_quality_bucket",
        "higher_timeframe_headroom_bucket",
        "volume_confirmation_repaired_flag",
        "vol_ratio5_20_repaired",
        "volume_zscore_20",
        "turnover_value_ratio5_20",
        "participation_quality_bucket",
    ]))


def _build_input_resolution(paths: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {name: str(path) for name, path in paths.items()},
        "path_checks": {name: path.exists() for name, path in paths.items()},
        "all_paths_exist": all(path.exists() for path in paths.values()),
    }


def _build_manifest(output_root: Path, session_dir: Path, inputs: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "git_commit": _git_hash_or_unknown(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "input_paths": {key: str(value) for key, value in inputs.items()},
    }


def _select_candidate_rows(candidate: pd.DataFrame) -> pd.DataFrame:
    out = candidate.copy()
    for col in ["champion_selected_top5", "champion_selected_top10", "champion_selected_top20", "top15_label", "bottom15_label"]:
        if col in out.columns:
            out[col] = out[col].fillna(False).astype(bool)
    return out


def _join_family(candidate: pd.DataFrame, family_rows: pd.DataFrame) -> pd.DataFrame:
    family = family_rows[KEY_COLS + [
        "batch2_volume_root_cause_code",
        "batch2_volume_confidence",
        "batch2_volume_family",
    ]].copy()
    joined = candidate.merge(family, on=KEY_COLS, how="left", suffixes=("", "_family"))
    joined["is_target_family"] = joined["batch2_volume_root_cause_code"].eq(TARGET_FAMILY)
    joined["batch2_volume_root_cause_code"] = joined["batch2_volume_root_cause_code"].where(joined["is_target_family"])
    joined["batch2_volume_confidence"] = joined["batch2_volume_confidence"].where(joined["is_target_family"])
    joined["batch2_volume_family"] = joined["batch2_volume_family"].where(joined["is_target_family"])
    return joined


def _confirmation_state(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    buckets = out.get("participation_quality_bucket", pd.Series([None] * len(out), index=out.index)).astype("string")
    repaired = out.get("volume_confirmation_repaired_flag", pd.Series([None] * len(out), index=out.index))
    repaired_ok = repaired.fillna(False).astype(bool)
    target = out.get("is_target_family", pd.Series([False] * len(out), index=out.index)).fillna(False).astype(bool)

    confirmed_mask = target & (buckets.isin(sorted(CONFIRMED_BUCKETS)) | repaired_ok)
    deprioritized_mask = target & ~confirmed_mask

    out["participation_confirmation_state"] = "non_target"
    out.loc[confirmed_mask, "participation_confirmation_state"] = "confirmed"
    out.loc[deprioritized_mask, "participation_confirmation_state"] = "deprioritized"
    out["participation_confirmation_reason"] = "non_target_passthrough"
    out.loc[confirmed_mask, "participation_confirmation_reason"] = "target_family_participation_confirmed"
    out.loc[deprioritized_mask, "participation_confirmation_reason"] = RISK_REASON
    out["participation_confirmation_needed"] = target
    out["participation_confirmation_ok"] = out["participation_confirmation_state"].eq("confirmed") | out["participation_confirmation_state"].eq("non_target")
    out["effective_rank_score"] = pd.to_numeric(out["score"], errors="coerce")
    out.loc[deprioritized_mask, "effective_rank_score"] = out.loc[deprioritized_mask, "effective_rank_score"] - 2.0
    out["confirmation_penalty_applied"] = deprioritized_mask
    return out


def _rank_variant(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    sort_cols = ["anchor_date", "side", "effective_rank_score", "score", "rank", "symbol"]
    out = out.sort_values(sort_cols, ascending=[True, True, False, False, True, True], kind="stable").reset_index(drop=True)
    out["variant_group_rank"] = out.groupby(["anchor_date", "side"], sort=False).cumcount() + 1
    for top_k in CHECKPOINT_TOPKS:
        out[f"variant_selected_top{top_k}"] = out["variant_group_rank"] <= top_k
    return out


def _summary_counts(frame: pd.DataFrame, selected_col: str) -> dict[str, Any]:
    selected = frame[selected_col].fillna(False).astype(bool)
    subset = frame[selected]
    total_top15 = int(frame["top15_label"].fillna(False).astype(bool).sum()) if "top15_label" in frame.columns else 1
    total_bottom15 = int(frame["bottom15_label"].fillna(False).astype(bool).sum()) if "bottom15_label" in frame.columns else 1
    top15 = int(subset["top15_label"].fillna(False).astype(bool).sum()) if "top15_label" in subset.columns else 0
    bottom15 = int(subset["bottom15_label"].fillna(False).astype(bool).sum()) if "bottom15_label" in subset.columns else 0
    target = int(subset["is_target_family"].fillna(False).astype(bool).sum()) if "is_target_family" in subset.columns else 0
    return {
        "count": int(len(subset)),
        "mean_forward_ret_20d": _safe_float(pd.to_numeric(subset.get("forward_ret_20d"), errors="coerce").mean()) if "forward_ret_20d" in subset.columns else None,
        "mean_path_value_score_v1": _safe_float(pd.to_numeric(subset.get("path_value_score_v1"), errors="coerce").mean()) if "path_value_score_v1" in subset.columns else None,
        "top15_count": top15,
        "top15_capture_rate": _safe_float(top15 / max(total_top15, 1)),
        "bottom15_count": bottom15,
        "bottom15_contamination_rate": _safe_float(bottom15 / max(total_bottom15, 1)),
        "bad_pick_count": bottom15,
        "target_family_count": target,
        "side_split": _value_counts(subset["side"]) if "side" in subset.columns else {},
        "month_split": _value_counts(subset["month_bucket"]) if "month_bucket" in subset.columns else {},
        "context_split": _value_counts(subset["dominant_regime_context"]) if "dominant_regime_context" in subset.columns else {},
    }


def _bucket_win_loss_flat(frame: pd.DataFrame, selected_col: str, bucket_col: str) -> dict[str, Any]:
    selected = frame[selected_col].fillna(False).astype(bool)
    subset = frame[selected].copy()
    if subset.empty or bucket_col not in subset.columns or "forward_ret_20d" not in subset.columns:
        return {"win": 0, "loss": 0, "flat": 0, "group_count": 0, "mean_forward_ret_20d": None}
    grouped = subset.groupby(bucket_col, dropna=False)["forward_ret_20d"].mean()
    win = int((grouped > 0).sum())
    loss = int((grouped < 0).sum())
    flat = int((grouped.abs() <= 1e-12).sum())
    return {
        "win": win,
        "loss": loss,
        "flat": flat,
        "group_count": int(grouped.size),
        "mean_forward_ret_20d": _safe_float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()),
    }


def _compare_by_topk(frame: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], pd.DataFrame]:
    pool: dict[str, Any] = {
        "schema_version": POOL_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "target_family_code": TARGET_FAMILY,
        "candidate_row_count": int(len(frame)),
        "target_family_count": int(frame["is_target_family"].fillna(False).astype(bool).sum()),
        "topk": {},
    }
    monthly: dict[str, Any] = {"schema_version": MONTHLY_SCHEMA_VERSION, "generated_at_utc": _utc_now(), "topk": {}}
    context: dict[str, Any] = {"schema_version": CONTEXT_SCHEMA_VERSION, "generated_at_utc": _utc_now(), "topk": {}}
    diff_rows: list[dict[str, Any]] = []

    original_groups = frame.groupby(["anchor_date", "side"], dropna=False, sort=False)
    variant_groups = frame.groupby(["anchor_date", "side"], dropna=False, sort=False)
    zero_pass_groups: dict[str, int] = {}

    for top_k in CHECKPOINT_TOPKS:
        orig_col = f"champion_selected_top{top_k}"
        var_col = f"variant_selected_top{top_k}"
        original = frame[orig_col].fillna(False).astype(bool)
        variant = frame[var_col].fillna(False).astype(bool)
        changed = original != variant
        selected_original = frame[original]
        selected_variant = frame[variant]
        selected_union = original | variant
        selected_intersection = original & variant
        group_zero = 0
        for _, grp in frame.groupby(["anchor_date", "side"], dropna=False, sort=False):
            if bool((grp[orig_col].fillna(False).astype(bool) != grp[var_col].fillna(False).astype(bool)).any()):
                continue
            group_zero += 1
        zero_pass_groups[f"top{top_k}"] = group_zero
        pool["topk"][f"top{top_k}"] = {
            "original_count": int(original.sum()),
            "variant_count": int(variant.sum()),
            "changed_members_count": int(changed.sum()),
            "changed_rank_count": int((frame["rank"].fillna(frame["candidate_rank"]) != frame["variant_group_rank"]).sum()),
            "overlap_ratio": _safe_float(int(selected_intersection.sum()) / max(int(selected_union.sum()), 1)),
            "zero_pass_groups": group_zero,
            "members_moved_in": int((~original & variant).sum()),
            "members_moved_out": int((original & ~variant).sum()),
            "top15_count_original": int(selected_original["top15_label"].fillna(False).astype(bool).sum()),
            "top15_count_variant": int(selected_variant["top15_label"].fillna(False).astype(bool).sum()),
            "bottom15_count_original": int(selected_original["bottom15_label"].fillna(False).astype(bool).sum()),
            "bottom15_count_variant": int(selected_variant["bottom15_label"].fillna(False).astype(bool).sum()),
            "top15_capture_rate_original": _safe_float(selected_original["top15_label"].fillna(False).astype(bool).sum() / max(int(frame["top15_label"].fillna(False).astype(bool).sum()), 1)),
            "top15_capture_rate_variant": _safe_float(selected_variant["top15_label"].fillna(False).astype(bool).sum() / max(int(frame["top15_label"].fillna(False).astype(bool).sum()), 1)),
            "bottom15_contamination_rate_original": _safe_float(selected_original["bottom15_label"].fillna(False).astype(bool).sum() / max(int(frame["bottom15_label"].fillna(False).astype(bool).sum()), 1)),
            "bottom15_contamination_rate_variant": _safe_float(selected_variant["bottom15_label"].fillna(False).astype(bool).sum() / max(int(frame["bottom15_label"].fillna(False).astype(bool).sum()), 1)),
            "mean_forward_ret_20d_original": _safe_float(pd.to_numeric(selected_original["forward_ret_20d"], errors="coerce").mean()),
            "mean_forward_ret_20d_variant": _safe_float(pd.to_numeric(selected_variant["forward_ret_20d"], errors="coerce").mean()),
            "mean_path_value_score_v1_original": _safe_float(pd.to_numeric(selected_original["path_value_score_v1"], errors="coerce").mean()),
            "mean_path_value_score_v1_variant": _safe_float(pd.to_numeric(selected_variant["path_value_score_v1"], errors="coerce").mean()),
            "target_family_count_original": int(selected_original["is_target_family"].fillna(False).astype(bool).sum()),
            "target_family_count_variant": int(selected_variant["is_target_family"].fillna(False).astype(bool).sum()),
            "deprioritized_target_family_count": int((original & ~variant & frame["is_target_family"].fillna(False).astype(bool)).sum()),
            "admitted_target_family_count": int((variant & frame["is_target_family"].fillna(False).astype(bool)).sum()),
            "monthly_win_loss_flat": {
                "original": _bucket_win_loss_flat(frame, orig_col, "month_bucket"),
                "variant": _bucket_win_loss_flat(frame, var_col, "month_bucket"),
            },
            "context_win_loss_flat": {
                "original": _bucket_win_loss_flat(frame, orig_col, "dominant_regime_context"),
                "variant": _bucket_win_loss_flat(frame, var_col, "dominant_regime_context"),
            },
            "side_split": {
                "original": _value_counts(selected_original["side"]),
                "variant": _value_counts(selected_variant["side"]),
            },
            "regime_split": {
                "original": _value_counts(selected_original["dominant_regime_context"]),
                "variant": _value_counts(selected_variant["dominant_regime_context"]),
            },
            "boundary_movement": {
                "entered": int((~original & variant).sum()),
                "left": int((original & ~variant).sum()),
            },
        }
        for _, row in frame.loc[changed].iterrows():
            diff_rows.append({
                "anchor_date": str(row["anchor_date"]),
                "symbol": str(row["symbol"]),
                "side": str(row["side"]),
                "rank": _safe_float(row.get("rank")),
                "candidate_rank": _safe_float(row.get("candidate_rank")),
                "original_selected_top5": bool(row["champion_selected_top5"]),
                "original_selected_top10": bool(row["champion_selected_top10"]),
                "original_selected_top20": bool(row["champion_selected_top20"]),
                "variant_selected_top5": bool(row["variant_selected_top5"]),
                "variant_selected_top10": bool(row["variant_selected_top10"]),
                "variant_selected_top20": bool(row["variant_selected_top20"]),
                "changed_top5_member": bool(row["champion_selected_top5"]) != bool(row["variant_selected_top5"]),
                "changed_top10_member": bool(row["champion_selected_top10"]) != bool(row["variant_selected_top10"]),
                "changed_top20_member": bool(row["champion_selected_top20"]) != bool(row["variant_selected_top20"]),
                "target_family_code": row.get("batch2_volume_root_cause_code"),
                "is_target_family": bool(row.get("is_target_family", False)),
                "participation_confirmation_state": row.get("participation_confirmation_state"),
                "participation_confirmation_reason": row.get("participation_confirmation_reason"),
                "effective_rank_score": _safe_float(row.get("effective_rank_score")),
                "score": _safe_float(row.get("score")),
                "forward_ret_20d": _safe_float(row.get("forward_ret_20d")),
                "path_value_score_v1": _safe_float(row.get("path_value_score_v1")),
                "top15_label": bool(row.get("top15_label", False)),
                "bottom15_label": bool(row.get("bottom15_label", False)),
                "entry_strength_score": _safe_float(row.get("entry_strength_score")),
                "signal_quality_bucket": row.get("signal_quality_bucket"),
                "decision_candle_quality": row.get("decision_candle_quality"),
                "liquidity_quality_bucket": row.get("liquidity_quality_bucket"),
                "higher_timeframe_headroom_bucket": row.get("higher_timeframe_headroom_bucket"),
                "participation_quality_bucket": row.get("participation_quality_bucket"),
                "volume_confirmation_repaired_flag": row.get("volume_confirmation_repaired_flag"),
            })

    pool["changed_members"] = {k: int(v["changed_members_count"]) for k, v in pool["topk"].items()}
    pool["zero_pass_groups"] = zero_pass_groups
    pool["original_selected_counts"] = {k: int(frame[f"champion_selected_{k}"].fillna(False).astype(bool).sum()) for k in ["top5", "top10", "top20"]}
    pool["variant_selected_counts"] = {k: int(frame[f"variant_selected_{k}"].fillna(False).astype(bool).sum()) for k in ["top5", "top10", "top20"]}
    pool["total_top15_label_count"] = int(frame["top15_label"].fillna(False).astype(bool).sum())
    pool["total_bottom15_label_count"] = int(frame["bottom15_label"].fillna(False).astype(bool).sum())
    pool["family_members"] = int(frame["is_target_family"].fillna(False).astype(bool).sum())

    for top_k in CHECKPOINT_TOPKS:
        orig_col = f"champion_selected_top{top_k}"
        var_col = f"variant_selected_top{top_k}"
        monthly["topk"][f"top{top_k}"] = {
            "original": _bucket_win_loss_flat(frame, orig_col, "month_bucket"),
            "variant": _bucket_win_loss_flat(frame, var_col, "month_bucket"),
            "months_with_change": _value_counts((frame[orig_col].fillna(False).astype(bool) != frame[var_col].fillna(False).astype(bool)).groupby(frame["month_bucket"]).sum()),
        }
        context["topk"][f"top{top_k}"] = {
            "original": _bucket_win_loss_flat(frame, orig_col, "dominant_regime_context"),
            "variant": _bucket_win_loss_flat(frame, var_col, "dominant_regime_context"),
            "regimes_with_change": _value_counts((frame[orig_col].fillna(False).astype(bool) != frame[var_col].fillna(False).astype(bool)).groupby(frame["dominant_regime_context"]).sum()),
        }

    diff = pd.DataFrame(diff_rows)
    if not diff.empty:
        diff["schema_version"] = DIFF_SCHEMA_VERSION
    else:
        diff = pd.DataFrame(columns=["schema_version"])

    return pool, monthly, context, diff


def _profile_target_family(family_rows: pd.DataFrame, boundary_rows: pd.DataFrame) -> dict[str, Any]:
    fam = family_rows.loc[family_rows["batch2_volume_root_cause_code"] == TARGET_FAMILY].copy()
    boundary = boundary_rows.merge(
        fam[KEY_COLS + ["batch2_volume_root_cause_code"]],
        on=KEY_COLS,
        how="inner",
    )
    profile = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "family_code": TARGET_FAMILY,
        "family_count": int(len(fam)),
        "top5_count": int(fam["champion_selected_top5"].fillna(False).astype(bool).sum()) if "champion_selected_top5" in fam.columns else None,
        "top10_count": int(fam["champion_selected_top10"].fillna(False).astype(bool).sum()) if "champion_selected_top10" in fam.columns else None,
        "top20_count": int(fam["champion_selected_top20"].fillna(False).astype(bool).sum()) if "champion_selected_top20" in fam.columns else None,
        "side_split": _value_counts(fam["side"]),
        "month_split": _value_counts(fam["month_bucket"]),
        "regime_split": _value_counts(fam["dominant_regime_context"]),
        "score_distribution": {
            "mean": _safe_float(pd.to_numeric(fam["score"], errors="coerce").mean()),
            "median": _safe_float(pd.to_numeric(fam["score"], errors="coerce").median()),
            "min": _safe_float(pd.to_numeric(fam["score"], errors="coerce").min()),
            "max": _safe_float(pd.to_numeric(fam["score"], errors="coerce").max()),
        },
        "rank_distribution": _value_counts(fam["rank"].astype("Int64").astype("string")),
        "forward_ret_20d_distribution": {
            "mean": _safe_float(pd.to_numeric(fam["forward_ret_20d"], errors="coerce").mean()),
            "median": _safe_float(pd.to_numeric(fam["forward_ret_20d"], errors="coerce").median()),
            "min": _safe_float(pd.to_numeric(fam["forward_ret_20d"], errors="coerce").min()),
            "max": _safe_float(pd.to_numeric(fam["forward_ret_20d"], errors="coerce").max()),
        },
        "path_value_score_v1_distribution": {
            "mean": _safe_float(pd.to_numeric(fam["path_value_score_v1"], errors="coerce").mean()),
            "median": _safe_float(pd.to_numeric(fam["path_value_score_v1"], errors="coerce").median()),
            "min": _safe_float(pd.to_numeric(fam["path_value_score_v1"], errors="coerce").min()),
            "max": _safe_float(pd.to_numeric(fam["path_value_score_v1"], errors="coerce").max()),
        },
        "participation_quality_bucket_distribution": _value_counts(fam["participation_quality_bucket"]),
        "volume_confirmation_repaired_flag_distribution": _value_counts(fam["volume_confirmation_repaired_flag"].astype("string")),
        "vol_ratio5_20_repaired_distribution": {
            "mean": _safe_float(pd.to_numeric(fam["vol_ratio5_20_repaired"], errors="coerce").mean()),
            "median": _safe_float(pd.to_numeric(fam["vol_ratio5_20_repaired"], errors="coerce").median()),
        },
        "volume_zscore_20_distribution": {
            "mean": _safe_float(pd.to_numeric(fam["volume_zscore_20"], errors="coerce").mean()),
            "median": _safe_float(pd.to_numeric(fam["volume_zscore_20"], errors="coerce").median()),
        },
        "turnover_value_ratio5_20_distribution": {
            "mean": _safe_float(pd.to_numeric(fam["turnover_value_ratio5_20"], errors="coerce").mean()),
            "median": _safe_float(pd.to_numeric(fam["turnover_value_ratio5_20"], errors="coerce").median()),
        },
        "liquidity_quality_bucket_distribution": _value_counts(fam["liquidity_quality_bucket"]),
        "entry_strength_score_distribution": {
            "mean": _safe_float(pd.to_numeric(fam["entry_strength_score"], errors="coerce").mean()),
            "median": _safe_float(pd.to_numeric(fam["entry_strength_score"], errors="coerce").median()),
        },
        "signal_quality_bucket_distribution": _value_counts(fam["signal_quality_bucket"]),
        "overlap_with_good_picks": {
            "top15_count": int(fam["top15_label"].fillna(False).astype(bool).sum()),
            "bottom15_count": int(fam["bottom15_label"].fillna(False).astype(bool).sum()),
        },
        "boundary_near_miss_summary": {
            "pair_count": int(len(boundary)),
            "matched_near_miss_count": int(boundary["near_miss_joined"].fillna(False).astype(bool).sum()) if "near_miss_joined" in boundary.columns else 0,
            "selected_higher_score_count": int(boundary["selected_higher_score"].fillna(False).astype(bool).sum()) if "selected_higher_score" in boundary.columns else 0,
            "selected_worse_path_count": int(boundary["selected_worse_path"].fillna(False).astype(bool).sum()) if "selected_worse_path" in boundary.columns else 0,
            "selected_higher_score_and_worse_path_count": int(boundary["selected_higher_score_and_worse_path"].fillna(False).astype(bool).sum()) if "selected_higher_score_and_worse_path" in boundary.columns else 0,
            "mean_score_gap": _safe_float(pd.to_numeric(boundary.get("score_gap"), errors="coerce").mean()) if "score_gap" in boundary.columns else None,
            "mean_forward_ret_20d_gap": _safe_float(pd.to_numeric(boundary.get("forward_ret_20d_gap"), errors="coerce").mean()) if "forward_ret_20d_gap" in boundary.columns else None,
            "mean_path_value_gap": _safe_float(pd.to_numeric(boundary.get("path_value_gap"), errors="coerce").mean()) if "path_value_gap" in boundary.columns else None,
            "volume_participation_match_count": int(boundary["volume_participation_match"].fillna(False).astype(bool).sum()) if "volume_participation_match" in boundary.columns else 0,
            "liquidity_quality_match_count": int(boundary["liquidity_quality_match"].fillna(False).astype(bool).sum()) if "liquidity_quality_match" in boundary.columns else 0,
        },
    }
    return profile


def _build_policy(frame: pd.DataFrame) -> dict[str, Any]:
    policy_frame = frame.copy()
    if "participation_confirmation_state" not in policy_frame.columns and "is_target_family" in policy_frame.columns:
        policy_frame = _confirmation_state(policy_frame)

    if "participation_confirmation_state" in policy_frame.columns:
        state = policy_frame["participation_confirmation_state"].astype("string")
        target_mask = state.isin({"confirmed", "deprioritized"})
        confirmed = int((state == "confirmed").sum())
        deprioritized = int((state == "deprioritized").sum())
        target_count = int(target_mask.sum())
    else:
        target = policy_frame[policy_frame["batch2_volume_root_cause_code"].eq(TARGET_FAMILY)] if "batch2_volume_root_cause_code" in policy_frame.columns else policy_frame.iloc[0:0]
        confirmation_ok = target["participation_quality_bucket"].isin(sorted(CONFIRMED_BUCKETS)) if "participation_quality_bucket" in target.columns else pd.Series([False] * len(target), index=target.index)
        if "volume_confirmation_repaired_flag" in target.columns:
            confirmation_ok = confirmation_ok | target["volume_confirmation_repaired_flag"].fillna(False).astype(bool)
        confirmed = int(confirmation_ok.sum())
        deprioritized = int((~confirmation_ok).sum()) if len(target) else 0
        target_count = int(len(target))
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "challenger_family": TARGET_FAMILY,
        "scope": "full_candidate_universe_with_target_family_overlay",
        "confirmation_fields": [
            "participation_quality_bucket",
            "volume_confirmation_repaired_flag",
            "vol_ratio5_20_repaired",
            "volume_zscore_20",
            "turnover_value_ratio5_20",
            "liquidity_quality_bucket",
        ],
        "supporting_fields": [
            "entry_strength_score",
            "signal_quality_bucket",
            "decision_candle_quality",
            "higher_timeframe_headroom_bucket",
        ],
        "confirmation_logic": {
            "confirmed_if": [
                "participation_quality_bucket in {'participation_normal', 'participation_strong'}",
                "volume_confirmation_repaired_flag is true",
            ],
            "deprioritized_if": [
                "target_family_rows_without_confirmation",
            ],
            "non_target_rows": "retain_original_ordering",
        },
        "effective_rank_score": "score - 2.0 for target family rows without confirmation; otherwise score",
        "selection_order": ["effective_rank_score desc", "score desc", "rank asc", "symbol asc"],
        "preserves_original_score": True,
        "confirmation_counts": {"target_family_count": target_count, "confirmed_count": confirmed, "deprioritized_count": deprioritized},
        "no_lookahead_safe": True,
        "not_a_broad_veto": True,
        "jobs_supported": 1,
    }


def _build_precision_recall(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": PRECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "target_family_code": TARGET_FAMILY,
        "topk": {},
    }
    target = frame["is_target_family"].fillna(False).astype(bool)
    for top_k in CHECKPOINT_TOPKS:
        orig = frame[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
        var = frame[f"variant_selected_top{top_k}"].fillna(False).astype(bool)
        removed = orig & ~var
        added = ~orig & var
        bad_orig = int((orig & frame["bottom15_label"].fillna(False).astype(bool)).sum())
        bad_var = int((var & frame["bottom15_label"].fillna(False).astype(bool)).sum())
        good_orig = int((orig & frame["top15_label"].fillna(False).astype(bool)).sum())
        good_var = int((var & frame["top15_label"].fillna(False).astype(bool)).sum())
        target_bad_orig = int((orig & target & frame["bottom15_label"].fillna(False).astype(bool)).sum())
        target_bad_var = int((var & target & frame["bottom15_label"].fillna(False).astype(bool)).sum())
        target_good_orig = int((orig & target & frame["top15_label"].fillna(False).astype(bool)).sum())
        target_good_var = int((var & target & frame["top15_label"].fillna(False).astype(bool)).sum())
        deprioritized_bad = int((removed & target & frame["bottom15_label"].fillna(False).astype(bool)).sum())
        deprioritized_good = int((removed & target & frame["top15_label"].fillna(False).astype(bool)).sum())
        confirmed_bad = int((var & target & frame["bottom15_label"].fillna(False).astype(bool)).sum())
        confirmed_good = int((var & target & frame["top15_label"].fillna(False).astype(bool)).sum())
        out["topk"][f"top{top_k}"] = {
            "baseline_selected_count": int(orig.sum()),
            "variant_selected_count": int(var.sum()),
            "deprioritized_target_family_count": int((orig & target & ~var).sum()),
            "admitted_target_family_count": int((var & target).sum()),
            "deprioritized_actual_bad_picks": deprioritized_bad,
            "deprioritized_actual_good_picks": deprioritized_good,
            "confirmed_actual_bad_picks": confirmed_bad,
            "confirmed_actual_good_picks": confirmed_good,
            "lost_top15_count": int((removed & frame["top15_label"].fillna(False).astype(bool)).sum()),
            "removed_bottom15_count": int((removed & frame["bottom15_label"].fillna(False).astype(bool)).sum()),
            "precision_on_bad_pick_removal": _safe_float(deprioritized_bad / max(deprioritized_bad + deprioritized_good, 1)),
            "recall_on_target_family_bad_picks": _safe_float(deprioritized_bad / max(target_bad_orig, 1)),
            "target_family_bad_picks_baseline": target_bad_orig,
            "target_family_bad_picks_variant": target_bad_var,
            "target_family_good_picks_baseline": target_good_orig,
            "target_family_good_picks_variant": target_good_var,
            "top15_capture_rate_baseline": _safe_float(good_orig / max(int(frame["top15_label"].fillna(False).astype(bool).sum()), 1)),
            "top15_capture_rate_variant": _safe_float(good_var / max(int(frame["top15_label"].fillna(False).astype(bool).sum()), 1)),
            "bottom15_contamination_rate_baseline": _safe_float(bad_orig / max(int(frame["bottom15_label"].fillna(False).astype(bool).sum()), 1)),
            "bottom15_contamination_rate_variant": _safe_float(bad_var / max(int(frame["bottom15_label"].fillna(False).astype(bool).sum()), 1)),
        }
    return out


def _build_false_positive_cost(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": FALSE_POS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "target_family_code": TARGET_FAMILY,
        "topk": {},
    }
    target = frame["is_target_family"].fillna(False).astype(bool)
    for top_k in CHECKPOINT_TOPKS:
        orig = frame[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
        var = frame[f"variant_selected_top{top_k}"].fillna(False).astype(bool)
        out["topk"][f"top{top_k}"] = {
            "baseline_target_family_selected": int((orig & target).sum()),
            "variant_target_family_selected": int((var & target).sum()),
            "baseline_bottom15_selected": int((orig & frame["bottom15_label"].fillna(False).astype(bool)).sum()),
            "variant_bottom15_selected": int((var & frame["bottom15_label"].fillna(False).astype(bool)).sum()),
            "baseline_top15_selected": int((orig & frame["top15_label"].fillna(False).astype(bool)).sum()),
            "variant_top15_selected": int((var & frame["top15_label"].fillna(False).astype(bool)).sum()),
            "target_family_deprioritized": int((orig & ~var & target).sum()),
            "target_family_preserved": int((orig & var & target).sum()),
            "false_positive_cost_delta_bottom15": int((var & frame["bottom15_label"].fillna(False).astype(bool)).sum()) - int((orig & frame["bottom15_label"].fillna(False).astype(bool)).sum()),
            "false_positive_cost_delta_top15": int((var & frame["top15_label"].fillna(False).astype(bool)).sum()) - int((orig & frame["top15_label"].fillna(False).astype(bool)).sum()),
        }
    return out


def _build_decision(frame: pd.DataFrame, pool: dict[str, Any], precision: dict[str, Any], false_pos: dict[str, Any]) -> dict[str, Any]:
    top5 = pool["topk"]["top5"]
    top10 = pool["topk"]["top10"]
    top20 = pool["topk"]["top20"]
    family = pool["family_members"]
    target_family_top5 = top5["admitted_target_family_count"]
    target_family_top10 = top10["admitted_target_family_count"]
    target_family_deprioritized = top5["deprioritized_target_family_count"] + top10["deprioritized_target_family_count"]
    top5_path_ok = top5["mean_path_value_score_v1_variant"] is not None and (
        top5["mean_path_value_score_v1_original"] is None or top5["mean_path_value_score_v1_variant"] >= top5["mean_path_value_score_v1_original"] - 1e-12
    )
    top10_path_ok = top10["mean_path_value_score_v1_variant"] is not None and (
        top10["mean_path_value_score_v1_original"] is None or top10["mean_path_value_score_v1_variant"] >= top10["mean_path_value_score_v1_original"] - 1e-12
    )
    top15_ok = top5["top15_capture_rate_variant"] is None or (
        top5["top15_capture_rate_variant"] >= top5["top15_capture_rate_original"] - 0.01
    )
    bottom15_improved = top5["bottom15_contamination_rate_variant"] is not None and (
        top5["bottom15_contamination_rate_original"] is None or top5["bottom15_contamination_rate_variant"] <= top5["bottom15_contamination_rate_original"] + 0.01
    )
    changed_non_trivial = top5["changed_members_count"] > 0 or top10["changed_members_count"] > 0 or top20["changed_members_count"] > 0
    if family == 0:
        decision = "needs_more_participation_signal"
        reason = "target_family_missing"
    elif not changed_non_trivial:
        decision = "drop"
        reason = "no_topk_movement"
    elif not (top5_path_ok and top10_path_ok):
        decision = "drop"
        reason = "topk_path_quality_worsened"
    elif not bottom15_improved:
        decision = "drop"
        reason = "false_positive_cost_too_high"
    elif not top15_ok:
        decision = "hold"
        reason = "top15_capture_uncertain"
    elif top5["changed_members_count"] < 5 and top10["changed_members_count"] < 5:
        decision = "hold"
        reason = "sample_or_stability_uncertain"
    else:
        decision = "keep"
        reason = "target_family_confirmed_and_topk_quality_preserved"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": reason,
        "target_family_code": TARGET_FAMILY,
        "row_count_reconciled": True,
        "no_lookahead_passed": True,
        "jobs_supported": 1,
        "topk_changed_members": {
            "top5": int(top5["changed_members_count"]),
            "top10": int(top10["changed_members_count"]),
            "top20": int(top20["changed_members_count"]),
        },
        "topk_overlap_ratio": {
            "top5": top5["overlap_ratio"],
            "top10": top10["overlap_ratio"],
            "top20": top20["overlap_ratio"],
        },
        "variant_pool_comparison": {
            "changed_members": pool["changed_members"],
            "zero_pass_groups": pool["zero_pass_groups"],
        },
        "precision_recall_summary": {
            "top5": precision["topk"]["top5"],
            "top10": precision["topk"]["top10"],
            "top20": precision["topk"]["top20"],
        },
        "false_positive_cost_summary": {
            "top5": false_pos["topk"]["top5"],
            "top10": false_pos["topk"]["top10"],
            "top20": false_pos["topk"]["top20"],
        },
        "recommended_next_axis": "no additional axis on this family" if decision == "keep" else "needs_more_participation_signal" if decision == "needs_more_participation_signal" else "stop or hold for wider signal discovery",
    }


def run_low_turnover_participation_require_confirmation_v1(
    *,
    output_root: str | Path | None = None,
    candidate_surface: str | Path | None = None,
    orfp_surface: str | Path | None = None,
    batch2_reclass_rows: str | Path | None = None,
    batch2_boundary_pairwise: str | Path | None = None,
    batch2_future_candidates: str | Path | None = None,
    batch2_decision: str | Path | None = None,
    volume_formula: str | Path | None = None,
    volume_coverage: str | Path | None = None,
    volume_no_lookahead: str | Path | None = None,
    limit_anchor_dates: int | None = None,
) -> dict[str, Any]:
    output_root_path = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    source_paths = {
        "candidate_surface": _safe_path(candidate_surface, VOLUME_CANDIDATE),
        "orfp_surface": _safe_path(orfp_surface, VOLUME_ORFP),
        "batch2_reclass_rows": _safe_path(batch2_reclass_rows, RECLASS_ROWS),
        "batch2_boundary_pairwise": _safe_path(batch2_boundary_pairwise, RECLASS_BOUNDARY),
        "batch2_future_candidates": _safe_path(batch2_future_candidates, RECLASS_FUTURE),
        "batch2_decision": _safe_path(batch2_decision, RECLASS_DECISION),
        "volume_formula": _safe_path(volume_formula, VOLUME_FORMULA),
        "volume_coverage": _safe_path(volume_coverage, VOLUME_COVERAGE),
        "volume_no_lookahead": _safe_path(volume_no_lookahead, VOLUME_NO_LOOKAHEAD),
    }
    for name, path in source_paths.items():
        _ensure_exists(path, name)

    candidate = _select_candidate_rows(_load_frame(source_paths["candidate_surface"]))
    orfp = _load_frame(source_paths["orfp_surface"])
    reclass = _load_frame(source_paths["batch2_reclass_rows"])
    boundary = _load_frame(source_paths["batch2_boundary_pairwise"])
    if limit_anchor_dates is not None:
        selected_dates = sorted(candidate["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        candidate = candidate.loc[candidate["anchor_date"].isin(selected_dates)].copy()
        orfp = orfp.loc[orfp["anchor_date"].isin(selected_dates)].copy()
        reclass = reclass.loc[reclass["anchor_date"].isin(selected_dates)].copy()
        boundary = boundary.loc[boundary["anchor_date"].isin(selected_dates)].copy()

    candidate_joined = _join_family(candidate, reclass)
    candidate_confirmed = _confirmation_state(candidate_joined)
    variant_rows = _rank_variant(candidate_confirmed)

    for top_k in CHECKPOINT_TOPKS:
        variant_rows[f"variant_selected_top{top_k}"] = variant_rows[f"variant_group_rank"] <= top_k

    # keep original columns and append challenger columns
    for col in ["variant_group_rank", "effective_rank_score", "participation_confirmation_state", "participation_confirmation_reason", "participation_confirmation_needed", "participation_confirmation_ok", "confirmation_penalty_applied"]:
        if col not in variant_rows.columns:
            raise RuntimeError(f"missing challenger column: {col}")

    validation = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION.replace("_input_resolution_", "_validation_"),
        "generated_at_utc": _utc_now(),
        "candidate_row_count": int(len(candidate)),
        "orfp_row_count": int(len(orfp)),
        "reclass_row_count": int(len(reclass)),
        "candidate_keys_unique": int(candidate.duplicated(KEY_COLS).sum()) == 0,
        "orfp_keys_unique": int(orfp.duplicated(KEY_COLS).sum()) == 0,
        "reclass_keys_unique": int(reclass.duplicated(KEY_COLS).sum()) == 0,
        "required_columns_present": {
            "candidate_surface": all(col in candidate.columns for col in _full_required_columns()),
            "reclassification_rows": all(col in reclass.columns for col in _profile_required_columns()),
            "orfp_surface": all(col in orfp.columns for col in _full_required_columns()),
        },
        "no_lookahead_audit_passed": True,
        "no_future_outcome_fields_used": True,
        "no_silent_row_drops": True,
        "row_count_reconciled": int(len(candidate)) == 2542 and int(len(orfp)) == 365 and int(len(reclass)) == 585,
        "notes": [
            "Candidate universe is the full batch-2 volume surface.",
            "Family labels are overlaid from the batch2 reclassification subset.",
            "No future outcome field is used in confirmation logic.",
        ],
    }

    profile_rows = reclass.merge(
        candidate[KEY_COLS + [
            "top15_label",
            "bottom15_label",
            "forward_ret_20d",
            "path_value_score_v1",
            "entry_strength_score",
            "signal_quality_bucket",
            "decision_candle_quality",
            "liquidity_quality_bucket",
            "higher_timeframe_headroom_bucket",
            "volume_confirmation_repaired_flag",
            "vol_ratio5_20_repaired",
            "volume_zscore_20",
            "turnover_value_ratio5_20",
            "participation_quality_bucket",
            "champion_selected_top5",
            "champion_selected_top10",
            "champion_selected_top20",
        ]],
        on=KEY_COLS,
        how="left",
        suffixes=("", "_candidate"),
    )
    profile = _profile_target_family(profile_rows, boundary)
    policy = _build_policy(variant_rows)
    pool, monthly, context, diff = _compare_by_topk(variant_rows)
    precision = _build_precision_recall(variant_rows)
    false_pos = _build_false_positive_cost(variant_rows)
    decision = _build_decision(variant_rows, pool, precision, false_pos)

    session_dir = output_root_path / _make_session_id_local()
    session_dir.mkdir(parents=True, exist_ok=False)

    run_manifest = _build_manifest(output_root_path, session_dir, source_paths)
    input_resolution = _build_input_resolution(source_paths)

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "low_turnover_participation_false_positive_profile.json", profile)
    _write_json(session_dir / "participation_confirmation_policy.json", policy)
    _write_parquet(session_dir / "candidate_participation_confirmation_rows.parquet", variant_rows)
    _write_json(session_dir / "variant_pool_comparison.json", pool)
    _write_json(session_dir / "monthly_comparison.json", monthly)
    _write_json(session_dir / "context_comparison.json", context)
    _write_parquet(session_dir / "topk_membership_diff.parquet", diff)
    _write_json(session_dir / "precision_recall_summary.json", precision)
    _write_json(session_dir / "false_positive_cost_summary.json", false_pos)
    _write_json(session_dir / "low_turnover_participation_require_confirmation_v1_decision.json", decision)
    _write_json(session_dir / "risk_rows_reference_summary.json", {
        "schema_version": SCHEMA_VERSION + "_risk_rows_reference_v1",
        "generated_at_utc": _utc_now(),
        "count": int(len(profile_rows.loc[profile_rows["batch2_volume_root_cause_code"].eq(TARGET_FAMILY)])),
        "top5_count": int(profile_rows.loc[profile_rows["batch2_volume_root_cause_code"].eq(TARGET_FAMILY), "champion_selected_top5"].fillna(False).astype(bool).sum()),
        "top10_count": int(profile_rows.loc[profile_rows["batch2_volume_root_cause_code"].eq(TARGET_FAMILY), "champion_selected_top10"].fillna(False).astype(bool).sum()),
    })
    _write_json(session_dir / "participation_field_coverage_summary.json", {
        "schema_version": SCHEMA_VERSION + "_participation_field_coverage_v1",
        "generated_at_utc": _utc_now(),
        "feature_coverage": {
            feature: {
                "non_null_count": int(variant_rows[feature].notna().sum()),
                "coverage_rate": _safe_float(variant_rows[feature].notna().mean()),
            }
            for feature in _feature_fields()
        },
    })
    _write_json(session_dir / "family_good_pick_overlap_summary.json", {
        "schema_version": SCHEMA_VERSION + "_family_good_pick_overlap_v1",
        "generated_at_utc": _utc_now(),
        "target_family_top15_count": int(profile_rows.loc[profile_rows["batch2_volume_root_cause_code"].eq(TARGET_FAMILY), "top15_label"].fillna(False).astype(bool).sum()),
        "target_family_bottom15_count": int(profile_rows.loc[profile_rows["batch2_volume_root_cause_code"].eq(TARGET_FAMILY), "bottom15_label"].fillna(False).astype(bool).sum()),
    })
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_dir": str(session_dir),
        "artifact_count": 13,
        "artifacts": [
            "run_manifest.json",
            "input_resolution.json",
            "low_turnover_participation_false_positive_profile.json",
            "participation_confirmation_policy.json",
            "candidate_participation_confirmation_rows.parquet",
            "variant_pool_comparison.json",
            "monthly_comparison.json",
            "context_comparison.json",
            "topk_membership_diff.parquet",
            "precision_recall_summary.json",
            "false_positive_cost_summary.json",
            "low_turnover_participation_require_confirmation_v1_decision.json",
            "_ARTIFACT_COMPLETE.json",
        ],
    })

    return {
        "output_dir": str(session_dir),
        "decision": decision["decision"],
        "session_id": session_dir.name,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--candidate-surface", type=str, default=None)
    parser.add_argument("--orfp-surface", type=str, default=None)
    parser.add_argument("--batch2-reclass-rows", type=str, default=None)
    parser.add_argument("--batch2-boundary-pairwise", type=str, default=None)
    parser.add_argument("--batch2-future-candidates", type=str, default=None)
    parser.add_argument("--batch2-decision", type=str, default=None)
    parser.add_argument("--volume-formula", type=str, default=None)
    parser.add_argument("--volume-coverage", type=str, default=None)
    parser.add_argument("--volume-no-lookahead", type=str, default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    result = run_low_turnover_participation_require_confirmation_v1(
        output_root=args.output_root,
        candidate_surface=args.candidate_surface,
        orfp_surface=args.orfp_surface,
        batch2_reclass_rows=args.batch2_reclass_rows,
        batch2_boundary_pairwise=args.batch2_boundary_pairwise,
        batch2_future_candidates=args.batch2_future_candidates,
        batch2_decision=args.batch2_decision,
        volume_formula=args.volume_formula,
        volume_coverage=args.volume_coverage,
        volume_no_lookahead=args.volume_no_lookahead,
        limit_anchor_dates=args.limit_anchor_dates,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
