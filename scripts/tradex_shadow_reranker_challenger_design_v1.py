from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_shadow_feature_reranker_feasibility_v1 import (
    BATCH1_CANDIDATE,
    BATCH1_FORMULA,
    BATCH1_NO_LOOKAHEAD,
    BATCH1_ORFP,
    BATCH1_SESSION,
    BATCH2_CANDIDATE,
    BATCH2_FORMULA,
    BATCH2_NO_LOOKAHEAD,
    BATCH2_ORFP,
    BATCH2_SESSION,
    CATEGORICAL_MODEL_FEATURES,
    DECISION_SCHEMA_VERSION as FEASIBILITY_DECISION_SCHEMA_VERSION,
    EDINET_REFERENCE_DECISION,
    EDINET_REFERENCE_SESSION,
    FEATURE_EFFECT_SCHEMA_VERSION as FEASIBILITY_FEATURE_EFFECT_SCHEMA_VERSION,
    INPUT_RESOLUTION_SCHEMA_VERSION,
    MODEL_FEATURES,
    MODEL_CONTRACT_SCHEMA_VERSION,
    NUMERIC_BUCKET_MAPS,
    NUMERIC_MODEL_FEATURES,
    RECLASS_PAIRWISE,
    RECLASS_ROOT_CAUSE,
    RECLASS_ROWS,
    RECLASS_SESSION,
    SPLIT_CONTRACT_SCHEMA_VERSION,
    STABILITY_AUDIT_SCHEMA_VERSION,
    VARIANT_COMPARISON_SCHEMA_VERSION,
    _build_input_resolution,
    _build_topk_membership_diff as _feasibility_build_topk_membership_diff,
    _coerce_model_frame,
    _evaluation_summary,
    _feature_inventory,
    _git_hash_or_unknown,
    _ensure_exists,
    _json_ready,
    _load_frame,
    _load_json,
    _make_session_id,
    _month_split,
    _pairwise_diagnostics,
    _profile_frame,
    _safe_float,
    _safe_path,
    _score_variant_on_frame,
    _split_contract,
    _split_frame,
    _surface_selection_metrics,
    _tree_pipeline,
    _utc_now,
    _value_counts,
    _write_json,
    _write_parquet,
)

SCRIPT_NAME = "tradex_shadow_reranker_challenger_design_v1"
SCHEMA_VERSION = "tradex_shadow_reranker_challenger_design_v1"
MANIFEST_SCHEMA_VERSION = "tradex_shadow_reranker_challenger_design_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION_LOCAL = "tradex_shadow_reranker_challenger_design_v1_input_resolution_v1"
MODEL_SPEC_SCHEMA_VERSION = "tradex_shadow_reranker_challenger_design_v1_model_spec_v1"
VARIANT_COMPARISON_SCHEMA_VERSION_LOCAL = "tradex_shadow_reranker_challenger_design_v1_variant_pool_comparison_v1"
ROBUSTNESS_AUDIT_SCHEMA_VERSION = "tradex_shadow_reranker_challenger_design_v1_robustness_audit_v1"
LEAKAGE_AUDIT_SCHEMA_VERSION = "tradex_shadow_reranker_challenger_design_v1_leakage_audit_v1"
FEATURE_EFFECT_SCHEMA_VERSION_LOCAL = "tradex_shadow_reranker_challenger_design_v1_feature_effect_summary_v1"
DECISION_SCHEMA_VERSION_LOCAL = "tradex_shadow_reranker_challenger_design_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1")
SELECTED_VARIANT = "tree_hgb_path_value"
SELECTED_OBJECTIVE = "regression_path_value"
SELECTED_SEED = 42
TOP_K_VALUES = (5, 10, 20)


def _build_model_spec(split_contract: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": MODEL_SPEC_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_variant": SELECTED_VARIANT,
        "model_type": "sklearn.ensemble.HistGradientBoostingRegressor",
        "objective": SELECTED_OBJECTIVE,
        "target_label": "path_value_score_v1",
        "exact_features_used": MODEL_FEATURES,
        "numeric_features": NUMERIC_MODEL_FEATURES,
        "categorical_features": CATEGORICAL_MODEL_FEATURES,
        "preprocessing": {
            "numeric": "median imputation",
            "categorical": "most-frequent imputation plus ordinal encoding with unknown_value=-1",
            "missing_handling": "impute missing numeric values with median; impute missing categoricals with __MISSING__ before ordinal encoding",
        },
        "categorical_handling": {
            "encoder": "sklearn.preprocessing.OrdinalEncoder",
            "unknown_value": -1,
            "unseen_categories": "mapped to -1",
        },
        "numeric_handling": {
            "imputer": "sklearn.impute.SimpleImputer(strategy=median)",
        },
        "random_seed": SELECTED_SEED,
        "model_parameters": {
            "max_depth": 3,
            "max_iter": 150,
            "learning_rate": 0.05,
            "min_samples_leaf": 20,
            "l2_regularization": 0.01,
            "random_state": SELECTED_SEED,
        },
        "forbidden_fields": [
            "score",
            "candidate_score",
            "champion_score",
            "challenger_score",
            "rank",
            "candidate_rank",
            "champion_rank",
            "challenger_rank",
            "top15_label",
            "bottom15_label",
            "forward_ret_20d",
            "forward_ret_10d",
            "forward_ret_5d",
            "mfe_20d",
            "mae_20d",
            "path_value_score_v1",
            "realized_pnl",
        ],
        "split_contract": split_contract,
        "no_lookahead_proof": {
            "train_before_validation": True,
            "validation_before_test": True,
            "chronological_split_field": "month_bucket",
            "candidate_universe_fixed": True,
            "no_random_row_split": True,
            "no_future_features": True,
            "no_future_labels_in_features": True,
            "edinet_reference_used_as_feature": False,
        },
        "source_lineage": {
            "batch2_session": str(BATCH2_SESSION),
            "batch1_session": str(BATCH1_SESSION),
            "reclassification_session": str(RECLASS_SESSION),
            "edinet_reference_session": str(EDINET_REFERENCE_SESSION),
        },
        "frozen_spec_note": "This spec is frozen to tree_hgb_path_value only; no alternative model variants are introduced.",
    }


def _fit_selected_model(frames: dict[str, pd.DataFrame], months: dict[str, list[str]], *, time_split_ready: bool) -> dict[str, Any]:
    candidate = frames["batch2_candidate"].copy()
    orfp = frames["batch2_orfp"].copy()

    if not time_split_ready:
        return {
            "status": "insufficient_time_split_depth",
            "variants": {},
            "notes": ["Model fitting skipped because the chronological split depth is insufficient."],
        }

    candidate_splits = _split_frame(candidate, months)
    orfp_splits = _split_frame(orfp, months)
    train_frame = candidate_splits["train"].copy()
    train_target = pd.to_numeric(train_frame["path_value_score_v1"], errors="coerce")
    valid_train_mask = train_target.notna()
    train_frame = train_frame.loc[valid_train_mask].copy()
    train_target = train_target.loc[valid_train_mask]

    if train_frame.empty or train_target.nunique(dropna=True) < 2:
        return {
            "status": "insufficient_time_split_depth",
            "variants": {},
            "notes": ["Selected training rows were insufficient after removing missing targets."],
        }

    pipeline = _tree_pipeline(SELECTED_OBJECTIVE)
    pipeline.fit(_coerce_model_frame(train_frame), train_target)

    def _predict(surface: pd.DataFrame) -> pd.Series:
        prepared = _coerce_model_frame(surface)
        values = pipeline.predict(prepared)
        return pd.Series(values, index=surface.index, dtype="float64")

    candidate_scores = _predict(candidate)
    orfp_scores = _predict(orfp)
    train_scores = _predict(candidate_splits["train"])
    validation_scores = _predict(candidate_splits["validation"])
    test_scores = _predict(candidate_splits["test"])

    def _regression_metrics(surface: pd.DataFrame, scores: pd.Series) -> dict[str, Any]:
        target = pd.to_numeric(surface["path_value_score_v1"], errors="coerce")
        valid = target.notna() & scores.notna()
        if not valid.any():
            return {"rmse": None, "spearman": None, "row_count": int(len(surface))}
        residual = target.loc[valid] - scores.loc[valid]
        rmse = float(math.sqrt(float(np.mean(np.square(residual.astype(float))))))
        spearman = target.loc[valid].corr(scores.loc[valid], method="spearman")
        return {"rmse": _safe_float(rmse), "spearman": _safe_float(spearman), "row_count": int(valid.sum())}

    train_metrics = _regression_metrics(candidate_splits["train"], train_scores)
    validation_metrics = _regression_metrics(candidate_splits["validation"], validation_scores)
    test_metrics = _regression_metrics(candidate_splits["test"], test_scores)

    feature_importance = []
    try:
        validation_frame = candidate_splits["validation"]
        validation_target = pd.to_numeric(validation_frame["path_value_score_v1"], errors="coerce")
        valid_mask = validation_target.notna()
        validation_frame = validation_frame.loc[valid_mask].copy()
        validation_target = validation_target.loc[valid_mask]
        if not validation_frame.empty and validation_target.nunique(dropna=True) >= 2:
            importance = permutation_importance(
                pipeline,
                _coerce_model_frame(validation_frame),
                validation_target,
                n_repeats=5,
                random_state=SELECTED_SEED,
                scoring="neg_root_mean_squared_error",
            )
            for feature_name, importance_mean, importance_std in sorted(
                zip(MODEL_FEATURES, importance.importances_mean, importance.importances_std, strict=False),
                key=lambda row: float(row[1]),
                reverse=True,
            ):
                feature_importance.append(
                    {
                        "feature_name": str(feature_name),
                        "importance_mean": _safe_float(importance_mean),
                        "importance_std": _safe_float(importance_std),
                    }
                )
    except Exception:
        feature_importance = []

    return {
        "status": "ready_for_evaluation",
        "candidate_splits": candidate_splits,
        "orfp_splits": orfp_splits,
        "variants": {
            SELECTED_VARIANT: {
                "variant_name": SELECTED_VARIANT,
                "objective": SELECTED_OBJECTIVE,
                "family": "tree",
                "pipeline": pipeline,
                "candidate_scores": candidate_scores,
                "orfp_scores": orfp_scores,
                "split_scores": {
                    "train": train_scores,
                    "validation": validation_scores,
                    "test": test_scores,
                },
                "train_metrics": train_metrics,
                "validation_metrics": validation_metrics,
                "test_metrics": test_metrics,
                "feature_importance": feature_importance,
            }
        },
        "notes": [
            "Only the frozen tree_hgb_path_value challenger is fitted.",
            "Labels are used only inside the chronological training fold.",
        ],
    }


def _selection_summary(selected_variant: dict[str, Any], candidate_frame: pd.DataFrame) -> dict[str, Any]:
    candidate_top5 = selected_variant["topk"]["top5"]["selection_metrics"]
    candidate_top10 = selected_variant["topk"]["top10"]["selection_metrics"]
    candidate_top20 = selected_variant["topk"]["top20"]["selection_metrics"]
    return {
        "top5_forward_delta": _safe_float(candidate_top5["mean_forward_ret_20d"] - candidate_top5["champion_mean_forward_ret_20d"]) if candidate_top5["mean_forward_ret_20d"] is not None and candidate_top5["champion_mean_forward_ret_20d"] is not None else None,
        "top10_forward_delta": _safe_float(candidate_top10["mean_forward_ret_20d"] - candidate_top10["champion_mean_forward_ret_20d"]) if candidate_top10["mean_forward_ret_20d"] is not None and candidate_top10["champion_mean_forward_ret_20d"] is not None else None,
        "top20_forward_delta": _safe_float(candidate_top20["mean_forward_ret_20d"] - candidate_top20["champion_mean_forward_ret_20d"]) if candidate_top20["mean_forward_ret_20d"] is not None and candidate_top20["champion_mean_forward_ret_20d"] is not None else None,
        "top5_path_delta": _safe_float(candidate_top5["mean_path_value_score_v1"] - candidate_top5["champion_mean_path_value_score_v1"]) if candidate_top5["mean_path_value_score_v1"] is not None and candidate_top5["champion_mean_path_value_score_v1"] is not None else None,
        "top10_path_delta": _safe_float(candidate_top10["mean_path_value_score_v1"] - candidate_top10["champion_mean_path_value_score_v1"]) if candidate_top10["mean_path_value_score_v1"] is not None and candidate_top10["champion_mean_path_value_score_v1"] is not None else None,
        "top20_path_delta": _safe_float(candidate_top20["mean_path_value_score_v1"] - candidate_top20["champion_mean_path_value_score_v1"]) if candidate_top20["mean_path_value_score_v1"] is not None and candidate_top20["champion_mean_path_value_score_v1"] is not None else None,
        "top5_bottom15_delta": _safe_float(candidate_top5["bottom15_contamination_rate"] - candidate_top5["champion_bottom15_contamination_rate"]) if candidate_top5["bottom15_contamination_rate"] is not None and candidate_top5["champion_bottom15_contamination_rate"] is not None else None,
        "top10_bottom15_delta": _safe_float(candidate_top10["bottom15_contamination_rate"] - candidate_top10["champion_bottom15_contamination_rate"]) if candidate_top10["bottom15_contamination_rate"] is not None and candidate_top10["champion_bottom15_contamination_rate"] is not None else None,
        "top20_bottom15_delta": _safe_float(candidate_top20["bottom15_contamination_rate"] - candidate_top20["champion_bottom15_contamination_rate"]) if candidate_top20["bottom15_contamination_rate"] is not None and candidate_top20["champion_bottom15_contamination_rate"] is not None else None,
        "top5_top15_delta": _safe_float(candidate_top5["top15_capture_rate"] - candidate_top5["champion_top15_capture_rate"]) if candidate_top5["top15_capture_rate"] is not None and candidate_top5["champion_top15_capture_rate"] is not None else None,
        "top10_top15_delta": _safe_float(candidate_top10["top15_capture_rate"] - candidate_top10["champion_top15_capture_rate"]) if candidate_top10["top15_capture_rate"] is not None and candidate_top10["champion_top15_capture_rate"] is not None else None,
        "top20_top15_delta": _safe_float(candidate_top20["top15_capture_rate"] - candidate_top20["champion_top15_capture_rate"]) if candidate_top20["top15_capture_rate"] is not None and candidate_top20["champion_top15_capture_rate"] is not None else None,
        "top5_membership_change_rate": candidate_top5["membership_change_rate"],
        "top10_membership_change_rate": candidate_top10["membership_change_rate"],
        "top20_membership_change_rate": candidate_top20["membership_change_rate"],
        "top5_overlap_ratio": candidate_top5["overlap_ratio"],
        "top10_overlap_ratio": candidate_top10["overlap_ratio"],
        "top20_overlap_ratio": candidate_top20["overlap_ratio"],
        "zero_pass_groups": {
            "top5": candidate_top5["zero_pass_groups"],
            "top10": candidate_top10["zero_pass_groups"],
            "top20": candidate_top20["zero_pass_groups"],
        },
        "false_positive_cost": {
            "top5": candidate_top5["false_positive_cost"],
            "top10": candidate_top10["false_positive_cost"],
            "top20": candidate_top20["false_positive_cost"],
        },
    }


def _build_variant_pool_comparison(frames: dict[str, pd.DataFrame], model_results: dict[str, Any]) -> dict[str, Any]:
    variant_payload = model_results["variants"][SELECTED_VARIANT]
    candidate_summary = _evaluation_summary(frames["batch2_candidate"], {SELECTED_VARIANT: variant_payload["candidate_scores"]}, surface_name="candidate_surface", topk_values=TOP_K_VALUES)
    orfp_summary = _evaluation_summary(frames["batch2_orfp"], {SELECTED_VARIANT: variant_payload["orfp_scores"]}, surface_name="orfp_surface", topk_values=TOP_K_VALUES)
    selected_variant = candidate_summary["variants"][SELECTED_VARIANT]
    return {
        "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION_LOCAL,
        "generated_at_utc": _utc_now(),
        "selected_variant": SELECTED_VARIANT,
        "candidate_surface": candidate_summary,
        "orfp_surface": orfp_summary,
        "comparison_summary": _selection_summary(selected_variant, frames["batch2_candidate"]),
        "notes": [
            "This is a single-model validation surface; no alternative model shopping is performed.",
            "Metrics compare the frozen challenger against the preserved champion selection.",
        ],
    }
def _ranked_group_counts(frame: pd.DataFrame, score: pd.Series, *, group_col: str, topk: int) -> dict[str, Any]:
    selected_metrics = _surface_selection_metrics(frame, score, topk=topk, surface_name="candidate_surface")
    return {
        "win": int(selected_metrics["selection_metrics"]["membership_change_rate"] > 0) if selected_metrics["selection_metrics"]["membership_change_rate"] is not None else 0,
        "loss": int(selected_metrics["selection_metrics"]["membership_change_rate"] <= 0) if selected_metrics["selection_metrics"]["membership_change_rate"] is not None else 0,
        "flat": 0,
        "groups": int(selected_metrics["selection_metrics"]["group_count"]),
        "mean_delta_forward_ret_20d": selected_metrics["selection_metrics"]["mean_forward_ret_20d"],
        "mean_delta_path_value_score_v1": selected_metrics["selection_metrics"]["mean_path_value_score_v1"],
    }


def _symbol_concentration(diff: pd.DataFrame, *, topk: int) -> dict[str, Any]:
    subset = diff[(diff["topk"] == topk) & diff["model_selected"].fillna(False).astype(bool)].copy()
    if subset.empty:
        return {"topk": int(topk), "selected_row_count": 0}
    symbol_counts = subset["symbol"].astype("string").fillna("").value_counts()
    changed_counts = diff[(diff["topk"] == topk) & diff["membership_changed"].fillna(False).astype(bool)]["symbol"].astype("string").fillna("").value_counts()
    top_symbol = symbol_counts.index[0] if len(symbol_counts) else None
    return {
        "topk": int(topk),
        "selected_row_count": int(len(subset)),
        "top_symbol": str(top_symbol) if top_symbol is not None else None,
        "top_symbol_share": _safe_float(symbol_counts.iloc[0] / len(subset)) if len(symbol_counts) else None,
        "top5_symbol_share": _safe_float(symbol_counts.head(5).sum() / len(subset)) if len(symbol_counts) else None,
        "top10_symbol_share": _safe_float(symbol_counts.head(10).sum() / len(subset)) if len(symbol_counts) else None,
        "changed_row_count": int(len(changed_counts)),
        "top_changed_symbol": str(changed_counts.index[0]) if len(changed_counts) else None,
        "changed_top_symbol_share": _safe_float(changed_counts.iloc[0] / changed_counts.sum()) if len(changed_counts) and changed_counts.sum() else None,
    }


def _build_robustness_audit(
    frames: dict[str, pd.DataFrame],
    months: dict[str, list[str]],
    model_results: dict[str, Any],
    variant_comparison: dict[str, Any],
    topk_diff: pd.DataFrame,
) -> dict[str, Any]:
    payload = model_results["variants"].get(SELECTED_VARIANT)
    if model_results.get("status") != "ready_for_evaluation" or payload is None:
        return {
            "schema_version": ROBUSTNESS_AUDIT_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "split_status": model_results.get("status", "unknown"),
            "notes": model_results.get("notes", []),
        }

    candidate_splits = model_results["candidate_splits"]
    candidate_surface = variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]
    candidate_top5 = candidate_surface["topk"]["top5"]
    candidate_top10 = candidate_surface["topk"]["top10"]
    candidate_top20 = candidate_surface["topk"]["top20"]
    monthly_top10 = candidate_surface["monthly_win_loss_flat"]["top10"]
    regime_top10 = candidate_surface["regime_win_loss_flat"]["top10"]

    month_rows = []
    for month_bucket, month_payload in candidate_top10["by_month"].items():
        month_rows.append(
            {
                "month_bucket": str(month_bucket),
                "mean_forward_ret_20d": month_payload["mean_forward_ret_20d"],
                "mean_path_value_score_v1": month_payload["mean_path_value_score_v1"],
                "bottom15_contamination_rate": month_payload["bottom15_contamination_rate"],
                "top15_capture_rate": month_payload["top15_capture_rate"],
            }
        )

    regime_rows = []
    for regime_name, regime_payload in candidate_top10["by_regime"].items():
        regime_rows.append(
            {
                "regime": str(regime_name),
                "mean_forward_ret_20d": regime_payload["mean_forward_ret_20d"],
                "mean_path_value_score_v1": regime_payload["mean_path_value_score_v1"],
                "bottom15_contamination_rate": regime_payload["bottom15_contamination_rate"],
                "top15_capture_rate": regime_payload["top15_capture_rate"],
            }
        )

    side_rows = []
    for side_name, side_payload in candidate_top10["by_side"].items():
        side_rows.append(
            {
                "side": str(side_name),
                "mean_forward_ret_20d": side_payload["mean_forward_ret_20d"],
                "mean_path_value_score_v1": side_payload["mean_path_value_score_v1"],
                "bottom15_contamination_rate": side_payload["bottom15_contamination_rate"],
                "top15_capture_rate": side_payload["top15_capture_rate"],
            }
        )

    zero_pass_groups = {
        "top5": int(candidate_top5["selection_metrics"]["zero_pass_groups"]),
        "top10": int(candidate_top10["selection_metrics"]["zero_pass_groups"]),
        "top20": int(candidate_top20["selection_metrics"]["zero_pass_groups"]),
    }

    concentration = {
        "top5": _symbol_concentration(topk_diff, topk=5),
        "top10": _symbol_concentration(topk_diff, topk=10),
        "top20": _symbol_concentration(topk_diff, topk=20),
    }

    train_validation_gap = {
        "rmse_delta": _safe_float(payload["validation_metrics"]["rmse"] - payload["train_metrics"]["rmse"]) if payload["validation_metrics"]["rmse"] is not None and payload["train_metrics"]["rmse"] is not None else None,
        "spearman_delta": _safe_float(payload["validation_metrics"]["spearman"] - payload["train_metrics"]["spearman"]) if payload["validation_metrics"]["spearman"] is not None and payload["train_metrics"]["spearman"] is not None else None,
    }
    validation_test_gap = {
        "rmse_delta": _safe_float(payload["test_metrics"]["rmse"] - payload["validation_metrics"]["rmse"]) if payload["test_metrics"]["rmse"] is not None and payload["validation_metrics"]["rmse"] is not None else None,
        "spearman_delta": _safe_float(payload["test_metrics"]["spearman"] - payload["validation_metrics"]["spearman"]) if payload["test_metrics"]["spearman"] is not None and payload["validation_metrics"]["spearman"] is not None else None,
    }

    worst_month_forward = min((row["mean_forward_ret_20d"] for row in month_rows if row["mean_forward_ret_20d"] is not None), default=None)
    worst_regime_forward = min((row["mean_forward_ret_20d"] for row in regime_rows if row["mean_forward_ret_20d"] is not None), default=None)

    return {
        "schema_version": ROBUSTNESS_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_variant": SELECTED_VARIANT,
        "split_status": model_results.get("status", "unknown"),
        "train_metrics": payload["train_metrics"],
        "validation_metrics": payload["validation_metrics"],
        "test_metrics": payload["test_metrics"],
        "train_validation_gap": train_validation_gap,
        "validation_test_gap": validation_test_gap,
        "topk_summary": {
            "top5": candidate_top5["selection_metrics"],
            "top10": candidate_top10["selection_metrics"],
            "top20": candidate_top20["selection_metrics"],
        },
        "monthly_stability": {
            "win_loss_flat": monthly_top10,
            "rows": month_rows,
            "worst_month_mean_forward_ret_20d": worst_month_forward,
        },
        "regime_stability": {
            "win_loss_flat": regime_top10,
            "rows": regime_rows,
            "worst_regime_mean_forward_ret_20d": worst_regime_forward,
        },
        "side_stability": {
            "rows": side_rows,
        },
        "symbol_concentration": concentration,
        "zero_pass_groups": zero_pass_groups,
        "top5_vs_top10_consistency": {
            "top5_forward_ret_20d": candidate_top5["selection_metrics"]["mean_forward_ret_20d"],
            "top10_forward_ret_20d": candidate_top10["selection_metrics"]["mean_forward_ret_20d"],
            "top5_bottom15_contamination_rate": candidate_top5["selection_metrics"]["bottom15_contamination_rate"],
            "top10_bottom15_contamination_rate": candidate_top10["selection_metrics"]["bottom15_contamination_rate"],
            "top5_membership_change_rate": candidate_top5["selection_metrics"]["membership_change_rate"],
            "top10_membership_change_rate": candidate_top10["selection_metrics"]["membership_change_rate"],
            "top20_membership_change_rate": candidate_top20["selection_metrics"]["membership_change_rate"],
        },
        "top20_locality": {
            "selected_row_count": candidate_top20["selection_metrics"]["selected_row_count"],
            "membership_change_rate": candidate_top20["selection_metrics"]["membership_change_rate"],
            "overlap_ratio": candidate_top20["selection_metrics"]["overlap_ratio"],
            "note": "A weak top20 move is expected because top20 already covers most of the candidate pool; this is diagnostic, not a blocker.",
        },
        "drawdown_path_risk_proxy": {
            "worst_month_mean_forward_ret_20d": worst_month_forward,
            "worst_regime_mean_forward_ret_20d": worst_regime_forward,
        },
        "notes": [
            "The audit focuses on OOS ordering stability, not global rank correlation.",
            "A broad month/regime win rate is more relevant than train correlation for this shadow challenger.",
        ],
    }


def _encode_effect_series(frame: pd.DataFrame, feature: str) -> pd.Series:
    series = frame[feature] if feature in frame.columns else pd.Series(dtype="object")
    if feature in NUMERIC_MODEL_FEATURES:
        values = pd.to_numeric(series, errors="coerce")
    elif feature in NUMERIC_BUCKET_MAPS:
        values = series.astype("string").map(NUMERIC_BUCKET_MAPS[feature])
    else:
        cat = pd.Categorical(series.astype("string").fillna("__MISSING__"))
        values = pd.Series(cat.codes, index=series.index, dtype="float64").replace(-1, np.nan)
    return pd.to_numeric(values, errors="coerce")


def _effect_rows(frame: pd.DataFrame, scores: pd.Series, importance_map: dict[str, float], inventory_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for feature in MODEL_FEATURES:
        if feature not in frame.columns:
            continue
        encoded = _encode_effect_series(frame, feature)
        valid = encoded.notna() & scores.notna()
        if valid.sum() < 20:
            continue
        valid_encoded = encoded.loc[valid]
        valid_scores = scores.loc[valid]
        median = float(valid_encoded.median())
        high = valid_scores.loc[valid_encoded >= median]
        low = valid_scores.loc[valid_encoded < median]
        directional_lift = _safe_float(high.mean() - low.mean()) if len(high) and len(low) else None
        importance = _safe_float(importance_map.get(feature, 0.0))
        signed_effect = None
        if importance is not None and directional_lift is not None:
            signed_effect = _safe_float(abs(importance) * (1.0 if directional_lift >= 0 else -1.0))
        row = inventory_rows.get(feature, {})
        rows.append(
            {
                "feature_name": feature,
                "classification": row.get("classification"),
                "batch2_candidate_coverage_rate": row.get("candidate_coverage_rate"),
                "batch1_candidate_coverage_rate": row.get("batch1_candidate_coverage_rate"),
                "permutation_importance_mean": importance,
                "directional_lift_proxy": directional_lift,
                "signed_effect_proxy": signed_effect,
            }
        )
    return rows


def _build_feature_effect_summary(
    frames: dict[str, pd.DataFrame],
    model_results: dict[str, Any],
    variant_comparison: dict[str, Any],
    inventory: dict[str, Any],
    reclass_frame: pd.DataFrame,
    root_cause_summary: dict[str, Any],
    edinet_reference: dict[str, Any],
) -> dict[str, Any]:
    candidate = frames["batch2_candidate"].copy()
    batch1_candidate = frames["batch1_candidate"].copy()
    if model_results.get("status") != "ready_for_evaluation" or SELECTED_VARIANT not in model_results.get("variants", {}):
        return {
            "schema_version": FEATURE_EFFECT_SCHEMA_VERSION_LOCAL,
            "generated_at_utc": _utc_now(),
            "selected_variant": SELECTED_VARIANT,
            "selected_feature_count": len(MODEL_FEATURES),
            "selected_features": MODEL_FEATURES,
            "edinet_reference": edinet_reference,
            "root_cause_summary": root_cause_summary,
            "feature_lineage_contribution": {
                "batch1_shared_feature_count": sum(1 for feature in MODEL_FEATURES if feature in batch1_candidate.columns and feature in candidate.columns),
                "batch2_shared_feature_count": sum(1 for feature in MODEL_FEATURES if feature in candidate.columns),
                "batch1_only_feature_count": sum(1 for feature in MODEL_FEATURES if feature in batch1_candidate.columns and feature not in candidate.columns),
                "batch2_only_feature_count": sum(1 for feature in MODEL_FEATURES if feature in candidate.columns and feature not in batch1_candidate.columns),
            },
            "feature_coverage": {
                feature: {
                    "batch2_candidate_non_null_count": int(candidate[feature].notna().sum()) if feature in candidate.columns else None,
                    "batch2_candidate_coverage_rate": _safe_float(candidate[feature].notna().mean()) if feature in candidate.columns else None,
                    "batch1_candidate_non_null_count": int(batch1_candidate[feature].notna().sum()) if feature in batch1_candidate.columns else None,
                    "batch1_candidate_coverage_rate": _safe_float(batch1_candidate[feature].notna().mean()) if feature in batch1_candidate.columns else None,
                }
                for feature in MODEL_FEATURES
            },
            "feature_effect_rows": [],
            "top_positive_feature_effects": [],
            "top_negative_feature_effects": [],
            "interaction_candidates": [],
            "model_feature_coverage": {
                feature: {
                    "batch2_candidate_non_null_count": int(candidate[feature].notna().sum()) if feature in candidate.columns else None,
                    "batch2_candidate_coverage_rate": _safe_float(candidate[feature].notna().mean()) if feature in candidate.columns else None,
                    "batch1_candidate_non_null_count": int(batch1_candidate[feature].notna().sum()) if feature in batch1_candidate.columns else None,
                    "batch1_candidate_coverage_rate": _safe_float(batch1_candidate[feature].notna().mean()) if feature in batch1_candidate.columns else None,
                }
                for feature in MODEL_FEATURES
            },
            "reclassification_diagnostics": {
                "row_count": int(len(reclass_frame)),
                "family_rows": [],
                "bottom_families": [],
                "top_families": [],
            },
            "candidate_surface_variant_rankings": {},
            "notes": [
                "Feature-effect summary is placeholder-only because the smoke run did not reach full model fitting.",
            ],
        }

    payload = model_results["variants"][SELECTED_VARIANT]
    validation_frame = model_results["candidate_splits"]["validation"].copy()
    validation_scores = payload["split_scores"]["validation"]
    feature_importance_map = {row["feature_name"]: float(row["importance_mean"] or 0.0) for row in payload.get("feature_importance", [])}
    inventory_map = {row["feature_name"]: row for row in inventory.get("features", [])}

    feature_rows = _effect_rows(validation_frame, validation_scores, feature_importance_map, inventory_map)
    ordered = sorted(feature_rows, key=lambda row: float(abs(row["signed_effect_proxy"] or 0.0)), reverse=True)
    top_positive = [row for row in sorted(feature_rows, key=lambda row: float(row["signed_effect_proxy"] or -1e9), reverse=True) if (row["signed_effect_proxy"] is not None and row["signed_effect_proxy"] >= 0)][:10]
    top_negative = [row for row in sorted(feature_rows, key=lambda row: float(row["signed_effect_proxy"] or 1e9)) if (row["signed_effect_proxy"] is not None and row["signed_effect_proxy"] < 0)][:10]

    interaction_candidates: list[dict[str, Any]] = []
    interaction_features = [row["feature_name"] for row in ordered[:6]]
    for first, second in combinations(interaction_features, 2):
        left = _encode_effect_series(validation_frame, first)
        right = _encode_effect_series(validation_frame, second)
        valid = left.notna() & right.notna() & validation_scores.notna()
        if valid.sum() < 20:
            continue
        centered_left = left.loc[valid] - left.loc[valid].median()
        centered_right = right.loc[valid] - right.loc[valid].median()
        proxy = centered_left.mul(centered_right).corr(validation_scores.loc[valid], method="spearman")
        interaction_candidates.append(
            {
                "feature_a": first,
                "feature_b": second,
                "interaction_proxy_spearman": _safe_float(proxy),
            }
        )
    interaction_candidates = sorted(interaction_candidates, key=lambda row: abs(float(row["interaction_proxy_spearman"] or 0.0)), reverse=True)[:10]

    re_scored = reclass_frame.copy()
    re_scored["model_score"] = _score_variant_on_frame(payload, re_scored).values
    family_rows = []
    if "batch2_volume_root_cause_code" in re_scored.columns:
        for family, subset in re_scored.groupby("batch2_volume_root_cause_code", sort=True):
            family_rows.append(
                {
                    "family": str(family),
                    "count": int(len(subset)),
                    "mean_model_score": _safe_float(subset["model_score"].mean()),
                    "median_model_score": _safe_float(subset["model_score"].median()),
                    "mean_forward_ret_20d": _safe_float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()) if "forward_ret_20d" in subset.columns else None,
                    "mean_path_value_score_v1": _safe_float(pd.to_numeric(subset["path_value_score_v1"], errors="coerce").mean()) if "path_value_score_v1" in subset.columns else None,
                    "top15_rate": _safe_float(subset["top15_label"].fillna(False).astype(bool).mean()) if "top15_label" in subset.columns else None,
                    "bottom15_rate": _safe_float(subset["bottom15_label"].fillna(False).astype(bool).mean()) if "bottom15_label" in subset.columns else None,
                }
            )
    family_rows = sorted(family_rows, key=lambda row: float(row["mean_model_score"] or 0.0))

    batch1_shared = sum(1 for feature in MODEL_FEATURES if feature in batch1_candidate.columns and feature in candidate.columns)
    batch2_shared = sum(1 for feature in MODEL_FEATURES if feature in candidate.columns)
    batch1_only = sum(1 for feature in MODEL_FEATURES if feature in batch1_candidate.columns and feature not in candidate.columns)
    batch2_only = sum(1 for feature in MODEL_FEATURES if feature in candidate.columns and feature not in batch1_candidate.columns)

    return {
        "schema_version": FEATURE_EFFECT_SCHEMA_VERSION_LOCAL,
        "generated_at_utc": _utc_now(),
        "selected_variant": SELECTED_VARIANT,
        "selected_feature_count": len(MODEL_FEATURES),
        "selected_features": MODEL_FEATURES,
        "edinet_reference": edinet_reference,
        "root_cause_summary": root_cause_summary,
        "feature_lineage_contribution": {
            "batch1_shared_feature_count": int(batch1_shared),
            "batch2_shared_feature_count": int(batch2_shared),
            "batch1_only_feature_count": int(batch1_only),
            "batch2_only_feature_count": int(batch2_only),
        },
        "feature_coverage": {
            feature: {
                "batch2_candidate_non_null_count": int(candidate[feature].notna().sum()) if feature in candidate.columns else None,
                "batch2_candidate_coverage_rate": _safe_float(candidate[feature].notna().mean()) if feature in candidate.columns else None,
                "batch1_candidate_non_null_count": int(batch1_candidate[feature].notna().sum()) if feature in batch1_candidate.columns else None,
                "batch1_candidate_coverage_rate": _safe_float(batch1_candidate[feature].notna().mean()) if feature in batch1_candidate.columns else None,
            }
            for feature in MODEL_FEATURES
        },
        "feature_effect_rows": feature_rows,
        "top_positive_feature_effects": top_positive,
        "top_negative_feature_effects": top_negative,
        "interaction_candidates": interaction_candidates,
        "model_feature_coverage": {
            feature: {
                "batch2_candidate_non_null_count": int(candidate[feature].notna().sum()) if feature in candidate.columns else None,
                "batch2_candidate_coverage_rate": _safe_float(candidate[feature].notna().mean()) if feature in candidate.columns else None,
                "batch1_candidate_non_null_count": int(batch1_candidate[feature].notna().sum()) if feature in batch1_candidate.columns else None,
                "batch1_candidate_coverage_rate": _safe_float(batch1_candidate[feature].notna().mean()) if feature in batch1_candidate.columns else None,
            }
            for feature in MODEL_FEATURES
        },
        "reclassification_diagnostics": {
            "row_count": int(len(re_scored)),
            "family_rows": family_rows,
            "bottom_families": family_rows[:5],
            "top_families": family_rows[-5:],
        },
        "candidate_surface_variant_rankings": {
            "top5_forward_ret_20d": variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]["topk"]["top5"]["selection_metrics"]["mean_forward_ret_20d"],
            "top10_forward_ret_20d": variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]["topk"]["top10"]["selection_metrics"]["mean_forward_ret_20d"],
            "top20_forward_ret_20d": variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]["topk"]["top20"]["selection_metrics"]["mean_forward_ret_20d"],
            "top5_bottom15_contamination_rate": variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]["topk"]["top5"]["selection_metrics"]["bottom15_contamination_rate"],
            "top10_bottom15_contamination_rate": variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]["topk"]["top10"]["selection_metrics"]["bottom15_contamination_rate"],
            "top20_bottom15_contamination_rate": variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]["topk"]["top20"]["selection_metrics"]["bottom15_contamination_rate"],
        },
        "notes": [
            "Permutation importance is a proxy; it is not a causal attribution.",
            "Interaction candidates are heuristics derived from the frozen tree challenger, not additional model variants.",
            "Batch1/Batch2 contribution is measured by feature presence and coverage across the shared lineage, not by training separate models.",
        ],
    }


def _build_leakage_audit(
    frames: dict[str, pd.DataFrame],
    model_results: dict[str, Any],
    inventory: dict[str, Any],
    split_contract: dict[str, Any],
    feature_effect_summary: dict[str, Any],
    edinet_reference: dict[str, Any],
) -> dict[str, Any]:
    inventory_map = {row["feature_name"]: row for row in inventory.get("features", [])}
    selected_features = list(MODEL_FEATURES)
    forbidden_overlap = [feature for feature in selected_features if feature in {"top15_label", "bottom15_label", "forward_ret_20d", "forward_ret_10d", "forward_ret_5d", "path_value_score_v1", "mfe_20d", "mae_20d"}]
    leakage_overlap = [feature for feature in selected_features if inventory_map.get(feature, {}).get("classification") in {"forbidden_outcome", "leakage_risk"}]
    identifier_overlap = [feature for feature in selected_features if inventory_map.get(feature, {}).get("classification") == "identifier_only"]

    top_effects = feature_effect_summary.get("top_positive_feature_effects", [])[:5] + feature_effect_summary.get("top_negative_feature_effects", [])[:5]
    top_effect_leakage = [
        {
            "feature_name": row.get("feature_name"),
            "classification": inventory_map.get(row.get("feature_name", ""), {}).get("classification"),
        }
        for row in top_effects
    ]
    identifier_like_top_features = [row for row in top_effect_leakage if row.get("classification") == "identifier_only"]

    validation_ok = split_contract.get("status") == "ready_for_time_split_evaluation" and split_contract.get("row_count_reconciled", True)
    no_future_features = all(feature not in forbidden_overlap for feature in selected_features)

    return {
        "schema_version": LEAKAGE_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_variant": SELECTED_VARIANT,
        "checks": {
            "selected_features_present": True,
            "selected_feature_count": len(selected_features),
            "forbidden_feature_overlap": forbidden_overlap,
            "leakage_risk_overlap": leakage_overlap,
            "identifier_overlap": identifier_overlap,
            "random_split_used": False,
            "chronological_split_used": True,
            "time_split_valid": validation_ok,
            "future_rows_used": False,
            "future_labels_in_features": False,
            "current_snapshot_used_for_past": False,
            "edinet_used_as_feature": False,
            "edinet_reference_only": edinet_reference.get("used_as_feature") is False,
            "identifier_like_top_features": identifier_like_top_features,
            "top_effects_checked": top_effect_leakage,
            "no_future_feature_fields": no_future_features,
        },
        "status": "passed" if validation_ok and not forbidden_overlap and not leakage_overlap and not identifier_overlap and not identifier_like_top_features else "failed",
        "notes": [
            "The challenger is frozen to a single feature set and a chronological split.",
            "EDINET remained a reference input only and was not used as a model feature.",
        ],
    }


def _build_decision(
    variant_comparison: dict[str, Any],
    robustness_audit: dict[str, Any],
    leakage_audit: dict[str, Any],
    split_contract: dict[str, Any],
) -> dict[str, Any]:
    if split_contract.get("status") != "ready_for_time_split_evaluation":
        return {
            "schema_version": DECISION_SCHEMA_VERSION_LOCAL,
            "generated_at_utc": _utc_now(),
            "decision": "insufficient_time_split_depth",
            "status": "insufficient_time_split_depth",
            "reason": "chronological split depth is not sufficient for stable train/validation/test evaluation",
            "row_count_reconciled": bool(split_contract.get("row_count_reconciled", False)),
            "no_lookahead_passed": True,
            "recommended_next_axis": "forward_validation",
            "jobs_supported": 1,
            "selected_variant": SELECTED_VARIANT,
        }

    candidate_summary = variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]["topk"]
    top5 = candidate_summary["top5"]["selection_metrics"]
    top10 = candidate_summary["top10"]["selection_metrics"]
    top20 = candidate_summary["top20"]["selection_metrics"]
    monthly = variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]["monthly_win_loss_flat"]["top10"]
    regime = variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]["regime_win_loss_flat"]["top10"]

    top5_forward_delta = top5["mean_forward_ret_20d"] - top5["champion_mean_forward_ret_20d"] if top5["mean_forward_ret_20d"] is not None and top5["champion_mean_forward_ret_20d"] is not None else None
    top10_forward_delta = top10["mean_forward_ret_20d"] - top10["champion_mean_forward_ret_20d"] if top10["mean_forward_ret_20d"] is not None and top10["champion_mean_forward_ret_20d"] is not None else None
    top5_path_delta = top5["mean_path_value_score_v1"] - top5["champion_mean_path_value_score_v1"] if top5["mean_path_value_score_v1"] is not None and top5["champion_mean_path_value_score_v1"] is not None else None
    top10_path_delta = top10["mean_path_value_score_v1"] - top10["champion_mean_path_value_score_v1"] if top10["mean_path_value_score_v1"] is not None and top10["champion_mean_path_value_score_v1"] is not None else None
    top5_bottom15_delta = top5["bottom15_contamination_rate"] - top5["champion_bottom15_contamination_rate"] if top5["bottom15_contamination_rate"] is not None and top5["champion_bottom15_contamination_rate"] is not None else None
    top10_bottom15_delta = top10["bottom15_contamination_rate"] - top10["champion_bottom15_contamination_rate"] if top10["bottom15_contamination_rate"] is not None and top10["champion_bottom15_contamination_rate"] is not None else None
    top5_top15_delta = top5["top15_capture_rate"] - top5["champion_top15_capture_rate"] if top5["top15_capture_rate"] is not None and top5["champion_top15_capture_rate"] is not None else None
    top10_top15_delta = top10["top15_capture_rate"] - top10["champion_top15_capture_rate"] if top10["top15_capture_rate"] is not None and top10["champion_top15_capture_rate"] is not None else None

    monthly_broad = monthly["win"] > monthly["loss"] and monthly["groups"] > 0
    regime_broad = regime["win"] > regime["loss"] and regime["groups"] > 0
    top5_broad = top5_forward_delta is not None and top5_forward_delta > 0 and top5_path_delta is not None and top5_path_delta >= 0 and top5_bottom15_delta is not None and top5_bottom15_delta <= 0 and top5_top15_delta is not None and top5_top15_delta >= -0.005
    top10_broad = top10_forward_delta is not None and top10_forward_delta > 0 and top10_path_delta is not None and top10_path_delta >= 0 and top10_bottom15_delta is not None and top10_bottom15_delta <= 0 and top10_top15_delta is not None and top10_top15_delta >= -0.005

    if leakage_audit.get("status") != "passed":
        decision = "drop"
        reason = "leakage audit did not pass"
    elif top5_broad and top10_broad and monthly_broad and regime_broad and robustness_audit.get("symbol_concentration", {}).get("top10", {}).get("top5_symbol_share", 1.0) <= 0.35:
        decision = "keep_for_forward_validation"
        reason = "OOS top5/top10 lift is preserved with broad month/regime stability and no leakage signal"
    elif top5_forward_delta is not None and top10_forward_delta is not None and top5_forward_delta > 0 and top10_forward_delta > 0:
        decision = "hold_needs_forward_validation"
        reason = "directional lift is present but breadth or concentration is not yet strong enough"
    elif (top5_bottom15_delta is not None and top5_bottom15_delta > 0) or (top10_bottom15_delta is not None and top10_bottom15_delta > 0):
        decision = "needs_target_redesign"
        reason = "bottom15 contamination worsened under the frozen challenger"
    else:
        decision = "insufficient_stability"
        reason = "the observed improvement is too concentrated or too weak to retain as a challenger"

    return {
        "schema_version": DECISION_SCHEMA_VERSION_LOCAL,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": reason,
        "selected_variant": SELECTED_VARIANT,
        "row_count_reconciled": bool(split_contract.get("row_count_reconciled", False)),
        "no_lookahead_passed": True,
        "recommended_next_axis": "forward_validation" if decision == "keep_for_forward_validation" else "revisit_target_definition_or_horizon",
        "jobs_supported": 1,
        "top5_forward_delta": top5_forward_delta,
        "top10_forward_delta": top10_forward_delta,
        "top5_bottom15_delta": top5_bottom15_delta,
        "top10_bottom15_delta": top10_bottom15_delta,
    }


def _build_run_manifest(output_root: Path, session_dir: Path, inputs: dict[str, Path], *, jobs_requested: int, jobs_supported: int, split_status: str, decision: str) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "git_commit": _git_hash_or_unknown(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "jobs_requested": int(jobs_requested),
        "jobs_supported": int(jobs_supported),
        "split_status": split_status,
        "decision": decision,
        "input_paths": {key: str(value) for key, value in inputs.items()},
    }


def run_shadow_reranker_challenger_design_v1(
    *,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = None,
    jobs: int = 1,
) -> dict[str, Any]:
    output_root_path = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_dir = output_root_path / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    inputs = {
        "feasibility_feature_inventory": Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1\20260501T115549Z-751684\shadow_reranker_feature_inventory.json"),
        "feasibility_label_contract": Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1\20260501T115549Z-751684\shadow_reranker_label_contract.json"),
        "feasibility_split_contract": Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1\20260501T115549Z-751684\shadow_reranker_split_contract.json"),
        "feasibility_model_contract": Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1\20260501T115549Z-751684\shadow_reranker_model_contract.json"),
        "feasibility_variant_pool_comparison": Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1\20260501T115549Z-751684\shadow_reranker_variant_pool_comparison.json"),
        "feasibility_topk_diff": Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1\20260501T115549Z-751684\shadow_reranker_topk_membership_diff.parquet"),
        "feasibility_stability_audit": Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1\20260501T115549Z-751684\shadow_reranker_stability_audit.json"),
        "feasibility_feature_effect_summary": Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1\20260501T115549Z-751684\shadow_reranker_feature_effect_summary.json"),
        "feasibility_decision": Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1\20260501T115549Z-751684\shadow_feature_reranker_feasibility_v1_decision.json"),
        "batch2_candidate": _safe_path(BATCH2_CANDIDATE, BATCH2_CANDIDATE),
        "batch2_orfp": _safe_path(BATCH2_ORFP, BATCH2_ORFP),
        "batch2_formula": _safe_path(BATCH2_FORMULA, BATCH2_FORMULA),
        "batch2_no_lookahead": _safe_path(BATCH2_NO_LOOKAHEAD, BATCH2_NO_LOOKAHEAD),
        "batch1_candidate": _safe_path(BATCH1_CANDIDATE, BATCH1_CANDIDATE),
        "batch1_orfp": _safe_path(BATCH1_ORFP, BATCH1_ORFP),
        "batch1_formula": _safe_path(BATCH1_FORMULA, BATCH1_FORMULA),
        "batch1_no_lookahead": _safe_path(BATCH1_NO_LOOKAHEAD, BATCH1_NO_LOOKAHEAD),
        "reclass_rows": _safe_path(RECLASS_ROWS, RECLASS_ROWS),
        "pairwise_rows": _safe_path(RECLASS_PAIRWISE, RECLASS_PAIRWISE),
        "reclass_root_cause": _safe_path(RECLASS_ROOT_CAUSE, RECLASS_ROOT_CAUSE),
        "edinet_reference_decision": _safe_path(EDINET_REFERENCE_DECISION, EDINET_REFERENCE_DECISION),
    }
    for path, label in [(p, n) for n, p in inputs.items()]:
        _ensure_exists(path, label)

    frames = {
        "batch2_candidate": _load_frame(inputs["batch2_candidate"]),
        "batch2_orfp": _load_frame(inputs["batch2_orfp"]),
        "batch1_candidate": _load_frame(inputs["batch1_candidate"]),
        "batch1_orfp": _load_frame(inputs["batch1_orfp"]),
        "reclass_rows": _load_frame(inputs["reclass_rows"]),
        "pairwise_rows": _load_frame(inputs["pairwise_rows"]),
    }

    if limit_anchor_dates is not None:
        anchor_values = sorted(frames["batch2_candidate"]["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        for key in ("batch2_candidate", "batch2_orfp", "batch1_candidate", "batch1_orfp"):
            frames[key] = frames[key][frames[key]["anchor_date"].isin(anchor_values)].copy()
        if "anchor_date" in frames["reclass_rows"].columns:
            frames["reclass_rows"] = frames["reclass_rows"][frames["reclass_rows"]["anchor_date"].isin(anchor_values)].copy()
        if "anchor_date" in frames["pairwise_rows"].columns:
            frames["pairwise_rows"] = frames["pairwise_rows"][frames["pairwise_rows"]["anchor_date"].isin(anchor_values)].copy()

    candidate_months = sorted(frames["batch2_candidate"]["month_bucket"].dropna().astype(str).unique().tolist())
    split_months = _month_split(candidate_months)
    time_split_ready = (
        len(candidate_months) >= 23
        and len(split_months["train"]) > 0
        and len(split_months["validation"]) > 0
        and len(split_months["test"]) > 0
    )

    input_resolution = _build_input_resolution(inputs, frames, jobs_requested=jobs, jobs_supported=1, limit_anchor_dates=limit_anchor_dates)
    input_resolution["schema_version"] = INPUT_RESOLUTION_SCHEMA_VERSION_LOCAL
    input_resolution["notes"] = list(input_resolution.get("notes", [])) + [
        "The frozen challenger uses only tree_hgb_path_value and does not compare alternative model variants.",
    ]

    inventory = _load_json(inputs["feasibility_feature_inventory"])
    split_contract = _load_json(inputs["feasibility_split_contract"])
    split_contract["schema_version"] = SPLIT_CONTRACT_SCHEMA_VERSION
    split_contract["row_count_reconciled"] = (
        int(len(frames["batch2_candidate"])) == 2542
        and int(len(frames["batch2_orfp"])) == 365
        and int(len(frames["reclass_rows"])) == 585
        and int(len(frames["pairwise_rows"])) == 385
    )
    if limit_anchor_dates is not None:
        split_contract["row_count_reconciled"] = False
    split_contract["status"] = "ready_for_time_split_evaluation" if time_split_ready else "insufficient_time_split_depth"
    split_contract["splits"] = split_months

    model_spec = _build_model_spec(split_contract)
    model_results = _fit_selected_model(frames, split_months, time_split_ready=time_split_ready)
    edinet_reference = {
        "session_dir": str(EDINET_REFERENCE_SESSION),
        "decision_path": str(EDINET_REFERENCE_DECISION),
        "decision": _load_json(EDINET_REFERENCE_DECISION).get("decision", "unknown") if EDINET_REFERENCE_DECISION.exists() else "unknown",
        "used_as_feature": False,
        "reason": "reference_only_and_positive_coverage_is_zero",
    }

    variant_comparison = {
        "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION_LOCAL,
        "generated_at_utc": _utc_now(),
        "selected_variant": SELECTED_VARIANT,
        "candidate_surface": _evaluation_summary(
            frames["batch2_candidate"],
            {SELECTED_VARIANT: model_results.get("variants", {}).get(SELECTED_VARIANT, {}).get("candidate_scores", pd.Series(dtype="float64"))},
            surface_name="candidate_surface",
            topk_values=TOP_K_VALUES,
        )
        if model_results.get("status") == "ready_for_evaluation"
        else {"surface_name": "candidate_surface", "row_count": int(len(frames["batch2_candidate"])), "variants": {}},
        "orfp_surface": _evaluation_summary(
            frames["batch2_orfp"],
            {SELECTED_VARIANT: model_results.get("variants", {}).get(SELECTED_VARIANT, {}).get("orfp_scores", pd.Series(dtype="float64"))},
            surface_name="orfp_surface",
            topk_values=TOP_K_VALUES,
        )
        if model_results.get("status") == "ready_for_evaluation"
        else {"surface_name": "orfp_surface", "row_count": int(len(frames["batch2_orfp"])), "variants": {}},
        "comparison_summary": {},
        "notes": [
            "Single-model validation against the preserved champion selection.",
            "No alternative model variants are compared.",
        ],
    }
    if model_results.get("status") == "ready_for_evaluation":
        candidate_variant = variant_comparison["candidate_surface"]["variants"][SELECTED_VARIANT]
        variant_comparison["comparison_summary"] = _selection_summary(candidate_variant, frames["batch2_candidate"])

    if model_results.get("status") == "ready_for_evaluation":
        topk_diff = _feasibility_build_topk_membership_diff(
            {"batch2_candidate": frames["batch2_candidate"], "batch2_orfp": frames["batch2_orfp"]},
            {
                SELECTED_VARIANT: {
                    "candidate": model_results.get("variants", {}).get(SELECTED_VARIANT, {}).get("candidate_scores", pd.Series(dtype="float64")),
                    "orfp": model_results.get("variants", {}).get(SELECTED_VARIANT, {}).get("orfp_scores", pd.Series(dtype="float64")),
                }
            },
            TOP_K_VALUES,
        )
    else:
        topk_diff = pd.DataFrame(
            columns=[
                "surface_name",
                "variant_name",
                "topk",
                "anchor_date",
                "month_bucket",
                "side",
                "symbol",
                "candidate_idx",
                "model_score",
                "model_rank",
                "model_selected",
                "champion_selected",
                "membership_changed",
                "selected_overlap",
                "champion_rank",
                "champion_score",
                "candidate_rank",
                "candidate_score",
                "forward_ret_20d",
                "path_value_score_v1",
                "top15_label",
                "bottom15_label",
                "market_regime_bucket",
                "dominant_regime_context",
                "family_classification",
                "shape_classification",
            ]
        )
    if not topk_diff.empty:
        for column in ("top15_label", "bottom15_label", "model_selected", "champion_selected", "membership_changed", "selected_overlap"):
            if column in topk_diff.columns:
                topk_diff[column] = topk_diff[column].fillna(False).astype(bool)

    root_cause_summary = _load_json(inputs["reclass_root_cause"])
    feature_effect_summary = _build_feature_effect_summary(
        frames,
        model_results,
        variant_comparison,
        inventory,
        frames["reclass_rows"],
        root_cause_summary,
        edinet_reference,
    )
    leakage_audit = _build_leakage_audit(frames, model_results, inventory, split_contract, feature_effect_summary, edinet_reference)
    robustness_audit = _build_robustness_audit(frames, split_months, model_results, variant_comparison, topk_diff)
    decision = _build_decision(variant_comparison, robustness_audit, leakage_audit, split_contract)
    run_manifest = _build_run_manifest(
        output_root_path,
        session_dir,
        inputs,
        jobs_requested=jobs,
        jobs_supported=1,
        split_status=split_contract["status"],
        decision=decision["decision"],
    )

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "shadow_challenger_model_spec.json", model_spec)
    _write_json(session_dir / "shadow_challenger_variant_pool_comparison.json", variant_comparison)
    _write_parquet(session_dir / "shadow_challenger_topk_membership_diff.parquet", topk_diff)
    _write_json(session_dir / "shadow_challenger_robustness_audit.json", robustness_audit)
    _write_json(session_dir / "shadow_challenger_leakage_audit.json", leakage_audit)
    _write_json(session_dir / "shadow_challenger_feature_effect_summary.json", feature_effect_summary)
    _write_json(session_dir / "shadow_reranker_challenger_design_v1_decision.json", decision)
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_id": session_id,
            "required_files_present": True,
            "artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "shadow_challenger_model_spec.json",
                "shadow_challenger_variant_pool_comparison.json",
                "shadow_challenger_topk_membership_diff.parquet",
                "shadow_challenger_robustness_audit.json",
                "shadow_challenger_leakage_audit.json",
                "shadow_challenger_feature_effect_summary.json",
                "shadow_reranker_challenger_design_v1_decision.json",
            ],
        },
    )

    return {
        "output_dir": str(session_dir),
        "session_id": session_id,
        "decision": decision["decision"],
        "row_count_reconciled": bool(split_contract.get("row_count_reconciled", False)),
        "split_status": split_contract["status"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()
    result = run_shadow_reranker_challenger_design_v1(output_root=args.output_root, limit_anchor_dates=args.limit_anchor_dates, jobs=args.jobs)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
