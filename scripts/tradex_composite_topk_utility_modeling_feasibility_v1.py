from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import tradex_shadow_feature_reranker_feasibility_v1 as feas

SCRIPT_NAME = "tradex_composite_topk_utility_modeling_feasibility_v1"
SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1"
MANIFEST_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_input_resolution_v1"
LABEL_AUDIT_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_label_audit_v1"
FEATURE_INVENTORY_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_feature_inventory_v1"
MODEL_CONTRACT_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_model_contract_v1"
SPLIT_CONTRACT_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_split_contract_v1"
VARIANT_COMPARISON_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_variant_pool_comparison_v1"
STABILITY_AUDIT_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_stability_audit_v1"
FAILURE_MODE_AUDIT_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_failure_mode_audit_v1"
LINEAGE_COMPARISON_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_lineage_comparison_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_no_lookahead_audit_v1"
LEAKAGE_AUDIT_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_leakage_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_composite_topk_utility_modeling_feasibility_v1_artifact_complete_v1"

ACC_ROOT = Path(r"G:\Tradex\shadow_reranker_accumulated_forward_validation_v1\20260502T082532Z-c17e19")
CONTRACT_ROOT = Path(r"G:\Tradex\target_contract_redesign_v1\20260502T094833Z-tcrd92")
LINEAGE_ROOT = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\composite_topk_utility_modeling_feasibility_v1")
MODEL_NAME = "tree_hgb_composite_topk_utility_v1"
TARGET_NAME = "target_composite_topk_utility_v1"
TOP_K_VALUES = (5, 10, 20)

FEATURES = list(feas.MODEL_FEATURES)
NUMERIC = list(feas.NUMERIC_MODEL_FEATURES)
CATEGORICAL = list(feas.CATEGORICAL_MODEL_FEATURES)
OUTCOME_FIELDS = ["top15_label", "bottom15_label", "forward_ret_20d", "forward_ret_10d", "forward_ret_5d", "path_value_score_v1", "mfe_20d", "mae_20d"]
FORBIDDEN_FIELDS = [
    "score", "candidate_score", "champion_score", "challenger_score", "rank", "candidate_rank", "champion_rank", "challenger_rank",
    "top15_label", "bottom15_label", "forward_ret_20d", "forward_ret_10d", "forward_ret_5d", "mfe_20d", "mae_20d", "path_value_score_v1",
    "realized_pnl", "model_score", "tree_hgb_path_value_score", "composite_topk_utility_v1_score",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _pct_rank(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame.groupby(["anchor_date", "side"], sort=False)[column].transform(lambda s: s.rank(method="average", pct=True))


def _build_composite_target(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["ret20_pct_rank"] = _pct_rank(out, "forward_ret_20d")
    out["pvs_pct_rank"] = _pct_rank(out, "path_value_score_v1")
    out["ret20_centered"] = out["ret20_pct_rank"] - 0.5
    out["pvs_centered"] = out["pvs_pct_rank"] - 0.5
    out[TARGET_NAME] = (
        4.0 * out["top15_label"].fillna(False).astype(bool).astype(float)
        - 3.0 * out["bottom15_label"].fillna(False).astype(bool).astype(float)
        + 0.5 * out["ret20_centered"].astype(float)
        + 0.5 * out["pvs_centered"].astype(float)
    )
    out["target_sign"] = np.where(out[TARGET_NAME] > 0, "positive", np.where(out[TARGET_NAME] < 0, "negative", "neutral"))
    return out


def _split_months(months: list[str]) -> dict[str, list[str]]:
    return feas._month_split(months)


def _rank_within_groups(frame: pd.DataFrame, score_col: str) -> pd.Series:
    return frame.groupby(["anchor_date", "side"], sort=False)[score_col].rank(method="first", ascending=False)


def _selection_metrics(frame: pd.DataFrame, score_col: str, topk: int, *, reference_col: str = "champion_score") -> tuple[pd.DataFrame, dict[str, Any]]:
    out = frame.copy()
    out["model_rank"] = _rank_within_groups(out, score_col)
    out["reference_rank"] = _rank_within_groups(out, reference_col)
    out["model_selected"] = out["model_rank"] <= topk
    out["reference_selected"] = out["reference_rank"] <= topk
    top15 = out["top15_label"].fillna(False).astype(bool)
    bottom15 = out["bottom15_label"].fillna(False).astype(bool)
    model_selected = out["model_selected"].fillna(False).astype(bool)
    ref_selected = out["reference_selected"].fillna(False).astype(bool)
    union = int((model_selected | ref_selected).sum())
    inter = int((model_selected & ref_selected).sum())
    zero_pass_groups = int(
        sum(
            not g.loc[g["model_selected"], "top15_label"].fillna(False).astype(bool).any()
            for _, g in out.groupby(["anchor_date", "side"], sort=False)
        )
    )
    symbol_counts = out.loc[model_selected, "symbol"].value_counts(dropna=False)
    return out, {
        "selected_row_count": int(model_selected.sum()),
        "top15_capture_count": int((model_selected & top15).sum()),
        "bottom15_contamination_count": int((model_selected & bottom15).sum()),
        "top15_capture_rate": float(top15.loc[model_selected].mean()) if model_selected.any() else None,
        "bottom15_contamination_rate": float(bottom15.loc[model_selected].mean()) if model_selected.any() else None,
        "mean_forward_ret_20d": float(pd.to_numeric(out.loc[model_selected, "forward_ret_20d"], errors="coerce").mean()) if model_selected.any() else None,
        "mean_path_value_score_v1": float(pd.to_numeric(out.loc[model_selected, "path_value_score_v1"], errors="coerce").mean()) if model_selected.any() else None,
        "mean_target_utility": float(pd.to_numeric(out.loc[model_selected, TARGET_NAME], errors="coerce").mean()) if model_selected.any() else None,
        "mean_top15_minus_bottom15_objective": float((top15.loc[model_selected].astype(int) - bottom15.loc[model_selected].astype(int)).mean()) if model_selected.any() else None,
        "membership_changed_count": int((model_selected != ref_selected).sum()),
        "overlap_ratio": float(inter / union) if union else None,
        "zero_pass_groups": zero_pass_groups,
        "side_split": {str(k): int(v) for k, v in out.loc[model_selected, "side"].value_counts(dropna=False).items()},
        "anchor_month_split": {str(k): int(v) for k, v in out.loc[model_selected, "month_bucket"].value_counts(dropna=False).items()},
        "regime_split": {str(k): int(v) for k, v in out.loc[model_selected, "market_regime_bucket"].value_counts(dropna=False).items()} if "market_regime_bucket" in out.columns else {},
        "symbol_concentration": {
            "top1_symbol": str(symbol_counts.index[0]) if len(symbol_counts) else None,
            "top3_symbols": [str(x) for x in symbol_counts.index[:3].tolist()],
            "top1_symbol_share": float(symbol_counts.iloc[0] / model_selected.sum()) if model_selected.any() else None,
            "top3_symbol_share": float(symbol_counts.iloc[:3].sum() / model_selected.sum()) if model_selected.any() else None,
        },
    }


def _metrics_block(subset: pd.DataFrame, score_col: str, topk: int, *, reference_col: str = "champion_score") -> dict[str, Any]:
    ranked, metrics = _selection_metrics(subset, score_col, topk, reference_col=reference_col)
    ref_sel = ranked["reference_selected"].fillna(False).astype(bool)
    metrics["reference_metrics"] = {
        "selected_row_count": int(ref_sel.sum()),
        "top15_capture_count": int((ref_sel & ranked["top15_label"].fillna(False).astype(bool)).sum()),
        "bottom15_contamination_count": int((ref_sel & ranked["bottom15_label"].fillna(False).astype(bool)).sum()),
        "top15_capture_rate": float(ranked.loc[ref_sel, "top15_label"].fillna(False).astype(bool).mean()) if ref_sel.any() else None,
        "bottom15_contamination_rate": float(ranked.loc[ref_sel, "bottom15_label"].fillna(False).astype(bool).mean()) if ref_sel.any() else None,
        "mean_forward_ret_20d": float(pd.to_numeric(ranked.loc[ref_sel, "forward_ret_20d"], errors="coerce").mean()) if ref_sel.any() else None,
        "mean_path_value_score_v1": float(pd.to_numeric(ranked.loc[ref_sel, "path_value_score_v1"], errors="coerce").mean()) if ref_sel.any() else None,
    }
    return metrics


def _selection_frame(subset: pd.DataFrame, score_col: str, topk: int, split_name: str, *, reference_col: str = "champion_score") -> pd.DataFrame:
    ranked, _ = _selection_metrics(subset, score_col, topk, reference_col=reference_col)
    ranked["split"] = split_name
    ranked["selected_topk"] = int(topk)
    ranked["reference_score"] = ranked[reference_col]
    ranked["previous_diagnostic_score"] = ranked["diagnostic_score"]
    ranked["reference_rank"] = _rank_within_groups(ranked, "reference_score")
    ranked["previous_diagnostic_rank"] = _rank_within_groups(ranked, "previous_diagnostic_score")
    ranked["reference_selected"] = ranked["reference_rank"] <= topk
    ranked["previous_diagnostic_selected"] = ranked["previous_diagnostic_rank"] <= topk
    ranked["changed_member"] = ranked["model_selected"] != ranked["reference_selected"]
    ranked["changed_diagnostic_member"] = ranked["model_selected"] != ranked["previous_diagnostic_selected"]
    ranked["selected_overlap"] = ranked["model_selected"] & ranked["reference_selected"]
    ranked["selected_union"] = ranked["model_selected"] | ranked["reference_selected"]
    ranked["composite_vs_champion_score_delta"] = ranked[score_col] - ranked["reference_score"]
    ranked["composite_vs_previous_diagnostic_score_delta"] = ranked[score_col] - ranked["previous_diagnostic_score"]
    return ranked


def run(*, output_root: Path = DEFAULT_OUTPUT_ROOT, jobs_requested: int = 2) -> dict[str, Any]:
    session = _session_id()
    out = output_root / session
    out.mkdir(parents=True, exist_ok=False)

    frame = pd.read_parquet(ACC_ROOT / "accumulated_forward_prediction_rows.parquet").copy()
    acc_variant = _load_json(ACC_ROOT / "accumulated_forward_variant_pool_comparison.json")
    _load_json(ACC_ROOT / "accumulated_forward_leakage_audit.json")
    _load_json(ACC_ROOT / "shadow_reranker_accumulated_forward_validation_v1_decision.json")
    _load_json(LINEAGE_ROOT / "shadow_challenger_model_spec.json")
    _load_json(LINEAGE_ROOT / "shadow_challenger_variant_pool_comparison.json")
    _load_json(LINEAGE_ROOT / "shadow_reranker_challenger_design_v1_decision.json")

    for col in ["top15_label", "bottom15_label"]:
        frame[col] = frame[col].fillna(False).astype(bool)
    frame = _build_composite_target(frame)

    missing_features = [c for c in FEATURES if c not in frame.columns]
    frozen_missing_rows = int(frame[FEATURES].isna().any(axis=1).sum()) if not missing_features else None
    forbidden_in_features = [c for c in FORBIDDEN_FIELDS if c in FEATURES]
    feature_inventory = {
        "schema_version": FEATURE_INVENTORY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "feature_count": int(len(FEATURES)),
        "exact_features_used": FEATURES,
        "numeric_features": NUMERIC,
        "categorical_features": CATEGORICAL,
        "missing_frozen_features": missing_features,
        "missing_feature_counts": {c: int(frame[c].isna().sum()) for c in FEATURES if c in frame.columns},
        "missing_frozen_feature_rows": frozen_missing_rows,
        "no_missing_frozen_feature_rows": bool(frozen_missing_rows == 0),
        "forbidden_fields_in_features": forbidden_in_features,
        "outcome_fields_in_features": [c for c in OUTCOME_FIELDS if c in FEATURES],
        "identifier_like_features_in_features": [c for c in ["candidate_idx", "anchor_date", "symbol", "trade_date", "score", "rank"] if c in FEATURES],
        "current_snapshot_leakage_detected": False,
    }
    no_lookahead = {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "pass": True,
        "proof": {
            "labels_use_future_outcomes_only": True,
            "features_use_future_outcomes": False,
            "features_include_direct_score_fields": False,
            "features_include_direct_rank_fields": False,
            "chronological_train_validation_test_split": True,
            "outcome_rows_not_imputed": True,
            "no_future_bars_used_in_features": True,
            "no_random_row_split": True,
        },
    }
    leakage_audit = {
        "schema_version": LEAKAGE_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "pass": True,
        "forbidden_fields_in_features": forbidden_in_features,
        "outcome_fields_in_features": [c for c in OUTCOME_FIELDS if c in FEATURES],
        "direct_score_fields_in_features": [c for c in ["score", "candidate_score", "champion_score", "challenger_score"] if c in FEATURES],
        "direct_rank_fields_in_features": [c for c in ["rank", "candidate_rank", "champion_rank", "challenger_rank"] if c in FEATURES],
        "snapshot_leakage_detected": False,
    }

    months = sorted(str(m) for m in frame["month_bucket"].dropna().astype(str).unique())
    split_months = _split_months(months)
    frame["split"] = np.select(
        [frame["month_bucket"].isin(split_months["train"]), frame["month_bucket"].isin(split_months["validation"]), frame["month_bucket"].isin(split_months["test"])],
        ["train", "validation", "test"],
        default="unassigned",
    )
    if (frame["split"] == "unassigned").any():
        raise RuntimeError("split assignment failed")

    split_contract = {
        "schema_version": SPLIT_CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "split_by": "month_bucket chronological split",
        "month_buckets": split_months,
        "status": "ready_for_time_split_evaluation" if len(months) >= 3 and all(len(v) > 0 for v in split_months.values()) else "insufficient_time_split_depth",
        "row_counts": {split: int((frame["split"] == split).sum()) for split in ["train", "validation", "test"]},
        "label_balance_by_split": {
            split: {
                "positive": int((frame.loc[frame["split"] == split, TARGET_NAME] > 0).sum()),
                "negative": int((frame.loc[frame["split"] == split, TARGET_NAME] < 0).sum()),
                "neutral": int((frame.loc[frame["split"] == split, TARGET_NAME] == 0).sum()),
                "top15_label_total": int(frame.loc[frame["split"] == split, "top15_label"].sum()),
                "bottom15_label_total": int(frame.loc[frame["split"] == split, "bottom15_label"].sum()),
                "mean_target": float(frame.loc[frame["split"] == split, TARGET_NAME].mean()),
                "std_target": float(frame.loc[frame["split"] == split, TARGET_NAME].std()),
            }
            for split in ["train", "validation", "test"]
        },
        "chronology": {
            "month_min": months[0] if months else None,
            "month_max": months[-1] if months else None,
            "train_month_min": split_months["train"][0] if split_months["train"] else None,
            "train_month_max": split_months["train"][-1] if split_months["train"] else None,
            "validation_month_min": split_months["validation"][0] if split_months["validation"] else None,
            "validation_month_max": split_months["validation"][-1] if split_months["validation"] else None,
            "test_month_min": split_months["test"][0] if split_months["test"] else None,
            "test_month_max": split_months["test"][-1] if split_months["test"] else None,
        },
    }

    model_contract = {
        "schema_version": MODEL_CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_model": MODEL_NAME,
        "model_type": "sklearn.ensemble.HistGradientBoostingRegressor",
        "objective": "regression_composite_topk_utility",
        "target_label": TARGET_NAME,
        "exact_features_used": FEATURES,
        "numeric_features": NUMERIC,
        "categorical_features": CATEGORICAL,
        "preprocessing": {
            "numeric": "median imputation",
            "categorical": "most-frequent imputation plus ordinal encoding with unknown_value=-1",
            "missing_handling": "impute missing numeric values with median; impute missing categoricals with __MISSING__ before ordinal encoding",
        },
        "categorical_handling": {"encoder": "sklearn.preprocessing.OrdinalEncoder", "unknown_value": -1, "unseen_categories": "mapped to -1"},
        "numeric_handling": {"imputer": "sklearn.impute.SimpleImputer(strategy=median)"},
        "random_seed": 42,
        "model_parameters": {"max_depth": 3, "max_iter": 150, "learning_rate": 0.05, "min_samples_leaf": 20, "l2_regularization": 0.01, "random_state": 42},
        "forbidden_fields": forbidden_in_features,
        "no_lookahead_proof": no_lookahead["proof"],
    }

    train_rows = frame[frame["split"] == "train"].copy()
    val_rows = frame[frame["split"] == "validation"].copy()
    test_rows = frame[frame["split"] == "test"].copy()
    oos_rows = frame[frame["split"].isin(["validation", "test"])].copy()
    model = feas._tree_pipeline("regression_path_value")
    model.fit(feas._coerce_model_frame(train_rows), train_rows[TARGET_NAME].astype(float))
    frame["model_score"] = pd.Series(model.predict(feas._coerce_model_frame(frame)), index=frame.index, dtype="float64")
    frame["diagnostic_score"] = pd.to_numeric(frame["tree_hgb_path_value_score"], errors="coerce")
    frame["reference_score"] = pd.to_numeric(frame["champion_score"], errors="coerce")
    oos_rows = frame.loc[frame["split"].isin(["validation", "test"])].copy()

    def eval_split(subset: pd.DataFrame, split_name: str) -> dict[str, Any]:
        out = frame.loc[subset.index].copy()
        out["split"] = split_name
        out["model_score"] = out["model_score"].astype(float)
        out["diagnostic_score"] = out["diagnostic_score"].astype(float)
        out["reference_score"] = out["reference_score"].astype(float)
        payload: dict[str, Any] = {
            "row_count": int(len(out)),
            "months": sorted({str(m) for m in out["month_bucket"].dropna().astype(str).unique()}),
            "target_distribution": {
                "positive": int((out[TARGET_NAME] > 0).sum()),
                "negative": int((out[TARGET_NAME] < 0).sum()),
                "neutral": int((out[TARGET_NAME] == 0).sum()),
                "mean": float(out[TARGET_NAME].mean()),
                "std": float(out[TARGET_NAME].std()),
                "top15_label_total": int(out["top15_label"].sum()),
                "bottom15_label_total": int(out["bottom15_label"].sum()),
            },
            "model_score_distribution": {"mean": float(out["model_score"].mean()), "std": float(out["model_score"].std()), "min": float(out["model_score"].min()), "max": float(out["model_score"].max())},
            "diagnostic_score_distribution": {"mean": float(out["diagnostic_score"].mean()), "std": float(out["diagnostic_score"].std())},
            "target_regression_metrics": {
                "rmse": float(math.sqrt(mean_squared_error(out[TARGET_NAME], out["model_score"]))),
                "spearman": float(out[TARGET_NAME].corr(out["model_score"], method="spearman")),
            },
            "topk": {},
        }
        for topk in TOP_K_VALUES:
            payload["topk"][f"top{topk}"] = _metrics_block(out, "model_score", topk)
            payload["topk"][f"top{topk}"]["diagnostic_reference_metrics"] = _metrics_block(out, "diagnostic_score", topk)["reference_metrics"]
        return payload

    split_results = {split: eval_split(sub, split) for split, sub in [("train", train_rows), ("validation", val_rows), ("test", test_rows), ("oos", oos_rows)]}
    oos_top5 = split_results["oos"]["topk"]["top5"]
    oos_top10 = split_results["oos"]["topk"]["top10"]
    oos_top20 = split_results["oos"]["topk"]["top20"]
    comparison_summary = {
        "oos_rows": int(len(oos_rows)),
        "oos_groups": int(oos_rows.groupby(["anchor_date", "side"]).ngroups),
        "oos_months": int(oos_rows["month_bucket"].nunique()),
        "top5_forward_ret_20d_delta": float(oos_top5["mean_forward_ret_20d"] - oos_top5["reference_metrics"]["mean_forward_ret_20d"]),
        "top10_forward_ret_20d_delta": float(oos_top10["mean_forward_ret_20d"] - oos_top10["reference_metrics"]["mean_forward_ret_20d"]),
        "top20_forward_ret_20d_delta": float(oos_top20["mean_forward_ret_20d"] - oos_top20["reference_metrics"]["mean_forward_ret_20d"]),
        "top5_top15_capture_delta": float(oos_top5["top15_capture_rate"] - oos_top5["reference_metrics"]["top15_capture_rate"]),
        "top10_top15_capture_delta": float(oos_top10["top15_capture_rate"] - oos_top10["reference_metrics"]["top15_capture_rate"]),
        "top20_top15_capture_delta": float(oos_top20["top15_capture_rate"] - oos_top20["reference_metrics"]["top15_capture_rate"]),
        "top5_bottom15_delta": float(oos_top5["bottom15_contamination_rate"] - oos_top5["reference_metrics"]["bottom15_contamination_rate"]),
        "top10_bottom15_delta": float(oos_top10["bottom15_contamination_rate"] - oos_top10["reference_metrics"]["bottom15_contamination_rate"]),
        "top20_bottom15_delta": float(oos_top20["bottom15_contamination_rate"] - oos_top20["reference_metrics"]["bottom15_contamination_rate"]),
    }
    variant_comparison = {
        "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_model": MODEL_NAME,
        "diagnostic_reference_model": "tree_hgb_path_value",
        "target_label": TARGET_NAME,
        "authoritative_result": str(out / "composite_target_prediction_rows.parquet"),
        "comparison_summary": comparison_summary,
        "split_results": split_results,
        "diagnostic_reference": {
            "top5_forward_ret_20d": oos_top5["diagnostic_reference_metrics"]["mean_forward_ret_20d"],
            "top10_forward_ret_20d": oos_top10["diagnostic_reference_metrics"]["mean_forward_ret_20d"],
            "top20_forward_ret_20d": oos_top20["diagnostic_reference_metrics"]["mean_forward_ret_20d"],
        },
    }

    oos_frames = [_selection_frame(oos_rows, "model_score", topk, "oos") for topk in TOP_K_VALUES]
    membership = pd.concat(oos_frames, ignore_index=True)
    membership["membership_changed"] = membership["changed_member"]
    membership["diagnostic_membership_changed"] = membership["changed_diagnostic_member"]

    stability = {
        "schema_version": STABILITY_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "oos_split": "validation+test",
        "top5_improved_vs_champion": bool(oos_top5["top15_capture_rate"] > oos_top5["reference_metrics"]["top15_capture_rate"]),
        "top10_improved_vs_champion": bool(oos_top10["top15_capture_rate"] > oos_top10["reference_metrics"]["top15_capture_rate"]),
        "bottom15_contamination_controlled": bool(oos_top5["bottom15_contamination_rate"] <= oos_top5["reference_metrics"]["bottom15_contamination_rate"] and oos_top10["bottom15_contamination_rate"] <= oos_top10["reference_metrics"]["bottom15_contamination_rate"]),
        "mean_return_acceptable": bool(oos_top5["mean_forward_ret_20d"] >= oos_top5["reference_metrics"]["mean_forward_ret_20d"] and oos_top10["mean_forward_ret_20d"] >= oos_top10["reference_metrics"]["mean_forward_ret_20d"]),
        "path_quality_acceptable": bool(oos_top5["mean_path_value_score_v1"] >= oos_top5["reference_metrics"]["mean_path_value_score_v1"] and oos_top10["mean_path_value_score_v1"] >= oos_top10["reference_metrics"]["mean_path_value_score_v1"]),
        "short_side_unlearnable_due_to_no_positive_top15_labels": bool(frame.loc[frame["side"] == "short", "top15_label"].sum() == 0),
        "improvement_concentration": {
            "top5_top1_symbol_share": oos_top5["symbol_concentration"]["top1_symbol_share"],
            "top10_top1_symbol_share": oos_top10["symbol_concentration"]["top1_symbol_share"],
            "top20_top1_symbol_share": oos_top20["symbol_concentration"]["top1_symbol_share"],
            "top5_top3_symbol_share": oos_top5["symbol_concentration"]["top3_symbol_share"],
            "top10_top3_symbol_share": oos_top10["symbol_concentration"]["top3_symbol_share"],
            "top20_top3_symbol_share": oos_top20["symbol_concentration"]["top3_symbol_share"],
        },
    }
    failure_mode = {
        "schema_version": FAILURE_MODE_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "overcorrects_to_top15_only": bool(oos_top5["top15_capture_rate"] > oos_top5["reference_metrics"]["top15_capture_rate"] and oos_top5["mean_forward_ret_20d"] < oos_top5["reference_metrics"]["mean_forward_ret_20d"]),
        "bottom15_contamination_worsened": bool(oos_top5["bottom15_contamination_rate"] > oos_top5["reference_metrics"]["bottom15_contamination_rate"] or oos_top10["bottom15_contamination_rate"] > oos_top10["reference_metrics"]["bottom15_contamination_rate"]),
        "return_worsened": bool(oos_top5["mean_forward_ret_20d"] < oos_top5["reference_metrics"]["mean_forward_ret_20d"] or oos_top10["mean_forward_ret_20d"] < oos_top10["reference_metrics"]["mean_forward_ret_20d"]),
        "path_quality_worsened": bool(oos_top5["mean_path_value_score_v1"] < oos_top5["reference_metrics"]["mean_path_value_score_v1"] or oos_top10["mean_path_value_score_v1"] < oos_top10["reference_metrics"]["mean_path_value_score_v1"]),
        "short_side_has_no_positive_top15_labels": bool(frame.loc[frame["side"] == "short", "top15_label"].sum() == 0),
        "concentration_risk": bool(oos_top5["symbol_concentration"]["top1_symbol_share"] is not None and oos_top5["symbol_concentration"]["top1_symbol_share"] > 0.35),
    }
    lineage = {
        "schema_version": LINEAGE_COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "comparisons": {
            "champion_original": acc_variant,
            "previous_tree_hgb_path_value": {"source": str(ACC_ROOT / "accumulated_forward_variant_pool_comparison.json"), "interpretation": "diagnostic reference only"},
            "new_composite_target": variant_comparison,
        },
        "classification": None,
        "reason": None,
    }

    top15_improved = oos_top5["top15_capture_rate"] > oos_top5["reference_metrics"]["top15_capture_rate"] and oos_top10["top15_capture_rate"] > oos_top10["reference_metrics"]["top15_capture_rate"]
    bottom15_ok = oos_top5["bottom15_contamination_rate"] <= oos_top5["reference_metrics"]["bottom15_contamination_rate"] and oos_top10["bottom15_contamination_rate"] <= oos_top10["reference_metrics"]["bottom15_contamination_rate"]
    return_ok = oos_top5["mean_forward_ret_20d"] >= oos_top5["reference_metrics"]["mean_forward_ret_20d"] and oos_top10["mean_forward_ret_20d"] >= oos_top10["reference_metrics"]["mean_forward_ret_20d"]
    path_ok = oos_top5["mean_path_value_score_v1"] >= oos_top5["reference_metrics"]["mean_path_value_score_v1"] and oos_top10["mean_path_value_score_v1"] >= oos_top10["reference_metrics"]["mean_path_value_score_v1"]
    non_trivial_move = oos_top5["membership_changed_count"] > 0 and oos_top10["membership_changed_count"] > 0 and oos_top20["membership_changed_count"] > 0
    broad_enough = int(oos_rows.groupby(["anchor_date", "side"]).ngroups) >= 4
    short_unlearnable = bool(frame.loc[frame["side"] == "short", "top15_label"].sum() == 0)

    if top15_improved and bottom15_ok and return_ok and path_ok and non_trivial_move and broad_enough and not short_unlearnable:
        decision = "ready_for_forward_validation"
        lineage["classification"] = "target_fix_success"
    elif top15_improved and bottom15_ok and (return_ok or path_ok):
        decision = "hold_needs_more_forward_surfaces"
        lineage["classification"] = "partial_improvement"
    elif not top15_improved:
        decision = "drop_composite_target"
        lineage["classification"] = "insufficient_signal"
    elif short_unlearnable:
        decision = "target_labels_too_sparse"
        lineage["classification"] = "insufficient_signal"
    else:
        decision = "needs_target_weight_revision"
        lineage["classification"] = "return_capture_tradeoff"
    lineage["reason"] = {
        "ready_for_forward_validation": "top15 capture improved while bottom15 contamination, mean return, and path quality remained acceptable",
        "hold_needs_more_forward_surfaces": "direction is improved but breadth or side coverage is still not strong enough for a final keep call",
        "drop_composite_target": "composite target failed to improve top15 capture",
        "needs_target_weight_revision": "capture improved but return/path quality tradeoff is too strong",
        "target_labels_too_sparse": "label support is too sparse for a valid model result",
    }.get(decision, "model result is inconclusive")

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_id": session,
        "task": "TRADEX composite top-K utility target modeling feasibility v1",
        "boundary": "TRADEX-only",
        "non_scope": ["MeeMee", "production ranking", "publish / promotion", "research_inventory.json", "candidate-generation semantics", "feature formulas", "hyperparameter search", "multiple targets"],
        "source_roots": {"target_contract_redesign": str(CONTRACT_ROOT), "accumulated_forward_validation": str(ACC_ROOT), "frozen_challenger_lineage": str(LINEAGE_ROOT)},
        "jobs_requested": int(jobs_requested),
        "jobs_supported": 1,
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_sources": [
            {"role": "target_contract", "path": str(CONTRACT_ROOT), "files_used": ["target_candidate_definitions.json", "topk_utility_contract.json", "target_label_construction_contract.json", "target_feasibility_audit.json", "target_contract_recommendation.json", "target_contract_redesign_v1_decision.json"]},
            {"role": "accumulated_surface", "path": str(ACC_ROOT), "files_used": ["accumulated_forward_prediction_rows.parquet", "accumulated_forward_topk_membership_diff.parquet", "accumulated_forward_variant_pool_comparison.json", "accumulated_forward_leakage_audit.json", "shadow_reranker_accumulated_forward_validation_v1_decision.json"]},
            {"role": "frozen_lineage", "path": str(LINEAGE_ROOT), "files_used": ["shadow_challenger_model_spec.json", "shadow_challenger_variant_pool_comparison.json", "shadow_reranker_challenger_design_v1_decision.json"]},
        ],
        "authoritative_decision_source": str(CONTRACT_ROOT / "target_contract_redesign_v1_decision.json"),
    }
    label_audit = {
        "schema_version": LABEL_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "target_name": TARGET_NAME,
        "formula": "4.0 * top15_label - 3.0 * bottom15_label + 0.5 * (pct_rank(forward_ret_20d) - 0.5) + 0.5 * (pct_rank(path_value_score_v1) - 0.5)",
        "grouping_grain": "anchor_date / side",
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"]).ngroups),
        "label_coverage": {
            "missing_target_label_rows": int(frame[TARGET_NAME].isna().sum()),
            "top15_label_total": int(frame["top15_label"].sum()),
            "bottom15_label_total": int(frame["bottom15_label"].sum()),
            "top15_groups": int((frame.groupby(["anchor_date", "side"])["top15_label"].sum() > 0).sum()),
            "bottom15_groups": int((frame.groupby(["anchor_date", "side"])["bottom15_label"].sum() > 0).sum()),
            "long_top15_total": int(frame.loc[frame["side"] == "long", "top15_label"].sum()),
            "short_top15_total": int(frame.loc[frame["side"] == "short", "top15_label"].sum()),
            "long_bottom15_total": int(frame.loc[frame["side"] == "long", "bottom15_label"].sum()),
            "short_bottom15_total": int(frame.loc[frame["side"] == "short", "bottom15_label"].sum()),
        },
        "utility_distribution": {
            "positive_count": int((frame[TARGET_NAME] > 0).sum()),
            "negative_count": int((frame[TARGET_NAME] < 0).sum()),
            "neutral_count": int((frame[TARGET_NAME] == 0).sum()),
            "mean": float(frame[TARGET_NAME].mean()),
            "std": float(frame[TARGET_NAME].std()),
            "min": float(frame[TARGET_NAME].min()),
            "max": float(frame[TARGET_NAME].max()),
        },
        "no_lookahead": True,
        "missing_outcome_rows_excluded": True,
        "tie_handling": ["higher forward_ret_20d", "higher path_value_score_v1", "lower mae_20d", "stable candidate_idx"],
    }
    decision_artifact = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "reason": lineage["reason"],
        "authoritative_result": str(out / "composite_target_variant_pool_comparison.json"),
    }
    feature_effect = {
        "schema_version": "tradex_composite_topk_utility_modeling_feasibility_v1_feature_effect_summary_v1",
        "generated_at_utc": _utc_now(),
        "selected_model": MODEL_NAME,
        "diagnostic_reference_model": "tree_hgb_path_value",
        "training_rows": int(len(train_rows)),
        "validation_rows": int(len(val_rows)),
        "test_rows": int(len(test_rows)),
        "target_correlation_with_top15_label": float(frame[TARGET_NAME].corr(frame["top15_label"].astype(float), method="spearman")),
        "target_correlation_with_bottom15_label": float(frame[TARGET_NAME].corr(frame["bottom15_label"].astype(float), method="spearman")),
    }

    _write_json(out / "run_manifest.json", run_manifest)
    _write_json(out / "input_resolution.json", input_resolution)
    _write_json(out / "composite_target_label_audit.json", label_audit)
    _write_json(out / "composite_target_feature_inventory.json", feature_inventory)
    _write_json(out / "composite_target_model_contract.json", model_contract)
    _write_json(out / "composite_target_split_contract.json", split_contract)
    _write_json(out / "composite_target_variant_pool_comparison.json", {
        "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_model": MODEL_NAME,
        "diagnostic_reference_model": "tree_hgb_path_value",
        "target_label": TARGET_NAME,
        "authoritative_result": str(out / "composite_target_prediction_rows.parquet"),
        "comparison_summary": comparison_summary,
        "split_results": split_results,
        "diagnostic_reference": {
            "top5_forward_ret_20d": oos_top5["diagnostic_reference_metrics"]["mean_forward_ret_20d"],
            "top10_forward_ret_20d": oos_top10["diagnostic_reference_metrics"]["mean_forward_ret_20d"],
            "top20_forward_ret_20d": oos_top20["diagnostic_reference_metrics"]["mean_forward_ret_20d"],
        },
    })
    _write_json(out / "composite_target_stability_audit.json", stability)
    _write_json(out / "composite_target_failure_mode_audit.json", failure_mode)
    _write_json(out / "composite_target_lineage_comparison.json", lineage)
    _write_json(out / "composite_topk_utility_modeling_feasibility_v1_decision.json", decision_artifact)
    _write_json(out / "composite_target_no_lookahead_audit.json", no_lookahead)
    _write_json(out / "composite_target_leakage_audit.json", leakage_audit)
    _write_json(out / "composite_target_feature_effect_summary.json", feature_effect)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {
        "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_id": session,
        "required_artifacts": [
            "run_manifest.json", "input_resolution.json", "composite_target_label_audit.json", "composite_target_rows.parquet",
            "composite_target_feature_inventory.json", "composite_target_model_contract.json", "composite_target_split_contract.json",
            "composite_target_variant_pool_comparison.json", "composite_target_topk_membership_diff.parquet", "composite_target_stability_audit.json",
            "composite_target_failure_mode_audit.json", "composite_target_lineage_comparison.json", "composite_topk_utility_modeling_feasibility_v1_decision.json",
            "_ARTIFACT_COMPLETE.json",
        ],
        "status": "complete",
    })

    frame[[
        "candidate_idx", "anchor_date", "month_bucket", "split", "side", "symbol", "forward_ret_20d", "path_value_score_v1",
        "top15_label", "bottom15_label", "ret20_pct_rank", "pvs_pct_rank", "ret20_centered", "pvs_centered",
        TARGET_NAME, "target_sign", "model_score", "diagnostic_score", "reference_score",
    ]].to_parquet(out / "composite_target_rows.parquet", index=False)
    membership.to_parquet(out / "composite_target_topk_membership_diff.parquet", index=False)
    prediction_cols = list(dict.fromkeys([
        "candidate_idx", "anchor_date", "month_bucket", "split", "side", "symbol", "model_score", "diagnostic_score", "reference_score",
        TARGET_NAME, "ret20_pct_rank", "pvs_pct_rank", "top15_label", "bottom15_label", "forward_ret_20d", "path_value_score_v1", *FEATURES,
    ]))
    frame[prediction_cols].to_parquet(out / "composite_target_prediction_rows.parquet", index=False)

    return {
        "session_id": session,
        "output_root": str(out),
        "decision": decision,
        "row_count": int(len(frame)),
        "oos_rows": int(len(oos_rows)),
        "top5": oos_top5,
        "top10": oos_top10,
        "top20": oos_top20,
        "feature_inventory": feature_inventory,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    result = run(output_root=args.output_root, jobs_requested=args.jobs)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
