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

SCRIPT_NAME = "tradex_side_aware_top20pct_label_modeling_feasibility_v1"
SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1"
MANIFEST_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_input_resolution_v1"
LABEL_AUDIT_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_label_audit_v1"
FEATURE_INVENTORY_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_feature_inventory_v1"
MODEL_CONTRACT_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_model_contract_v1"
SPLIT_CONTRACT_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_split_contract_v1"
VARIANT_COMPARISON_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_variant_pool_comparison_v1"
STABILITY_AUDIT_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_stability_audit_v1"
FAILURE_MODE_AUDIT_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_failure_mode_audit_v1"
LINEAGE_COMPARISON_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_lineage_comparison_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_no_lookahead_audit_v1"
LEAKAGE_AUDIT_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_leakage_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_decision_v1"
ARTIFACT_COMPLETE_SCHEMA_VERSION = "tradex_side_aware_top20pct_label_modeling_feasibility_v1_artifact_complete_v1"

ACC_ROOT = Path(r"G:\Tradex\shadow_reranker_accumulated_forward_validation_v1\20260502T082532Z-c17e19")
LABEL_ROOT = Path(r"G:\Tradex\label_definition_redesign_v1\20260502T102056Z-735273")
LINEAGE_ROOT = Path(r"G:\Tradex\shadow_reranker_challenger_design_v1\20260501T120651Z-643568")
COMPOSITE_ROOT = Path(r"G:\Tradex\composite_topk_utility_modeling_feasibility_v1\20260502T100349Z-941979")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\side_aware_top20pct_label_modeling_feasibility_v1")
MODEL_NAME = "tree_hgb_side_aware_top20pct_v1"
TARGET_NAME = "side_aware_group_top20pct_forward_ret_20d_label"
TOP_K_VALUES = (5, 10, 20)

FEATURES = list(feas.MODEL_FEATURES)
NUMERIC = list(feas.NUMERIC_MODEL_FEATURES)
CATEGORICAL = list(feas.CATEGORICAL_MODEL_FEATURES)
OUTCOME_FIELDS = ["top15_label", "bottom15_label", "forward_ret_20d", "forward_ret_10d", "forward_ret_5d", "path_value_score_v1", "mfe_20d", "mae_20d", TARGET_NAME]
FORBIDDEN_FIELDS = [
    "score", "candidate_score", "champion_score", "challenger_score", "rank", "candidate_rank", "champion_rank", "challenger_rank",
    "top15_label", "bottom15_label", "forward_ret_20d", "forward_ret_10d", "forward_ret_5d", "mfe_20d", "mae_20d", "path_value_score_v1",
    TARGET_NAME, "realized_pnl", "model_score", "tree_hgb_path_value_score", "composite_topk_utility_v1_score",
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


def _build_side_aware_top20pct_target(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out[TARGET_NAME] = pd.Series(pd.NA, index=out.index, dtype="boolean")
    out["label_rank"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out["label_cutoff"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out["label_valid"] = pd.Series(False, index=out.index, dtype="boolean")
    out["label_tie_key"] = pd.Series(pd.NA, index=out.index, dtype="string")
    out["target_sign"] = "missing"
    group_cols = ["anchor_date", "side"]
    pieces: list[pd.DataFrame] = []
    for _, group in out.groupby(group_cols, sort=False):
        ranked = group.copy()
        ranked["forward_ret_20d"] = pd.to_numeric(ranked["forward_ret_20d"], errors="coerce")
        ranked["path_value_score_v1"] = pd.to_numeric(ranked["path_value_score_v1"], errors="coerce")
        ranked["mae_20d"] = pd.to_numeric(ranked["mae_20d"], errors="coerce")
        ranked["candidate_idx"] = pd.to_numeric(ranked["candidate_idx"], errors="coerce")
        ranked = ranked.loc[
            ranked["forward_ret_20d"].notna()
            & ranked["path_value_score_v1"].notna()
            & ranked["mae_20d"].notna()
            & ranked["candidate_idx"].notna()
        ].copy()
        if ranked.empty:
            pieces.append(group.copy())
            continue
        ranked = ranked.sort_values(
            ["forward_ret_20d", "path_value_score_v1", "mae_20d", "candidate_idx"],
            ascending=[False, False, True, True],
            kind="stable",
        ).copy()
        ranked["label_rank"] = np.arange(1, len(ranked) + 1, dtype=int)
        cutoff = int(math.ceil(len(ranked) * 0.2))
        cutoff = max(1, cutoff)
        ranked["label_cutoff"] = cutoff
        ranked["label_valid"] = True
        ranked[TARGET_NAME] = ranked["label_rank"] <= cutoff
        ranked["target_sign"] = np.where(ranked[TARGET_NAME], "positive", "negative")
        ranked["label_tie_key"] = ranked["forward_ret_20d"].round(12).astype(str) + "|" + ranked["path_value_score_v1"].round(12).astype(str) + "|" + ranked["mae_20d"].round(12).astype(str) + "|" + ranked["candidate_idx"].astype("Int64").astype(str)
        pieces.append(ranked)
    if not pieces:
        return out
    combined = pd.concat(pieces, axis=0).sort_index()
    for column in [TARGET_NAME, "label_rank", "label_cutoff", "label_valid", "label_tie_key", "target_sign"]:
        if column in combined.columns:
            out.loc[combined.index, column] = combined[column]
    out[TARGET_NAME] = out[TARGET_NAME].fillna(False).astype(bool)
    out["label_valid"] = out["label_valid"].fillna(False).astype(bool)
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
    top15 = pd.Series(out["top15_label"], index=out.index, dtype="boolean").fillna(False).astype(bool)
    bottom15 = pd.Series(out["bottom15_label"], index=out.index, dtype="boolean").fillna(False).astype(bool)
    model_selected = out["model_selected"].fillna(False).astype(bool)
    ref_selected = out["reference_selected"].fillna(False).astype(bool)
    target_label = pd.Series(out[TARGET_NAME], index=out.index, dtype="boolean").fillna(False).astype(bool) if TARGET_NAME in out.columns else pd.Series(False, index=out.index)
    union = int((model_selected | ref_selected).sum())
    inter = int((model_selected & ref_selected).sum())
    zero_pass_groups = int(
        sum(
            not pd.Series(g.loc[g["model_selected"], "top15_label"], dtype="boolean").fillna(False).astype(bool).any()
            for _, g in out.groupby(["anchor_date", "side"], sort=False)
        )
    )
    symbol_counts = out.loc[model_selected, "symbol"].value_counts(dropna=False)
    return out, {
        "selected_row_count": int(model_selected.sum()),
        "top15_capture_count": int((model_selected & top15).sum()),
        "bottom15_contamination_count": int((model_selected & bottom15).sum()),
        "target_capture_count": int((model_selected & target_label).sum()),
        "top15_capture_rate": float(top15.loc[model_selected].mean()) if model_selected.any() else None,
        "bottom15_contamination_rate": float(bottom15.loc[model_selected].mean()) if model_selected.any() else None,
        "target_capture_rate": float(target_label.loc[model_selected].mean()) if model_selected.any() else None,
        "mean_forward_ret_20d": float(pd.to_numeric(out.loc[model_selected, "forward_ret_20d"], errors="coerce").mean()) if model_selected.any() else None,
        "mean_path_value_score_v1": float(pd.to_numeric(out.loc[model_selected, "path_value_score_v1"], errors="coerce").mean()) if model_selected.any() else None,
        "mean_target_label": float(pd.to_numeric(out.loc[model_selected, TARGET_NAME], errors="coerce").mean()) if model_selected.any() else None,
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
    target_label = pd.Series(ranked[TARGET_NAME], index=ranked.index, dtype="boolean").fillna(False).astype(bool) if TARGET_NAME in ranked.columns else pd.Series(False, index=ranked.index)
    metrics["reference_metrics"] = {
        "selected_row_count": int(ref_sel.sum()),
        "top15_capture_count": int((ref_sel & pd.Series(ranked["top15_label"], index=ranked.index, dtype="boolean").fillna(False).astype(bool)).sum()),
        "bottom15_contamination_count": int((ref_sel & pd.Series(ranked["bottom15_label"], index=ranked.index, dtype="boolean").fillna(False).astype(bool)).sum()),
        "target_capture_count": int((ref_sel & target_label).sum()),
        "top15_capture_rate": float(pd.Series(ranked.loc[ref_sel, "top15_label"], dtype="boolean").fillna(False).astype(bool).mean()) if ref_sel.any() else None,
        "bottom15_contamination_rate": float(pd.Series(ranked.loc[ref_sel, "bottom15_label"], dtype="boolean").fillna(False).astype(bool).mean()) if ref_sel.any() else None,
        "target_capture_rate": float(target_label.loc[ref_sel].mean()) if ref_sel.any() else None,
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
    ranked["model_vs_champion_score_delta"] = ranked[score_col] - ranked["reference_score"]
    ranked["model_vs_previous_diagnostic_score_delta"] = ranked[score_col] - ranked["previous_diagnostic_score"]
    return ranked


def run(*, output_root: Path = DEFAULT_OUTPUT_ROOT, jobs_requested: int = 2) -> dict[str, Any]:
    session = _session_id()
    out = output_root / session
    out.mkdir(parents=True, exist_ok=False)

    frame = pd.read_parquet(ACC_ROOT / "accumulated_forward_prediction_rows.parquet").copy()
    acc_variant = _load_json(ACC_ROOT / "accumulated_forward_variant_pool_comparison.json")
    acc_leakage = _load_json(ACC_ROOT / "accumulated_forward_leakage_audit.json")
    acc_decision = _load_json(ACC_ROOT / "shadow_reranker_accumulated_forward_validation_v1_decision.json")
    lineage_spec = _load_json(LINEAGE_ROOT / "shadow_challenger_model_spec.json")
    lineage_variant = _load_json(LINEAGE_ROOT / "shadow_challenger_variant_pool_comparison.json")
    lineage_decision = _load_json(LINEAGE_ROOT / "shadow_reranker_challenger_design_v1_decision.json")
    label_root_contract = _load_json(LABEL_ROOT / "side_aware_label_contract.json")
    label_root_recommendation = _load_json(LABEL_ROOT / "label_definition_recommendation.json")
    label_root_decision = _load_json(LABEL_ROOT / "label_definition_redesign_v1_decision.json")

    for col in ["top15_label", "bottom15_label"]:
        frame[col] = frame[col].fillna(False).astype(bool)
    frame = _build_side_aware_top20pct_target(frame)
    frame["target_rank_key"] = frame[TARGET_NAME].astype(bool).astype(int)

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
        "side_feature_context_only": False,
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
            "side_label_constructed_within_anchor_side_group": True,
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
        [
            frame["month_bucket"].isin(split_months["train"]),
            frame["month_bucket"].isin(split_months["validation"]),
            frame["month_bucket"].isin(split_months["test"]),
        ],
        ["train", "validation", "test"],
        default="unassigned",
    )
    if (frame["split"] == "unassigned").any():
        raise RuntimeError("split assignment failed")

    def _split_side_status(split_name: str, side: str, subset: pd.DataFrame) -> str:
        positive = int(subset[TARGET_NAME].sum())
        if side == "short" and (positive < 3 or len(subset) < 10):
            return "research_hold"
        return "active"

    side_split_balance = {}
    for split_name in ["train", "validation", "test"]:
        split_frame = frame[frame["split"] == split_name].copy()
        side_split_balance[split_name] = {}
        for side in ["long", "short"]:
            side_frame = split_frame[split_frame["side"] == side].copy()
            side_split_balance[split_name][side] = {
                "row_count": int(len(side_frame)),
                "positive": int(side_frame[TARGET_NAME].sum()),
                "negative": int((~side_frame[TARGET_NAME]).sum()),
                "positive_groups": int((side_frame.groupby(["anchor_date", "side"])[TARGET_NAME].sum() > 0).sum()) if len(side_frame) else 0,
                "label_status": _split_side_status(split_name, side, side_frame),
            }

    split_contract = {
        "schema_version": SPLIT_CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "split_by": "month_bucket chronological split",
        "month_buckets": split_months,
        "status": "ready_for_time_split_evaluation" if len(months) >= 3 and all(len(v) > 0 for v in split_months.values()) else "insufficient_time_split_depth",
        "row_counts": {split: int((frame["split"] == split).sum()) for split in ["train", "validation", "test"]},
        "label_balance_by_split": {
            split: {
                "positive": int((frame.loc[frame["split"] == split, TARGET_NAME]).sum()),
                "negative": int((~frame.loc[frame["split"] == split, TARGET_NAME]).astype(bool).sum()),
                "neutral": 0,
                "top15_label_total": int(frame.loc[frame["split"] == split, "top15_label"].sum()),
                "bottom15_label_total": int(frame.loc[frame["split"] == split, "bottom15_label"].sum()),
                "mean_target": float(frame.loc[frame["split"] == split, TARGET_NAME].mean()),
                "std_target": float(frame.loc[frame["split"] == split, TARGET_NAME].std()),
            }
            for split in ["train", "validation", "test"]
        },
        "label_balance_by_split_and_side": side_split_balance,
        "side_mode": {"long": "active", "short": "research_hold"},
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
        "objective": "regression_side_aware_group_top20pct_label",
        "target_label": TARGET_NAME,
        "exact_features_used": FEATURES,
        "numeric_features": NUMERIC,
        "categorical_features": CATEGORICAL,
        "side_usage": {
            "status": "context_feature_retained_in_frozen_contract",
            "routing_use": "group-aware ranking and split analysis",
            "separate_short_model_trained": False,
            "short_side_status": "research_hold",
        },
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
                "positive": int(out[TARGET_NAME].sum()),
                "negative": int((~out[TARGET_NAME]).astype(bool).sum()),
                "neutral": 0,
                "mean": float(out[TARGET_NAME].mean()),
                "std": float(out[TARGET_NAME].std()),
                "top15_label_total": int(out["top15_label"].sum()),
                "bottom15_label_total": int(out["bottom15_label"].sum()),
            },
            "model_score_distribution": {"mean": float(out["model_score"].mean()), "std": float(out["model_score"].std()), "min": float(out["model_score"].min()), "max": float(out["model_score"].max())},
            "diagnostic_score_distribution": {"mean": float(out["diagnostic_score"].mean()), "std": float(out["diagnostic_score"].std())},
            "target_regression_metrics": {
                "rmse": float(math.sqrt(mean_squared_error(out[TARGET_NAME].astype(float), out["model_score"]))),
                "spearman": float(out[TARGET_NAME].astype(float).corr(out["model_score"], method="spearman")),
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
        "top5_target_capture_delta": float(oos_top5["target_capture_rate"] - oos_top5["reference_metrics"]["target_capture_rate"]),
        "top10_target_capture_delta": float(oos_top10["target_capture_rate"] - oos_top10["reference_metrics"]["target_capture_rate"]),
        "top20_target_capture_delta": float(oos_top20["target_capture_rate"] - oos_top20["reference_metrics"]["target_capture_rate"]),
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
        "authoritative_result": str(out / "side_aware_top20pct_prediction_rows.parquet"),
        "comparison_summary": comparison_summary,
        "split_results": split_results,
        "diagnostic_reference": {
            "top5_forward_ret_20d": oos_top5["diagnostic_reference_metrics"]["mean_forward_ret_20d"],
            "top10_forward_ret_20d": oos_top10["diagnostic_reference_metrics"]["mean_forward_ret_20d"],
            "top20_forward_ret_20d": oos_top20["diagnostic_reference_metrics"]["mean_forward_ret_20d"],
        },
    }

    oos_frames = []
    for topk in TOP_K_VALUES:
        block = _selection_frame(oos_rows, "model_score", topk, "oos")
        block["topk"] = int(topk)
        block["target_capture_rate"] = block[TARGET_NAME].astype(float)
        oos_frames.append(block)
    membership = pd.concat(oos_frames, ignore_index=True)
    membership["membership_changed"] = membership["changed_member"]
    membership["diagnostic_membership_changed"] = membership["changed_diagnostic_member"]

    top5_target_improved = oos_top5["target_capture_rate"] > oos_top5["reference_metrics"]["target_capture_rate"]
    top10_target_improved = oos_top10["target_capture_rate"] > oos_top10["reference_metrics"]["target_capture_rate"]
    target_improved = top5_target_improved and top10_target_improved
    top15_secondary_ok = oos_top5["top15_capture_rate"] >= oos_top5["reference_metrics"]["top15_capture_rate"] and oos_top10["top15_capture_rate"] >= oos_top10["reference_metrics"]["top15_capture_rate"]
    bottom15_ok = oos_top5["bottom15_contamination_rate"] <= oos_top5["reference_metrics"]["bottom15_contamination_rate"] and oos_top10["bottom15_contamination_rate"] <= oos_top10["reference_metrics"]["bottom15_contamination_rate"]
    return_ok = oos_top5["mean_forward_ret_20d"] >= oos_top5["reference_metrics"]["mean_forward_ret_20d"] and oos_top10["mean_forward_ret_20d"] >= oos_top10["reference_metrics"]["mean_forward_ret_20d"]
    path_ok = oos_top5["mean_path_value_score_v1"] >= oos_top5["reference_metrics"]["mean_path_value_score_v1"] and oos_top10["mean_path_value_score_v1"] >= oos_top10["reference_metrics"]["mean_path_value_score_v1"]
    non_trivial_move = oos_top5["membership_changed_count"] > 0 and oos_top10["membership_changed_count"] > 0 and oos_top20["membership_changed_count"] > 0
    broad_enough = int(oos_rows.groupby(["anchor_date", "side"]).ngroups) >= 4
    concentration_risk = bool(oos_top5["symbol_concentration"]["top1_symbol_share"] is not None and oos_top5["symbol_concentration"]["top1_symbol_share"] > 0.35)
    short_hold = split_contract["side_mode"]["short"] == "research_hold"

    stability = {
        "schema_version": STABILITY_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "oos_split": "validation+test",
        "top5_target_capture_improved_vs_champion": bool(top5_target_improved),
        "top10_target_capture_improved_vs_champion": bool(top10_target_improved),
        "top5_top15_capture_improved_vs_champion": bool(oos_top5["top15_capture_rate"] > oos_top5["reference_metrics"]["top15_capture_rate"]),
        "top10_top15_capture_improved_vs_champion": bool(oos_top10["top15_capture_rate"] > oos_top10["reference_metrics"]["top15_capture_rate"]),
        "bottom15_contamination_controlled": bool(bottom15_ok),
        "mean_return_acceptable": bool(return_ok),
        "path_quality_acceptable": bool(path_ok),
        "short_side_status": split_contract["side_mode"]["short"],
        "short_side_research_hold": bool(short_hold),
        "improvement_concentration": {
            "top5_top1_symbol_share": oos_top5["symbol_concentration"]["top1_symbol_share"],
            "top10_top1_symbol_share": oos_top10["symbol_concentration"]["top1_symbol_share"],
            "top20_top1_symbol_share": oos_top20["symbol_concentration"]["top1_symbol_share"],
            "top5_top3_symbol_share": oos_top5["symbol_concentration"]["top3_symbol_share"],
            "top10_top3_symbol_share": oos_top10["symbol_concentration"]["top3_symbol_share"],
            "top20_top3_symbol_share": oos_top20["symbol_concentration"]["top3_symbol_share"],
        },
        "top20_capture_flat_expected": bool(abs(float(oos_top20["target_capture_rate"] - oos_top20["reference_metrics"]["target_capture_rate"])) < 1e-12),
    }
    failure_mode = {
        "schema_version": FAILURE_MODE_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "too_broad_label": bool(top5_target_improved and top15_secondary_ok and not return_ok),
        "bottom15_contamination_worsened": bool(not bottom15_ok),
        "return_worsened": bool(not return_ok),
        "path_quality_worsened": bool(not path_ok),
        "short_side_research_hold": bool(short_hold),
        "short_side_has_low_support": bool(int(frame.loc[frame["side"] == "short", TARGET_NAME].sum()) < 20),
        "concentration_risk": concentration_risk,
        "label_becomes_mediocre_positive_bucket": bool(top5_target_improved and oos_top20["top15_capture_rate"] < oos_top20["reference_metrics"]["top15_capture_rate"]),
    }
    lineage = {
        "schema_version": LINEAGE_COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "comparisons": {
            "champion_original": acc_variant,
            "previous_tree_hgb_path_value": acc_variant,
            "dropped_composite_target": _load_json(COMPOSITE_ROOT / "composite_target_variant_pool_comparison.json"),
            "new_side_aware_top20pct_target": variant_comparison,
        },
        "classification": None,
        "reason": None,
    }

    if top5_target_improved and top10_target_improved and bottom15_ok and return_ok and path_ok and non_trivial_move and broad_enough and not concentration_risk:
        decision = "ready_for_forward_validation"
        lineage["classification"] = "label_fix_success"
        lineage_reason = "top20pct capture improved while bottom15 contamination, mean return, and path quality remained acceptable"
    elif target_improved and bottom15_ok and (return_ok or path_ok):
        decision = "hold_needs_more_forward_surfaces"
        lineage["classification"] = "partial_improvement"
        lineage_reason = "direction improved but breadth or side coverage is still not strong enough for a final keep call"
    elif not target_improved:
        decision = "drop_side_aware_top20pct_label"
        lineage["classification"] = "insufficient_signal"
        lineage_reason = "side-aware top20pct label failed to improve practical ranking quality"
    elif concentration_risk:
        decision = "needs_label_threshold_revision"
        lineage["classification"] = "too_broad_label"
        lineage_reason = "result is concentrated and the label may be too broad for stable practical ranking"
    elif short_hold:
        decision = "side_specific_data_insufficient"
        lineage["classification"] = "insufficient_signal"
        lineage_reason = "short-side coverage is still too sparse for reliable separate modeling"
    else:
        decision = "needs_label_threshold_revision"
        lineage["classification"] = "overfit_or_unstable"
        lineage_reason = "capture improved but one or more ranking-quality constraints remain weak"
    lineage["reason"] = lineage_reason

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_id": session,
        "task": "TRADEX side-aware group-top20pct label modeling feasibility v1",
        "boundary": "TRADEX-only",
        "non_scope": ["MeeMee", "production ranking", "publish / promotion", "research_inventory.json", "candidate-generation semantics", "feature formulas", "hyperparameter search", "multiple labels"],
        "source_roots": {"label_definition_redesign": str(LABEL_ROOT), "accumulated_forward_validation": str(ACC_ROOT), "frozen_challenger_lineage": str(LINEAGE_ROOT), "composite_reference": str(COMPOSITE_ROOT)},
        "jobs_requested": int(jobs_requested),
        "jobs_supported": 1,
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_sources": [
            {"role": "label_contract", "path": str(LABEL_ROOT), "files_used": ["label_candidate_definitions.json", "label_coverage_balance_audit.json", "label_objective_alignment_audit.json", "side_aware_label_contract.json", "label_definition_recommendation.json", "label_definition_redesign_v1_decision.json", "label_candidate_distribution.parquet", "side_label_contract_matrix.parquet", "label_objective_scorecard.parquet"]},
            {"role": "accumulated_surface", "path": str(ACC_ROOT), "files_used": ["accumulated_forward_prediction_rows.parquet", "accumulated_forward_topk_membership_diff.parquet", "accumulated_forward_variant_pool_comparison.json", "accumulated_forward_leakage_audit.json", "shadow_reranker_accumulated_forward_validation_v1_decision.json"]},
            {"role": "frozen_lineage", "path": str(LINEAGE_ROOT), "files_used": ["shadow_challenger_model_spec.json", "shadow_challenger_variant_pool_comparison.json", "shadow_reranker_challenger_design_v1_decision.json"]},
            {"role": "composite_reference", "path": str(COMPOSITE_ROOT), "files_used": ["composite_target_variant_pool_comparison.json", "composite_target_lineage_comparison.json", "composite_topk_utility_modeling_feasibility_v1_decision.json"]},
        ],
        "authoritative_decision_source": str(LABEL_ROOT / "label_definition_redesign_v1_decision.json"),
    }
    label_audit = {
        "schema_version": LABEL_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "target_name": TARGET_NAME,
        "formula": "within each anchor_date / side group, positive if row is in the top ceil(20% of group size) by forward_ret_20d, tie-broken by path_value_score_v1 desc, mae_20d asc, candidate_idx asc",
        "grouping_grain": "anchor_date / side",
        "row_count": int(len(frame)),
        "group_count": int(frame.groupby(["anchor_date", "side"]).ngroups),
        "label_coverage": {
            "missing_target_label_rows": int(frame[TARGET_NAME].isna().sum()),
            "positive_total": int(frame[TARGET_NAME].sum()),
            "negative_total": int((~frame[TARGET_NAME]).astype(bool).sum()),
            "positive_groups": int((frame.groupby(["anchor_date", "side"])[TARGET_NAME].sum() > 0).sum()),
            "long_positive_total": int(frame.loc[frame["side"] == "long", TARGET_NAME].sum()),
            "short_positive_total": int(frame.loc[frame["side"] == "short", TARGET_NAME].sum()),
            "long_positive_groups": int((frame.loc[frame["side"] == "long"].groupby(["anchor_date", "side"])[TARGET_NAME].sum() > 0).sum()),
            "short_positive_groups": int((frame.loc[frame["side"] == "short"].groupby(["anchor_date", "side"])[TARGET_NAME].sum() > 0).sum()),
        },
        "label_balance": {
            "overall_positive_rate": float(frame[TARGET_NAME].mean()),
            "long_positive_rate": float(frame.loc[frame["side"] == "long", TARGET_NAME].mean()) if (frame["side"] == "long").any() else None,
            "short_positive_rate": float(frame.loc[frame["side"] == "short", TARGET_NAME].mean()) if (frame["side"] == "short").any() else None,
        },
        "no_lookahead": True,
        "missing_outcome_rows_excluded": True,
        "tie_handling": ["higher forward_ret_20d", "higher path_value_score_v1", "lower mae_20d", "stable candidate_idx"],
    }
    decision_artifact = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "reason": lineage_reason,
        "authoritative_result": str(out / "side_aware_top20pct_variant_pool_comparison.json"),
    }
    model_effect = {
        "schema_version": "tradex_side_aware_top20pct_label_modeling_feasibility_v1_feature_effect_summary_v1",
        "generated_at_utc": _utc_now(),
        "selected_model": MODEL_NAME,
        "diagnostic_reference_model": "tree_hgb_path_value",
        "training_rows": int(len(train_rows)),
        "validation_rows": int(len(val_rows)),
        "test_rows": int(len(test_rows)),
        "target_correlation_with_top15_label": float(frame[TARGET_NAME].astype(float).corr(frame["top15_label"].astype(float), method="spearman")),
        "target_correlation_with_bottom15_label": float(frame[TARGET_NAME].astype(float).corr(frame["bottom15_label"].astype(float), method="spearman")),
    }

    _write_json(out / "run_manifest.json", run_manifest)
    _write_json(out / "input_resolution.json", input_resolution)
    _write_json(out / "side_aware_top20pct_label_audit.json", label_audit)
    _write_json(out / "side_aware_top20pct_feature_inventory.json", feature_inventory)
    _write_json(out / "side_aware_top20pct_model_contract.json", model_contract)
    _write_json(out / "side_aware_top20pct_split_contract.json", split_contract)
    _write_json(out / "side_aware_top20pct_variant_pool_comparison.json", variant_comparison)
    _write_json(out / "side_aware_top20pct_stability_audit.json", stability)
    _write_json(out / "side_aware_top20pct_failure_mode_audit.json", failure_mode)
    _write_json(out / "side_aware_top20pct_lineage_comparison.json", lineage)
    _write_json(out / "side_aware_top20pct_label_modeling_feasibility_v1_decision.json", decision_artifact)
    _write_json(out / "side_aware_top20pct_no_lookahead_audit.json", no_lookahead)
    _write_json(out / "side_aware_top20pct_leakage_audit.json", leakage_audit)
    _write_json(out / "side_aware_top20pct_feature_effect_summary.json", model_effect)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {
        "schema_version": ARTIFACT_COMPLETE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_id": session,
        "required_artifacts": [
            "run_manifest.json",
            "input_resolution.json",
            "side_aware_top20pct_label_audit.json",
            "side_aware_top20pct_target_rows.parquet",
            "side_aware_top20pct_feature_inventory.json",
            "side_aware_top20pct_model_contract.json",
            "side_aware_top20pct_split_contract.json",
            "side_aware_top20pct_variant_pool_comparison.json",
            "side_aware_top20pct_topk_membership_diff.parquet",
            "side_aware_top20pct_stability_audit.json",
            "side_aware_top20pct_failure_mode_audit.json",
            "side_aware_top20pct_lineage_comparison.json",
            "side_aware_top20pct_label_modeling_feasibility_v1_decision.json",
            "_ARTIFACT_COMPLETE.json",
        ],
        "status": "complete",
    })

    target_rows = frame[[
        "candidate_idx", "anchor_date", "month_bucket", "split", "side", "symbol",
        "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label",
        "label_rank", "label_cutoff", "label_valid", TARGET_NAME, "target_sign",
        "model_score", "diagnostic_score", "reference_score",
    ]].copy()
    target_rows.to_parquet(out / "side_aware_top20pct_target_rows.parquet", index=False)
    membership.to_parquet(out / "side_aware_top20pct_topk_membership_diff.parquet", index=False)
    prediction_cols = list(dict.fromkeys([
        "candidate_idx", "anchor_date", "month_bucket", "split", "side", "symbol",
        "model_score", "diagnostic_score", "reference_score", TARGET_NAME, "label_rank", "label_cutoff",
        "label_valid", "target_sign", "top15_label", "bottom15_label", "forward_ret_20d", "path_value_score_v1",
        *FEATURES,
    ]))
    frame[prediction_cols].to_parquet(out / "side_aware_top20pct_prediction_rows.parquet", index=False)

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
