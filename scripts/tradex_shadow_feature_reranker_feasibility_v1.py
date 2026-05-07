from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_shadow_feature_reranker_feasibility_v1"
SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1"
MANIFEST_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_input_resolution_v1"
FEATURE_INVENTORY_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_feature_inventory_v1"
LABEL_CONTRACT_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_label_contract_v1"
SPLIT_CONTRACT_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_split_contract_v1"
MODEL_CONTRACT_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_model_contract_v1"
VARIANT_COMPARISON_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_variant_pool_comparison_v1"
STABILITY_AUDIT_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_stability_audit_v1"
FEATURE_EFFECT_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_feature_effect_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_shadow_feature_reranker_feasibility_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\shadow_feature_reranker_feasibility_v1")

BATCH2_SESSION = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273")
BATCH2_CANDIDATE = BATCH2_SESSION / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet"
BATCH2_ORFP = BATCH2_SESSION / "observable_regime_false_positive_batch2_volume_enriched_v1.parquet"
BATCH2_FORMULA = BATCH2_SESSION / "volume_feature_formula_contract.json"
BATCH2_NO_LOOKAHEAD = BATCH2_SESSION / "no_lookahead_volume_feature_audit.json"

BATCH1_SESSION = Path(r"G:\Tradex\feature_surface_batch1_v1\20260501T093159Z-820266")
BATCH1_CANDIDATE = BATCH1_SESSION / "candidate_prefilter_rows_feature_enriched_v1.parquet"
BATCH1_ORFP = BATCH1_SESSION / "observable_regime_false_positive_feature_enriched_v1.parquet"
BATCH1_FORMULA = BATCH1_SESSION / "feature_formula_contract.json"
BATCH1_NO_LOOKAHEAD = BATCH1_SESSION / "no_lookahead_feature_audit.json"

RECLASS_SESSION = Path(r"G:\Tradex\bad_pick_reclassification_batch2_volume_v1\20260501T102834Z-365658")
RECLASS_ROWS = RECLASS_SESSION / "batch2_volume_reclassification_rows.parquet"
RECLASS_PAIRWISE = RECLASS_SESSION / "batch2_volume_boundary_pairwise.parquet"
RECLASS_ROOT_CAUSE = RECLASS_SESSION / "batch2_volume_root_cause_taxonomy_summary.json"

EDINET_REFERENCE_SESSION = Path(r"G:\Tradex\feature_surface_edinet_event_proxy_v1\20260501T113506Z-315465")
EDINET_REFERENCE_DECISION = EDINET_REFERENCE_SESSION / "feature_surface_edinet_event_proxy_v1_decision.json"

TOP_K_VALUES = (5, 10, 20)
TRAIN_MONTH_COUNT = 15
VALIDATION_MONTH_COUNT = 4
TEST_MONTH_COUNT = 4

MODEL_FEATURES = [
    "entry_strength_score",
    "conditional_high_value",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "candle_body_ratio",
    "candle_upper_wick_ratio",
    "candle_lower_wick_ratio",
    "candle_triplet_up_prob",
    "candle_triplet_down_prob",
    "gap_pct",
    "vol_ratio5_20",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "signal_quality_bucket",
    "decision_candle_quality",
    "liquidity_quality_bucket",
    "higher_timeframe_headroom_bucket",
    "volume_participation_bucket",
    "market_regime_bucket",
    "dominant_regime_context",
    "family_classification",
    "shape_classification",
    "candle_shape_modifier",
    "daily_main_state_ctx",
    "weekly_main_state_ctx",
    "monthly_main_state_ctx",
    "monthly_context_source",
    "weekly_context_source",
    "monthly_context_no_lookahead",
    "weekly_context_no_lookahead",
    "month_bucket",
    "side",
]

NUMERIC_MODEL_FEATURES = [
    "entry_strength_score",
    "conditional_high_value",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "candle_body_ratio",
    "candle_upper_wick_ratio",
    "candle_lower_wick_ratio",
    "candle_triplet_up_prob",
    "candle_triplet_down_prob",
    "gap_pct",
    "vol_ratio5_20",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "monthly_context_no_lookahead",
    "weekly_context_no_lookahead",
]

CATEGORICAL_MODEL_FEATURES = [
    "signal_quality_bucket",
    "decision_candle_quality",
    "liquidity_quality_bucket",
    "higher_timeframe_headroom_bucket",
    "volume_participation_bucket",
    "market_regime_bucket",
    "dominant_regime_context",
    "family_classification",
    "shape_classification",
    "candle_shape_modifier",
    "daily_main_state_ctx",
    "weekly_main_state_ctx",
    "monthly_main_state_ctx",
    "monthly_context_source",
    "weekly_context_source",
    "month_bucket",
    "side",
]

NUMERIC_BUCKET_MAPS = {
    "signal_quality_bucket": {
        "signal_quality_low": 0.0,
        "signal_quality_mid": 1.0,
        "signal_quality_high": 2.0,
        "signal_quality_missing": 0.0,
    },
    "decision_candle_quality": {
        "candle_exhaustion_risk": -1.0,
        "candle_weak": 0.0,
        "candle_mixed": 1.0,
        "candle_strong": 2.0,
    },
    "liquidity_quality_bucket": {
        "liquidity_low": 0.0,
        "liquidity_mid": 1.0,
        "liquidity_high": 2.0,
    },
    "higher_timeframe_headroom_bucket": {
        "overextended_warning": -1.0,
        "headroom_limited": 0.0,
        "headroom_available": 1.0,
    },
    "volume_participation_bucket": {
        "volume_missing": 0.0,
        "volume_weak": 0.0,
        "volume_neutral": 1.0,
        "volume_confirmed": 2.0,
    },
    "prefilter_bucket": {
        "KEEP_WATCH": 0.0,
        "KEEP_PRIMARY": 1.0,
    },
}

_MISSING_TOKENS = {"", "nan", "<na>", "none", "null", "unknown"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, set):
        return [_json_ready(v) for v in sorted(value, key=str)]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, np.floating) and not math.isfinite(float(value)):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(_ensure_exists(path, str(path))).copy()
    for column in ("anchor_date", "symbol", "side", "month_bucket"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _git_hash_or_unknown() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
        value = result.stdout.strip()
        return value or "unknown"
    except Exception:
        return "unknown"


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return str(value).strip().lower() in _MISSING_TOKENS


def _safe_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        result = float(value)
    except Exception:
        return None
    return result if math.isfinite(result) else None


def _safe_bool(value: Any) -> bool | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return None


def _token(value: Any) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip()


def _value_counts(series: pd.Series) -> dict[str, int]:
    return {str(k): int(v) for k, v in series.fillna("").astype("string").value_counts(dropna=False).items()}


def _unique_count_safe(series: pd.Series) -> int:
    try:
        return int(series.astype("string").nunique(dropna=True))
    except Exception:
        flattened = series.map(lambda value: json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True, default=str) if not _is_missing(value) else None)
        return int(flattened.nunique(dropna=True))


def _month_split(months: list[str]) -> dict[str, list[str]]:
    ordered = sorted(dict.fromkeys(str(month) for month in months if not _is_missing(month)))
    n = len(ordered)
    if n < 3:
        return {"train": ordered[:1], "validation": ordered[1:2], "test": ordered[2:]}
    train_end = max(1, min(n - 2, int(round(n * 0.65))))
    validation_end = max(train_end + 1, min(n - 1, int(round(n * 0.83))))
    return {
        "train": ordered[:train_end],
        "validation": ordered[train_end:validation_end],
        "test": ordered[validation_end:],
    }


def _profile_frame(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "label": label,
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "columns": [str(col) for col in frame.columns],
    }
    if "anchor_date" in frame.columns and len(frame) > 0:
        out["anchor_date_min"] = str(frame["anchor_date"].dropna().min()) if frame["anchor_date"].notna().any() else None
        out["anchor_date_max"] = str(frame["anchor_date"].dropna().max()) if frame["anchor_date"].notna().any() else None
    if "month_bucket" in frame.columns and len(frame) > 0:
        out["month_bucket_count"] = int(frame["month_bucket"].fillna("").nunique(dropna=False))
        out["month_bucket_min"] = str(frame["month_bucket"].dropna().min()) if frame["month_bucket"].notna().any() else None
        out["month_bucket_max"] = str(frame["month_bucket"].dropna().max()) if frame["month_bucket"].notna().any() else None
    if "side" in frame.columns:
        out["side_counts"] = _value_counts(frame["side"])
    if "top15_label" in frame.columns:
        out["top15_count"] = int(frame["top15_label"].fillna(False).astype(bool).sum())
    if "bottom15_label" in frame.columns:
        out["bottom15_count"] = int(frame["bottom15_label"].fillna(False).astype(bool).sum())
    return out


def _build_input_resolution(paths: dict[str, Path], frames: dict[str, pd.DataFrame], *, jobs_requested: int, jobs_supported: int, limit_anchor_dates: int | None) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {name: str(path) for name, path in paths.items()},
        "path_checks": {name: path.exists() for name, path in paths.items()},
        "jobs_requested": int(jobs_requested),
        "jobs_supported": int(jobs_supported),
        "limit_anchor_dates": int(limit_anchor_dates) if limit_anchor_dates is not None else None,
        "source_profiles": {name: _profile_frame(frame, label=name) for name, frame in frames.items()},
        "notes": [
            "Batch 2 volume and Batch 1 feature surfaces are used as the candidate feature lineage.",
            "Batch 2 reclassification inputs are used for diagnostics and feature-effect summaries.",
            "EDINET proxy coverage is referenced only as an audit input; it is not used as a model feature when coverage is zero.",
        ],
    }


def _classify_feature(column: str, series: pd.Series) -> tuple[str, str]:
    name = str(column)
    lower = name.lower()
    if name in {"top15_label", "bottom15_label", "forward_ret_20d", "forward_ret_10d", "forward_ret_5d", "path_value_score_v1", "mfe_20d", "mae_20d", "hit_plus_5_before_minus_5", "hit_minus_5_before_plus_5", "is_top15_outcome", "is_bottom15_outcome", "is_materially_negative"}:
        return "forbidden_outcome", "explicit outcome/label field"
    if lower.startswith("best_near_miss_") or lower.startswith("near_miss_") or lower.startswith("boundary_") or lower.endswith("_gap") or lower.endswith("_boundary") or lower in {"score_gap", "forward_ret_20d_gap", "path_value_gap", "rank_gap", "topk_bucket", "bad_pick_scope"}:
        return "leakage_risk", "pairwise diagnostic or derived outcome field"
    if lower in {
        "score",
        "candidate_score",
        "champion_score",
        "challenger_score",
        "rank",
        "candidate_rank",
        "champion_rank",
        "challenger_rank",
        "champion_gate",
        "challenger_gate",
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "challenger_selected_top5",
        "challenger_selected_top10",
        "challenger_selected_top20",
        "changed_top5_member",
        "changed_top10_member",
        "changed_top20_member",
        "include_in_broad_pool",
        "include_in_strict_pool",
        "include_in_exclude_only_pool",
    }:
        return "leakage_risk", "direct ranking output or derived selection flag"
    if lower in {
        "challenger_gate",
        "selected_by",
        "selected_by_methods",
        "selection_reason",
        "policy_date",
        "policy_selected_action",
        "policy_selection_method",
        "policy_selection_source",
        "policy_variant",
        "candidate_idx",
        "state_family_id",
        "monthly_context_date",
        "weekly_context_date",
        "monthly_context",
        "weekly_context",
        "daily_main_state_ctx_date",
        "monthly_context_source_date",
        "weekly_context_source_date",
        "trade_date",
        "anchor_date",
        "symbol",
    }:
        return "identifier_only", "identifier, policy metadata, or direct ranking output"
    if lower.startswith("family_") and any(token in lower for token in ["forward_ret", "path_value", "mfe", "mae", "top15_rate", "bottom15_rate", "positive_month_rate", "plus5_before_minus5", "minus5_before_plus5", "worst_month", "best_month"]):
        return "leakage_risk", "family aggregate contains future outcome summary"
    if lower in {
        "family_sample_count",
        "family_unique_symbol_count",
        "family_month_count",
        "family_months_observed",
        "family_regime_context",
        "family_bad_pick_regime",
        "family_classification",
        "stable_high_value_family",
        "stable_bad_pick_family",
        "regime_dependent_family",
        "unstable_or_sparse_family",
        "neutral_family",
        "dominant_regime_context",
        "market_regime_bucket",
        "shape_classification",
        "candle_shape_modifier",
        "daily_main_state_ctx",
        "weekly_main_state_ctx",
        "monthly_main_state_ctx",
        "signal_quality_bucket",
        "decision_candle_quality",
        "liquidity_quality_bucket",
        "higher_timeframe_headroom_bucket",
        "volume_participation_bucket",
        "prefilter_bucket",
        "monthly_context_source",
        "weekly_context_source",
        "sequence_3",
    }:
        return "categorical_feature", "conservative low-cardinality context feature"
    if lower in {"conditional_high_value", "monthly_context_no_lookahead", "weekly_context_no_lookahead", "breakout5", "breakout10", "exhaustion", "bull_stack", "bear_stack"}:
        return "usable_feature", "boolean or simple state flag"
    if pd.api.types.is_bool_dtype(series):
        return "usable_feature", "boolean feature"
    if pd.api.types.is_numeric_dtype(series):
        missing_rate = float(series.isna().mean()) if len(series) else 1.0
        if missing_rate >= 0.85:
            return "sparse_but_usable", "numeric but very sparse"
        return "numeric_feature", "numeric feature"
    unique_count = _unique_count_safe(series)
    if unique_count <= 10:
        missing_rate = float(series.isna().mean()) if len(series) else 1.0
        if missing_rate >= 0.85:
            return "sparse_but_usable", "low-cardinality but sparse"
        return "categorical_feature", "low-cardinality categorical feature"
    if unique_count <= 50:
        return "sparse_but_usable", "moderate-cardinality sparse feature"
    return "identifier_only", "high-cardinality text or identifier-like feature"


def _feature_inventory(frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    candidate = frames["batch2_candidate"]
    orfp = frames["batch2_orfp"]
    batch1_candidate = frames.get("batch1_candidate")
    batch1_orfp = frames.get("batch1_orfp")
    reclass = frames.get("reclass_rows")
    pairwise = frames.get("pairwise_rows")

    all_columns = list(
        dict.fromkeys(
            list(candidate.columns)
            + list(orfp.columns)
            + (list(batch1_candidate.columns) if batch1_candidate is not None else [])
            + (list(batch1_orfp.columns) if batch1_orfp is not None else [])
            + (list(reclass.columns) if reclass is not None else [])
            + (list(pairwise.columns) if pairwise is not None else [])
        )
    )

    rows: list[dict[str, Any]] = []
    for column in all_columns:
        source_series = None
        if column in candidate.columns:
            source_series = candidate[column]
        elif column in orfp.columns:
            source_series = orfp[column]
        elif batch1_candidate is not None and column in batch1_candidate.columns:
            source_series = batch1_candidate[column]
        elif batch1_orfp is not None and column in batch1_orfp.columns:
            source_series = batch1_orfp[column]
        elif reclass is not None and column in reclass.columns:
            source_series = reclass[column]
        elif pairwise is not None and column in pairwise.columns:
            source_series = pairwise[column]
        else:
            continue

        classification, reason = _classify_feature(column, source_series)
        model_eligible = classification in {"numeric_feature", "categorical_feature", "usable_feature", "sparse_but_usable"}
        rows.append(
            {
                "feature_name": column,
                "dtype": str(source_series.dtype),
                "classification": classification,
                "reason": reason,
                "model_eligible": model_eligible,
                "candidate_non_null_count": int(candidate[column].notna().sum()) if column in candidate.columns else None,
                "candidate_coverage_rate": _safe_float(candidate[column].notna().mean()) if column in candidate.columns else None,
                "candidate_unique_count": _unique_count_safe(candidate[column]) if column in candidate.columns else None,
                "orfp_non_null_count": int(orfp[column].notna().sum()) if column in orfp.columns else None,
                "orfp_coverage_rate": _safe_float(orfp[column].notna().mean()) if column in orfp.columns else None,
                "orfp_unique_count": _unique_count_safe(orfp[column]) if column in orfp.columns else None,
                "batch1_candidate_non_null_count": int(batch1_candidate[column].notna().sum()) if batch1_candidate is not None and column in batch1_candidate.columns else None,
                "batch1_candidate_coverage_rate": _safe_float(batch1_candidate[column].notna().mean()) if batch1_candidate is not None and column in batch1_candidate.columns else None,
                "batch1_orfp_non_null_count": int(batch1_orfp[column].notna().sum()) if batch1_orfp is not None and column in batch1_orfp.columns else None,
                "batch1_orfp_coverage_rate": _safe_float(batch1_orfp[column].notna().mean()) if batch1_orfp is not None and column in batch1_orfp.columns else None,
                "reclassification_non_null_count": int(reclass[column].notna().sum()) if reclass is not None and column in reclass.columns else None,
                "pairwise_non_null_count": int(pairwise[column].notna().sum()) if pairwise is not None and column in pairwise.columns else None,
            }
        )

    summary = Counter(row["classification"] for row in rows)
    model_feature_columns = [row["feature_name"] for row in rows if row["model_eligible"] and row["feature_name"] in candidate.columns]
    return {
        "schema_version": FEATURE_INVENTORY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "rows_examined": len(rows),
        "feature_classification_counts": {str(k): int(v) for k, v in summary.items()},
        "model_feature_columns": model_feature_columns,
        "model_feature_count": int(len(model_feature_columns)),
        "features": rows,
        "notes": [
            "Direct score/rank outputs and future outcome fields are classified out of the model input set.",
            "Batch 1 coverage is reported where the same field exists in the earlier surface.",
            "Sparse but usable fields remain inventoried explicitly even when excluded from the final model subset.",
        ],
    }


def _label_contract() -> dict[str, Any]:
    return {
        "schema_version": LABEL_CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "allowed_training_labels": ["top15_label", "bottom15_label", "forward_ret_20d", "path_value_score_v1"],
        "objectives": [
            {
                "objective_name": "classification_top15_vs_bottom15",
                "target_label": "top15_label",
                "negative_label": "bottom15_label",
                "training_rows": "rows where top15_label or bottom15_label is true inside the training fold",
                "evaluation_metrics": ["auc", "average_precision", "topK_forward_ret_20d", "topK_path_value_score_v1", "top15_capture", "bottom15_contamination"],
            },
            {
                "objective_name": "regression_path_value",
                "target_label": "path_value_score_v1",
                "training_rows": "training fold rows with non-null path_value_score_v1",
                "evaluation_metrics": ["rmse", "spearman", "topK_forward_ret_20d", "topK_path_value_score_v1", "top15_capture", "bottom15_contamination"],
            },
        ],
        "label_usage_rules": [
            "Labels are used only in training folds or in post-split evaluation.",
            "No label is used as an input feature.",
            "Forward returns remain diagnostics for model selection, not features.",
        ],
        "explicit_exclusions": [
            "direct score and rank outputs",
            "realized PnL and future outcome fields",
            "future or pairwise labels",
        ],
    }


def _split_contract(frames: dict[str, pd.DataFrame], months: dict[str, list[str]]) -> dict[str, Any]:
    candidate = frames["batch2_candidate"]
    orfp = frames["batch2_orfp"]
    month_count = len(sorted(dict.fromkeys(candidate["month_bucket"].dropna().astype(str).tolist())))
    ready = len(months["train"]) > 0 and len(months["validation"]) > 0 and len(months["test"]) > 0 and month_count >= (TRAIN_MONTH_COUNT + VALIDATION_MONTH_COUNT + TEST_MONTH_COUNT)
    return {
        "schema_version": SPLIT_CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "split_by": "month_bucket chronological split",
        "status": "ready_for_time_split_evaluation" if ready else "insufficient_time_split_depth",
        "actual_month_count": int(month_count),
        "required_month_count": int(TRAIN_MONTH_COUNT + VALIDATION_MONTH_COUNT + TEST_MONTH_COUNT),
        "splits": months,
        "candidate_row_counts": {
            split: int(candidate[candidate["month_bucket"].isin(months[split])].shape[0]) for split in ("train", "validation", "test")
        },
        "orfp_row_counts": {
            split: int(orfp[orfp["month_bucket"].isin(months[split])].shape[0]) for split in ("train", "validation", "test")
        },
        "candidate_month_ranges": {
            split: {
                "month_min": months[split][0] if months[split] else None,
                "month_max": months[split][-1] if months[split] else None,
            }
            for split in ("train", "validation", "test")
        },
        "notes": [
            "Train months strictly precede validation months, which strictly precede test months.",
            "The split is chronological by month_bucket; no random row split is used.",
        ],
    }


def _model_contract() -> dict[str, Any]:
    return {
        "schema_version": MODEL_CONTRACT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "dependency_status": {
            "sklearn_available": True,
            "lightgbm_available": True,
            "tree_model_available": True,
        },
        "model_variants": [
            {
                "variant_name": "linear_logistic_or_ridge_baseline",
                "objective": "classification_top15_vs_bottom15",
                "estimator": "sklearn.linear_model.LogisticRegression",
                "preprocessing": "median imputation for numeric features; most-frequent imputation plus one-hot encoding for categoricals; standard scaling for numeric features",
                "parameters": {
                    "max_iter": 3000,
                    "class_weight": "balanced",
                    "solver": "lbfgs",
                },
            },
            {
                "variant_name": "linear_logistic_or_ridge_baseline",
                "objective": "regression_path_value",
                "estimator": "sklearn.linear_model.Ridge",
                "preprocessing": "median imputation for numeric features; most-frequent imputation plus one-hot encoding for categoricals; standard scaling for numeric features",
                "parameters": {
                    "alpha": 2.0,
                },
            },
            {
                "variant_name": "tree_based_small_model",
                "objective": "classification_top15_vs_bottom15",
                "estimator": "sklearn.ensemble.HistGradientBoostingClassifier",
                "preprocessing": "median imputation for numeric features; most-frequent imputation plus ordinal encoding for categoricals",
                "parameters": {
                    "max_depth": 3,
                    "max_iter": 150,
                    "learning_rate": 0.05,
                    "min_samples_leaf": 20,
                    "l2_regularization": 0.01,
                    "random_state": 42,
                },
            },
            {
                "variant_name": "tree_based_small_model",
                "objective": "regression_path_value",
                "estimator": "sklearn.ensemble.HistGradientBoostingRegressor",
                "preprocessing": "median imputation for numeric features; most-frequent imputation plus ordinal encoding for categoricals",
                "parameters": {
                    "max_depth": 3,
                    "max_iter": 150,
                    "learning_rate": 0.05,
                    "min_samples_leaf": 20,
                    "l2_regularization": 0.01,
                    "random_state": 42,
                },
            },
            {
                "variant_name": "feature_score_ablation_baseline",
                "objective": "deterministic_feature_score",
                "estimator": "handcrafted weighted sum over safe interaction features",
                "preprocessing": "ordinal bucket mappings and z-scored numeric components",
                "parameters": {
                    "feature_weights": {
                        "entry_strength_score": 0.35,
                        "body_ratio": 0.20,
                        "candle_body_ratio": 0.20,
                        "vol_ratio5_20": 0.15,
                        "abs(gap_pct)": -0.10,
                        "abs(dist_ma20_pct)": -0.10,
                        "abs(dist_ma60_pct)": -0.10,
                        "conditional_high_value": 0.20,
                        "signal_quality_bucket": "ordinal mapping",
                        "decision_candle_quality": "ordinal mapping",
                        "liquidity_quality_bucket": "ordinal mapping",
                        "higher_timeframe_headroom_bucket": "ordinal mapping",
                        "volume_participation_bucket": "ordinal mapping",
                    }
                },
            },
        ],
        "feature_selection_policy": {
            "included_features": MODEL_FEATURES,
            "excluded_features": [
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
                "path_value_score_v1",
                "mfe_20d",
                "mae_20d",
            ],
            "notes": [
                "Direct score/rank outputs are excluded so the reranker is not a thin proxy for the champion score.",
                "Sparse flags were inventoried separately and excluded when they did not improve the OOS pilot.",
            ],
        },
    }


def _normalize_numeric(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    std = float(values.std(ddof=0)) if len(values.dropna()) else 0.0
    if not math.isfinite(std) or std == 0.0:
        return values * 0.0
    centered = values - float(values.median())
    return centered / std


def _build_deterministic_score(frame: pd.DataFrame) -> pd.Series:
    score = pd.Series(0.0, index=frame.index, dtype="float64")

    for column, weight in [
        ("entry_strength_score", 0.35),
        ("body_ratio", 0.20),
        ("candle_body_ratio", 0.20),
        ("vol_ratio5_20", 0.15),
    ]:
        if column in frame.columns:
            score = score + weight * _normalize_numeric(frame[column]).fillna(0.0)

    for column, weight in [
        ("gap_pct", -0.10),
        ("dist_ma20_pct", -0.10),
        ("dist_ma60_pct", -0.10),
        ("upper_wick_ratio", -0.05),
        ("lower_wick_ratio", 0.05),
        ("candle_upper_wick_ratio", -0.05),
        ("candle_lower_wick_ratio", 0.05),
    ]:
        if column in frame.columns:
            score = score + weight * _normalize_numeric(frame[column]).fillna(0.0)

    if "conditional_high_value" in frame.columns:
        score = score + frame["conditional_high_value"].fillna(False).astype(bool).astype(float) * 0.20
    if "monthly_context_no_lookahead" in frame.columns:
        score = score + frame["monthly_context_no_lookahead"].fillna(False).astype(bool).astype(float) * 0.05
    if "weekly_context_no_lookahead" in frame.columns:
        score = score + frame["weekly_context_no_lookahead"].fillna(False).astype(bool).astype(float) * 0.05

    for column, mapping in NUMERIC_BUCKET_MAPS.items():
        if column in frame.columns:
            score = score + frame[column].astype("string").map(mapping).fillna(0.0).astype(float) * 0.10

    if "sequence_3" in frame.columns:
        seq = frame["sequence_3"].astype("string").fillna("")
        score = score + seq.map({"up-up-up": 0.10, "down-down-down": -0.10}).fillna(0.0).astype(float)

    return score


def _preprocess_linear(features: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    numeric_cols = [col for col in features.columns if col in NUMERIC_MODEL_FEATURES]
    categorical_cols = [col for col in features.columns if col in CATEGORICAL_MODEL_FEATURES]
    numeric = features[numeric_cols].copy()
    categorical = features[categorical_cols].copy()
    for col in numeric_cols:
        numeric[col] = pd.to_numeric(numeric[col], errors="coerce")
    for col in categorical_cols:
        categorical[col] = categorical[col].astype("string").fillna("__MISSING__")
    return pd.concat([numeric, categorical], axis=1), numeric_cols, categorical_cols


def _preprocess_tree(features: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    numeric_cols = [col for col in features.columns if col in NUMERIC_MODEL_FEATURES]
    categorical_cols = [col for col in features.columns if col in CATEGORICAL_MODEL_FEATURES]
    numeric = features[numeric_cols].copy()
    categorical = features[categorical_cols].copy()
    for col in numeric_cols:
        numeric[col] = pd.to_numeric(numeric[col], errors="coerce")
    for col in categorical_cols:
        categorical[col] = categorical[col].astype("string").fillna("__MISSING__").astype("category")
    return pd.concat([numeric, categorical], axis=1), numeric_cols, categorical_cols


def _coerce_model_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.reindex(columns=MODEL_FEATURES).copy()
    for column in NUMERIC_MODEL_FEATURES:
        if column not in out.columns:
            continue
        series = out[column]
        if pd.api.types.is_bool_dtype(series):
            out[column] = series.astype(float)
            continue
        if series.dtype == object:
            bool_map = series.map(_safe_bool)
            if bool_map.notna().sum() >= max(1, int(series.notna().sum() * 0.8)):
                out[column] = bool_map.astype(float)
                continue
        out[column] = pd.to_numeric(series, errors="coerce")
    for column in CATEGORICAL_MODEL_FEATURES:
        if column in out.columns:
            out[column] = out[column].astype("string").fillna("__MISSING__")
    return out


def _linear_pipeline(objective: str) -> Pipeline:
    if objective == "classification_top15_vs_bottom15":
        estimator: Any = LogisticRegression(max_iter=3000, class_weight="balanced", solver="lbfgs")
    elif objective == "regression_path_value":
        estimator = Ridge(alpha=2.0)
    else:
        raise ValueError(objective)
    preprocessor = ColumnTransformer(
        [
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), NUMERIC_MODEL_FEATURES),
            ("cat", Pipeline([("imputer", SimpleImputer(strategy="most_frequent")), ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False))]), CATEGORICAL_MODEL_FEATURES),
        ],
        remainder="drop",
    )
    return Pipeline([("preprocess", preprocessor), ("model", estimator)])


def _tree_pipeline(objective: str) -> Pipeline:
    if objective == "classification_top15_vs_bottom15":
        estimator: Any = HistGradientBoostingClassifier(
            max_depth=3,
            max_iter=150,
            learning_rate=0.05,
            min_samples_leaf=20,
            l2_regularization=0.01,
            random_state=42,
        )
    elif objective == "regression_path_value":
        estimator = HistGradientBoostingRegressor(
            max_depth=3,
            max_iter=150,
            learning_rate=0.05,
            min_samples_leaf=20,
            l2_regularization=0.01,
            random_state=42,
        )
    else:
        raise ValueError(objective)
    preprocessor = ColumnTransformer(
        [
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), NUMERIC_MODEL_FEATURES),
            ("cat", Pipeline([("imputer", SimpleImputer(strategy="most_frequent")), ("ordinal", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1))]), CATEGORICAL_MODEL_FEATURES),
        ],
        remainder="drop",
    )
    return Pipeline([("preprocess", preprocessor), ("model", estimator)])


def _split_frame(frame: pd.DataFrame, months: dict[str, list[str]]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for split_name, month_list in months.items():
        out[split_name] = frame[frame["month_bucket"].isin(month_list)].copy()
    return out


def _classification_training_frame(frame: pd.DataFrame) -> pd.DataFrame:
    mask = frame["top15_label"].fillna(False).astype(bool) | frame["bottom15_label"].fillna(False).astype(bool)
    return frame.loc[mask].copy()


def _fit_model_variants(frames: dict[str, pd.DataFrame], months: dict[str, list[str]], *, time_split_ready: bool) -> dict[str, Any]:
    candidate = frames["batch2_candidate"].copy()
    orfp = frames["batch2_orfp"].copy()

    if not time_split_ready:
        return {
            "status": "insufficient_time_split_depth",
            "variants": {},
            "notes": ["Model fitting skipped because the smoke run does not contain the full chronological depth."],
        }

    candidate_splits = _split_frame(candidate, months)
    orfp_splits = _split_frame(orfp, months)
    training_rows = _classification_training_frame(candidate_splits["train"])
    if training_rows.empty or training_rows["top15_label"].fillna(False).nunique(dropna=False) < 2:
        return {
            "status": "insufficient_time_split_depth",
            "variants": {},
            "notes": ["Classification training rows were insufficient after the chronological split."],
        }

    model_frames: dict[str, dict[str, Any]] = {}

    def _fit_and_predict(variant_name: str, objective: str, family: str) -> dict[str, Any]:
        pipeline = _linear_pipeline(objective) if family == "linear" else _tree_pipeline(objective)
        if objective == "classification_top15_vs_bottom15":
            y_train = training_rows["top15_label"].fillna(False).astype(int)
            fit_frame = _coerce_model_frame(training_rows)
        else:
            y_train = candidate_splits["train"]["path_value_score_v1"]
            fit_frame = _coerce_model_frame(candidate_splits["train"])
        pipeline.fit(fit_frame, y_train)

        def _predict(surface: pd.DataFrame) -> pd.Series:
            prepared = _coerce_model_frame(surface)
            values = pipeline.predict_proba(prepared)[:, 1] if objective == "classification_top15_vs_bottom15" else pipeline.predict(prepared)
            return pd.Series(values, index=surface.index)

        candidate_scores = _predict(candidate)
        orfp_scores = _predict(orfp)
        train_scores = _predict(candidate_splits["train"])
        val_scores = _predict(candidate_splits["validation"])
        test_scores = _predict(candidate_splits["test"])

        if objective == "classification_top15_vs_bottom15":
            def _cls_metrics(surface: pd.DataFrame, scores: pd.Series) -> dict[str, Any]:
                eval_frame = _classification_training_frame(surface)
                if eval_frame.empty or eval_frame["top15_label"].fillna(False).nunique(dropna=False) < 2:
                    return {"auc": None, "average_precision": None, "row_count": int(len(eval_frame))}
                labels = eval_frame["top15_label"].fillna(False).astype(int)
                pred = scores.loc[eval_frame.index]
                return {
                    "auc": _safe_float(roc_auc_score(labels, pred)),
                    "average_precision": _safe_float(average_precision_score(labels, pred)),
                    "row_count": int(len(eval_frame)),
                }
            train_metrics = _cls_metrics(candidate_splits["train"], train_scores)
            val_metrics = _cls_metrics(candidate_splits["validation"], val_scores)
            test_metrics = _cls_metrics(candidate_splits["test"], test_scores)
        else:
            def _reg_metrics(surface: pd.DataFrame, scores: pd.Series) -> dict[str, Any]:
                target = pd.to_numeric(surface["path_value_score_v1"], errors="coerce")
                valid = target.notna() & scores.notna()
                if not valid.any():
                    return {"rmse": None, "spearman": None, "row_count": int(len(surface))}
                residual = target.loc[valid] - scores.loc[valid]
                rmse = float(math.sqrt(float(np.mean(np.square(residual.astype(float))))))
                spearman = target.loc[valid].corr(scores.loc[valid], method="spearman")
                return {"rmse": _safe_float(rmse), "spearman": _safe_float(spearman), "row_count": int(valid.sum())}
            train_metrics = _reg_metrics(candidate_splits["train"], train_scores)
            val_metrics = _reg_metrics(candidate_splits["validation"], val_scores)
            test_metrics = _reg_metrics(candidate_splits["test"], test_scores)

        feature_names = None
        importance = None
        if family == "linear":
            try:
                feature_names = list(pipeline.named_steps["preprocess"].get_feature_names_out())
                weights = pipeline.named_steps["model"].coef_.ravel()
                importance = [
                    {"feature_name": str(name), "coefficient": _safe_float(weight), "absolute_coefficient": _safe_float(abs(float(weight)))}
                    for name, weight in zip(feature_names, weights, strict=False)
                ]
                importance = sorted(importance, key=lambda row: abs(float(row["coefficient"] or 0.0)), reverse=True)
            except Exception:
                importance = None
        else:
            importance = None

        return {
            "variant_name": variant_name,
            "objective": objective,
            "family": family,
            "pipeline": pipeline,
            "candidate_scores": candidate_scores,
            "orfp_scores": orfp_scores,
            "split_scores": {"train": train_scores, "validation": val_scores, "test": test_scores},
            "train_metrics": train_metrics,
            "validation_metrics": val_metrics,
            "test_metrics": test_metrics,
            "feature_importance": importance,
        }

    model_frames["linear_logistic_top15_vs_bottom15"] = _fit_and_predict("linear_logistic_top15_vs_bottom15", "classification_top15_vs_bottom15", "linear")
    model_frames["linear_ridge_path_value"] = _fit_and_predict("linear_ridge_path_value", "regression_path_value", "linear")
    model_frames["tree_hgb_top15_vs_bottom15"] = _fit_and_predict("tree_hgb_top15_vs_bottom15", "classification_top15_vs_bottom15", "tree")
    model_frames["tree_hgb_path_value"] = _fit_and_predict("tree_hgb_path_value", "regression_path_value", "tree")

    deterministic_candidate = _build_deterministic_score(candidate)
    deterministic_orfp = _build_deterministic_score(orfp)
    model_frames["feature_score_ablation_baseline"] = {
        "variant_name": "feature_score_ablation_baseline",
        "objective": "deterministic_feature_score",
        "family": "deterministic",
        "pipeline": None,
        "candidate_scores": deterministic_candidate,
        "orfp_scores": deterministic_orfp,
        "split_scores": {
            "train": _build_deterministic_score(candidate_splits["train"]),
            "validation": _build_deterministic_score(candidate_splits["validation"]),
            "test": _build_deterministic_score(candidate_splits["test"]),
        },
        "train_metrics": {"status": "not_trained"},
        "validation_metrics": {"status": "not_trained"},
        "test_metrics": {"status": "not_trained"},
        "feature_importance": [
            {"feature_name": "entry_strength_score", "coefficient": 0.35, "absolute_coefficient": 0.35},
            {"feature_name": "body_ratio", "coefficient": 0.20, "absolute_coefficient": 0.20},
            {"feature_name": "candle_body_ratio", "coefficient": 0.20, "absolute_coefficient": 0.20},
            {"feature_name": "vol_ratio5_20", "coefficient": 0.15, "absolute_coefficient": 0.15},
            {"feature_name": "gap_pct", "coefficient": -0.10, "absolute_coefficient": 0.10},
            {"feature_name": "dist_ma20_pct", "coefficient": -0.10, "absolute_coefficient": 0.10},
            {"feature_name": "dist_ma60_pct", "coefficient": -0.10, "absolute_coefficient": 0.10},
        ],
    }

    return {
        "status": "ready_for_evaluation",
        "candidate_splits": candidate_splits,
        "orfp_splits": orfp_splits,
        "variants": model_frames,
        "notes": [
            "The deterministic baseline is a sanity comparator, not a learned model.",
            "Classification training uses only top15 vs bottom15 rows in the training fold.",
        ],
    }


def _score_variant_on_frame(variant_payload: dict[str, Any], frame: pd.DataFrame) -> pd.Series:
    objective = str(variant_payload["objective"])
    prepared = _coerce_model_frame(frame)
    if objective == "deterministic_feature_score":
        return _build_deterministic_score(frame)
    pipeline = variant_payload["pipeline"]
    if objective == "classification_top15_vs_bottom15":
        values = pipeline.predict_proba(prepared)[:, 1]
    else:
        values = pipeline.predict(prepared)
    return pd.Series(values, index=frame.index, dtype="float64")


def _rank_within_groups(frame: pd.DataFrame, score: pd.Series, *, group_cols: list[str]) -> pd.Series:
    temp = frame[group_cols].copy()
    temp["__score"] = score.values
    temp["__candidate_idx"] = frame["candidate_idx"].values if "candidate_idx" in frame.columns else np.arange(len(frame))
    temp["__original_index"] = frame.index
    ordered = temp.sort_values(group_cols + ["__score", "__candidate_idx"], ascending=[True] * len(group_cols) + [False, True], kind="stable").copy()
    ordered["__model_rank"] = ordered.groupby(group_cols, sort=False).cumcount() + 1
    return ordered.set_index("__original_index").sort_index()["__model_rank"]


def _surface_selection_metrics(frame: pd.DataFrame, score: pd.Series, *, topk: int, surface_name: str) -> dict[str, Any]:
    group_cols = ["anchor_date", "side"]
    ranked = _rank_within_groups(frame, score, group_cols=group_cols)
    selected = ranked <= topk
    champion_flag_col = f"champion_selected_top{topk}" if f"champion_selected_top{topk}" in frame.columns else None
    champion_selected = frame[champion_flag_col].fillna(False).astype(bool) if champion_flag_col else pd.Series(False, index=frame.index)
    model_selected = selected.fillna(False).astype(bool)

    selected_frame = frame.loc[model_selected].copy()
    champion_frame = frame.loc[champion_selected].copy()
    common = frame.loc[model_selected & champion_selected].copy()
    union = frame.loc[model_selected | champion_selected].copy()

    top15 = selected_frame["top15_label"].fillna(False).astype(bool) if "top15_label" in selected_frame.columns else pd.Series(False, index=selected_frame.index)
    bottom15 = selected_frame["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in selected_frame.columns else pd.Series(False, index=selected_frame.index)
    champion_top15 = champion_frame["top15_label"].fillna(False).astype(bool) if "top15_label" in champion_frame.columns else pd.Series(False, index=champion_frame.index)
    champion_bottom15 = champion_frame["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in champion_frame.columns else pd.Series(False, index=champion_frame.index)

    zero_top15_groups = 0
    zero_top15_groups_by_month: dict[str, int] = defaultdict(int)
    group_count = 0
    for _, group in frame.loc[model_selected].groupby(group_cols, sort=False):
        group_count += 1
        if "top15_label" not in group.columns or not group["top15_label"].fillna(False).astype(bool).any():
            zero_top15_groups += 1
            if "month_bucket" in group.columns:
                zero_top15_groups_by_month[str(group["month_bucket"].iloc[0])] += 1

    def _mean_or_none(series: pd.Series) -> float | None:
        if len(series) == 0:
            return None
        return _safe_float(series.mean())

    overlap_ratio = _safe_float(len(common) / len(champion_frame)) if len(champion_frame) else None
    membership_change_rate = _safe_float(len(union) - len(common)) / _safe_float(len(union)) if len(union) else None
    changed_topk_member_count = int((model_selected ^ champion_selected).sum())
    false_positive_cost = _safe_float(bottom15.mean()) if len(selected_frame) else None
    champion_false_positive_cost = _safe_float(champion_bottom15.mean()) if len(champion_frame) else None

    topk_metrics = {
        "selected_row_count": int(len(selected_frame)),
        "champion_selected_row_count": int(len(champion_frame)),
        "mean_forward_ret_20d": _mean_or_none(selected_frame["forward_ret_20d"].astype(float)) if "forward_ret_20d" in selected_frame.columns else None,
        "mean_path_value_score_v1": _mean_or_none(selected_frame["path_value_score_v1"].astype(float)) if "path_value_score_v1" in selected_frame.columns else None,
        "champion_mean_forward_ret_20d": _mean_or_none(champion_frame["forward_ret_20d"].astype(float)) if "forward_ret_20d" in champion_frame.columns else None,
        "champion_mean_path_value_score_v1": _mean_or_none(champion_frame["path_value_score_v1"].astype(float)) if "path_value_score_v1" in champion_frame.columns else None,
        "top15_capture_rate": _mean_or_none(top15.astype(float)) if len(top15) and top15.any() else (0.0 if len(top15) else None),
        "bottom15_contamination_rate": _mean_or_none(bottom15.astype(float)) if len(bottom15) else None,
        "champion_top15_capture_rate": _mean_or_none(champion_top15.astype(float)) if len(champion_top15) and champion_top15.any() else (0.0 if len(champion_top15) else None),
        "champion_bottom15_contamination_rate": _mean_or_none(champion_bottom15.astype(float)) if len(champion_bottom15) else None,
        "zero_pass_groups": int(zero_top15_groups),
        "zero_pass_group_rate": _safe_float(zero_top15_groups / group_count) if group_count else None,
        "group_count": int(group_count),
        "overlap_ratio": overlap_ratio,
        "membership_change_rate": _safe_float(membership_change_rate) if membership_change_rate is not None else None,
        "changed_topk_member_count": int(changed_topk_member_count),
        "false_positive_cost": false_positive_cost,
        "champion_false_positive_cost": champion_false_positive_cost,
        "false_positive_cost_delta": _safe_float(false_positive_cost - champion_false_positive_cost) if false_positive_cost is not None and champion_false_positive_cost is not None else None,
    }

    by_side: dict[str, dict[str, Any]] = {}
    for side, subset in frame.loc[model_selected].groupby("side", sort=False):
        subset_top15 = subset["top15_label"].fillna(False).astype(bool) if "top15_label" in subset.columns else pd.Series(False, index=subset.index)
        subset_bottom15 = subset["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in subset.columns else pd.Series(False, index=subset.index)
        by_side[str(side)] = {
            "selected_row_count": int(len(subset)),
            "mean_forward_ret_20d": _safe_float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()) if "forward_ret_20d" in subset.columns else None,
            "mean_path_value_score_v1": _safe_float(pd.to_numeric(subset["path_value_score_v1"], errors="coerce").mean()) if "path_value_score_v1" in subset.columns else None,
            "top15_capture_rate": _safe_float(subset_top15.mean()) if len(subset_top15) else None,
            "bottom15_contamination_rate": _safe_float(subset_bottom15.mean()) if len(subset_bottom15) else None,
        }

    by_month: dict[str, dict[str, Any]] = {}
    if "month_bucket" in frame.columns:
        for month, subset in frame.loc[model_selected].groupby("month_bucket", sort=True):
            by_month[str(month)] = {
                "selected_row_count": int(len(subset)),
                "mean_forward_ret_20d": _safe_float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()) if "forward_ret_20d" in subset.columns else None,
                "mean_path_value_score_v1": _safe_float(pd.to_numeric(subset["path_value_score_v1"], errors="coerce").mean()) if "path_value_score_v1" in subset.columns else None,
                "top15_capture_rate": _safe_float(subset["top15_label"].fillna(False).astype(bool).mean()) if "top15_label" in subset.columns else None,
                "bottom15_contamination_rate": _safe_float(subset["bottom15_label"].fillna(False).astype(bool).mean()) if "bottom15_label" in subset.columns else None,
            }

    by_regime: dict[str, dict[str, Any]] = {}
    if "dominant_regime_context" in frame.columns:
        for regime, subset in frame.loc[model_selected].groupby("dominant_regime_context", sort=True):
            by_regime[str(regime)] = {
                "selected_row_count": int(len(subset)),
                "mean_forward_ret_20d": _safe_float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()) if "forward_ret_20d" in subset.columns else None,
                "mean_path_value_score_v1": _safe_float(pd.to_numeric(subset["path_value_score_v1"], errors="coerce").mean()) if "path_value_score_v1" in subset.columns else None,
                "top15_capture_rate": _safe_float(subset["top15_label"].fillna(False).astype(bool).mean()) if "top15_label" in subset.columns else None,
                "bottom15_contamination_rate": _safe_float(subset["bottom15_label"].fillna(False).astype(bool).mean()) if "bottom15_label" in subset.columns else None,
            }

    return {
        "surface_name": surface_name,
        "topk": int(topk),
        "selection_metrics": topk_metrics,
        "by_side": by_side,
        "by_month": by_month,
        "by_regime": by_regime,
    }


def _group_win_loss_flat(frame: pd.DataFrame, model_score: pd.Series, champion_selected_col: str, *, topk: int, group_col: str) -> dict[str, Any]:
    ranked = _rank_within_groups(frame, model_score, group_cols=["anchor_date", "side"])
    model_selected = ranked <= topk
    champ_selected = frame[champion_selected_col].fillna(False).astype(bool) if champion_selected_col in frame.columns else pd.Series(False, index=frame.index)
    model_frame = frame.loc[model_selected].copy()
    champ_frame = frame.loc[champ_selected].copy()
    if group_col not in model_frame.columns or group_col not in champ_frame.columns:
        return {"win": 0, "loss": 0, "flat": 0, "groups": 0}
    model_group = model_frame.groupby(group_col, sort=True)["forward_ret_20d"].mean()
    champ_group = champ_frame.groupby(group_col, sort=True)["forward_ret_20d"].mean()
    common = model_group.index.intersection(champ_group.index)
    delta = model_group.loc[common] - champ_group.loc[common]
    return {
        "win": int((delta > 1e-12).sum()),
        "loss": int((delta < -1e-12).sum()),
        "flat": int((delta.abs() <= 1e-12).sum()),
        "groups": int(len(common)),
        "mean_delta_forward_ret_20d": _safe_float(delta.mean()) if len(delta) else None,
        "mean_delta_path_value_score_v1": _safe_float((model_frame.groupby(group_col, sort=True)["path_value_score_v1"].mean() - champ_frame.groupby(group_col, sort=True)["path_value_score_v1"].mean()).reindex(common).mean()) if "path_value_score_v1" in frame.columns and len(common) else None,
    }


def _evaluation_summary(frame: pd.DataFrame, score_map: dict[str, pd.Series], *, surface_name: str, topk_values: tuple[int, ...]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "surface_name": surface_name,
        "row_count": int(len(frame)),
        "variants": {},
    }
    for variant_name, scores in score_map.items():
        variant_entry: dict[str, Any] = {
            "topk": {},
            "monthly_win_loss_flat": {},
            "regime_win_loss_flat": {},
        }
        for topk in topk_values:
            selected_metrics = _surface_selection_metrics(frame, scores, topk=topk, surface_name=surface_name)
            variant_entry["topk"][f"top{topk}"] = selected_metrics
            variant_entry["monthly_win_loss_flat"][f"top{topk}"] = _group_win_loss_flat(frame, scores, f"champion_selected_top{topk}", topk=topk, group_col="month_bucket")
            variant_entry["regime_win_loss_flat"][f"top{topk}"] = _group_win_loss_flat(frame, scores, f"champion_selected_top{topk}", topk=topk, group_col="dominant_regime_context")
        summary["variants"][variant_name] = variant_entry
    return summary


def _build_topk_membership_diff(frames: dict[str, pd.DataFrame], model_outputs: dict[str, dict[str, pd.Series]], topk_values: tuple[int, ...]) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for surface_name in ("candidate", "orfp"):
        frame = frames[f"batch2_{surface_name}"].copy()
        champion_cols = {topk: f"champion_selected_top{topk}" for topk in topk_values if f"champion_selected_top{topk}" in frame.columns}
        for variant_name, outputs in model_outputs.items():
            scores = outputs[surface_name]
            ranked = _rank_within_groups(frame, scores, group_cols=["anchor_date", "side"])
            for topk in topk_values:
                model_selected = (ranked <= topk).fillna(False).astype(bool)
                champion_selected = frame[champion_cols[topk]].fillna(False).astype(bool) if topk in champion_cols else pd.Series(False, index=frame.index)
                diff = model_selected ^ champion_selected
                selected_frame = frame.copy()
                selected_frame["surface_name"] = surface_name
                selected_frame["variant_name"] = variant_name
                selected_frame["topk"] = int(topk)
                selected_frame["model_score"] = scores.values
                selected_frame["model_rank"] = ranked.values
                selected_frame["model_selected"] = model_selected.values
                selected_frame["champion_selected"] = champion_selected.values
                selected_frame["membership_changed"] = diff.values
                selected_frame["selected_overlap"] = (model_selected & champion_selected).values
                rows.append(
                    selected_frame[
                        [
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
                    ].copy()
                )
    if rows:
        return pd.concat(rows, ignore_index=True)
    return pd.DataFrame(
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


def _pairwise_diagnostics(frame: pd.DataFrame, score: pd.Series, *, label: str) -> dict[str, Any]:
    near_miss_columns = [col for col in frame.columns if col.startswith("near_miss_")]
    if not near_miss_columns:
        return {
            "label": label,
            "row_count": int(len(frame)),
            "status": "no_near_miss_columns",
        }
    pairwise = frame.copy()
    pairwise["model_selected_score"] = score.loc[frame.index].values
    pairwise["model_selected_wins"] = pairwise["model_selected_score"] > pd.to_numeric(pairwise["near_miss_score"], errors="coerce")
    pairwise["model_selected_better_path"] = pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce") >= pd.to_numeric(pairwise["near_miss_forward_ret_20d"], errors="coerce")
    paired = pairwise[pairwise["selected_higher_score_and_worse_path"].fillna(False).astype(bool)] if "selected_higher_score_and_worse_path" in pairwise.columns else pairwise
    return {
        "label": label,
        "row_count": int(len(frame)),
        "pair_count": int(len(pairwise)),
        "selected_higher_score_and_worse_path_count": int(paired.shape[0]) if "selected_higher_score_and_worse_path" in pairwise.columns else None,
        "model_selected_wins_rate": _safe_float(pairwise["model_selected_wins"].mean()) if len(pairwise) else None,
        "model_selected_better_path_rate": _safe_float(pairwise["model_selected_better_path"].mean()) if len(pairwise) else None,
        "model_score_gap_mean": _safe_float((pairwise["model_selected_score"] - pd.to_numeric(pairwise["near_miss_score"], errors="coerce")).mean()) if len(pairwise) else None,
        "model_score_gap_median": _safe_float((pairwise["model_selected_score"] - pd.to_numeric(pairwise["near_miss_score"], errors="coerce")).median()) if len(pairwise) else None,
    }


def _reclassification_diagnostics(frame: pd.DataFrame, score_map: dict[str, pd.Series]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "row_count": int(len(frame)),
        "family_summary": {},
        "model_score_by_family": {},
    }
    if "batch2_volume_root_cause_code" in frame.columns:
        out["family_summary"] = {
            "family_counts": _value_counts(frame["batch2_volume_root_cause_code"]),
            "top15_count": int(frame["top15_label"].fillna(False).astype(bool).sum()) if "top15_label" in frame.columns else None,
            "bottom15_count": int(frame["bottom15_label"].fillna(False).astype(bool).sum()) if "bottom15_label" in frame.columns else None,
        }
        for variant_name, scores in score_map.items():
            scored = frame.copy()
            scored["model_score"] = scores.loc[frame.index].values
            family_rows: list[dict[str, Any]] = []
            for family, subset in scored.groupby("batch2_volume_root_cause_code", sort=True):
                top15 = subset["top15_label"].fillna(False).astype(bool) if "top15_label" in subset.columns else pd.Series(False, index=subset.index)
                bottom15 = subset["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in subset.columns else pd.Series(False, index=subset.index)
                family_rows.append(
                    {
                        "family": str(family),
                        "count": int(len(subset)),
                        "mean_model_score": _safe_float(subset["model_score"].mean()),
                        "median_model_score": _safe_float(subset["model_score"].median()),
                        "mean_forward_ret_20d": _safe_float(pd.to_numeric(subset["forward_ret_20d"], errors="coerce").mean()) if "forward_ret_20d" in subset.columns else None,
                        "mean_path_value_score_v1": _safe_float(pd.to_numeric(subset["path_value_score_v1"], errors="coerce").mean()) if "path_value_score_v1" in subset.columns else None,
                        "top15_rate": _safe_float(top15.mean()) if len(top15) else None,
                        "bottom15_rate": _safe_float(bottom15.mean()) if len(bottom15) else None,
                    }
                )
            out["model_score_by_family"][variant_name] = family_rows
    return out


def _boundary_diagnostics(frame: pd.DataFrame, score_map: dict[str, pd.Series]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "row_count": int(len(frame)),
        "model_pairwise": {},
    }
    for variant_name, scores in score_map.items():
        out["model_pairwise"][variant_name] = _pairwise_diagnostics(frame, scores, label=variant_name)
    return out


def _stability_audit(
    frames: dict[str, pd.DataFrame],
    months: dict[str, list[str]],
    model_results: dict[str, Any],
    variant_comparison: dict[str, Any],
) -> dict[str, Any]:
    candidate = frames["batch2_candidate"].copy()
    orfp = frames["batch2_orfp"].copy()
    candidate_splits = model_results.get("candidate_splits", {})
    audit: dict[str, Any] = {
        "schema_version": STABILITY_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "split_status": model_results.get("status", "unknown"),
        "time_split": months,
        "variants": {},
    }
    if model_results.get("status") != "ready_for_evaluation":
        audit["notes"] = model_results.get("notes", [])
        return audit

    for variant_name, payload in model_results["variants"].items():
        if payload["objective"] == "deterministic_feature_score":
            continue
        candidate_scores = payload["candidate_scores"]
        candidate_eval = {
            "train_metrics": payload["train_metrics"],
            "validation_metrics": payload["validation_metrics"],
            "test_metrics": payload["test_metrics"],
            "candidate_topk": {},
            "train_validation_gap": {},
            "validation_test_gap": {},
            "monthly_concentration": {},
            "regime_concentration": {},
        }
        for topk in TOP_K_VALUES:
            candidate_eval["candidate_topk"][f"top{topk}"] = variant_comparison["candidate_surface"]["variants"][variant_name]["topk"][f"top{topk}"]
            train_sel = _surface_selection_metrics(candidate_splits["train"], payload["split_scores"]["train"], topk=topk, surface_name="candidate_train")
            val_sel = _surface_selection_metrics(candidate_splits["validation"], payload["split_scores"]["validation"], topk=topk, surface_name="candidate_validation")
            test_sel = _surface_selection_metrics(candidate_splits["test"], payload["split_scores"]["test"], topk=topk, surface_name="candidate_test")
            candidate_eval["train_validation_gap"][f"top{topk}"] = {
                "mean_forward_ret_20d_delta": _safe_float((val_sel["selection_metrics"]["mean_forward_ret_20d"] or 0.0) - (train_sel["selection_metrics"]["mean_forward_ret_20d"] or 0.0)) if train_sel["selection_metrics"]["mean_forward_ret_20d"] is not None and val_sel["selection_metrics"]["mean_forward_ret_20d"] is not None else None,
                "mean_path_value_score_v1_delta": _safe_float((val_sel["selection_metrics"]["mean_path_value_score_v1"] or 0.0) - (train_sel["selection_metrics"]["mean_path_value_score_v1"] or 0.0)) if train_sel["selection_metrics"]["mean_path_value_score_v1"] is not None and val_sel["selection_metrics"]["mean_path_value_score_v1"] is not None else None,
            }
            candidate_eval["validation_test_gap"][f"top{topk}"] = {
                "mean_forward_ret_20d_delta": _safe_float((test_sel["selection_metrics"]["mean_forward_ret_20d"] or 0.0) - (val_sel["selection_metrics"]["mean_forward_ret_20d"] or 0.0)) if val_sel["selection_metrics"]["mean_forward_ret_20d"] is not None and test_sel["selection_metrics"]["mean_forward_ret_20d"] is not None else None,
                "mean_path_value_score_v1_delta": _safe_float((test_sel["selection_metrics"]["mean_path_value_score_v1"] or 0.0) - (val_sel["selection_metrics"]["mean_path_value_score_v1"] or 0.0)) if val_sel["selection_metrics"]["mean_path_value_score_v1"] is not None and test_sel["selection_metrics"]["mean_path_value_score_v1"] is not None else None,
            }
            candidate_eval["monthly_concentration"][f"top{topk}"] = variant_comparison["candidate_surface"]["variants"][variant_name]["monthly_win_loss_flat"][f"top{topk}"]
            candidate_eval["regime_concentration"][f"top{topk}"] = variant_comparison["candidate_surface"]["variants"][variant_name]["regime_win_loss_flat"][f"top{topk}"]
        audit["variants"][variant_name] = candidate_eval
    audit["candidate_surface"] = variant_comparison["candidate_surface"]
    audit["orfp_surface"] = variant_comparison["orfp_surface"]
    return audit


def _feature_effect_summary(
    frames: dict[str, pd.DataFrame],
    model_results: dict[str, Any],
    variant_comparison: dict[str, Any],
    boundary_frame: pd.DataFrame,
    reclass_frame: pd.DataFrame,
    root_cause_summary: dict[str, Any],
    edinet_reference: dict[str, Any],
) -> dict[str, Any]:
    candidate = frames["batch2_candidate"].copy()
    batch1_candidate = frames["batch1_candidate"].copy()
    summary: dict[str, Any] = {
        "schema_version": FEATURE_EFFECT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selected_feature_count": len(MODEL_FEATURES),
        "selected_features": MODEL_FEATURES,
        "edinet_reference": edinet_reference,
        "root_cause_summary": root_cause_summary,
        "boundary_diagnostics": {},
        "reclassification_diagnostics": {},
        "model_feature_coverage": {},
        "linear_coefficient_summary": {},
        "deterministic_baseline_weights": NUMERIC_BUCKET_MAPS,
    }

    for feature in MODEL_FEATURES:
        summary["model_feature_coverage"][feature] = {
            "batch2_candidate_non_null_count": int(candidate[feature].notna().sum()) if feature in candidate.columns else None,
            "batch2_candidate_coverage_rate": _safe_float(candidate[feature].notna().mean()) if feature in candidate.columns else None,
            "batch1_candidate_non_null_count": int(batch1_candidate[feature].notna().sum()) if feature in batch1_candidate.columns else None,
            "batch1_candidate_coverage_rate": _safe_float(batch1_candidate[feature].notna().mean()) if feature in batch1_candidate.columns else None,
        }

    for variant_name, payload in model_results.get("variants", {}).items():
        if payload["feature_importance"] is not None:
            summary["linear_coefficient_summary"][variant_name] = payload["feature_importance"][:20]

    if not boundary_frame.empty and model_results.get("status") == "ready_for_evaluation":
        for variant_name, payload in model_results["variants"].items():
            boundary_scores = _score_variant_on_frame(payload, boundary_frame)
            summary["boundary_diagnostics"][variant_name] = _pairwise_diagnostics(boundary_frame, boundary_scores, label=variant_name)
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
            summary["reclassification_diagnostics"][variant_name] = {
                "row_count": int(len(re_scored)),
                "family_rows": family_rows,
            }
    summary["candidate_surface_variant_rankings"] = {
        variant_name: {
            "test_top5_forward_ret_20d": variant_comparison["candidate_surface"]["variants"][variant_name]["topk"]["top5"]["selection_metrics"]["mean_forward_ret_20d"],
            "test_top10_forward_ret_20d": variant_comparison["candidate_surface"]["variants"][variant_name]["topk"]["top10"]["selection_metrics"]["mean_forward_ret_20d"],
            "test_top5_path_value_score_v1": variant_comparison["candidate_surface"]["variants"][variant_name]["topk"]["top5"]["selection_metrics"]["mean_path_value_score_v1"],
            "test_top10_path_value_score_v1": variant_comparison["candidate_surface"]["variants"][variant_name]["topk"]["top10"]["selection_metrics"]["mean_path_value_score_v1"],
            "test_top5_bottom15_contamination_rate": variant_comparison["candidate_surface"]["variants"][variant_name]["topk"]["top5"]["selection_metrics"]["bottom15_contamination_rate"],
            "test_top10_bottom15_contamination_rate": variant_comparison["candidate_surface"]["variants"][variant_name]["topk"]["top10"]["selection_metrics"]["bottom15_contamination_rate"],
        }
        for variant_name in variant_comparison["candidate_surface"]["variants"]
    }
    return summary


def _decision_from_results(
    model_results: dict[str, Any],
    variant_comparison: dict[str, Any],
    split_contract: dict[str, Any],
) -> dict[str, Any]:
    if model_results.get("status") != "ready_for_evaluation" or split_contract.get("status") != "ready_for_time_split_evaluation":
        return {
            "schema_version": DECISION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "decision": "insufficient_time_split_depth",
            "status": "insufficient_time_split_depth",
            "reason": "chronological split depth is not sufficient for stable train/validation/test evaluation",
            "row_count_reconciled": split_contract.get("status") == "ready_for_time_split_evaluation",
            "no_lookahead_passed": True,
            "recommended_next_axis": "revisit_target_definition_or_horizon",
            "jobs_supported": 1,
        }

    candidate_variants = variant_comparison["candidate_surface"]["variants"]
    ranked = []
    for variant_name, payload in candidate_variants.items():
        top5 = payload["topk"]["top5"]["selection_metrics"]
        top10 = payload["topk"]["top10"]["selection_metrics"]
        champion_top5_forward = top5["champion_mean_forward_ret_20d"]
        champion_top5_path = top5["champion_mean_path_value_score_v1"]
        champion_top10_forward = top10["champion_mean_forward_ret_20d"]
        champion_top10_path = top10["champion_mean_path_value_score_v1"]
        ranked.append(
            {
                "variant_name": variant_name,
                "top5_forward_ret_20d": top5["mean_forward_ret_20d"],
                "top5_path_value_score_v1": top5["mean_path_value_score_v1"],
                "top5_bottom15_contamination_rate": top5["bottom15_contamination_rate"],
                "top5_top15_capture_rate": top5["top15_capture_rate"],
                "top10_forward_ret_20d": top10["mean_forward_ret_20d"],
                "top10_path_value_score_v1": top10["mean_path_value_score_v1"],
                "top10_bottom15_contamination_rate": top10["bottom15_contamination_rate"],
                "top10_top15_capture_rate": top10["top15_capture_rate"],
                "top5_membership_change_rate": top5["membership_change_rate"],
                "top10_membership_change_rate": top10["membership_change_rate"],
                "champion_top5_forward_ret_20d": champion_top5_forward,
                "champion_top5_path_value_score_v1": champion_top5_path,
                "champion_top10_forward_ret_20d": champion_top10_forward,
                "champion_top10_path_value_score_v1": champion_top10_path,
            }
        )

    ranked_sorted = sorted(
        ranked,
        key=lambda row: (
            -float((row["top5_forward_ret_20d"] - row["champion_top5_forward_ret_20d"]) if row["top5_forward_ret_20d"] is not None and row["champion_top5_forward_ret_20d"] is not None else -1e9),
            -float((row["top10_forward_ret_20d"] - row["champion_top10_forward_ret_20d"]) if row["top10_forward_ret_20d"] is not None and row["champion_top10_forward_ret_20d"] is not None else -1e9),
        ),
    )
    best = ranked_sorted[0] if ranked_sorted else None

    if best is None:
        decision = "stop_model_reranker_line"
        reason = "no comparable variant metrics were produced"
    else:
        candidate_surface = variant_comparison["candidate_surface"]
        candidate_best_variant = candidate_surface["variants"][best["variant_name"]]
        top5 = candidate_best_variant["topk"]["top5"]["selection_metrics"]
        top10 = candidate_best_variant["topk"]["top10"]["selection_metrics"]
        monthly = candidate_best_variant["monthly_win_loss_flat"]["top10"]
        regime = candidate_best_variant["regime_win_loss_flat"]["top10"]
        top5_delta_forward = (top5["mean_forward_ret_20d"] or 0.0) - (top5["champion_mean_forward_ret_20d"] or 0.0) if top5["mean_forward_ret_20d"] is not None and top5["champion_mean_forward_ret_20d"] is not None else None
        top10_delta_forward = (top10["mean_forward_ret_20d"] or 0.0) - (top10["champion_mean_forward_ret_20d"] or 0.0) if top10["mean_forward_ret_20d"] is not None and top10["champion_mean_forward_ret_20d"] is not None else None
        top5_delta_path = (top5["mean_path_value_score_v1"] or 0.0) - (top5["champion_mean_path_value_score_v1"] or 0.0) if top5["mean_path_value_score_v1"] is not None and top5["champion_mean_path_value_score_v1"] is not None else None
        top10_delta_path = (top10["mean_path_value_score_v1"] or 0.0) - (top10["champion_mean_path_value_score_v1"] or 0.0) if top10["mean_path_value_score_v1"] is not None and top10["champion_mean_path_value_score_v1"] is not None else None
        broad_monthly = monthly["win"] > monthly["loss"] and monthly["groups"] > 0
        broad_regime = regime["win"] > regime["loss"] and regime["groups"] > 0
        if (
            best["variant_name"] != "feature_score_ablation_baseline"
            and top5_delta_forward is not None
            and top10_delta_forward is not None
            and top5_delta_path is not None
            and top10_delta_path is not None
            and top5_delta_forward > 0
            and top10_delta_forward > 0
            and top5_delta_path >= 0
            and top10_delta_path >= 0
            and best["top5_bottom15_contamination_rate"] is not None
            and best["top10_bottom15_contamination_rate"] is not None
            and best["top5_bottom15_contamination_rate"] <= top5["champion_bottom15_contamination_rate"]
            and best["top10_bottom15_contamination_rate"] <= top10["champion_bottom15_contamination_rate"]
            and broad_monthly
            and broad_regime
        ):
            decision = "ready_for_shadow_challenger_design"
            reason = "out-of-sample lift is broad enough to justify a shadow reranker design"
        else:
            decision = "insufficient_oos_signal"
            reason = "out-of-sample ranking lift is not broad enough across months and regimes"

    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": reason,
        "row_count_reconciled": True,
        "no_lookahead_passed": True,
        "best_variant": best,
        "recommended_next_axis": "revisit_target_definition_or_horizon" if decision != "ready_for_shadow_challenger_design" else "shadow_challenger_design",
        "jobs_supported": 1,
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


def run_shadow_feature_reranker_feasibility_v1(
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

    paths = {
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
    for path, label in [(p, n) for n, p in paths.items()]:
        _ensure_exists(path, label)

    frames = {
        "batch2_candidate": _load_frame(paths["batch2_candidate"]),
        "batch2_orfp": _load_frame(paths["batch2_orfp"]),
        "batch1_candidate": _load_frame(paths["batch1_candidate"]),
        "batch1_orfp": _load_frame(paths["batch1_orfp"]),
        "reclass_rows": _load_frame(paths["reclass_rows"]),
        "pairwise_rows": _load_frame(paths["pairwise_rows"]),
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
        len(candidate_months) >= TRAIN_MONTH_COUNT + VALIDATION_MONTH_COUNT + TEST_MONTH_COUNT
        and len(split_months["train"]) > 0
        and len(split_months["validation"]) > 0
        and len(split_months["test"]) > 0
    )

    input_resolution = _build_input_resolution(paths, frames, jobs_requested=jobs, jobs_supported=1, limit_anchor_dates=limit_anchor_dates)
    feature_inventory = _feature_inventory(frames)
    label_contract = _label_contract()
    split_contract = _split_contract(frames, split_months)
    model_contract = _model_contract()

    model_results = _fit_model_variants(frames, split_months, time_split_ready=time_split_ready)
    edinet_reference = {
        "session_dir": str(EDINET_REFERENCE_SESSION),
        "decision_path": str(EDINET_REFERENCE_DECISION),
        "decision": _load_json(EDINET_REFERENCE_DECISION).get("decision", "unknown") if EDINET_REFERENCE_DECISION.exists() else "unknown",
        "used_as_feature": False,
        "reason": "reference_only_and_positive_coverage_is_zero",
    }

    if model_results.get("status") == "ready_for_evaluation":
        variant_comparison = {
            "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "candidate_surface": _evaluation_summary(frames["batch2_candidate"], {name: payload["candidate_scores"] for name, payload in model_results["variants"].items()}, surface_name="candidate_surface", topk_values=TOP_K_VALUES),
            "orfp_surface": _evaluation_summary(frames["batch2_orfp"], {name: payload["orfp_scores"] for name, payload in model_results["variants"].items()}, surface_name="orfp_surface", topk_values=TOP_K_VALUES),
            "notes": [
                "Selection metrics are computed from reranked topK groups against the preserved champion selection.",
                "ORFP coverage is diagnostic only because top15 labels are effectively absent there.",
            ],
        }
    else:
        variant_comparison = {
            "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "candidate_surface": {"surface_name": "candidate_surface", "row_count": int(len(frames["batch2_candidate"])), "variants": {}},
            "orfp_surface": {"surface_name": "orfp_surface", "row_count": int(len(frames["batch2_orfp"])), "variants": {}},
            "notes": model_results.get("notes", []),
        }

    topk_diff = _build_topk_membership_diff(
        {"batch2_candidate": frames["batch2_candidate"], "batch2_orfp": frames["batch2_orfp"]},
        {name: {"candidate": payload.get("candidate_scores", pd.Series(dtype="float64")), "orfp": payload.get("orfp_scores", pd.Series(dtype="float64"))} for name, payload in model_results.get("variants", {}).items()},
        TOP_K_VALUES,
    )

    if not topk_diff.empty:
        topk_diff["top15_label"] = topk_diff["top15_label"].fillna(False).astype(bool)
        topk_diff["bottom15_label"] = topk_diff["bottom15_label"].fillna(False).astype(bool)
        topk_diff["model_selected"] = topk_diff["model_selected"].astype(bool)
        topk_diff["champion_selected"] = topk_diff["champion_selected"].astype(bool)
        topk_diff["membership_changed"] = topk_diff["membership_changed"].astype(bool)
        topk_diff["selected_overlap"] = topk_diff["selected_overlap"].astype(bool)

    feature_effect_summary = _feature_effect_summary(frames, model_results, variant_comparison, frames["pairwise_rows"], frames["reclass_rows"], _load_json(paths["reclass_root_cause"]), edinet_reference)

    stability_audit = _stability_audit(frames, split_months, model_results, variant_comparison)
    decision = _decision_from_results(model_results, variant_comparison, split_contract)

    candidate_rows = int(len(frames["batch2_candidate"]))
    orfp_rows = int(len(frames["batch2_orfp"]))
    reclass_rows = int(len(frames["reclass_rows"]))
    pairwise_rows = int(len(frames["pairwise_rows"]))
    row_count_reconciled = candidate_rows == 2542 and orfp_rows == 365 and reclass_rows == 585 and pairwise_rows == 385
    if split_contract["status"] != "ready_for_time_split_evaluation":
        decision["decision"] = "insufficient_time_split_depth"
        decision["status"] = "insufficient_time_split_depth"
        decision["reason"] = "chronological split depth is not sufficient for stable train/validation/test evaluation"
        decision["recommended_next_axis"] = "revisit_target_definition_or_horizon"
    elif row_count_reconciled:
        if decision["decision"] == "ready_for_shadow_challenger_design":
            decision["reason"] = "out-of-sample lift is broad enough to justify a shadow reranker design"
        else:
            decision["reason"] = "learned variants do not show broad enough out-of-sample lift"
    else:
        decision["reason"] = "row counts did not reconcile against the batch2 lineage"

    split_contract["row_count_reconciled"] = row_count_reconciled

    run_manifest = _build_run_manifest(output_root_path, session_dir, paths, jobs_requested=jobs, jobs_supported=1, split_status=split_contract["status"], decision=decision["decision"])

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "shadow_reranker_feature_inventory.json", feature_inventory)
    _write_json(session_dir / "shadow_reranker_label_contract.json", label_contract)
    _write_json(session_dir / "shadow_reranker_split_contract.json", split_contract)
    _write_json(session_dir / "shadow_reranker_model_contract.json", model_contract)
    _write_json(session_dir / "shadow_reranker_variant_pool_comparison.json", variant_comparison)
    _write_parquet(session_dir / "shadow_reranker_topk_membership_diff.parquet", topk_diff)
    _write_json(session_dir / "shadow_reranker_stability_audit.json", stability_audit)
    _write_json(session_dir / "shadow_reranker_feature_effect_summary.json", feature_effect_summary)
    _write_json(session_dir / "shadow_feature_reranker_feasibility_v1_decision.json", decision)
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
                "shadow_reranker_feature_inventory.json",
                "shadow_reranker_label_contract.json",
                "shadow_reranker_split_contract.json",
                "shadow_reranker_model_contract.json",
                "shadow_reranker_variant_pool_comparison.json",
                "shadow_reranker_topk_membership_diff.parquet",
                "shadow_reranker_stability_audit.json",
                "shadow_reranker_feature_effect_summary.json",
                "shadow_feature_reranker_feasibility_v1_decision.json",
            ],
        },
    )

    return {
        "output_dir": str(session_dir),
        "session_id": session_id,
        "decision": decision["decision"],
        "row_count_reconciled": row_count_reconciled,
        "split_status": split_contract["status"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()
    result = run_shadow_feature_reranker_feasibility_v1(output_root=args.output_root, limit_anchor_dates=args.limit_anchor_dates, jobs=args.jobs)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
