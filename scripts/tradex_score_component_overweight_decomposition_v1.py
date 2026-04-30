from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import (  # noqa: E402
    get_rankings_freshness,
    get_runtime_stock_db_status,
)
from scripts.tradex_bad_pick_root_cause_audit_v1 import (  # noqa: E402
    _add_outcome_labels,
    _load_policy_feature_overlay,
)
from scripts.tradex_ma_state_family_high_value_boost_v1 import (  # noqa: E402
    _load_json,
    _make_session_id,
    _safe_float,
    _safe_int,
    _write_json,
)

DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_AUDIT_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
DEFAULT_BOUNDARY_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
DEFAULT_POLICY_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\score_component_overweight_decomposition_v1")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1"
MANIFEST_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_input_resolution_v1"
INVENTORY_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_score_component_inventory_v1"
COHORT_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_score_overweight_cohort_summary_v1"
CONTRAST_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_score_component_contrast_summary_v1"
PAIRWISE_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_score_component_pairwise_boundary_summary_v1"
REGIME_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_score_component_regime_breakdown_v1"
FAILURE_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_score_component_failure_patterns_v1"
HYPOTHESIS_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_score_component_challenger_hypotheses_v1"
DECISION_SCHEMA_VERSION = "tradex_score_component_overweight_decomposition_v1_decision_v1"

TOP_K_VALUES = (5, 10, 20)
BAD_PICK_ROOT_CAUSE = "score_component_overweight"
BAD_OUTCOME_THRESHOLD = 0.0

TOTAL_SCORE_FIELDS = ["score", "candidate_score", "champion_score", "challenger_score"]
RANK_FIELDS = ["candidate_rank", "champion_rank", "challenger_rank", "rank"]
CONTEXT_FIELDS = [
    "monthly_context",
    "weekly_context",
    "daily_main_state_ctx",
    "monthly_main_state_ctx",
    "weekly_main_state_ctx",
    "dominant_regime_context",
    "market_regime_bucket",
    "family_classification",
    "shape_classification",
    "candle_shape_modifier",
    "conditional_high_value",
]
NUMERIC_FEATURE_FIELDS = [
    "score",
    "candidate_score",
    "champion_score",
    "challenger_score",
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "path_value_score_v1",
    "mfe_20d",
    "mae_20d",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "gap_pct",
    "vol_ratio5_20",
    "liquidity20d",
    "candle_body_ratio",
    "candle_upper_wick_ratio",
    "candle_lower_wick_ratio",
    "candle_triplet_up_prob",
    "candle_triplet_down_prob",
    "family_sample_count",
    "family_unique_symbol_count",
    "family_month_count",
    "family_mean_forward_ret_20d",
    "family_median_forward_ret_20d",
    "family_mean_path_value_score_v1",
    "family_median_path_value_score_v1",
    "family_plus5_before_minus5_rate",
    "family_minus5_before_plus5_rate",
    "family_top15_rate",
    "family_bottom15_rate",
    "family_positive_month_rate",
    "family_worst_month_mean_path_value",
    "family_best_month_mean_path_value",
]

FEATURE_GROUPS: dict[str, list[str]] = {
    "total_score": ["score", "candidate_score", "champion_score", "challenger_score"],
    "rank_fields": ["candidate_rank", "champion_rank", "challenger_rank", "rank"],
    "path_quality_proxy": ["forward_ret_20d", "path_value_score_v1", "mfe_20d", "mae_20d"],
    "ma_extension_proxy": ["dist_ma20_pct", "dist_ma60_pct"],
    "candle_geometry_proxy": [
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "gap_pct",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
    ],
    "liquidity_proxy": ["vol_ratio5_20", "liquidity20d"],
    "family_quality_proxy": [
        "family_sample_count",
        "family_unique_symbol_count",
        "family_month_count",
        "family_mean_forward_ret_20d",
        "family_median_forward_ret_20d",
        "family_mean_path_value_score_v1",
        "family_median_path_value_score_v1",
        "family_plus5_before_minus5_rate",
        "family_minus5_before_plus5_rate",
        "family_top15_rate",
        "family_bottom15_rate",
        "family_positive_month_rate",
        "family_worst_month_mean_path_value",
        "family_best_month_mean_path_value",
    ],
    "context_proxy": [
        "monthly_context",
        "weekly_context",
        "monthly_main_state_ctx",
        "weekly_main_state_ctx",
        "daily_main_state_ctx",
        "dominant_regime_context",
        "market_regime_bucket",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "conditional_high_value",
    ],
}

MISSING_INTERNAL_COMPONENT_FIELDS = [
    "retrieval_score",
    "ranking_score",
    "risk_penalty",
    "regime_adjustment",
    "reason_codes",
]

MISSING_EVENT_FIELDS = [
    "event_flag",
    "earnings_flag",
    "dividend_flag",
    "rights_flag",
    "ex_rights_flag",
    "volume_atr_ratio",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return _safe_path(output_root, DEFAULT_OUTPUT_ROOT)


def _resolve_source_path(value: str | Path | None, default: Path, label: str) -> Path:
    path = _safe_path(value, default)
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


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


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    for column in ("anchor_date", "symbol", "side", "month_bucket", "trade_date"):
        if column in frame.columns:
            frame[column] = frame[column].astype(str)
    for column in RANK_FIELDS + ["candidate_idx"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    for column in NUMERIC_FEATURE_FIELDS:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in [
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "top15_label",
        "bottom15_label",
        "conditional_high_value",
        "shape_joined",
        "stable_high_value_family",
        "stable_bad_pick_family",
        "regime_dependent_family",
        "unstable_or_sparse_family",
        "neutral_family",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "is_top15_outcome",
        "is_bottom15_outcome",
        "is_materially_negative",
        "is_bad_pick",
        "is_good_pick",
        "is_neutral_pick",
    ]:
        if column in frame.columns:
            frame[column] = frame[column].fillna(False).astype(bool)
    for column in [
        "daily_main_state_ctx",
        "weekly_main_state_ctx",
        "monthly_main_state_ctx",
        "monthly_context",
        "weekly_context",
        "market_regime_bucket",
        "dominant_regime_context",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "prefilter_bucket",
        "selected_by",
        "selected_by_methods",
        "selection_reason",
        "bad_pick_scope",
        "root_cause_code",
        "root_cause_confidence",
        "root_cause_notes",
    ]:
        if column in frame.columns:
            frame[column] = frame[column].astype(object)
    return frame


def _load_selected_surface(
    source_rows_parquet: Path,
    audit_cases_path: Path,
    boundary_path: Path,
    policy_ledger_path: Path,
    limit_anchor_dates: int | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source = _normalize_frame(pd.read_parquet(source_rows_parquet))
    if "champion_selected_top20" not in source.columns:
        raise RuntimeError("candidate surface missing champion_selected_top20")
    selected = source.loc[source["champion_selected_top20"].fillna(False).astype(bool)].copy()
    if limit_anchor_dates and limit_anchor_dates > 0:
        anchors = sorted(selected["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        selected = selected.loc[selected["anchor_date"].isin(anchors)].copy()
    selected_keys = {
        (str(row.anchor_date), str(row.symbol), str(row.side))
        for row in selected[["anchor_date", "symbol", "side"]].itertuples(index=False)
    }
    overlay = _load_policy_feature_overlay(policy_ledger_path, selected_keys)
    if not overlay.empty:
        selected = selected.merge(overlay, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_overlay"))

    audit = _normalize_frame(pd.read_parquet(audit_cases_path))
    audit_cols = [
        "anchor_date",
        "symbol",
        "side",
        "root_cause_code",
        "root_cause_confidence",
        "evidence_fields_used",
        "root_cause_notes",
        "missing_fields",
        "bad_pick_scope",
        "topk_bucket",
        "top15_label",
        "bottom15_label",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "monthly_context",
        "weekly_context",
        "daily_main_state_ctx",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "dominant_regime_context",
        "market_regime_bucket",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "shape_joined",
        "conditional_high_value",
        "family_sample_count",
        "family_unique_symbol_count",
        "family_month_count",
        "family_mean_forward_ret_20d",
        "family_median_forward_ret_20d",
        "family_mean_path_value_score_v1",
        "family_median_path_value_score_v1",
        "family_plus5_before_minus5_rate",
        "family_minus5_before_plus5_rate",
        "family_top15_rate",
        "family_bottom15_rate",
        "family_positive_month_rate",
    ]
    selected = selected.merge(
        audit[[col for col in audit_cols if col in audit.columns]],
        on=["anchor_date", "symbol", "side"],
        how="left",
        suffixes=("", "_audit"),
    )

    boundary = _normalize_frame(pd.read_parquet(boundary_path))
    boundary_cols = [
        "anchor_date",
        "symbol",
        "side",
        "boundary_candidate_count",
        "boundary_rank_range",
        "boundary_mean_forward_ret_20d",
        "boundary_median_forward_ret_20d",
        "boundary_mean_path_value_score_v1",
        "boundary_median_path_value_score_v1",
        "best_near_miss_rank",
        "best_near_miss_symbol",
        "best_near_miss_score",
        "best_near_miss_forward_ret_20d",
        "best_near_miss_path_value_score_v1",
        "best_near_miss_shape_classification",
        "score_gap",
        "forward_ret_20d_gap",
        "path_value_gap",
        "rank_gap",
        "boundary_candidate_ranks",
    ]
    selected = selected.merge(
        boundary[[col for col in boundary_cols if col in boundary.columns]],
        on=["anchor_date", "symbol", "side"],
        how="left",
        suffixes=("", "_boundary"),
    )

    selected = _normalize_frame(selected)
    selected = _add_outcome_labels(selected)
    source_info = {
        "source_rows_parquet": str(source_rows_parquet),
        "audit_cases_parquet": str(audit_cases_path),
        "boundary_parquet": str(boundary_path),
        "policy_trade_ledger": str(policy_ledger_path),
        "selected_row_count": int(len(selected)),
        "selected_anchor_count": int(selected["anchor_date"].nunique()) if "anchor_date" in selected.columns else None,
    }
    return selected, source_info


def _group_topk_mask(frame: pd.DataFrame, top_k: int) -> pd.Series:
    return frame[f"champion_selected_top{top_k}"].fillna(False).astype(bool)


def _group_rate(mask: pd.Series, subset: pd.Series) -> float | None:
    denom = int(mask.sum())
    if denom == 0:
        return None
    return float((mask & subset).sum() / denom)


def _safe_mean(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        return None
    return _safe_float(series.mean())


def _safe_median(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns:
        return None
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        return None
    return _safe_float(series.median())


def _safe_mode(frame: pd.DataFrame, column: str) -> tuple[str | None, float | None]:
    if column not in frame.columns:
        return None, None
    series = frame[column].fillna("missing").astype(str)
    if series.empty:
        return None, None
    counts = series.value_counts(dropna=False)
    if counts.empty:
        return None, None
    mode = str(counts.index[0])
    rate = float(counts.iloc[0] / len(series))
    return mode, rate


def _percentile_rank_of_mean(series: pd.Series, value: float | None) -> float | None:
    if value is None:
        return None
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    if cleaned.empty:
        return None
    return float((cleaned <= value).mean())


def _feature_classification(
    *,
    bad_mean: float | None,
    good_mean: float | None,
    near_miss_mean: float | None,
    bad_median: float | None,
    good_median: float | None,
    near_miss_median: float | None,
    bad_missing_rate: float | None,
    good_missing_rate: float | None,
    near_miss_missing_rate: float | None,
    min_abs_delta: float = 0.01,
) -> str:
    if bad_mean is None or good_mean is None:
        return "insufficient_data"
    delta_good = bad_mean - good_mean
    delta_near = None if near_miss_mean is None else bad_mean - near_miss_mean
    if abs(delta_good) < min_abs_delta and (delta_near is None or abs(delta_near) < min_abs_delta):
        return "non_discriminative"
    if delta_good > min_abs_delta and (delta_near is None or delta_near > 0):
        return "bad_pick_enriched"
    if delta_good < -min_abs_delta and (delta_near is None or delta_near < 0):
        return "good_pick_enriched"
    if (
        bad_missing_rate is not None
        and good_missing_rate is not None
        and near_miss_missing_rate is not None
        and (bad_missing_rate > good_missing_rate + 0.10 or bad_missing_rate > near_miss_missing_rate + 0.10)
    ):
        return "unstable"
    return "mixed_or_context_dependent"


def _numeric_feature_row(
    frame: pd.DataFrame,
    *,
    feature: str,
    bad: pd.DataFrame,
    good: pd.DataFrame,
    neutral: pd.DataFrame,
    near_miss: pd.DataFrame,
    all_bad: pd.DataFrame,
    all_good: pd.DataFrame,
    topk: str,
) -> dict[str, Any]:
    bad_series = pd.to_numeric(bad[feature], errors="coerce") if feature in bad.columns else pd.Series(dtype=float)
    good_series = pd.to_numeric(good[feature], errors="coerce") if feature in good.columns else pd.Series(dtype=float)
    neutral_series = pd.to_numeric(neutral[feature], errors="coerce") if feature in neutral.columns else pd.Series(dtype=float)
    near_series = pd.to_numeric(near_miss[feature], errors="coerce") if feature in near_miss.columns else pd.Series(dtype=float)
    all_bad_series = pd.to_numeric(all_bad[feature], errors="coerce") if feature in all_bad.columns else pd.Series(dtype=float)
    all_good_series = pd.to_numeric(all_good[feature], errors="coerce") if feature in all_good.columns else pd.Series(dtype=float)
    bad_mean = _safe_float(bad_series.mean()) if not bad_series.dropna().empty else None
    bad_median = _safe_float(bad_series.median()) if not bad_series.dropna().empty else None
    good_mean = _safe_float(good_series.mean()) if not good_series.dropna().empty else None
    good_median = _safe_float(good_series.median()) if not good_series.dropna().empty else None
    neutral_mean = _safe_float(neutral_series.mean()) if not neutral_series.dropna().empty else None
    neutral_median = _safe_float(neutral_series.median()) if not neutral_series.dropna().empty else None
    near_mean = _safe_float(near_series.mean()) if not near_series.dropna().empty else None
    near_median = _safe_float(near_series.median()) if not near_series.dropna().empty else None
    all_bad_mean = _safe_float(all_bad_series.mean()) if not all_bad_series.dropna().empty else None
    all_bad_median = _safe_float(all_bad_series.median()) if not all_bad_series.dropna().empty else None
    all_good_mean = _safe_float(all_good_series.mean()) if not all_good_series.dropna().empty else None
    all_good_median = _safe_float(all_good_series.median()) if not all_good_series.dropna().empty else None
    bad_missing_rate = float(bad_series.isna().mean()) if len(bad_series) else None
    good_missing_rate = float(good_series.isna().mean()) if len(good_series) else None
    near_missing_rate = float(near_series.isna().mean()) if len(near_series) else None
    return {
        "topk": topk,
        "feature": feature,
        "availability": "available" if feature in frame.columns else "missing",
        "bad_mean": bad_mean,
        "bad_median": bad_median,
        "good_mean": good_mean,
        "good_median": good_median,
        "neutral_mean": neutral_mean,
        "neutral_median": neutral_median,
        "near_miss_mean": near_mean,
        "near_miss_median": near_median,
        "all_bad_mean": all_bad_mean,
        "all_bad_median": all_bad_median,
        "all_good_mean": all_good_mean,
        "all_good_median": all_good_median,
        "bad_missing_rate": bad_missing_rate,
        "good_missing_rate": good_missing_rate,
        "neutral_missing_rate": float(neutral_series.isna().mean()) if len(neutral_series) else None,
        "near_miss_missing_rate": near_missing_rate,
        "bad_percentile_rank": _percentile_rank_of_mean(
            pd.concat([bad_series, good_series, neutral_series, near_series, all_bad_series, all_good_series], ignore_index=True),
            bad_mean,
        ),
        "delta_mean_bad_minus_good": None if bad_mean is None or good_mean is None else _safe_float(bad_mean - good_mean),
        "delta_mean_bad_minus_near_miss": None if bad_mean is None or near_mean is None else _safe_float(bad_mean - near_mean),
        "delta_median_bad_minus_good": None if bad_median is None or good_median is None else _safe_float(bad_median - good_median),
        "delta_median_bad_minus_near_miss": None if bad_median is None or near_median is None else _safe_float(bad_median - near_median),
        "classification": _feature_classification(
            bad_mean=bad_mean,
            good_mean=good_mean,
            near_miss_mean=near_mean,
            bad_median=bad_median,
            good_median=good_median,
            near_miss_median=near_median,
            bad_missing_rate=bad_missing_rate,
            good_missing_rate=good_missing_rate,
            near_miss_missing_rate=near_missing_rate,
        ),
    }


def _categorical_feature_row(
    *,
    feature: str,
    bad: pd.DataFrame,
    good: pd.DataFrame,
    neutral: pd.DataFrame,
    near_miss: pd.DataFrame,
    topk: str,
) -> dict[str, Any]:
    def _mode_and_rate(frame: pd.DataFrame) -> tuple[str | None, float | None]:
        if feature not in frame.columns or len(frame) == 0:
            return None, None
        series = frame[feature].fillna("missing").astype(str)
        if series.empty:
            return None, None
        counts = series.value_counts(dropna=False)
        if counts.empty:
            return None, None
        mode = str(counts.index[0])
        rate = float(counts.iloc[0] / len(series))
        return mode, rate

    bad_mode, bad_rate = _mode_and_rate(bad)
    good_mode, good_rate = _mode_and_rate(good)
    neutral_mode, neutral_rate = _mode_and_rate(neutral)
    near_mode, near_rate = _mode_and_rate(near_miss)
    if bad_mode is None or good_mode is None:
        classification = "insufficient_data"
    elif bad_rate is not None and good_rate is not None and bad_mode != good_mode and bad_rate > good_rate:
        classification = "bad_pick_enriched"
    elif bad_rate is not None and good_rate is not None and bad_mode == good_mode and abs(bad_rate - good_rate) < 0.05:
        classification = "non_discriminative"
    elif bad_rate is not None and good_rate is not None and bad_rate < good_rate:
        classification = "good_pick_enriched"
    else:
        classification = "mixed_or_context_dependent"
    return {
        "topk": topk,
        "feature": feature,
        "availability": "available" if feature in bad.columns or feature in good.columns or feature in neutral.columns or feature in near_miss.columns else "missing",
        "bad_mode": bad_mode,
        "bad_mode_rate": bad_rate,
        "good_mode": good_mode,
        "good_mode_rate": good_rate,
        "neutral_mode": neutral_mode,
        "neutral_mode_rate": neutral_rate,
        "near_miss_mode": near_mode,
        "near_miss_mode_rate": near_rate,
        "delta_mode_rate_bad_minus_good": None if bad_rate is None or good_rate is None else _safe_float(bad_rate - good_rate),
        "delta_mode_rate_bad_minus_near_miss": None if bad_rate is None or near_rate is None else _safe_float(bad_rate - near_rate),
        "classification": classification,
    }


def _bucket_liquidity(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    if value < 0.8:
        return "low"
    if value < 1.2:
        return "mid"
    return "high"


def _bucket_volatility(dist_ma20: float | None, dist_ma60: float | None, vol_ratio: float | None) -> str:
    candidates = [value for value in [dist_ma20, dist_ma60, vol_ratio] if value is not None and not pd.isna(value)]
    if not candidates:
        return "unknown"
    score = max(abs(float(v)) for v in candidates)
    if score < 0.02:
        return "low"
    if score < 0.05:
        return "mid"
    return "high"


def _boundary_pair_rows(selected: pd.DataFrame, score_overweight: pd.DataFrame) -> pd.DataFrame:
    if score_overweight.empty:
        return score_overweight.iloc[0:0].copy()
    selected_lookup = selected.copy()
    pair_rows: list[dict[str, Any]] = []
    feature_copy_fields = [
        "score",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "gap_pct",
        "vol_ratio5_20",
        "liquidity20d",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "monthly_context",
        "weekly_context",
        "daily_main_state_ctx",
        "monthly_main_state_ctx",
        "weekly_main_state_ctx",
        "dominant_regime_context",
        "market_regime_bucket",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "family_mean_path_value_score_v1",
        "family_median_path_value_score_v1",
        "family_top15_rate",
        "family_bottom15_rate",
        "family_positive_month_rate",
    ]
    for _, row in score_overweight.iterrows():
        selected_dict = row.to_dict()
        near_miss = pd.DataFrame()
        if pd.notna(row.get("best_near_miss_symbol")) and pd.notna(row.get("best_near_miss_rank")):
            near_miss = selected_lookup.loc[
                (selected_lookup["anchor_date"] == row["anchor_date"])
                & (selected_lookup["side"] == row["side"])
                & (selected_lookup["symbol"].astype(str) == str(row.get("best_near_miss_symbol")))
                & (selected_lookup["champion_rank"] == row.get("best_near_miss_rank"))
            ].copy()
        near_miss_row = near_miss.iloc[0] if not near_miss.empty else None
        out: dict[str, Any] = {
            "anchor_date": str(row.get("anchor_date")),
            "month_bucket": str(row.get("month_bucket")),
            "side": str(row.get("side")),
            "selected_symbol": str(row.get("symbol")),
            "selected_rank": _safe_int(row.get("champion_rank")),
            "selected_scope": str(row.get("bad_pick_scope") or "unknown"),
            "root_cause_code": str(row.get("root_cause_code") or "unknown"),
            "root_cause_confidence": str(row.get("root_cause_confidence") or "unknown"),
            "selected_is_bad_pick": bool(row.get("is_bad_pick")),
            "selected_is_good_pick": bool(row.get("is_good_pick")),
            "selected_is_neutral_pick": bool(row.get("is_neutral_pick")),
            "near_miss_joined": near_miss_row is not None,
            "best_near_miss_symbol": str(row.get("best_near_miss_symbol") or "unknown"),
            "best_near_miss_rank": _safe_int(row.get("best_near_miss_rank")),
            "score_gap": _safe_float(row.get("score_gap")),
            "forward_ret_20d_gap": _safe_float(row.get("forward_ret_20d_gap")),
            "path_value_gap": _safe_float(row.get("path_value_gap")),
            "rank_gap": _safe_int(row.get("rank_gap")),
        }
        for field in feature_copy_fields:
            out[f"selected_{field}"] = _json_ready(row.get(field))
            out[f"selected_{field}_available"] = field in row.index and not pd.isna(row.get(field))
            if near_miss_row is not None:
                out[f"near_miss_{field}"] = _json_ready(near_miss_row.get(field))
                out[f"near_miss_{field}_available"] = field in near_miss_row.index and not pd.isna(near_miss_row.get(field))
            else:
                out[f"near_miss_{field}"] = None
                out[f"near_miss_{field}_available"] = False
        pair_rows.append(out)
    return pd.DataFrame(pair_rows)


def _build_score_component_inventory(frame: pd.DataFrame) -> dict[str, Any]:
    observed = []
    for group_name, fields in FEATURE_GROUPS.items():
        present = [field for field in fields if field in frame.columns]
        missing = [field for field in fields if field not in frame.columns]
        observed.append(
            {
                "group": group_name,
                "fields": fields,
                "available_fields": present,
                "missing_fields": missing,
                "available_count": len(present),
                "missing_count": len(missing),
                "non_null_counts": {field: int(frame[field].notna().sum()) for field in present},
            }
        )
    field_rows = []
    for field in sorted(set(sum(FEATURE_GROUPS.values(), [])) | set(TOTAL_SCORE_FIELDS) | set(RANK_FIELDS)):
        field_rows.append(
            {
                "field": field,
                "available": field in frame.columns,
                "non_null_count": int(frame[field].notna().sum()) if field in frame.columns else 0,
                "non_null_rate": _safe_float(frame[field].notna().mean()) if field in frame.columns else 0.0,
            }
        )
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "observed_score_related_fields": field_rows,
        "proxy_component_groups": observed,
        "total_score_fields": [field for field in TOTAL_SCORE_FIELDS if field in frame.columns],
        "selection_rank_fields": [field for field in RANK_FIELDS if field in frame.columns],
        "missing_internal_component_fields": MISSING_INTERNAL_COMPONENT_FIELDS,
        "missing_event_fields": MISSING_EVENT_FIELDS,
        "notes": [
            "The audited champion surface does not expose internal candidate_component_scores columns.",
            "This inventory therefore separates confirmed total-score fields from proxy component groups available on the research surface.",
        ],
    }


def _build_score_overweight_cohort_summary(frame: pd.DataFrame, score_overweight: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "schema_version": COHORT_SCHEMA_VERSION,
        "selected_rows_top20": int(len(frame)),
        "bad_pick_count": int(frame["is_bad_pick"].sum()),
        "good_pick_count": int(frame["is_good_pick"].sum()),
        "neutral_pick_count": int(frame["is_neutral_pick"].sum()),
        "score_component_overweight_count": int(len(score_overweight)),
        "score_component_overweight_share_of_bad_picks": _safe_float(len(score_overweight) / max(int(frame["is_bad_pick"].sum()), 1)),
        "top5_selected_count": int(frame["champion_selected_top5"].sum()),
        "top10_selected_count": int(frame["champion_selected_top10"].sum()),
        "top20_selected_count": int(frame["champion_selected_top20"].sum()),
        "top5_score_overweight_count": int((score_overweight["champion_selected_top5"]).sum()) if len(score_overweight) else 0,
        "top10_score_overweight_count": int((score_overweight["champion_selected_top10"]).sum()) if len(score_overweight) else 0,
        "top20_score_overweight_count": int((score_overweight["champion_selected_top20"]).sum()) if len(score_overweight) else 0,
        "top5_bad_pick_rate": _safe_float(((frame["champion_selected_top5"]) & frame["is_bad_pick"]).mean()),
        "top10_bad_pick_rate": _safe_float(((frame["champion_selected_top10"]) & frame["is_bad_pick"]).mean()),
        "top20_bad_pick_rate": _safe_float(((frame["champion_selected_top20"]) & frame["is_bad_pick"]).mean()),
        "top5_top15_capture_rate": _safe_float(((frame["champion_selected_top5"]) & frame["top15_label"]).mean()),
        "top10_top15_capture_rate": _safe_float(((frame["champion_selected_top10"]) & frame["top15_label"]).mean()),
        "top20_top15_capture_rate": _safe_float(((frame["champion_selected_top20"]) & frame["top15_label"]).mean()),
        "top5_bottom15_contamination_rate": _safe_float(((frame["champion_selected_top5"]) & frame["bottom15_label"]).mean()),
        "top10_bottom15_contamination_rate": _safe_float(((frame["champion_selected_top10"]) & frame["bottom15_label"]).mean()),
        "top20_bottom15_contamination_rate": _safe_float(((frame["champion_selected_top20"]) & frame["bottom15_label"]).mean()),
        "bad_pick_bottom15_rate": _safe_float(score_overweight["bottom15_label"].mean()) if len(score_overweight) else None,
        "bad_pick_top15_capture_rate": _safe_float(score_overweight["top15_label"].mean()) if len(score_overweight) else None,
        "bad_pick_scope_counts": score_overweight["bad_pick_scope"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "confidence_distribution": score_overweight["root_cause_confidence"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "side_counts": score_overweight["side"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "month_counts": score_overweight["month_bucket"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "dominant_regime_counts": score_overweight["dominant_regime_context"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "monthly_context_counts": score_overweight["monthly_context"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "weekly_context_counts": score_overweight["weekly_context"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "daily_state_counts": score_overweight["daily_main_state_ctx"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "family_classification_counts": score_overweight["family_classification"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "shape_classification_counts": score_overweight["shape_classification"].value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "liquidity_bucket_counts": score_overweight["vol_ratio5_20"].apply(_bucket_liquidity).value_counts(dropna=False).to_dict() if len(score_overweight) else {},
        "volatility_bucket_counts": [
            _bucket_volatility(row.get("dist_ma20_pct"), row.get("dist_ma60_pct"), row.get("vol_ratio5_20"))
            for _, row in score_overweight.iterrows()
        ],
    }
    summary["volatility_bucket_counts"] = Counter(summary["volatility_bucket_counts"]).most_common()
    summary["monthly_summary"] = _build_monthly_summary(frame, score_overweight)
    summary["context_summary"] = _build_context_summary(frame, score_overweight)
    return summary


def _build_monthly_summary(frame: pd.DataFrame, score_overweight: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": COHORT_SCHEMA_VERSION, "monthly": []}
    if "month_bucket" not in frame.columns:
        return out
    for month, group in frame.groupby("month_bucket", dropna=False):
        bad = group[group["root_cause_code"].eq(BAD_PICK_ROOT_CAUSE)]
        out["monthly"].append(
            {
                "month_bucket": str(month),
                "count": int(len(group)),
                "bad_pick_count": int(len(bad)),
                "bad_pick_rate": _safe_float(len(bad) / len(group)) if len(group) else None,
                "score_component_overweight_count": int((bad["root_cause_code"].eq(BAD_PICK_ROOT_CAUSE)).sum()) if len(bad) else 0,
                "score_component_overweight_rate": _safe_float((bad["root_cause_code"].eq(BAD_PICK_ROOT_CAUSE)).mean()) if len(bad) else 0.0,
                "mean_forward_ret_20d": _safe_float(group["forward_ret_20d"].mean()) if "forward_ret_20d" in group.columns else None,
                "mean_path_value_score_v1": _safe_float(group["path_value_score_v1"].mean()) if "path_value_score_v1" in group.columns else None,
            }
        )
    out["monthly"] = sorted(out["monthly"], key=lambda item: item["month_bucket"])
    return out


def _build_context_summary(frame: pd.DataFrame, score_overweight: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": COHORT_SCHEMA_VERSION}
    for field in ("dominant_regime_context", "market_regime_bucket", "monthly_context", "weekly_context", "daily_main_state_ctx"):
        if field not in frame.columns:
            continue
        groups = []
        for value, group in frame.groupby(frame[field].fillna("unknown").astype(str), dropna=False):
            groups.append(
                {
                    field: str(value),
                    "count": int(len(group)),
                    "bad_pick_count": int(group["is_bad_pick"].sum()),
                    "bad_pick_rate": _safe_float(group["is_bad_pick"].mean()),
                    "score_component_overweight_count": int((group["root_cause_code"].eq(BAD_PICK_ROOT_CAUSE)).sum()) if "root_cause_code" in group.columns else 0,
                    "mean_forward_ret_20d": _safe_float(group["forward_ret_20d"].mean()) if "forward_ret_20d" in group.columns else None,
                    "mean_path_value_score_v1": _safe_float(group["path_value_score_v1"].mean()) if "path_value_score_v1" in group.columns else None,
                }
            )
        out[field] = sorted(groups, key=lambda item: item["bad_pick_rate"] if item["bad_pick_rate"] is not None else -1, reverse=True)
    return out


def _build_feature_contrast_summary(
    frame: pd.DataFrame,
    score_overweight: pd.DataFrame,
    all_bad: pd.DataFrame,
    all_good: pd.DataFrame,
) -> dict[str, Any]:
    top5_mask = _group_topk_mask(frame, 5)
    top10_mask = _group_topk_mask(frame, 10)

    def _cohort_for(mask: pd.Series, topk_top5: bool, topk_top10: bool) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        selected = frame.loc[mask].copy()
        bad = selected.loc[selected["root_cause_code"].eq(BAD_PICK_ROOT_CAUSE)].copy() if "root_cause_code" in selected.columns else selected.iloc[0:0].copy()
        good = selected.loc[selected["is_good_pick"]].copy()
        neutral = selected.loc[selected["is_neutral_pick"]].copy()
        near_miss = selected.loc[selected["best_near_miss_rank"].notna()].copy() if "best_near_miss_rank" in selected.columns else selected.iloc[0:0].copy()
        return selected, bad, good, neutral, near_miss, selected

    top5_selected, top5_bad, top5_good, top5_neutral, top5_near, _ = _cohort_for(top5_mask, True, False)
    top10_selected, top10_bad, top10_good, top10_neutral, top10_near, _ = _cohort_for(top10_mask, False, True)

    numeric_rows_top5 = [
        _numeric_feature_row(frame, feature=field, bad=top5_bad, good=top5_good, neutral=top5_neutral, near_miss=top5_near, all_bad=all_bad, all_good=all_good, topk="top5")
        for field in NUMERIC_FEATURE_FIELDS
    ]
    numeric_rows_top10 = [
        _numeric_feature_row(frame, feature=field, bad=top10_bad, good=top10_good, neutral=top10_neutral, near_miss=top10_near, all_bad=all_bad, all_good=all_good, topk="top10")
        for field in NUMERIC_FEATURE_FIELDS
    ]

    categorical_fields = [
        "monthly_context",
        "weekly_context",
        "daily_main_state_ctx",
        "dominant_regime_context",
        "market_regime_bucket",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "prefilter_bucket",
        "selected_by",
    ]
    categorical_rows_top5 = [
        _categorical_feature_row(feature=field, bad=top5_bad, good=top5_good, neutral=top5_neutral, near_miss=top5_near, topk="top5")
        for field in categorical_fields
        if field in frame.columns
    ]
    categorical_rows_top10 = [
        _categorical_feature_row(feature=field, bad=top10_bad, good=top10_good, neutral=top10_neutral, near_miss=top10_near, topk="top10")
        for field in categorical_fields
        if field in frame.columns
    ]

    summary = {
        "schema_version": CONTRAST_SCHEMA_VERSION,
        "top5_summary": {
            "bad_count": int(len(top5_bad)),
            "good_count": int(len(top5_good)),
            "neutral_count": int(len(top5_neutral)),
            "near_miss_count": int(len(top5_near)),
            "all_bad_count": int(len(all_bad.loc[all_bad["champion_selected_top5"]])) if "champion_selected_top5" in all_bad.columns else None,
            "all_good_count": int(len(all_good.loc[all_good["champion_selected_top5"]])) if "champion_selected_top5" in all_good.columns else None,
            "bad_forward_ret_20d_mean": _safe_float(top5_bad["forward_ret_20d"].mean()) if len(top5_bad) else None,
            "bad_path_value_score_v1_mean": _safe_float(top5_bad["path_value_score_v1"].mean()) if len(top5_bad) else None,
            "good_forward_ret_20d_mean": _safe_float(top5_good["forward_ret_20d"].mean()) if len(top5_good) else None,
            "good_path_value_score_v1_mean": _safe_float(top5_good["path_value_score_v1"].mean()) if len(top5_good) else None,
        },
        "top10_summary": {
            "bad_count": int(len(top10_bad)),
            "good_count": int(len(top10_good)),
            "neutral_count": int(len(top10_neutral)),
            "near_miss_count": int(len(top10_near)),
            "all_bad_count": int(len(all_bad.loc[all_bad["champion_selected_top10"]])) if "champion_selected_top10" in all_bad.columns else None,
            "all_good_count": int(len(all_good.loc[all_good["champion_selected_top10"]])) if "champion_selected_top10" in all_good.columns else None,
            "bad_forward_ret_20d_mean": _safe_float(top10_bad["forward_ret_20d"].mean()) if len(top10_bad) else None,
            "bad_path_value_score_v1_mean": _safe_float(top10_bad["path_value_score_v1"].mean()) if len(top10_bad) else None,
            "good_forward_ret_20d_mean": _safe_float(top10_good["forward_ret_20d"].mean()) if len(top10_good) else None,
            "good_path_value_score_v1_mean": _safe_float(top10_good["path_value_score_v1"].mean()) if len(top10_good) else None,
        },
        "top5_feature_rows": sorted(
            numeric_rows_top5,
            key=lambda item: abs(item["delta_mean_bad_minus_good"]) if item["delta_mean_bad_minus_good"] is not None else -1,
            reverse=True,
        ),
        "top10_feature_rows": sorted(
            numeric_rows_top10,
            key=lambda item: abs(item["delta_mean_bad_minus_good"]) if item["delta_mean_bad_minus_good"] is not None else -1,
            reverse=True,
        ),
        "top5_categorical_rows": categorical_rows_top5,
        "top10_categorical_rows": categorical_rows_top10,
        "all_bad_vs_all_good_reference_rows": [
            {
                "feature": field,
                "all_bad_mean": _safe_float(all_bad[field].mean()) if field in all_bad.columns and pd.api.types.is_numeric_dtype(all_bad[field]) else None,
                "all_good_mean": _safe_float(all_good[field].mean()) if field in all_good.columns and pd.api.types.is_numeric_dtype(all_good[field]) else None,
                "availability": "available" if field in frame.columns else "missing",
            }
            for field in NUMERIC_FEATURE_FIELDS
            if field in frame.columns
        ],
        "unavailable_fields": sorted(
            field
            for field in [
                *MISSING_INTERNAL_COMPONENT_FIELDS,
                *MISSING_EVENT_FIELDS,
            ]
            if field not in frame.columns
        ),
    }
    return summary


def _build_pairwise_summary(pairs: pd.DataFrame) -> dict[str, Any]:
    if pairs.empty:
        return {
            "schema_version": PAIRWISE_SCHEMA_VERSION,
            "pair_count": 0,
            "matched_near_miss_count": 0,
            "unmatched_near_miss_count": 0,
        }
    score_gap = pd.to_numeric(pairs["score_gap"], errors="coerce")
    path_gap = pd.to_numeric(pairs["path_value_gap"], errors="coerce")
    ret_gap = pd.to_numeric(pairs["forward_ret_20d_gap"], errors="coerce")
    summary = {
        "schema_version": PAIRWISE_SCHEMA_VERSION,
        "pair_count": int(len(pairs)),
        "matched_near_miss_count": int(pairs["near_miss_joined"].fillna(False).astype(bool).sum()),
        "unmatched_near_miss_count": int((~pairs["near_miss_joined"].fillna(False).astype(bool)).sum()),
        "selected_higher_score_count": int((score_gap > 0).sum()),
        "selected_worse_path_count": int((path_gap < 0).sum()),
        "selected_worse_return_count": int((ret_gap < 0).sum()),
        "selected_higher_score_and_worse_path_count": int(((score_gap > 0) & (path_gap < 0)).sum()),
        "score_gap_mean": _safe_float(score_gap.mean()),
        "score_gap_median": _safe_float(score_gap.median()),
        "path_value_gap_mean": _safe_float(path_gap.mean()),
        "path_value_gap_median": _safe_float(path_gap.median()),
        "forward_ret_20d_gap_mean": _safe_float(ret_gap.mean()),
        "forward_ret_20d_gap_median": _safe_float(ret_gap.median()),
        "rank_gap_mean": _safe_float(pd.to_numeric(pairs["rank_gap"], errors="coerce").mean()),
        "rank_gap_median": _safe_float(pd.to_numeric(pairs["rank_gap"], errors="coerce").median()),
        "top5_pair_count": int((pairs["selected_scope"] == "top5").sum()),
        "top10_pair_count": int((pairs["selected_scope"] == "top10").sum()),
        "top5_score_gap_mean": _safe_float(score_gap[pairs["selected_scope"] == "top5"].mean()),
        "top10_score_gap_mean": _safe_float(score_gap[pairs["selected_scope"] == "top10"].mean()),
        "top5_path_gap_mean": _safe_float(path_gap[pairs["selected_scope"] == "top5"].mean()),
        "top10_path_gap_mean": _safe_float(path_gap[pairs["selected_scope"] == "top10"].mean()),
    }
    return summary


def _build_regime_breakdown(frame: pd.DataFrame, score_overweight: pd.DataFrame) -> dict[str, Any]:
    def _group_summary(group: pd.DataFrame, label_col: str) -> list[dict[str, Any]]:
        rows = []
        for value, sub in group.groupby(group[label_col].fillna("unknown").astype(str), dropna=False):
            rows.append(
                {
                    label_col: str(value),
                    "count": int(len(sub)),
                    "bad_pick_count": int(sub["is_bad_pick"].sum()),
                    "bad_pick_rate": _safe_float(sub["is_bad_pick"].mean()),
                    "score_component_overweight_count": int((sub["root_cause_code"].eq(BAD_PICK_ROOT_CAUSE)).sum()) if "root_cause_code" in sub.columns else 0,
                    "score_component_overweight_rate": _safe_float((sub["root_cause_code"].eq(BAD_PICK_ROOT_CAUSE)).mean()) if "root_cause_code" in sub.columns else None,
                    "mean_forward_ret_20d": _safe_float(sub["forward_ret_20d"].mean()) if "forward_ret_20d" in sub.columns else None,
                    "mean_path_value_score_v1": _safe_float(sub["path_value_score_v1"].mean()) if "path_value_score_v1" in sub.columns else None,
                }
            )
        return sorted(rows, key=lambda item: item["bad_pick_rate"] if item["bad_pick_rate"] is not None else -1, reverse=True)

    liquidity_bucket = frame["vol_ratio5_20"].apply(_bucket_liquidity) if "vol_ratio5_20" in frame.columns else pd.Series(["unknown"] * len(frame), index=frame.index)
    volatility_bucket = [
        _bucket_volatility(row.get("dist_ma20_pct"), row.get("dist_ma60_pct"), row.get("vol_ratio5_20"))
        for _, row in frame.iterrows()
    ]
    out = {
        "schema_version": REGIME_SCHEMA_VERSION,
        "side": _group_summary(frame, "side"),
        "topk": [
            {
                "topk": topk,
                "count": int(mask.sum()),
                "bad_pick_count": int((mask & frame["is_bad_pick"]).sum()),
                "bad_pick_rate": _safe_float(frame.loc[mask, "is_bad_pick"].mean()),
                "score_component_overweight_count": int((mask & frame["root_cause_code"].eq(BAD_PICK_ROOT_CAUSE)).sum()) if "root_cause_code" in frame.columns else 0,
                "mean_forward_ret_20d": _safe_float(frame.loc[mask, "forward_ret_20d"].mean()),
                "mean_path_value_score_v1": _safe_float(frame.loc[mask, "path_value_score_v1"].mean()),
            }
            for topk, mask in ((5, _group_topk_mask(frame, 5)), (10, _group_topk_mask(frame, 10)), (20, _group_topk_mask(frame, 20)))
        ],
        "month": _group_summary(frame, "month_bucket"),
        "dominant_regime_context": _group_summary(frame, "dominant_regime_context"),
        "monthly_context": _group_summary(frame, "monthly_context"),
        "weekly_context": _group_summary(frame, "weekly_context"),
        "daily_main_state_ctx": _group_summary(frame, "daily_main_state_ctx"),
        "liquidity_bucket": _group_summary(frame.assign(liquidity_bucket=liquidity_bucket), "liquidity_bucket"),
        "volatility_bucket": _group_summary(frame.assign(volatility_bucket=volatility_bucket), "volatility_bucket"),
        "monthly_context_no_lookahead_missing_count": int(frame["monthly_context_no_lookahead"].isna().sum()) if "monthly_context_no_lookahead" in frame.columns else None,
        "weekly_context_no_lookahead_missing_count": int(frame["weekly_context_no_lookahead"].isna().sum()) if "weekly_context_no_lookahead" in frame.columns else None,
        "score_component_overweight_months": sorted(score_overweight["month_bucket"].dropna().astype(str).unique().tolist()),
        "score_component_overweight_regimes": sorted(score_overweight["dominant_regime_context"].dropna().astype(str).unique().tolist()),
    }
    return out


def _build_failure_patterns(score_overweight: pd.DataFrame, pairs: pd.DataFrame) -> dict[str, Any]:
    patterns: list[dict[str, Any]] = []
    if len(score_overweight):
        score_gap = pd.to_numeric(pairs["score_gap"], errors="coerce") if not pairs.empty else pd.Series(dtype=float)
        path_gap = pd.to_numeric(pairs["path_value_gap"], errors="coerce") if not pairs.empty else pd.Series(dtype=float)
        ret_gap = pd.to_numeric(pairs["forward_ret_20d_gap"], errors="coerce") if not pairs.empty else pd.Series(dtype=float)
        patterns.append(
            {
                "pattern_id": "PATTERN-01",
                "pattern": "score_higher_than_near_miss_but_path_worse",
                "count": int(((score_gap > 0) & (path_gap < 0)).sum()) if len(pairs) else 0,
                "evidence_fields_used": ["score", "best_near_miss_score", "path_value_score_v1", "best_near_miss_path_value_score_v1", "score_gap", "path_value_gap"],
                "notes": "the selected champion row usually had the higher score but the near-miss candidate had the better realized 20-day path",
            }
        )
        patterns.append(
            {
                "pattern_id": "PATTERN-02",
                "pattern": "higher_timeframe_overextension_cluster",
                "count": int(
                    (
                        score_overweight["monthly_context"].astype(str).str.contains("overextended", na=False)
                        & score_overweight["weekly_context"].astype(str).str.contains("overextended", na=False)
                    ).sum()
                ),
                "evidence_fields_used": [
                    "monthly_context",
                    "weekly_context",
                    "daily_main_state_ctx",
                    "dist_ma20_pct",
                    "dist_ma60_pct",
                ],
                "notes": "most score-overweight bad picks sit in monthly/weekly overextended states with stretched MA distances when overlay is available",
            }
        )
        patterns.append(
            {
                "pattern_id": "PATTERN-03",
                "pattern": "regime_dependent_family_and_high_score",
                "count": int((score_overweight["family_classification"].astype(str) == "regime_dependent_family").sum()),
                "evidence_fields_used": [
                    "family_classification",
                    "family_mean_path_value_score_v1",
                    "family_bottom15_rate",
                    "score",
                ],
                "notes": "regime-dependent families show up repeatedly in the bad cohort and the selected score does not offset the weaker path profile",
            }
        )
        patterns.append(
            {
                "pattern_id": "PATTERN-04",
                "pattern": "shape_positive_modifier_not_sufficient",
                "count": int((score_overweight["shape_classification"].astype(str) == "shape_positive_modifier").sum()),
                "evidence_fields_used": ["shape_classification", "candle_shape_modifier", "score", "path_value_score_v1"],
                "notes": "shape-positive rows still appear in the bad cohort, so shape alone cannot rescue an overweight score selection",
            }
        )
    return {
        "schema_version": FAILURE_SCHEMA_VERSION,
        "patterns": patterns,
        "matched_pair_count": int(len(pairs)),
        "score_gap_positive_count": int((pd.to_numeric(pairs["score_gap"], errors="coerce") > 0).sum()) if len(pairs) else 0,
        "path_gap_negative_count": int((pd.to_numeric(pairs["path_value_gap"], errors="coerce") < 0).sum()) if len(pairs) else 0,
        "return_gap_negative_count": int((pd.to_numeric(pairs["forward_ret_20d_gap"], errors="coerce") < 0).sum()) if len(pairs) else 0,
    }


def _build_hypotheses(score_overweight: pd.DataFrame, pairs: pd.DataFrame) -> dict[str, Any]:
    overextended_count = int(
        (
            score_overweight["monthly_context"].astype(str).str.contains("overextended", na=False)
            & score_overweight["weekly_context"].astype(str).str.contains("overextended", na=False)
            & score_overweight["daily_main_state_ctx"].astype(str).isin(["daily_reversal_up_candidate", "daily_up_mid"])
        ).sum()
    ) if len(score_overweight) else 0
    score_gap_mean = _safe_float(pd.to_numeric(pairs["score_gap"], errors="coerce").mean()) if len(pairs) else None
    path_gap_mean = _safe_float(pd.to_numeric(pairs["path_value_gap"], errors="coerce").mean()) if len(pairs) else None
    hypotheses = [
        {
            "hypothesis_id": "HP-SCO-01",
            "failing_component_or_combination": "score plus higher-timeframe overextension proxy",
            "plain_language_condition": "Require confirmation or cap the score when monthly and weekly contexts are both overextended and daily MA distance is stretched.",
            "required_fields": [
                "score",
                "monthly_context",
                "weekly_context",
                "daily_main_state_ctx",
                "dist_ma20_pct",
                "dist_ma60_pct",
            ],
            "expected_benefit": "Reduce selection of late, path-poor long candidates that win only on total score.",
            "expected_false_positive_risk": "May drop some strong continuation winners in persistent trend regimes.",
            "why_it_may_move_top5_top10_boundary": "The boundary audit shows score-overweight rows usually have higher score than the near miss while the near miss has a better realized path.",
            "recommended_next_validation_method": "One-axis cap or require-confirmation challenger on the high-extension slice only.",
            "test_style": "require-confirmation",
        },
        {
            "hypothesis_id": "HP-SCO-02",
            "failing_component_or_combination": "score with regime-dependent family evidence",
            "plain_language_condition": "Deprioritize selected rows when score is high but the family is regime-dependent and the family-level path profile is weak.",
            "required_fields": [
                "score",
                "family_classification",
                "family_mean_path_value_score_v1",
                "family_bottom15_rate",
            ],
            "expected_benefit": "Lower false positives where the score outruns the family evidence.",
            "expected_false_positive_risk": "Could suppress a subset of valid regime-sensitive winners.",
            "why_it_may_move_top5_top10_boundary": "The bad cohort is enriched in regime-dependent families and boundary near-miss rows frequently have better path support.",
            "recommended_next_validation_method": "Narrow deprioritize challenger on regime-dependent family with poor family path.",
            "test_style": "deprioritize",
        },
        {
            "hypothesis_id": "HP-SCO-03",
            "failing_component_or_combination": "score with positive shape but weak path confirmation",
            "plain_language_condition": "Add a confirmation requirement when shape is positive but the realized path proxy remains weak relative to near-miss candidates.",
            "required_fields": [
                "score",
                "shape_classification",
                "candle_shape_modifier",
                "path_value_score_v1",
                "family_mean_path_value_score_v1",
            ],
            "expected_benefit": "Keep the useful shape information while preventing score-only wins.",
            "expected_false_positive_risk": "May remove continuation setups that need time to realize.",
            "why_it_may_move_top5_top10_boundary": "Score-overweight rows still appear with shape-positive labels, so shape alone does not protect the boundary.",
            "recommended_next_validation_method": "Explanation-only first, then cap or confirmation on the narrow slice if the boundary still moves.",
            "test_style": "explanation-only",
        },
    ]
    return {
        "schema_version": HYPOTHESIS_SCHEMA_VERSION,
        "hypotheses": hypotheses,
        "count": len(hypotheses),
        "overextended_pair_count": overextended_count,
        "score_gap_mean": score_gap_mean,
        "path_gap_mean": path_gap_mean,
    }


def _build_decision(
    *,
    score_overweight: pd.DataFrame,
    pair_summary: dict[str, Any],
    contrast_summary: dict[str, Any],
    root_cause_summary: dict[str, Any],
) -> dict[str, Any]:
    pair_count = int(pair_summary.get("pair_count") or 0)
    matched_count = int(pair_summary.get("matched_near_miss_count") or 0)
    score_gap_mean = _safe_float(pair_summary.get("score_gap_mean"))
    path_gap_mean = _safe_float(pair_summary.get("path_value_gap_mean"))
    return_gap_mean = _safe_float(pair_summary.get("forward_ret_20d_gap_mean"))
    score_gap_positive = int(pair_summary.get("selected_higher_score_count") or 0)
    path_gap_negative = int(pair_summary.get("selected_worse_path_count") or 0)
    if len(score_overweight) and pair_count:
        if score_gap_positive >= max(1, int(pair_count * 0.8)) and path_gap_negative >= max(1, int(pair_count * 0.8)):
            decision = "ready_for_single_axis_challenger_design"
            reason = "score_component_overweight_is_consistent_and_boundary_pairs_show_higher_score_but_worse_path"
        else:
            decision = "explanation_only"
            reason = "score_component_overweight_pattern_is_present_but_boundary_signal_is_not_uniform_enough"
    else:
        decision = "needs_more_input_data"
        reason = "score_component_overweight_pairs_could_not_be_materialized"
    strong_root_cause_candidates = ["score_component_overweight"] if len(score_overweight) else []
    next_single_axis_challenger_recommended = bool(strong_root_cause_candidates)
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "decision": decision,
        "typed_reason": reason,
        "authoritative_rollup_decision": decision,
        "strong_root_cause_candidates": strong_root_cause_candidates,
        "primary_next_axis_root_cause": strong_root_cause_candidates[0] if strong_root_cause_candidates else None,
        "next_single_axis_challenger_recommended": next_single_axis_challenger_recommended,
        "pairwise_match_rate": _safe_float(matched_count / max(pair_count, 1)),
        "score_gap_mean": score_gap_mean,
        "path_gap_mean": path_gap_mean,
        "return_gap_mean": return_gap_mean,
        "root_cause_summary_snapshot": {
            "bad_pick_count": root_cause_summary.get("bad_pick_count"),
            "score_component_overweight_count": root_cause_summary.get("root_cause_counts", {}).get(BAD_PICK_ROOT_CAUSE),
            "unknown_or_insufficient_data_count": root_cause_summary.get("root_cause_counts", {}).get("unknown_or_insufficient_data"),
        },
        "feature_contrast_available": bool(contrast_summary.get("top5_feature_rows") and contrast_summary.get("top10_feature_rows")),
    }


def _build_input_resolution(
    *,
    source_rows_parquet: Path,
    audit_session: Path,
    boundary_session: Path,
    policy_ledger_path: Path,
    selected_rows: pd.DataFrame,
    runtime_status: dict[str, Any] | None,
    freshness: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "selected_source_surface": str(source_rows_parquet),
        "selected_audit_session": str(audit_session),
        "selected_boundary_session": str(boundary_session),
        "selected_policy_ledger": str(policy_ledger_path),
        "candidate_surface_selected_row_count": int(len(selected_rows)),
        "candidate_surface_selected_anchor_count": int(selected_rows["anchor_date"].nunique()) if "anchor_date" in selected_rows.columns else None,
        "candidate_surface_selected_top20_count": int(selected_rows["champion_selected_top20"].sum()) if "champion_selected_top20" in selected_rows.columns else None,
        "selected_reason": "candidate_prefilter_rows.parquet provides the audited champion topK surface; policy ledger adds point-in-time context; audit and boundary artifacts add outcome and near-miss labels.",
        "candidate_alternatives_checked": [
            {
                "source": str(DEFAULT_POLICY_LEDGER),
                "reason_rejected": "policy ledger alone does not provide the full champion surface and outcome labels needed for same-condition comparison.",
            },
            {
                "source": str(DEFAULT_AUDIT_SESSION / "bad_pick_cases.parquet"),
                "reason_rejected": "audit cases alone only cover bad picks and cannot reconstruct the good/neutral cohorts or near-miss winners.",
            },
            {
                "source": str(DEFAULT_BOUNDARY_SESSION / "boundary_near_miss_comparison.parquet"),
                "reason_rejected": "boundary summary alone lacks the full selected-row surface and score-context inventory.",
            },
        ],
        "runtime_status": runtime_status or {},
        "rankings_freshness": freshness or {},
        "selected_rows_have_no_lookahead_flags": bool(
            {"monthly_context_no_lookahead", "weekly_context_no_lookahead"}.issubset(set(selected_rows.columns))
        ),
        "missing_fields": [
            field
            for field in [
                "daily_main_state_ctx",
                "dist_ma20_pct",
                "dist_ma60_pct",
                "monthly_context_no_lookahead",
                "weekly_context_no_lookahead",
            ]
            if field not in selected_rows.columns
        ],
    }


def run_score_component_overweight_decomposition_v1(
    *,
    source_rows_parquet: str | Path | None = None,
    audit_session: str | Path | None = None,
    boundary_session: str | Path | None = None,
    policy_ledger_path: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = DEFAULT_LIMIT_ANCHOR_DATES,
    jobs: int = 2,
) -> dict[str, Any]:
    source_rows_parquet = _resolve_source_path(source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET, "source rows parquet")
    audit_session = _resolve_source_path(audit_session, DEFAULT_AUDIT_SESSION, "audit session")
    boundary_session = _resolve_source_path(boundary_session, DEFAULT_BOUNDARY_SESSION, "boundary session")
    policy_ledger_path = _resolve_source_path(policy_ledger_path, DEFAULT_POLICY_LEDGER, "policy ledger")
    output_root = _resolve_output_root(output_root)

    runtime_status = get_runtime_stock_db_status()
    freshness = get_rankings_freshness()

    selected, source_info = _load_selected_surface(
        source_rows_parquet=source_rows_parquet,
        audit_cases_path=audit_session / "bad_pick_cases.parquet",
        boundary_path=boundary_session / "boundary_near_miss_comparison.parquet",
        policy_ledger_path=policy_ledger_path,
        limit_anchor_dates=limit_anchor_dates,
    )
    if "champion_selected_top20" not in selected.columns:
        raise RuntimeError("selected surface lost top20 selection column")
    selected = _normalize_frame(selected)
    selected["audit_bad_pick"] = selected["is_bad_pick"].fillna(False).astype(bool) if "is_bad_pick" in selected.columns else False
    selected["audit_good_pick"] = selected["is_good_pick"].fillna(False).astype(bool) if "is_good_pick" in selected.columns else False
    selected["audit_neutral_pick"] = selected["is_neutral_pick"].fillna(False).astype(bool) if "is_neutral_pick" in selected.columns else False
    selected["root_cause_code"] = selected.get("root_cause_code", pd.Series(["unknown"] * len(selected), index=selected.index)).fillna("unknown").astype(str)
    selected["root_cause_confidence"] = selected.get("root_cause_confidence", pd.Series(["unknown"] * len(selected), index=selected.index)).fillna("unknown").astype(str)
    selected["bad_pick_scope"] = selected.get("bad_pick_scope", pd.Series(["unknown"] * len(selected), index=selected.index)).fillna("unknown").astype(str)
    selected["score_component_overweight_flag"] = selected["root_cause_code"].eq(BAD_PICK_ROOT_CAUSE)
    selected["liquidity_bucket"] = selected["vol_ratio5_20"].apply(_bucket_liquidity) if "vol_ratio5_20" in selected.columns else "unknown"
    selected["volatility_bucket"] = [
        _bucket_volatility(row.get("dist_ma20_pct"), row.get("dist_ma60_pct"), row.get("vol_ratio5_20"))
        for _, row in selected.iterrows()
    ]

    bad_pick_cases = selected.loc[selected["score_component_overweight_flag"]].copy()
    all_bad = selected.loc[selected["is_bad_pick"]].copy() if "is_bad_pick" in selected.columns else selected.iloc[0:0].copy()
    all_good = selected.loc[selected["is_good_pick"]].copy() if "is_good_pick" in selected.columns else selected.iloc[0:0].copy()

    inventory = _build_score_component_inventory(selected)
    cohort_summary = _build_score_overweight_cohort_summary(selected, bad_pick_cases)
    pairwise_rows = _boundary_pair_rows(selected, bad_pick_cases)
    pairwise_summary = _build_pairwise_summary(pairwise_rows)
    contrast_summary = _build_feature_contrast_summary(selected, bad_pick_cases, all_bad, all_good)
    regime_breakdown = _build_regime_breakdown(selected, bad_pick_cases)
    failure_patterns = _build_failure_patterns(bad_pick_cases, pairwise_rows)
    hypotheses = _build_hypotheses(bad_pick_cases, pairwise_rows)
    root_cause_summary = {
        "schema_version": "derived_from_tradex_bad_pick_root_cause_audit_v1",
        "bad_pick_count": int((selected["is_bad_pick"]).sum()),
        "root_cause_counts": selected["root_cause_code"].value_counts(dropna=False).to_dict(),
        "confidence_distribution": selected["root_cause_confidence"].value_counts(dropna=False).to_dict(),
        "missingness_summary": {
            field: int(selected[field].isna().sum()) if field in selected.columns else None
            for field in ("monthly_context", "weekly_context", "daily_main_state_ctx", "shape_classification", "candle_shape_modifier", "vol_ratio5_20", "gap_pct")
        },
    }
    decision = _build_decision(
        score_overweight=bad_pick_cases,
        pair_summary=pairwise_summary,
        contrast_summary=contrast_summary,
        root_cause_summary=root_cause_summary,
    )

    session_id = _make_session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "session_id": session_id,
        "generated_at": _utc_now(),
        "git_hash": _git_hash_or_unknown(),
        "source_rows_parquet": str(source_rows_parquet),
        "audit_session": str(audit_session),
        "boundary_session": str(boundary_session),
        "policy_ledger_path": str(policy_ledger_path),
        "selected_row_count": int(len(selected)),
        "score_overweight_row_count": int(len(bad_pick_cases)),
        "pairwise_row_count": int(len(pairwise_rows)),
        "limit_anchor_dates": limit_anchor_dates,
        "jobs": int(jobs),
        "runtime_status": runtime_status,
        "rankings_freshness": freshness,
        "no_lookahead_inherited": True,
        "monthly_context_no_lookahead": bool(selected["monthly_context_no_lookahead"].fillna(False).astype(bool).all()) if "monthly_context_no_lookahead" in selected.columns else False,
        "weekly_context_no_lookahead": bool(selected["weekly_context_no_lookahead"].fillna(False).astype(bool).all()) if "weekly_context_no_lookahead" in selected.columns else False,
        "source_surface_rows": int(len(pd.read_parquet(source_rows_parquet))),
        "source_surface_selected_rows": int(len(selected)),
        "selected_top5_rows": int(selected["champion_selected_top5"].sum()),
        "selected_top10_rows": int(selected["champion_selected_top10"].sum()),
        "selected_top20_rows": int(selected["champion_selected_top20"].sum()),
        "policy_overlay_rows": int(len(_load_policy_feature_overlay(policy_ledger_path, {(str(r.anchor_date), str(r.symbol), str(r.side)) for r in selected[["anchor_date", "symbol", "side"]].itertuples(index=False)}))),
        "source_info": source_info,
    }

    input_resolution = _build_input_resolution(
        source_rows_parquet=source_rows_parquet,
        audit_session=audit_session,
        boundary_session=boundary_session,
        policy_ledger_path=policy_ledger_path,
        selected_rows=selected,
        runtime_status=runtime_status,
        freshness=freshness,
    )

    pairwise_rows = pairwise_rows.copy()
    pairwise_rows["schema_version"] = PAIRWISE_SCHEMA_VERSION

    artifacts = {
        "run_manifest.json": manifest,
        "input_resolution.json": input_resolution,
        "score_component_inventory.json": inventory,
        "score_overweight_cohort_summary.json": cohort_summary,
        "score_component_contrast_summary.json": contrast_summary,
        "score_component_pairwise_boundary_summary.json": pairwise_summary,
        "score_component_regime_breakdown.json": regime_breakdown,
        "score_component_failure_patterns.json": failure_patterns,
        "score_component_challenger_hypotheses.json": hypotheses,
        "score_component_overweight_decomposition_v1_decision.json": decision,
    }
    for name, payload in artifacts.items():
        _write_json(session_dir / name, payload)
    pairwise_rows.to_parquet(session_dir / "score_component_pairwise_boundary.parquet", index=False)
    selected.to_parquet(session_dir / "score_component_overweight_rows.parquet", index=False)
    complete = {
        "schema_version": "tradex_score_component_overweight_decomposition_v1_artifact_complete_v1",
        "session_id": session_id,
        "required_files": sorted([*artifacts.keys(), "score_component_pairwise_boundary.parquet", "score_component_overweight_rows.parquet", "_ARTIFACT_COMPLETE.json"]),
        "parse_status": {name: True for name in artifacts},
        "parquet_readable": True,
        "row_reconciliation": {
            "source_surface_selected_rows": int(len(selected)),
            "score_overweight_rows": int(len(bad_pick_cases)),
            "pairwise_rows": int(len(pairwise_rows)),
            "selected_vs_source_match": int(len(selected)) == int(len(pd.read_parquet(source_rows_parquet).loc[pd.read_parquet(source_rows_parquet)["champion_selected_top20"].fillna(False).astype(bool)])),
        },
    }
    _write_json(session_dir / "_ARTIFACT_COMPLETE.json", complete)

    result = {
        "session_id": session_id,
        "session_dir": str(session_dir),
        "manifest_path": str(session_dir / "run_manifest.json"),
        "input_resolution_path": str(session_dir / "input_resolution.json"),
        "inventory_path": str(session_dir / "score_component_inventory.json"),
        "cohort_summary_path": str(session_dir / "score_overweight_cohort_summary.json"),
        "contrast_summary_path": str(session_dir / "score_component_contrast_summary.json"),
        "pairwise_parquet_path": str(session_dir / "score_component_pairwise_boundary.parquet"),
        "pairwise_summary_path": str(session_dir / "score_component_pairwise_boundary_summary.json"),
        "regime_breakdown_path": str(session_dir / "score_component_regime_breakdown.json"),
        "failure_patterns_path": str(session_dir / "score_component_failure_patterns.json"),
        "hypotheses_path": str(session_dir / "score_component_challenger_hypotheses.json"),
        "decision_path": str(session_dir / "score_component_overweight_decomposition_v1_decision.json"),
        "complete_path": str(session_dir / "_ARTIFACT_COMPLETE.json"),
        "selected_row_count": int(len(selected)),
        "score_overweight_row_count": int(len(bad_pick_cases)),
        "pairwise_row_count": int(len(pairwise_rows)),
    }
    return result


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX score component overweight decomposition audit")
    parser.add_argument("--source-rows-parquet", type=str, default=None)
    parser.add_argument("--audit-session", type=str, default=None)
    parser.add_argument("--boundary-session", type=str, default=None)
    parser.add_argument("--policy-ledger", type=str, default=None)
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=DEFAULT_LIMIT_ANCHOR_DATES)
    parser.add_argument("--jobs", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(argv)
    result = run_score_component_overweight_decomposition_v1(
        source_rows_parquet=args.source_rows_parquet,
        audit_session=args.audit_session,
        boundary_session=args.boundary_session,
        policy_ledger_path=args.policy_ledger,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
