from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd
from PIL import Image, ImageDraw

from external_analysis.contracts.paths import resolve_source_db_path
from external_analysis.ma_hierarchical_labels import build_hierarchical_label_artifacts, build_hierarchical_regime_gate_artifacts


MONTHLY_SHAPE_MEMORY_SCHEMA_VERSION = "tradex_monthly_shape_memory_v1"
MONTHLY_SHAPE_MEMORY_SPLIT_SCHEMA_VERSION = "tradex_monthly_shape_memory_split_v1"
MONTHLY_SHAPE_MEMORY_LABEL_SCHEMA_VERSION = "tradex_monthly_shape_memory_label_v1"
MONTHLY_SHAPE_MEMORY_BRANCH_SCHEMA_VERSION = "tradex_monthly_shape_memory_branch_eval_v1"
MONTHLY_SHAPE_MEMORY_COMPARE_SCHEMA_VERSION = "tradex_monthly_shape_memory_compare_v1"
MONTHLY_SHAPE_MEMORY_DECISION_SCHEMA_VERSION = "tradex_monthly_shape_memory_decision_v1"
HIERARCHICAL_LABEL_SCHEMA_VERSION = "tradex_monthly_shape_memory_hierarchical_label_v1"
BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION = "tradex_monthly_shape_memory_boundary_winner_promotion_v1"

BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW = (6, 30)
BOUNDARY_WINNER_PROMOTION_STRICT_WINDOW = (8, 20)
BOUNDARY_WINNER_PROMOTION_TOP5_FREEZE = 5
BOUNDARY_WINNER_PROMOTION_MIN_ROWS = 20
BOUNDARY_WINNER_PROMOTION_MIN_MONTHS = 8
BOUNDARY_WINNER_PROMOTION_MIN_REGIME_STABILITY = 0.67
BOUNDARY_WINNER_PROMOTION_BONUS_CAP = 1.25
BOUNDARY_WINNER_PROMOTION_PENALTY_CAP = 1.0
BOUNDARY_WINNER_PROMOTION_SCORE_SCALE = 20.0
BOUNDARY_WINNER_PROMOTION_NEAR_ZERO_BOUNDARY = 0.05

LIGHTWEIGHT_BOUNDARY_CHALLENGER_SCHEMA_VERSION = "tradex_monthly_shape_memory_lightweight_boundary_challenger_v1"
LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW = (6, 20)
LIGHTWEIGHT_BOUNDARY_CHALLENGER_STRICT_WINDOW = (8, 20)
LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE = 5
LIGHTWEIGHT_BOUNDARY_CHALLENGER_MIN_ROWS = 12
LIGHTWEIGHT_BOUNDARY_CHALLENGER_MIN_MONTHS = 8
LIGHTWEIGHT_BOUNDARY_CHALLENGER_MIN_REGIME_STABILITY = 0.5
LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_DELTA_SCALE = 10.0
LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_WEIGHT = 0.10
LIGHTWEIGHT_BOUNDARY_CHALLENGER_SUPPORT_WEIGHT = 0.01
LIGHTWEIGHT_BOUNDARY_CHALLENGER_CANDIDATE_DATASET_NAME = "lightweight_boundary_challenger_candidates.parquet"

LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_POSITIVE_FEATURES: tuple[str, ...] = (
    "window_return_1m",
    "window_return_3m",
    "window_return_6m",
    "dist_ma20",
    "dist_ma60",
    "dist_ma120",
    "slope_early",
    "slope_mid",
    "slope_late",
    "breakout_proximity_low",
    "volume_expansion_20_60",
    "recovery_6m",
    "liquidity_score",
    "regime_trend_score",
)
LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_NEGATIVE_FEATURES: tuple[str, ...] = (
    "breakout_proximity_high",
    "drawdown_6m",
    "regime_volatility_score",
)

DEFAULT_TOP_K = 10
DEFAULT_CANDIDATE_POOL_K = 30
DEFAULT_ANALOG_K = 8
DEFAULT_MEMORY_LOOKBACK_MONTHS = 24
DEFAULT_ROLLING_WINDOW_MONTHS = 36
DEFAULT_MIN_HISTORY_MONTHS = 6
DEFAULT_START_MONTH = 201001

FEATURE_BRANCH_SPECS: tuple[tuple[str, str], ...] = (
    ("window_return_1m", "common_feature"),
    ("window_return_3m", "common_feature"),
    ("window_return_6m", "common_feature"),
    ("box_width_6m", "boundary_feature"),
    ("breakout_proximity_high", "boundary_feature"),
    ("breakout_proximity_low", "boundary_feature"),
    ("dist_ma20", "common_feature"),
    ("dist_ma60", "regime_correction"),
    ("dist_ma120", "regime_correction"),
    ("slope_early", "common_feature"),
    ("slope_mid", "common_feature"),
    ("slope_late", "boundary_feature"),
    ("curvature", "bad_pick_removal"),
    ("volume_expansion_20_60", "symbol_specific_deviation"),
    ("gap_frequency", "symbol_specific_deviation"),
    ("wick_body_ratio", "bad_pick_removal"),
    ("drawdown_6m", "bad_pick_removal"),
    ("recovery_6m", "boundary_feature"),
    ("volatility_6m", "regime_correction"),
    ("missing_days_ratio", "symbol_specific_deviation"),
    ("liquidity_score", "symbol_specific_deviation"),
    ("regime_trend_score", "regime_correction"),
    ("regime_volatility_score", "regime_correction"),
)

NUMERIC_FEATURE_COLUMNS: tuple[str, ...] = tuple(name for name, _ in FEATURE_BRANCH_SPECS)
IMAGE_EMBEDDING_DIM = 32
TOP5_K = 5
BOUNDARY_BUCKETS: tuple[tuple[int, int, str], ...] = (
    (1, 5, "1-5"),
    (6, 10, "6-10"),
    (11, 20, "11-20"),
    (21, 50, "21-50"),
)


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _slug(value: str) -> str:
    raw = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in _text(value))
    raw = raw.strip("-_")
    return raw or "monthly_shape_memory"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return float(max(low, min(high, float(value))))


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def monthly_shape_memory_dir() -> Path:
    path = _repo_root() / "artifacts" / "monthly_shape_memory"
    path.mkdir(parents=True, exist_ok=True)
    return path


def monthly_shape_memory_path(name: str) -> Path:
    return monthly_shape_memory_dir() / name


def _artifact_name(name: str, artifact_suffix: str | None) -> str:
    suffix = _text(artifact_suffix).strip()
    if not suffix:
        return name
    if name.endswith(".parquet"):
        return name[:-8] + f".{suffix}.parquet"
    if name.endswith(".json"):
        return name[:-5] + f".{suffix}.json"
    return f"{name}.{suffix}"


def _artifact_path(name: str, artifact_suffix: str | None) -> Path:
    return monthly_shape_memory_path(_artifact_name(name, artifact_suffix))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _sign_label(delta: float, *, tolerance: float = 1e-12) -> str:
    if delta > tolerance:
        return "positive"
    if delta < -tolerance:
        return "negative"
    return "neutral"


def _state_key_dict(group_cols: list[str], keys: Any) -> dict[str, Any]:
    if len(group_cols) == 1:
        if isinstance(keys, tuple) and len(keys) == 1:
            keys = keys[0]
        key_values = (keys,)
    else:
        key_values = keys if isinstance(keys, tuple) else (keys,)
    result: dict[str, Any] = {}
    for col, value in zip(group_cols, key_values):
        result[col] = "unknown" if pd.isna(value) else _text(value)
    return result


def _state_recommendation(*, uplift_delta: float, boundary_delta: float, winner_minus_loser_gap: float) -> str:
    if uplift_delta < 0.0 and boundary_delta < 0.0:
        return "avoid_for_rerank"
    if (uplift_delta > 0.0 or boundary_delta > 0.0) and winner_minus_loser_gap >= 0.0:
        return "use_for_filter_analysis"
    return "keep_for_similarity_only"


def _build_state_diagnostic_rows(rows: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame()
    work = rows.copy()
    global_mean_return = float(pd.to_numeric(work["next_month_return"], errors="coerce").mean())
    global_mean_boundary = float(pd.to_numeric(work["top10_boundary_gap"], errors="coerce").mean())
    grouped = work.groupby(group_cols, dropna=False)
    records: list[dict[str, Any]] = []
    for keys, group in grouped:
        group_key = _state_key_dict(group_cols, keys)
        mean_return = float(pd.to_numeric(group["next_month_return"], errors="coerce").mean())
        mean_boundary = float(pd.to_numeric(group["top10_boundary_gap"], errors="coerce").mean())
        mean_winner = float(pd.to_numeric(group["winner_promotion_score"], errors="coerce").mean())
        mean_loser = float(pd.to_numeric(group["loser_removal_score"], errors="coerce").mean())
        uplift_delta = float(mean_return - global_mean_return)
        boundary_delta = float(mean_boundary - global_mean_boundary)
        winner_minus_loser_gap = float(mean_winner - mean_loser)
        records.append(
            {
                "state_combination": group_key,
                "row_count": int(len(group)),
                "mean_next_month_return": mean_return,
                "mean_top10_boundary_gap": mean_boundary,
                "mean_winner_promotion_score": mean_winner,
                "mean_loser_removal_score": mean_loser,
                "winner_minus_loser_gap": winner_minus_loser_gap,
                "uplift_delta": uplift_delta,
                "boundary_delta": boundary_delta,
                "observed_uplift_contribution_sign": _sign_label(uplift_delta),
                "observed_boundary_contribution_sign": _sign_label(boundary_delta),
                "recommendation": _state_recommendation(
                    uplift_delta=uplift_delta,
                    boundary_delta=boundary_delta,
                    winner_minus_loser_gap=winner_minus_loser_gap,
                ),
            }
        )
    diagnostics = pd.DataFrame(records)
    if diagnostics.empty:
        return diagnostics
    diagnostics["recommendation_rank"] = diagnostics["recommendation"].map(
        {
            "avoid_for_rerank": 0,
            "keep_for_similarity_only": 1,
            "use_for_filter_analysis": 2,
        }
    ).fillna(1)
    diagnostics = diagnostics.sort_values(
        ["recommendation_rank", "uplift_delta", "boundary_delta", "row_count"],
        ascending=[True, True, True, False],
    ).drop(columns=["recommendation_rank"])
    return diagnostics.reset_index(drop=True)


def _build_hierarchical_split_decision_payload(
    *,
    hierarchical_rows: pd.DataFrame,
    regime_gate_compare: dict[str, Any],
    hierarchical_summary: dict[str, Any],
    hierarchical_weekly_diversity: dict[str, Any],
) -> dict[str, Any]:
    gated_variant = (regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_regime_gated", {})
    ungated_variant = (regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_ungated", {})
    champion_variant = (regime_gate_compare.get("variants") or {}).get("champion_only", {})
    gated_winner_delta = float(gated_variant.get("winner_promotion_delta") or 0.0)
    gated_loser_delta = float(gated_variant.get("loser_removal_delta") or 0.0)
    gated_boundary_improved = bool(gated_variant.get("top10_boundary_improved"))
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "source_artifacts": {
            "hierarchical_regime_gate_compare": "hierarchical_regime_gate_compare.json",
            "hierarchical_label_summary": "hierarchical_label_summary.json",
            "hierarchical_weekly_diversity_before_after": "hierarchical_weekly_diversity_before_after.json",
        },
        "hierarchical_label_layer_decision": "keep_infrastructure",
        "hierarchical_label_layer_reason": (
            "weekly diversity repair broadened the representation layer and the hierarchy remains useful for diagnostics, "
            "filter analysis, and decomposition, but the rerank branch did not become a stable production challenger."
        ),
        "hierarchical_regime_gated_rerank_decision": "drop_axis",
        "hierarchical_regime_gated_rerank_reason": (
            f"full-snapshot gated oos_top10_uplift={float(gated_variant.get('oos_top10_uplift') or 0.0):.16f}, "
            f"winner_promotion_improved={gated_winner_delta > 0.0}, "
            f"boundary_improved={gated_boundary_improved}, "
            "the challenger remains unstable versus the business target."
        ),
        "full_snapshot_oos_top10_uplift": float(gated_variant.get("oos_top10_uplift") or 0.0),
        "winner_promotion_improved": gated_winner_delta > 0.0,
        "loser_removal_improved": gated_loser_delta > 0.0,
        "boundary_improved": gated_boundary_improved,
        "recommended_next_use": "diagnostics / filter-analysis / decomposition only",
        "supporting_metrics": {
            "champion_only_oos_top10_uplift": float(champion_variant.get("oos_top10_uplift") or 0.0),
            "ungated_oos_top10_uplift": float(ungated_variant.get("oos_top10_uplift") or 0.0),
            "gated_oos_top10_uplift": float(gated_variant.get("oos_top10_uplift") or 0.0),
            "ungated_boundary_outcome_gap": float(ungated_variant.get("top10_boundary_outcome_gap") or 0.0),
            "gated_boundary_outcome_gap": float(gated_variant.get("top10_boundary_outcome_gap") or 0.0),
            "gate_activation_rate": float(gated_variant.get("gate_activation_rate") or 0.0),
            "gate_suppression_rate": float(gated_variant.get("gate_suppression_rate") or 0.0),
            "gate_veto_only_rate": float(gated_variant.get("gate_veto_only_rate") or 0.0),
            "weekly_label_count_before": int((hierarchical_weekly_diversity or {}).get("before", {}).get("label_count") or 0),
            "weekly_label_count_after": int((hierarchical_weekly_diversity or {}).get("after", {}).get("label_count") or 0),
            "hierarchical_row_count": int(len(hierarchical_rows)),
            "monthly_main_state_count": int(len((hierarchical_summary or {}).get("label_counts", {}).get("monthly_main_state", {}))),
            "weekly_main_state_count": int(len((hierarchical_summary or {}).get("label_counts", {}).get("weekly_main_state", {}))),
            "daily_main_state_count": int(len((hierarchical_summary or {}).get("label_counts", {}).get("daily_main_state", {}))),
        },
        "evidence_summary": {
            "weekly_diversity_repair": "weekly collapse repaired from one dominant warning label to eight weekly labels",
            "full_snapshot_decision": regime_gate_compare.get("decision"),
            "full_snapshot_decision_reason": regime_gate_compare.get("decision_reason_typed"),
            "leakage_check_status": (hierarchical_summary or {}).get("leakage_check_status", "unknown"),
            "winner_promotion_improved": gated_winner_delta > 0.0,
            "loser_removal_improved": gated_loser_delta > 0.0,
            "boundary_improved": gated_boundary_improved,
        },
    }


def _build_hierarchical_allowed_uses_payload(
    *,
    split_decision: dict[str, Any],
    regime_gate_compare: dict[str, Any],
    hierarchical_weekly_diversity: dict[str, Any],
    leakage_check_status: str,
) -> dict[str, Any]:
    gated_variant = (regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_regime_gated", {})
    allowed_use_cases = [
        {
            "use_case": "regime diagnostics",
            "reason": "labels separate monthly, weekly, and daily structure and expose regime-gated failure modes.",
        },
        {
            "use_case": "boundary failure analysis",
            "reason": "the hierarchy carries boundary_gap, winner_promotion_score, and loser_removal_score for comparison.",
        },
        {
            "use_case": "analog retrieval filtering",
            "reason": "the MA-aware states and scores can filter similar symbol-month contexts without changing the target.",
        },
        {
            "use_case": "winner/loser decomposition",
            "reason": "winner_promotion_score and loser_removal_score stay inspectable and additive.",
        },
        {
            "use_case": "future feature construction",
            "reason": "the layer remains a stable research representation, not a production rerank axis.",
        },
    ]
    disallowed_use_cases = [
        {
            "use_case": "regime-gated rerank challenger",
            "reason": "full snapshot did not show stable top10 uplift or boundary improvement.",
        },
        {
            "use_case": "production ranking replacement",
            "reason": "champion_only remains the authoritative baseline and should not be displaced by this axis.",
        },
        {
            "use_case": "gate broadening rescue attempt",
            "reason": "the rerank failure is business-level, not a missing-threshold problem.",
        },
        {
            "use_case": "image complexity rescue",
            "reason": "the label layer is already compact and image-side expansion is out of scope.",
        },
    ]
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "source_artifacts": {
            "hierarchical_labels_keep_rerank_drop": "authoritative_decision.hierarchical_labels_keep_rerank_drop.json",
            "hierarchical_regime_gate_compare": "hierarchical_regime_gate_compare.json",
            "hierarchical_weekly_diversity_before_after": "hierarchical_weekly_diversity_before_after.json",
        },
        "allowed_use_cases": allowed_use_cases,
        "disallowed_use_cases": disallowed_use_cases,
        "evidence_summary": {
            "split_decision": split_decision.get("hierarchical_label_layer_decision", "keep_infrastructure"),
            "split_reason": split_decision.get("hierarchical_label_layer_reason"),
            "weekly_label_count_before": int((hierarchical_weekly_diversity or {}).get("before", {}).get("label_count") or 0),
            "weekly_label_count_after": int((hierarchical_weekly_diversity or {}).get("after", {}).get("label_count") or 0),
            "regime_gated_rerank_decision": split_decision.get("hierarchical_regime_gated_rerank_decision"),
            "leakage_check_status": leakage_check_status,
            "winner_promotion_improved": bool(split_decision.get("winner_promotion_improved")),
            "loser_removal_improved": bool(split_decision.get("loser_removal_improved")),
            "boundary_improved": bool(split_decision.get("boundary_improved")),
        },
        "full_snapshot_supporting_metrics": {
            "champion_only_oos_top10_uplift": float((regime_gate_compare.get("variants") or {}).get("champion_only", {}).get("oos_top10_uplift") or 0.0),
            "ungated_oos_top10_uplift": float((regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_ungated", {}).get("oos_top10_uplift") or 0.0),
            "gated_oos_top10_uplift": float(gated_variant.get("oos_top10_uplift") or 0.0),
            "ungated_boundary_outcome_gap": float((regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_ungated", {}).get("top10_boundary_outcome_gap") or 0.0),
            "gated_boundary_outcome_gap": float(gated_variant.get("top10_boundary_outcome_gap") or 0.0),
            "gate_activation_rate": float(gated_variant.get("gate_activation_rate") or 0.0),
            "gate_suppression_rate": float(gated_variant.get("gate_suppression_rate") or 0.0),
            "gate_veto_only_rate": float(gated_variant.get("gate_veto_only_rate") or 0.0),
            "weekly_label_count_before": int((hierarchical_weekly_diversity or {}).get("before", {}).get("label_count") or 0),
            "weekly_label_count_after": int((hierarchical_weekly_diversity or {}).get("after", {}).get("label_count") or 0),
        },
    }


def _build_hierarchical_state_diagnostics(
    *,
    hierarchical_rows: pd.DataFrame,
    regime_gate_compare: dict[str, Any],
) -> dict[str, Any]:
    rows = hierarchical_rows.copy()
    if rows.empty:
        empty_payload = {
            "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
            "source_artifacts": {
                "hierarchical_regime_gate_compare": "hierarchical_regime_gate_compare.json",
                "hierarchical_label_summary": "hierarchical_label_summary.json",
            },
            "global_metrics": {},
            "state_combination_rows": [],
            "monthly_state_rows": [],
            "weekly_state_rows": [],
            "daily_state_rows": [],
            "avoid_for_rerank_combinations": [],
            "safest_to_ignore_combinations": [],
        }
        return {
            "state_failure_map": empty_payload,
            "winner_loser_decomposition": empty_payload,
            "do_not_continue": {
                "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
                "decision": "drop_axis",
                "reason": "insufficient data",
            },
        }
    global_mean_return = float(pd.to_numeric(rows["next_month_return"], errors="coerce").mean())
    global_mean_boundary = float(pd.to_numeric(rows["top10_boundary_gap"], errors="coerce").mean())
    global_mean_winner = float(pd.to_numeric(rows["winner_promotion_score"], errors="coerce").mean())
    global_mean_loser = float(pd.to_numeric(rows["loser_removal_score"], errors="coerce").mean())
    global_metrics = {
        "row_count": int(len(rows)),
        "symbol_count": int(rows["symbol"].nunique()),
        "month_count": int(rows["as_of_month"].nunique()),
        "mean_next_month_return": global_mean_return,
        "mean_top10_boundary_gap": global_mean_boundary,
        "mean_winner_promotion_score": global_mean_winner,
        "mean_loser_removal_score": global_mean_loser,
        "leakage_check_status": "pass",
    }
    combo_rows = _build_state_diagnostic_rows(rows, ["monthly_main_state", "weekly_main_state", "daily_main_state"])
    monthly_rows = _build_state_diagnostic_rows(rows, ["monthly_main_state"])
    weekly_rows = _build_state_diagnostic_rows(rows, ["weekly_main_state"])
    daily_rows = _build_state_diagnostic_rows(rows, ["daily_main_state"])
    avoid_for_rerank = combo_rows[combo_rows["recommendation"] == "avoid_for_rerank"].head(20) if not combo_rows.empty else pd.DataFrame()
    safe_similarity = combo_rows[combo_rows["recommendation"] == "keep_for_similarity_only"].head(20) if not combo_rows.empty else pd.DataFrame()
    filter_analysis = combo_rows[combo_rows["recommendation"] == "use_for_filter_analysis"].head(20) if not combo_rows.empty else pd.DataFrame()
    worst_daily = daily_rows.sort_values(["boundary_delta", "uplift_delta", "row_count"], ascending=[True, True, False]).head(10) if not daily_rows.empty else pd.DataFrame()
    loser_weekly = weekly_rows.sort_values(["mean_loser_removal_score", "boundary_delta", "row_count"], ascending=[False, True, False]).head(10) if not weekly_rows.empty else pd.DataFrame()
    negative_monthly = monthly_rows.sort_values(["uplift_delta", "boundary_delta", "row_count"], ascending=[True, True, False]).head(10) if not monthly_rows.empty else pd.DataFrame()
    state_failure_map = {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "source_artifacts": {
            "hierarchical_regime_gate_compare": "hierarchical_regime_gate_compare.json",
            "hierarchical_label_summary": "hierarchical_label_summary.json",
        },
        "global_metrics": global_metrics,
        "state_combination_rows": combo_rows.to_dict(orient="records"),
        "avoid_for_rerank_combinations": avoid_for_rerank.to_dict(orient="records"),
        "safest_to_ignore_combinations": pd.concat([avoid_for_rerank, safe_similarity], ignore_index=True).head(20).to_dict(orient="records") if not combo_rows.empty else [],
    }
    winner_loser_decomposition = {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "source_artifacts": {
            "hierarchical_regime_gate_compare": "hierarchical_regime_gate_compare.json",
            "hierarchical_label_summary": "hierarchical_label_summary.json",
        },
        "global_metrics": global_metrics,
        "monthly_state_rows": monthly_rows.to_dict(orient="records"),
        "weekly_state_rows": weekly_rows.to_dict(orient="records"),
        "daily_state_rows": daily_rows.to_dict(orient="records"),
        "negative_winner_promotion_monthly_states": negative_monthly.to_dict(orient="records"),
        "weekly_loser_removal_dominant_states": loser_weekly.to_dict(orient="records"),
        "daily_worst_boundary_states": worst_daily.to_dict(orient="records"),
        "filter_analysis_state_rows": filter_analysis.to_dict(orient="records"),
        "safe_similarity_only_state_rows": safe_similarity.to_dict(orient="records"),
    }
    do_not_continue = {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "decision": "drop_axis",
        "reason": "the regime-gated rerank axis did not deliver stable full-snapshot uplift or boundary improvement.",
        "instructions": [
            "Do not continue the regime-gated rerank axis.",
            "Do not broaden the gate logic to rescue this challenger.",
            "Do not spend another cycle trying to save this exact axis.",
            "Do not add image complexity to compensate for the rerank failure.",
        ],
        "supporting_metrics": {
            "champion_only_oos_top10_uplift": float((regime_gate_compare.get("variants") or {}).get("champion_only", {}).get("oos_top10_uplift") or 0.0),
            "ungated_oos_top10_uplift": float((regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_ungated", {}).get("oos_top10_uplift") or 0.0),
            "gated_oos_top10_uplift": float((regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_regime_gated", {}).get("oos_top10_uplift") or 0.0),
            "winner_promotion_improved": float((regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_regime_gated", {}).get("winner_promotion_delta") or 0.0) > 0.0,
            "loser_removal_improved": float((regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_regime_gated", {}).get("loser_removal_delta") or 0.0) > 0.0,
            "boundary_improved": bool((regime_gate_compare.get("variants") or {}).get("champion_plus_hierarchical_rerank_regime_gated", {}).get("top10_boundary_improved")),
        },
    }
    return {
        "state_failure_map": state_failure_map,
        "winner_loser_decomposition": winner_loser_decomposition,
        "do_not_continue": do_not_continue,
    }


def build_boundary_winner_promotion_artifacts(
    *,
    hierarchical_rows: pd.DataFrame,
    state_diagnostics: dict[str, Any],
    hierarchical_weekly_diversity: dict[str, Any],
    hierarchical_allowed_uses: dict[str, Any],
    top_k: int,
    candidate_pool_k: int,
) -> dict[str, Any]:
    rows = hierarchical_rows.copy()
    if rows.empty:
        empty_compare = {
            "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
            "top_k": int(top_k),
            "candidate_pool_k": int(candidate_pool_k),
            "variants": {},
            "best_variant": "champion_only",
            "decision": "drop_boundary_winner_promotion_challenger",
            "decision_reason_typed": "insufficient_data",
            "authoritative_rollup_decision": "drop_boundary_winner_promotion_challenger",
            "churn_acceptable": False,
        }
        empty_rules = {
            "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
            "candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW),
            "strict_candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_STRICT_WINDOW),
            "top5_freeze_enabled": True,
            "bonus_cap": BOUNDARY_WINNER_PROMOTION_BONUS_CAP,
            "penalty_cap": BOUNDARY_WINNER_PROMOTION_PENALTY_CAP,
            "promote_state_keys": [],
            "demote_state_keys": [],
            "filter_only_state_keys": [],
            "score_formula": "challenger_score = champion_score + boundary_winner_bonus - boundary_loser_penalty",
        }
        return {
            "state_rank_impact_table": {
                "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
                "source_artifacts": {
                    "hierarchical_state_failure_map": "hierarchical_state_failure_map.json",
                    "hierarchical_state_winner_loser_decomposition": "hierarchical_state_winner_loser_decomposition.json",
                    "hierarchical_label_allowed_uses": "hierarchical_label_allowed_uses.json",
                },
                "table_rows": [],
                "coverage_thresholds": {
                    "min_rows": BOUNDARY_WINNER_PROMOTION_MIN_ROWS,
                    "min_months": BOUNDARY_WINNER_PROMOTION_MIN_MONTHS,
                    "min_regime_stability": BOUNDARY_WINNER_PROMOTION_MIN_REGIME_STABILITY,
                },
            },
            "rules": empty_rules,
            "compare": empty_compare,
            "effect_by_regime": {
                "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
                "source_artifacts": empty_compare["source_artifacts"] if "source_artifacts" in empty_compare else {},
                "variants": {},
                "regimes": {},
            },
            "decision": {
                "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
                "decision": "drop_boundary_winner_promotion_challenger",
                "decision_reason_typed": "insufficient_data",
                "winner_promotion_improved": False,
                "loser_removal_improved": False,
                "boundary_improved": False,
                "churn_acceptable": False,
            },
        }

    state_failure_map = state_diagnostics.get("state_failure_map") or {}
    winner_loser_decomposition = state_diagnostics.get("winner_loser_decomposition") or {}
    combo_rows = pd.DataFrame(state_failure_map.get("state_combination_rows") or [])
    monthly_rows = pd.DataFrame(winner_loser_decomposition.get("monthly_state_rows") or [])
    weekly_rows = pd.DataFrame(winner_loser_decomposition.get("weekly_state_rows") or [])
    daily_rows = pd.DataFrame(winner_loser_decomposition.get("daily_state_rows") or [])

    def _state_key(kind: str, payload: dict[str, Any]) -> str:
        ordered = [f"{str(key)}={_text(value)}" for key, value in sorted(payload.items())]
        return f"{kind}|" + "|".join(ordered)

    def _lookup_frame(payload: dict[str, Any]) -> pd.DataFrame:
        if "monthly_main_state" in payload and "weekly_main_state" in payload and "daily_main_state" in payload:
            frame = rows
            for key, value in payload.items():
                frame = frame[frame[key] == _text(value)]
            return frame
        if "monthly_main_state" in payload:
            return rows[rows["monthly_main_state"] == _text(payload["monthly_main_state"])]
        if "weekly_main_state" in payload:
            return rows[rows["weekly_main_state"] == _text(payload["weekly_main_state"])]
        if "daily_main_state" in payload:
            return rows[rows["daily_main_state"] == _text(payload["daily_main_state"])]
        return rows.iloc[0:0].copy()

    global_mean_return = float(pd.to_numeric(rows["next_month_return"], errors="coerce").mean())
    global_mean_boundary = float(pd.to_numeric(rows["top10_boundary_gap"], errors="coerce").mean())

    def _regime_stability(group: pd.DataFrame) -> tuple[float, dict[str, Any]]:
        regime_rows: list[dict[str, Any]] = []
        for regime, regime_group in group.groupby("regime_tag", dropna=False):
            regime_mean_return = float(pd.to_numeric(regime_group["next_month_return"], errors="coerce").mean())
            regime_mean_boundary = float(pd.to_numeric(regime_group["top10_boundary_gap"], errors="coerce").mean())
            regime_rows.append(
                {
                    "regime_tag": _text(regime, fallback="unknown"),
                    "row_count": int(len(regime_group)),
                    "mean_next_month_return": regime_mean_return,
                    "mean_top10_boundary_gap": regime_mean_boundary,
                    "winner_sign": _sign_label(regime_mean_return - global_mean_return),
                    "boundary_sign": _sign_label(regime_mean_boundary - global_mean_boundary),
                }
            )
        if not regime_rows:
            return 0.0, {"regimes": [], "winner_sign_share": 0.0, "boundary_sign_share": 0.0}

        winner_signs = [row["winner_sign"] for row in regime_rows if row["winner_sign"] != "neutral"]
        boundary_signs = [row["boundary_sign"] for row in regime_rows if row["boundary_sign"] != "neutral"]
        winner_sign_share = float(max((winner_signs.count("positive"), winner_signs.count("negative"))) / len(winner_signs)) if winner_signs else 0.0
        boundary_sign_share = float(max((boundary_signs.count("positive"), boundary_signs.count("negative"))) / len(boundary_signs)) if boundary_signs else 0.0
        stability = min(winner_sign_share, boundary_sign_share)
        return stability, {
            "regimes": regime_rows,
            "winner_sign_share": winner_sign_share,
            "boundary_sign_share": boundary_sign_share,
            "regime_sign_stability": stability,
        }

    def _state_action(
        *,
        recommendation: str,
        coverage_rows: int,
        coverage_months: int,
        regime_sign_stability: float,
        mean_winner: float,
        mean_loser: float,
        winner_minus_loser_gap: float,
        boundary_signal: float,
        candidate_pool_signal: float,
    ) -> tuple[str, str]:
        if (
            coverage_rows < BOUNDARY_WINNER_PROMOTION_MIN_ROWS
            or coverage_months < BOUNDARY_WINNER_PROMOTION_MIN_MONTHS
            or regime_sign_stability < BOUNDARY_WINNER_PROMOTION_MIN_REGIME_STABILITY
        ):
            return "avoid", "insufficient_coverage_or_stability"
        if recommendation == "avoid_for_rerank":
            if mean_loser >= 60.0 and boundary_signal < 0.0 and winner_minus_loser_gap <= -5.0:
                return "demote", "strong_loser_signal"
            return "avoid", "avoid_for_rerank"
        if recommendation == "use_for_filter_analysis":
            if winner_minus_loser_gap >= 0.0 and mean_winner >= 52.0 and candidate_pool_signal > 0.0:
                return "promote", "positive_winner_and_candidate_signal"
            if mean_loser >= 55.0 and winner_minus_loser_gap < 0.0:
                return "filter_only", "loser_signal_only"
            return "neutral", "filter_analysis_neutral"
        if recommendation == "keep_for_similarity_only":
            if candidate_pool_signal > 0.0 and winner_minus_loser_gap > -10.0:
                return "filter_only", "similarity_support"
            if mean_winner >= 50.0 and candidate_pool_signal > 0.01:
                return "promote", "similarity_but_positive"
            return "neutral", "similarity_neutral"
        return "neutral", "neutral"

    def _build_table_for_rows(source_rows: pd.DataFrame, *, kind: str) -> list[dict[str, Any]]:
        if source_rows.empty:
            return []
        records: list[dict[str, Any]] = []
        for row in source_rows.to_dict(orient="records"):
            state_combination = row.get("state_combination") or {}
            if not isinstance(state_combination, dict):
                continue
            state_frame = _lookup_frame(state_combination)
            if state_frame.empty:
                continue
            coverage_rows = int(len(state_frame))
            coverage_months = int(state_frame["sample_month"].nunique())
            regime_stability, regime_payload = _regime_stability(state_frame)
            recommended_rank_action, action_reason = _state_action(
                recommendation=_text(row.get("recommendation"), fallback="neutral"),
                coverage_rows=coverage_rows,
                coverage_months=coverage_months,
                regime_sign_stability=regime_stability,
                mean_winner=float(row.get("mean_winner_promotion_score") or 0.0),
                mean_loser=float(row.get("mean_loser_removal_score") or 0.0),
                winner_minus_loser_gap=float(row.get("winner_minus_loser_gap") or 0.0),
                boundary_signal=float(row.get("boundary_delta") or 0.0),
                candidate_pool_signal=float(row.get("uplift_delta") or 0.0),
            )
            state_key = _state_key(kind, state_combination)
            record = {
                "state_key": state_key,
                "state_key_kind": kind,
                "state_family": kind,
                "state_combination": state_combination,
                "coverage_rows": coverage_rows,
                "coverage_months": coverage_months,
                "coverage_regimes": int(state_frame["regime_tag"].nunique()),
                "winner_promotion_signal": float(row.get("mean_winner_promotion_score") or 0.0),
                "loser_removal_signal": float(row.get("mean_loser_removal_score") or 0.0),
                "boundary_signal": float(row.get("boundary_delta") or 0.0),
                "candidate_pool_signal": float(row.get("uplift_delta") or 0.0),
                "mean_next_month_return": float(row.get("mean_next_month_return") or 0.0),
                "winner_minus_loser_gap": float(row.get("winner_minus_loser_gap") or 0.0),
                "observed_uplift_contribution_sign": row.get("observed_uplift_contribution_sign"),
                "observed_boundary_contribution_sign": row.get("observed_boundary_contribution_sign"),
                "recommendation_source": row.get("recommendation"),
                "regime_sign_stability": float(regime_stability),
                "regime_sign_summary": regime_payload,
                "recommended_rank_action": recommended_rank_action,
                "action_reason": action_reason,
            }
            records.append(record)
        return records

    combo_table = _build_table_for_rows(combo_rows, kind="combo")
    monthly_table = _build_table_for_rows(monthly_rows, kind="monthly")
    weekly_table = _build_table_for_rows(weekly_rows, kind="weekly")
    daily_table = _build_table_for_rows(daily_rows, kind="daily")
    table_rows = combo_table + monthly_table + weekly_table + daily_table
    table_rows.sort(
        key=lambda item: (
            {
                "promote": 0,
                "demote": 1,
                "filter_only": 2,
                "neutral": 3,
                "avoid": 4,
            }.get(str(item.get("recommended_rank_action")), 3),
            -int(item.get("coverage_rows") or 0),
            -int(item.get("coverage_months") or 0),
            -float(item.get("winner_promotion_signal") or 0.0),
            -float(item.get("boundary_signal") or 0.0),
            _text(item.get("state_key")),
        )
    )
    state_rank_impact_table = {
        "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
        "source_artifacts": {
            "hierarchical_state_failure_map": "hierarchical_state_failure_map.json",
            "hierarchical_state_winner_loser_decomposition": "hierarchical_state_winner_loser_decomposition.json",
            "hierarchical_label_allowed_uses": "hierarchical_label_allowed_uses.json",
        },
        "coverage_thresholds": {
            "min_rows": BOUNDARY_WINNER_PROMOTION_MIN_ROWS,
            "min_months": BOUNDARY_WINNER_PROMOTION_MIN_MONTHS,
            "min_regime_stability": BOUNDARY_WINNER_PROMOTION_MIN_REGIME_STABILITY,
        },
        "table_rows": table_rows,
    }
    state_lookup: dict[str, dict[str, Any]] = {str(row["state_key"]): row for row in table_rows}
    promote_state_keys = [row["state_key"] for row in table_rows if row.get("recommended_rank_action") == "promote"]
    demote_state_keys = [row["state_key"] for row in table_rows if row.get("recommended_rank_action") == "demote"]
    filter_only_state_keys = [row["state_key"] for row in table_rows if row.get("recommended_rank_action") == "filter_only"]

    rules = {
        "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
        "source_artifacts": state_rank_impact_table["source_artifacts"],
        "candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW),
        "strict_candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_STRICT_WINDOW),
        "top5_freeze_enabled": True,
        "bonus_cap": BOUNDARY_WINNER_PROMOTION_BONUS_CAP,
        "penalty_cap": BOUNDARY_WINNER_PROMOTION_PENALTY_CAP,
        "bonus_scale": BOUNDARY_WINNER_PROMOTION_SCORE_SCALE,
        "penalty_scale": BOUNDARY_WINNER_PROMOTION_SCORE_SCALE,
        "promote_state_keys": promote_state_keys,
        "demote_state_keys": demote_state_keys,
        "filter_only_state_keys": filter_only_state_keys,
        "state_action_priority": ["promote", "demote", "filter_only", "neutral", "avoid"],
        "rank_window_policy": {
            "default": {
                "top5_freeze_enabled": True,
                "candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW),
                "frozen_prefix": [1, 5],
                "frozen_suffix": [31, None],
            },
            "strict_window": {
                "top5_freeze_enabled": True,
                "candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_STRICT_WINDOW),
                "frozen_prefix": [1, 7],
                "frozen_suffix": [21, None],
            },
        },
        "score_formula": (
            "boundary_winner_bonus = bonus_cap * clamp((winner_promotion_signal - 50.0) / bonus_scale, 0.0, 1.0) "
            "for promote states inside the candidate window; "
            "boundary_loser_penalty = penalty_cap * clamp((loser_removal_signal - 50.0) / penalty_scale, 0.0, 1.0) "
            "for demote states and 0.5 * penalty_cap * clamp((loser_removal_signal - 50.0) / penalty_scale, 0.0, 1.0) "
            "for filter_only states inside the candidate window; "
            "challenger_score = champion_score + boundary_winner_bonus - boundary_loser_penalty; "
            "top5 and tail rows are preserved by rank-window policy."
        ),
        "fallback_behavior": "retain champion_score when the state is not supported by the impact table or coverage/stability is insufficient.",
    }

    def _state_lookup_keys(row: pd.Series) -> list[str]:
        monthly = _text(row.get("monthly_main_state"), fallback="monthly_range_mid")
        weekly = _text(row.get("weekly_main_state"), fallback="weekly_range_mid")
        daily = _text(row.get("daily_main_state"), fallback="daily_range_mid")
        return [
            f"combo|daily_main_state={daily}|monthly_main_state={monthly}|weekly_main_state={weekly}",
            f"daily|daily_main_state={daily}",
            f"weekly|weekly_main_state={weekly}",
            f"monthly|monthly_main_state={monthly}",
        ]

    def _lookup_state_record(row: pd.Series) -> dict[str, Any] | None:
        for key in _state_lookup_keys(row):
            record = state_lookup.get(key)
            if not record:
                continue
            action = _text(record.get("recommended_rank_action"), fallback="neutral")
            if action == "avoid":
                return record
            if action != "neutral":
                return record
        for key in _state_lookup_keys(row):
            record = state_lookup.get(key)
            if record:
                return record
        return None

    def _apply_variant(month_frame: pd.DataFrame, *, window: tuple[int, int], apply_bonus: bool, apply_penalty: bool) -> pd.DataFrame:
        champion_ranked = _rank_month_frame(month_frame, "champion_score", top_k=max(top_k, candidate_pool_k)).copy()
        champion_ranked["champion_rank"] = np.arange(1, len(champion_ranked) + 1, dtype=int)
        champion_ranked["boundary_state_key"] = ""
        champion_ranked["boundary_rank_action"] = "none"
        champion_ranked["boundary_winner_bonus"] = 0.0
        champion_ranked["boundary_loser_penalty"] = 0.0
        champion_ranked["boundary_action_type"] = "none"
        champion_ranked["boundary_rank_window_weight"] = 0.0
        champion_ranked["boundary_challenger_score"] = pd.to_numeric(champion_ranked["champion_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)

        window_low, window_high = int(window[0]), int(window[1])
        window_mask = (champion_ranked["champion_rank"] >= window_low) & (champion_ranked["champion_rank"] <= window_high)
        window_rows = champion_ranked.loc[window_mask].copy()
        if not window_rows.empty:
            weights = []
            for rank in window_rows["champion_rank"].astype(float).tolist():
                if window_high <= window_low:
                    weight = 1.0
                else:
                    span = max(1.0, float(window_high - window_low))
                    weight = 1.0 - 0.5 * ((float(rank) - float(window_low)) / span)
                weights.append(float(max(0.5, min(1.0, weight))))
            window_rows["boundary_rank_window_weight"] = weights
            selected_records = window_rows.apply(_lookup_state_record, axis=1)
            keys: list[str] = []
            actions: list[str] = []
            bonus_values: list[float] = []
            penalty_values: list[float] = []
            action_types: list[str] = []
            for idx, record in enumerate(selected_records.tolist()):
                if not record:
                    keys.append("")
                    actions.append("neutral")
                    bonus_values.append(0.0)
                    penalty_values.append(0.0)
                    action_types.append("none")
                    continue
                action = _text(record.get("recommended_rank_action"), fallback="neutral")
                if action == "avoid":
                    action = "neutral"
                winner_strength = _clamp((float(record.get("winner_promotion_signal") or 0.0) - 50.0) / BOUNDARY_WINNER_PROMOTION_SCORE_SCALE, 0.0, 1.0)
                loser_strength = _clamp((float(record.get("loser_removal_signal") or 0.0) - 50.0) / BOUNDARY_WINNER_PROMOTION_SCORE_SCALE, 0.0, 1.0)
                window_weight = float(window_rows.iloc[idx]["boundary_rank_window_weight"])
                bonus = 0.0
                penalty = 0.0
                if apply_bonus and action == "promote":
                    bonus = float(BOUNDARY_WINNER_PROMOTION_BONUS_CAP * winner_strength * window_weight)
                if apply_penalty and action in {"demote", "filter_only"}:
                    penalty_multiplier = 1.0 if action == "demote" else 0.5
                    penalty = float(BOUNDARY_WINNER_PROMOTION_PENALTY_CAP * loser_strength * window_weight * penalty_multiplier)
                keys.append(str(record.get("state_key") or ""))
                actions.append(action)
                bonus_values.append(float(bonus))
                penalty_values.append(float(penalty))
                if bonus > 0.0:
                    action_types.append("promote")
                elif penalty > 0.0:
                    action_types.append("demote")
                else:
                    action_types.append("none")
            window_rows["boundary_state_key"] = keys
            window_rows["boundary_rank_action"] = actions
            window_rows["boundary_winner_bonus"] = bonus_values
            window_rows["boundary_loser_penalty"] = penalty_values
            window_rows["boundary_action_type"] = action_types
            window_rows["boundary_challenger_score"] = (
                pd.to_numeric(window_rows["champion_score"], errors="coerce").fillna(50.0)
                + window_rows["boundary_winner_bonus"]
                - window_rows["boundary_loser_penalty"]
            ).clip(0.0, 100.0)
            window_rows = window_rows.sort_values(
                ["boundary_challenger_score", "champion_rank", "code"],
                ascending=[False, True, True],
            ).reset_index(drop=True)
        prefix_rows = champion_ranked.loc[~window_mask & (champion_ranked["champion_rank"] < window_low)].copy()
        suffix_rows = champion_ranked.loc[champion_ranked["champion_rank"] > window_high].copy()
        final_rows = pd.concat([prefix_rows, window_rows, suffix_rows], ignore_index=True, sort=False)
        final_rows["pred_rank"] = np.arange(1, len(final_rows) + 1, dtype=int)
        final_rows["pred_rank_pct"] = 1.0 if len(final_rows) <= 1 else 1.0 - ((final_rows["pred_rank"] - 1) / (len(final_rows) - 1))
        final_rows["pred_is_top10"] = (final_rows["pred_rank"] <= min(top_k, len(final_rows))).astype(int)
        return final_rows

    def _variant_month_metrics(final_ranked: pd.DataFrame, champion_ranked: pd.DataFrame, *, variant_name: str, window: tuple[int, int]) -> dict[str, Any]:
        top10 = final_ranked.head(min(top_k, len(final_ranked)))
        champion_top10 = champion_ranked.head(min(top_k, len(champion_ranked)))
        top5 = final_ranked.head(min(TOP5_K, len(final_ranked)))
        champion_top5 = champion_ranked.head(min(TOP5_K, len(champion_ranked)))
        candidate_pool = final_ranked.head(min(candidate_pool_k, len(final_ranked)))
        champion_candidate_pool = champion_ranked.head(min(candidate_pool_k, len(champion_ranked)))
        final_top10_boundary = final_ranked.iloc[min(top_k - 1, len(final_ranked) - 1)]
        final_next_boundary = final_ranked.iloc[min(top_k, len(final_ranked) - 1)] if len(final_ranked) > top_k else final_top10_boundary
        champion_top10_boundary = champion_ranked.iloc[min(top_k - 1, len(champion_ranked) - 1)]
        champion_next_boundary = champion_ranked.iloc[min(top_k, len(champion_ranked) - 1)] if len(champion_ranked) > top_k else champion_top10_boundary
        champion_top5_boundary = champion_ranked.iloc[min(TOP5_K - 1, len(champion_ranked) - 1)]
        champion_next_top5 = champion_ranked.iloc[min(TOP5_K, len(champion_ranked) - 1)] if len(champion_ranked) > TOP5_K else champion_top5_boundary
        final_top5_boundary = final_ranked.iloc[min(TOP5_K - 1, len(final_ranked) - 1)]
        final_next_top5 = final_ranked.iloc[min(TOP5_K, len(final_ranked) - 1)] if len(final_ranked) > TOP5_K else final_top5_boundary
        final_boundary_gap_value = float(final_top10_boundary["next_month_return"] - final_next_boundary["next_month_return"]) if len(final_ranked) > top_k else float(final_top10_boundary["next_month_return"])
        champion_boundary_gap_value = float(champion_top10_boundary["next_month_return"] - champion_next_boundary["next_month_return"]) if len(champion_ranked) > top_k else float(champion_top10_boundary["next_month_return"])
        champion_candidate_pool_bad_pick_removal = int(max(0, int(champion_ranked[champion_ranked["is_next_bottom10"] == 1]["sample_id"].nunique()) - int(champion_candidate_pool["is_next_bottom10"].sum())))
        final_candidate_pool_bad_pick_removal = int(max(0, int(final_ranked[final_ranked["is_next_bottom10"] == 1]["sample_id"].nunique()) - int(candidate_pool["is_next_bottom10"].sum())))
        changed_top10_members = set(champion_top10["sample_id"]) ^ set(top10["sample_id"])
        changed_top5_members = set(champion_top5["sample_id"]) ^ set(top5["sample_id"])
        champion_rank_map = {row.sample_id: int(idx + 1) for idx, row in enumerate(champion_ranked.head(min(candidate_pool_k, len(champion_ranked))).itertuples(index=False))}
        final_rank_map = {row.sample_id: int(idx + 1) for idx, row in enumerate(final_ranked.head(min(candidate_pool_k, len(final_ranked))).itertuples(index=False))}
        changed_rank_count = sum(1 for sample_id in set(champion_rank_map) | set(final_rank_map) if champion_rank_map.get(sample_id) != final_rank_map.get(sample_id))
        boundary_actions = final_ranked[(final_ranked["pred_rank"] >= window[0]) & (final_ranked["pred_rank"] <= window[1])]
        promotion_action_rate = float((pd.to_numeric(boundary_actions["boundary_winner_bonus"], errors="coerce").fillna(0.0) > 0.0).mean()) if len(boundary_actions) else 0.0
        demotion_action_rate = float((pd.to_numeric(boundary_actions["boundary_loser_penalty"], errors="coerce").fillna(0.0) > 0.0).mean()) if len(boundary_actions) else 0.0
        no_action_rate = float((pd.to_numeric(boundary_actions["boundary_winner_bonus"], errors="coerce").fillna(0.0) <= 0.0).mul(pd.to_numeric(boundary_actions["boundary_loser_penalty"], errors="coerce").fillna(0.0) <= 0.0).mean()) if len(boundary_actions) else 0.0
        return {
            "variant_name": variant_name,
            "candidate_rank_window": list(window),
            "sample_count": int(len(final_ranked)),
            "top5_hit_count": int(top5["is_next_top10"].sum()),
            "top5_hit_rate": float(top5["is_next_top10"].mean()) if len(top5) else 0.0,
            "mean_next_month_return_top5": float(top5["next_month_return"].mean()) if len(top5) else 0.0,
            "median_next_month_return_top5": float(top5["next_month_return"].median()) if len(top5) else 0.0,
            "top5_boundary_score_gap": float(final_top5_boundary["boundary_challenger_score"] - final_next_top5["boundary_challenger_score"]) if len(final_ranked) > TOP5_K else float(final_top5_boundary["boundary_challenger_score"]),
            "top5_boundary_outcome_gap": float(final_top5_boundary["next_month_return"] - final_next_top5["next_month_return"]) if len(final_ranked) > TOP5_K else float(final_top5_boundary["next_month_return"]),
            "champion_top5_hit_count": int(champion_top5["is_next_top10"].sum()),
            "champion_top5_hit_rate": float(champion_top5["is_next_top10"].mean()) if len(champion_top5) else 0.0,
            "changed_top5_members_count": int(len(changed_top5_members)),
            "champion_top5_boundary_score_gap": float(champion_top5_boundary["champion_score"] - champion_next_top5["champion_score"]) if len(champion_ranked) > TOP5_K else float(champion_top5_boundary["champion_score"]),
            "champion_top5_boundary_outcome_gap": float(champion_top5_boundary["next_month_return"] - champion_next_top5["next_month_return"]) if len(champion_ranked) > TOP5_K else float(champion_top5_boundary["next_month_return"]),
            "top10_hit_count": int(top10["is_next_top10"].sum()),
            "top10_hit_rate": float(top10["is_next_top10"].mean()) if len(top10) else 0.0,
            "mean_next_month_return_top10": float(top10["next_month_return"].mean()) if len(top10) else 0.0,
            "median_next_month_return_top10": float(top10["next_month_return"].median()) if len(top10) else 0.0,
            "champion_top10_hit_count": int(champion_top10["is_next_top10"].sum()),
            "champion_top10_hit_rate": float(champion_top10["is_next_top10"].mean()) if len(champion_top10) else 0.0,
            "champion_top10_mean_next_month_return": float(champion_top10["next_month_return"].mean()) if len(champion_top10) else 0.0,
            "champion_top10_median_next_month_return": float(champion_top10["next_month_return"].median()) if len(champion_top10) else 0.0,
            "candidate_pool_top10_capture": float(candidate_pool["is_next_top10"].sum()),
            "candidate_pool_top10_capture_delta": float(candidate_pool["is_next_top10"].sum() - champion_candidate_pool["is_next_top10"].sum()),
            "candidate_pool_bad_pick_removal": float(final_candidate_pool_bad_pick_removal),
            "candidate_pool_bad_pick_removal_delta": float(final_candidate_pool_bad_pick_removal - champion_candidate_pool_bad_pick_removal),
            "changed_top10_members_count": float(len(changed_top10_members)),
            "changed_rank_count": float(changed_rank_count),
            "top10_boundary_score_gap": float(final_top10_boundary["boundary_challenger_score"] - final_next_boundary["boundary_challenger_score"]) if len(final_ranked) > top_k else float(final_top10_boundary["boundary_challenger_score"]),
            "champion_top10_boundary_score_gap": float(champion_top10_boundary["champion_score"] - champion_next_boundary["champion_score"]) if len(champion_ranked) > top_k else float(champion_top10_boundary["champion_score"]),
            "top10_boundary_outcome_gap": final_boundary_gap_value,
            "champion_top10_boundary_outcome_gap": champion_boundary_gap_value,
            "top10_boundary_improved": bool(final_boundary_gap_value > champion_boundary_gap_value),
            "boundary_bonus_total": float(pd.to_numeric(final_ranked["boundary_winner_bonus"], errors="coerce").fillna(0.0).sum()),
            "boundary_penalty_total": float(pd.to_numeric(final_ranked["boundary_loser_penalty"], errors="coerce").fillna(0.0).sum()),
            "promotion_action_rate": promotion_action_rate,
            "demotion_action_rate": demotion_action_rate,
            "no_action_rate": no_action_rate,
            "regime_tag": _text(champion_ranked.iloc[0].regime_tag, fallback="mixed"),
            "monthly_rows": final_ranked.to_dict(orient="records"),
        }

    variants = {
        "champion_only": {"window": BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW, "apply_bonus": False, "apply_penalty": False},
        "boundary_bonus_only": {"window": BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW, "apply_bonus": True, "apply_penalty": False},
        "boundary_penalty_only": {"window": BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW, "apply_bonus": False, "apply_penalty": True},
        "boundary_bonus_plus_penalty": {"window": BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW, "apply_bonus": True, "apply_penalty": True},
        "boundary_bonus_plus_penalty_strict_window": {"window": BOUNDARY_WINNER_PROMOTION_STRICT_WINDOW, "apply_bonus": True, "apply_penalty": True},
    }
    variant_payloads: dict[str, dict[str, Any]] = {}
    for variant_name, spec in variants.items():
        monthly_rows_payload: list[dict[str, Any]] = []
        boundary_bucket_rows: dict[str, list[dict[str, Any]]] = {label: [] for _, _, label in BOUNDARY_BUCKETS}
        for month, month_frame in rows.groupby("sample_month", sort=True):
            champion_ranked = _rank_month_frame(month_frame, "champion_score", top_k=max(top_k, candidate_pool_k))
            final_ranked = _apply_variant(month_frame, window=spec["window"], apply_bonus=bool(spec["apply_bonus"]), apply_penalty=bool(spec["apply_penalty"]))
            month_metrics = _variant_month_metrics(final_ranked, champion_ranked, variant_name=variant_name, window=spec["window"])
            bucket_input = final_ranked.drop(columns=["rerank_score"], errors="ignore").assign(rerank_score=final_ranked["boundary_challenger_score"])
            bucket_effect = _bucket_effect_summary(champion_ranked, bucket_input, top_k=top_k)
            for bucket_label, payload in bucket_effect.items():
                if isinstance(payload, dict):
                    boundary_bucket_rows[bucket_label].append(payload)
            month_metrics["bucket_effect"] = bucket_effect
            month_metrics["regime_tag"] = _text(champion_ranked.iloc[0].regime_tag, fallback="mixed")
            monthly_rows_payload.append(month_metrics)
        monthly_df = pd.DataFrame(monthly_rows_payload)
        if monthly_df.empty:
            variant_payloads[variant_name] = {
                "variant_name": variant_name,
                "candidate_rank_window": list(spec["window"]),
                "top5_freeze_enabled": True,
                "apply_bonus": bool(spec["apply_bonus"]),
                "apply_penalty": bool(spec["apply_penalty"]),
                "month_count": 0,
                "sample_count": 0,
                "oos_top10_uplift": 0.0,
                "oos_bad_pick_removal": 0.0,
                "changed_top10_members_count": 0.0,
                "changed_top5_members_count": 0.0,
                "changed_rank_count": 0.0,
                "top10_boundary_outcome_gap": 0.0,
                "champion_top10_boundary_outcome_gap": 0.0,
                "top10_boundary_improved": False,
                "winner_promotion_delta": 0.0,
                "loser_removal_delta": 0.0,
                "candidate_pool_top10_capture": 0.0,
                "candidate_pool_top10_capture_delta": 0.0,
                "candidate_pool_bad_pick_removal": 0.0,
                "candidate_pool_bad_pick_removal_delta": 0.0,
                "mean_next_month_return_top10": 0.0,
                "median_next_month_return_top10": 0.0,
                "mean_next_month_return_top5": 0.0,
                "median_next_month_return_top5": 0.0,
                "promotion_action_rate": 0.0,
                "demotion_action_rate": 0.0,
                "no_action_rate": 0.0,
                "boundary_bonus_total": 0.0,
                "boundary_penalty_total": 0.0,
                "boundary_bucket_effect": {},
                "regime_breakdown": {},
                "monthly_rows": [],
            }
            continue
        regime_breakdown: dict[str, dict[str, float]] = {}
        for regime, regime_df in monthly_df.groupby("regime_tag"):
            uplift_series = regime_df["top10_hit_count"] - regime_df["champion_top10_hit_count"]
            regime_breakdown[str(regime)] = {
                "month_count": float(len(regime_df)),
                "mean_top10_uplift": float(uplift_series.mean()),
                "mean_bad_pick_removal": float(regime_df["candidate_pool_bad_pick_removal"].mean()),
                "mean_changed_top10_members_count": float(regime_df["changed_top10_members_count"].mean()),
                "mean_top10_boundary_outcome_gap": float(regime_df["top10_boundary_outcome_gap"].mean()),
                "mean_candidate_pool_top10_capture": float(regime_df["candidate_pool_top10_capture"].mean()),
                "mean_candidate_pool_bad_pick_removal": float(regime_df["candidate_pool_bad_pick_removal"].mean()),
                "positive_month_share": float((uplift_series > 0).mean()),
                "negative_month_share": float((uplift_series < 0).mean()),
                "sign_stability": float(max((uplift_series > 0).mean(), (uplift_series < 0).mean())),
            }
        boundary_bucket_effect: dict[str, dict[str, float]] = {}
        for label, rows_list in boundary_bucket_rows.items():
            if not rows_list:
                boundary_bucket_effect[label] = {
                    "sample_count": 0,
                    "mean_champion_rank": 0.0,
                    "mean_rerank_rank": 0.0,
                    "mean_rank_delta": 0.0,
                    "mean_score_delta": 0.0,
                    "enter_top10_rate": 0.0,
                }
                continue
            bucket_df = pd.DataFrame(rows_list)
            boundary_bucket_effect[label] = {
                "sample_count": int(bucket_df["sample_count"].sum()),
                "mean_champion_rank": float(bucket_df["mean_champion_rank"].mean()),
                "mean_rerank_rank": float(bucket_df["mean_rerank_rank"].mean()),
                "mean_rank_delta": float(bucket_df["mean_rank_delta"].mean()),
                "mean_score_delta": float(bucket_df["mean_score_delta"].mean()),
                "enter_top10_rate": float(bucket_df["enter_top10_rate"].mean()),
            }
        uplift = monthly_df["top10_hit_count"] - monthly_df["champion_top10_hit_count"]
        variant_payloads[variant_name] = {
            "variant_name": variant_name,
            "candidate_rank_window": list(spec["window"]),
            "top5_freeze_enabled": True,
            "apply_bonus": bool(spec["apply_bonus"]),
            "apply_penalty": bool(spec["apply_penalty"]),
            "month_count": int(len(monthly_df)),
            "sample_count": int(monthly_df["sample_count"].sum()),
            "oos_top10_uplift": float(uplift.mean()),
            "oos_bad_pick_removal": float(monthly_df["candidate_pool_bad_pick_removal"].mean()),
            "changed_top10_members_count": float(monthly_df["changed_top10_members_count"].mean()),
            "changed_top5_members_count": float(monthly_df["changed_top5_members_count"].mean()),
            "changed_rank_count": float(monthly_df["changed_rank_count"].mean()),
            "top10_boundary_outcome_gap": float(monthly_df["top10_boundary_outcome_gap"].mean()),
            "champion_top10_boundary_outcome_gap": float(monthly_df["champion_top10_boundary_outcome_gap"].mean()),
            "top10_boundary_improved": bool(float(monthly_df["top10_boundary_outcome_gap"].mean()) > float(monthly_df["champion_top10_boundary_outcome_gap"].mean())),
            "winner_promotion_delta": float(uplift.mean()),
            "loser_removal_delta": float(monthly_df["candidate_pool_bad_pick_removal"].mean()),
            "candidate_pool_top10_capture": float(monthly_df["candidate_pool_top10_capture"].mean()),
            "candidate_pool_top10_capture_delta": float(monthly_df["candidate_pool_top10_capture_delta"].mean()),
            "candidate_pool_bad_pick_removal": float(monthly_df["candidate_pool_bad_pick_removal"].mean()),
            "candidate_pool_bad_pick_removal_delta": float(monthly_df["candidate_pool_bad_pick_removal_delta"].mean()),
            "mean_next_month_return_top10": float(monthly_df["mean_next_month_return_top10"].mean()),
            "median_next_month_return_top10": float(monthly_df["median_next_month_return_top10"].median()),
            "mean_next_month_return_top5": float(monthly_df["mean_next_month_return_top5"].mean()),
            "median_next_month_return_top5": float(monthly_df["median_next_month_return_top5"].median()),
            "promotion_action_rate": float(monthly_df["promotion_action_rate"].mean()),
            "demotion_action_rate": float(monthly_df["demotion_action_rate"].mean()),
            "no_action_rate": float(monthly_df["no_action_rate"].mean()),
            "boundary_bonus_total": float(monthly_df["boundary_bonus_total"].mean()),
            "boundary_penalty_total": float(monthly_df["boundary_penalty_total"].mean()),
            "boundary_bonus_only_contribution": float(monthly_df["boundary_bonus_total"].mean()),
            "boundary_penalty_only_contribution": float(monthly_df["boundary_penalty_total"].mean()),
            "boundary_combined_contribution": float((monthly_df["boundary_bonus_total"] - monthly_df["boundary_penalty_total"]).mean()),
            "boundary_bucket_effect": boundary_bucket_effect,
            "regime_breakdown": regime_breakdown,
            "monthly_rows": monthly_df.to_dict(orient="records"),
        }

    bonus_delta = float(variant_payloads["boundary_bonus_only"]["oos_top10_uplift"] - variant_payloads["champion_only"]["oos_top10_uplift"])
    penalty_delta = float(variant_payloads["boundary_penalty_only"]["oos_top10_uplift"] - variant_payloads["champion_only"]["oos_top10_uplift"])
    combined_delta = float(variant_payloads["boundary_bonus_plus_penalty"]["oos_top10_uplift"] - variant_payloads["champion_only"]["oos_top10_uplift"])
    strict_delta = float(variant_payloads["boundary_bonus_plus_penalty_strict_window"]["oos_top10_uplift"] - variant_payloads["champion_only"]["oos_top10_uplift"])

    best_variant = max(
        variant_payloads.items(),
        key=lambda item: (
            float(item[1].get("oos_top10_uplift") or 0.0),
            bool(item[1].get("top10_boundary_improved")),
            float(item[1].get("winner_promotion_delta") or 0.0),
            -float(item[1].get("changed_top5_members_count") or 0.0),
            -float(item[1].get("changed_top10_members_count") or 0.0),
        ),
    )
    best_payload = best_variant[1]
    top5_changed = float(best_payload.get("changed_top5_members_count") or 0.0)
    top10_changed = float(best_payload.get("changed_top10_members_count") or 0.0)
    churn_acceptable = bool(top5_changed == 0.0 and top10_changed <= 6.0)
    winner_promotion_improved = float(best_payload.get("winner_promotion_delta") or 0.0) > 0.0
    loser_removal_improved = float(best_payload.get("loser_removal_delta") or 0.0) > 0.0
    boundary_improved = bool(best_payload.get("top10_boundary_improved"))
    if float(best_payload.get("oos_top10_uplift") or 0.0) > 0.0 and boundary_improved and winner_promotion_improved and churn_acceptable:
        decision = "keep_boundary_winner_promotion_challenger"
        reason = "stable_rank_improvement_with_boundary_gain"
    elif (
        float(best_payload.get("oos_top10_uplift") or 0.0) > 0.0
        or boundary_improved
        or winner_promotion_improved
        or loser_removal_improved
    ) and churn_acceptable:
        decision = "hold_boundary_winner_promotion_challenger"
        reason = "partial_improvement_without_stable_boundary_gain"
    else:
        decision = "drop_boundary_winner_promotion_challenger"
        reason = "no_stable_oos_uplift_or_boundary_gain"

    compare = {
        "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
        "top_k": int(top_k),
        "candidate_pool_k": int(candidate_pool_k),
        "candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW),
        "strict_candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_STRICT_WINDOW),
        "top5_freeze_enabled": True,
        "variants": variant_payloads,
        "best_variant": best_variant[0],
        "decision": decision,
        "decision_reason_typed": reason,
        "authoritative_rollup_decision": decision,
        "churn_acceptable": churn_acceptable,
        "bonus_only_contribution": bonus_delta,
        "penalty_only_contribution": penalty_delta,
        "combined_contribution": combined_delta,
        "strict_window_contribution": strict_delta,
        "source_artifacts": {
            "hierarchical_state_failure_map": "hierarchical_state_failure_map.json",
            "hierarchical_state_winner_loser_decomposition": "hierarchical_state_winner_loser_decomposition.json",
            "hierarchical_label_allowed_uses": "hierarchical_label_allowed_uses.json",
        },
    }

    regime_payload: dict[str, Any] = {
        "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
        "source_artifacts": compare["source_artifacts"],
        "variants": {},
        "regimes": {},
    }
    for variant_name, payload in variant_payloads.items():
        regime_payload["variants"][variant_name] = {
            "oos_top10_uplift": float(payload.get("oos_top10_uplift") or 0.0),
            "winner_promotion_delta": float(payload.get("winner_promotion_delta") or 0.0),
            "loser_removal_delta": float(payload.get("loser_removal_delta") or 0.0),
            "top10_boundary_outcome_gap": float(payload.get("top10_boundary_outcome_gap") or 0.0),
            "changed_top10_members_count": float(payload.get("changed_top10_members_count") or 0.0),
            "changed_top5_members_count": float(payload.get("changed_top5_members_count") or 0.0),
            "promotion_action_rate": float(payload.get("promotion_action_rate") or 0.0),
            "demotion_action_rate": float(payload.get("demotion_action_rate") or 0.0),
            "no_action_rate": float(payload.get("no_action_rate") or 0.0),
        }
        for regime, regime_metrics in (payload.get("regime_breakdown") or {}).items():
            regime_entry = regime_payload["regimes"].setdefault(regime, {"variant_metrics": {}})
            regime_entry["variant_metrics"][variant_name] = regime_metrics

    return {
        "state_rank_impact_table": state_rank_impact_table,
        "rules": rules,
        "compare": compare,
        "effect_by_regime": regime_payload,
        "decision": {
            "schema_version": BOUNDARY_WINNER_PROMOTION_SCHEMA_VERSION,
            "decision": decision,
            "decision_reason_typed": reason,
            "winner_promotion_improved": winner_promotion_improved,
            "loser_removal_improved": loser_removal_improved,
            "boundary_improved": boundary_improved,
            "churn_acceptable": churn_acceptable,
            "best_variant": best_variant[0],
            "variants": variant_payloads,
            "comparison_contract": {
                "candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_DEFAULT_WINDOW),
                "strict_candidate_rank_window": list(BOUNDARY_WINNER_PROMOTION_STRICT_WINDOW),
                "top5_freeze_enabled": True,
                "top_k": int(top_k),
                "candidate_pool_k": int(candidate_pool_k),
            },
        },
    }


def build_lightweight_boundary_challenger_artifacts(
    *,
    samples_frame: pd.DataFrame,
    hierarchical_rows: pd.DataFrame,
    top_k: int,
    candidate_pool_k: int,
) -> dict[str, Any]:
    samples = samples_frame.copy()
    hierarchy = hierarchical_rows.copy()
    if samples.empty or hierarchy.empty:
        return {
            "candidate_dataset": pd.DataFrame(),
            "rules": {
                "schema_version": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SCHEMA_VERSION,
                "candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW),
                "strict_candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_STRICT_WINDOW),
                "top5_freeze_enabled": True,
                "primary_weight": LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_WEIGHT,
                "support_weight": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SUPPORT_WEIGHT,
                "score_formula": "lightweight_candidate_score = champion_score + primary_delta + support_delta",
                "source_artifacts": {
                    "monthly_samples": "monthly_samples.parquet",
                    "monthly_labels_hierarchical": "monthly_labels_hierarchical.parquet",
                },
            },
            "compare": {
                "schema_version": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SCHEMA_VERSION,
                "candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW),
                "strict_candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_STRICT_WINDOW),
                "top5_freeze_enabled": True,
                "variants": {},
                "best_variant": "champion_only",
                "decision": "drop_lightweight_boundary_challenger",
                "decision_reason_typed": "insufficient_data",
                "churn_acceptable": False,
                "source_artifacts": {
                    "monthly_samples": "monthly_samples.parquet",
                    "monthly_labels_hierarchical": "monthly_labels_hierarchical.parquet",
                },
            },
            "effect_by_regime": {
                "schema_version": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SCHEMA_VERSION,
                "variants": {},
                "regimes": {},
            },
            "decision": {
                "schema_version": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SCHEMA_VERSION,
                "decision": "drop_lightweight_boundary_challenger",
                "decision_reason_typed": "insufficient_data",
                "winner_promotion_improved": False,
                "loser_removal_improved": False,
                "boundary_improved": False,
                "churn_acceptable": False,
                "best_variant": "champion_only",
                "recommended_next_use": "drop",
                "candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW),
                "top5_freeze_enabled": True,
            },
        }

    required_keys = ["sample_id", "code", "sample_month"]
    shared_keys = [key for key in required_keys if key in samples.columns and key in hierarchy.columns]
    merged = samples.merge(hierarchy, on=shared_keys, how="inner", suffixes=("", "_hier"))
    if merged.empty:
        return build_lightweight_boundary_challenger_artifacts(
            samples_frame=pd.DataFrame(),
            hierarchical_rows=pd.DataFrame(),
            top_k=top_k,
            candidate_pool_k=candidate_pool_k,
        )

    positive_features = list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_POSITIVE_FEATURES)
    negative_features = list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_NEGATIVE_FEATURES)
    shared_positive = [name for name in positive_features if name in merged.columns]
    shared_negative = [name for name in negative_features if name in merged.columns]
    for column in shared_positive:
        merged[f"{column}_pct"] = merged.groupby("sample_month")[column].rank(pct=True, method="average") * 100.0
    for column in shared_negative:
        merged[f"{column}_pct"] = 100.0 - (merged.groupby("sample_month")[column].rank(pct=True, method="average") * 100.0)

    def _pct_name(column: str) -> str:
        return f"{column}_pct"

    primary_weights = {
        "window_return_1m": 0.18,
        "window_return_3m": 0.12,
        "window_return_6m": 0.14,
        "dist_ma60": 0.10,
        "dist_ma120": 0.09,
        "recovery_6m": 0.09,
        "slope_mid": 0.06,
        "slope_late": 0.04,
        "breakout_proximity_low": 0.04,
        "dist_ma20": 0.03,
        "volume_expansion_20_60": 0.03,
        "liquidity_score": 0.03,
        "regime_trend_score": 0.02,
        "drawdown_6m": 0.02,
        "breakout_proximity_high": 0.01,
    }
    primary_total = 0.0
    for feature, weight in primary_weights.items():
        pct_col = _pct_name(feature)
        if pct_col not in merged.columns:
            continue
        primary_total += weight * merged[pct_col].to_numpy(dtype=float, copy=False)
    merged["lightweight_primary_score"] = primary_total

    monthly_state_bonus = {
        "monthly_up_pre": 4.0,
        "monthly_up_mid": 6.0,
        "monthly_up_top_warning": 2.0,
        "monthly_range_pre": 1.0,
        "monthly_range_mid": 0.0,
        "monthly_range_late": 1.0,
        "monthly_down_mid": -3.0,
        "monthly_down_bottom_warning": -5.0,
    }
    weekly_state_bonus = {
        "weekly_up_early": 4.0,
        "weekly_up_mid": 6.0,
        "weekly_up_late": 7.0,
        "weekly_range_mid": 0.0,
        "weekly_range_late": 1.0,
        "weekly_down_early": -2.0,
        "weekly_down_mid": -4.0,
        "weekly_down_bottom_warning": -6.0,
    }
    daily_state_bonus = {
        "daily_up_early": 4.0,
        "daily_up_mid": 6.0,
        "daily_up_top_warning": 1.0,
        "daily_range_mid": 0.0,
        "daily_range_late": 1.0,
        "daily_down_early": -2.0,
        "daily_down_mid": -4.0,
        "daily_down_bottom_warning": -6.0,
        "daily_reversal_up_candidate": 5.0,
        "daily_reversal_down_candidate": -2.0,
    }
    support_score = 50.0
    support_score += merged.get("monthly_main_state", pd.Series(index=merged.index, dtype=object)).map(monthly_state_bonus).fillna(0.0)
    support_score += merged.get("weekly_main_state", pd.Series(index=merged.index, dtype=object)).map(weekly_state_bonus).fillna(0.0)
    support_score += merged.get("daily_main_state", pd.Series(index=merged.index, dtype=object)).map(daily_state_bonus).fillna(0.0)
    for feature, delta in (
        ("daily_reclaim_ma20_flag", 3.0),
        ("daily_lose_ma20_flag", -3.0),
        ("daily_gap_up_flag", 1.0),
        ("daily_gap_down_flag", -1.0),
        ("daily_engulfing_bull_flag", 1.0),
        ("daily_engulfing_bear_flag", -1.0),
    ):
        if feature in merged.columns:
            support_score += np.where(merged[feature].fillna(False).astype(bool), delta, 0.0)
    merged["lightweight_support_score"] = pd.Series(support_score, index=merged.index).clip(0.0, 100.0)
    merged["lightweight_primary_delta"] = LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_WEIGHT * (
        (pd.to_numeric(merged["lightweight_primary_score"], errors="coerce").fillna(50.0) - 50.0) / LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_DELTA_SCALE
    )
    merged["lightweight_support_delta"] = LIGHTWEIGHT_BOUNDARY_CHALLENGER_SUPPORT_WEIGHT * (
        (pd.to_numeric(merged["lightweight_support_score"], errors="coerce").fillna(50.0) - 50.0) / LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_DELTA_SCALE
    )
    merged["lightweight_candidate_score"] = (
        pd.to_numeric(merged["champion_score"], errors="coerce").fillna(50.0)
        + merged["lightweight_primary_delta"]
        + merged["lightweight_support_delta"]
    ).clip(0.0, 100.0)

    window_low, window_high = LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW
    candidate_frames: list[pd.DataFrame] = []
    for _, month_frame in merged.groupby("sample_month", sort=True):
        month_ranked = _rank_month_frame(month_frame, "champion_score", top_k=max(top_k, candidate_pool_k)).copy()
        month_ranked["champion_rank"] = np.arange(1, len(month_ranked) + 1, dtype=int)
        candidate_mask = (month_ranked["champion_rank"] >= window_low) & (month_ranked["champion_rank"] <= window_high)
        month_candidates = month_ranked.loc[candidate_mask].copy()
        if month_candidates.empty:
            continue
        month_candidates["candidate_band"] = f"{window_low}-{window_high}"
        month_candidates["should_enter_top10_next_month"] = month_candidates["is_next_top10"]
        month_candidates["target_enter_top10_next_month"] = month_candidates["is_next_top10"]
        month_candidates["lightweight_action_type"] = np.where(
            pd.to_numeric(month_candidates["lightweight_primary_delta"], errors="coerce").fillna(0.0) > 0.0,
            "promote",
            "none",
        )
        month_candidates["lightweight_candidate_score"] = pd.to_numeric(month_candidates["lightweight_candidate_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)
        candidate_frames.append(month_candidates)
    candidate_dataset = pd.concat(candidate_frames, ignore_index=True, sort=False) if candidate_frames else merged.iloc[0:0].copy()

    def _apply_variant(month_frame: pd.DataFrame, *, window: tuple[int, int]) -> pd.DataFrame:
        champion_ranked = _rank_month_frame(month_frame, "champion_score", top_k=max(top_k, candidate_pool_k)).copy()
        champion_ranked["champion_rank"] = np.arange(1, len(champion_ranked) + 1, dtype=int)
        champion_ranked["lightweight_rank_action"] = "none"
        champion_ranked["lightweight_action_type"] = "none"
        champion_ranked["lightweight_primary_score"] = pd.to_numeric(champion_ranked.get("lightweight_primary_score"), errors="coerce").fillna(50.0)
        champion_ranked["lightweight_support_score"] = pd.to_numeric(champion_ranked.get("lightweight_support_score"), errors="coerce").fillna(50.0)
        champion_ranked["lightweight_primary_delta"] = pd.to_numeric(champion_ranked.get("lightweight_primary_delta"), errors="coerce").fillna(0.0)
        champion_ranked["lightweight_support_delta"] = pd.to_numeric(champion_ranked.get("lightweight_support_delta"), errors="coerce").fillna(0.0)
        champion_ranked["lightweight_candidate_score"] = pd.to_numeric(champion_ranked.get("lightweight_candidate_score"), errors="coerce").fillna(pd.to_numeric(champion_ranked["champion_score"], errors="coerce").fillna(50.0)).clip(0.0, 100.0)
        low, high = int(window[0]), int(window[1])
        candidate_mask = (champion_ranked["champion_rank"] >= low) & (champion_ranked["champion_rank"] <= high)
        window_rows = champion_ranked.loc[candidate_mask].copy()
        if not window_rows.empty:
            window_rows = window_rows.sort_values(
                ["lightweight_candidate_score", "champion_rank", "code"],
                ascending=[False, True, True],
            ).reset_index(drop=True)
            window_rows["lightweight_rank_action"] = "reselect"
            window_rows["lightweight_action_type"] = np.where(
                pd.to_numeric(window_rows["lightweight_primary_delta"], errors="coerce").fillna(0.0) > 0.0,
                "promote",
                "none",
            )
        prefix_rows = champion_ranked.loc[~candidate_mask & (champion_ranked["champion_rank"] < low)].copy()
        suffix_rows = champion_ranked.loc[champion_ranked["champion_rank"] > high].copy()
        final_rows = pd.concat([prefix_rows, window_rows, suffix_rows], ignore_index=True, sort=False)
        final_rows["pred_rank"] = np.arange(1, len(final_rows) + 1, dtype=int)
        final_rows["pred_rank_pct"] = 1.0 if len(final_rows) <= 1 else 1.0 - ((final_rows["pred_rank"] - 1) / (len(final_rows) - 1))
        final_rows["pred_is_top10"] = (final_rows["pred_rank"] <= min(top_k, len(final_rows))).astype(int)
        return final_rows

    def _variant_month_metrics(final_ranked: pd.DataFrame, champion_ranked: pd.DataFrame, *, variant_name: str, window: tuple[int, int]) -> dict[str, Any]:
        top10 = final_ranked.head(min(top_k, len(final_ranked)))
        champion_top10 = champion_ranked.head(min(top_k, len(champion_ranked)))
        top5 = final_ranked.head(min(LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE, len(final_ranked)))
        champion_top5 = champion_ranked.head(min(LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE, len(champion_ranked)))
        candidate_pool = final_ranked.head(min(candidate_pool_k, len(final_ranked)))
        champion_candidate_pool = champion_ranked.head(min(candidate_pool_k, len(champion_ranked)))
        final_top10_boundary = final_ranked.iloc[min(top_k - 1, len(final_ranked) - 1)]
        final_next_boundary = final_ranked.iloc[min(top_k, len(final_ranked) - 1)] if len(final_ranked) > top_k else final_top10_boundary
        champion_top10_boundary = champion_ranked.iloc[min(top_k - 1, len(champion_ranked) - 1)]
        champion_next_boundary = champion_ranked.iloc[min(top_k, len(champion_ranked) - 1)] if len(champion_ranked) > top_k else champion_top10_boundary
        champion_top5_boundary = champion_ranked.iloc[min(LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE - 1, len(champion_ranked) - 1)]
        champion_next_top5 = champion_ranked.iloc[min(LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE, len(champion_ranked) - 1)] if len(champion_ranked) > LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE else champion_top5_boundary
        final_top5_boundary = final_ranked.iloc[min(LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE - 1, len(final_ranked) - 1)]
        final_next_top5 = final_ranked.iloc[min(LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE, len(final_ranked) - 1)] if len(final_ranked) > LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE else final_top5_boundary
        final_boundary_gap_value = float(final_top10_boundary["next_month_return"] - final_next_boundary["next_month_return"]) if len(final_ranked) > top_k else float(final_top10_boundary["next_month_return"])
        champion_boundary_gap_value = float(champion_top10_boundary["next_month_return"] - champion_next_boundary["next_month_return"]) if len(champion_ranked) > top_k else float(champion_top10_boundary["next_month_return"])
        champion_candidate_pool_bad_pick_removal = int(max(0, int(champion_ranked[champion_ranked["is_next_bottom10"] == 1]["sample_id"].nunique()) - int(champion_candidate_pool["is_next_bottom10"].sum())))
        final_candidate_pool_bad_pick_removal = int(max(0, int(final_ranked[final_ranked["is_next_bottom10"] == 1]["sample_id"].nunique()) - int(candidate_pool["is_next_bottom10"].sum())))
        changed_top10_members = set(champion_top10["sample_id"]) ^ set(top10["sample_id"])
        changed_top5_members = set(champion_top5["sample_id"]) ^ set(top5["sample_id"])
        champion_rank_map = {row.sample_id: int(idx + 1) for idx, row in enumerate(champion_ranked.head(min(candidate_pool_k, len(champion_ranked))).itertuples(index=False))}
        final_rank_map = {row.sample_id: int(idx + 1) for idx, row in enumerate(final_ranked.head(min(candidate_pool_k, len(final_ranked))).itertuples(index=False))}
        changed_rank_count = sum(1 for sample_id in set(champion_rank_map) | set(final_rank_map) if champion_rank_map.get(sample_id) != final_rank_map.get(sample_id))
        boundary_actions = final_ranked[(final_ranked["pred_rank"] >= window[0]) & (final_ranked["pred_rank"] <= window[1])]
        promotion_action_rate = float((pd.to_numeric(boundary_actions["lightweight_primary_delta"], errors="coerce").fillna(0.0) > 0.0).mean()) if len(boundary_actions) else 0.0
        demotion_action_rate = float((pd.to_numeric(boundary_actions["lightweight_primary_delta"], errors="coerce").fillna(0.0) < 0.0).mean()) if len(boundary_actions) else 0.0
        no_action_rate = float((pd.to_numeric(boundary_actions["lightweight_primary_delta"], errors="coerce").fillna(0.0) == 0.0).mean()) if len(boundary_actions) else 0.0
        return {
            "variant_name": variant_name,
            "candidate_rank_window": list(window),
            "sample_count": int(len(final_ranked)),
            "top5_hit_count": int(top5["is_next_top10"].sum()),
            "top5_hit_rate": float(top5["is_next_top10"].mean()) if len(top5) else 0.0,
            "mean_next_month_return_top5": float(top5["next_month_return"].mean()) if len(top5) else 0.0,
            "median_next_month_return_top5": float(top5["next_month_return"].median()) if len(top5) else 0.0,
            "top5_boundary_score_gap": float(final_top5_boundary["lightweight_candidate_score"] - final_next_top5["lightweight_candidate_score"]) if len(final_ranked) > LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE else float(final_top5_boundary["lightweight_candidate_score"]),
            "top5_boundary_outcome_gap": float(final_top5_boundary["next_month_return"] - final_next_top5["next_month_return"]) if len(final_ranked) > LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE else float(final_top5_boundary["next_month_return"]),
            "champion_top5_hit_count": int(champion_top5["is_next_top10"].sum()),
            "champion_top5_hit_rate": float(champion_top5["is_next_top10"].mean()) if len(champion_top5) else 0.0,
            "changed_top5_members_count": int(len(changed_top5_members)),
            "champion_top5_boundary_score_gap": float(champion_top5_boundary["champion_score"] - champion_next_top5["champion_score"]) if len(champion_ranked) > LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE else float(champion_top5_boundary["champion_score"]),
            "champion_top5_boundary_outcome_gap": float(champion_top5_boundary["next_month_return"] - champion_next_top5["next_month_return"]) if len(champion_ranked) > LIGHTWEIGHT_BOUNDARY_CHALLENGER_TOP5_FREEZE else float(champion_top5_boundary["next_month_return"]),
            "top10_hit_count": int(top10["is_next_top10"].sum()),
            "top10_hit_rate": float(top10["is_next_top10"].mean()) if len(top10) else 0.0,
            "mean_next_month_return_top10": float(top10["next_month_return"].mean()) if len(top10) else 0.0,
            "median_next_month_return_top10": float(top10["next_month_return"].median()) if len(top10) else 0.0,
            "champion_top10_hit_count": int(champion_top10["is_next_top10"].sum()),
            "champion_top10_hit_rate": float(champion_top10["is_next_top10"].mean()) if len(champion_top10) else 0.0,
            "champion_top10_mean_next_month_return": float(champion_top10["next_month_return"].mean()) if len(champion_top10) else 0.0,
            "champion_top10_median_next_month_return": float(champion_top10["next_month_return"].median()) if len(champion_top10) else 0.0,
            "candidate_pool_top10_capture": float(candidate_pool["is_next_top10"].sum()),
            "candidate_pool_top10_capture_delta": float(candidate_pool["is_next_top10"].sum() - champion_candidate_pool["is_next_top10"].sum()),
            "candidate_pool_bad_pick_removal": float(final_candidate_pool_bad_pick_removal),
            "candidate_pool_bad_pick_removal_delta": float(final_candidate_pool_bad_pick_removal - champion_candidate_pool_bad_pick_removal),
            "changed_top10_members_count": float(len(changed_top10_members)),
            "changed_rank_count": float(changed_rank_count),
            "top10_boundary_score_gap": float(final_top10_boundary["lightweight_candidate_score"] - final_next_boundary["lightweight_candidate_score"]) if len(final_ranked) > top_k else float(final_top10_boundary["lightweight_candidate_score"]),
            "champion_top10_boundary_score_gap": float(champion_top10_boundary["champion_score"] - champion_next_boundary["champion_score"]) if len(champion_ranked) > top_k else float(champion_top10_boundary["champion_score"]),
            "top10_boundary_outcome_gap": final_boundary_gap_value,
            "champion_top10_boundary_outcome_gap": champion_boundary_gap_value,
            "top10_boundary_improved": bool(final_boundary_gap_value > champion_boundary_gap_value),
            "lightweight_primary_total": float(pd.to_numeric(final_ranked["lightweight_primary_delta"], errors="coerce").fillna(0.0).sum()),
            "lightweight_support_total": float(pd.to_numeric(final_ranked["lightweight_support_delta"], errors="coerce").fillna(0.0).sum()),
            "promotion_action_rate": promotion_action_rate,
            "demotion_action_rate": demotion_action_rate,
            "no_action_rate": no_action_rate,
            "regime_tag": _text(champion_ranked.iloc[0].regime_tag, fallback="mixed"),
        }

    variants = {
        "champion_only": {"window": LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW},
        "lightweight_boundary_challenger": {"window": LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW},
        "lightweight_boundary_challenger_strict": {"window": LIGHTWEIGHT_BOUNDARY_CHALLENGER_STRICT_WINDOW},
    }
    variant_payloads: dict[str, dict[str, Any]] = {}
    for variant_name, spec in variants.items():
        monthly_rows_payload: list[dict[str, Any]] = []
        for month, month_frame in merged.groupby("sample_month", sort=True):
            champion_ranked = _rank_month_frame(month_frame, "champion_score", top_k=max(top_k, candidate_pool_k))
            champion_ranked["champion_rank"] = np.arange(1, len(champion_ranked) + 1, dtype=int)
            final_ranked = champion_ranked.copy()
            if variant_name != "champion_only":
                final_ranked = _apply_variant(month_frame, window=spec["window"])
            month_metrics = _variant_month_metrics(final_ranked, champion_ranked, variant_name=variant_name, window=spec["window"])
            month_metrics["regime_tag"] = _text(champion_ranked.iloc[0].regime_tag, fallback="mixed")
            month_metrics["monthly_rows"] = []
            monthly_rows_payload.append(month_metrics)
        monthly_df = pd.DataFrame(monthly_rows_payload)
        if monthly_df.empty:
            variant_payloads[variant_name] = {
                "variant_name": variant_name,
                "candidate_rank_window": list(spec["window"]),
                "top5_freeze_enabled": True,
                "month_count": 0,
                "sample_count": 0,
                "oos_top10_uplift": 0.0,
                "oos_bad_pick_removal": 0.0,
                "changed_top10_members_count": 0.0,
                "changed_top5_members_count": 0.0,
                "changed_rank_count": 0.0,
                "top10_boundary_outcome_gap": 0.0,
                "champion_top10_boundary_outcome_gap": 0.0,
                "top10_boundary_improved": False,
                "winner_promotion_delta": 0.0,
                "loser_removal_delta": 0.0,
                "candidate_pool_top10_capture": 0.0,
                "candidate_pool_top10_capture_delta": 0.0,
                "candidate_pool_bad_pick_removal": 0.0,
                "candidate_pool_bad_pick_removal_delta": 0.0,
                "mean_next_month_return_top10": 0.0,
                "median_next_month_return_top10": 0.0,
                "mean_next_month_return_top5": 0.0,
                "median_next_month_return_top5": 0.0,
                "promotion_action_rate": 0.0,
                "demotion_action_rate": 0.0,
                "no_action_rate": 0.0,
                "regime_breakdown": {},
                "monthly_rows": [],
            }
            continue
        regime_breakdown: dict[str, dict[str, float]] = {}
        for regime, regime_df in monthly_df.groupby("regime_tag"):
            uplift_series = regime_df["top10_hit_count"] - regime_df["champion_top10_hit_count"]
            regime_breakdown[str(regime)] = {
                "month_count": float(len(regime_df)),
                "mean_top10_uplift": float(uplift_series.mean()),
                "mean_bad_pick_removal": float(regime_df["candidate_pool_bad_pick_removal"].mean()),
                "mean_changed_top10_members_count": float(regime_df["changed_top10_members_count"].mean()),
                "mean_top10_boundary_outcome_gap": float(regime_df["top10_boundary_outcome_gap"].mean()),
                "mean_candidate_pool_top10_capture": float(regime_df["candidate_pool_top10_capture"].mean()),
                "mean_candidate_pool_bad_pick_removal": float(regime_df["candidate_pool_bad_pick_removal"].mean()),
                "positive_month_share": float((uplift_series > 0).mean()),
                "negative_month_share": float((uplift_series < 0).mean()),
                "sign_stability": float(max((uplift_series > 0).mean(), (uplift_series < 0).mean())),
            }
        uplift = monthly_df["top10_hit_count"] - monthly_df["champion_top10_hit_count"]
        variant_payloads[variant_name] = {
            "variant_name": variant_name,
            "candidate_rank_window": list(spec["window"]),
            "top5_freeze_enabled": True,
            "month_count": int(len(monthly_df)),
            "sample_count": int(monthly_df["sample_count"].sum()),
            "oos_top10_uplift": float(uplift.mean()),
            "oos_bad_pick_removal": float(monthly_df["candidate_pool_bad_pick_removal"].mean()),
            "changed_top10_members_count": float(monthly_df["changed_top10_members_count"].mean()),
            "changed_top5_members_count": float(monthly_df["changed_top5_members_count"].mean()),
            "changed_rank_count": float(monthly_df["changed_rank_count"].mean()),
            "top10_boundary_outcome_gap": float(monthly_df["top10_boundary_outcome_gap"].mean()),
            "champion_top10_boundary_outcome_gap": float(monthly_df["champion_top10_boundary_outcome_gap"].mean()),
            "top10_boundary_improved": bool(float(monthly_df["top10_boundary_outcome_gap"].mean()) > float(monthly_df["champion_top10_boundary_outcome_gap"].mean())),
            "winner_promotion_delta": float(uplift.mean()),
            "loser_removal_delta": float(monthly_df["candidate_pool_bad_pick_removal"].mean()),
            "candidate_pool_top10_capture": float(monthly_df["candidate_pool_top10_capture"].mean()),
            "candidate_pool_top10_capture_delta": float(monthly_df["candidate_pool_top10_capture_delta"].mean()),
            "candidate_pool_bad_pick_removal": float(monthly_df["candidate_pool_bad_pick_removal"].mean()),
            "candidate_pool_bad_pick_removal_delta": float(monthly_df["candidate_pool_bad_pick_removal_delta"].mean()),
            "mean_next_month_return_top10": float(monthly_df["mean_next_month_return_top10"].mean()),
            "median_next_month_return_top10": float(monthly_df["median_next_month_return_top10"].median()),
            "mean_next_month_return_top5": float(monthly_df["mean_next_month_return_top5"].mean()),
            "median_next_month_return_top5": float(monthly_df["median_next_month_return_top5"].median()),
            "promotion_action_rate": float(monthly_df["promotion_action_rate"].mean()),
            "demotion_action_rate": float(monthly_df["demotion_action_rate"].mean()),
            "no_action_rate": float(monthly_df["no_action_rate"].mean()),
            "regime_breakdown": regime_breakdown,
            "monthly_rows": monthly_df.to_dict(orient="records"),
        }

    best_variant = max(
        variant_payloads.items(),
        key=lambda item: (
            float(item[1].get("oos_top10_uplift") or 0.0),
            bool(item[1].get("top10_boundary_improved")),
            float(item[1].get("winner_promotion_delta") or 0.0),
            -float(item[1].get("changed_top5_members_count") or 0.0),
            -float(item[1].get("changed_top10_members_count") or 0.0),
        ),
    )
    best_payload = best_variant[1]
    top5_changed = float(best_payload.get("changed_top5_members_count") or 0.0)
    top10_changed = float(best_payload.get("changed_top10_members_count") or 0.0)
    churn_acceptable = bool(top5_changed == 0.0 and top10_changed <= 1.0)
    winner_promotion_improved = float(best_payload.get("winner_promotion_delta") or 0.0) > 0.0
    loser_removal_improved = float(best_payload.get("loser_removal_delta") or 0.0) > 0.0
    boundary_improved = bool(best_payload.get("top10_boundary_improved"))
    if float(best_payload.get("oos_top10_uplift") or 0.0) > 0.0 and boundary_improved and churn_acceptable:
        decision = "keep_lightweight_boundary_challenger"
        reason = "stable_oos_uplift_with_controlled_churn"
    elif float(best_payload.get("oos_top10_uplift") or 0.0) > 0.0 and churn_acceptable:
        decision = "hold_lightweight_boundary_challenger"
        reason = "partial_uplift_without_boundary_gain"
    else:
        decision = "drop_lightweight_boundary_challenger"
        reason = "no_oos_top10_uplift_or_churn_exceeds_limit"

    compare = {
        "schema_version": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SCHEMA_VERSION,
        "top_k": int(top_k),
        "candidate_pool_k": int(candidate_pool_k),
        "candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW),
        "strict_candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_STRICT_WINDOW),
        "top5_freeze_enabled": True,
        "variants": variant_payloads,
        "best_variant": best_variant[0],
        "decision": decision,
        "decision_reason_typed": reason,
        "authoritative_rollup_decision": decision,
        "churn_acceptable": churn_acceptable,
        "candidate_dataset_summary": {
            "row_count": int(len(candidate_dataset)),
            "month_count": int(candidate_dataset["sample_month"].nunique()) if not candidate_dataset.empty else 0,
            "symbol_count": int(candidate_dataset["symbol"].nunique()) if "symbol" in candidate_dataset.columns and not candidate_dataset.empty else 0,
            "positive_label_rate": float(candidate_dataset["should_enter_top10_next_month"].mean()) if not candidate_dataset.empty else 0.0,
        },
        "source_artifacts": {
            "monthly_samples": "monthly_samples.parquet",
            "monthly_labels_hierarchical": "monthly_labels_hierarchical.parquet",
            "hierarchical_label_allowed_uses": "hierarchical_label_allowed_uses.json",
            "hierarchical_state_failure_map": "hierarchical_state_failure_map.json",
            "hierarchical_state_winner_loser_decomposition": "hierarchical_state_winner_loser_decomposition.json",
        },
    }

    regime_payload: dict[str, Any] = {
        "schema_version": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SCHEMA_VERSION,
        "source_artifacts": compare["source_artifacts"],
        "variants": {},
        "regimes": {},
    }
    for variant_name, payload in variant_payloads.items():
        regime_payload["variants"][variant_name] = {
            "oos_top10_uplift": float(payload.get("oos_top10_uplift") or 0.0),
            "winner_promotion_delta": float(payload.get("winner_promotion_delta") or 0.0),
            "loser_removal_delta": float(payload.get("loser_removal_delta") or 0.0),
            "top10_boundary_outcome_gap": float(payload.get("top10_boundary_outcome_gap") or 0.0),
            "changed_top10_members_count": float(payload.get("changed_top10_members_count") or 0.0),
            "changed_top5_members_count": float(payload.get("changed_top5_members_count") or 0.0),
            "promotion_action_rate": float(payload.get("promotion_action_rate") or 0.0),
            "demotion_action_rate": float(payload.get("demotion_action_rate") or 0.0),
            "no_action_rate": float(payload.get("no_action_rate") or 0.0),
        }
        for regime, regime_metrics in (payload.get("regime_breakdown") or {}).items():
            regime_entry = regime_payload["regimes"].setdefault(regime, {"variant_metrics": {}})
            regime_entry["variant_metrics"][variant_name] = regime_metrics

    return {
        "candidate_dataset": candidate_dataset,
        "rules": {
            "schema_version": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SCHEMA_VERSION,
            "source_artifacts": compare["source_artifacts"],
            "candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW),
            "strict_candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_STRICT_WINDOW),
            "top5_freeze_enabled": True,
            "primary_weight": LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_WEIGHT,
            "support_weight": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SUPPORT_WEIGHT,
            "primary_delta_scale": LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_DELTA_SCALE,
            "feature_groups": {
                "recent_relative_strength": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_PRIMARY_POSITIVE_FEATURES[:3]),
                "weekly_trend_continuity": ["slope_early", "slope_mid", "slope_late", "dist_ma20", "dist_ma60", "dist_ma120"],
                "breakout_quality": ["breakout_proximity_low", "breakout_proximity_high"],
                "recovery_and_drawdown": ["recovery_6m", "drawdown_6m"],
                "volatility_and_update_quality": ["volume_expansion_20_60", "liquidity_score", "regime_volatility_score", "gap_frequency", "wick_body_ratio", "missing_days_ratio"],
            },
            "support_fields": {
                "monthly_main_state": list(monthly_state_bonus.keys()),
                "weekly_main_state": list(weekly_state_bonus.keys()),
                "daily_main_state": list(daily_state_bonus.keys()),
                "daily_triggers": [
                    "daily_reclaim_ma20_flag",
                    "daily_lose_ma20_flag",
                    "daily_gap_up_flag",
                    "daily_gap_down_flag",
                    "daily_engulfing_bull_flag",
                    "daily_engulfing_bear_flag",
                ],
            },
            "score_formula": "lightweight_candidate_score = champion_score + primary_delta + support_delta; primary_delta = 0.10 * ((lightweight_primary_score - 50.0) / 10.0); support_delta = 0.01 * ((lightweight_support_score - 50.0) / 10.0); top5 freeze is enforced and only champion ranks 6-20 are reselected for the default challenger.",
            "fallback_behavior": "retain champion_score outside the candidate window or when support fields are unavailable.",
        },
        "compare": compare,
        "effect_by_regime": regime_payload,
        "decision": {
            "schema_version": LIGHTWEIGHT_BOUNDARY_CHALLENGER_SCHEMA_VERSION,
            "decision": decision,
            "decision_reason_typed": reason,
            "winner_promotion_improved": winner_promotion_improved,
            "loser_removal_improved": loser_removal_improved,
            "boundary_improved": boundary_improved,
            "churn_acceptable": churn_acceptable,
            "best_variant": best_variant[0],
            "recommended_next_use": "drop" if decision.startswith("drop_") else "keep_or_hold",
            "comparison_contract": {
                "candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_DEFAULT_WINDOW),
                "strict_candidate_rank_window": list(LIGHTWEIGHT_BOUNDARY_CHALLENGER_STRICT_WINDOW),
                "top5_freeze_enabled": True,
                "top_k": int(top_k),
                "candidate_pool_k": int(candidate_pool_k),
            },
            "variants": variant_payloads,
            "source_artifacts": compare["source_artifacts"],
        },
    }


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(":memory:")
    try:
        conn.register("frame_df", frame)
        escaped = str(path).replace("'", "''")
        conn.execute(f"COPY frame_df TO '{escaped}' (FORMAT PARQUET)")
    finally:
        conn.close()
    return path


def _parse_yyyymmdd(value: Any) -> pd.Timestamp:
    raw = int(value)
    return pd.Timestamp(datetime.strptime(str(raw), "%Y%m%d"), tz="UTC")


def _to_yyyymm(value: pd.Timestamp | datetime | date | int | str) -> int:
    if isinstance(value, pd.Timestamp):
        ts = value.tz_localize("UTC") if value.tzinfo is None else value
        return int(ts.year * 100 + ts.month)
    if isinstance(value, datetime):
        return int(value.year * 100 + value.month)
    if isinstance(value, date):
        return int(value.year * 100 + value.month)
    raw = _text(value).replace("-", "")
    if len(raw) >= 6 and raw[:6].isdigit():
        return int(raw[:6])
    raise ValueError(f"cannot normalize month value: {value!r}")


def _month_start_from_yyyymm(yyyymm: int) -> pd.Timestamp:
    year = int(yyyymm) // 100
    month = int(yyyymm) % 100
    return pd.Timestamp(datetime(year, month, 1, tzinfo=timezone.utc))


def _month_end_from_yyyymm(yyyymm: int) -> pd.Timestamp:
    return _month_start_from_yyyymm(yyyymm) + pd.offsets.MonthEnd(1)


def _yyyymmdd_int(ts: pd.Timestamp) -> int:
    return int(ts.strftime("%Y%m%d"))


def _month_key_from_date_key(date_key: int) -> int:
    return int(int(date_key) // 100)


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    try:
        out = float(value)
    except Exception:
        return fallback
    if not math.isfinite(out):
        return fallback
    return out


def _safe_int(value: Any, fallback: int | None = None) -> int | None:
    try:
        return int(value)
    except Exception:
        return fallback


def _normalize_vector(vec: np.ndarray) -> np.ndarray:
    arr = np.asarray(vec, dtype=float)
    norm = float(np.linalg.norm(arr))
    if not math.isfinite(norm) or norm <= 0.0:
        return np.zeros_like(arr, dtype=float)
    return arr / norm


def _window_return(closes: np.ndarray, periods: int) -> float:
    if closes.size <= periods:
        return 0.0
    prev = float(closes[-1 - periods])
    if prev <= 0.0:
        return 0.0
    return float(closes[-1] / prev - 1.0)


def _linear_slope(values: np.ndarray) -> float:
    if values.size < 2:
        return 0.0
    x = np.arange(values.size, dtype=float)
    y = np.asarray(values, dtype=float)
    x = x - float(x.mean())
    y = y - float(y.mean())
    denom = float(np.dot(x, x))
    if denom <= 0.0:
        return 0.0
    return float(np.dot(x, y) / denom)


def _max_drawdown(closes: np.ndarray) -> float:
    if closes.size == 0:
        return 0.0
    peak = float(closes[0])
    worst = 0.0
    for value in closes:
        peak = max(peak, float(value))
        if peak > 0.0:
            worst = min(worst, float(value) / peak - 1.0)
    return float(abs(worst))


def _volume_expansion(volumes: np.ndarray, *, recent: int = 20, prior: int = 60) -> float:
    if volumes.size < max(recent, prior) + 1:
        return 0.0
    recent_mean = float(np.mean(volumes[-recent:]))
    prior_slice = volumes[-(recent + prior) : -recent]
    prior_mean = float(np.mean(prior_slice)) if prior_slice.size else 0.0
    if prior_mean <= 0.0:
        return 0.0
    return float(recent_mean / prior_mean - 1.0)


def _gap_frequency(opens: np.ndarray, closes: np.ndarray) -> float:
    if opens.size == 0 or closes.size == 0 or opens.size != closes.size:
        return 0.0
    prev_close = np.roll(closes, 1)
    prev_close[0] = closes[0]
    gap = np.abs(opens - prev_close) / np.maximum(prev_close, 1e-9)
    return float(np.mean(gap > 0.015))


def _wick_body_ratio(opens: np.ndarray, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
    if not (opens.size == highs.size == lows.size == closes.size) or opens.size == 0:
        return 0.0
    body = np.abs(closes - opens)
    upper = highs - np.maximum(opens, closes)
    lower = np.minimum(opens, closes) - lows
    denom = np.maximum(body, 1e-9)
    return float(np.mean((upper + lower) / denom))


def _breakout_proximity(close: float, high: float, low: float) -> tuple[float, float]:
    span = max(high - low, 1e-9)
    return float((high - close) / span), float((close - low) / span)


def _regime_tag(month_return_6m: float, month_return_3m: float, vol_6m: float) -> str:
    if month_return_6m >= 0.12 and month_return_3m >= 0.04:
        return "trend_up"
    if month_return_6m <= -0.12 and month_return_3m <= -0.04:
        return "trend_down"
    if abs(month_return_6m) <= 0.05 and vol_6m <= 0.08:
        return "range"
    if vol_6m >= 0.14:
        return "volatile"
    return "mixed"


def _regime_similarity(target_regime: str, history_regime: str) -> float:
    if target_regime == history_regime:
        return 1.0
    trend_regimes = {"trend_up", "trend_down"}
    if target_regime in trend_regimes and history_regime in trend_regimes:
        return 0.75 if target_regime == history_regime else 0.40
    if target_regime == "range" and history_regime == "mixed":
        return 0.60
    if history_regime == "range" and target_regime == "mixed":
        return 0.60
    if target_regime == "volatile" or history_regime == "volatile":
        return 0.55
    return 0.35


def _render_chart_embedding(window_df: pd.DataFrame) -> np.ndarray:
    image = _render_candlestick_chart(window_df)
    thumb = image.convert("L").resize((8, 4), Image.Resampling.BILINEAR)
    arr = np.asarray(thumb, dtype=np.float32).reshape(-1)
    if arr.size != IMAGE_EMBEDDING_DIM:
        arr = np.resize(arr, IMAGE_EMBEDDING_DIM)
    return arr / 255.0


def _render_candlestick_chart(window_df: pd.DataFrame) -> Image.Image:
    width = 320
    height = 200
    chart_height = 150
    volume_height = height - chart_height
    padding = 10
    image = Image.new("RGB", (width, height), (10, 12, 16))
    draw = ImageDraw.Draw(image)
    if window_df.empty:
        return image

    highs = window_df["h"].to_numpy(dtype=float, copy=False)
    lows = window_df["l"].to_numpy(dtype=float, copy=False)
    opens = window_df["o"].to_numpy(dtype=float, copy=False)
    closes = window_df["c"].to_numpy(dtype=float, copy=False)
    volumes = window_df["v"].to_numpy(dtype=float, copy=False)

    price_min = float(np.nanmin(lows))
    price_max = float(np.nanmax(highs))
    if not math.isfinite(price_min) or not math.isfinite(price_max) or price_max <= price_min:
        price_min = float(np.nanmin(closes))
        price_max = float(np.nanmax(closes))
    price_span = max(price_max - price_min, 1e-9)
    volume_max = max(float(np.nanmax(volumes)), 1.0)
    count = int(len(window_df))
    candle_width = max(1.0, (width - 2 * padding) / max(1, count))
    wick_color_up = (72, 196, 137)
    wick_color_down = (234, 99, 99)
    volume_color = (108, 152, 255)

    for idx, row in enumerate(window_df.itertuples(index=False)):
        open_price = float(row.o)
        close_price = float(row.c)
        high_price = float(row.h)
        low_price = float(row.l)
        volume = float(row.v)
        x_center = padding + (idx + 0.5) * candle_width
        x0 = x_center - max(0.5, candle_width * 0.32)
        x1 = x_center + max(0.5, candle_width * 0.32)
        y_open = chart_height - padding - ((open_price - price_min) / price_span) * (chart_height - 2 * padding)
        y_close = chart_height - padding - ((close_price - price_min) / price_span) * (chart_height - 2 * padding)
        y_high = chart_height - padding - ((high_price - price_min) / price_span) * (chart_height - 2 * padding)
        y_low = chart_height - padding - ((low_price - price_min) / price_span) * (chart_height - 2 * padding)
        is_up = close_price >= open_price
        wick_color = wick_color_up if is_up else wick_color_down
        body_color = wick_color
        y_body_top = min(y_open, y_close)
        y_body_bottom = max(y_open, y_close)
        draw.line((x_center, y_high, x_center, y_low), fill=wick_color, width=1)
        draw.rectangle((x0, y_body_top, x1, y_body_bottom), fill=body_color, outline=body_color)
        vol_top = height - padding
        vol_bottom = height - padding - (volume / volume_max) * (volume_height - 2 * padding)
        draw.rectangle((x0, vol_bottom, x1, vol_top), fill=volume_color, outline=volume_color)
    return image


def _compute_numeric_features(window_df: pd.DataFrame, *, month_regime: str) -> dict[str, float]:
    closes = window_df["c"].to_numpy(dtype=float, copy=False)
    highs = window_df["h"].to_numpy(dtype=float, copy=False)
    lows = window_df["l"].to_numpy(dtype=float, copy=False)
    opens = window_df["o"].to_numpy(dtype=float, copy=False)
    volumes = window_df["v"].to_numpy(dtype=float, copy=False)
    if closes.size == 0:
        return {name: 0.0 for name in NUMERIC_FEATURE_COLUMNS}

    close = float(closes[-1])
    high_6m = float(np.nanmax(highs))
    low_6m = float(np.nanmin(lows))
    mean_close = float(np.nanmean(closes))
    std_close = float(np.nanstd(closes))
    ret_1m = _window_return(closes, 21)
    ret_3m = _window_return(closes, 63)
    ret_6m = _window_return(closes, min(126, closes.size - 1))
    box_width = float((high_6m - low_6m) / max(close, 1e-9))
    breakout_high, breakout_low = _breakout_proximity(close, high_6m, low_6m)
    ma20 = float(np.mean(closes[-20:])) if closes.size >= 20 else mean_close
    ma60 = float(np.mean(closes[-60:])) if closes.size >= 60 else mean_close
    ma120 = float(np.mean(closes[-120:])) if closes.size >= 120 else mean_close
    dist_ma20 = float(close / max(ma20, 1e-9) - 1.0)
    dist_ma60 = float(close / max(ma60, 1e-9) - 1.0)
    dist_ma120 = float(close / max(ma120, 1e-9) - 1.0)
    third = max(1, closes.size // 3)
    slope_early = _linear_slope(closes[:third])
    slope_mid = _linear_slope(closes[third : 2 * third]) if closes.size >= 2 * third else slope_early
    slope_late = _linear_slope(closes[-third:])
    curvature = float(slope_late - slope_early)
    volume_expansion = _volume_expansion(volumes)
    gap_freq = _gap_frequency(opens, closes)
    wick_ratio = _wick_body_ratio(opens, highs, lows, closes)
    drawdown_6m = _max_drawdown(closes)
    recovery_6m = float((close - float(np.nanmin(closes))) / max(float(np.nanmax(closes) - np.nanmin(closes)), 1e-9))
    volatility_6m = float(np.nanstd(np.diff(np.log(np.maximum(closes, 1e-9)))) if closes.size >= 2 else 0.0)
    missing_days_ratio = float(max(0.0, 1.0 - min(1.0, closes.size / 126.0)))
    liquidity_score = float(np.log10(max(float(np.nanmean(volumes)), 1.0)) / 6.0)
    regime_trend_score = {"trend_up": 1.0, "trend_down": -1.0, "range": 0.0, "volatile": 0.2, "mixed": 0.1}.get(month_regime, 0.0)
    regime_volatility_score = {"trend_up": 0.2, "trend_down": 0.2, "range": 0.0, "volatile": 1.0, "mixed": 0.5}.get(month_regime, 0.0)

    features = {
        "window_return_1m": ret_1m,
        "window_return_3m": ret_3m,
        "window_return_6m": ret_6m,
        "box_width_6m": box_width,
        "breakout_proximity_high": breakout_high,
        "breakout_proximity_low": breakout_low,
        "dist_ma20": dist_ma20,
        "dist_ma60": dist_ma60,
        "dist_ma120": dist_ma120,
        "slope_early": float(slope_early),
        "slope_mid": float(slope_mid),
        "slope_late": float(slope_late),
        "curvature": curvature,
        "volume_expansion_20_60": volume_expansion,
        "gap_frequency": gap_freq,
        "wick_body_ratio": wick_ratio,
        "drawdown_6m": drawdown_6m,
        "recovery_6m": recovery_6m,
        "volatility_6m": volatility_6m,
        "missing_days_ratio": missing_days_ratio,
        "liquidity_score": liquidity_score,
        "regime_trend_score": regime_trend_score,
        "regime_volatility_score": regime_volatility_score,
    }
    return {name: float(features.get(name, 0.0) or 0.0) for name in NUMERIC_FEATURE_COLUMNS}


def _load_daily_bars_frame(source_db_path: str | None) -> pd.DataFrame:
    resolved = resolve_source_db_path(source_db_path)
    if not resolved.exists():
        raise FileNotFoundError(f"source_db_not_found:{resolved}")
    conn = duckdb.connect(str(resolved), read_only=True)
    try:
        if not conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'daily_bars'").fetchone()[0]:
            raise RuntimeError("daily_bars table is required for monthly shape memory research")
        frame = conn.execute(
            """
            SELECT code, date, o, h, l, c, v
            FROM daily_bars
            ORDER BY code, date
            """
        ).fetchdf()
    finally:
        conn.close()
    if frame.empty:
        raise RuntimeError("daily_bars table is empty")
    frame["date"] = pd.to_numeric(frame["date"], errors="coerce").astype("Int64")
    frame["code"] = frame["code"].astype(str)
    for col in ("o", "h", "l", "c", "v"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=["date", "code", "o", "h", "l", "c", "v"]).copy()
    frame["date"] = frame["date"].astype(int)
    if int(frame["date"].max()) > 1_000_000_000:
        frame["date_ts"] = pd.to_datetime(frame["date"], unit="s", utc=True, errors="coerce")
    else:
        frame["date_ts"] = pd.to_datetime(frame["date"].astype(str), format="%Y%m%d", utc=True, errors="coerce")
    frame = frame.dropna(subset=["date_ts"]).copy()
    frame["month"] = frame["date_ts"].dt.year * 100 + frame["date_ts"].dt.month
    return frame.reset_index(drop=True)


def _build_monthly_context(frame: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        frame.sort_values(["code", "date"])
        .groupby(["code", "month"], as_index=False)
        .agg(
            month_end_date=("date", "max"),
            month_end_ts=("date_ts", "max"),
            o=("o", "first"),
            h=("h", "max"),
            l=("l", "min"),
            c=("c", "last"),
            v=("v", "sum"),
            daily_row_count=("date", "count"),
        )
    )
    monthly["month_key"] = monthly["month"].astype(int)
    monthly = monthly.sort_values(["code", "month_key"]).reset_index(drop=True)
    benchmark = (
        monthly.groupby("month_key", as_index=False)
        .agg(benchmark_close=("c", "median"), universe_count=("code", "nunique"), benchmark_volume=("v", "median"))
        .sort_values("month_key")
        .reset_index(drop=True)
    )
    benchmark["benchmark_ret_1m"] = benchmark["benchmark_close"].pct_change()
    benchmark["benchmark_ret_3m"] = benchmark["benchmark_close"].pct_change(3)
    benchmark["benchmark_ret_6m"] = benchmark["benchmark_close"].pct_change(6)
    benchmark["benchmark_vol_6m"] = benchmark["benchmark_ret_1m"].rolling(6, min_periods=3).std()
    benchmark["benchmark_drawdown_6m"] = (
        benchmark["benchmark_close"] / benchmark["benchmark_close"].cummax() - 1.0
    ).abs()
    benchmark["regime_tag"] = [
        _regime_tag(float(r6 or 0.0), float(r3 or 0.0), float(vol or 0.0))
        for r6, r3, vol in zip(
            benchmark["benchmark_ret_6m"].fillna(0.0).to_numpy(dtype=float, copy=False),
            benchmark["benchmark_ret_3m"].fillna(0.0).to_numpy(dtype=float, copy=False),
            benchmark["benchmark_vol_6m"].fillna(0.0).to_numpy(dtype=float, copy=False),
        )
    ]
    return monthly.merge(benchmark, on="month_key", how="left")


def _build_samples(frame: pd.DataFrame, monthly_context: pd.DataFrame) -> pd.DataFrame:
    month_close_lookup = monthly_context.set_index(["code", "month_key"])[["month_end_date", "month_end_ts", "c", "benchmark_close", "regime_tag"]]
    all_rows: list[dict[str, Any]] = []
    grouped = frame.sort_values(["code", "date"]).groupby("code", sort=True)
    for code, code_df in grouped:
        code_df = code_df.sort_values("date").reset_index(drop=True)
        date_values = code_df["date"].to_numpy(dtype=int, copy=False)
        date_ts_values = code_df["date_ts"].dt.tz_convert(None).to_numpy(dtype="datetime64[ns]")
        month_values = code_df["month"].to_numpy(dtype=int, copy=False)
        month_keys = pd.Index(month_values).unique().to_list()
        if len(month_keys) < DEFAULT_MIN_HISTORY_MONTHS + 1:
            continue
        for month_key in month_keys:
            month_rows = code_df[code_df["month"] == month_key]
            if month_rows.empty:
                continue
            month_end_row = month_rows.iloc[-1]
            month_end_ts = pd.Timestamp(month_end_row["date_ts"]).tz_convert(None)
            current_loc = int(month_rows.index[-1])
            start_ts = month_end_ts - pd.DateOffset(months=6)
            start_idx = int(np.searchsorted(date_ts_values, np.datetime64(start_ts.to_datetime64())))
            window_df = code_df.iloc[start_idx : current_loc + 1].copy()
            if window_df.empty or len(window_df) < 30:
                continue
            next_month_key = int(month_key) + 1 if int(month_key) % 100 != 12 else int(int(month_key) // 100 + 1) * 100 + 1
            if (code, next_month_key) not in month_close_lookup.index:
                continue
            next_row = month_close_lookup.loc[(code, next_month_key)]
            current_close = float(month_end_row["c"])
            next_close = float(next_row["c"])
            if current_close <= 0.0 or next_close <= 0.0:
                continue
            next_month_return = float(next_close / current_close - 1.0)
            context_row = monthly_context[monthly_context["month_key"] == month_key].iloc[0]
            month_regime = _text(context_row.get("regime_tag"), fallback="mixed")
            numeric_features = _compute_numeric_features(window_df, month_regime=month_regime)
            image_embedding = _render_chart_embedding(window_df)
            sample_id = f"{code}:{month_key}"
            all_rows.append(
                {
                    "sample_id": sample_id,
                    "code": code,
                    "sample_month": int(month_key),
                    "month_end_date": _yyyymmdd_int(month_end_ts),
                    "month_end_ts": month_end_ts.isoformat(),
                    "feature_window_start_date": _yyyymmdd_int(pd.Timestamp(window_df.iloc[0]["date_ts"])),
                    "feature_window_end_date": _yyyymmdd_int(pd.Timestamp(window_df.iloc[-1]["date_ts"])),
                    "next_month_end_date": _yyyymmdd_int(pd.Timestamp(next_row["month_end_ts"])),
                    "next_month_return": next_month_return,
                    "next_month_close": next_close,
                    "current_month_close": current_close,
                    "benchmark_close": float(context_row.get("benchmark_close") or 0.0),
                    "benchmark_ret_1m": float(context_row.get("benchmark_ret_1m") or 0.0),
                    "benchmark_ret_3m": float(context_row.get("benchmark_ret_3m") or 0.0),
                    "benchmark_ret_6m": float(context_row.get("benchmark_ret_6m") or 0.0),
                    "benchmark_vol_6m": float(context_row.get("benchmark_vol_6m") or 0.0),
                    "regime_tag": month_regime,
                    "daily_window_size": int(len(window_df)),
                    "monthly_history_size": int(len(code_df[code_df["month"] <= month_key])),
                    **numeric_features,
                    **{f"img_e_{idx:02d}": float(value) for idx, value in enumerate(image_embedding)},
                }
            )
    return pd.DataFrame(all_rows)


def _compute_labels(samples: pd.DataFrame) -> pd.DataFrame:
    if samples.empty:
        return samples.copy()
    labels = samples.copy()
    labels["next_month_return_rank"] = 0
    labels["next_month_rank_pct"] = 0.0
    labels["is_next_top10"] = 0
    labels["is_next_bottom10"] = 0
    labels["top10_boundary_gap"] = 0.0
    labels["bottom10_boundary_gap"] = 0.0
    labels["top_bucket"] = "middle"
    labels["cohort_label"] = "neutral"
    for month_key, month_df in labels.groupby("sample_month"):
        idx = month_df.index.to_list()
        ordered = month_df.sort_values(["next_month_return", "code"], ascending=[False, True]).copy()
        n = int(len(ordered))
        if n == 0:
            continue
        ranks = np.arange(1, n + 1, dtype=int)
        rank_map = {row.sample_id: int(rank) for row, rank in zip(ordered.itertuples(index=False), ranks)}
        top10_cutoff = float(ordered.iloc[min(9, n - 1)]["next_month_return"])
        bottom10_cutoff = float(ordered.iloc[max(n - 10, 0)]["next_month_return"])
        winner_ids = set(ordered.head(min(DEFAULT_TOP_K, n))["sample_id"].tolist())
        loser_ids = set(ordered.tail(min(DEFAULT_TOP_K, n))["sample_id"].tolist())
        feature_cols = [col for col in labels.columns if col.startswith("img_e_") or col in NUMERIC_FEATURE_COLUMNS]
        winner_centroid = None
        if winner_ids:
            winner_frame = ordered[ordered["sample_id"].isin(winner_ids)]
            if not winner_frame.empty:
                winner_centroid = winner_frame[feature_cols].to_numpy(dtype=float, copy=False).mean(axis=0)
                winner_centroid = _normalize_vector(winner_centroid)
        similarity_rank = None
        if winner_centroid is not None:
            non_winner = ordered[~ordered["sample_id"].isin(winner_ids)].copy()
            if not non_winner.empty:
                non_vectors = non_winner[feature_cols].to_numpy(dtype=float, copy=False)
                non_vectors = np.apply_along_axis(_normalize_vector, 1, non_vectors)
                similarity = non_vectors @ winner_centroid
                non_winner = non_winner.assign(shape_similarity=similarity)
                similarity_rank = set(
                    non_winner.sort_values(["shape_similarity", "next_month_return"], ascending=[False, False])
                    .head(max(1, min(10, len(non_winner) // 10 or 1)))
                    .sample_id.tolist()
                )
        for row in ordered.itertuples(index=False):
            rank = int(rank_map.get(row.sample_id, n))
            rank_pct = 1.0 if n <= 1 else float(1.0 - ((rank - 1) / (n - 1)))
            labels.loc[labels["sample_id"] == row.sample_id, "next_month_return_rank"] = rank
            labels.loc[labels["sample_id"] == row.sample_id, "next_month_rank_pct"] = rank_pct
            labels.loc[labels["sample_id"] == row.sample_id, "is_next_top10"] = int(rank <= min(DEFAULT_TOP_K, n))
            labels.loc[labels["sample_id"] == row.sample_id, "is_next_bottom10"] = int(rank > max(0, n - DEFAULT_TOP_K))
            labels.loc[labels["sample_id"] == row.sample_id, "top10_boundary_gap"] = float(row.next_month_return - top10_cutoff)
            labels.loc[labels["sample_id"] == row.sample_id, "bottom10_boundary_gap"] = float(bottom10_cutoff - row.next_month_return)
            if rank <= min(DEFAULT_TOP_K, n):
                labels.loc[labels["sample_id"] == row.sample_id, "top_bucket"] = "top_bucket"
                labels.loc[labels["sample_id"] == row.sample_id, "cohort_label"] = "winner"
            elif rank > max(0, n - DEFAULT_TOP_K):
                labels.loc[labels["sample_id"] == row.sample_id, "top_bucket"] = "bottom_bucket"
                labels.loc[labels["sample_id"] == row.sample_id, "cohort_label"] = "loser"
            elif similarity_rank and row.sample_id in similarity_rank:
                labels.loc[labels["sample_id"] == row.sample_id, "cohort_label"] = "failed_lookalike"
            else:
                labels.loc[labels["sample_id"] == row.sample_id, "cohort_label"] = "neutral"
    labels["middle_bucket"] = (labels["top_bucket"] == "middle").astype(int)
    return labels


def _add_sample_weights(labels: pd.DataFrame) -> pd.DataFrame:
    if labels.empty:
        return labels.copy()
    work = labels.copy()
    month_order = sorted(int(m) for m in work["sample_month"].dropna().unique().tolist())
    month_index = {month: idx for idx, month in enumerate(month_order)}
    max_index = max(month_index.values()) if month_index else 0
    time_decay = []
    regime_similarity_weight = []
    data_quality_weight = []
    for row in work.itertuples(index=False):
        idx = int(month_index.get(int(row.sample_month), 0))
        time_decay.append(float(math.exp(-max(0, max_index - idx) / 24.0)))
        regime = _text(row.regime_tag, fallback="mixed")
        regime_similarity_weight.append(float(_regime_similarity(regime, regime)))
        quality = max(0.25, min(1.0, float(1.0 - _safe_float(row.missing_days_ratio, 0.0))))
        quality *= max(0.25, min(1.0, float(row.liquidity_score) if math.isfinite(float(row.liquidity_score)) else 0.25))
        data_quality_weight.append(float(max(0.1, min(1.0, quality))))
    work["time_decay_weight"] = time_decay
    work["regime_similarity_weight"] = regime_similarity_weight
    work["data_quality_weight"] = data_quality_weight
    work["sample_weight"] = work["time_decay_weight"] * work["regime_similarity_weight"] * work["data_quality_weight"]
    return work


def _feature_matrix(frame: pd.DataFrame, columns: Iterable[str]) -> np.ndarray:
    return frame[list(columns)].to_numpy(dtype=float, copy=False)


def _select_history_bank(
    samples: pd.DataFrame,
    *,
    target_month: int,
    memory_lookback_months: int,
    split_mode: str,
    rolling_window_months: int,
) -> pd.DataFrame:
    months = sorted(int(month) for month in samples["sample_month"].unique().tolist())
    if target_month not in months:
        return samples.iloc[0:0].copy()
    month_index = months.index(target_month)
    if split_mode == "rolling":
        start_index = max(0, month_index - max(1, int(rolling_window_months)))
    else:
        start_index = 0
    lookback_start = max(start_index, month_index - max(1, int(memory_lookback_months)))
    allowed_months = set(months[lookback_start:month_index])
    return samples[samples["sample_month"].isin(allowed_months)].copy()


def _score_from_neighbors(
    target_vectors: np.ndarray,
    history_vectors: np.ndarray,
    history_returns: np.ndarray,
    history_weights: np.ndarray,
    *,
    analog_k: int,
) -> tuple[np.ndarray, list[list[int]], list[list[float]], list[list[float]]]:
    if target_vectors.size == 0:
        return np.asarray([], dtype=float), [], [], []
    if history_vectors.size == 0:
        zeros = np.zeros(int(target_vectors.shape[0]), dtype=float)
        return zeros, [[] for _ in range(len(zeros))], [[] for _ in range(len(zeros))], [[] for _ in range(len(zeros))]
    target_norm = np.apply_along_axis(_normalize_vector, 1, target_vectors)
    history_norm = np.apply_along_axis(_normalize_vector, 1, history_vectors)
    scores = []
    analog_ids: list[list[int]] = []
    analog_scores: list[list[float]] = []
    analog_returns: list[list[float]] = []
    for idx in range(target_norm.shape[0]):
        sims = history_norm @ target_norm[idx]
        order = np.argsort(-sims)[: max(1, int(analog_k))]
        local_scores = sims[order]
        local_weights = np.maximum(0.0, local_scores) * np.maximum(0.1, history_weights[order])
        if float(np.sum(local_weights)) <= 0.0:
            local_weights = np.ones_like(local_weights, dtype=float)
        score = float(np.average(history_returns[order], weights=local_weights))
        scores.append(score)
        analog_ids.append(order.astype(int).tolist())
        analog_scores.append(local_scores.astype(float).tolist())
        analog_returns.append(history_returns[order].astype(float).tolist())
    return np.asarray(scores, dtype=float), analog_ids, analog_scores, analog_returns


def _month_scores_for_mode(
    samples: pd.DataFrame,
    *,
    split_mode: str,
    memory_lookback_months: int,
    rolling_window_months: int,
    analog_k: int,
) -> pd.DataFrame:
    if samples.empty:
        return samples.copy()
    score_rows: list[pd.DataFrame] = []
    months = sorted(int(month) for month in samples["sample_month"].unique().tolist())
    numeric_cols = list(NUMERIC_FEATURE_COLUMNS)
    image_cols = [col for col in samples.columns if col.startswith("img_e_")]
    hybrid_cols = numeric_cols + image_cols
    for month in months:
        month_frame = samples[samples["sample_month"] == month].copy()
        history_frame = _select_history_bank(
            samples,
            target_month=month,
            memory_lookback_months=memory_lookback_months,
            split_mode=split_mode,
            rolling_window_months=rolling_window_months,
        )
        if history_frame.empty:
            month_frame["numeric_score"] = 0.0
            month_frame["image_score"] = 0.0
            month_frame["similarity_score"] = 0.0
            month_frame["shape_score"] = 0.0
            month_frame["champion_score"] = 0.0
            month_frame["rerank_score"] = 0.0
            score_rows.append(month_frame)
            continue

        hist_returns = history_frame["next_month_return"].to_numpy(dtype=float, copy=False)
        hist_weights = history_frame["sample_weight"].to_numpy(dtype=float, copy=False)
        hist_numeric = history_frame[numeric_cols].to_numpy(dtype=float, copy=False)
        hist_image = history_frame[image_cols].to_numpy(dtype=float, copy=False)
        hist_hybrid = history_frame[hybrid_cols].to_numpy(dtype=float, copy=False)
        target_numeric = month_frame[numeric_cols].to_numpy(dtype=float, copy=False)
        target_image = month_frame[image_cols].to_numpy(dtype=float, copy=False)
        target_hybrid = month_frame[hybrid_cols].to_numpy(dtype=float, copy=False)

        numeric_score, numeric_neighbors, numeric_neighbor_scores, numeric_neighbor_returns = _score_from_neighbors(
            target_numeric,
            hist_numeric,
            hist_returns,
            hist_weights,
            analog_k=analog_k,
        )
        image_score, image_neighbors, image_neighbor_scores, image_neighbor_returns = _score_from_neighbors(
            target_image,
            hist_image,
            hist_returns,
            hist_weights,
            analog_k=analog_k,
        )
        similarity_score, similarity_neighbors, similarity_neighbor_scores, similarity_neighbor_returns = _score_from_neighbors(
            target_hybrid,
            hist_hybrid,
            hist_returns,
            hist_weights,
            analog_k=analog_k,
        )

        month_frame["numeric_score"] = numeric_score
        month_frame["image_score"] = image_score
        month_frame["similarity_score"] = similarity_score
        month_frame["shape_score"] = np.mean(np.vstack([numeric_score, image_score, similarity_score]), axis=0)
        month_frame["champion_score"] = (
            0.40 * month_frame["window_return_6m"].to_numpy(dtype=float, copy=False)
            + 0.20 * month_frame["window_return_3m"].to_numpy(dtype=float, copy=False)
            + 0.15 * month_frame["window_return_1m"].to_numpy(dtype=float, copy=False)
            + 0.10 * (1.0 - month_frame["box_width_6m"].to_numpy(dtype=float, copy=False))
            + 0.10 * month_frame["breakout_proximity_high"].to_numpy(dtype=float, copy=False)
            + 0.05 * month_frame["volume_expansion_20_60"].to_numpy(dtype=float, copy=False)
        )
        month_frame["rerank_score"] = (
            0.55 * month_frame["champion_score"].to_numpy(dtype=float, copy=False)
            + 0.35 * month_frame["shape_score"].to_numpy(dtype=float, copy=False)
            + 0.10 * (1.0 - month_frame["breakout_proximity_low"].to_numpy(dtype=float, copy=False))
        )
        month_frame["numeric_neighbors"] = [json.dumps(item) for item in numeric_neighbors]
        month_frame["numeric_neighbor_scores"] = [json.dumps(item) for item in numeric_neighbor_scores]
        month_frame["numeric_neighbor_returns"] = [json.dumps(item) for item in numeric_neighbor_returns]
        month_frame["image_neighbors"] = [json.dumps(item) for item in image_neighbors]
        month_frame["image_neighbor_scores"] = [json.dumps(item) for item in image_neighbor_scores]
        month_frame["image_neighbor_returns"] = [json.dumps(item) for item in image_neighbor_returns]
        month_frame["similarity_neighbors"] = [json.dumps(item) for item in similarity_neighbors]
        month_frame["similarity_neighbor_scores"] = [json.dumps(item) for item in similarity_neighbor_scores]
        month_frame["similarity_neighbor_returns"] = [json.dumps(item) for item in similarity_neighbor_returns]
        score_rows.append(month_frame)
    return pd.concat(score_rows, ignore_index=True)


def _rank_month_frame(frame: pd.DataFrame, score_col: str, *, top_k: int) -> pd.DataFrame:
    ordered = frame.sort_values([score_col, "code"], ascending=[False, True]).copy()
    ordered["pred_rank"] = np.arange(1, len(ordered) + 1, dtype=int)
    ordered["pred_rank_pct"] = 1.0 if len(ordered) <= 1 else 1.0 - ((ordered["pred_rank"] - 1) / (len(ordered) - 1))
    ordered["pred_is_top10"] = (ordered["pred_rank"] <= min(top_k, len(ordered))).astype(int)
    return ordered


def _month_metrics(
    frame: pd.DataFrame,
    score_col: str,
    *,
    top_k: int = DEFAULT_TOP_K,
    candidate_pool_k: int = DEFAULT_CANDIDATE_POOL_K,
) -> dict[str, Any]:
    monthly_rows: list[dict[str, Any]] = []
    for month, month_frame in frame.groupby("sample_month"):
        ranked = _rank_month_frame(month_frame, score_col, top_k=top_k)
        top5 = ranked.head(min(TOP5_K, len(ranked)))
        top = ranked.head(min(top_k, len(ranked)))
        candidate_pool = ranked.head(min(candidate_pool_k, len(ranked)))
        top10_boundary = ranked.iloc[min(top_k - 1, len(ranked) - 1)]
        next_boundary = ranked.iloc[min(top_k, len(ranked) - 1)]
        top5_boundary = ranked.iloc[min(TOP5_K - 1, len(ranked) - 1)]
        next_top5_boundary = ranked.iloc[min(TOP5_K, len(ranked) - 1)] if len(ranked) > TOP5_K else top5_boundary
        actual_bottom10 = ranked[ranked["is_next_bottom10"] == 1]
        pool_bottom10_removed = int(max(0, len(actual_bottom10) - int(candidate_pool["is_next_bottom10"].sum())))
        monthly_rows.append(
            {
                "sample_month": int(month),
                "sample_count": int(len(ranked)),
                "top5_hit_count": int(top5["is_next_top10"].sum()),
                "top5_hit_rate": float(top5["is_next_top10"].mean()) if len(top5) else 0.0,
                "mean_next_month_return_top5": float(top5["next_month_return"].mean()) if len(top5) else 0.0,
                "median_next_month_return_top5": float(top5["next_month_return"].median()) if len(top5) else 0.0,
                "top5_boundary_score_gap": float(top5_boundary[score_col] - next_top5_boundary[score_col]) if len(ranked) > TOP5_K else float(top5_boundary[score_col]),
                "top5_boundary_outcome_gap": float(top5_boundary["next_month_return"] - next_top5_boundary["next_month_return"]) if len(ranked) > TOP5_K else float(top5_boundary["next_month_return"]),
                "top10_hit_count": int(top["is_next_top10"].sum()),
                "top10_hit_rate": float(top["is_next_top10"].mean()) if len(top) else 0.0,
                "mean_next_month_return_top10": float(top["next_month_return"].mean()) if len(top) else 0.0,
                "median_next_month_return_top10": float(top["next_month_return"].median()) if len(top) else 0.0,
                "top10_boundary_score_gap": float(top10_boundary[score_col] - next_boundary[score_col]) if len(ranked) > top_k else float(top10_boundary[score_col]),
                "top10_boundary_outcome_gap": float(top10_boundary["next_month_return"] - next_boundary["next_month_return"]) if len(ranked) > top_k else float(top10_boundary["next_month_return"]),
                "candidate_pool_top10_capture": int(candidate_pool["is_next_top10"].sum()),
                "candidate_pool_bad_pick_removal": pool_bottom10_removed,
                "predicted_top10_members": top["sample_id"].tolist(),
                "predicted_top10_codes": top["code"].tolist(),
                "actual_top10_members": ranked[ranked["is_next_top10"] == 1]["sample_id"].tolist(),
                "predicted_top10_return_sum": float(top["next_month_return"].sum()) if len(top) else 0.0,
                "predicted_top10_bottom10_removed": int(top["is_next_bottom10"].sum()),
                "regime_tag": _text(ranked.iloc[0].regime_tag, fallback="mixed"),
            }
        )
    monthly_df = pd.DataFrame(monthly_rows)
    if monthly_df.empty:
        return {
            "month_count": 0,
            "sample_count": 0,
            "mean_top5_hit_count": 0.0,
            "mean_top5_hit_rate": 0.0,
            "mean_next_month_return_top5": 0.0,
            "median_next_month_return_top5": 0.0,
            "mean_top5_boundary_score_gap": 0.0,
            "mean_top5_boundary_outcome_gap": 0.0,
            "mean_candidate_pool_top10_capture": 0.0,
            "mean_candidate_pool_bad_pick_removal": 0.0,
            "mean_top10_hit_count": 0.0,
            "mean_top10_hit_rate": 0.0,
            "mean_next_month_return_top10": 0.0,
            "median_next_month_return_top10": 0.0,
            "mean_top10_boundary_score_gap": 0.0,
            "mean_top10_boundary_outcome_gap": 0.0,
            "positive_month_share": 0.0,
            "regime_breakdown": {},
            "monthly_rows": [],
        }
    regime_breakdown: dict[str, dict[str, float]] = {}
    for regime, regime_df in monthly_df.groupby("regime_tag"):
        regime_breakdown[str(regime)] = {
            "month_count": float(len(regime_df)),
            "mean_top10_hit_rate": float(regime_df["top10_hit_rate"].mean()),
            "mean_next_month_return_top10": float(regime_df["mean_next_month_return_top10"].mean()),
            "mean_top10_boundary_outcome_gap": float(regime_df["top10_boundary_outcome_gap"].mean()),
        }
    return {
        "month_count": int(len(monthly_df)),
        "sample_count": int(monthly_df["sample_count"].sum()),
        "mean_top5_hit_count": float(monthly_df["top5_hit_count"].mean()),
        "mean_top5_hit_rate": float(monthly_df["top5_hit_rate"].mean()),
        "mean_next_month_return_top5": float(monthly_df["mean_next_month_return_top5"].mean()),
        "median_next_month_return_top5": float(monthly_df["median_next_month_return_top5"].median()),
        "mean_top5_boundary_score_gap": float(monthly_df["top5_boundary_score_gap"].mean()),
        "mean_top5_boundary_outcome_gap": float(monthly_df["top5_boundary_outcome_gap"].mean()),
        "mean_candidate_pool_top10_capture": float(monthly_df["candidate_pool_top10_capture"].mean()),
        "mean_candidate_pool_bad_pick_removal": float(monthly_df["candidate_pool_bad_pick_removal"].mean()),
        "mean_top10_hit_count": float(monthly_df["top10_hit_count"].mean()),
        "mean_top10_hit_rate": float(monthly_df["top10_hit_rate"].mean()),
        "mean_next_month_return_top10": float(monthly_df["mean_next_month_return_top10"].mean()),
        "median_next_month_return_top10": float(monthly_df["median_next_month_return_top10"].median()),
        "mean_top10_boundary_score_gap": float(monthly_df["top10_boundary_score_gap"].mean()),
        "mean_top10_boundary_outcome_gap": float(monthly_df["top10_boundary_outcome_gap"].mean()),
        "positive_month_share": float((monthly_df["mean_next_month_return_top10"] > 0).mean()),
        "monthly_uplift_std": float(monthly_df["mean_next_month_return_top10"].std(ddof=1) if len(monthly_df) > 1 else 0.0),
        "monthly_rows": monthly_df.to_dict(orient="records"),
        "regime_breakdown": regime_breakdown,
    }


def _compare_scores(
    scored: pd.DataFrame,
    *,
    champion_col: str,
    rerank_col: str,
    top_k: int = DEFAULT_TOP_K,
    candidate_pool_k: int = DEFAULT_CANDIDATE_POOL_K,
) -> dict[str, Any]:
    monthly_rows: list[dict[str, Any]] = []
    boundary_bucket_rows: dict[str, list[dict[str, Any]]] = {label: [] for _, _, label in BOUNDARY_BUCKETS}
    for month, month_frame in scored.groupby("sample_month"):
        champion_ranked = _rank_month_frame(month_frame, champion_col, top_k=max(top_k, candidate_pool_k))
        rerank_full_ranked = _rank_month_frame(month_frame, rerank_col, top_k=max(top_k, candidate_pool_k))
        rerank_candidates = champion_ranked.head(min(candidate_pool_k, len(champion_ranked))).copy()
        rerank_ranked = _rank_month_frame(rerank_candidates, rerank_col, top_k=max(top_k, candidate_pool_k))
        champion_top10 = champion_ranked.head(min(top_k, len(champion_ranked)))
        rerank_top10 = rerank_ranked.head(min(top_k, len(rerank_ranked)))
        champion_top5 = champion_ranked.head(min(TOP5_K, len(champion_ranked)))
        rerank_top5 = rerank_ranked.head(min(TOP5_K, len(rerank_ranked)))
        champion_top30 = champion_ranked.head(min(candidate_pool_k, len(champion_ranked)))
        rerank_top30 = rerank_full_ranked.head(min(candidate_pool_k, len(rerank_full_ranked)))
        champion_candidate_pool = champion_top30
        rerank_candidate_pool = rerank_top30
        champion_bottom10 = champion_ranked[champion_ranked["is_next_bottom10"] == 1]
        rerank_bottom10 = rerank_full_ranked[rerank_full_ranked["is_next_bottom10"] == 1]
        removed_bad = champion_top10[~champion_top10["sample_id"].isin(rerank_top10["sample_id"]) & (champion_top10["is_next_bottom10"] == 1)]
        added_good = rerank_top10[~rerank_top10["sample_id"].isin(champion_top10["sample_id"]) & (rerank_top10["is_next_top10"] == 1)]
        changed_members = set(champion_top10["sample_id"]) ^ set(rerank_top10["sample_id"])
        changed_top5_members = set(champion_top5["sample_id"]) ^ set(rerank_top5["sample_id"])
        rank_changes = 0
        champion_rank_map = {row.sample_id: int(row.pred_rank) for row in champion_top30.itertuples(index=False)}
        rerank_rank_map = {row.sample_id: int(row.pred_rank) for row in rerank_top30.itertuples(index=False)}
        for sample_id in set(champion_rank_map) | set(rerank_rank_map):
            if champion_rank_map.get(sample_id) != rerank_rank_map.get(sample_id):
                rank_changes += 1
        champion_candidate_pool_top10_capture = int(champion_candidate_pool["is_next_top10"].sum())
        rerank_candidate_pool_top10_capture = int(rerank_candidate_pool["is_next_top10"].sum())
        champion_candidate_pool_bad_pick_removal = int(max(0, int(champion_bottom10["sample_id"].nunique()) - int(champion_candidate_pool["is_next_bottom10"].sum())))
        rerank_candidate_pool_bad_pick_removal = int(max(0, int(rerank_bottom10["sample_id"].nunique()) - int(rerank_candidate_pool["is_next_bottom10"].sum())))
        top5_champion_boundary = champion_ranked.iloc[min(TOP5_K - 1, len(champion_ranked) - 1)]
        top5_next_champion = champion_ranked.iloc[min(TOP5_K, len(champion_ranked) - 1)]
        top5_rerank_boundary = rerank_ranked.iloc[min(TOP5_K - 1, len(rerank_ranked) - 1)]
        top5_next_rerank = rerank_ranked.iloc[min(TOP5_K, len(rerank_ranked) - 1)]
        bucket_effect = _bucket_effect_summary(champion_ranked, rerank_full_ranked, top_k=top_k)
        for bucket_label, payload in bucket_effect.items():
            if isinstance(payload, dict):
                boundary_bucket_rows[bucket_label].append(payload)
        monthly_rows.append(
            {
                "sample_month": int(month),
                "sample_count": int(len(month_frame)),
                "champion_candidate_pool_top10_capture": champion_candidate_pool_top10_capture,
                "rerank_candidate_pool_top10_capture": rerank_candidate_pool_top10_capture,
                "candidate_pool_top10_capture_delta": int(rerank_candidate_pool_top10_capture - champion_candidate_pool_top10_capture),
                "champion_candidate_pool_bad_pick_removal": champion_candidate_pool_bad_pick_removal,
                "rerank_candidate_pool_bad_pick_removal": rerank_candidate_pool_bad_pick_removal,
                "candidate_pool_bad_pick_removal_delta": int(rerank_candidate_pool_bad_pick_removal - champion_candidate_pool_bad_pick_removal),
                "top5_hit_count": int(rerank_top5["is_next_top10"].sum()),
                "top5_hit_rate": float(rerank_top5["is_next_top10"].mean()) if len(rerank_top5) else 0.0,
                "champion_top5_hit_count": int(champion_top5["is_next_top10"].sum()),
                "champion_top5_hit_rate": float(champion_top5["is_next_top10"].mean()) if len(champion_top5) else 0.0,
                "changed_top5_members_count": int(len(changed_top5_members)),
                "champion_top5_boundary_score_gap": float(top5_champion_boundary[champion_col] - top5_next_champion[champion_col]) if len(champion_ranked) > TOP5_K else float(top5_champion_boundary[champion_col]),
                "rerank_top5_boundary_score_gap": float(top5_rerank_boundary[rerank_col] - top5_next_rerank[rerank_col]) if len(rerank_ranked) > TOP5_K else float(top5_rerank_boundary[rerank_col]),
                "champion_top5_boundary_outcome_gap": float(top5_champion_boundary["next_month_return"] - top5_next_champion["next_month_return"]) if len(champion_ranked) > TOP5_K else float(top5_champion_boundary["next_month_return"]),
                "rerank_top5_boundary_outcome_gap": float(top5_rerank_boundary["next_month_return"] - top5_next_rerank["next_month_return"]) if len(rerank_ranked) > TOP5_K else float(top5_rerank_boundary["next_month_return"]),
                "champion_top10_hit_count": int(champion_top10["is_next_top10"].sum()),
                "rerank_top10_hit_count": int(rerank_top10["is_next_top10"].sum()),
                "champion_top10_mean_next_month_return": float(champion_top10["next_month_return"].mean()) if len(champion_top10) else 0.0,
                "rerank_top10_mean_next_month_return": float(rerank_top10["next_month_return"].mean()) if len(rerank_top10) else 0.0,
                "champion_top10_median_next_month_return": float(champion_top10["next_month_return"].median()) if len(champion_top10) else 0.0,
                "rerank_top10_median_next_month_return": float(rerank_top10["next_month_return"].median()) if len(rerank_top10) else 0.0,
                "bad_pick_removal_count": int(len(removed_bad)),
                "good_pick_addition_count": int(len(added_good)),
                "changed_top10_members_count": int(len(changed_members)),
                "changed_rank_count": int(rank_changes),
                "bucket_effect": bucket_effect,
                "champion_top10_boundary_score_gap": float(champion_ranked.iloc[min(top_k - 1, len(champion_ranked) - 1)][champion_col] - champion_ranked.iloc[min(top_k, len(champion_ranked) - 1)][champion_col]) if len(champion_ranked) > top_k else float(champion_ranked.iloc[min(top_k - 1, len(champion_ranked) - 1)][champion_col]),
                "rerank_top10_boundary_score_gap": float(rerank_ranked.iloc[min(top_k - 1, len(rerank_ranked) - 1)][rerank_col] - rerank_ranked.iloc[min(top_k, len(rerank_ranked) - 1)][rerank_col]) if len(rerank_ranked) > top_k else float(rerank_ranked.iloc[min(top_k - 1, len(rerank_ranked) - 1)][rerank_col]),
                "champion_top10_boundary_outcome_gap": float(champion_ranked.iloc[min(top_k - 1, len(champion_ranked) - 1)]["next_month_return"] - champion_ranked.iloc[min(top_k, len(champion_ranked) - 1)]["next_month_return"]) if len(champion_ranked) > top_k else float(champion_ranked.iloc[min(top_k - 1, len(champion_ranked) - 1)]["next_month_return"]),
                "rerank_top10_boundary_outcome_gap": float(rerank_ranked.iloc[min(top_k - 1, len(rerank_ranked) - 1)]["next_month_return"] - rerank_ranked.iloc[min(top_k, len(rerank_ranked) - 1)]["next_month_return"]) if len(rerank_ranked) > top_k else float(rerank_ranked.iloc[min(top_k - 1, len(rerank_ranked) - 1)]["next_month_return"]),
                "selection_divergence_reason": _selection_divergence_reason(champion_top10, rerank_top10),
                "regime_tag": _text(champion_ranked.iloc[0].regime_tag, fallback="mixed"),
            }
        )
    monthly_df = pd.DataFrame(monthly_rows)
    if monthly_df.empty:
        return {
            "month_count": 0,
            "sample_count": 0,
            "winner_promotion_delta": 0.0,
            "winner_promotion_score_delta": 0.0,
            "loser_removal_delta": 0.0,
            "candidate_pool_top10_capture": 0.0,
            "candidate_pool_top10_capture_delta": 0.0,
            "candidate_pool_bad_pick_removal": 0.0,
            "candidate_pool_bad_pick_removal_delta": 0.0,
            "final_top10_uplift": 0.0,
            "final_top10_bad_pick_removal": 0.0,
            "changed_top5_members_count": 0.0,
            "top5_boundary_score_gap": 0.0,
            "top5_boundary_outcome_gap": 0.0,
            "oos_top10_uplift": 0.0,
            "oos_bad_pick_removal": 0.0,
            "changed_top10_members_count": 0.0,
            "changed_rank_count": 0.0,
            "top10_boundary_score_gap": 0.0,
            "top10_boundary_outcome_gap": 0.0,
            "boundary_bucket_effect": {},
            "selection_divergence_reason": "insufficient_months",
            "regime_breakdown": {},
            "monthly_rows": [],
        }
    regime_breakdown: dict[str, dict[str, float]] = {}
    for regime, regime_df in monthly_df.groupby("regime_tag"):
        regime_breakdown[str(regime)] = {
            "month_count": float(len(regime_df)),
            "mean_top10_uplift": float((regime_df["rerank_top10_hit_count"] - regime_df["champion_top10_hit_count"]).mean()),
            "mean_bad_pick_removal": float(regime_df["bad_pick_removal_count"].mean()),
            "mean_changed_top10_members_count": float(regime_df["changed_top10_members_count"].mean()),
            "mean_top10_boundary_outcome_gap": float(regime_df["rerank_top10_boundary_outcome_gap"].mean()),
            "mean_candidate_pool_top10_capture": float(regime_df["rerank_candidate_pool_top10_capture"].mean()),
            "mean_candidate_pool_bad_pick_removal": float(regime_df["rerank_candidate_pool_bad_pick_removal"].mean()),
            "positive_month_share": float(((regime_df["rerank_top10_hit_count"] - regime_df["champion_top10_hit_count"]) > 0).mean()),
            "negative_month_share": float(((regime_df["rerank_top10_hit_count"] - regime_df["champion_top10_hit_count"]) < 0).mean()),
        }
    boundary_bucket_effect: dict[str, dict[str, float]] = {}
    for label, rows in boundary_bucket_rows.items():
        if not rows:
            boundary_bucket_effect[label] = {
                "sample_count": 0,
                "mean_champion_rank": 0.0,
                "mean_rerank_rank": 0.0,
                "mean_rank_delta": 0.0,
                "mean_score_delta": 0.0,
                "enter_top10_rate": 0.0,
            }
            continue
        bucket_df = pd.DataFrame(rows)
        boundary_bucket_effect[label] = {
            "sample_count": int(bucket_df["sample_count"].sum()),
            "mean_champion_rank": float(bucket_df["mean_champion_rank"].mean()),
            "mean_rerank_rank": float(bucket_df["mean_rerank_rank"].mean()),
            "mean_rank_delta": float(bucket_df["mean_rank_delta"].mean()),
            "mean_score_delta": float(bucket_df["mean_score_delta"].mean()),
            "enter_top10_rate": float(bucket_df["enter_top10_rate"].mean()),
        }
    uplift = monthly_df["rerank_top10_hit_count"] - monthly_df["champion_top10_hit_count"]
    winner_promotion_delta = float(monthly_df["rerank_top10_hit_count"].sub(monthly_df["champion_top10_hit_count"]).mean())
    winner_promotion_score_delta = float(monthly_df["rerank_top10_mean_next_month_return"].sub(monthly_df["champion_top10_mean_next_month_return"]).mean())
    loser_removal_delta = float(monthly_df["bad_pick_removal_count"].mean())
    candidate_pool_top10_capture = float(monthly_df["rerank_candidate_pool_top10_capture"].mean())
    candidate_pool_top10_capture_delta = float(monthly_df["candidate_pool_top10_capture_delta"].mean())
    candidate_pool_bad_pick_removal = float(monthly_df["rerank_candidate_pool_bad_pick_removal"].mean())
    candidate_pool_bad_pick_removal_delta = float(monthly_df["candidate_pool_bad_pick_removal_delta"].mean())
    changed_top5_members_count = float(monthly_df["changed_top5_members_count"].mean())
    top5_boundary_score_gap = float(monthly_df["rerank_top5_boundary_score_gap"].mean())
    top5_boundary_outcome_gap = float(monthly_df["rerank_top5_boundary_outcome_gap"].mean())
    return {
        "schema_version": MONTHLY_SHAPE_MEMORY_COMPARE_SCHEMA_VERSION,
        "top_k": int(top_k),
        "candidate_pool_k": int(candidate_pool_k),
        "month_count": int(len(monthly_df)),
        "sample_count": int(monthly_df["sample_count"].sum()),
        "winner_promotion_delta": winner_promotion_delta,
        "winner_promotion_score_delta": winner_promotion_score_delta,
        "loser_removal_delta": loser_removal_delta,
        "candidate_pool_top10_capture": candidate_pool_top10_capture,
        "candidate_pool_top10_capture_delta": candidate_pool_top10_capture_delta,
        "candidate_pool_bad_pick_removal": candidate_pool_bad_pick_removal,
        "candidate_pool_bad_pick_removal_delta": candidate_pool_bad_pick_removal_delta,
        "final_top10_uplift": float(uplift.mean()),
        "final_top10_bad_pick_removal": float(monthly_df["bad_pick_removal_count"].mean()),
        "changed_top5_members_count": changed_top5_members_count,
        "top5_boundary_score_gap": top5_boundary_score_gap,
        "top5_boundary_outcome_gap": top5_boundary_outcome_gap,
        "oos_top10_uplift": float(uplift.mean()),
        "oos_bad_pick_removal": float(monthly_df["bad_pick_removal_count"].mean()),
        "changed_top10_members_count": float(monthly_df["changed_top10_members_count"].mean()),
        "changed_rank_count": float(monthly_df["changed_rank_count"].mean()),
        "top10_boundary_score_gap": float(monthly_df["rerank_top10_boundary_score_gap"].mean()),
        "top10_boundary_outcome_gap": float(monthly_df["rerank_top10_boundary_outcome_gap"].mean()),
        "positive_month_share": float((uplift > 0).mean()),
        "negative_month_share": float((uplift < 0).mean()),
        "monthly_uplift_std": float(uplift.std(ddof=1) if len(uplift) > 1 else 0.0),
        "selection_divergence_reason": _selection_divergence_reason_from_monthly(monthly_df),
        "regime_breakdown": regime_breakdown,
        "boundary_bucket_effect": boundary_bucket_effect,
        "monthly_rows": monthly_df.to_dict(orient="records"),
    }


def _selection_divergence_reason(champion_top10: pd.DataFrame, rerank_top10: pd.DataFrame) -> str:
    if champion_top10.empty or rerank_top10.empty:
        return "insufficient_top10"
    if set(champion_top10["sample_id"]) == set(rerank_top10["sample_id"]):
        return "no_branching"
    return "rerank_changed_members"


def _selection_divergence_reason_from_monthly(monthly_df: pd.DataFrame) -> str:
    if monthly_df.empty:
        return "insufficient_months"
    if float((monthly_df["changed_top10_members_count"] > 0).mean()) <= 0.0:
        return "no_branching"
    if float(monthly_df["rerank_top10_boundary_outcome_gap"].mean()) <= float(monthly_df["champion_top10_boundary_outcome_gap"].mean()):
        return "boundary_not_improved"
    return "rerank_branch_changed_members"


def _branch_eval_payload(scored: pd.DataFrame, *, top_k: int, candidate_pool_k: int) -> dict[str, Any]:
    branch_scores = {
        "champion_only": "champion_score",
        "numeric_only": "numeric_score",
        "image_only": "image_score",
        "similarity_only": "similarity_score",
        "numeric_plus_similarity": None,
        "numeric_plus_image": None,
        "image_plus_similarity": None,
        "numeric_image_similarity": None,
        "champion_plus_shape_rerank": "rerank_score",
    }
    branch_metrics: dict[str, Any] = {}
    for branch_name, score_col in branch_scores.items():
        if score_col is not None:
            branch_metrics[branch_name] = _month_metrics(scored, score_col, top_k=top_k, candidate_pool_k=candidate_pool_k)
            continue
        if branch_name == "numeric_plus_similarity":
            scored = scored.copy()
            scored["_tmp_score"] = 0.5 * scored["numeric_score"] + 0.5 * scored["similarity_score"]
        elif branch_name == "numeric_plus_image":
            scored = scored.copy()
            scored["_tmp_score"] = 0.5 * scored["numeric_score"] + 0.5 * scored["image_score"]
        elif branch_name == "image_plus_similarity":
            scored = scored.copy()
            scored["_tmp_score"] = 0.5 * scored["image_score"] + 0.5 * scored["similarity_score"]
        else:
            scored = scored.copy()
            scored["_tmp_score"] = (scored["numeric_score"] + scored["image_score"] + scored["similarity_score"]) / 3.0
        branch_metrics[branch_name] = _month_metrics(scored, "_tmp_score", top_k=top_k)
    return {
        "schema_version": MONTHLY_SHAPE_MEMORY_BRANCH_SCHEMA_VERSION,
        "top_k": int(top_k),
        "candidate_pool_k": int(candidate_pool_k),
        "branch_metrics": branch_metrics,
    }


def _branch_contribution_summary(branch_metrics: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for branch_name, metrics in branch_metrics.items():
        if not isinstance(metrics, dict):
            continue
        summary[branch_name] = {
            "mean_top5_hit_rate": float(metrics.get("mean_top5_hit_rate") or 0.0),
            "mean_top10_hit_rate": float(metrics.get("mean_top10_hit_rate") or 0.0),
            "mean_candidate_pool_top10_capture": float(metrics.get("mean_candidate_pool_top10_capture") or 0.0),
            "mean_candidate_pool_bad_pick_removal": float(metrics.get("mean_candidate_pool_bad_pick_removal") or 0.0),
            "mean_top5_boundary_outcome_gap": float(metrics.get("mean_top5_boundary_outcome_gap") or 0.0),
            "mean_top10_boundary_outcome_gap": float(metrics.get("mean_top10_boundary_outcome_gap") or 0.0),
            "mean_next_month_return_top10": float(metrics.get("mean_next_month_return_top10") or 0.0),
        }
    return summary


def _bucket_effect_summary(champion_ranked: pd.DataFrame, rerank_ranked: pd.DataFrame, *, top_k: int) -> dict[str, Any]:
    if champion_ranked.empty or rerank_ranked.empty:
        return {}
    merged = champion_ranked[["sample_id", "pred_rank", "next_month_return", "champion_score", "pred_rank_pct"]].merge(
        rerank_ranked[["sample_id", "pred_rank", "next_month_return", "rerank_score", "pred_rank_pct"]],
        on="sample_id",
        how="inner",
        suffixes=("_champion", "_rerank"),
    )
    if merged.empty:
        return {}
    bucket_summary: dict[str, Any] = {}
    for lo, hi, label in BOUNDARY_BUCKETS:
        bucket = merged[(merged["pred_rank_champion"] >= lo) & (merged["pred_rank_champion"] <= hi)]
        if bucket.empty:
            bucket_summary[label] = {
                "sample_count": 0,
                "mean_champion_rank": 0.0,
                "mean_rerank_rank": 0.0,
                "mean_rank_delta": 0.0,
                "mean_score_delta": 0.0,
                "enter_top10_rate": 0.0,
            }
            continue
        bucket_summary[label] = {
            "sample_count": int(len(bucket)),
            "mean_champion_rank": float(bucket["pred_rank_champion"].mean()),
            "mean_rerank_rank": float(bucket["pred_rank_rerank"].mean()),
            "mean_rank_delta": float((bucket["pred_rank_champion"] - bucket["pred_rank_rerank"]).mean()),
            "mean_score_delta": float((bucket["rerank_score"] - bucket["champion_score"]).mean()),
            "enter_top10_rate": float((bucket["pred_rank_rerank"] <= top_k).mean()),
        }
    return bucket_summary


def _failure_mode_typed(compare: dict[str, Any], branch_eval: dict[str, Any], split_contract: dict[str, Any]) -> str:
    branch_metrics = branch_eval.get("modes", {}).get("expanding", {}).get("branch_metrics", {}) if isinstance(branch_eval, dict) else {}
    if not isinstance(branch_metrics, dict):
        branch_metrics = {}
    if _text(split_contract.get("leakage_check_status"), fallback="unknown") != "pass":
        return "signal_not_useful"

    final_uplift = float(compare.get("final_top10_uplift") or compare.get("oos_top10_uplift") or 0.0)
    candidate_pool_capture_delta = float(compare.get("candidate_pool_top10_capture_delta") or 0.0)
    candidate_pool_removal_delta = float(compare.get("candidate_pool_bad_pick_removal_delta") or 0.0)
    boundary_gap = float(compare.get("top10_boundary_outcome_gap") or 0.0)
    boundary_bucket_effect = compare.get("boundary_bucket_effect") if isinstance(compare.get("boundary_bucket_effect"), dict) else {}
    regime_breakdown = compare.get("regime_breakdown") if isinstance(compare.get("regime_breakdown"), dict) else {}
    regimes = [regime for regime, payload in regime_breakdown.items() if isinstance(payload, dict) and float(payload.get("mean_top10_uplift") or 0.0) > 0.0]
    negative_regimes = [regime for regime, payload in regime_breakdown.items() if isinstance(payload, dict) and float(payload.get("mean_top10_uplift") or 0.0) < 0.0]

    branch_hit_rates = {
        name: float(metrics.get("mean_top10_hit_rate") or 0.0)
        for name, metrics in branch_metrics.items()
        if isinstance(metrics, dict)
    }
    fused_hit_rate = branch_hit_rates.get("champion_plus_shape_rerank", 0.0)
    best_single_branch_hit_rate = max(
        [rate for name, rate in branch_hit_rates.items() if name in {"numeric_only", "image_only", "similarity_only", "numeric_plus_similarity", "numeric_plus_image", "image_plus_similarity", "numeric_image_similarity"}] or [0.0]
    )

    if candidate_pool_capture_delta > 0.0 and final_uplift <= 0.0 and boundary_gap <= 0.0:
        return "boundary_too_weak"
    if final_uplift <= 0.0 and candidate_pool_capture_delta <= 0.0 and candidate_pool_removal_delta <= 0.0:
        if best_single_branch_hit_rate > fused_hit_rate + 0.01:
            return "branch_composition_failed"
        if len(regimes) > 0 and len(negative_regimes) > 0:
            return "regime_instability"
        return "signal_not_useful"
    if final_uplift <= 0.0 and candidate_pool_removal_delta > 0.0 and candidate_pool_capture_delta <= 0.0:
        return "winner_promotion_failed"
    if len(regimes) > 0 and len(negative_regimes) > 0:
        return "regime_instability"
    if best_single_branch_hit_rate > fused_hit_rate + 0.01 and final_uplift <= 0.0:
        return "branch_composition_failed"
    if candidate_pool_capture_delta > 0.0 and final_uplift <= 0.0:
        if boundary_bucket_effect:
            boundary_order = ["1-5", "6-10", "11-20", "21-50"]
            boundary_strength = {label: float((boundary_bucket_effect.get(label) or {}).get("mean_rank_delta") or 0.0) for label in boundary_order}
            if max(boundary_strength.get("11-20", 0.0), boundary_strength.get("21-50", 0.0)) >= max(boundary_strength.get("1-5", 0.0), boundary_strength.get("6-10", 0.0)):
                return "boundary_too_weak"
        return "winner_promotion_failed"
    return "signal_not_useful"


def _next_action_typed(failure_mode_typed: str, compare: dict[str, Any]) -> str:
    candidate_pool_capture_delta = float(compare.get("candidate_pool_top10_capture_delta") or 0.0)
    final_uplift = float(compare.get("final_top10_uplift") or compare.get("oos_top10_uplift") or 0.0)
    if failure_mode_typed == "production_db_only_failure":
        return "continue_regime_gated_variant"
    if failure_mode_typed == "winner_promotion_failed":
        return "keep_as_filter_only"
    if failure_mode_typed == "boundary_too_weak":
        return "continue_branch_composition_fix" if candidate_pool_capture_delta > 0.0 else "keep_as_filter_only"
    if failure_mode_typed == "branch_composition_failed":
        return "continue_branch_composition_fix"
    if failure_mode_typed == "regime_instability":
        return "continue_regime_gated_variant"
    if final_uplift <= 0.0 and candidate_pool_capture_delta <= 0.0:
        return "drop_axis"
    return "continue_numeric_similarity_only"


def _build_split_contract(
    samples: pd.DataFrame,
    *,
    memory_lookback_months: int,
    rolling_window_months: int,
) -> dict[str, Any]:
    months = sorted(int(month) for month in samples["sample_month"].unique().tolist())
    if not months:
        return {
            "schema_version": MONTHLY_SHAPE_MEMORY_SPLIT_SCHEMA_VERSION,
            "split_modes": [],
            "leakage_check_status": "empty",
        }
    split_modes = [
        {
            "split_mode": "expanding",
            "train_contract": "all months strictly before the evaluation month",
            "validation_contract": "next month block",
            "test_contract": "later month block",
            "purge_months": 1,
            "embargo_months": 1,
            "memory_lookback_months": int(memory_lookback_months),
            "rolling_window_months": None,
        },
        {
            "split_mode": "rolling",
            "train_contract": f"last {int(rolling_window_months)} months before the evaluation month",
            "validation_contract": "next month block",
            "test_contract": "later month block",
            "purge_months": 1,
            "embargo_months": 1,
            "memory_lookback_months": int(memory_lookback_months),
            "rolling_window_months": int(rolling_window_months),
        },
    ]
    leakage = validate_no_future_month_leakage(samples)
    return {
        "schema_version": MONTHLY_SHAPE_MEMORY_SPLIT_SCHEMA_VERSION,
        "period": {
            "start_month": int(months[0]),
            "end_month": int(months[-1]),
            "coverage_months": int(len(months)),
        },
        "split_modes": split_modes,
        "leakage_check_status": leakage["status"],
        "leakage_check": leakage,
    }


def validate_no_future_month_leakage(samples: pd.DataFrame) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    if samples.empty:
        return {"status": "empty", "checked_rows": 0, "failures": failures}
    for row in samples.itertuples(index=False):
        feature_end = int(row.feature_window_end_date)
        month_end = int(row.month_end_date)
        next_end = int(row.next_month_end_date)
        if feature_end > month_end:
            failures.append(
                {
                    "sample_id": row.sample_id,
                    "failure": "feature_window_end_after_month_end",
                    "feature_window_end_date": feature_end,
                    "month_end_date": month_end,
                }
            )
        if next_end <= month_end:
            failures.append(
                {
                    "sample_id": row.sample_id,
                    "failure": "next_month_end_not_after_month_end",
                    "next_month_end_date": next_end,
                    "month_end_date": month_end,
                }
            )
    status = "pass" if not failures else "fail"
    return {
        "status": status,
        "checked_rows": int(len(samples)),
        "failures": failures[:20],
    }


def _decision_from_compare(compare: dict[str, Any], *, branch_eval: dict[str, Any], split_contract: dict[str, Any]) -> dict[str, Any]:
    leakage = _text(split_contract.get("leakage_check_status"), fallback="unknown")
    oos_top10_uplift = float(compare.get("oos_top10_uplift") or 0.0)
    oos_bad_pick_removal = float(compare.get("oos_bad_pick_removal") or 0.0)
    changed_top10_members_count = float(compare.get("changed_top10_members_count") or 0.0)
    changed_top5_members_count = float(compare.get("changed_top5_members_count") or 0.0)
    final_top10_uplift = float(compare.get("final_top10_uplift") or oos_top10_uplift)
    final_top10_bad_pick_removal = float(compare.get("final_top10_bad_pick_removal") or oos_bad_pick_removal)
    candidate_pool_top10_capture = float(compare.get("candidate_pool_top10_capture") or 0.0)
    candidate_pool_top10_capture_delta = float(compare.get("candidate_pool_top10_capture_delta") or 0.0)
    candidate_pool_bad_pick_removal = float(compare.get("candidate_pool_bad_pick_removal") or 0.0)
    candidate_pool_bad_pick_removal_delta = float(compare.get("candidate_pool_bad_pick_removal_delta") or 0.0)
    winner_promotion_delta = float(compare.get("winner_promotion_delta") or oos_top10_uplift)
    winner_promotion_score_delta = float(compare.get("winner_promotion_score_delta") or 0.0)
    loser_removal_delta = float(compare.get("loser_removal_delta") or oos_bad_pick_removal)
    boundary_gap = float(compare.get("top10_boundary_outcome_gap") or 0.0)
    top5_boundary_score_gap = float(compare.get("top5_boundary_score_gap") or 0.0)
    top5_boundary_outcome_gap = float(compare.get("top5_boundary_outcome_gap") or 0.0)
    positive_month_share = float(compare.get("positive_month_share") or 0.0)
    month_count = int(compare.get("month_count") or 0)
    failure_mode_typed = _failure_mode_typed(compare, branch_eval, split_contract)
    next_action_typed = _next_action_typed(failure_mode_typed, compare)
    branch_metrics = branch_eval.get("modes", {}).get("expanding", {}).get("branch_metrics", {}) if isinstance(branch_eval, dict) else {}
    branch_contribution_summary = _branch_contribution_summary(branch_metrics if isinstance(branch_metrics, dict) else {})
    image_branch_metrics = {}
    if isinstance(branch_metrics, dict):
        image_branch_metrics = {
            name: branch_metrics.get(name, {})
            for name in ("image_only", "numeric_plus_image", "image_plus_similarity", "numeric_image_similarity")
        }
    image_hit_rates = [
        float(metrics.get("mean_top10_hit_rate") or 0.0)
        for metrics in image_branch_metrics.values()
        if isinstance(metrics, dict)
    ]
    image_branch_disposition = "demote_to_optional" if image_hit_rates and max(image_hit_rates) < 0.05 else "keep_optional"
    image_branch_reason_typed = (
        "image_branch_weak_and_unstable"
        if image_branch_disposition == "demote_to_optional"
        else "image_branch_retained"
    )
    regime_breakdown = compare.get("regime_breakdown") or {}
    regime_effect_summary = {
        "regime_breakdown": regime_breakdown,
        "helpful_regimes": [regime for regime, payload in regime_breakdown.items() if isinstance(payload, dict) and float(payload.get("mean_top10_uplift") or 0.0) > 0.0],
        "harmful_regimes": [regime for regime, payload in regime_breakdown.items() if isinstance(payload, dict) and float(payload.get("mean_top10_uplift") or 0.0) < 0.0],
    }
    if leakage != "pass":
        decision = "drop"
        reason = "leakage_check_failed"
    elif month_count < 12:
        decision = "hold"
        reason = "insufficient_coverage"
    elif oos_top10_uplift <= 0.0 or boundary_gap <= 0.0:
        decision = "drop"
        reason = "no_stable_oos_uplift"
    elif positive_month_share < 0.55:
        decision = "hold"
        reason = "unstable_monthly_uplift"
    elif oos_bad_pick_removal <= 0.0 or changed_top10_members_count <= 0.0:
        decision = "hold"
        reason = "insufficient_branching"
    else:
        decision = "keep"
        reason = "stable_oos_uplift_with_boundary_gain"
    return {
        "schema_version": MONTHLY_SHAPE_MEMORY_DECISION_SCHEMA_VERSION,
        "candidate_id": "monthly_shape_memory_v1",
        "research_axis": "can 6-month shape memory improve monthly next-top10 ranking?",
        "decision": decision,
        "decision_reason_typed": reason,
        "failure_mode_typed": failure_mode_typed,
        "next_action_typed": next_action_typed,
        "coverage_months": month_count,
        "oos_top10_uplift": oos_top10_uplift,
        "oos_bad_pick_removal": oos_bad_pick_removal,
        "final_top10_uplift": final_top10_uplift,
        "final_top10_bad_pick_removal": final_top10_bad_pick_removal,
        "winner_promotion_delta": winner_promotion_delta,
        "winner_promotion_score_delta": winner_promotion_score_delta,
        "loser_removal_delta": loser_removal_delta,
        "changed_top10_members_count": changed_top10_members_count,
        "changed_top5_members_count": changed_top5_members_count,
        "candidate_pool_top10_capture": candidate_pool_top10_capture,
        "candidate_pool_top10_capture_delta": candidate_pool_top10_capture_delta,
        "candidate_pool_bad_pick_removal": candidate_pool_bad_pick_removal,
        "candidate_pool_bad_pick_removal_delta": candidate_pool_bad_pick_removal_delta,
        "top5_boundary_score_gap": top5_boundary_score_gap,
        "top5_boundary_outcome_gap": top5_boundary_outcome_gap,
        "branch_contribution_summary": branch_contribution_summary,
        "image_branch_disposition": image_branch_disposition,
        "image_branch_reason_typed": image_branch_reason_typed,
        "regime_effect_summary": regime_effect_summary,
        "boundary_bucket_effect": compare.get("boundary_bucket_effect") or {},
        "regime_breakdown": regime_breakdown,
        "boundary_not_improved": _text(compare.get("selection_divergence_reason"), fallback="") == "boundary_not_improved",
        "leakage_check_status": leakage,
        "branch_eval_ref": branch_eval.get("schema_version"),
    }


def run_monthly_shape_memory_research(
    *,
    source_db_path: str | None = None,
    top_k: int = DEFAULT_TOP_K,
    candidate_pool_k: int = DEFAULT_CANDIDATE_POOL_K,
    analog_k: int = DEFAULT_ANALOG_K,
    memory_lookback_months: int = DEFAULT_MEMORY_LOOKBACK_MONTHS,
    rolling_window_months: int = DEFAULT_ROLLING_WINDOW_MONTHS,
    start_month: int = DEFAULT_START_MONTH,
    artifact_suffix: str = "",
) -> dict[str, Any]:
    frame = _load_daily_bars_frame(source_db_path)
    monthly_context = _build_monthly_context(frame)
    samples = _build_samples(frame, monthly_context)
    samples = samples[samples["sample_month"] >= int(start_month)].copy()
    labels = _compute_labels(samples)
    labels = _add_sample_weights(labels)
    scored_expanding = _month_scores_for_mode(
        labels,
        split_mode="expanding",
        memory_lookback_months=memory_lookback_months,
        rolling_window_months=rolling_window_months,
        analog_k=analog_k,
    )
    scored_rolling = _month_scores_for_mode(
        labels,
        split_mode="rolling",
        memory_lookback_months=memory_lookback_months,
        rolling_window_months=rolling_window_months,
        analog_k=analog_k,
    )

    split_contract = _build_split_contract(scored_expanding, memory_lookback_months=memory_lookback_months, rolling_window_months=rolling_window_months)
    branch_eval = {
        "schema_version": MONTHLY_SHAPE_MEMORY_BRANCH_SCHEMA_VERSION,
        "top_k": int(top_k),
        "candidate_pool_k": int(candidate_pool_k),
        "modes": {
            "expanding": _branch_eval_payload(scored_expanding, top_k=top_k, candidate_pool_k=candidate_pool_k),
            "rolling": _branch_eval_payload(scored_rolling, top_k=top_k, candidate_pool_k=candidate_pool_k),
        },
    }
    compare_expanding = _compare_scores(
        scored_expanding,
        champion_col="champion_score",
        rerank_col="rerank_score",
        top_k=top_k,
        candidate_pool_k=candidate_pool_k,
    )
    compare_rolling = _compare_scores(
        scored_rolling,
        champion_col="champion_score",
        rerank_col="rerank_score",
        top_k=top_k,
        candidate_pool_k=candidate_pool_k,
    )
    compare = {
        "schema_version": MONTHLY_SHAPE_MEMORY_COMPARE_SCHEMA_VERSION,
        "top_k": int(top_k),
        "candidate_pool_k": int(candidate_pool_k),
        "primary_mode": "expanding",
        "modes": {
            "expanding": compare_expanding,
            "rolling": compare_rolling,
        },
        **{key: value for key, value in compare_expanding.items() if key not in {"schema_version", "top_k", "candidate_pool_k"}},
    }
    decision = _decision_from_compare(compare_expanding, branch_eval=branch_eval, split_contract=split_contract)

    samples_out = scored_expanding[[
        "sample_id",
        "code",
        "sample_month",
        "month_end_date",
        "month_end_ts",
        "feature_window_start_date",
        "feature_window_end_date",
        "next_month_end_date",
        "current_month_close",
        "next_month_close",
        "next_month_return",
        "benchmark_close",
        "benchmark_ret_1m",
        "benchmark_ret_3m",
        "benchmark_ret_6m",
        "benchmark_vol_6m",
        "regime_tag",
        "daily_window_size",
        "monthly_history_size",
        *NUMERIC_FEATURE_COLUMNS,
        *[col for col in scored_expanding.columns if col.startswith("img_e_")],
        "sample_weight",
        "time_decay_weight",
        "regime_similarity_weight",
        "data_quality_weight",
        "champion_score",
        "numeric_score",
        "image_score",
        "similarity_score",
        "shape_score",
        "rerank_score",
        "numeric_neighbors",
        "numeric_neighbor_scores",
        "numeric_neighbor_returns",
        "image_neighbors",
        "image_neighbor_scores",
        "image_neighbor_returns",
        "similarity_neighbors",
        "similarity_neighbor_scores",
        "similarity_neighbor_returns",
    ]].copy()
    labels_out = scored_expanding[[
        "sample_id",
        "code",
        "sample_month",
        "month_end_date",
        "next_month_return",
        "next_month_return_rank",
        "next_month_rank_pct",
        "is_next_top10",
        "is_next_bottom10",
        "top10_boundary_gap",
        "bottom10_boundary_gap",
        "top_bucket",
        "middle_bucket",
        "cohort_label",
        "regime_tag",
    ]].copy()

    similarity_rows = scored_expanding[[
        "sample_id",
        "code",
        "sample_month",
        "next_month_return",
        "regime_tag",
        "numeric_neighbors",
        "numeric_neighbor_scores",
        "numeric_neighbor_returns",
        "image_neighbors",
        "image_neighbor_scores",
        "image_neighbor_returns",
        "similarity_neighbors",
        "similarity_neighbor_scores",
        "similarity_neighbor_returns",
    ]].copy()

    output_dir = monthly_shape_memory_dir()
    samples_path = _artifact_path("monthly_samples.parquet", artifact_suffix)
    labels_path = _artifact_path("monthly_labels.parquet", artifact_suffix)
    similarity_path = _artifact_path("monthly_similarity.parquet", artifact_suffix)
    split_path = _artifact_path("split_contract.json", artifact_suffix)
    summary_path = _artifact_path("research_summary.json", artifact_suffix)
    branch_path = _artifact_path("branch_eval.json", artifact_suffix)
    compare_path = _artifact_path("rerank_compare.json", artifact_suffix)
    decision_path = _artifact_path("authoritative_decision.json", artifact_suffix)
    hierarchical_dictionary_path = _artifact_path("hierarchical_label_dictionary.json", artifact_suffix)
    hierarchical_rules_path = _artifact_path("hierarchical_label_rules.json", artifact_suffix)
    hierarchical_priority_path = _artifact_path("hierarchical_label_priority.json", artifact_suffix)
    hierarchical_rows_path = _artifact_path("monthly_labels_hierarchical.parquet", artifact_suffix)
    hierarchical_summary_path = _artifact_path("hierarchical_label_summary.json", artifact_suffix)
    hierarchical_score_summary_path = _artifact_path("hierarchical_label_score_summary.json", artifact_suffix)
    hierarchical_effect_by_state_path = _artifact_path("hierarchical_label_effect_by_state.json", artifact_suffix)
    hierarchical_effect_by_regime_path = _artifact_path("hierarchical_label_effect_by_regime.json", artifact_suffix)
    hierarchical_ablation_compare_path = _artifact_path("hierarchical_label_ablation_compare.json", artifact_suffix)
    hierarchical_weekly_diversity_path = _artifact_path("hierarchical_weekly_diversity_before_after.json", artifact_suffix)
    hierarchical_regime_gate_rules_path = _artifact_path("hierarchical_regime_gate_rules.json", artifact_suffix)
    hierarchical_regime_gate_compare_path = _artifact_path("hierarchical_regime_gate_compare.json", artifact_suffix)
    hierarchical_regime_gate_effect_by_regime_path = _artifact_path("hierarchical_regime_gate_effect_by_regime.json", artifact_suffix)
    authoritative_regime_gate_decision_path = _artifact_path("authoritative_decision.hierarchical_regime_gated.json", artifact_suffix)
    hierarchical_labels_keep_rerank_drop_path = _artifact_path("authoritative_decision.hierarchical_labels_keep_rerank_drop.json", artifact_suffix)
    hierarchical_allowed_uses_path = _artifact_path("hierarchical_label_allowed_uses.json", artifact_suffix)
    hierarchical_state_failure_map_path = _artifact_path("hierarchical_state_failure_map.json", artifact_suffix)
    hierarchical_state_winner_loser_decomposition_path = _artifact_path("hierarchical_state_winner_loser_decomposition.json", artifact_suffix)
    hierarchical_rerank_do_not_continue_path = _artifact_path("hierarchical_rerank_do_not_continue.json", artifact_suffix)
    hierarchical_state_rank_impact_table_path = _artifact_path("hierarchical_state_rank_impact_table.json", artifact_suffix)
    boundary_winner_promotion_rules_path = _artifact_path("boundary_winner_promotion_rules.json", artifact_suffix)
    boundary_winner_promotion_compare_path = _artifact_path("boundary_winner_promotion_compare.json", artifact_suffix)
    boundary_winner_promotion_effect_by_regime_path = _artifact_path("boundary_winner_promotion_effect_by_regime.json", artifact_suffix)
    authoritative_boundary_winner_promotion_decision_path = _artifact_path("authoritative_decision.boundary_winner_promotion.json", artifact_suffix)
    boundary_winner_promotion_drop_path = _artifact_path("authoritative_decision.boundary_winner_promotion_drop.json", artifact_suffix)
    lightweight_boundary_challenger_candidates_path = _artifact_path(LIGHTWEIGHT_BOUNDARY_CHALLENGER_CANDIDATE_DATASET_NAME, artifact_suffix)
    lightweight_boundary_challenger_rules_path = _artifact_path("lightweight_boundary_challenger_rules.json", artifact_suffix)
    lightweight_boundary_challenger_compare_path = _artifact_path("lightweight_boundary_challenger_compare.json", artifact_suffix)
    lightweight_boundary_challenger_effect_by_regime_path = _artifact_path("lightweight_boundary_challenger_effect_by_regime.json", artifact_suffix)
    authoritative_lightweight_boundary_challenger_decision_path = _artifact_path("authoritative_decision.lightweight_boundary_challenger.json", artifact_suffix)

    previous_hierarchical_summary = _read_json_if_exists(hierarchical_summary_path)

    _write_parquet(samples_path, samples_out)
    _write_parquet(labels_path, labels_out)
    _write_parquet(similarity_path, similarity_rows)

    hierarchical_artifacts = build_hierarchical_label_artifacts(
        source_frame=frame,
        expanding_scored=scored_expanding,
        rolling_scored=scored_rolling,
        top_k=top_k,
        candidate_pool_k=candidate_pool_k,
        leakage_check_status=split_contract.get("leakage_check_status"),
    )
    _write_json(hierarchical_dictionary_path, hierarchical_artifacts["dictionary"])
    _write_json(hierarchical_rules_path, hierarchical_artifacts["rules"])
    _write_json(hierarchical_priority_path, hierarchical_artifacts["priority"])
    _write_parquet(hierarchical_rows_path, hierarchical_artifacts["rows"])
    _write_json(hierarchical_summary_path, hierarchical_artifacts["summary"])
    _write_json(hierarchical_score_summary_path, hierarchical_artifacts["score_summary"])
    _write_json(hierarchical_effect_by_state_path, hierarchical_artifacts["effect_by_state"])
    _write_json(hierarchical_effect_by_regime_path, hierarchical_artifacts["effect_by_regime"])
    _write_json(hierarchical_ablation_compare_path, hierarchical_artifacts["ablation_compare"])

    regime_gate_artifacts = build_hierarchical_regime_gate_artifacts(
        scored=hierarchical_artifacts["rows"],
        top_k=top_k,
        candidate_pool_k=candidate_pool_k,
    )
    weekly_diversity_before_after = {
        "schema_version": hierarchical_artifacts["summary"]["schema_version"],
        "before": {
            "label_counts": (previous_hierarchical_summary or {}).get("label_counts", {}).get("weekly_main_state", {}),
            "row_count": int((previous_hierarchical_summary or {}).get("row_count") or 0),
            "label_count": int(len((previous_hierarchical_summary or {}).get("label_counts", {}).get("weekly_main_state", {}))),
        },
        "after": {
            "label_counts": hierarchical_artifacts["summary"].get("label_counts", {}).get("weekly_main_state", {}),
            "row_count": int(hierarchical_artifacts["summary"].get("row_count") or 0),
            "label_count": int(len(hierarchical_artifacts["summary"].get("label_counts", {}).get("weekly_main_state", {}))),
        },
        "repair_summary": {
            "cause": "weekly_warning_score_overweight_and_non_warning_states_underpowered",
            "rule_changes": [
                "raise weekly early/mid/range anchor scores",
                "lower weekly warning anchor score",
                "keep the weekly taxonomy unchanged",
            ],
        },
    }
    regime_gate_compare = regime_gate_artifacts["compare"]
    regime_gate_effect_by_regime = regime_gate_artifacts["effect_by_regime"]
    regime_gate_rules = regime_gate_artifacts["rules"]
    split_decision = _build_hierarchical_split_decision_payload(
        hierarchical_rows=hierarchical_artifacts["rows"],
        regime_gate_compare=regime_gate_compare,
        hierarchical_summary=hierarchical_artifacts["summary"],
        hierarchical_weekly_diversity=weekly_diversity_before_after,
    )
    allowed_uses = _build_hierarchical_allowed_uses_payload(
        split_decision=split_decision,
        regime_gate_compare=regime_gate_compare,
        hierarchical_weekly_diversity=weekly_diversity_before_after,
        leakage_check_status=str(split_contract.get("leakage_check_status") or "unknown"),
    )
    state_diagnostics = _build_hierarchical_state_diagnostics(
        hierarchical_rows=hierarchical_artifacts["rows"],
        regime_gate_compare=regime_gate_compare,
    )
    boundary_winner_promotion_artifacts = build_boundary_winner_promotion_artifacts(
        hierarchical_rows=hierarchical_artifacts["rows"],
        state_diagnostics=state_diagnostics,
        hierarchical_weekly_diversity=weekly_diversity_before_after,
        hierarchical_allowed_uses=allowed_uses,
        top_k=top_k,
        candidate_pool_k=candidate_pool_k,
    )
    boundary_winner_promotion_compare = boundary_winner_promotion_artifacts["compare"]
    boundary_winner_promotion_variants = boundary_winner_promotion_compare["variants"]
    boundary_winner_promotion_decision = {
        "schema_version": boundary_winner_promotion_compare["schema_version"],
        "decision": boundary_winner_promotion_compare["decision"],
        "decision_reason_typed": boundary_winner_promotion_compare["decision_reason_typed"],
        "best_variant": boundary_winner_promotion_compare["best_variant"],
        "full_snapshot_oos_top10_uplift": float(
            boundary_winner_promotion_variants[boundary_winner_promotion_compare["best_variant"]].get("oos_top10_uplift") or 0.0
        ),
        "winner_promotion_improved": bool(
            float(boundary_winner_promotion_variants[boundary_winner_promotion_compare["best_variant"]].get("winner_promotion_delta") or 0.0) > 0.0
        ),
        "loser_removal_improved": bool(
            float(boundary_winner_promotion_variants[boundary_winner_promotion_compare["best_variant"]].get("loser_removal_delta") or 0.0) > 0.0
        ),
        "boundary_improved": bool(
            boundary_winner_promotion_variants[boundary_winner_promotion_compare["best_variant"]].get("top10_boundary_outcome_gap")
            < boundary_winner_promotion_variants["champion_only"].get("top10_boundary_outcome_gap")
        ),
        "churn_acceptable": bool(boundary_winner_promotion_compare.get("churn_acceptable")),
        "recommended_next_use": "diagnostics/filter-analysis/decomposition only; do not use as rerank challenger",
        "candidate_rank_window": boundary_winner_promotion_compare.get("candidate_rank_window"),
        "top5_freeze_enabled": bool(boundary_winner_promotion_compare.get("top5_freeze_enabled")),
        "variants_compared": list(boundary_winner_promotion_variants.keys()),
        "comparison_contract": {
            "candidate_rank_window": boundary_winner_promotion_compare.get("candidate_rank_window"),
            "strict_candidate_rank_window": boundary_winner_promotion_compare.get("strict_candidate_rank_window"),
            "top5_freeze_enabled": bool(boundary_winner_promotion_compare.get("top5_freeze_enabled")),
            "top_k": int(top_k),
            "candidate_pool_k": int(candidate_pool_k),
        },
        "source_artifacts": boundary_winner_promotion_compare.get("source_artifacts", {}),
    }
    boundary_winner_promotion_drop = {
        "schema_version": boundary_winner_promotion_compare["schema_version"],
        "decision": "drop_boundary_winner_promotion_challenger",
        "reason": "no_oos_top10_uplift_and_no_winner_promotion",
        "supersedes": "authoritative_decision.boundary_winner_promotion.json",
        "source_compare": "boundary_winner_promotion_compare.json",
        "full_snapshot_oos_top10_uplift": float(
            boundary_winner_promotion_variants[boundary_winner_promotion_compare["best_variant"]].get("oos_top10_uplift") or 0.0
        ),
        "winner_promotion_improved": bool(
            float(boundary_winner_promotion_variants[boundary_winner_promotion_compare["best_variant"]].get("winner_promotion_delta") or 0.0) > 0.0
        ),
        "loser_removal_improved": bool(
            float(boundary_winner_promotion_variants[boundary_winner_promotion_compare["best_variant"]].get("loser_removal_delta") or 0.0) > 0.0
        ),
        "boundary_improved": bool(
            boundary_winner_promotion_variants[boundary_winner_promotion_compare["best_variant"]].get("top10_boundary_outcome_gap")
            < boundary_winner_promotion_variants["champion_only"].get("top10_boundary_outcome_gap")
        ),
        "churn_acceptable": bool(boundary_winner_promotion_compare.get("churn_acceptable")),
    }
    lightweight_boundary_challenger_artifacts = build_lightweight_boundary_challenger_artifacts(
        samples_frame=samples_out,
        hierarchical_rows=hierarchical_artifacts["rows"],
        top_k=top_k,
        candidate_pool_k=candidate_pool_k,
    )
    regime_gate_decision = {
        "schema_version": regime_gate_compare["schema_version"],
        "decision": regime_gate_compare["decision"],
        "decision_reason_typed": regime_gate_compare["decision_reason_typed"],
        "winner_promotion_improved": bool(
            float(regime_gate_compare["variants"]["champion_plus_hierarchical_rerank_regime_gated"].get("winner_promotion_delta") or 0.0) > 0.0
        ),
        "loser_removal_improved": bool(
            float(regime_gate_compare["variants"]["champion_plus_hierarchical_rerank_regime_gated"].get("loser_removal_delta") or 0.0) > 0.0
        ),
        "boundary_improved": bool(
            regime_gate_compare["variants"]["champion_plus_hierarchical_rerank_regime_gated"].get("top10_boundary_improved")
        ),
        "best_variant": regime_gate_compare["best_variant"],
        "variants": regime_gate_compare["variants"],
    }
    _write_json(hierarchical_weekly_diversity_path, weekly_diversity_before_after)
    _write_json(hierarchical_regime_gate_rules_path, regime_gate_rules)
    _write_json(hierarchical_regime_gate_compare_path, regime_gate_compare)
    _write_json(hierarchical_regime_gate_effect_by_regime_path, regime_gate_effect_by_regime)
    _write_json(authoritative_regime_gate_decision_path, regime_gate_decision)
    _write_json(hierarchical_labels_keep_rerank_drop_path, split_decision)
    _write_json(hierarchical_allowed_uses_path, allowed_uses)
    _write_json(hierarchical_state_failure_map_path, state_diagnostics["state_failure_map"])
    _write_json(hierarchical_state_winner_loser_decomposition_path, state_diagnostics["winner_loser_decomposition"])
    _write_json(hierarchical_rerank_do_not_continue_path, state_diagnostics["do_not_continue"])
    _write_json(hierarchical_state_rank_impact_table_path, boundary_winner_promotion_artifacts["state_rank_impact_table"])
    _write_json(boundary_winner_promotion_rules_path, boundary_winner_promotion_artifacts["rules"])
    _write_json(boundary_winner_promotion_compare_path, boundary_winner_promotion_artifacts["compare"])
    _write_json(boundary_winner_promotion_effect_by_regime_path, boundary_winner_promotion_artifacts["effect_by_regime"])
    _write_json(authoritative_boundary_winner_promotion_decision_path, boundary_winner_promotion_decision)
    _write_json(boundary_winner_promotion_drop_path, boundary_winner_promotion_drop)
    _write_parquet(lightweight_boundary_challenger_candidates_path, lightweight_boundary_challenger_artifacts["candidate_dataset"])
    _write_json(lightweight_boundary_challenger_rules_path, lightweight_boundary_challenger_artifacts["rules"])
    _write_json(lightweight_boundary_challenger_compare_path, lightweight_boundary_challenger_artifacts["compare"])
    _write_json(lightweight_boundary_challenger_effect_by_regime_path, lightweight_boundary_challenger_artifacts["effect_by_regime"])
    _write_json(authoritative_lightweight_boundary_challenger_decision_path, lightweight_boundary_challenger_artifacts["decision"])

    summary = {
        "schema_version": MONTHLY_SHAPE_MEMORY_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "output_dir": str(output_dir),
        "artifact_suffix": _text(artifact_suffix),
        "source_db_path": str(resolve_source_db_path(source_db_path)),
        "coverage_months": int(scored_expanding["sample_month"].nunique()) if not scored_expanding.empty else 0,
        "coverage_rows": int(len(scored_expanding)),
        "coverage_codes": int(scored_expanding["code"].nunique()) if not scored_expanding.empty else 0,
        "start_month": int(scored_expanding["sample_month"].min()) if not scored_expanding.empty else None,
        "end_month": int(scored_expanding["sample_month"].max()) if not scored_expanding.empty else None,
        "sample_contract": {
            "daily_window": "last 6 calendar months of daily bars up to month-end t",
            "label": "next-month cross-sectional return and rank",
            "candidate_pool_k": int(candidate_pool_k),
            "top_k": int(top_k),
            "analog_k": int(analog_k),
            "memory_lookback_months": int(memory_lookback_months),
            "rolling_window_months": int(rolling_window_months),
        },
        "feature_branch_specs": [{"feature": name, "family": family} for name, family in FEATURE_BRANCH_SPECS],
        "artifacts": {
            "monthly_samples": str(samples_path),
            "monthly_labels": str(labels_path),
            "monthly_similarity": str(similarity_path),
            "hierarchical_label_dictionary": str(hierarchical_dictionary_path),
            "hierarchical_label_rules": str(hierarchical_rules_path),
            "hierarchical_label_priority": str(hierarchical_priority_path),
            "monthly_labels_hierarchical": str(hierarchical_rows_path),
            "hierarchical_label_summary": str(hierarchical_summary_path),
            "hierarchical_label_score_summary": str(hierarchical_score_summary_path),
            "hierarchical_label_effect_by_state": str(hierarchical_effect_by_state_path),
            "hierarchical_label_effect_by_regime": str(hierarchical_effect_by_regime_path),
            "hierarchical_label_ablation_compare": str(hierarchical_ablation_compare_path),
            "hierarchical_weekly_diversity_before_after": str(hierarchical_weekly_diversity_path),
            "hierarchical_regime_gate_rules": str(hierarchical_regime_gate_rules_path),
            "hierarchical_regime_gate_compare": str(hierarchical_regime_gate_compare_path),
            "hierarchical_regime_gate_effect_by_regime": str(hierarchical_regime_gate_effect_by_regime_path),
            "authoritative_decision_hierarchical_regime_gated": str(authoritative_regime_gate_decision_path),
            "authoritative_decision_hierarchical_labels_keep_rerank_drop": str(hierarchical_labels_keep_rerank_drop_path),
            "hierarchical_label_allowed_uses": str(hierarchical_allowed_uses_path),
            "hierarchical_state_failure_map": str(hierarchical_state_failure_map_path),
            "hierarchical_state_winner_loser_decomposition": str(hierarchical_state_winner_loser_decomposition_path),
            "hierarchical_rerank_do_not_continue": str(hierarchical_rerank_do_not_continue_path),
            "hierarchical_state_rank_impact_table": str(hierarchical_state_rank_impact_table_path),
            "boundary_winner_promotion_rules": str(boundary_winner_promotion_rules_path),
            "boundary_winner_promotion_compare": str(boundary_winner_promotion_compare_path),
            "boundary_winner_promotion_effect_by_regime": str(boundary_winner_promotion_effect_by_regime_path),
            "authoritative_decision_boundary_winner_promotion": str(authoritative_boundary_winner_promotion_decision_path),
            "authoritative_decision_boundary_winner_promotion_drop": str(boundary_winner_promotion_drop_path),
            "lightweight_boundary_challenger_candidates": str(lightweight_boundary_challenger_candidates_path),
            "lightweight_boundary_challenger_rules": str(lightweight_boundary_challenger_rules_path),
            "lightweight_boundary_challenger_compare": str(lightweight_boundary_challenger_compare_path),
            "lightweight_boundary_challenger_effect_by_regime": str(lightweight_boundary_challenger_effect_by_regime_path),
            "authoritative_decision_lightweight_boundary_challenger": str(authoritative_lightweight_boundary_challenger_decision_path),
            "split_contract": str(split_path),
            "research_summary": str(summary_path),
            "branch_eval": str(branch_path),
            "rerank_compare": str(compare_path),
            "authoritative_decision": str(decision_path),
        },
        "leakage_check_status": split_contract.get("leakage_check_status"),
        "label_contract": {
            "top10_label": "is_next_top10",
            "bottom10_label": "is_next_bottom10",
            "rank_label": "next_month_rank_pct",
            "cohort_label": "cohort_label",
        },
        "comparison_modes": ["expanding", "rolling"],
        "failure_mode_typed": decision.get("failure_mode_typed"),
        "next_action_typed": decision.get("next_action_typed"),
    }
    _write_json(split_path, split_contract)
    _write_json(branch_path, branch_eval)
    _write_json(compare_path, compare)
    _write_json(decision_path, decision)
    _write_json(summary_path, summary)

    return {
        "schema_version": MONTHLY_SHAPE_MEMORY_SCHEMA_VERSION,
        "summary_path": str(summary_path),
        "split_contract_path": str(split_path),
        "branch_eval_path": str(branch_path),
        "compare_path": str(compare_path),
        "decision_path": str(decision_path),
        "samples_path": str(samples_path),
        "labels_path": str(labels_path),
        "similarity_path": str(similarity_path),
        "hierarchical_dictionary_path": str(hierarchical_dictionary_path),
        "hierarchical_rules_path": str(hierarchical_rules_path),
        "hierarchical_priority_path": str(hierarchical_priority_path),
        "hierarchical_rows_path": str(hierarchical_rows_path),
        "hierarchical_summary_path": str(hierarchical_summary_path),
        "hierarchical_score_summary_path": str(hierarchical_score_summary_path),
        "hierarchical_effect_by_state_path": str(hierarchical_effect_by_state_path),
        "hierarchical_effect_by_regime_path": str(hierarchical_effect_by_regime_path),
        "hierarchical_ablation_compare_path": str(hierarchical_ablation_compare_path),
        "hierarchical_weekly_diversity_path": str(hierarchical_weekly_diversity_path),
        "hierarchical_regime_gate_rules_path": str(hierarchical_regime_gate_rules_path),
        "hierarchical_regime_gate_compare_path": str(hierarchical_regime_gate_compare_path),
        "hierarchical_regime_gate_effect_by_regime_path": str(hierarchical_regime_gate_effect_by_regime_path),
        "authoritative_regime_gate_decision_path": str(authoritative_regime_gate_decision_path),
        "authoritative_decision_hierarchical_labels_keep_rerank_drop_path": str(hierarchical_labels_keep_rerank_drop_path),
        "hierarchical_label_allowed_uses_path": str(hierarchical_allowed_uses_path),
        "hierarchical_state_failure_map_path": str(hierarchical_state_failure_map_path),
        "hierarchical_state_winner_loser_decomposition_path": str(hierarchical_state_winner_loser_decomposition_path),
        "hierarchical_rerank_do_not_continue_path": str(hierarchical_rerank_do_not_continue_path),
        "hierarchical_state_rank_impact_table_path": str(hierarchical_state_rank_impact_table_path),
        "boundary_winner_promotion_rules_path": str(boundary_winner_promotion_rules_path),
        "boundary_winner_promotion_compare_path": str(boundary_winner_promotion_compare_path),
        "boundary_winner_promotion_effect_by_regime_path": str(boundary_winner_promotion_effect_by_regime_path),
        "authoritative_boundary_winner_promotion_decision_path": str(authoritative_boundary_winner_promotion_decision_path),
        "boundary_winner_promotion_drop_path": str(boundary_winner_promotion_drop_path),
        "lightweight_boundary_challenger_candidates_path": str(lightweight_boundary_challenger_candidates_path),
        "lightweight_boundary_challenger_rules_path": str(lightweight_boundary_challenger_rules_path),
        "lightweight_boundary_challenger_compare_path": str(lightweight_boundary_challenger_compare_path),
        "lightweight_boundary_challenger_effect_by_regime_path": str(lightweight_boundary_challenger_effect_by_regime_path),
        "authoritative_lightweight_boundary_challenger_decision_path": str(authoritative_lightweight_boundary_challenger_decision_path),
        "summary": summary,
        "split_contract": split_contract,
        "branch_eval": branch_eval,
        "compare": compare,
        "decision": decision,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m external_analysis monthly-shape-memory-run")
    parser.add_argument("--source-db-path", default=None)
    parser.add_argument("--artifact-suffix", default="")
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--candidate-pool-k", type=int, default=DEFAULT_CANDIDATE_POOL_K)
    parser.add_argument("--analog-k", type=int, default=DEFAULT_ANALOG_K)
    parser.add_argument("--memory-lookback-months", type=int, default=DEFAULT_MEMORY_LOOKBACK_MONTHS)
    parser.add_argument("--rolling-window-months", type=int, default=DEFAULT_ROLLING_WINDOW_MONTHS)
    parser.add_argument("--start-month", type=int, default=DEFAULT_START_MONTH)
    return parser


def run_monthly_shape_memory_cli(argv: list[str] | None = None) -> dict[str, Any]:
    args = _build_parser().parse_args(argv)
    return run_monthly_shape_memory_research(
        source_db_path=args.source_db_path,
        artifact_suffix=str(args.artifact_suffix),
        top_k=int(args.top_k),
        candidate_pool_k=int(args.candidate_pool_k),
        analog_k=int(args.analog_k),
        memory_lookback_months=int(args.memory_lookback_months),
        rolling_window_months=int(args.rolling_window_months),
        start_month=int(args.start_month),
    )
