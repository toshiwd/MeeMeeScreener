from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE_SESSION = Path(r"G:\Tradex\ma_position_path_research\20260429T054053Z-c90c5fdf")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma_position_path_research_family_filter")

SCHEMA_VERSION = "tradex_ma_state_family_stability_filter_v1"
SUMMARY_SCHEMA_VERSION = "tradex_ma_state_family_summary_v1"
BY_REGIME_SCHEMA_VERSION = "tradex_ma_state_family_by_regime_v1"
MONTHLY_SCHEMA_VERSION = "tradex_ma_state_family_monthly_stability_v1"
CLASSIFICATION_SCHEMA_VERSION = "tradex_ma_state_family_classification_v1"
DECISION_SCHEMA_VERSION = "tradex_ma_state_family_filter_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_ma_state_family_manifest_v1"

CONFIRMED_REGIME_SOURCE = "confirmed_market_regime_daily"
PROVISIONAL_REGIME_SOURCE = "provisional_regime_proxy"

DEFAULT_MIN_SAMPLE_COUNT = 300
DEFAULT_MIN_UNIQUE_SYMBOL_COUNT = 30
DEFAULT_MIN_MONTH_COUNT = 12
DEFAULT_LIMIT_FAMILIES = 50


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
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    if value is None:
        return fallback
    try:
        out = float(value)
    except Exception:
        return fallback
    if not math.isfinite(out):
        return fallback
    return float(out)


def _safe_int(value: Any, fallback: int | None = None) -> int | None:
    if value is None:
        return fallback
    try:
        return int(value)
    except Exception:
        return fallback


def _progress_log(message: str) -> None:
    print(f"[ma_state_family_filter] {message}", file=sys.stderr, flush=True)


def _make_session_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _resolve_source_session(source_session: str | Path | None) -> Path:
    if source_session and str(source_session).strip():
        path = Path(str(source_session)).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"source session not found: {path}")
        return path
    if DEFAULT_SOURCE_SESSION.exists():
        return DEFAULT_SOURCE_SESSION.resolve()
    raise FileNotFoundError("Could not resolve source session. Pass --source-session.")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_source_session(source_session: Path) -> dict[str, Any]:
    manifest = _load_json(source_session / "run_manifest.json")
    summary = _load_json(source_session / "position_state_value_summary.json")
    by_regime = _load_json(source_session / "position_state_value_by_regime.json")
    monthly = _load_json(source_session / "position_state_monthly_stability.json")
    classification = _load_json(source_session / "position_state_classification.json")
    decision = _load_json(source_session / "ma_candle_position_value_v1_decision.json")
    row_parquet = source_session / "position_state_forward_path_rows.parquet"
    if not row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {row_parquet}")
    return {
        "manifest": manifest,
        "summary": summary,
        "by_regime": by_regime,
        "monthly": monthly,
        "classification": classification,
        "decision": decision,
        "row_parquet": row_parquet,
    }


def _streak_bucket_expr(token_expr: str) -> str:
    number_expr = f"CAST(NULLIF(regexp_extract({token_expr}, '([0-9]+)', 1), '') AS INTEGER)"
    bucket_expr = f"""
        CASE
            WHEN {token_expr} IS NULL OR {token_expr} = '' THEN 'U'
            WHEN {number_expr} <= 0 THEN '0'
            WHEN {number_expr} <= 2 THEN '1_2'
            WHEN {number_expr} <= 6 THEN '3_6'
            WHEN {number_expr} <= 13 THEN '7_13'
            ELSE '14p'
        END
    """.strip()
    return f"CASE WHEN {token_expr} LIKE '-%' THEN 'B' ELSE 'A' END || ({bucket_expr})"


def _family_row_select_sql(row_parquet: Path) -> str:
    source = row_parquet.as_posix()
    return f"""
    WITH src AS (
        SELECT
            code,
            trade_date,
            position_state_id,
            regime_source,
            regime_label,
            entry_next_open,
            entry_day_close,
            forward_window_days,
            candle_state_code,
            volume_condition,
            forward_ret_3d,
            forward_ret_5d,
            forward_ret_10d,
            forward_ret_20d,
            mfe_20d,
            mae_20d,
            days_to_mfe_20d,
            days_to_mae_20d,
            days_to_positive_close,
            days_to_plus_3pct,
            days_to_plus_5pct,
            days_to_minus_3pct,
            days_to_minus_5pct,
            hit_plus_5_before_minus_5,
            hit_minus_5_before_plus_5,
            hit_plus_3_before_minus_3,
            hit_minus_3_before_plus_3,
            hit_plus_1atr_before_minus_1atr,
            mfe_atr_20d,
            mae_atr_20d,
            close_above_entry_days_20d,
            close_below_entry_days_20d,
            path_value_score_v1,
            body_norm_atr,
            upper_wick_ratio,
            lower_wick_ratio,
            volume,
            regexp_extract(position_state_id, 'c7=([^|]+)', 1) AS raw_c7,
            regexp_extract(position_state_id, 'c20=([^|]+)', 1) AS raw_c20,
            regexp_extract(position_state_id, 'c60=([^|]+)', 1) AS raw_c60,
            regexp_extract(position_state_id, 'b7=([^|]+)', 1) AS raw_b7,
            regexp_extract(position_state_id, 'b20=([^|]+)', 1) AS raw_b20,
            regexp_extract(position_state_id, 'b60=([^|]+)', 1) AS raw_b60,
            regexp_extract(position_state_id, 'stk=([^|]+)', 1) AS raw_stk,
            regexp_extract(position_state_id, 's7=([^|]+)', 1) AS raw_s7,
            regexp_extract(position_state_id, 's20=([^|]+)', 1) AS raw_s20,
            regexp_extract(position_state_id, 's60=([^|]+)', 1) AS raw_s60,
            regexp_extract(position_state_id, 'st7=([^|]+)', 1) AS raw_st7,
            regexp_extract(position_state_id, 'st20=([^|]+)', 1) AS raw_st20,
            regexp_extract(position_state_id, 'st60=([^|]+)', 1) AS raw_st60,
            regexp_extract(position_state_id, 'cd=([^|]+)', 1) AS raw_cd,
            regexp_extract(position_state_id, 'p20=([^|]+)', 1) AS raw_p20,
            regexp_extract(position_state_id, 'p60=([^|]+)', 1) AS raw_p60,
            regexp_extract(position_state_id, 'vol=([^|]+)', 1) AS raw_vol,
            CAST(SUBSTR(CAST(trade_date AS VARCHAR), 1, 6) AS INTEGER) AS trade_month,
            CASE WHEN regime_source = '{CONFIRMED_REGIME_SOURCE}' THEN 'C:' || regime_label ELSE 'P:' || regime_label END AS family_regime_context,
            CASE WHEN regexp_extract(position_state_id, 'cd=([^|]+)', 1) = 'LBB' THEN 'large_bullish'
                 WHEN regexp_extract(position_state_id, 'cd=([^|]+)', 1) = 'LBR' THEN 'large_bearish'
                 ELSE 'neutral_small' END AS family_candle_strength,
            CASE WHEN regexp_extract(position_state_id, 'cd=([^|]+)', 1) = 'GU' THEN 'gap_up'
                 WHEN regexp_extract(position_state_id, 'cd=([^|]+)', 1) = 'GD' THEN 'gap_down'
                 ELSE 'no_major_gap' END AS family_gap_group,
            CASE WHEN regexp_extract(position_state_id, 'p20=([^|]+)', 1) = 'H' AND regexp_extract(position_state_id, 'p60=([^|]+)', 1) = 'H' THEN 'near_high'
                 WHEN regexp_extract(position_state_id, 'p20=([^|]+)', 1) = 'L' AND regexp_extract(position_state_id, 'p60=([^|]+)', 1) = 'L' THEN 'near_low'
                 ELSE 'middle' END AS family_price_location,
            {_streak_bucket_expr("regexp_extract(position_state_id, 'st7=([^|]+)', 1)")} AS family_streak_7_bucket,
            {_streak_bucket_expr("regexp_extract(position_state_id, 'st20=([^|]+)', 1)")} AS family_streak_20_bucket,
            {_streak_bucket_expr("regexp_extract(position_state_id, 'st60=([^|]+)', 1)")} AS family_streak_60_bucket
        FROM read_parquet('{source}')
    )
    SELECT
        *,
        raw_c7 || '|' || raw_c20 || '|' || raw_c60 || '|' || raw_stk || '|' || raw_s20 || '|' || raw_s60 || '|' ||
        family_streak_7_bucket || '|' || family_streak_20_bucket || '|' || family_streak_60_bucket || '|' ||
        family_candle_strength || '|' || family_gap_group || '|' || family_price_location || '|' || raw_vol AS state_family_id
    FROM src
    """


def _cardinality_report(conn: duckdb.DuckDBPyConnection, row_parquet: Path) -> dict[str, Any]:
    q = f"""
    WITH src AS (
        SELECT
            position_state_id,
            regime_source,
            regime_label,
            regexp_extract(position_state_id, 'c7=([^|]+)', 1) AS c7,
            regexp_extract(position_state_id, 'c20=([^|]+)', 1) AS c20,
            regexp_extract(position_state_id, 'c60=([^|]+)', 1) AS c60,
            regexp_extract(position_state_id, 'b7=([^|]+)', 1) AS b7,
            regexp_extract(position_state_id, 'b20=([^|]+)', 1) AS b20,
            regexp_extract(position_state_id, 'b60=([^|]+)', 1) AS b60,
            regexp_extract(position_state_id, 'stk=([^|]+)', 1) AS stk,
            regexp_extract(position_state_id, 's7=([^|]+)', 1) AS s7,
            regexp_extract(position_state_id, 's20=([^|]+)', 1) AS s20,
            regexp_extract(position_state_id, 's60=([^|]+)', 1) AS s60,
            regexp_extract(position_state_id, 'st7=([^|]+)', 1) AS st7,
            regexp_extract(position_state_id, 'st20=([^|]+)', 1) AS st20,
            regexp_extract(position_state_id, 'st60=([^|]+)', 1) AS st60,
            regexp_extract(position_state_id, 'cd=([^|]+)', 1) AS cd,
            regexp_extract(position_state_id, 'p20=([^|]+)', 1) AS p20,
            regexp_extract(position_state_id, 'p60=([^|]+)', 1) AS p60,
            regexp_extract(position_state_id, 'vol=([^|]+)', 1) AS vol
        FROM read_parquet('{row_parquet.as_posix()}')
    )
    SELECT
        COUNT(DISTINCT position_state_id) AS raw_state_count,
        COUNT(DISTINCT c7) AS c7_cardinality,
        COUNT(DISTINCT c20) AS c20_cardinality,
        COUNT(DISTINCT c60) AS c60_cardinality,
        COUNT(DISTINCT b7) AS b7_cardinality,
        COUNT(DISTINCT b20) AS b20_cardinality,
        COUNT(DISTINCT b60) AS b60_cardinality,
        COUNT(DISTINCT stk) AS stk_cardinality,
        COUNT(DISTINCT s7) AS s7_cardinality,
        COUNT(DISTINCT s20) AS s20_cardinality,
        COUNT(DISTINCT s60) AS s60_cardinality,
        COUNT(DISTINCT st7) AS st7_cardinality,
        COUNT(DISTINCT st20) AS st20_cardinality,
        COUNT(DISTINCT st60) AS st60_cardinality,
        COUNT(DISTINCT cd) AS cd_cardinality,
        COUNT(DISTINCT p20) AS p20_cardinality,
        COUNT(DISTINCT p60) AS p60_cardinality,
        COUNT(DISTINCT vol) AS vol_cardinality,
        COUNT(DISTINCT regime_source) AS regime_source_cardinality,
        COUNT(DISTINCT regime_label) AS regime_label_cardinality
    FROM src
    """
    row = conn.execute(q).fetchone()
    keys = [
        "raw_state_count",
        "c7_cardinality",
        "c20_cardinality",
        "c60_cardinality",
        "b7_cardinality",
        "b20_cardinality",
        "b60_cardinality",
        "stk_cardinality",
        "s7_cardinality",
        "s20_cardinality",
        "s60_cardinality",
        "st7_cardinality",
        "st20_cardinality",
        "st60_cardinality",
        "cd_cardinality",
        "p20_cardinality",
        "p60_cardinality",
        "vol_cardinality",
        "regime_source_cardinality",
        "regime_label_cardinality",
    ]
    return {key: int(value or 0) for key, value in zip(keys, row, strict=False)}


def _family_summary_frame(conn: duckdb.DuckDBPyConnection, *, top15_threshold: float, bottom15_threshold: float) -> pd.DataFrame:
    return conn.execute(
        f"""
        SELECT
            fr.state_family_id,
            COUNT(*) AS sample_count,
            COUNT(DISTINCT code) AS unique_symbol_count,
            COUNT(DISTINCT trade_month) AS month_count,
            AVG(forward_ret_5d) AS mean_forward_ret_5d,
            AVG(forward_ret_10d) AS mean_forward_ret_10d,
            AVG(forward_ret_20d) AS mean_forward_ret_20d,
            MEDIAN(forward_ret_20d) AS median_forward_ret_20d,
            AVG(mfe_20d) AS mean_mfe_20d,
            AVG(mae_20d) AS mean_mae_20d,
            AVG(path_value_score_v1) AS mean_path_value_score_v1,
            MEDIAN(path_value_score_v1) AS median_path_value_score_v1,
            AVG(hit_plus_5_before_minus_5) AS plus5_before_minus5_rate,
            AVG(hit_minus_5_before_plus_5) AS minus5_before_plus5_rate,
            AVG(CASE WHEN path_value_score_v1 >= {top15_threshold} THEN 1 ELSE 0 END) AS top15_rate,
            AVG(CASE WHEN path_value_score_v1 <= {bottom15_threshold} THEN 1 ELSE 0 END) AS bottom15_rate
        FROM family_rows fr
        GROUP BY fr.state_family_id
        ORDER BY mean_path_value_score_v1 DESC, sample_count DESC, mean_forward_ret_20d DESC
        """
    ).fetchdf()


def _family_regime_frame(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        WITH family_regime AS (
            SELECT
                state_family_id,
                family_regime_context,
                COUNT(*) AS sample_count,
                COUNT(DISTINCT code) AS unique_symbol_count,
                AVG(forward_ret_20d) AS mean_forward_ret_20d,
                AVG(mfe_20d) AS mean_mfe_20d,
                AVG(mae_20d) AS mean_mae_20d,
                AVG(path_value_score_v1) AS mean_path_value_score_v1,
                MIN(path_value_score_v1) AS min_path_value_score_v1,
                MAX(path_value_score_v1) AS max_path_value_score_v1
            FROM family_rows
            GROUP BY 1, 2
        )
        SELECT
            state_family_id,
            family_regime_context,
            sample_count,
            unique_symbol_count,
            mean_forward_ret_20d,
            mean_mfe_20d,
            mean_mae_20d,
            mean_path_value_score_v1,
            max_path_value_score_v1 - min_path_value_score_v1 AS score_spread
        FROM family_regime
        ORDER BY score_spread DESC, sample_count DESC, mean_path_value_score_v1 DESC
        """
    ).fetchdf()


def _family_monthly_frame(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(
        """
        WITH monthly AS (
            SELECT
                state_family_id,
                trade_month,
                COUNT(*) AS sample_count,
                COUNT(DISTINCT code) AS unique_symbol_count,
                AVG(path_value_score_v1) AS month_mean_path_value,
                AVG(forward_ret_20d) AS month_mean_forward_ret_20d,
                AVG(mae_20d) AS month_mean_mae_20d
            FROM family_rows
            GROUP BY 1, 2
        )
        SELECT
            state_family_id,
            COUNT(*) AS months_observed,
            AVG(CASE WHEN month_mean_path_value > 0 THEN 1 ELSE 0 END) AS positive_month_rate,
            MIN(month_mean_path_value) AS worst_month_mean_path_value,
            MAX(month_mean_path_value) AS best_month_mean_path_value,
            AVG(month_mean_path_value) AS mean_monthly_path_value,
            STDDEV_SAMP(month_mean_path_value) AS std_monthly_path_value,
            SUM(sample_count) AS month_sample_count
        FROM monthly
        GROUP BY 1
        ORDER BY std_monthly_path_value ASC, month_sample_count DESC
        """
    ).fetchdf()


def _classify_family(row: pd.Series, *, thresholds: dict[str, Any]) -> str:
    sample_count = _safe_int(row.get("sample_count"), 0) or 0
    symbol_count = _safe_int(row.get("unique_symbol_count"), 0) or 0
    month_count = _safe_int(row.get("month_count"), 0) or 0
    if sample_count < thresholds["min_sample_count"] or symbol_count < thresholds["min_unique_symbol_count"] or month_count < thresholds["min_month_count"]:
        return "unstable_or_sparse_family"
    mean_score = _safe_float(row.get("mean_path_value_score_v1"))
    median_score = _safe_float(row.get("median_path_value_score_v1"))
    plus5 = _safe_float(row.get("plus5_before_minus5_rate"))
    minus5 = _safe_float(row.get("minus5_before_plus5_rate"))
    bottom15 = _safe_float(row.get("bottom15_rate"))
    positive_month = _safe_float(row.get("positive_month_rate"))
    regime_count = _safe_int(row.get("regime_count"), 0) or 0
    regime_consistency = _safe_float(row.get("regime_consistency_score"))
    score_spread = _safe_float(row.get("score_spread"))
    if (
        mean_score is not None
        and median_score is not None
        and plus5 is not None
        and bottom15 is not None
        and positive_month is not None
        and mean_score > thresholds["baseline_mean_path_value_score_v1"]
        and median_score > 0
        and plus5 > thresholds["baseline_plus5_before_minus5_rate"]
        and bottom15 < thresholds["baseline_bottom15_rate"]
        and positive_month >= 0.55
    ):
        return "stable_high_value_family"
    if (
        mean_score is not None
        and median_score is not None
        and minus5 is not None
        and bottom15 is not None
        and positive_month is not None
        and mean_score < thresholds["baseline_mean_path_value_score_v1"]
        and median_score < 0
        and minus5 > thresholds["baseline_minus5_before_plus5_rate"]
        and bottom15 > thresholds["baseline_bottom15_rate"]
        and positive_month <= 0.45
    ):
        return "stable_bad_pick_family"
    if regime_count >= 2 and regime_consistency is not None and regime_consistency < 0.7 and score_spread is not None and score_spread >= 0.02:
        return "regime_dependent_family"
    return "neutral_family"


def _top_examples(frame: pd.DataFrame, *, class_name: str, limit: int) -> list[dict[str, Any]]:
    subset = frame.loc[frame["family_classification"] == class_name].copy()
    if class_name == "stable_high_value_family":
        subset = subset.sort_values(["mean_path_value_score_v1", "sample_count", "positive_month_rate"], ascending=[False, False, False])
    elif class_name == "stable_bad_pick_family":
        subset = subset.sort_values(["mean_path_value_score_v1", "sample_count", "mean_mae_20d"], ascending=[True, False, True])
    elif class_name == "regime_dependent_family":
        subset = subset.sort_values(["score_spread", "sample_count"], ascending=[False, False])
    else:
        subset = subset.sort_values(["sample_count", "mean_path_value_score_v1"], ascending=[False, False])
    return subset.head(limit).to_dict(orient="records")


def _build_decision_payload(
    *,
    source_session_id: str,
    source_paths: dict[str, str],
    family_summary: pd.DataFrame,
    thresholds: dict[str, Any],
    cardinality_report: dict[str, Any],
    limit_families: int,
) -> dict[str, Any]:
    stable_high = family_summary.loc[family_summary["family_classification"] == "stable_high_value_family"]
    stable_bad = family_summary.loc[family_summary["family_classification"] == "stable_bad_pick_family"]
    regime_dep = family_summary.loc[family_summary["family_classification"] == "regime_dependent_family"]
    sparse = family_summary.loc[family_summary["family_classification"] == "unstable_or_sparse_family"]
    neutral = family_summary.loc[family_summary["family_classification"] == "neutral_family"]
    if len(stable_high) >= 10 and len(stable_bad) >= 10:
        recommendation = "keep"
        reason = "enough_stable_high_and_bad_pick_families_survive_coarse_filter"
    elif len(stable_high) > 0 or len(stable_bad) > 0 or len(regime_dep) > 0:
        recommendation = "hold"
        reason = "state_family_signal_exists_but_compression_or_thresholds_need_refinement"
    else:
        recommendation = "drop"
        reason = "no_reliable_state_families_survive_stability_filter"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_session_id": source_session_id,
        "source_artifacts": source_paths,
        "state_family_id_definition": {
            "raw_state_id_source": "position_state_id from the verified full MA position-path session parquet",
            "family_components": [
                "close_vs_ma7",
                "close_vs_ma20",
                "close_vs_ma60",
                "ma_stack",
                "ma20_slope",
                "ma60_slope",
                "bucketed_consecutive_close_streaks_for_ma7_ma20_ma60",
                "candle_strength",
                "gap_group",
                "price_location",
                "volume_condition",
            ],
            "bucket_rules": {
                "consecutive_streaks": ["0", "1-2", "3-6", "7-13", "14+"],
                "candle_strength": ["large_bullish", "large_bearish", "neutral_small"],
                "gap_group": ["gap_up", "gap_down", "no_major_gap"],
                "price_location": ["near_high", "middle", "near_low"],
            },
            "analysis_axes": {
                "regime_context": "tracked separately for stability and regime-dependent family classification",
            },
            "note": "position_state_id remains in parquet for audit, but state_family_id is the challenger input.",
        },
        "filters_used": thresholds,
        "baseline_metrics": {
            "mean_path_value_score_v1": thresholds["baseline_mean_path_value_score_v1"],
            "median_path_value_score_v1": thresholds["baseline_median_path_value_score_v1"],
            "plus5_before_minus5_rate": thresholds["baseline_plus5_before_minus5_rate"],
            "minus5_before_plus5_rate": thresholds["baseline_minus5_before_plus5_rate"],
            "bottom15_rate": thresholds["baseline_bottom15_rate"],
            "top15_rate": thresholds["baseline_top15_rate"],
            "bottom15_score_threshold": thresholds["bottom15_score_threshold"],
            "top15_score_threshold": thresholds["top15_score_threshold"],
        },
        "source_state_id_cardinality": cardinality_report,
        "total_families": int(len(family_summary)),
        "stable_high_value_family_count": int(len(stable_high)),
        "stable_bad_pick_family_count": int(len(stable_bad)),
        "regime_dependent_family_count": int(len(regime_dep)),
        "unstable_or_sparse_family_count": int(len(sparse)),
        "neutral_family_count": int(len(neutral)),
        "top_50_stable_high_value_families": _top_examples(family_summary, class_name="stable_high_value_family", limit=min(50, limit_families)),
        "top_50_stable_bad_pick_families": _top_examples(family_summary, class_name="stable_bad_pick_family", limit=min(50, limit_families)),
        "recommendation": recommendation,
        "typed_reasons": [reason],
    }


def _build_manifest_payload(
    *,
    session_id: str,
    source_session_id: str,
    source_session: Path,
    source_paths: dict[str, str],
    output_root: Path,
    family_counts: dict[str, int],
    thresholds: dict[str, Any],
    artifact_paths: dict[str, str],
) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "source_session_id": source_session_id,
        "source_session_path": str(source_session),
        "output_root": str(output_root),
        "source_artifacts": source_paths,
        "family_counts": family_counts,
        "thresholds": thresholds,
        "no_lookahead_inherited": True,
        "output_artifacts": artifact_paths,
    }


def _finalize_session_dir(session_tmp: Path, session_final: Path) -> None:
    if session_final.exists():
        raise FileExistsError(f"final session output already exists: {session_final}")
    try:
        session_tmp.replace(session_final)
    except Exception:
        shutil.move(str(session_tmp), str(session_final))


def run_state_family_stability_filter(
    *,
    source_session: str | Path | None = None,
    output_root: str | Path | None = None,
    min_sample_count: int = DEFAULT_MIN_SAMPLE_COUNT,
    min_unique_symbol_count: int = DEFAULT_MIN_UNIQUE_SYMBOL_COUNT,
    min_month_count: int = DEFAULT_MIN_MONTH_COUNT,
    limit_families: int = DEFAULT_LIMIT_FAMILIES,
) -> dict[str, Any]:
    run_started = time.perf_counter()
    source_session_path = _resolve_source_session(source_session)
    source_payloads = _load_source_session(source_session_path)
    output_root_path = Path(output_root).expanduser().resolve() if output_root else DEFAULT_OUTPUT_ROOT.resolve()
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_tmp = output_root_path / f"{session_id}.tmp"
    session_final = output_root_path / session_id
    session_tmp.mkdir(parents=True, exist_ok=False)

    _progress_log(f"start source={source_session_path} out_root={output_root_path} session={session_id}")

    thresholds = {
        "min_sample_count": int(min_sample_count),
        "min_unique_symbol_count": int(min_unique_symbol_count),
        "min_month_count": int(min_month_count),
    }

    conn = duckdb.connect()
    try:
        family_select_sql = _family_row_select_sql(source_payloads["row_parquet"])
        conn.execute(f"CREATE TEMP VIEW family_rows AS {family_select_sql}")
        cardinality_report = _cardinality_report(conn, source_payloads["row_parquet"])
        family_count = int(conn.execute("SELECT COUNT(DISTINCT state_family_id) FROM family_rows").fetchone()[0])
        source_row_count = int(conn.execute("SELECT COUNT(*) FROM family_rows").fetchone()[0])
        top15_score_threshold, bottom15_score_threshold = conn.execute(
            "SELECT quantile_cont(path_value_score_v1, 0.85), quantile_cont(path_value_score_v1, 0.15) FROM family_rows"
        ).fetchone()
        if top15_score_threshold is None or bottom15_score_threshold is None:
            raise RuntimeError("unable_to_compute_path_value_thresholds")
        thresholds["baseline_mean_path_value_score_v1"] = _safe_float(source_payloads["manifest"]["overall_metrics"]["mean_path_value_score_v1"])
        thresholds["baseline_median_path_value_score_v1"] = _safe_float(source_payloads["manifest"]["overall_metrics"]["median_path_value_score_v1"])
        thresholds["baseline_plus5_before_minus5_rate"] = _safe_float(source_payloads["manifest"]["overall_metrics"]["hit_plus_5_before_minus_5_rate"])
        thresholds["baseline_minus5_before_plus5_rate"] = _safe_float(source_payloads["manifest"]["overall_metrics"]["hit_minus_5_before_plus_5_rate"])
        thresholds["top15_score_threshold"] = float(top15_score_threshold)
        thresholds["bottom15_score_threshold"] = float(bottom15_score_threshold)
        thresholds["baseline_top15_rate"] = float(
            conn.execute(
                f"SELECT AVG(CASE WHEN path_value_score_v1 >= {top15_score_threshold} THEN 1 ELSE 0 END) FROM family_rows"
            ).fetchone()[0]
        )
        thresholds["baseline_bottom15_rate"] = float(
            conn.execute(
                f"SELECT AVG(CASE WHEN path_value_score_v1 <= {bottom15_score_threshold} THEN 1 ELSE 0 END) FROM family_rows"
            ).fetchone()[0]
        )

        row_parquet_path = session_tmp / "state_family_rows.parquet"
        conn.execute(f"COPY family_rows TO '{row_parquet_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")

        family_summary = _family_summary_frame(
            conn,
            top15_threshold=float(top15_score_threshold),
            bottom15_threshold=float(bottom15_score_threshold),
        )
        family_regime = _family_regime_frame(conn)
        family_monthly = _family_monthly_frame(conn)
    finally:
        conn.close()

    if family_summary.empty:
        raise RuntimeError("no_family_rows")

    family_regime_rollup = (
        family_regime.groupby("state_family_id", dropna=False)
        .apply(
            lambda group: pd.Series(
                {
                    "regime_count": int(group["family_regime_context"].nunique()),
                    "regime_consistency_score": _safe_float(group["sample_count"].max() / group["sample_count"].sum()),
                    "score_spread": _safe_float(group["score_spread"].max()),
                    "dominant_regime_context": str(group.sort_values("sample_count", ascending=False).iloc[0]["family_regime_context"]),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )

    family_summary = family_summary.merge(family_monthly, on="state_family_id", how="left").merge(
        family_regime_rollup, on="state_family_id", how="left"
    )
    family_summary["family_classification"] = family_summary.apply(_classify_family, axis=1, thresholds=thresholds)
    family_counts = {str(key): int(value) for key, value in family_summary["family_classification"].value_counts(dropna=False).to_dict().items()}

    stable_high = family_summary.loc[family_summary["family_classification"] == "stable_high_value_family"].sort_values(
        ["mean_path_value_score_v1", "sample_count", "positive_month_rate"], ascending=[False, False, False]
    )
    stable_bad = family_summary.loc[family_summary["family_classification"] == "stable_bad_pick_family"].sort_values(
        ["mean_path_value_score_v1", "sample_count", "mean_mae_20d"], ascending=[True, False, True]
    )
    regime_dep = family_summary.loc[family_summary["family_classification"] == "regime_dependent_family"].sort_values(
        ["regime_consistency_score", "score_spread", "sample_count"], ascending=[True, False, False]
    )
    unstable = family_summary.loc[family_summary["family_classification"] == "unstable_or_sparse_family"].sort_values(
        ["sample_count", "mean_path_value_score_v1"], ascending=[False, False]
    )
    neutral = family_summary.loc[family_summary["family_classification"] == "neutral_family"].sort_values(
        ["sample_count", "mean_path_value_score_v1"], ascending=[False, False]
    )

    summary_payload = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_session_id": source_payloads["manifest"]["session_id"],
        "source_session_path": str(source_session_path),
        "source_artifacts": {
            "run_manifest_json": str(source_session_path / "run_manifest.json"),
            "position_state_value_summary_json": str(source_session_path / "position_state_value_summary.json"),
            "position_state_value_by_regime_json": str(source_session_path / "position_state_value_by_regime.json"),
            "position_state_monthly_stability_json": str(source_session_path / "position_state_monthly_stability.json"),
            "position_state_classification_json": str(source_session_path / "position_state_classification.json"),
            "ma_candle_position_value_v1_decision_json": str(source_session_path / "ma_candle_position_value_v1_decision.json"),
            "position_state_forward_path_rows_parquet": str(source_session_path / "position_state_forward_path_rows.parquet"),
        },
        "state_id_structure": {
            "raw_position_state_components": [
                "c7",
                "c20",
                "c60",
                "b7",
                "b20",
                "b60",
                "stk",
                "s7",
                "s20",
                "s60",
                "st7",
                "st20",
                "st60",
                "cd",
                "p20",
                "p60",
                "vol",
                "regime_source",
                "regime_label",
            ],
            "raw_component_cardinality": cardinality_report,
            "raw_state_count": int(cardinality_report["raw_state_count"]),
            "approximate_fragmentation_note": "The raw state map is much finer than the family map because the family filter buckets streaks, simplifies candle tags, and drops body-vs-MA detail.",
        },
        "family_definition": {
            "components": [
                "close_vs_ma7/20/60",
                "ma_stack",
                "ma20_slope",
                "ma60_slope",
                "bucketed_consecutive_streaks",
                "candle_strength",
                "gap_group",
                "price_location",
                "volume_condition",
            ],
            "bucket_rules": {
                "consecutive_streaks": ["0", "1-2", "3-6", "7-13", "14+"],
                "candle_strength": ["large_bullish", "large_bearish", "neutral_small"],
                "gap_group": ["gap_up", "gap_down", "no_major_gap"],
                "price_location": ["near_high", "middle", "near_low"],
            },
            "analysis_axes": {
                "regime_context": "tracked separately for stability and regime-dependent family classification",
            },
            "dropped_raw_state_fields": ["body_vs_ma7", "body_vs_ma20", "body_vs_ma60", "ma7_slope"],
        },
        "baseline_metrics": {
            "mean_path_value_score_v1": thresholds["baseline_mean_path_value_score_v1"],
            "median_path_value_score_v1": thresholds["baseline_median_path_value_score_v1"],
            "plus5_before_minus5_rate": thresholds["baseline_plus5_before_minus5_rate"],
            "minus5_before_plus5_rate": thresholds["baseline_minus5_before_plus5_rate"],
            "top15_rate": thresholds["baseline_top15_rate"],
            "bottom15_rate": thresholds["baseline_bottom15_rate"],
            "top15_score_threshold": thresholds["top15_score_threshold"],
            "bottom15_score_threshold": thresholds["bottom15_score_threshold"],
        },
        "filter_thresholds": thresholds,
        "family_counts": {
            "total_families": int(len(family_summary)),
            "stable_high_value_family_count": int(len(stable_high)),
            "stable_bad_pick_family_count": int(len(stable_bad)),
            "regime_dependent_family_count": int(len(regime_dep)),
            "unstable_or_sparse_family_count": int(len(unstable)),
            "neutral_family_count": int(len(neutral)),
            "source_row_count": int(source_row_count),
            "source_session_family_count": int(family_count),
            "family_rows_parquet_row_count": int(source_row_count),
        },
        "top_high_value_families": stable_high.head(limit_families).to_dict(orient="records"),
        "top_bad_pick_families": stable_bad.head(limit_families).to_dict(orient="records"),
        "top_regime_dependent_families": regime_dep.head(limit_families).to_dict(orient="records"),
        "top_unstable_families": unstable.head(limit_families).to_dict(orient="records"),
        "top_neutral_families": neutral.head(limit_families).to_dict(orient="records"),
        "notes": [
            "Family filter is TRADEX-only and reuses the verified full-session parquet.",
            "Raw position_state_id is retained in parquet for audit, but the family id is the intended challenger input.",
            "No MeeMee, production ranking, or publish flow was changed.",
        ],
    }

    by_regime_payload = {
        "schema_version": BY_REGIME_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_session_id": source_payloads["manifest"]["session_id"],
        "family_regime_row_count": int(len(family_regime)),
        "top_regime_dependent_families": regime_dep.head(max(100, limit_families)).to_dict(orient="records"),
        "regime_family_summary": family_regime.head(max(100, limit_families)).to_dict(orient="records"),
    }

    monthly_payload = {
        "schema_version": MONTHLY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_session_id": source_payloads["manifest"]["session_id"],
        "family_month_row_count": int(len(family_monthly)),
        "top_stable_families": family_monthly.sort_values(
            ["std_monthly_path_value", "month_sample_count"], ascending=[True, False]
        ).head(limit_families).to_dict(orient="records"),
        "top_unstable_families": family_monthly.sort_values(
            ["std_monthly_path_value", "month_sample_count"], ascending=[False, False]
        ).head(limit_families).to_dict(orient="records"),
    }

    classification_payload = {
        "schema_version": CLASSIFICATION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_session_id": source_payloads["manifest"]["session_id"],
        "state_family_classification_counts": family_counts,
        "examples": {
            "stable_high_value_family": stable_high.head(limit_families).to_dict(orient="records"),
            "stable_bad_pick_family": stable_bad.head(limit_families).to_dict(orient="records"),
            "regime_dependent_family": regime_dep.head(limit_families).to_dict(orient="records"),
            "unstable_or_sparse_family": unstable.head(limit_families).to_dict(orient="records"),
            "neutral_family": neutral.head(limit_families).to_dict(orient="records"),
        },
    }

    final_artifact_paths = {
        "state_family_summary_json": str(session_tmp / "state_family_summary.json"),
        "state_family_by_regime_json": str(session_tmp / "state_family_by_regime.json"),
        "state_family_monthly_stability_json": str(session_tmp / "state_family_monthly_stability.json"),
        "state_family_classification_json": str(session_tmp / "state_family_classification.json"),
        "state_family_rows_parquet": str(session_tmp / "state_family_rows.parquet"),
        "state_family_filter_v1_decision_json": str(session_tmp / "state_family_filter_v1_decision.json"),
        "run_manifest_json": str(session_tmp / "run_manifest.json"),
        "_artifact_complete_json": str(session_tmp / "_ARTIFACT_COMPLETE.json"),
    }

    decision_payload = _build_decision_payload(
        source_session_id=source_payloads["manifest"]["session_id"],
        source_paths=summary_payload["source_artifacts"],
        family_summary=family_summary,
        thresholds=thresholds,
        cardinality_report=cardinality_report,
        limit_families=limit_families,
    )

    manifest_payload = _build_manifest_payload(
        session_id=session_id,
        source_session_id=source_payloads["manifest"]["session_id"],
        source_session=source_session_path,
        source_paths=summary_payload["source_artifacts"],
        output_root=output_root_path,
        family_counts=family_counts,
        thresholds=thresholds,
        artifact_paths=final_artifact_paths,
    )

    _write_json(session_tmp / "state_family_summary.json", summary_payload)
    _write_json(session_tmp / "state_family_by_regime.json", by_regime_payload)
    _write_json(session_tmp / "state_family_monthly_stability.json", monthly_payload)
    _write_json(session_tmp / "state_family_classification.json", classification_payload)
    _write_json(session_tmp / "state_family_filter_v1_decision.json", decision_payload)
    _write_json(session_tmp / "run_manifest.json", manifest_payload)

    _write_json(session_tmp / "_ARTIFACT_COMPLETE.json", {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "validated": True,
    })

    _finalize_session_dir(session_tmp, session_final)
    _progress_log(f"finalized session={session_id} elapsed={time.perf_counter() - run_started:.1f}s")

    final_paths = {
        "state_family_summary_json": str(session_final / "state_family_summary.json"),
        "state_family_by_regime_json": str(session_final / "state_family_by_regime.json"),
        "state_family_monthly_stability_json": str(session_final / "state_family_monthly_stability.json"),
        "state_family_classification_json": str(session_final / "state_family_classification.json"),
        "state_family_rows_parquet": str(session_final / "state_family_rows.parquet"),
        "state_family_filter_v1_decision_json": str(session_final / "state_family_filter_v1_decision.json"),
        "run_manifest_json": str(session_final / "run_manifest.json"),
        "_artifact_complete_json": str(session_final / "_ARTIFACT_COMPLETE.json"),
    }

    summary_payload["output_artifacts"] = final_paths
    by_regime_payload["output_artifacts"] = final_paths
    monthly_payload["output_artifacts"] = final_paths
    classification_payload["output_artifacts"] = final_paths
    decision_payload["artifact_paths"] = final_paths
    manifest_payload["output_artifacts"] = final_paths
    _write_json(session_final / "state_family_summary.json", summary_payload)
    _write_json(session_final / "state_family_by_regime.json", by_regime_payload)
    _write_json(session_final / "state_family_monthly_stability.json", monthly_payload)
    _write_json(session_final / "state_family_classification.json", classification_payload)
    _write_json(session_final / "state_family_filter_v1_decision.json", decision_payload)
    _write_json(session_final / "run_manifest.json", manifest_payload)

    return {
        "session_id": session_id,
        "session_dir": str(session_final),
        "summary_path": final_paths["state_family_summary_json"],
        "by_regime_path": final_paths["state_family_by_regime_json"],
        "monthly_stability_path": final_paths["state_family_monthly_stability_json"],
        "classification_path": final_paths["state_family_classification_json"],
        "detail_path": final_paths["state_family_rows_parquet"],
        "decision_path": final_paths["state_family_filter_v1_decision_json"],
        "manifest_path": final_paths["run_manifest_json"],
        "summary": summary_payload,
        "by_regime": by_regime_payload,
        "monthly_stability": monthly_payload,
        "classification": classification_payload,
        "decision": decision_payload,
        "manifest": manifest_payload,
        "family_summary": family_summary,
        "family_regime": family_regime,
        "family_monthly": family_monthly,
        "cardinality_report": cardinality_report,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX MA position state-family stability filter.")
    parser.add_argument("--source-session", default=str(DEFAULT_SOURCE_SESSION))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--min-sample-count", type=int, default=DEFAULT_MIN_SAMPLE_COUNT)
    parser.add_argument("--min-unique-symbol-count", type=int, default=DEFAULT_MIN_UNIQUE_SYMBOL_COUNT)
    parser.add_argument("--min-month-count", type=int, default=DEFAULT_MIN_MONTH_COUNT)
    parser.add_argument("--limit-families", type=int, default=DEFAULT_LIMIT_FAMILIES)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = run_state_family_stability_filter(
        source_session=args.source_session,
        output_root=args.output_root,
        min_sample_count=args.min_sample_count,
        min_unique_symbol_count=args.min_unique_symbol_count,
        min_month_count=args.min_month_count,
        limit_families=args.limit_families,
    )
    print(json.dumps(result["summary"]["output_artifacts"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
