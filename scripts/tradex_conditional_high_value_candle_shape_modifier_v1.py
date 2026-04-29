from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_ma_state_family_high_value_boost_v1 import (  # noqa: E402
    _load_source_family_session,
    _make_session_id,
    _progress_log,
    _safe_float,
    _safe_int,
    _write_json,
)
from scripts.tradex_multi_timeframe_conditional_state_value_v1 import (  # noqa: E402
    CONTEXT_SCHEMA_VERSION,
    SUMMARY_SCHEMA_VERSION,
    _build_group_summary_sql,
    _classify_conditional_group,
    _count_classes,
)

DEFAULT_CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
DEFAULT_SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
DEFAULT_CONTEXT_GATED_BOOST_SESSION = Path(r"G:\Tradex\multi_timeframe_context_gated_high_value_boost_v1\20260429T094730Z-7e1acdee")
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1")
DEFAULT_LIMIT_SYMBOLS = None

SCHEMA_VERSION = "tradex_conditional_high_value_candle_shape_modifier_v1"
DEFINITION_SCHEMA_VERSION = "tradex_conditional_high_value_candle_shape_modifier_v1_definition_v1"
SUMMARY_SCHEMA = "tradex_conditional_high_value_candle_shape_modifier_v1_summary_v1"
CLASSIFICATION_SCHEMA = "tradex_conditional_high_value_candle_shape_modifier_v1_classification_v1"
COMPARISON_SCHEMA = "tradex_conditional_high_value_candle_shape_modifier_v1_shape_vs_base_v1"
STABILITY_SCHEMA = "tradex_conditional_high_value_candle_shape_modifier_v1_monthly_stability_v1"
DECISION_SCHEMA_VERSION = "tradex_conditional_high_value_candle_shape_modifier_v1_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_conditional_high_value_candle_shape_modifier_v1_manifest_v1"

TOP_EXAMPLE_LIMIT = 50
BASE_SAMPLE_COUNT_MIN = 50
BASE_UNIQUE_SYMBOL_MIN = 15
BASE_MONTH_COUNT_MIN = 6

SHAPE_BUCKETS = [
    "inside_bar",
    "bull_engulfing",
    "bear_engulfing",
    "gap_up_bull",
    "gap_down_bear",
    "koma_then_bull",
    "koma_then_bear",
    "lower_wick_then_bull",
    "upper_wick_then_bear",
    "bull_large",
    "bear_large",
    "doji_like",
    "neutral_small",
    "lower_wick_dominant",
    "upper_wick_dominant",
    "no_clear_shape",
]


def _progress_log(message: str) -> None:
    print(f"[conditional_high_value_candle_shape_modifier_v1] {message}", file=sys.stderr, flush=True)


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


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _resolve_context_session(source_context_session: str | Path | None) -> Path:
    path = _safe_path(source_context_session, DEFAULT_CONTEXT_SESSION)
    if not path.exists():
        raise FileNotFoundError(f"context session not found: {path}")
    return path


def _resolve_source_family_session(source_family_session: str | Path | None) -> Path:
    path = _safe_path(source_family_session, DEFAULT_SOURCE_FAMILY_SESSION)
    if not path.exists():
        raise FileNotFoundError(f"source family session not found: {path}")
    return path


def _resolve_context_gated_boost_session(context_gated_boost_session: str | Path | None) -> Path:
    path = _safe_path(context_gated_boost_session, DEFAULT_CONTEXT_GATED_BOOST_SESSION)
    if not path.exists():
        raise FileNotFoundError(f"context-gated boost session not found: {path}")
    return path


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return _safe_path(output_root, DEFAULT_OUTPUT_ROOT)


def _load_context_session(context_session: Path) -> dict[str, Any]:
    manifest = json.loads((context_session / "run_manifest.json").read_text(encoding="utf-8"))
    summary = json.loads((context_session / "conditional_state_value_summary.json").read_text(encoding="utf-8"))
    classification = json.loads((context_session / "conditional_state_classification.json").read_text(encoding="utf-8"))
    decision = json.loads((context_session / "multi_timeframe_conditional_state_value_v1_decision.json").read_text(encoding="utf-8"))
    comparison = json.loads((context_session / "global_vs_conditional_comparison.json").read_text(encoding="utf-8"))
    context_definition = json.loads((context_session / "context_definition.json").read_text(encoding="utf-8"))
    row_parquet = context_session / "conditional_state_rows.parquet"
    if not row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {row_parquet}")
    return {
        "manifest": manifest,
        "summary": summary,
        "classification": classification,
        "decision": decision,
        "comparison": comparison,
        "context_definition": context_definition,
        "row_parquet": row_parquet,
    }


def _load_context_gated_boost_session(boost_session: Path) -> dict[str, Any]:
    compare = json.loads((boost_session / "multi_timeframe_context_gated_high_value_boost_v1_compare.json").read_text(encoding="utf-8"))
    decision = json.loads((boost_session / "multi_timeframe_context_gated_high_value_boost_v1_decision.json").read_text(encoding="utf-8"))
    coverage = json.loads((boost_session / "boost_coverage_summary.json").read_text(encoding="utf-8"))
    return {"compare": compare, "decision": decision, "coverage": coverage}


def _extract_source_thresholds(source_family_payloads: dict[str, Any]) -> tuple[float, float, dict[str, Any]]:
    manifest_thresholds = source_family_payloads["manifest"].get("thresholds") or {}
    summary_thresholds = source_family_payloads.get("family_summary_report", {}).get("filter_thresholds") or {}
    source_thresholds = manifest_thresholds or summary_thresholds
    top15_threshold = _safe_float(source_thresholds.get("top15_score_threshold"))
    bottom15_threshold = _safe_float(source_thresholds.get("bottom15_score_threshold"))
    if top15_threshold is None or bottom15_threshold is None:
        raise RuntimeError("missing bottom15/top15 thresholds from family filter session")
    return float(top15_threshold), float(bottom15_threshold), source_thresholds


def _build_conditional_gate(
    *,
    context_row_parquet: Path,
    thresholds: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    conn = duckdb.connect()
    try:
        conn.execute(f"CREATE TEMP VIEW conditional_rows AS SELECT * FROM read_parquet('{context_row_parquet.as_posix()}')")
        triple_frame = conn.execute(_build_group_summary_sql(["monthly_context", "weekly_context", "state_family_id"])).fetchdf()
        monthly_context_frame = conn.execute(_build_group_summary_sql(["monthly_context"])).fetchdf()
        weekly_context_frame = conn.execute(_build_group_summary_sql(["weekly_context"])).fetchdf()
        monthly_weekly_frame = conn.execute(_build_group_summary_sql(["monthly_context", "weekly_context"])).fetchdf()
    finally:
        conn.close()

    for frame in (triple_frame, monthly_context_frame, weekly_context_frame, monthly_weekly_frame):
        frame["conditional_classification"] = frame.apply(_classify_conditional_group, axis=1, thresholds=thresholds)
        frame["sample_safe"] = (
            frame["sample_count"].fillna(0).astype(int) >= int(thresholds["min_sample_count"])
        ) & (
            frame["unique_symbol_count"].fillna(0).astype(int) >= int(thresholds["min_unique_symbol_count"])
        ) & (
            frame["month_count"].fillna(0).astype(int) >= int(thresholds["min_month_count"])
        )

    gate = triple_frame.loc[triple_frame["conditional_classification"] == "conditional_high_value"].copy()
    gate["conditional_high_value"] = True
    gate = gate[
        [
            "monthly_context",
            "weekly_context",
            "state_family_id",
            "sample_count",
            "unique_symbol_count",
            "month_count",
            "mean_forward_ret_5d",
            "mean_forward_ret_10d",
            "mean_forward_ret_20d",
            "median_forward_ret_20d",
            "mean_path_value_score_v1",
            "median_path_value_score_v1",
            "mean_mfe_20d",
            "mean_mae_20d",
            "plus5_before_minus5_rate",
            "minus5_before_plus5_rate",
            "top15_rate",
            "bottom15_rate",
            "positive_month_rate",
            "worst_month_mean_path_value",
            "best_month_mean_path_value",
            "regime_count",
            "regime_consistency_score",
            "score_spread",
            "dominant_regime_context",
            "conditional_high_value",
        ]
    ].copy()
    return triple_frame, monthly_context_frame, weekly_context_frame, gate


def _shape_case_sql() -> str:
    return """
        CASE
            WHEN o IS NULL OR h IS NULL OR l IS NULL OR c IS NULL THEN 'no_clear_shape'
            WHEN prev_h IS NOT NULL AND prev_l IS NOT NULL AND h <= prev_h AND l >= prev_l THEN 'inside_bar'
            WHEN prev_o IS NOT NULL AND prev_c IS NOT NULL AND c > o AND o <= prev_c AND c >= prev_o AND candle_body_ratio >= 0.35 THEN 'bull_engulfing'
            WHEN prev_o IS NOT NULL AND prev_c IS NOT NULL AND c < o AND o >= prev_c AND c <= prev_o AND candle_body_ratio >= 0.35 THEN 'bear_engulfing'
            WHEN gap_pct >= 0.01 AND c > o AND candle_body_ratio >= 0.35 THEN 'gap_up_bull'
            WHEN gap_pct <= -0.01 AND c < o AND candle_body_ratio >= 0.35 THEN 'gap_down_bear'
            WHEN prev_body_ratio IS NOT NULL AND prev_body_ratio <= 0.10 AND c > o AND candle_body_ratio >= 0.35 THEN 'koma_then_bull'
            WHEN prev_body_ratio IS NOT NULL AND prev_body_ratio <= 0.10 AND c < o AND candle_body_ratio >= 0.35 THEN 'koma_then_bear'
            WHEN prev_lower_wick_ratio IS NOT NULL AND prev_upper_wick_ratio IS NOT NULL AND prev_lower_wick_ratio >= GREATEST(prev_upper_wick_ratio * 1.2, 0.35) AND c > o AND candle_body_ratio >= 0.35 THEN 'lower_wick_then_bull'
            WHEN prev_lower_wick_ratio IS NOT NULL AND prev_upper_wick_ratio IS NOT NULL AND prev_upper_wick_ratio >= GREATEST(prev_lower_wick_ratio * 1.2, 0.35) AND c < o AND candle_body_ratio >= 0.35 THEN 'upper_wick_then_bear'
            WHEN candle_body_ratio >= 0.60 AND c > o THEN 'bull_large'
            WHEN candle_body_ratio >= 0.60 AND c < o THEN 'bear_large'
            WHEN candle_body_ratio <= 0.10 THEN 'doji_like'
            WHEN candle_body_ratio <= 0.25 THEN 'neutral_small'
            WHEN candle_lower_wick_ratio >= GREATEST(candle_upper_wick_ratio * 1.2, 0.35) THEN 'lower_wick_dominant'
            WHEN candle_upper_wick_ratio >= GREATEST(candle_lower_wick_ratio * 1.2, 0.35) THEN 'upper_wick_dominant'
            ELSE 'no_clear_shape'
        END AS candle_shape_modifier
    """


def _build_joined_rows_sql(
    *,
    conditional_row_view: str,
    scope_code_filter_sql: str,
    shape_source_filter_sql: str,
) -> str:
    return f"""
    WITH bars AS (
        SELECT
            code::VARCHAR AS code,
            CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) AS trade_date,
            o,
            h,
            l,
            c,
            v,
            LAG(o) OVER w AS prev_o,
            LAG(h) OVER w AS prev_h,
            LAG(l) OVER w AS prev_l,
            LAG(c) OVER w AS prev_c,
            LAG(CASE WHEN h = l THEN NULL ELSE ABS(c - o) / NULLIF(h - l, 0) END) OVER w AS prev_body_ratio,
            LAG(CASE WHEN h = l THEN NULL ELSE (h - GREATEST(o, c)) / NULLIF(h - l, 0) END) OVER w AS prev_upper_wick_ratio,
            LAG(CASE WHEN h = l THEN NULL ELSE (LEAST(o, c) - l) / NULLIF(h - l, 0) END) OVER w AS prev_lower_wick_ratio
        FROM daily_bars
        {scope_code_filter_sql}
        WINDOW w AS (PARTITION BY code ORDER BY date)
    ),
    feature AS (
        SELECT
            code::VARCHAR AS code,
            CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) AS trade_date,
            candle_body_ratio,
            candle_upper_wick_ratio,
            candle_lower_wick_ratio,
            candle_triplet_up_prob,
            candle_triplet_down_prob,
            gap_pct,
            vol_ratio5_20
        FROM ml_feature_daily
        {shape_source_filter_sql}
    ),
    joined AS (
        SELECT
            c.*,
            b.o,
            b.h,
            b.l,
            b.c,
            b.v,
            b.prev_o,
            b.prev_h,
            b.prev_l,
            b.prev_c,
            b.prev_body_ratio,
            b.prev_upper_wick_ratio,
            b.prev_lower_wick_ratio,
            f.candle_body_ratio,
            f.candle_upper_wick_ratio,
            f.candle_lower_wick_ratio,
            f.candle_triplet_up_prob,
            f.candle_triplet_down_prob,
            f.gap_pct,
            f.vol_ratio5_20,
            {_shape_case_sql()}
        FROM {conditional_row_view} c
        LEFT JOIN bars b
          ON b.code = c.code
         AND b.trade_date = c.trade_date
        LEFT JOIN feature f
          ON f.code = c.code
         AND f.trade_date = c.trade_date
    )
    SELECT
        *,
        CASE WHEN gate.state_family_id IS NOT NULL THEN TRUE ELSE FALSE END AS conditional_high_value
    FROM joined
    LEFT JOIN conditional_high_value_gate gate
      ON gate.monthly_context = joined.monthly_context
     AND gate.weekly_context = joined.weekly_context
     AND gate.state_family_id = joined.state_family_id
    """


def _derive_candle_shape_modifier(row: pd.Series | dict[str, Any]) -> str:
    o = row.get("o")
    h = row.get("h")
    l = row.get("l")
    c = row.get("c")
    candle_body_ratio = _safe_float(row.get("candle_body_ratio"))
    candle_upper_wick_ratio = _safe_float(row.get("candle_upper_wick_ratio"))
    candle_lower_wick_ratio = _safe_float(row.get("candle_lower_wick_ratio"))
    gap_pct = _safe_float(row.get("gap_pct"))
    prev_body_ratio = _safe_float(row.get("prev_body_ratio"))
    prev_upper_wick_ratio = _safe_float(row.get("prev_upper_wick_ratio"))
    prev_lower_wick_ratio = _safe_float(row.get("prev_lower_wick_ratio"))

    if any(value is None for value in (o, h, l, c)):
        return "no_clear_shape"
    if h == l:
        return "no_clear_shape"
    if row.get("prev_h") is not None and row.get("prev_l") is not None and h <= row.get("prev_h") and l >= row.get("prev_l"):
        return "inside_bar"
    if (
        row.get("prev_o") is not None
        and row.get("prev_c") is not None
        and c > o
        and o <= row.get("prev_c")
        and c >= row.get("prev_o")
        and candle_body_ratio is not None
        and candle_body_ratio >= 0.35
    ):
        return "bull_engulfing"
    if (
        row.get("prev_o") is not None
        and row.get("prev_c") is not None
        and c < o
        and o >= row.get("prev_c")
        and c <= row.get("prev_o")
        and candle_body_ratio is not None
        and candle_body_ratio >= 0.35
    ):
        return "bear_engulfing"
    if gap_pct is not None and gap_pct >= 0.01 and c > o and candle_body_ratio is not None and candle_body_ratio >= 0.35:
        return "gap_up_bull"
    if gap_pct is not None and gap_pct <= -0.01 and c < o and candle_body_ratio is not None and candle_body_ratio >= 0.35:
        return "gap_down_bear"
    if prev_body_ratio is not None and prev_body_ratio <= 0.10 and c > o and candle_body_ratio is not None and candle_body_ratio >= 0.35:
        return "koma_then_bull"
    if prev_body_ratio is not None and prev_body_ratio <= 0.10 and c < o and candle_body_ratio is not None and candle_body_ratio >= 0.35:
        return "koma_then_bear"
    if (
        prev_lower_wick_ratio is not None
        and prev_upper_wick_ratio is not None
        and prev_lower_wick_ratio >= max(prev_upper_wick_ratio * 1.2, 0.35)
        and c > o
        and candle_body_ratio is not None
        and candle_body_ratio >= 0.35
    ):
        return "lower_wick_then_bull"
    if (
        prev_lower_wick_ratio is not None
        and prev_upper_wick_ratio is not None
        and prev_upper_wick_ratio >= max(prev_lower_wick_ratio * 1.2, 0.35)
        and c < o
        and candle_body_ratio is not None
        and candle_body_ratio >= 0.35
    ):
        return "upper_wick_then_bear"
    if candle_body_ratio is not None and candle_body_ratio >= 0.60 and c > o:
        return "bull_large"
    if candle_body_ratio is not None and candle_body_ratio >= 0.60 and c < o:
        return "bear_large"
    if candle_body_ratio is not None and candle_body_ratio <= 0.10:
        return "doji_like"
    if candle_body_ratio is not None and candle_body_ratio <= 0.25:
        return "neutral_small"
    if (
        candle_lower_wick_ratio is not None
        and candle_upper_wick_ratio is not None
        and candle_lower_wick_ratio >= max(candle_upper_wick_ratio * 1.2, 0.35)
    ):
        return "lower_wick_dominant"
    if (
        candle_upper_wick_ratio is not None
        and candle_lower_wick_ratio is not None
        and candle_upper_wick_ratio >= max(candle_lower_wick_ratio * 1.2, 0.35)
    ):
        return "upper_wick_dominant"
    return "no_clear_shape"


def _aggregate_overall_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "sample_count": 0,
            "unique_symbol_count": 0,
            "month_count": 0,
            "mean_forward_ret_5d": None,
            "mean_forward_ret_10d": None,
            "mean_forward_ret_20d": None,
            "median_forward_ret_20d": None,
            "mean_path_value_score_v1": None,
            "median_path_value_score_v1": None,
            "mean_mfe_20d": None,
            "mean_mae_20d": None,
            "plus5_before_minus5_rate": None,
            "minus5_before_plus5_rate": None,
            "top15_rate": None,
            "bottom15_rate": None,
            "positive_month_rate": None,
            "worst_month_mean_path_value": None,
            "best_month_mean_path_value": None,
        }
    return {
        "sample_count": int(len(frame)),
        "unique_symbol_count": int(frame["code"].nunique()),
        "month_count": int(frame["trade_month"].nunique()),
        "mean_forward_ret_5d": _safe_float(frame["forward_ret_5d"].mean()),
        "mean_forward_ret_10d": _safe_float(frame["forward_ret_10d"].mean()),
        "mean_forward_ret_20d": _safe_float(frame["forward_ret_20d"].mean()),
        "median_forward_ret_20d": _safe_float(frame["forward_ret_20d"].median()),
        "mean_path_value_score_v1": _safe_float(frame["path_value_score_v1"].mean()),
        "median_path_value_score_v1": _safe_float(frame["path_value_score_v1"].median()),
        "mean_mfe_20d": _safe_float(frame["mfe_20d"].mean()),
        "mean_mae_20d": _safe_float(frame["mae_20d"].mean()),
        "plus5_before_minus5_rate": _safe_float(frame["hit_plus_5_before_minus_5"].mean()),
        "minus5_before_plus5_rate": _safe_float(frame["hit_minus_5_before_plus_5"].mean()),
        "top15_rate": _safe_float(frame["top15_label"].mean()),
        "bottom15_rate": _safe_float(frame["bottom15_label"].mean()),
        "positive_month_rate": _safe_float((frame.groupby("trade_month")["path_value_score_v1"].mean() > 0).mean()) if frame["trade_month"].nunique() > 0 else None,
        "worst_month_mean_path_value": _safe_float(frame.groupby("trade_month")["path_value_score_v1"].mean().min()) if frame["trade_month"].nunique() > 0 else None,
        "best_month_mean_path_value": _safe_float(frame.groupby("trade_month")["path_value_score_v1"].mean().max()) if frame["trade_month"].nunique() > 0 else None,
    }


def _aggregate_shape_groups(shape_frame: pd.DataFrame) -> pd.DataFrame:
    if shape_frame.empty:
        return pd.DataFrame()
    grouped = (
        shape_frame.groupby("candle_shape_modifier", dropna=False)
        .agg(
            sample_count=("candle_shape_modifier", "size"),
            unique_symbol_count=("code", "nunique"),
            month_count=("trade_month", "nunique"),
            mean_forward_ret_5d=("forward_ret_5d", "mean"),
            mean_forward_ret_10d=("forward_ret_10d", "mean"),
            mean_forward_ret_20d=("forward_ret_20d", "mean"),
            median_forward_ret_20d=("forward_ret_20d", "median"),
            mean_path_value_score_v1=("path_value_score_v1", "mean"),
            median_path_value_score_v1=("path_value_score_v1", "median"),
            mean_mfe_20d=("mfe_20d", "mean"),
            mean_mae_20d=("mae_20d", "mean"),
            plus5_before_minus5_rate=("hit_plus_5_before_minus_5", "mean"),
            minus5_before_plus5_rate=("hit_minus_5_before_plus_5", "mean"),
            top15_rate=("top15_label", "mean"),
            bottom15_rate=("bottom15_label", "mean"),
        )
        .reset_index()
    )
    grouped["positive_month_rate"] = grouped["candle_shape_modifier"].map(
        lambda shape: _safe_float(
            (shape_frame.loc[shape_frame["candle_shape_modifier"] == shape]
             .groupby("trade_month")["path_value_score_v1"].mean() > 0).mean()
        )
    )
    grouped["worst_month_mean_path_value"] = grouped["candle_shape_modifier"].map(
        lambda shape: _safe_float(
            shape_frame.loc[shape_frame["candle_shape_modifier"] == shape]
            .groupby("trade_month")["path_value_score_v1"].mean().min()
        )
    )
    grouped["best_month_mean_path_value"] = grouped["candle_shape_modifier"].map(
        lambda shape: _safe_float(
            shape_frame.loc[shape_frame["candle_shape_modifier"] == shape]
            .groupby("trade_month")["path_value_score_v1"].mean().max()
        )
    )
    return grouped


def _classify_shape_rows(
    summary_frame: pd.DataFrame,
    *,
    base_mean: float | None,
    base_median: float | None,
    base_bottom15: float | None,
    monthly_stats_by_shape: dict[str, dict[str, Any]] | None = None,
) -> pd.DataFrame:
    frame = summary_frame.copy()
    classifications: list[str] = []
    for _, row in frame.iterrows():
        sample_count = int(row["sample_count"]) if pd.notna(row["sample_count"]) else 0
        unique_symbol_count = int(row["unique_symbol_count"]) if pd.notna(row["unique_symbol_count"]) else 0
        month_count = int(row["month_count"]) if pd.notna(row["month_count"]) else 0
        mean_score = _safe_float(row.get("mean_path_value_score_v1"))
        median_score = _safe_float(row.get("median_path_value_score_v1"))
        bottom15 = _safe_float(row.get("bottom15_rate"))
        positive_month_rate = _safe_float(row.get("positive_month_rate"))
        shape_name = str(row.get("candle_shape_modifier"))
        monthly_stats = monthly_stats_by_shape.get(shape_name, {}) if monthly_stats_by_shape else {}
        win_month_count = int(monthly_stats.get("win_month_count", 0) or 0)
        loss_month_count = int(monthly_stats.get("loss_month_count", 0) or 0)
        if sample_count < BASE_SAMPLE_COUNT_MIN or unique_symbol_count < BASE_UNIQUE_SYMBOL_MIN or month_count < BASE_MONTH_COUNT_MIN:
            classifications.append("shape_sparse_or_unstable")
            continue
        if (
            mean_score is not None
            and median_score is not None
            and base_mean is not None
            and base_median is not None
            and base_bottom15 is not None
            and mean_score > base_mean
            and median_score > 0
            and bottom15 is not None
            and bottom15 <= base_bottom15
            and positive_month_rate is not None
            and positive_month_rate >= 0.55
        ):
            classifications.append("shape_positive_modifier")
            continue
        if (
            mean_score is not None
            and median_score is not None
            and base_mean is not None
            and base_median is not None
            and base_bottom15 is not None
            and mean_score < base_mean
            and median_score < 0
            and bottom15 is not None
            and bottom15 >= base_bottom15
            and positive_month_rate is not None
            and positive_month_rate <= 0.45
        ):
            classifications.append("shape_negative_modifier")
            continue
        if win_month_count > 0 and loss_month_count > 0:
            classifications.append("shape_context_dependent")
            continue
        classifications.append("shape_neutral")
    frame["shape_classification"] = classifications
    return frame


def _build_monthly_stability(shape_vs_base: pd.DataFrame, *, base_monthly_frame: pd.DataFrame) -> dict[str, Any]:
    if shape_vs_base.empty:
        return {"schema_version": STABILITY_SCHEMA, "generated_at": _utc_now(), "rows": []}
    monthly_deltas = []
    for shape, shape_group in shape_vs_base.groupby("candle_shape_modifier", dropna=False):
        month_values = shape_group.groupby("trade_month").agg(
            shape_month_mean=("shape_month_mean_path_value", "mean"),
            base_month_mean=("base_month_mean_path_value", "mean"),
        ).reset_index()
        month_values["delta"] = month_values["shape_month_mean"] - month_values["base_month_mean"]
        deltas = month_values["delta"].dropna()
        monthly_deltas.append(
            {
                "candle_shape_modifier": shape,
                "month_count": int(len(month_values)),
                "win_month_count": int((month_values["delta"] > 0).sum()),
                "loss_month_count": int((month_values["delta"] < 0).sum()),
                "flat_month_count": int((month_values["delta"] == 0).sum()),
                "worst_month_delta": None if deltas.empty else _safe_float(deltas.min()),
                "best_month_delta": None if deltas.empty else _safe_float(deltas.max()),
            }
        )
    return {
        "schema_version": STABILITY_SCHEMA,
        "generated_at": _utc_now(),
        "base_monthly_summary": _json_ready(base_monthly_frame.to_dict(orient="records")),
        "rows": monthly_deltas,
    }


def run_conditional_high_value_candle_shape_modifier_v1(
    *,
    source_context_session: str | Path | None = None,
    source_family_session: str | Path | None = None,
    context_gated_boost_session: str | Path | None = None,
    db_path: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_symbols: int | None = DEFAULT_LIMIT_SYMBOLS,
) -> dict[str, Any]:
    run_started = time.perf_counter()
    context_session_path = _resolve_context_session(source_context_session)
    source_family_session_path = _resolve_source_family_session(source_family_session)
    boost_session_path = _resolve_context_gated_boost_session(context_gated_boost_session)
    db_path = _safe_path(db_path, DEFAULT_DB_PATH)
    if not db_path.exists():
        raise FileNotFoundError(f"runtime DB not found: {db_path}")
    output_root_path = _resolve_output_root(output_root)
    output_root_path.mkdir(parents=True, exist_ok=True)

    session_id = _make_session_id()
    session_tmp = output_root_path / f"{session_id}.tmp"
    session_final = output_root_path / session_id
    session_tmp.mkdir(parents=True, exist_ok=False)

    _progress_log(
        f"start context_session={context_session_path} family_session={source_family_session_path} boost_session={boost_session_path} db={db_path} out_root={output_root_path} session={session_id}"
    )

    context_payloads = _load_context_session(context_session_path)
    source_family_payloads = _load_source_family_session(source_family_session_path)
    boost_payloads = _load_context_gated_boost_session(boost_session_path)
    top15_threshold, bottom15_threshold, source_thresholds = _extract_source_thresholds(source_family_payloads)

    thresholds = {
        "baseline_mean_path_value_score_v1": _safe_float(context_payloads["summary"]["baseline_metrics"]["mean_path_value_score_v1"]),
        "baseline_median_path_value_score_v1": _safe_float(context_payloads["summary"]["baseline_metrics"]["median_path_value_score_v1"]),
        "baseline_plus5_before_minus5_rate": _safe_float(context_payloads["summary"]["baseline_metrics"]["plus5_before_minus5_rate"]),
        "baseline_minus5_before_plus5_rate": _safe_float(context_payloads["summary"]["baseline_metrics"]["minus5_before_plus5_rate"]),
        "baseline_bottom15_rate": _safe_float(context_payloads["summary"]["baseline_metrics"]["bottom15_rate"]),
        "baseline_top15_rate": _safe_float(context_payloads["summary"]["baseline_metrics"]["top15_rate"]),
        "min_sample_count": 100,
        "min_unique_symbol_count": 20,
        "min_month_count": 8,
    }

    triple_frame, monthly_context_frame, weekly_context_frame, gate_frame = _build_conditional_gate(
        context_row_parquet=context_payloads["row_parquet"],
        thresholds=thresholds,
    )
    triple_class_counts = _count_classes(triple_frame)
    monthly_context_class_counts = _count_classes(monthly_context_frame)
    weekly_context_class_counts = _count_classes(weekly_context_frame)

    if limit_symbols is None:
        limit_symbols = None
    else:
        limit_symbols = _safe_int(limit_symbols)
    code_filter_sql = ""
    code_filter_clause = ""
    if limit_symbols is not None and limit_symbols > 0:
        limited_codes = (
            triple_frame["code"].dropna().astype(str).sort_values().unique().tolist() if "code" in triple_frame.columns else []
        )
        # Fallback to the source rows if the group summary does not carry code-level labels.
        if not limited_codes:
            conn = duckdb.connect()
            try:
                conn.execute(f"CREATE TEMP VIEW conditional_rows_source AS SELECT * FROM read_parquet('{context_payloads['row_parquet'].as_posix()}')")
                limited_codes = [row[0] for row in conn.execute(
                    "SELECT DISTINCT code FROM conditional_rows_source ORDER BY code LIMIT ?",
                    [limit_symbols],
                ).fetchall()]
            finally:
                conn.close()
        else:
            limited_codes = limited_codes[:limit_symbols]
        if limited_codes:
            quoted_codes = []
            for code in limited_codes:
                quoted_codes.append("'" + str(code).replace("'", "''") + "'")
            code_filter_clause = ", ".join(quoted_codes)
            code_filter_sql = f"WHERE code IN ({code_filter_clause})"
    scope_symbol_count = None
    if limit_symbols is not None and limit_symbols > 0:
        scope_symbol_count = int(limit_symbols)

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        conn.execute(f"CREATE TEMP VIEW conditional_rows_source AS SELECT * FROM read_parquet('{context_payloads['row_parquet'].as_posix()}')")
        if code_filter_sql:
            conn.execute(f"CREATE TEMP VIEW conditional_rows_scope AS SELECT * FROM conditional_rows_source {code_filter_sql}")
        else:
            conn.execute("CREATE TEMP VIEW conditional_rows_scope AS SELECT * FROM conditional_rows_source")
        conn.register("conditional_high_value_gate", gate_frame[["monthly_context", "weekly_context", "state_family_id"]].copy())
        conn.execute(
            f"""
            CREATE TEMP VIEW shape_source AS
            WITH bars AS (
                SELECT
                    code::VARCHAR AS code,
                    CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) AS trade_date,
                    o,
                    h,
                    l,
                    c,
                    v,
                    LAG(o) OVER w AS prev_o,
                    LAG(h) OVER w AS prev_h,
                    LAG(l) OVER w AS prev_l,
                    LAG(c) OVER w AS prev_c,
                    LAG(CASE WHEN h = l THEN NULL ELSE ABS(c - o) / NULLIF(h - l, 0) END) OVER w AS prev_body_ratio,
                    LAG(CASE WHEN h = l THEN NULL ELSE (h - GREATEST(o, c)) / NULLIF(h - l, 0) END) OVER w AS prev_upper_wick_ratio,
                    LAG(CASE WHEN h = l THEN NULL ELSE (LEAST(o, c) - l) / NULLIF(h - l, 0) END) OVER w AS prev_lower_wick_ratio
                FROM daily_bars
                {code_filter_sql}
                WINDOW w AS (PARTITION BY code ORDER BY date)
            ),
            features AS (
                SELECT
                    code::VARCHAR AS code,
                    CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) AS trade_date,
                    candle_body_ratio,
                    candle_upper_wick_ratio,
                    candle_lower_wick_ratio,
                    candle_triplet_up_prob,
                    candle_triplet_down_prob,
                    gap_pct,
                    vol_ratio5_20
                FROM ml_feature_daily
                {code_filter_sql}
            )
            SELECT
                b.code,
                b.trade_date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                b.prev_o,
                b.prev_h,
                b.prev_l,
                b.prev_c,
                b.prev_body_ratio,
                b.prev_upper_wick_ratio,
                b.prev_lower_wick_ratio,
                f.candle_body_ratio,
                f.candle_upper_wick_ratio,
                f.candle_lower_wick_ratio,
                f.candle_triplet_up_prob,
                f.candle_triplet_down_prob,
                f.gap_pct,
                f.vol_ratio5_20
            FROM bars b
            LEFT JOIN features f
              ON f.code = b.code
             AND f.trade_date = b.trade_date
            """
        )
        row_parquet_path = session_tmp / "conditional_shape_rows.parquet"
        joined_sql = f"""
        WITH joined AS (
            SELECT
                c.code,
                c.trade_date,
                c.trade_dt,
                c.trade_month,
                c.state_family_id,
                c.position_state_id,
                c.family_classification,
                c.stable_high_value_family,
                c.stable_bad_pick_family,
                c.regime_dependent_family,
                c.unstable_or_sparse_family,
                c.neutral_family,
                c.family_regime_context,
                c.family_bad_pick_regime,
                c.dominant_regime_context,
                c.family_sample_count,
                c.family_unique_symbol_count,
                c.family_month_count,
                c.family_mean_forward_ret_5d,
                c.family_mean_forward_ret_10d,
                c.family_mean_forward_ret_20d,
                c.family_median_forward_ret_20d,
                c.family_mean_mfe_20d,
                c.family_mean_mae_20d,
                c.family_mean_path_value_score_v1,
                c.family_median_path_value_score_v1,
                c.family_plus5_before_minus5_rate,
                c.family_minus5_before_plus5_rate,
                c.family_top15_rate,
                c.family_bottom15_rate,
                c.family_months_observed,
                c.family_positive_month_rate,
                c.family_worst_month_mean_path_value,
                c.family_best_month_mean_path_value,
                c.family_regime_count,
                c.family_regime_consistency_score,
                c.family_score_spread,
                c.entry_next_open,
                c.entry_day_close,
                c.forward_window_days,
                c.forward_ret_5d,
                c.forward_ret_10d,
                c.forward_ret_20d,
                c.mfe_20d,
                c.mae_20d,
                c.days_to_mfe_20d,
                c.days_to_mae_20d,
                c.days_to_positive_close,
                c.days_to_plus_3pct,
                c.days_to_plus_5pct,
                c.days_to_minus_3pct,
                c.days_to_minus_5pct,
                c.hit_plus_5_before_minus_5,
                c.hit_minus_5_before_plus_5,
                c.hit_plus_3_before_minus_3,
                c.hit_minus_3_before_plus_3,
                c.hit_plus_1atr_before_minus_1atr,
                c.mfe_atr_20d,
                c.mae_atr_20d,
                c.close_above_entry_days_20d,
                c.close_below_entry_days_20d,
                c.path_value_score_v1,
                c.monthly_context,
                c.monthly_context_date,
                c.monthly_context_source,
                c.monthly_context_no_lookahead,
                c.weekly_context,
                c.weekly_context_date,
                c.weekly_context_source,
                c.weekly_context_no_lookahead,
                c.top15_label,
                c.bottom15_label,
                CASE WHEN gate.state_family_id IS NOT NULL THEN TRUE ELSE FALSE END AS conditional_high_value,
                s.o,
                s.h,
                s.l,
                s.c,
                s.v,
                s.prev_o,
                s.prev_h,
                s.prev_l,
                s.prev_c,
                s.prev_body_ratio,
                s.prev_upper_wick_ratio,
                s.prev_lower_wick_ratio,
                s.candle_body_ratio,
                s.candle_upper_wick_ratio,
                s.candle_lower_wick_ratio,
                s.candle_triplet_up_prob,
                s.candle_triplet_down_prob,
                s.gap_pct,
                s.vol_ratio5_20,
                {_shape_case_sql()}
            FROM conditional_rows_scope c
            LEFT JOIN shape_source s
              ON s.code = c.code
             AND s.trade_date = c.trade_date
            LEFT JOIN conditional_high_value_gate gate
              ON gate.monthly_context = c.monthly_context
             AND gate.weekly_context = c.weekly_context
             AND gate.state_family_id = c.state_family_id
        )
        SELECT * FROM joined
        WHERE conditional_high_value = TRUE
        ORDER BY code, trade_dt, state_family_id
        """
        conn.execute(f"COPY ({joined_sql}) TO '{row_parquet_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        conn.execute(f"CREATE TEMP VIEW conditional_rows AS SELECT * FROM read_parquet('{row_parquet_path.as_posix()}')")
        if limit_symbols is not None and limit_symbols > 0:
            scope_symbol_count = int(conn.execute("SELECT COUNT(DISTINCT code) FROM conditional_rows").fetchone()[0])
        conditional_row_count = int(conn.execute("SELECT COUNT(*) FROM conditional_rows").fetchone()[0])
        conditional_code_count = int(conn.execute("SELECT COUNT(DISTINCT code) FROM conditional_rows").fetchone()[0])
        conditional_gate_count = int(len(gate_frame))

        if limit_symbols is None and conditional_gate_count != int(context_payloads["classification"]["triple_level"]["class_counts"]["conditional_high_value"]):
            raise RuntimeError("triple-level conditional_high_value count does not match authoritative context artifact")

        source_rows_df = pd.read_parquet(context_payloads["row_parquet"])
    finally:
        conn.close()

    # Re-open for summary queries on the materialized parquet.
    conn = duckdb.connect()
    try:
        conn.execute(f"CREATE TEMP VIEW conditional_rows AS SELECT * FROM read_parquet('{row_parquet_path.as_posix()}')")
        base_slice_frame = conn.execute(_build_group_summary_sql(["monthly_context", "weekly_context", "state_family_id"])).fetchdf()
        shape_slice_frame = conn.execute(_build_group_summary_sql(["monthly_context", "weekly_context", "state_family_id", "candle_shape_modifier"])).fetchdf()
        shape_only_frame = conn.execute(_build_group_summary_sql(["candle_shape_modifier"])).fetchdf()
        monthly_shape_frame = conn.execute(_build_group_summary_sql(["monthly_context", "candle_shape_modifier"])).fetchdf()
        weekly_shape_frame = conn.execute(_build_group_summary_sql(["weekly_context", "candle_shape_modifier"])).fetchdf()
    finally:
        conn.close()

    base_overall = _aggregate_overall_metrics(pd.read_parquet(row_parquet_path))
    conditional_rows_df = pd.read_parquet(row_parquet_path)
    conditional_key_frame = conditional_rows_df[["code", "trade_dt", "state_family_id"]].drop_duplicates()
    reference_non_conditional = source_rows_df.merge(
        conditional_key_frame.assign(_is_conditional=1),
        on=["code", "trade_dt", "state_family_id"],
        how="left",
    )
    reference_non_conditional = reference_non_conditional.loc[reference_non_conditional["_is_conditional"].isna()].drop(columns=["_is_conditional"])
    shape_only_summary = shape_only_frame.copy()
    monthly_shape_summary = monthly_shape_frame.copy()
    weekly_shape_summary = weekly_shape_frame.copy()
    base_slice_frame = base_slice_frame.copy()
    shape_slice_frame = shape_slice_frame.copy()

    shape_vs_base = shape_slice_frame.merge(
        base_slice_frame[
            [
                "monthly_context",
                "weekly_context",
                "state_family_id",
                "sample_count",
                "unique_symbol_count",
                "month_count",
                "mean_forward_ret_5d",
                "mean_forward_ret_10d",
                "mean_forward_ret_20d",
                "median_forward_ret_20d",
                "mean_path_value_score_v1",
                "median_path_value_score_v1",
                "mean_mfe_20d",
                "mean_mae_20d",
                "plus5_before_minus5_rate",
                "minus5_before_plus5_rate",
                "top15_rate",
                "bottom15_rate",
                "positive_month_rate",
                "worst_month_mean_path_value",
                "best_month_mean_path_value",
            ]
        ],
        on=["monthly_context", "weekly_context", "state_family_id"],
        how="left",
        suffixes=("_shape", "_base"),
    )
    for metric in [
        "mean_forward_ret_5d",
        "mean_forward_ret_10d",
        "mean_forward_ret_20d",
        "median_forward_ret_20d",
        "mean_path_value_score_v1",
        "median_path_value_score_v1",
        "mean_mfe_20d",
        "mean_mae_20d",
        "plus5_before_minus5_rate",
        "minus5_before_plus5_rate",
        "top15_rate",
        "bottom15_rate",
        "positive_month_rate",
    ]:
        shape_vs_base[f"delta_{metric}"] = shape_vs_base[f"{metric}_shape"] - shape_vs_base[f"{metric}_base"]
    shape_vs_base["sample_retention_rate"] = shape_vs_base["sample_count_shape"] / shape_vs_base["sample_count_base"].replace(0, pd.NA)
    shape_vs_base["base_month_mean_path_value"] = shape_vs_base.groupby(["monthly_context", "weekly_context", "state_family_id"])["mean_path_value_score_v1_base"].transform("first")
    shape_vs_base["shape_month_mean_path_value"] = shape_vs_base["mean_path_value_score_v1_shape"]

    shape_by_modifier = []
    for shape, group in shape_vs_base.groupby("candle_shape_modifier", dropna=False):
        deltas = group["delta_mean_path_value_score_v1"].dropna()
        shape_by_modifier.append(
            {
                "candle_shape_modifier": shape,
                "sample_count": int(group["sample_count_shape"].sum()),
                "unique_symbol_count": int(shape_only_frame.loc[shape_only_frame["candle_shape_modifier"] == shape, "unique_symbol_count"].sum()) if not shape_only_frame.empty else 0,
                "month_count": int(shape_only_frame.loc[shape_only_frame["candle_shape_modifier"] == shape, "month_count"].max()) if not shape_only_frame.empty else 0,
                "mean_forward_ret_5d": _safe_float(group["mean_forward_ret_5d_shape"].mean()),
                "mean_forward_ret_10d": _safe_float(group["mean_forward_ret_10d_shape"].mean()),
                "mean_forward_ret_20d": _safe_float(group["mean_forward_ret_20d_shape"].mean()),
                "median_forward_ret_20d": _safe_float(group["median_forward_ret_20d_shape"].median()),
                "mean_path_value_score_v1": _safe_float(group["mean_path_value_score_v1_shape"].mean()),
                "median_path_value_score_v1": _safe_float(group["median_path_value_score_v1_shape"].median()),
                "mean_mfe_20d": _safe_float(group["mean_mfe_20d_shape"].mean()),
                "mean_mae_20d": _safe_float(group["mean_mae_20d_shape"].mean()),
                "plus5_before_minus5_rate": _safe_float(group["plus5_before_minus5_rate_shape"].mean()),
                "minus5_before_plus5_rate": _safe_float(group["minus5_before_plus5_rate_shape"].mean()),
                "top15_rate": _safe_float(group["top15_rate_shape"].mean()),
                "bottom15_rate": _safe_float(group["bottom15_rate_shape"].mean()),
                "positive_month_rate": _safe_float(group["positive_month_rate_shape"].mean()),
                "worst_month_mean_path_value": _safe_float(group["worst_month_mean_path_value_shape"].min()),
                "best_month_mean_path_value": _safe_float(group["best_month_mean_path_value_shape"].max()),
                "delta_mean_forward_ret_20d": _safe_float(group["delta_mean_forward_ret_20d"].mean()),
                "delta_mean_path_value_score_v1": _safe_float(group["delta_mean_path_value_score_v1"].mean()),
                "delta_bottom15_rate": _safe_float(group["delta_bottom15_rate"].mean()),
                "delta_top15_rate": _safe_float(group["delta_top15_rate"].mean()),
                "sample_retention_rate": _safe_float(group["sample_retention_rate"].mean()),
                "base_slice_count": int(group["state_family_id"].nunique()),
            }
        )

    monthly_base = (
        conditional_rows_df.groupby("trade_month")["path_value_score_v1"].mean().reset_index(name="base_month_mean_path_value")
    )
    monthly_shape = (
        conditional_rows_df.groupby(["trade_month", "candle_shape_modifier"])["path_value_score_v1"]
        .mean()
        .reset_index(name="shape_month_mean_path_value")
    )
    monthly_shape_joined = monthly_shape.merge(monthly_base, on="trade_month", how="left")
    monthly_shape_joined["delta"] = monthly_shape_joined["shape_month_mean_path_value"] - monthly_shape_joined["base_month_mean_path_value"]
    shape_monthly_rows = []
    for shape, group in monthly_shape_joined.groupby("candle_shape_modifier", dropna=False):
        deltas = group["delta"].dropna()
        shape_monthly_rows.append(
            {
                "candle_shape_modifier": shape,
                "month_count": int(len(group)),
                "win_month_count": int((group["delta"] > 0).sum()),
                "loss_month_count": int((group["delta"] < 0).sum()),
                "flat_month_count": int((group["delta"] == 0).sum()),
                "worst_month_delta": None if deltas.empty else _safe_float(deltas.min()),
                "best_month_delta": None if deltas.empty else _safe_float(deltas.max()),
            }
        )

    monthly_stats_by_shape = {row["candle_shape_modifier"]: row for row in shape_monthly_rows}
    shape_by_modifier_frame = pd.DataFrame(shape_by_modifier)
    shape_by_modifier_frame = _classify_shape_rows(
        shape_by_modifier_frame,
        base_mean=base_overall["mean_path_value_score_v1"],
        base_median=base_overall["median_path_value_score_v1"],
        base_bottom15=base_overall["bottom15_rate"],
        monthly_stats_by_shape=monthly_stats_by_shape,
    )

    base_slice_count = int(base_slice_frame["state_family_id"].nunique()) if not base_slice_frame.empty else 0
    shape_vs_base_table = []
    for _, row in shape_by_modifier_frame.sort_values(["delta_mean_path_value_score_v1", "sample_count"], ascending=[False, False]).iterrows():
        shape_vs_base_table.append(_json_ready(row.to_dict()))

    reference_non_conditional_summary = {
        "sample_count": int(len(reference_non_conditional)) if not reference_non_conditional.empty else 0,
        "unique_symbol_count": int(reference_non_conditional["code"].nunique()) if not reference_non_conditional.empty else 0,
        "month_count": int(reference_non_conditional["trade_month"].nunique()) if "trade_month" in reference_non_conditional.columns and not reference_non_conditional.empty else 0,
        "mean_forward_ret_20d": _safe_float(reference_non_conditional["forward_ret_20d"].mean()) if not reference_non_conditional.empty else None,
        "mean_path_value_score_v1": _safe_float(reference_non_conditional["path_value_score_v1"].mean()) if not reference_non_conditional.empty else None,
    }

    shape_positive_examples = shape_by_modifier_frame.sort_values(["delta_mean_path_value_score_v1", "sample_count"], ascending=[False, False]).head(TOP_EXAMPLE_LIMIT)
    shape_negative_examples = shape_by_modifier_frame.sort_values(["delta_mean_path_value_score_v1", "sample_count"], ascending=[True, False]).head(TOP_EXAMPLE_LIMIT)

    summary_payload = {
        "schema_version": SUMMARY_SCHEMA,
        "generated_at": _utc_now(),
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "reference_context_gated_boost_session_id": boost_session_path.name,
        "source_thresholds": {
            "top15_score_threshold": top15_threshold,
            "bottom15_score_threshold": bottom15_threshold,
            "source_threshold_keys": sorted(map(str, source_thresholds.keys())),
        },
        "conditional_high_value_gate_count": int(len(gate_frame)),
        "conditional_high_value_row_count": int(len(conditional_rows_df)),
        "non_conditional_reference_row_count": int(len(reference_non_conditional)),
        "base_overall_summary": base_overall,
        "reference_non_conditional_summary": reference_non_conditional_summary,
        "shape_bucket_count": int(len(shape_by_modifier_frame)),
        "shape_bucket_rows": shape_vs_base_table,
        "shape_bucket_counts": {
            str(shape): int(count)
            for shape, count in conditional_rows_df["candle_shape_modifier"].value_counts(dropna=False).sort_index().items()
        },
        "base_slice_count": base_slice_count,
    }

    classification_payload = {
        "schema_version": CLASSIFICATION_SCHEMA,
        "generated_at": _utc_now(),
        "rows": shape_vs_base_table,
        "positive_examples": _json_ready(shape_positive_examples.to_dict(orient="records")),
        "negative_examples": _json_ready(shape_negative_examples.to_dict(orient="records")),
        "shape_class_counts": {
            str(label): int((shape_by_modifier_frame["shape_classification"] == label).sum())
            for label in [
                "shape_positive_modifier",
                "shape_negative_modifier",
                "shape_context_dependent",
                "shape_sparse_or_unstable",
                "shape_neutral",
            ]
        },
    }

    comparison_payload = {
        "schema_version": COMPARISON_SCHEMA,
        "generated_at": _utc_now(),
        "base_slice_comparison": _json_ready(base_slice_frame.head(TOP_EXAMPLE_LIMIT).to_dict(orient="records")),
        "shape_vs_base_slice_rows": shape_vs_base_table,
        "monthly_context_shape_rows": _json_ready(monthly_shape_frame.head(TOP_EXAMPLE_LIMIT).to_dict(orient="records")),
        "weekly_context_shape_rows": _json_ready(weekly_shape_frame.head(TOP_EXAMPLE_LIMIT).to_dict(orient="records")),
    }

    stability_payload = {
        "schema_version": STABILITY_SCHEMA,
        "generated_at": _utc_now(),
        "rows": shape_monthly_rows,
    }

    if not shape_by_modifier_frame.empty:
        best_shape = shape_by_modifier_frame.sort_values(["delta_mean_path_value_score_v1", "sample_count"], ascending=[False, False]).iloc[0]
        worst_shape = shape_by_modifier_frame.sort_values(["delta_mean_path_value_score_v1", "sample_count"], ascending=[True, False]).iloc[0]
    else:
        best_shape = None
        worst_shape = None

    recommendation = "hold"
    decision_reason = "shape_signal_exists_but_remains_sparse_or_context_specific"
    if best_shape is not None:
        positive_count = int((shape_by_modifier_frame["shape_classification"] == "shape_positive_modifier").sum())
        negative_count = int((shape_by_modifier_frame["shape_classification"] == "shape_negative_modifier").sum())
        if positive_count >= 3 and _safe_float(best_shape["delta_mean_path_value_score_v1"]) is not None and _safe_float(best_shape["delta_mean_path_value_score_v1"]) > 0 and _safe_float(shape_by_modifier_frame["bottom15_rate"].mean()) is not None and _safe_float(shape_by_modifier_frame["bottom15_rate"].mean()) <= base_overall["bottom15_rate"]:
            recommendation = "keep"
            decision_reason = "several_shape_modifiers_show_positive_lift_inside_conditional_gate"
        elif positive_count == 0 or _safe_float(best_shape["delta_mean_path_value_score_v1"]) is None or _safe_float(best_shape["delta_mean_path_value_score_v1"]) <= 0:
            recommendation = "drop"
            decision_reason = "shape_buckets_do_not_improve_over_conditional_base"

    decision_payload = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "reference_context_gated_boost_session_id": boost_session_path.name,
        "conditional_high_value_gate_count": int(len(gate_frame)),
        "conditional_high_value_row_count": int(len(conditional_rows_df)),
        "shape_bucket_count": int(len(shape_by_modifier_frame)),
        "shape_positive_modifier_count": int((shape_by_modifier_frame["shape_classification"] == "shape_positive_modifier").sum()),
        "shape_negative_modifier_count": int((shape_by_modifier_frame["shape_classification"] == "shape_negative_modifier").sum()),
        "shape_context_dependent_count": int((shape_by_modifier_frame["shape_classification"] == "shape_context_dependent").sum()),
        "shape_sparse_or_unstable_count": int((shape_by_modifier_frame["shape_classification"] == "shape_sparse_or_unstable").sum()),
        "shape_neutral_count": int((shape_by_modifier_frame["shape_classification"] == "shape_neutral").sum()),
        "recommendation": recommendation,
        "typed_reasons": [decision_reason],
        "no_lookahead_inherited": bool(context_payloads["decision"].get("no_lookahead_inherited", True)),
        "monthly_context_no_lookahead": True,
        "weekly_context_no_lookahead": True,
    }

    manifest_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_context_session_path": str(context_session_path),
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "reference_context_gated_boost_session_id": boost_session_path.name,
        "reference_context_gated_boost_session_path": str(boost_session_path),
        "db_path": str(db_path),
        "output_root": str(output_root_path),
        "scope_symbol_limit": scope_symbol_count,
        "scope_symbol_count": conditional_code_count,
        "conditional_high_value_gate_count": int(len(gate_frame)),
        "conditional_high_value_row_count": int(len(conditional_rows_df)),
        "non_conditional_reference_row_count": int(max(0, len(pd.read_parquet(context_payloads["row_parquet"])) - len(conditional_rows_df))),
        "monthly_context_count": int(conditional_rows_df["monthly_context"].nunique()) if not conditional_rows_df.empty else 0,
        "weekly_context_count": int(conditional_rows_df["weekly_context"].nunique()) if not conditional_rows_df.empty else 0,
        "source_artifacts": {
            "context_run_manifest_json": str(context_session_path / "run_manifest.json"),
            "context_summary_json": str(context_session_path / "conditional_state_value_summary.json"),
            "context_classification_json": str(context_session_path / "conditional_state_classification.json"),
            "context_decision_json": str(context_session_path / "multi_timeframe_conditional_state_value_v1_decision.json"),
            "context_rows_parquet": str(context_payloads["row_parquet"]),
            "family_filter_run_manifest_json": str(source_family_session_path / "run_manifest.json"),
            "family_filter_decision_json": str(source_family_session_path / "state_family_filter_v1_decision.json"),
            "family_filter_rows_parquet": str(source_family_session_path / "state_family_rows.parquet"),
            "prior_context_gated_boost_compare_json": str(boost_session_path / "multi_timeframe_context_gated_high_value_boost_v1_compare.json"),
            "prior_context_gated_boost_decision_json": str(boost_session_path / "multi_timeframe_context_gated_high_value_boost_v1_decision.json"),
        },
        "output_artifacts": {},
        "no_lookahead_inherited": bool(context_payloads["decision"].get("no_lookahead_inherited", True)),
        "monthly_context_no_lookahead": True,
        "weekly_context_no_lookahead": True,
        "conditional_high_value_gate_count_authoritative": int(context_payloads["classification"]["triple_level"]["class_counts"]["conditional_high_value"]),
        "shape_feature_source": {
            "confirmed_fields": [
                "candle_body_ratio",
                "candle_upper_wick_ratio",
                "candle_lower_wick_ratio",
                "candle_triplet_up_prob",
                "candle_triplet_down_prob",
                "gap_pct",
                "vol_ratio5_20",
                "o",
                "h",
                "l",
                "c",
                "prev_o",
                "prev_h",
                "prev_l",
                "prev_c",
            ],
            "provisional_fields": [
                "inside_bar",
                "bull_engulfing",
                "bear_engulfing",
                "gap_up_bull",
                "gap_down_bear",
                "koma_then_bull",
                "koma_then_bear",
                "lower_wick_then_bull",
                "upper_wick_then_bear",
                "bull_large",
                "bear_large",
                "doji_like",
                "neutral_small",
                "lower_wick_dominant",
                "upper_wick_dominant",
                "no_clear_shape",
            ],
            "note": "feature_snapshot_daily.candle_flags is present in the DB but null in the inspected table sample, so v1 uses ml_feature_daily + daily_bars instead.",
        },
        "shape_buckets_omitted": [],
        "source_thresholds": {
            "top15_score_threshold": top15_threshold,
            "bottom15_score_threshold": bottom15_threshold,
            "source_threshold_keys": sorted(map(str, source_thresholds.keys())),
        },
    }

    output_files = {
        "run_manifest_json": session_tmp / "run_manifest.json",
        "candle_shape_definition_json": session_tmp / "candle_shape_definition.json",
        "conditional_shape_value_summary_json": session_tmp / "conditional_shape_value_summary.json",
        "conditional_shape_modifier_classification_json": session_tmp / "conditional_shape_modifier_classification.json",
        "shape_vs_base_slice_comparison_json": session_tmp / "shape_vs_base_slice_comparison.json",
        "shape_monthly_stability_json": session_tmp / "shape_monthly_stability.json",
        "conditional_shape_rows_parquet": session_tmp / "conditional_shape_rows.parquet",
        "conditional_high_value_candle_shape_modifier_v1_decision_json": session_tmp / "conditional_high_value_candle_shape_modifier_v1_decision.json",
        "_artifact_complete_json": session_tmp / "_ARTIFACT_COMPLETE.json",
    }
    manifest_payload["output_artifacts"] = {key: str(session_final / path.name) for key, path in output_files.items()}
    _write_json(output_files["run_manifest_json"], manifest_payload)
    _write_json(output_files["candle_shape_definition_json"], manifest_payload["shape_feature_source"])
    _write_json(output_files["conditional_shape_value_summary_json"], summary_payload)
    _write_json(output_files["conditional_shape_modifier_classification_json"], classification_payload)
    _write_json(output_files["shape_vs_base_slice_comparison_json"], comparison_payload)
    _write_json(output_files["shape_monthly_stability_json"], stability_payload)
    _write_json(output_files["conditional_high_value_candle_shape_modifier_v1_decision_json"], decision_payload)
    _write_json(
        output_files["_artifact_complete_json"],
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "session_id": session_id,
            "validated": True,
        },
    )

    for json_path in (
        output_files["run_manifest_json"],
        output_files["candle_shape_definition_json"],
        output_files["conditional_shape_value_summary_json"],
        output_files["conditional_shape_modifier_classification_json"],
        output_files["shape_vs_base_slice_comparison_json"],
        output_files["shape_monthly_stability_json"],
        output_files["conditional_high_value_candle_shape_modifier_v1_decision_json"],
        output_files["_artifact_complete_json"],
    ):
        json.loads(json_path.read_text(encoding="utf-8"))
    pd.read_parquet(output_files["conditional_shape_rows_parquet"])

    session_final.mkdir(parents=True, exist_ok=False)
    for path in output_files.values():
        shutil.move(str(path), str(session_final / path.name))

    _progress_log(f"finalized session={session_id} elapsed={time.perf_counter() - run_started:.1f}s")
    return {
        "session_id": session_id,
        "session_dir": str(session_final),
        "source_context_session_id": context_payloads["manifest"]["session_id"],
        "source_family_session_id": source_family_payloads["manifest"]["session_id"],
        "reference_context_gated_boost_session_id": boost_session_path.name,
        "conditional_high_value_gate_count": int(len(gate_frame)),
        "conditional_high_value_row_count": int(len(conditional_rows_df)),
        "shape_bucket_count": int(len(shape_by_modifier_frame)),
        "recommendation": recommendation,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze candle-shape modifiers inside the conditional-high-value gate.")
    parser.add_argument("--source-context-session", default=None)
    parser.add_argument("--source-family-session", default=None)
    parser.add_argument("--reference-context-gated-boost-session", default=None)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--limit-symbols", "--limit-codes", dest="limit_symbols", type=int, default=DEFAULT_LIMIT_SYMBOLS)
    args = parser.parse_args(argv)
    run_conditional_high_value_candle_shape_modifier_v1(
        source_context_session=args.source_context_session,
        source_family_session=args.source_family_session,
        context_gated_boost_session=args.reference_context_gated_boost_session,
        db_path=args.db_path,
        output_root=args.output_root,
        limit_symbols=args.limit_symbols,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
