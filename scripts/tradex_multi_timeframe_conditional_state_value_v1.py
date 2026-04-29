from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_ma_state_family_stability_filter import (  # noqa: E402
    _classify_family,
    _family_monthly_frame,
    _family_regime_frame,
    _family_summary_frame,
)

DEFAULT_SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
DEFAULT_RUNTIME_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1")
DEFAULT_LIMIT_CODES = None

SCHEMA_VERSION = "tradex_multi_timeframe_conditional_state_value_v1"
CONTEXT_SCHEMA_VERSION = "tradex_multi_timeframe_conditional_state_value_v1_context_definition_v1"
SUMMARY_SCHEMA_VERSION = "tradex_multi_timeframe_conditional_state_value_v1_summary_v1"
MONTHLY_SCHEMA_VERSION = "tradex_multi_timeframe_conditional_state_value_v1_by_monthly_v1"
WEEKLY_SCHEMA_VERSION = "tradex_multi_timeframe_conditional_state_value_v1_by_weekly_v1"
CLASSIFICATION_SCHEMA_VERSION = "tradex_multi_timeframe_conditional_state_value_v1_classification_v1"
GLOBAL_COMPARISON_SCHEMA_VERSION = "tradex_multi_timeframe_conditional_state_value_v1_global_comparison_v1"
DECISION_SCHEMA_VERSION = "tradex_multi_timeframe_conditional_state_value_v1_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_multi_timeframe_conditional_state_value_v1_manifest_v1"

TOP_EXAMPLE_LIMIT = 50
MIN_GROUP_SAMPLE_COUNT = 100
MIN_GROUP_UNIQUE_SYMBOL_COUNT = 20
MIN_GROUP_MONTH_COUNT = 8


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


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    if value is None:
        return fallback
    try:
        out = float(value)
    except Exception:
        return fallback
    return out if math.isfinite(out) else fallback


def _safe_int(value: Any, fallback: int | None = None) -> int | None:
    if value is None:
        return fallback
    try:
        return int(value)
    except Exception:
        return fallback


def _progress_log(message: str) -> None:
    print(f"[multi_timeframe_conditional_state_value_v1] {message}", file=sys.stderr, flush=True)


def _make_session_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _finalize_session_dir(session_tmp: Path, session_final: Path) -> None:
    if session_final.exists():
        raise FileExistsError(f"final session output already exists: {session_final}")
    try:
        session_tmp.replace(session_final)
    except Exception:
        shutil.move(str(session_tmp), str(session_final))


def _resolve_source_family_session(source_family_session: str | Path | None) -> Path:
    if source_family_session and str(source_family_session).strip():
        path = Path(str(source_family_session)).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"source family session not found: {path}")
        return path
    if DEFAULT_SOURCE_FAMILY_SESSION.exists():
        return DEFAULT_SOURCE_FAMILY_SESSION.resolve()
    raise FileNotFoundError("Could not resolve source family session. Pass --source-family-session.")


def _resolve_runtime_db_path(runtime_db_path: str | Path | None) -> Path:
    if runtime_db_path and str(runtime_db_path).strip():
        path = Path(str(runtime_db_path)).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"runtime db path not found: {path}")
        return path
    if DEFAULT_RUNTIME_DB_PATH.exists():
        return DEFAULT_RUNTIME_DB_PATH.resolve()
    raise FileNotFoundError("Could not resolve runtime db path. Pass --runtime-db-path.")


def _resolve_output_root(output_root: str | Path | None) -> Path:
    if output_root and str(output_root).strip():
        return Path(str(output_root)).expanduser().resolve()
    return DEFAULT_OUTPUT_ROOT.resolve()


def _load_source_family_session(source_family_session: Path) -> dict[str, Any]:
    manifest = _load_json(source_family_session / "run_manifest.json")
    family_summary_report = _load_json(source_family_session / "state_family_summary.json")
    decision = _load_json(source_family_session / "state_family_filter_v1_decision.json")
    classification = _load_json(source_family_session / "state_family_classification.json")
    by_regime = _load_json(source_family_session / "state_family_by_regime.json")
    monthly_stability = _load_json(source_family_session / "state_family_monthly_stability.json")
    row_parquet = source_family_session / "state_family_rows.parquet"
    if not row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {row_parquet}")
    thresholds = manifest.get("thresholds") or family_summary_report.get("filter_thresholds") or {}
    top15_threshold = _safe_float(thresholds.get("top15_score_threshold"))
    bottom15_threshold = _safe_float(thresholds.get("bottom15_score_threshold"))
    if top15_threshold is None or bottom15_threshold is None:
        raise RuntimeError("missing top15/bottom15 thresholds from family filter session")

    conn = duckdb.connect()
    try:
        conn.execute(f"CREATE TEMP VIEW family_rows AS SELECT * FROM read_parquet('{row_parquet.as_posix()}')")
        family_summary_frame = _family_summary_frame(
            conn,
            top15_threshold=float(top15_threshold),
            bottom15_threshold=float(bottom15_threshold),
        )
        family_regime_frame = _family_regime_frame(conn)
        family_monthly_frame = _family_monthly_frame(conn)
    finally:
        conn.close()

    family_regime_rollup = (
        family_regime_frame.groupby("state_family_id", dropna=False)
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
    family_summary_frame = family_summary_frame.merge(family_monthly_frame, on="state_family_id", how="left").merge(
        family_regime_rollup, on="state_family_id", how="left"
    )
    family_summary_frame["family_classification"] = family_summary_frame.apply(
        _classify_family,
        axis=1,
        thresholds={
            "baseline_mean_path_value_score_v1": _safe_float(thresholds.get("baseline_mean_path_value_score_v1")),
            "baseline_median_path_value_score_v1": _safe_float(thresholds.get("baseline_median_path_value_score_v1")),
            "baseline_plus5_before_minus5_rate": _safe_float(thresholds.get("baseline_plus5_before_minus5_rate")),
            "baseline_minus5_before_plus5_rate": _safe_float(thresholds.get("baseline_minus5_before_plus5_rate")),
            "baseline_bottom15_rate": _safe_float(thresholds.get("baseline_bottom15_rate")),
            "baseline_top15_rate": _safe_float(thresholds.get("baseline_top15_rate")),
            "min_sample_count": int(thresholds.get("min_sample_count", 300)),
            "min_unique_symbol_count": int(thresholds.get("min_unique_symbol_count", 30)),
            "min_month_count": int(thresholds.get("min_month_count", 12)),
        },
    )
    family_summary_frame = family_summary_frame.rename(
        columns={
            "sample_count": "family_sample_count",
            "unique_symbol_count": "family_unique_symbol_count",
            "month_count": "family_month_count",
            "mean_forward_ret_5d": "family_mean_forward_ret_5d",
            "mean_forward_ret_10d": "family_mean_forward_ret_10d",
            "mean_forward_ret_20d": "family_mean_forward_ret_20d",
            "median_forward_ret_20d": "family_median_forward_ret_20d",
            "mean_mfe_20d": "family_mean_mfe_20d",
            "mean_mae_20d": "family_mean_mae_20d",
            "mean_path_value_score_v1": "family_mean_path_value_score_v1",
            "median_path_value_score_v1": "family_median_path_value_score_v1",
            "plus5_before_minus5_rate": "family_plus5_before_minus5_rate",
            "minus5_before_plus5_rate": "family_minus5_before_plus5_rate",
            "top15_rate": "family_top15_rate",
            "bottom15_rate": "family_bottom15_rate",
            "months_observed": "family_months_observed",
            "positive_month_rate": "family_positive_month_rate",
            "worst_month_mean_path_value": "family_worst_month_mean_path_value",
            "best_month_mean_path_value": "family_best_month_mean_path_value",
            "mean_monthly_path_value": "family_mean_monthly_path_value",
            "std_monthly_path_value": "family_std_monthly_path_value",
            "month_sample_count": "family_month_sample_count",
            "regime_count": "family_regime_count",
            "regime_consistency_score": "family_regime_consistency_score",
            "score_spread": "family_score_spread",
            "dominant_regime_context": "family_bad_pick_regime",
        }
    )
    family_summary_frame["stable_high_value_family"] = family_summary_frame["family_classification"].eq("stable_high_value_family")
    family_summary_frame["stable_bad_pick_family"] = family_summary_frame["family_classification"].eq("stable_bad_pick_family")
    family_summary_frame["regime_dependent_family"] = family_summary_frame["family_classification"].eq("regime_dependent_family")
    family_summary_frame["unstable_or_sparse_family"] = family_summary_frame["family_classification"].eq("unstable_or_sparse_family")
    family_summary_frame["neutral_family"] = family_summary_frame["family_classification"].eq("neutral_family")
    family_summary_frame["family_bad_pick_regime"] = family_summary_frame["family_bad_pick_regime"].astype(str)
    family_summary_frame["state_family_id"] = family_summary_frame["state_family_id"].astype(str)

    return {
        "manifest": manifest,
        "decision": decision,
        "family_summary_report": family_summary_report,
        "family_summary_frame": family_summary_frame,
        "classification": classification,
        "by_regime": by_regime,
        "monthly_stability": monthly_stability,
        "row_parquet": row_parquet,
        "thresholds": thresholds,
        "top15_threshold": float(top15_threshold),
        "bottom15_threshold": float(bottom15_threshold),
        "source_ma_manifest_path": Path(manifest["source_artifacts"]["run_manifest_json"]),
        "source_ma_session_path": Path(manifest["source_artifacts"]["run_manifest_json"]).parent,
        "source_ma_row_parquet": Path(manifest["source_artifacts"]["position_state_forward_path_rows_parquet"]),
        "source_ma_manifest": _load_json(Path(manifest["source_artifacts"]["run_manifest_json"])),
    }


def _context_monthly_sql(runtime_codes_filter_sql: str = "") -> str:
    return f"""
    WITH monthly_source AS (
        SELECT
            b.code::VARCHAR AS code,
            CAST(to_timestamp(b.month) AS DATE) AS month_start_date,
            CAST(date_trunc('month', CAST(to_timestamp(b.month) AS DATE)) + INTERVAL '1 month' - INTERVAL '1 day' AS DATE) AS month_end_date,
            b.o,
            b.h,
            b.l,
            b.c,
            b.v,
            m.ma7,
            m.ma20,
            m.ma60,
            ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY b.month) AS month_index,
            MAX(b.h) OVER (PARTITION BY b.code ORDER BY b.month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS high_12m,
            MIN(b.l) OVER (PARTITION BY b.code ORDER BY b.month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS low_12m,
            LAG(m.ma20, 3) OVER (PARTITION BY b.code ORDER BY b.month) AS ma20_lag3,
            LAG(m.ma60, 3) OVER (PARTITION BY b.code ORDER BY b.month) AS ma60_lag3
        FROM monthly_bars b
        LEFT JOIN monthly_ma m
          ON m.code = b.code AND m.month = b.month
        {runtime_codes_filter_sql}
    ),
    scored AS (
        SELECT
            *,
            CASE WHEN ma20_lag3 IS NOT NULL THEN (ma20 - ma20_lag3) / NULLIF(ABS(ma20_lag3), 0) END AS ma20_slope_pct,
            CASE WHEN ma60_lag3 IS NOT NULL THEN (ma60 - ma60_lag3) / NULLIF(ABS(ma60_lag3), 0) END AS ma60_slope_pct,
            CASE WHEN high_12m > low_12m THEN (c - low_12m) / NULLIF(high_12m - low_12m, 0) END AS range_pos_12m
        FROM monthly_source
    )
    SELECT
        code,
        month_end_date AS context_date,
        month_index,
        c AS close_value,
        ma7,
        ma20,
        ma60,
        ma20_slope_pct,
        ma60_slope_pct,
        range_pos_12m,
        CASE
            WHEN month_index < 60 OR ma20 IS NULL OR ma60 IS NULL THEN 'monthly_unknown'
            WHEN ma7 > ma20 AND ma20 > ma60 AND (c / NULLIF(ma20, 0) >= 1.08 OR range_pos_12m >= 0.85) THEN 'monthly_overextended'
            WHEN c < ma7 AND c < ma20 AND c > ma60 AND ma20_slope_pct >= 0 AND range_pos_12m <= 0.35 THEN 'monthly_bottoming'
            WHEN c > ma20 AND ma20 > ma60 AND ma20_slope_pct >= 0.01 AND ma60_slope_pct >= 0.01 THEN 'monthly_uptrend'
            WHEN c < ma20 AND ma20 < ma60 AND ma20_slope_pct <= -0.01 AND ma60_slope_pct <= -0.01 THEN 'monthly_downtrend'
            WHEN ABS(ma20 - ma60) / NULLIF(ABS(c), 0) <= 0.06 OR (ABS(ma20_slope_pct) <= 0.01 AND ABS(ma60_slope_pct) <= 0.01) THEN 'monthly_range'
            ELSE 'monthly_range'
        END AS monthly_context,
        'confirmed_monthly_bars_monthly_ma' AS monthly_context_source,
        TRUE AS monthly_context_no_lookahead
    FROM scored
    """


def _context_weekly_sql(runtime_codes_filter_sql: str = "") -> str:
    return f"""
    WITH daily_source AS (
        SELECT
            b.code::VARCHAR AS code,
            CAST(to_timestamp(b.date) AS DATE) AS dt,
            b.o,
            b.h,
            b.l,
            b.c,
            b.v,
            date_trunc('week', CAST(to_timestamp(b.date) AS DATE)) AS week_start_date
        FROM daily_bars b
        {runtime_codes_filter_sql}
    ),
    weekly_bars AS (
        SELECT
            code,
            CAST(week_start_date + INTERVAL '4 days' AS DATE) AS week_end_date,
            arg_min(o, dt) AS o,
            MAX(h) AS h,
            MIN(l) AS l,
            arg_max(c, dt) AS c,
            SUM(v) AS v
        FROM daily_source
        GROUP BY 1, 2
    ),
    scored AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY code ORDER BY week_end_date) AS week_index,
            MAX(h) OVER (PARTITION BY code ORDER BY week_end_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS high_12w,
            MIN(l) OVER (PARTITION BY code ORDER BY week_end_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS low_12w,
            AVG(c) OVER (PARTITION BY code ORDER BY week_end_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7,
            AVG(c) OVER (PARTITION BY code ORDER BY week_end_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
            AVG(c) OVER (PARTITION BY code ORDER BY week_end_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60
        FROM weekly_bars
    ),
    scored_lagged AS (
        SELECT
            *,
            LAG(ma20, 3) OVER (PARTITION BY code ORDER BY week_end_date) AS ma20_lag3,
            LAG(ma60, 3) OVER (PARTITION BY code ORDER BY week_end_date) AS ma60_lag3
        FROM scored
    ),
    final AS (
        SELECT
            *,
            CASE WHEN ma20_lag3 IS NOT NULL THEN (ma20 - ma20_lag3) / NULLIF(ABS(ma20_lag3), 0) END AS ma20_slope_pct,
            CASE WHEN ma60_lag3 IS NOT NULL THEN (ma60 - ma60_lag3) / NULLIF(ABS(ma60_lag3), 0) END AS ma60_slope_pct,
            CASE WHEN high_12w > low_12w THEN (c - low_12w) / NULLIF(high_12w - low_12w, 0) END AS range_pos_12w
        FROM scored_lagged
    )
    SELECT
        code,
        week_end_date AS context_date,
        week_index,
        c AS close_value,
        ma7,
        ma20,
        ma60,
        ma20_slope_pct,
        ma60_slope_pct,
        range_pos_12w,
        CASE
            WHEN week_index < 60 OR ma20 IS NULL OR ma60 IS NULL THEN 'weekly_unknown'
            WHEN ma7 > ma20 AND ma20 > ma60 AND (c / NULLIF(ma20, 0) >= 1.06 OR range_pos_12w >= 0.85) THEN 'weekly_overextended'
            WHEN c < ma7 AND c < ma20 AND c > ma60 AND ma20_slope_pct >= 0.005 AND range_pos_12w <= 0.35 THEN 'weekly_pullback'
            WHEN c >= ma7 AND c < ma20 AND c > ma60 AND ma20_slope_pct >= 0.005 THEN 'weekly_rebound'
            WHEN c > ma20 AND ma20 > ma60 AND ma20_slope_pct >= 0.01 AND ma60_slope_pct >= 0.01 THEN 'weekly_uptrend'
            WHEN c < ma20 AND ma20 < ma60 AND ma20_slope_pct <= -0.01 AND ma60_slope_pct <= -0.01 THEN 'weekly_downtrend'
            WHEN ABS(ma20 - ma60) / NULLIF(ABS(c), 0) <= 0.06 OR (ABS(ma20_slope_pct) <= 0.01 AND ABS(ma60_slope_pct) <= 0.01) THEN 'weekly_range'
            ELSE 'weekly_range'
        END AS weekly_context,
        'provisional_weekly_from_daily_bars_daily_ma' AS weekly_context_source,
        TRUE AS weekly_context_no_lookahead
    FROM final
    """


def _build_group_summary_sql(group_cols: list[str], view_name: str = "conditional_rows") -> str:
    group_expr = ", ".join(group_cols)
    month_group_expr = f"{group_expr}, trade_month"
    regime_group_expr = f"{group_expr}, family_regime_context"
    return f"""
    WITH base AS (
        SELECT
            {group_expr},
            code,
            trade_month,
            family_regime_context,
            forward_ret_5d,
            forward_ret_10d,
            forward_ret_20d,
            path_value_score_v1,
            mfe_20d,
            mae_20d,
            hit_plus_5_before_minus_5,
            hit_minus_5_before_plus_5,
            top15_label,
            bottom15_label
        FROM {view_name}
    ),
    monthly AS (
        SELECT
            {month_group_expr},
            AVG(path_value_score_v1) AS month_mean_path_value
        FROM base
        GROUP BY {month_group_expr}
    ),
    regime AS (
        SELECT
            {regime_group_expr},
            COUNT(*) AS regime_sample_count,
            AVG(path_value_score_v1) AS regime_mean_path_value
        FROM base
        GROUP BY {regime_group_expr}
    ),
    agg AS (
        SELECT
            {group_expr},
            COUNT(*) AS sample_count,
            COUNT(DISTINCT code) AS unique_symbol_count,
            COUNT(DISTINCT trade_month) AS month_count,
            AVG(forward_ret_5d) AS mean_forward_ret_5d,
            AVG(forward_ret_10d) AS mean_forward_ret_10d,
            AVG(forward_ret_20d) AS mean_forward_ret_20d,
            quantile_cont(forward_ret_20d, 0.5) AS median_forward_ret_20d,
            AVG(path_value_score_v1) AS mean_path_value_score_v1,
            quantile_cont(path_value_score_v1, 0.5) AS median_path_value_score_v1,
            AVG(mfe_20d) AS mean_mfe_20d,
            AVG(mae_20d) AS mean_mae_20d,
            AVG(CASE WHEN hit_plus_5_before_minus_5 THEN 1 ELSE 0 END) AS plus5_before_minus5_rate,
            AVG(CASE WHEN hit_minus_5_before_plus_5 THEN 1 ELSE 0 END) AS minus5_before_plus5_rate,
            AVG(CASE WHEN top15_label THEN 1 ELSE 0 END) AS top15_rate,
            AVG(CASE WHEN bottom15_label THEN 1 ELSE 0 END) AS bottom15_rate
        FROM base
        GROUP BY {group_expr}
    ),
    month_summary AS (
        SELECT
            {group_expr},
            COUNT(*) AS months_observed,
            AVG(CASE WHEN month_mean_path_value > 0 THEN 1 ELSE 0 END) AS positive_month_rate,
            MIN(month_mean_path_value) AS worst_month_mean_path_value,
            MAX(month_mean_path_value) AS best_month_mean_path_value,
            AVG(month_mean_path_value) AS mean_monthly_path_value,
            STDDEV_SAMP(month_mean_path_value) AS std_monthly_path_value
        FROM monthly
        GROUP BY {group_expr}
    ),
    regime_summary AS (
        SELECT
            {group_expr},
            COUNT(*) AS regime_count,
            MAX(regime_sample_count)::DOUBLE / SUM(regime_sample_count) AS regime_consistency_score,
            MAX(regime_mean_path_value) - MIN(regime_mean_path_value) AS score_spread,
            MAX_BY(family_regime_context, regime_sample_count) AS dominant_regime_context
        FROM regime
        GROUP BY {group_expr}
    )
    SELECT
        agg.*,
        month_summary.months_observed,
        month_summary.positive_month_rate,
        month_summary.worst_month_mean_path_value,
        month_summary.best_month_mean_path_value,
        month_summary.mean_monthly_path_value,
        month_summary.std_monthly_path_value,
        regime_summary.regime_count,
        regime_summary.regime_consistency_score,
        regime_summary.score_spread,
        regime_summary.dominant_regime_context
    FROM agg
    LEFT JOIN month_summary USING ({group_expr})
    LEFT JOIN regime_summary USING ({group_expr})
    """


def _classify_conditional_group(row: pd.Series, *, thresholds: dict[str, Any]) -> str:
    sample_count = _safe_int(row.get("sample_count"), 0) or 0
    unique_symbol_count = _safe_int(row.get("unique_symbol_count"), 0) or 0
    month_count = _safe_int(row.get("month_count"), 0) or 0
    if (
        sample_count < int(thresholds["min_sample_count"])
        or unique_symbol_count < int(thresholds["min_unique_symbol_count"])
        or month_count < int(thresholds["min_month_count"])
    ):
        return "sparse_or_unstable"
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
        and bottom15 <= thresholds["baseline_bottom15_rate"]
        and positive_month >= 0.55
    ):
        return "conditional_high_value"
    if (
        mean_score is not None
        and median_score is not None
        and minus5 is not None
        and bottom15 is not None
        and positive_month is not None
        and mean_score < thresholds["baseline_mean_path_value_score_v1"]
        and median_score < 0
        and minus5 > thresholds["baseline_minus5_before_plus5_rate"]
        and bottom15 >= thresholds["baseline_bottom15_rate"]
        and positive_month <= 0.45
    ):
        return "conditional_bad_pick"
    if regime_count >= 2 and regime_consistency is not None and regime_consistency < 0.7 and score_spread is not None and score_spread >= 0.02:
        return "conditional_regime_dependent"
    return "neutral"


def _top_examples(frame: pd.DataFrame, *, class_name: str, limit: int = TOP_EXAMPLE_LIMIT) -> list[dict[str, Any]]:
    subset = frame.loc[frame["conditional_classification"] == class_name].copy()
    if class_name == "conditional_high_value":
        subset = subset.sort_values(
            ["mean_path_value_score_v1", "sample_count", "positive_month_rate"],
            ascending=[False, False, False],
            kind="stable",
        )
    elif class_name == "conditional_bad_pick":
        subset = subset.sort_values(
            ["mean_path_value_score_v1", "sample_count", "mean_mae_20d"],
            ascending=[True, False, True],
            kind="stable",
        )
    elif class_name == "conditional_regime_dependent":
        subset = subset.sort_values(
            ["score_spread", "sample_count", "regime_consistency_score"],
            ascending=[False, False, True],
            kind="stable",
        )
    else:
        subset = subset.sort_values(["sample_count", "mean_path_value_score_v1"], ascending=[False, False], kind="stable")
    return subset.head(limit).to_dict(orient="records")


def _context_examples_from_summary(frame: pd.DataFrame, context_col: str, limit: int = 20) -> list[dict[str, Any]]:
    subset = frame.sort_values(
        ["conditional_classification", "sample_count", "mean_path_value_score_v1"],
        ascending=[True, False, False],
        kind="stable",
    )
    return subset.head(limit).to_dict(orient="records")


def _count_classes(frame: pd.DataFrame) -> dict[str, int]:
    counts = frame["conditional_classification"].value_counts(dropna=False).to_dict()
    return {
        "conditional_high_value": int(counts.get("conditional_high_value", 0)),
        "conditional_bad_pick": int(counts.get("conditional_bad_pick", 0)),
        "conditional_regime_dependent": int(counts.get("conditional_regime_dependent", 0)),
        "sparse_or_unstable": int(counts.get("sparse_or_unstable", 0)),
        "neutral": int(counts.get("neutral", 0)),
    }


def _build_global_vs_conditional_comparison(
    *,
    family_source: dict[str, Any],
    monthly_context_frame: pd.DataFrame,
    weekly_context_frame: pd.DataFrame,
    triple_frame: pd.DataFrame,
    monthly_daily_frame: pd.DataFrame,
    weekly_daily_frame: pd.DataFrame,
    monthly_weekly_frame: pd.DataFrame,
) -> dict[str, Any]:
    global_baseline = {
        "stable_high_value_family_count": int(family_source["decision"]["stable_high_value_family_count"]),
        "stable_bad_pick_family_count": int(family_source["decision"]["stable_bad_pick_family_count"]),
        "mean_path_value_score_v1": _safe_float(family_source["thresholds"]["baseline_mean_path_value_score_v1"]),
        "median_path_value_score_v1": _safe_float(family_source["thresholds"]["baseline_median_path_value_score_v1"]),
        "plus5_before_minus5_rate": _safe_float(family_source["thresholds"]["baseline_plus5_before_minus5_rate"]),
        "minus5_before_plus5_rate": _safe_float(family_source["thresholds"]["baseline_minus5_before_plus5_rate"]),
        "bottom15_rate": _safe_float(family_source["thresholds"]["baseline_bottom15_rate"]),
        "top15_rate": _safe_float(family_source["thresholds"]["baseline_top15_rate"]),
    }

    def _level_report(frame: pd.DataFrame, *, context_columns: tuple[str, ...]) -> dict[str, Any]:
        high = frame.loc[frame["conditional_classification"] == "conditional_high_value"]
        bad = frame.loc[frame["conditional_classification"] == "conditional_bad_pick"]
        useful_context_counts: dict[str, int] = {}
        for context_col in context_columns:
            if context_col in frame.columns:
                useful_context_counts[f"useful_{context_col}_count"] = int(
                    frame.loc[frame["conditional_classification"].isin(["conditional_high_value", "conditional_bad_pick"]), context_col].nunique()
                )
        useful_combo_count = int(len(high) + len(bad))
        report = {
            "total_groups": int(len(frame)),
            "sample_safe_group_count": int((frame["conditional_classification"] != "sparse_or_unstable").sum()),
            "conditional_high_value_count": int(len(high)),
            "conditional_bad_pick_count": int(len(bad)),
            "conditional_regime_dependent_count": int((frame["conditional_classification"] == "conditional_regime_dependent").sum()),
            "sparse_or_unstable_count": int((frame["conditional_classification"] == "sparse_or_unstable").sum()),
            "neutral_count": int((frame["conditional_classification"] == "neutral").sum()),
            "high_value_mean_path_lift_vs_global": None
            if high.empty
            else float(high["mean_path_value_score_v1"].mean() - global_baseline["mean_path_value_score_v1"]),
            "high_value_bottom15_delta_vs_global": None
            if high.empty
            else float(high["bottom15_rate"].mean() - global_baseline["bottom15_rate"]),
            "bad_pick_mean_path_delta_vs_global": None
            if bad.empty
            else float(bad["mean_path_value_score_v1"].mean() - global_baseline["mean_path_value_score_v1"]),
            "bad_pick_bottom15_delta_vs_global": None
            if bad.empty
            else float(bad["bottom15_rate"].mean() - global_baseline["bottom15_rate"]),
            "combo_count_using_class_labels": useful_combo_count,
            "high_value_examples": _top_examples(frame, class_name="conditional_high_value"),
            "bad_pick_examples": _top_examples(frame, class_name="conditional_bad_pick"),
        }
        report.update(useful_context_counts)
        return report

    return {
        "schema_version": GLOBAL_COMPARISON_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "global_baseline": global_baseline,
        "monthly_context_level": _level_report(monthly_context_frame, context_columns=("monthly_context",)),
        "weekly_context_level": _level_report(weekly_context_frame, context_columns=("weekly_context",)),
        "triple_level": _level_report(triple_frame, context_columns=("monthly_context", "weekly_context")),
        "monthly_daily_level": _level_report(monthly_daily_frame, context_columns=("monthly_context",)),
        "weekly_daily_level": _level_report(weekly_daily_frame, context_columns=("weekly_context",)),
        "monthly_weekly_level": _level_report(monthly_weekly_frame, context_columns=("monthly_context", "weekly_context")),
    }


def _build_family_filter_decision_snapshot(family_source: dict[str, Any]) -> dict[str, Any]:
    return {
        "stable_high_value_family_count": int(family_source["decision"]["stable_high_value_family_count"]),
        "stable_bad_pick_family_count": int(family_source["decision"]["stable_bad_pick_family_count"]),
        "regime_dependent_family_count": int(family_source["decision"]["regime_dependent_family_count"]),
        "unstable_or_sparse_family_count": int(family_source["decision"]["unstable_or_sparse_family_count"]),
        "neutral_family_count": int(family_source["decision"].get("neutral_family_count", 0)),
        "total_families": int(family_source["decision"]["total_families"]),
        "baseline_metrics": {
            "mean_path_value_score_v1": _safe_float(family_source["thresholds"]["baseline_mean_path_value_score_v1"]),
            "median_path_value_score_v1": _safe_float(family_source["thresholds"]["baseline_median_path_value_score_v1"]),
            "plus5_before_minus5_rate": _safe_float(family_source["thresholds"]["baseline_plus5_before_minus5_rate"]),
            "minus5_before_plus5_rate": _safe_float(family_source["thresholds"]["baseline_minus5_before_plus5_rate"]),
            "bottom15_rate": _safe_float(family_source["thresholds"]["baseline_bottom15_rate"]),
            "top15_rate": _safe_float(family_source["thresholds"]["baseline_top15_rate"]),
        },
    }


def run_multi_timeframe_conditional_state_value_v1(
    *,
    source_family_session: str | Path | None = None,
    runtime_db_path: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_codes: int | None = DEFAULT_LIMIT_CODES,
) -> dict[str, Any]:
    run_started = time.perf_counter()
    source_family_session_path = _resolve_source_family_session(source_family_session)
    runtime_db_path_resolved = _resolve_runtime_db_path(runtime_db_path)
    output_root_path = _resolve_output_root(output_root)
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_tmp = output_root_path / f"{session_id}.tmp"
    session_final = output_root_path / session_id
    session_tmp.mkdir(parents=True, exist_ok=False)

    _progress_log(f"start source_family={source_family_session_path} runtime_db={runtime_db_path_resolved} session={session_id}")

    source_family = _load_source_family_session(source_family_session_path)
    family_summary_frame = source_family["family_summary_frame"].copy()
    family_summary_frame["state_family_id"] = family_summary_frame["state_family_id"].astype(str)
    family_summary_frame["family_bad_pick_regime"] = family_summary_frame["family_bad_pick_regime"].astype(str)
    family_summary_frame["stable_high_value_family"] = family_summary_frame["stable_high_value_family"].fillna(False).astype(bool)
    family_summary_frame["family_classification"] = family_summary_frame["family_classification"].fillna("neutral_family").astype(str)

    conn = duckdb.connect(str(runtime_db_path_resolved), read_only=True)
    try:
        conn.register("family_summary_frame", family_summary_frame)
        family_row_parquet = source_family["row_parquet"]
        conn.execute(f"CREATE TEMP VIEW family_rows_raw AS SELECT * FROM read_parquet('{family_row_parquet.as_posix()}')")
        if limit_codes is not None:
            limit = max(0, int(limit_codes))
            conn.execute(
                f"""
                CREATE TEMP VIEW selected_codes AS
                SELECT code
                FROM (
                    SELECT DISTINCT code
                    FROM family_rows_raw
                    ORDER BY code
                    LIMIT {limit}
                )
                """
            )
            code_filter_sql = "WHERE b.code IN (SELECT code FROM selected_codes)"
            family_rows_filter_sql = "WHERE code IN (SELECT code FROM selected_codes)"
        else:
            code_filter_sql = ""
            family_rows_filter_sql = ""

        conn.execute(
            f"""
            CREATE TEMP VIEW family_rows AS
            SELECT
                r.code::VARCHAR AS code,
                CAST(strptime(CAST(r.trade_date AS VARCHAR), '%Y%m%d') AS DATE) AS trade_dt,
                r.trade_date::INTEGER AS trade_date,
                CAST(strftime(CAST(strptime(CAST(r.trade_date AS VARCHAR), '%Y%m%d') AS DATE), '%Y%m') AS INTEGER) AS trade_month,
                r.state_family_id,
                r.position_state_id,
                r.family_regime_context,
                r.entry_next_open,
                r.entry_day_close,
                r.forward_window_days,
                r.forward_ret_5d,
                r.forward_ret_10d,
                r.forward_ret_20d,
                r.mfe_20d,
                r.mae_20d,
                r.days_to_mfe_20d,
                r.days_to_mae_20d,
                r.days_to_positive_close,
                r.days_to_plus_3pct,
                r.days_to_plus_5pct,
                r.days_to_minus_3pct,
                r.days_to_minus_5pct,
                r.hit_plus_5_before_minus_5,
                r.hit_minus_5_before_plus_5,
                r.hit_plus_3_before_minus_3,
                r.hit_minus_3_before_plus_3,
                r.hit_plus_1atr_before_minus_1atr,
                r.mfe_atr_20d,
                r.mae_atr_20d,
                r.close_above_entry_days_20d,
                r.close_below_entry_days_20d,
                r.path_value_score_v1
            FROM family_rows_raw r
            {family_rows_filter_sql}
            """
        )

        conn.execute(
            f"""
            CREATE TEMP VIEW monthly_context_rows AS
            {_context_monthly_sql(code_filter_sql)}
            """
        )
        conn.execute(
            f"""
            CREATE TEMP VIEW weekly_context_rows AS
            {_context_weekly_sql(code_filter_sql)}
            """
        )

        top15_threshold = float(source_family["thresholds"]["top15_score_threshold"])
        bottom15_threshold = float(source_family["thresholds"]["bottom15_score_threshold"])

        row_parquet_path = session_tmp / "conditional_state_rows.parquet"
        row_sql = f"""
        WITH base AS (
            SELECT
                f.*,
                fs.family_classification,
                fs.stable_high_value_family,
                fs.stable_bad_pick_family,
                fs.regime_dependent_family,
                fs.unstable_or_sparse_family,
                fs.neutral_family,
                fs.family_sample_count,
                fs.family_unique_symbol_count,
                fs.family_month_count,
                fs.family_mean_forward_ret_5d,
                fs.family_mean_forward_ret_10d,
                fs.family_mean_forward_ret_20d,
                fs.family_median_forward_ret_20d,
                fs.family_mean_mfe_20d,
                fs.family_mean_mae_20d,
                fs.family_mean_path_value_score_v1,
                fs.family_median_path_value_score_v1,
                fs.family_plus5_before_minus5_rate,
                fs.family_minus5_before_plus5_rate,
                fs.family_top15_rate,
                fs.family_bottom15_rate,
                fs.family_months_observed,
                fs.family_positive_month_rate,
                fs.family_worst_month_mean_path_value,
                fs.family_best_month_mean_path_value,
                fs.family_mean_monthly_path_value,
                fs.family_std_monthly_path_value,
                fs.family_month_sample_count,
                fs.family_regime_count,
                fs.family_regime_consistency_score,
                fs.family_score_spread,
                fs.family_bad_pick_regime,
                fs.family_bad_pick_regime AS dominant_regime_context
            FROM family_rows f
            LEFT JOIN family_summary_frame fs
              ON fs.state_family_id = f.state_family_id
        ),
        joined AS (
            SELECT
                base.code,
                base.trade_date,
                base.trade_dt,
                base.trade_month,
                base.state_family_id,
                base.position_state_id,
                base.family_classification,
                base.stable_high_value_family,
                base.stable_bad_pick_family,
                base.regime_dependent_family,
                base.unstable_or_sparse_family,
                base.neutral_family,
                base.family_regime_context,
                base.family_bad_pick_regime,
                base.dominant_regime_context,
                base.family_sample_count,
                base.family_unique_symbol_count,
                base.family_month_count,
                base.family_mean_forward_ret_5d,
                base.family_mean_forward_ret_10d,
                base.family_mean_forward_ret_20d,
                base.family_median_forward_ret_20d,
                base.family_mean_mfe_20d,
                base.family_mean_mae_20d,
                base.family_mean_path_value_score_v1,
                base.family_median_path_value_score_v1,
                base.family_plus5_before_minus5_rate,
                base.family_minus5_before_plus5_rate,
                base.family_top15_rate,
                base.family_bottom15_rate,
                base.family_months_observed,
                base.family_positive_month_rate,
                base.family_worst_month_mean_path_value,
                base.family_best_month_mean_path_value,
                base.family_mean_monthly_path_value,
                base.family_std_monthly_path_value,
                base.family_month_sample_count,
                base.family_regime_count,
                base.family_regime_consistency_score,
                base.family_score_spread,
                base.entry_next_open,
                base.entry_day_close,
                base.forward_window_days,
                base.forward_ret_5d,
                base.forward_ret_10d,
                base.forward_ret_20d,
                base.mfe_20d,
                base.mae_20d,
                base.days_to_mfe_20d,
                base.days_to_mae_20d,
                base.days_to_positive_close,
                base.days_to_plus_3pct,
                base.days_to_plus_5pct,
                base.days_to_minus_3pct,
                base.days_to_minus_5pct,
                base.hit_plus_5_before_minus_5,
                base.hit_minus_5_before_plus_5,
                base.hit_plus_3_before_minus_3,
                base.hit_minus_3_before_plus_3,
                base.hit_plus_1atr_before_minus_1atr,
                base.mfe_atr_20d,
                base.mae_atr_20d,
                base.close_above_entry_days_20d,
                base.close_below_entry_days_20d,
                base.path_value_score_v1,
                COALESCE(m.monthly_context, 'monthly_unknown') AS monthly_context,
                COALESCE(m.context_date, CAST(date_trunc('month', base.trade_dt) - INTERVAL '1 day' AS DATE)) AS monthly_context_date,
                COALESCE(m.monthly_context_source, 'confirmed_monthly_bars_monthly_ma') AS monthly_context_source,
                COALESCE(m.monthly_context_no_lookahead, TRUE) AS monthly_context_no_lookahead,
                COALESCE(w.weekly_context, 'weekly_unknown') AS weekly_context,
                COALESCE(w.context_date, CAST(date_trunc('week', base.trade_dt) - INTERVAL '3 days' AS DATE)) AS weekly_context_date,
                COALESCE(w.weekly_context_source, 'provisional_weekly_from_daily_bars_daily_ma') AS weekly_context_source,
                COALESCE(w.weekly_context_no_lookahead, TRUE) AS weekly_context_no_lookahead,
                CASE WHEN base.path_value_score_v1 >= {top15_threshold} THEN TRUE ELSE FALSE END AS top15_label,
                CASE WHEN base.path_value_score_v1 <= {bottom15_threshold} THEN TRUE ELSE FALSE END AS bottom15_label
            FROM base
            LEFT JOIN monthly_context_rows m
              ON m.code = base.code
             AND m.context_date = CAST(date_trunc('month', base.trade_dt) - INTERVAL '1 day' AS DATE)
            LEFT JOIN weekly_context_rows w
              ON w.code = base.code
             AND w.context_date = CAST(date_trunc('week', base.trade_dt) - INTERVAL '3 days' AS DATE)
        )
        SELECT * FROM joined
        ORDER BY code, trade_dt, state_family_id
        """
        conn.execute(
            f"COPY ({row_sql}) TO '{row_parquet_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        conn.execute(f"CREATE TEMP VIEW conditional_rows AS SELECT * FROM read_parquet('{row_parquet_path.as_posix()}')")
        row_validation = conn.execute("SELECT COUNT(*) AS row_count, COUNT(DISTINCT code) AS code_count FROM conditional_rows").fetchdf()
        if row_validation.empty:
            raise RuntimeError("conditional rows parquet validation failed")
        row_count = int(row_validation.loc[0, "row_count"])
        code_count = int(row_validation.loc[0, "code_count"])

        contexts = conn.execute(
            """
            SELECT
                COUNT(DISTINCT monthly_context) AS monthly_context_count,
                COUNT(DISTINCT weekly_context) AS weekly_context_count,
                AVG(CASE WHEN monthly_context_no_lookahead THEN 1 ELSE 0 END) AS monthly_no_lookahead_rate,
                AVG(CASE WHEN weekly_context_no_lookahead THEN 1 ELSE 0 END) AS weekly_no_lookahead_rate
            FROM conditional_rows
            """
        ).fetchdf()

        monthly_context_frame = conn.execute(_build_group_summary_sql(["monthly_context"])).fetchdf()
        weekly_context_frame = conn.execute(_build_group_summary_sql(["weekly_context"])).fetchdf()
        triple_query = _build_group_summary_sql(["monthly_context", "weekly_context", "state_family_id"])
        monthly_daily_query = _build_group_summary_sql(["monthly_context", "state_family_id"])
        weekly_daily_query = _build_group_summary_sql(["weekly_context", "state_family_id"])
        monthly_weekly_query = _build_group_summary_sql(["monthly_context", "weekly_context"])

        monthly_context_frame = monthly_context_frame if not monthly_context_frame.empty else pd.DataFrame(columns=["monthly_context"])
        weekly_context_frame = weekly_context_frame if not weekly_context_frame.empty else pd.DataFrame(columns=["weekly_context"])
        triple_frame = conn.execute(triple_query).fetchdf()
        monthly_daily_frame = conn.execute(monthly_daily_query).fetchdf()
        weekly_daily_frame = conn.execute(weekly_daily_query).fetchdf()
        monthly_weekly_frame = conn.execute(monthly_weekly_query).fetchdf()

    finally:
        conn.close()

    thresholds = {
        "baseline_mean_path_value_score_v1": _safe_float(source_family["thresholds"]["baseline_mean_path_value_score_v1"]),
        "baseline_median_path_value_score_v1": _safe_float(source_family["thresholds"]["baseline_median_path_value_score_v1"]),
        "baseline_plus5_before_minus5_rate": _safe_float(source_family["thresholds"]["baseline_plus5_before_minus5_rate"]),
        "baseline_minus5_before_plus5_rate": _safe_float(source_family["thresholds"]["baseline_minus5_before_plus5_rate"]),
        "baseline_bottom15_rate": _safe_float(source_family["thresholds"]["baseline_bottom15_rate"]),
        "baseline_top15_rate": _safe_float(source_family["thresholds"]["baseline_top15_rate"]),
        "min_sample_count": MIN_GROUP_SAMPLE_COUNT,
        "min_unique_symbol_count": MIN_GROUP_UNIQUE_SYMBOL_COUNT,
        "min_month_count": MIN_GROUP_MONTH_COUNT,
    }

    for frame in (triple_frame, monthly_daily_frame, weekly_daily_frame, monthly_weekly_frame):
        frame["conditional_classification"] = frame.apply(_classify_conditional_group, axis=1, thresholds=thresholds)
        frame["sample_safe"] = (
            frame["sample_count"].fillna(0).astype(int) >= MIN_GROUP_SAMPLE_COUNT
        ) & (
            frame["unique_symbol_count"].fillna(0).astype(int) >= MIN_GROUP_UNIQUE_SYMBOL_COUNT
        ) & (
            frame["month_count"].fillna(0).astype(int) >= MIN_GROUP_MONTH_COUNT
        )
    for frame in (monthly_context_frame, weekly_context_frame):
        if not frame.empty:
            frame["conditional_classification"] = frame.apply(_classify_conditional_group, axis=1, thresholds=thresholds)
            frame["sample_safe"] = (
                frame["sample_count"].fillna(0).astype(int) >= MIN_GROUP_SAMPLE_COUNT
            ) & (
                frame["unique_symbol_count"].fillna(0).astype(int) >= MIN_GROUP_UNIQUE_SYMBOL_COUNT
            ) & (
                frame["month_count"].fillna(0).astype(int) >= MIN_GROUP_MONTH_COUNT
            )

    triple_counts = _count_classes(triple_frame)
    monthly_counts = _count_classes(monthly_daily_frame)
    weekly_counts = _count_classes(weekly_daily_frame)
    monthly_weekly_counts = _count_classes(monthly_weekly_frame)

    source_family_decision = {
        "stable_high_value_family_count": int(source_family["decision"]["stable_high_value_family_count"]),
        "stable_bad_pick_family_count": int(source_family["decision"]["stable_bad_pick_family_count"]),
        "regime_dependent_family_count": int(source_family["decision"]["regime_dependent_family_count"]),
        "unstable_or_sparse_family_count": int(source_family["decision"]["unstable_or_sparse_family_count"]),
        "neutral_family_count": int(source_family["decision"].get("neutral_family_count", 0)),
        "total_families": int(source_family["decision"]["total_families"]),
    }

    triple_high = triple_frame.loc[triple_frame["conditional_classification"] == "conditional_high_value"]
    triple_bad = triple_frame.loc[triple_frame["conditional_classification"] == "conditional_bad_pick"]
    monthly_examples = monthly_context_frame.sort_values(["sample_count", "mean_path_value_score_v1"], ascending=[False, False]).head(TOP_EXAMPLE_LIMIT).to_dict(orient="records") if not monthly_context_frame.empty else []
    weekly_examples = weekly_context_frame.sort_values(["sample_count", "mean_path_value_score_v1"], ascending=[False, False]).head(TOP_EXAMPLE_LIMIT).to_dict(orient="records") if not weekly_context_frame.empty else []

    context_definition = {
        "schema_version": CONTEXT_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_family_session_id": source_family["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "source_runtime_db_path": str(runtime_db_path_resolved),
        "source_artifacts": {
            "family_filter_run_manifest_json": str(source_family_session_path / "run_manifest.json"),
            "family_filter_decision_json": str(source_family_session_path / "state_family_filter_v1_decision.json"),
            "family_filter_summary_json": str(source_family_session_path / "state_family_summary.json"),
            "family_filter_classification_json": str(source_family_session_path / "state_family_classification.json"),
            "family_filter_by_regime_json": str(source_family_session_path / "state_family_by_regime.json"),
            "family_filter_monthly_stability_json": str(source_family_session_path / "state_family_monthly_stability.json"),
            "family_filter_rows_parquet": str(source_family_session_path / "state_family_rows.parquet"),
            "source_ma_run_manifest_json": str(source_family["source_ma_manifest_path"]),
            "source_ma_row_parquet": str(source_family["source_ma_row_parquet"]),
        },
        "confirmed_vs_provisional_fields": {
            "confirmed_monthly_fields": [
                "monthly_bars",
                "monthly_ma",
            ],
            "provisional_weekly_fields": [
                "weekly_bars_derived_from_daily_bars",
                "weekly_ma_derived_from_daily_bars",
            ],
            "confirmed_daily_fields": [
                "daily_bars",
                "daily_ma",
            ],
            "missing_dedicated_weekly_table": True,
            "missing_monthly_context_label_table": True,
        },
        "monthly_context_definition": {
            "source": "confirmed monthly_bars + monthly_ma from runtime DB",
            "lookahead_rule": "use the previous completed month only; current incomplete month is excluded",
            "lookahead_free": True,
            "lookahead_anchor": "month_end_date = last calendar day of the completed month",
            "labels": [
                "monthly_uptrend",
                "monthly_range",
                "monthly_downtrend",
                "monthly_overextended",
                "monthly_bottoming",
                "monthly_unknown",
            ],
            "helpers": {
                "ma_stack": "ma7 > ma20 > ma60 => bullish_stack; ma7 < ma20 < ma60 => bearish_stack; else mixed_stack",
                "slope": "ma20 and ma60 percent change vs 3 completed months earlier",
                "range_position": "close within 12-month high/low range",
            },
            "thresholds": {
                "lookback_min_months": 60,
                "uptrend_ma20_slope_min": 0.01,
                "uptrend_ma60_slope_min": 0.01,
                "downtrend_ma20_slope_max": -0.01,
                "downtrend_ma60_slope_max": -0.01,
                "overextended_close_vs_ma20_min": 1.08,
                "overextended_range_pos_min": 0.85,
                "bottoming_range_pos_max": 0.35,
            },
        },
        "weekly_context_definition": {
            "source": "provisional weekly bars derived from daily_bars + daily_ma in runtime DB",
            "lookahead_rule": "use the previous completed Friday only; current incomplete week is excluded",
            "lookahead_free": True,
            "lookahead_anchor": "week_end_date = Friday of the previous completed week",
            "labels": [
                "weekly_uptrend",
                "weekly_pullback",
                "weekly_range",
                "weekly_downtrend",
                "weekly_rebound",
                "weekly_overextended",
                "weekly_unknown",
            ],
            "helpers": {
                "ma_stack": "ma7 > ma20 > ma60 => bullish_stack; ma7 < ma20 < ma60 => bearish_stack; else mixed_stack",
                "slope": "ma20 and ma60 percent change vs 3 completed weeks earlier",
                "range_position": "close within 12-week high/low range",
            },
            "thresholds": {
                "lookback_min_weeks": 60,
                "uptrend_ma20_slope_min": 0.01,
                "uptrend_ma60_slope_min": 0.01,
                "downtrend_ma20_slope_max": -0.01,
                "downtrend_ma60_slope_max": -0.01,
                "overextended_close_vs_ma20_min": 1.06,
                "overextended_range_pos_min": 0.85,
                "pullback_close_below_ma7": True,
                "rebound_close_above_ma7": True,
            },
        },
    }

    conditional_summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_family_session_id": source_family["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "source_runtime_db_path": str(runtime_db_path_resolved),
        "row_count": row_count,
        "code_count": code_count,
        "monthly_context_count": int(contexts.loc[0, "monthly_context_count"]),
        "weekly_context_count": int(contexts.loc[0, "weekly_context_count"]),
        "monthly_no_lookahead_rate": _safe_float(contexts.loc[0, "monthly_no_lookahead_rate"]),
        "weekly_no_lookahead_rate": _safe_float(contexts.loc[0, "weekly_no_lookahead_rate"]),
        "baseline_metrics": {
            "mean_path_value_score_v1": thresholds["baseline_mean_path_value_score_v1"],
            "median_path_value_score_v1": thresholds["baseline_median_path_value_score_v1"],
            "plus5_before_minus5_rate": thresholds["baseline_plus5_before_minus5_rate"],
            "minus5_before_plus5_rate": thresholds["baseline_minus5_before_plus5_rate"],
            "bottom15_rate": thresholds["baseline_bottom15_rate"],
            "top15_rate": thresholds["baseline_top15_rate"],
        },
        "global_family_counts": source_family_decision,
        "monthly_context_level": {
            "class_counts": _count_classes(monthly_context_frame) if not monthly_context_frame.empty else _count_classes(pd.DataFrame({"conditional_classification": []})),
            "sample_safe_group_count": int(monthly_context_frame["sample_safe"].sum()) if not monthly_context_frame.empty else 0,
            "high_value_examples": _top_examples(monthly_context_frame, class_name="conditional_high_value") if not monthly_context_frame.empty else [],
            "bad_pick_examples": _top_examples(monthly_context_frame, class_name="conditional_bad_pick") if not monthly_context_frame.empty else [],
        },
        "weekly_context_level": {
            "class_counts": _count_classes(weekly_context_frame) if not weekly_context_frame.empty else _count_classes(pd.DataFrame({"conditional_classification": []})),
            "sample_safe_group_count": int(weekly_context_frame["sample_safe"].sum()) if not weekly_context_frame.empty else 0,
            "high_value_examples": _top_examples(weekly_context_frame, class_name="conditional_high_value") if not weekly_context_frame.empty else [],
            "bad_pick_examples": _top_examples(weekly_context_frame, class_name="conditional_bad_pick") if not weekly_context_frame.empty else [],
        },
        "triple_level": {
            "class_counts": triple_counts,
            "sample_safe_group_count": int(triple_frame["sample_safe"].sum()),
            "high_value_examples": _top_examples(triple_frame, class_name="conditional_high_value"),
            "bad_pick_examples": _top_examples(triple_frame, class_name="conditional_bad_pick"),
            "regime_dependent_examples": _top_examples(triple_frame, class_name="conditional_regime_dependent"),
            "neutral_examples": _top_examples(triple_frame, class_name="neutral"),
        },
        "monthly_daily_level": {
            "class_counts": monthly_counts,
            "sample_safe_group_count": int(monthly_daily_frame["sample_safe"].sum()),
            "high_value_examples": _top_examples(monthly_daily_frame, class_name="conditional_high_value"),
            "bad_pick_examples": _top_examples(monthly_daily_frame, class_name="conditional_bad_pick"),
        },
        "weekly_daily_level": {
            "class_counts": weekly_counts,
            "sample_safe_group_count": int(weekly_daily_frame["sample_safe"].sum()),
            "high_value_examples": _top_examples(weekly_daily_frame, class_name="conditional_high_value"),
            "bad_pick_examples": _top_examples(weekly_daily_frame, class_name="conditional_bad_pick"),
        },
        "monthly_weekly_level": {
            "class_counts": monthly_weekly_counts,
            "sample_safe_group_count": int(monthly_weekly_frame["sample_safe"].sum()),
            "high_value_examples": _top_examples(monthly_weekly_frame, class_name="conditional_high_value"),
            "bad_pick_examples": _top_examples(monthly_weekly_frame, class_name="conditional_bad_pick"),
        },
        "monthly_context_level": {
            "class_counts": _count_classes(monthly_context_frame) if not monthly_context_frame.empty else _count_classes(pd.DataFrame({"conditional_classification": []})),
            "sample_safe_group_count": int(monthly_context_frame["sample_safe"].sum()) if not monthly_context_frame.empty else 0,
            "high_value_examples": _top_examples(monthly_context_frame, class_name="conditional_high_value") if not monthly_context_frame.empty else [],
            "bad_pick_examples": _top_examples(monthly_context_frame, class_name="conditional_bad_pick") if not monthly_context_frame.empty else [],
        },
        "weekly_context_level": {
            "class_counts": _count_classes(weekly_context_frame) if not weekly_context_frame.empty else _count_classes(pd.DataFrame({"conditional_classification": []})),
            "sample_safe_group_count": int(weekly_context_frame["sample_safe"].sum()) if not weekly_context_frame.empty else 0,
            "high_value_examples": _top_examples(weekly_context_frame, class_name="conditional_high_value") if not weekly_context_frame.empty else [],
            "bad_pick_examples": _top_examples(weekly_context_frame, class_name="conditional_bad_pick") if not weekly_context_frame.empty else [],
        },
        "monthly_context_examples": monthly_examples,
        "weekly_context_examples": weekly_examples,
    }

    by_monthly_payload = {
        "schema_version": MONTHLY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_family_session_id": source_family["manifest"]["session_id"],
        "monthly_contexts": []
        if monthly_context_frame.empty
        else monthly_context_frame.sort_values(["sample_count", "mean_path_value_score_v1"], ascending=[False, False], kind="stable").to_dict(orient="records"),
    }
    by_weekly_payload = {
        "schema_version": WEEKLY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_family_session_id": source_family["manifest"]["session_id"],
        "weekly_contexts": []
        if weekly_context_frame.empty
        else weekly_context_frame.sort_values(["sample_count", "mean_path_value_score_v1"], ascending=[False, False], kind="stable").to_dict(orient="records"),
    }

    classification_payload = {
        "schema_version": CLASSIFICATION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_family_session_id": source_family["manifest"]["session_id"],
        "triple_level": {
            "class_counts": triple_counts,
            "sample_safe_group_count": int(triple_frame["sample_safe"].sum()),
            "top_examples": {
                "conditional_high_value": _top_examples(triple_frame, class_name="conditional_high_value"),
                "conditional_bad_pick": _top_examples(triple_frame, class_name="conditional_bad_pick"),
                "conditional_regime_dependent": _top_examples(triple_frame, class_name="conditional_regime_dependent"),
                "sparse_or_unstable": _top_examples(triple_frame, class_name="sparse_or_unstable"),
                "neutral": _top_examples(triple_frame, class_name="neutral"),
            },
        },
        "monthly_daily_level": {
            "class_counts": monthly_counts,
            "sample_safe_group_count": int(monthly_daily_frame["sample_safe"].sum()),
            "top_examples": {
                "conditional_high_value": _top_examples(monthly_daily_frame, class_name="conditional_high_value"),
                "conditional_bad_pick": _top_examples(monthly_daily_frame, class_name="conditional_bad_pick"),
                "conditional_regime_dependent": _top_examples(monthly_daily_frame, class_name="conditional_regime_dependent"),
                "sparse_or_unstable": _top_examples(monthly_daily_frame, class_name="sparse_or_unstable"),
                "neutral": _top_examples(monthly_daily_frame, class_name="neutral"),
            },
        },
        "weekly_daily_level": {
            "class_counts": weekly_counts,
            "sample_safe_group_count": int(weekly_daily_frame["sample_safe"].sum()),
            "top_examples": {
                "conditional_high_value": _top_examples(weekly_daily_frame, class_name="conditional_high_value"),
                "conditional_bad_pick": _top_examples(weekly_daily_frame, class_name="conditional_bad_pick"),
                "conditional_regime_dependent": _top_examples(weekly_daily_frame, class_name="conditional_regime_dependent"),
                "sparse_or_unstable": _top_examples(weekly_daily_frame, class_name="sparse_or_unstable"),
                "neutral": _top_examples(weekly_daily_frame, class_name="neutral"),
            },
        },
        "monthly_weekly_level": {
            "class_counts": monthly_weekly_counts,
            "sample_safe_group_count": int(monthly_weekly_frame["sample_safe"].sum()),
            "top_examples": {
                "conditional_high_value": _top_examples(monthly_weekly_frame, class_name="conditional_high_value"),
                "conditional_bad_pick": _top_examples(monthly_weekly_frame, class_name="conditional_bad_pick"),
                "conditional_regime_dependent": _top_examples(monthly_weekly_frame, class_name="conditional_regime_dependent"),
                "sparse_or_unstable": _top_examples(monthly_weekly_frame, class_name="sparse_or_unstable"),
                "neutral": _top_examples(monthly_weekly_frame, class_name="neutral"),
            },
        },
        "monthly_context_level": {
            "class_counts": _count_classes(monthly_context_frame) if not monthly_context_frame.empty else _count_classes(pd.DataFrame({"conditional_classification": []})),
            "sample_safe_group_count": int(monthly_context_frame["sample_safe"].sum()) if not monthly_context_frame.empty else 0,
            "top_examples": {
                "conditional_high_value": _top_examples(monthly_context_frame, class_name="conditional_high_value") if not monthly_context_frame.empty else [],
                "conditional_bad_pick": _top_examples(monthly_context_frame, class_name="conditional_bad_pick") if not monthly_context_frame.empty else [],
                "conditional_regime_dependent": _top_examples(monthly_context_frame, class_name="conditional_regime_dependent") if not monthly_context_frame.empty else [],
                "sparse_or_unstable": _top_examples(monthly_context_frame, class_name="sparse_or_unstable") if not monthly_context_frame.empty else [],
                "neutral": _top_examples(monthly_context_frame, class_name="neutral") if not monthly_context_frame.empty else [],
            },
        },
        "weekly_context_level": {
            "class_counts": _count_classes(weekly_context_frame) if not weekly_context_frame.empty else _count_classes(pd.DataFrame({"conditional_classification": []})),
            "sample_safe_group_count": int(weekly_context_frame["sample_safe"].sum()) if not weekly_context_frame.empty else 0,
            "top_examples": {
                "conditional_high_value": _top_examples(weekly_context_frame, class_name="conditional_high_value") if not weekly_context_frame.empty else [],
                "conditional_bad_pick": _top_examples(weekly_context_frame, class_name="conditional_bad_pick") if not weekly_context_frame.empty else [],
                "conditional_regime_dependent": _top_examples(weekly_context_frame, class_name="conditional_regime_dependent") if not weekly_context_frame.empty else [],
                "sparse_or_unstable": _top_examples(weekly_context_frame, class_name="sparse_or_unstable") if not weekly_context_frame.empty else [],
                "neutral": _top_examples(weekly_context_frame, class_name="neutral") if not weekly_context_frame.empty else [],
            },
        },
    }

    global_vs_conditional = _build_global_vs_conditional_comparison(
        family_source=source_family,
        monthly_context_frame=monthly_context_frame,
        weekly_context_frame=weekly_context_frame,
        triple_frame=triple_frame,
        monthly_daily_frame=monthly_daily_frame,
        weekly_daily_frame=weekly_daily_frame,
        monthly_weekly_frame=monthly_weekly_frame,
    )

    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_family_session_id": source_family["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "source_runtime_db_path": str(runtime_db_path_resolved),
        "source_artifacts": {
            "family_filter_run_manifest_json": str(source_family_session_path / "run_manifest.json"),
            "family_filter_decision_json": str(source_family_session_path / "state_family_filter_v1_decision.json"),
            "family_filter_classification_json": str(source_family_session_path / "state_family_classification.json"),
            "family_filter_rows_parquet": str(source_family_session_path / "state_family_rows.parquet"),
            "source_ma_run_manifest_json": str(source_family["source_ma_manifest_path"]),
            "source_ma_row_parquet": str(source_family["source_ma_row_parquet"]),
        },
        "baseline_metrics": conditional_summary["baseline_metrics"],
        "global_family_counts": source_family_decision,
        "monthly_context_level_class_counts": _count_classes(monthly_context_frame) if not monthly_context_frame.empty else _count_classes(pd.DataFrame({"conditional_classification": []})),
        "weekly_context_level_class_counts": _count_classes(weekly_context_frame) if not weekly_context_frame.empty else _count_classes(pd.DataFrame({"conditional_classification": []})),
        "triple_level_class_counts": triple_counts,
        "monthly_daily_level_class_counts": monthly_counts,
        "weekly_daily_level_class_counts": weekly_counts,
        "monthly_weekly_level_class_counts": monthly_weekly_counts,
        "global_vs_conditional_comparison": {
            "monthly_context_level": {
                "conditional_high_value_count": _count_classes(monthly_context_frame)["conditional_high_value"] if not monthly_context_frame.empty else 0,
                "conditional_bad_pick_count": _count_classes(monthly_context_frame)["conditional_bad_pick"] if not monthly_context_frame.empty else 0,
                "average_path_value_lift_vs_global": global_vs_conditional["monthly_context_level"]["high_value_mean_path_lift_vs_global"],
                "bottom15_delta_vs_global": global_vs_conditional["monthly_context_level"]["high_value_bottom15_delta_vs_global"],
            },
            "weekly_context_level": {
                "conditional_high_value_count": _count_classes(weekly_context_frame)["conditional_high_value"] if not weekly_context_frame.empty else 0,
                "conditional_bad_pick_count": _count_classes(weekly_context_frame)["conditional_bad_pick"] if not weekly_context_frame.empty else 0,
                "average_path_value_lift_vs_global": global_vs_conditional["weekly_context_level"]["high_value_mean_path_lift_vs_global"],
                "bottom15_delta_vs_global": global_vs_conditional["weekly_context_level"]["high_value_bottom15_delta_vs_global"],
            },
            "triple_level": {
                "conditional_high_value_count": triple_counts["conditional_high_value"],
                "conditional_bad_pick_count": triple_counts["conditional_bad_pick"],
                "average_path_value_lift_vs_global": global_vs_conditional["triple_level"]["high_value_mean_path_lift_vs_global"],
                "bottom15_delta_vs_global": global_vs_conditional["triple_level"]["high_value_bottom15_delta_vs_global"],
            },
            "monthly_daily_level": {
                "conditional_high_value_count": monthly_counts["conditional_high_value"],
                "conditional_bad_pick_count": monthly_counts["conditional_bad_pick"],
                "average_path_value_lift_vs_global": global_vs_conditional["monthly_daily_level"]["high_value_mean_path_lift_vs_global"],
                "bottom15_delta_vs_global": global_vs_conditional["monthly_daily_level"]["high_value_bottom15_delta_vs_global"],
            },
            "weekly_daily_level": {
                "conditional_high_value_count": weekly_counts["conditional_high_value"],
                "conditional_bad_pick_count": weekly_counts["conditional_bad_pick"],
                "average_path_value_lift_vs_global": global_vs_conditional["weekly_daily_level"]["high_value_mean_path_lift_vs_global"],
                "bottom15_delta_vs_global": global_vs_conditional["weekly_daily_level"]["high_value_bottom15_delta_vs_global"],
            },
            "monthly_weekly_level": {
                "conditional_high_value_count": monthly_weekly_counts["conditional_high_value"],
                "conditional_bad_pick_count": monthly_weekly_counts["conditional_bad_pick"],
                "average_path_value_lift_vs_global": global_vs_conditional["monthly_weekly_level"]["high_value_mean_path_lift_vs_global"],
                "bottom15_delta_vs_global": global_vs_conditional["monthly_weekly_level"]["high_value_bottom15_delta_vs_global"],
            },
        },
        "recommendation": "hold",
        "typed_reasons": [
            "conditioning_needed_but_signal_is_still_sparse_or_context_specific",
        ],
    }

    output_files = {
        "run_manifest_json": session_tmp / "run_manifest.json",
        "context_definition_json": session_tmp / "context_definition.json",
        "conditional_state_value_summary_json": session_tmp / "conditional_state_value_summary.json",
        "conditional_state_value_by_monthly_json": session_tmp / "conditional_state_value_by_monthly.json",
        "conditional_state_value_by_weekly_json": session_tmp / "conditional_state_value_by_weekly.json",
        "conditional_state_classification_json": session_tmp / "conditional_state_classification.json",
        "global_vs_conditional_comparison_json": session_tmp / "global_vs_conditional_comparison.json",
        "multi_timeframe_conditional_state_value_v1_decision_json": session_tmp / "multi_timeframe_conditional_state_value_v1_decision.json",
        "conditional_state_rows_parquet": row_parquet_path,
        "_artifact_complete_json": session_tmp / "_ARTIFACT_COMPLETE.json",
    }

    manifest_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "source_family_session_id": source_family["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "source_runtime_db_path": str(runtime_db_path_resolved),
        "output_root": str(output_root_path),
        "source_artifacts": {
            "family_filter_run_manifest_json": str(source_family_session_path / "run_manifest.json"),
            "family_filter_decision_json": str(source_family_session_path / "state_family_filter_v1_decision.json"),
            "family_filter_summary_json": str(source_family_session_path / "state_family_summary.json"),
            "family_filter_classification_json": str(source_family_session_path / "state_family_classification.json"),
            "family_filter_by_regime_json": str(source_family_session_path / "state_family_by_regime.json"),
            "family_filter_monthly_stability_json": str(source_family_session_path / "state_family_monthly_stability.json"),
            "family_filter_rows_parquet": str(source_family_session_path / "state_family_rows.parquet"),
            "source_ma_run_manifest_json": str(source_family["source_ma_manifest_path"]),
            "source_ma_row_parquet": str(source_family["source_ma_row_parquet"]),
        },
        "output_artifacts": {
            key: str(session_final / path.name) if key != "conditional_state_rows_parquet" else str(session_final / row_parquet_path.name)
            for key, path in output_files.items()
        },
        "limit_codes": limit_codes,
        "no_lookahead_inherited": True,
        "monthly_context_no_lookahead": True,
        "weekly_context_no_lookahead": True,
        "conditional_row_count": row_count,
        "conditional_code_count": code_count,
        "monthly_context_count": int(contexts.loc[0, "monthly_context_count"]),
        "weekly_context_count": int(contexts.loc[0, "weekly_context_count"]),
        "authoritative_source": "state_family_rows.parquet from the verified family filter session plus confirmed/provisional higher-timeframe context derivation",
    }

    _write_json(output_files["context_definition_json"], context_definition)
    _write_json(output_files["conditional_state_value_summary_json"], conditional_summary)
    _write_json(output_files["conditional_state_value_by_monthly_json"], by_monthly_payload)
    _write_json(output_files["conditional_state_value_by_weekly_json"], by_weekly_payload)
    _write_json(output_files["conditional_state_classification_json"], classification_payload)
    _write_json(output_files["global_vs_conditional_comparison_json"], global_vs_conditional)
    _write_json(output_files["multi_timeframe_conditional_state_value_v1_decision_json"], decision)
    _write_json(output_files["run_manifest_json"], manifest_payload)
    _write_json(output_files["_artifact_complete_json"], {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "validated": True,
    })

    # Light verification before finalizing the session directory.
    with duckdb.connect() as verify_con:
        verify_con.execute(f"SELECT COUNT(*) FROM read_parquet('{row_parquet_path.as_posix()}')").fetchone()
    for json_path in (
        output_files["context_definition_json"],
        output_files["conditional_state_value_summary_json"],
        output_files["conditional_state_value_by_monthly_json"],
        output_files["conditional_state_value_by_weekly_json"],
        output_files["conditional_state_classification_json"],
        output_files["global_vs_conditional_comparison_json"],
        output_files["multi_timeframe_conditional_state_value_v1_decision_json"],
        output_files["run_manifest_json"],
        output_files["_artifact_complete_json"],
    ):
        json.loads(json_path.read_text(encoding="utf-8"))

    session_final.mkdir(parents=True, exist_ok=False)
    for key, path in output_files.items():
        if key == "conditional_state_rows_parquet":
            continue
        final_path = session_final / path.name
        shutil.move(str(path), str(final_path))
    shutil.move(str(row_parquet_path), str(session_final / row_parquet_path.name))
    _progress_log(f"finalized session={session_id} elapsed={time.perf_counter() - run_started:.1f}s")

    return {
        "session_id": session_id,
        "source_family_session_id": source_family["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "source_runtime_db_path": str(runtime_db_path_resolved),
        "session_dir": str(session_final),
        "row_count": row_count,
        "code_count": code_count,
        "monthly_context_count": int(contexts.loc[0, "monthly_context_count"]),
        "weekly_context_count": int(contexts.loc[0, "weekly_context_count"]),
        "triple_level_counts": triple_counts,
        "monthly_daily_level_counts": monthly_counts,
        "weekly_daily_level_counts": weekly_counts,
        "monthly_weekly_level_counts": monthly_weekly_counts,
        "decision": decision,
        "global_vs_conditional_comparison": global_vs_conditional,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run TRADEX multi-timeframe conditional state value analysis.")
    parser.add_argument("--source-family-session", default=str(DEFAULT_SOURCE_FAMILY_SESSION))
    parser.add_argument("--runtime-db-path", default=str(DEFAULT_RUNTIME_DB_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--limit-codes", type=int, default=None)
    args = parser.parse_args(argv)

    run_multi_timeframe_conditional_state_value_v1(
        source_family_session=args.source_family_session,
        runtime_db_path=args.runtime_db_path,
        output_root=args.output_root,
        limit_codes=args.limit_codes,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
