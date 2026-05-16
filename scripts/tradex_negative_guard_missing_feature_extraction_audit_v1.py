from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_wide_pool_winner_nonwinner_feature_diagnosis_v1 as diagnosis_mod


AXIS_ID = "negative_guard_missing_feature_extraction_audit_v1"
SCHEMA_PREFIX = "tradex_negative_guard_missing_feature_extraction_audit_v1"
DEFAULT_PATTERN_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"
DEFAULT_GUARD_RUN_ID = "20260513T010000Z-pre-strength-guard-validation-v1"
DEFAULT_UPSIDE_RUN_ID = "20260513T020000Z-upside-capture-missed-winner-diagnosis-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"
DEFAULT_RISK_RUN_ID = "20260513T040000Z-selection-risk-control-for-wide-pool-v1"
DEFAULT_THRESHOLD_RUN_ID = "20260513T050000Z-threshold-no-trade-control-for-wide-pool-v1"
DEFAULT_FEATURE_DIAGNOSIS_RUN_ID = "20260513T060000Z-wide-pool-winner-nonwinner-feature-diagnosis-v1"
DEFAULT_PATTERN_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\pre_strength_guard_validation_v1")
DEFAULT_UPSIDE_ROOT = Path(r"G:\Tradex\upside_capture_missed_winner_diagnosis_v1")
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_RISK_ROOT = Path(r"G:\Tradex\selection_risk_control_for_wide_pool_v1")
DEFAULT_THRESHOLD_ROOT = Path(r"G:\Tradex\threshold_no_trade_control_for_wide_pool_v1")
DEFAULT_FEATURE_DIAGNOSIS_ROOT = Path(r"G:\Tradex\wide_pool_winner_nonwinner_feature_diagnosis_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\negative_guard_missing_feature_extraction_audit_v1")

TOP_K = 3
RANDOM_SEED = 20260513
BASE_SCORE_COLUMN = diagnosis_mod.BASE_SCORE_COLUMN
MAE_MATERIALLY_BAD_THRESHOLD = -0.07

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "extraction_feature_contract.json",
    "group_definition_contract.json",
    "feature_availability_audit.json",
    "leakage_audit.json",
    "extracted_negative_guard_feature_ledger.jsonl",
    "negative_guard_same_date_pair_ledger.jsonl",
    "negative_guard_score_bucket_pair_ledger.jsonl",
    "negative_guard_feature_contrast_report.json",
    "same_date_negative_guard_contrast_report.json",
    "score_bucket_negative_guard_contrast_report.json",
    "run_maturity_contrast_report.json",
    "previous_shortlist_retest_report.json",
    "time_block_stability.json",
    "candidate_feature_shortlist_v2.json",
    "rejected_feature_report.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

OHLCV_FEATURE_COLUMNS = {
    "close_vs_ma20_pct",
    "close_vs_ma60_pct",
    "close_vs_ma20_atr",
    "close_vs_ma60_atr",
    "max_close_vs_ma20_pct_pre20",
    "max_close_vs_ma60_pct_pre20",
    "ma20_extension_zscore_by_symbol",
    "ma60_extension_zscore_by_symbol",
    "consecutive_up_days_pre5",
    "consecutive_up_days_pre10",
    "close_above_ma20_days_pre20",
    "close_above_ma60_days_pre20",
    "days_since_ma20_reclaim",
    "days_since_20d_high_break",
    "days_since_60d_high_break",
    "prior_run_length",
    "ret5",
    "ret10",
    "ret20",
    "ret5_minus_ret20_slope",
    "close_position_in_20d_range",
    "close_position_in_60d_range",
    "upper_wick_ratio_event_day",
    "upper_wick_ratio_pre5_avg",
    "lower_wick_ratio_pre5_avg",
    "range_expansion_ratio_event_day",
    "atr20_pct",
    "atr20_zscore_by_symbol",
    "volatility_expansion_pre5_vs_pre20",
    "volume_vs_20d_avg",
    "volume_vs_60d_avg",
    "volume_zscore_by_symbol",
    "volume_expansion_pre5_vs_pre20",
    "up_day_volume_ratio_pre10",
    "down_day_volume_ratio_pre10",
    "volume_on_breakout_vs_base",
    "pre20_range_pct",
    "pre10_range_pct",
    "pre5_range_pct",
    "pre20_range_compression_vs_60",
    "ma20_slope_pre20",
    "ma60_slope_pre20",
    "ma20_slope_acceleration",
    "base_days_before_breakout",
    "pullback_depth_after_prior_high",
    "higher_low_count_pre20",
    "score_rank_within_date",
    "research_score_decile",
    "risk_estimate_bucket",
    "candidate_count_same_day",
}
EVALUATION_ONLY_REPORT_COLUMNS = {"same_day_median_ret20_evaluation_only_for_reporting"}
FUTURE_LABEL_COLUMNS = set(diagnosis_mod.FUTURE_LABEL_COLUMNS) | {
    "negative_guard_continuation_winner",
    "negative_guard_blowoff_loser",
    "selected_negative_guard_winner",
    "selected_negative_guard_severe_loser",
    "same_date_negative_guard_oracle_miss",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    return diagnosis_mod._json_ready(value)


def _json_text(payload: Any) -> str:
    return diagnosis_mod._json_text(payload)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return diagnosis_mod._write_json(path, payload)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    return diagnosis_mod._write_jsonl(path, rows)


def _load_json(path: Path) -> dict[str, Any]:
    return diagnosis_mod._load_json(path)


def _stable_hash(payload: Any) -> str:
    return diagnosis_mod._stable_hash(payload)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return diagnosis_mod._safe_path(value, default)


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return diagnosis_mod._run_dir(root, run_id, default_root)


def _safe_rate(count: int | float, total: int | float) -> float:
    return diagnosis_mod._safe_rate(count, total)


def _source_db_from_pattern(pattern_dir: Path) -> Path | None:
    path = pattern_dir / "evaluation_contract.json"
    if not path.exists():
        return None
    source_db = _load_json(path).get("source_db")
    if not source_db:
        return None
    candidate = Path(str(source_db)).expanduser()
    return candidate if candidate.exists() else None


def _date_norm_expr(column: str) -> str:
    return (
        f"CASE WHEN CAST({column} AS BIGINT) > 30000000 "
        f"THEN CAST(strftime(CAST(to_timestamp(CAST({column} AS BIGINT)) AS TIMESTAMP), '%Y%m%d') AS INTEGER) "
        f"ELSE CAST({column} AS INTEGER) END"
    )


def _load_daily_rows(source_db: Path, *, codes: list[str], start_ymd: int, end_ymd: int) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    with duckdb.connect(str(source_db), read_only=True) as conn:
        b_expr = _date_norm_expr("date")
        m_expr = _date_norm_expr("date")
        frame = conn.execute(
            f"""
            WITH b AS (
                SELECT code, {b_expr} AS ymd, o, h, l, c, v, source
                FROM daily_bars
            ),
            m AS (
                SELECT code, {m_expr} AS ymd, ma20, ma60
                FROM daily_ma
            )
            SELECT b.code, b.ymd, b.o, b.h, b.l, b.c, b.v, m.ma20, m.ma60
            FROM b
            LEFT JOIN m ON b.code = m.code AND b.ymd = m.ymd
            WHERE b.ymd BETWEEN ? AND ?
              AND b.code IN (SELECT * FROM UNNEST(?))
              AND lower(coalesce(b.source, '')) = 'pan'
              AND b.o > 0 AND b.h > 0 AND b.l > 0 AND b.c > 0
            ORDER BY b.code, b.ymd
            """,
            [int(start_ymd), int(end_ymd), codes],
        ).fetchdf()
    if frame.empty:
        return frame
    frame["code"] = frame["code"].astype(str)
    frame["date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    return frame


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    num = pd.to_numeric(numerator, errors="coerce")
    den = pd.to_numeric(denominator, errors="coerce").replace(0, pd.NA)
    return num / den


def _rolling_prior_sum(series: pd.Series, window: int) -> pd.Series:
    return series.shift(1).rolling(window, min_periods=1).sum()


def _days_since(mask: pd.Series) -> pd.Series:
    out = []
    last_seen: int | None = None
    for idx, value in enumerate(mask.fillna(False).astype(bool).tolist()):
        if value:
            last_seen = idx
            out.append(0)
        elif last_seen is None:
            out.append(None)
        else:
            out.append(idx - last_seen)
    return pd.Series(out, index=mask.index, dtype="float64")


def _consecutive_true(mask: pd.Series) -> pd.Series:
    values = []
    current = 0
    for value in mask.fillna(False).astype(bool).tolist():
        current = current + 1 if value else 0
        values.append(current)
    return pd.Series(values, index=mask.index, dtype="float64")


def _group_series(frame: pd.DataFrame, builder: Any) -> pd.Series:
    parts = [builder(group) for _, group in frame.groupby("code", sort=False)]
    if not parts:
        return pd.Series(dtype="float64")
    return pd.concat(parts).sort_index()


def build_ohlcv_feature_frame(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(columns=["code", "event_ymd"])
    frame = daily.sort_values(["code", "date"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False, group_keys=False)
    frame["ma20"] = frame["ma20"].fillna(grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean()))
    frame["ma60"] = frame["ma60"].fillna(grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean()))
    frame["prev_c"] = grouped["c"].shift(1)
    true_range = pd.concat(
        [
            frame["h"] - frame["l"],
            (frame["h"] - frame["prev_c"]).abs(),
            (frame["l"] - frame["prev_c"]).abs(),
        ],
        axis=1,
    ).max(axis=1)
    frame["tr"] = true_range
    frame["atr20"] = grouped["tr"].transform(lambda s: s.rolling(20, min_periods=10).mean())
    frame["close_vs_ma20_pct"] = _safe_div(frame["c"] - frame["ma20"], frame["ma20"])
    frame["close_vs_ma60_pct"] = _safe_div(frame["c"] - frame["ma60"], frame["ma60"])
    frame["close_vs_ma20_atr"] = _safe_div(frame["c"] - frame["ma20"], frame["atr20"])
    frame["close_vs_ma60_atr"] = _safe_div(frame["c"] - frame["ma60"], frame["atr20"])
    frame["max_close_vs_ma20_pct_pre20"] = grouped["close_vs_ma20_pct"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).max())
    frame["max_close_vs_ma60_pct_pre20"] = grouped["close_vs_ma60_pct"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).max())
    frame["ma20_extension_zscore_by_symbol"] = grouped["close_vs_ma20_pct"].transform(lambda s: (s - s.shift(1).rolling(252, min_periods=30).mean()) / s.shift(1).rolling(252, min_periods=30).std())
    frame["ma60_extension_zscore_by_symbol"] = grouped["close_vs_ma60_pct"].transform(lambda s: (s - s.shift(1).rolling(252, min_periods=30).mean()) / s.shift(1).rolling(252, min_periods=30).std())
    up_day = frame["c"] > frame["prev_c"]
    frame["consecutive_up_days_raw"] = _group_series(frame, lambda g: _consecutive_true(g["c"] > g["prev_c"]))
    frame["consecutive_up_days_pre5"] = frame["consecutive_up_days_raw"].clip(upper=5)
    frame["consecutive_up_days_pre10"] = frame["consecutive_up_days_raw"].clip(upper=10)
    frame["close_above_ma20_days_pre20"] = _group_series(frame, lambda g: _rolling_prior_sum(g["c"] > g["ma20"], 20))
    frame["close_above_ma60_days_pre20"] = _group_series(frame, lambda g: _rolling_prior_sum(g["c"] > g["ma60"], 20))
    reclaim = (frame["c"] > frame["ma20"]) & (frame["prev_c"] <= grouped["ma20"].shift(1))
    high20_prior = grouped["h"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).max())
    high60_prior = grouped["h"].transform(lambda s: s.shift(1).rolling(60, min_periods=10).max())
    frame["days_since_ma20_reclaim"] = _group_series(frame, lambda g: _days_since(reclaim.loc[g.index]))
    frame["days_since_20d_high_break"] = _group_series(frame, lambda g: _days_since(g["c"] > high20_prior.loc[g.index]))
    frame["days_since_60d_high_break"] = _group_series(frame, lambda g: _days_since(g["c"] > high60_prior.loc[g.index]))
    frame["prior_run_length"] = _group_series(frame, lambda g: _consecutive_true(g["c"] > g["ma20"]))
    frame["ret5"] = grouped["c"].transform(lambda s: s / s.shift(5) - 1.0)
    frame["ret10"] = grouped["c"].transform(lambda s: s / s.shift(10) - 1.0)
    frame["ret20"] = grouped["c"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["ret5_minus_ret20_slope"] = frame["ret5"] - frame["ret20"] / 4.0
    low20 = grouped["l"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).min())
    low60 = grouped["l"].transform(lambda s: s.shift(1).rolling(60, min_periods=10).min())
    frame["close_position_in_20d_range"] = _safe_div(frame["c"] - low20, high20_prior - low20)
    frame["close_position_in_60d_range"] = _safe_div(frame["c"] - low60, high60_prior - low60)
    candle_range = (frame["h"] - frame["l"]).replace(0, pd.NA)
    frame["upper_wick_ratio_event_day"] = _safe_div(frame["h"] - pd.concat([frame["o"], frame["c"]], axis=1).max(axis=1), candle_range)
    frame["lower_wick_ratio_event_day"] = _safe_div(pd.concat([frame["o"], frame["c"]], axis=1).min(axis=1) - frame["l"], candle_range)
    frame["upper_wick_ratio_pre5_avg"] = grouped["upper_wick_ratio_event_day"].transform(lambda s: s.shift(1).rolling(5, min_periods=2).mean())
    frame["lower_wick_ratio_pre5_avg"] = grouped["lower_wick_ratio_event_day"].transform(lambda s: s.shift(1).rolling(5, min_periods=2).mean())
    prior_range20 = grouped["tr"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    frame["range_expansion_ratio_event_day"] = _safe_div(frame["tr"], prior_range20)
    frame["atr20_pct"] = _safe_div(frame["atr20"], frame["c"])
    frame["atr20_zscore_by_symbol"] = grouped["atr20_pct"].transform(lambda s: (s - s.shift(1).rolling(252, min_periods=30).mean()) / s.shift(1).rolling(252, min_periods=30).std())
    frame["volatility_expansion_pre5_vs_pre20"] = _safe_div(grouped["tr"].transform(lambda s: s.shift(1).rolling(5, min_periods=2).mean()), prior_range20)
    vol20 = grouped["v"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    vol60 = grouped["v"].transform(lambda s: s.shift(1).rolling(60, min_periods=10).mean())
    frame["volume_vs_20d_avg"] = _safe_div(frame["v"], vol20)
    frame["volume_vs_60d_avg"] = _safe_div(frame["v"], vol60)
    frame["volume_zscore_by_symbol"] = grouped["v"].transform(lambda s: (s - s.shift(1).rolling(252, min_periods=30).mean()) / s.shift(1).rolling(252, min_periods=30).std())
    frame["volume_expansion_pre5_vs_pre20"] = _safe_div(grouped["v"].transform(lambda s: s.shift(1).rolling(5, min_periods=2).mean()), vol20)
    up_vol = frame["v"].where(up_day, 0)
    down_vol = frame["v"].where(~up_day, 0)
    frame["up_day_volume_ratio_pre10"] = _safe_div(_group_series(frame, lambda g: up_vol.loc[g.index].shift(1).rolling(10, min_periods=3).sum()), grouped["v"].transform(lambda s: s.shift(1).rolling(10, min_periods=3).sum()))
    frame["down_day_volume_ratio_pre10"] = _safe_div(_group_series(frame, lambda g: down_vol.loc[g.index].shift(1).rolling(10, min_periods=3).sum()), grouped["v"].transform(lambda s: s.shift(1).rolling(10, min_periods=3).sum()))
    frame["volume_on_breakout_vs_base"] = frame["volume_vs_20d_avg"].where(frame["c"] > high20_prior, 0.0)
    frame["pre20_range_pct"] = _safe_div(high20_prior - low20, frame["prev_c"])
    high10 = grouped["h"].transform(lambda s: s.shift(1).rolling(10, min_periods=3).max())
    low10 = grouped["l"].transform(lambda s: s.shift(1).rolling(10, min_periods=3).min())
    high5 = grouped["h"].transform(lambda s: s.shift(1).rolling(5, min_periods=2).max())
    low5 = grouped["l"].transform(lambda s: s.shift(1).rolling(5, min_periods=2).min())
    high60 = grouped["h"].transform(lambda s: s.shift(1).rolling(60, min_periods=10).max())
    low60_range = grouped["l"].transform(lambda s: s.shift(1).rolling(60, min_periods=10).min())
    frame["pre10_range_pct"] = _safe_div(high10 - low10, frame["prev_c"])
    frame["pre5_range_pct"] = _safe_div(high5 - low5, frame["prev_c"])
    frame["pre20_range_compression_vs_60"] = _safe_div(high20_prior - low20, high60 - low60_range)
    frame["ma20_slope_pre20"] = grouped["ma20"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["ma60_slope_pre20"] = grouped["ma60"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["ma20_slope_acceleration"] = frame["ma20_slope_pre20"] - grouped["ma20_slope_pre20"].shift(20)
    range_threshold = grouped["pre20_range_pct"].transform(lambda s: s.shift(1).rolling(252, min_periods=30).quantile(0.4))
    base_like = (frame["pre20_range_pct"] <= range_threshold) | frame["close_position_in_20d_range"].between(0.25, 0.75)
    frame["base_days_before_breakout"] = _group_series(frame, lambda g: _rolling_prior_sum(base_like.loc[g.index], 20))
    frame["pullback_depth_after_prior_high"] = _safe_div(frame["c"] - high60_prior, high60_prior)
    higher_low = frame["l"] > grouped["l"].shift(1)
    frame["higher_low_count_pre20"] = _group_series(frame, lambda g: _rolling_prior_sum(higher_low.loc[g.index], 20))
    frame["event_ymd"] = frame["ymd"].astype(int)
    return frame[["code", "event_ymd", *sorted(OHLCV_FEATURE_COLUMNS.intersection(frame.columns))]].copy()


def load_source_artifacts(
    pattern_dir: Path,
    guard_dir: Path,
    upside_dir: Path,
    wide_dir: Path,
    risk_dir: Path,
    threshold_dir: Path,
    feature_diagnosis_dir: Path,
) -> dict[str, Any]:
    required = ["_ARTIFACT_COMPLETE.json", "research_decision.json", "candidate_feature_shortlist.json", "negative_guard_decomposition_report.json"]
    missing = [name for name in required if not (feature_diagnosis_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"feature diagnosis source missing required artifacts: {missing} at {feature_diagnosis_dir}")
    feature_json = {name: _load_json(feature_diagnosis_dir / name) for name in required}
    if feature_json["_ARTIFACT_COMPLETE.json"].get("complete") is not True:
        raise RuntimeError("feature diagnosis source artifact is not complete")
    if feature_json["_ARTIFACT_COMPLETE.json"].get("silent_fallback_used") is not False or feature_json["research_decision.json"].get("silent_fallback_used") is not False:
        raise RuntimeError("feature diagnosis source artifact used silent fallback")
    if feature_json["_ARTIFACT_COMPLETE.json"].get("research_fallback_used") is not False or feature_json["research_decision.json"].get("research_fallback_used") is not False:
        raise RuntimeError("feature diagnosis source artifact used research fallback")
    if feature_json["research_decision.json"].get("authoritative_research_decision") != "winner_nonwinner_feature_diagnosis_hold":
        raise RuntimeError("feature diagnosis source decision is not winner_nonwinner_feature_diagnosis_hold")
    if int(feature_json["research_decision.json"].get("negative_guard_recommended_feature_count") or 0) != 0:
        raise RuntimeError("feature diagnosis source already has negative guard recommended features")
    loaded = diagnosis_mod.load_source_artifacts(pattern_dir, guard_dir, upside_dir, wide_dir, risk_dir, threshold_dir)
    events = diagnosis_mod.add_diagnostic_labels(loaded["events"], loaded["selected"])
    return {"events": events, "selected": loaded["selected"], "feature_json": feature_json}


def add_negative_guard_groups(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    frame["negative_guard_continuation_winner"] = frame["negative_guard_match"].astype(bool) & (
        frame["is_future_top10_by_ret20"].astype(bool)
        | pd.to_numeric(frame["ret20_fwd"], errors="coerce").ge(0.10)
        | pd.to_numeric(frame["mfe20"], errors="coerce").ge(0.15)
    )
    frame["negative_guard_blowoff_loser"] = frame["negative_guard_match"].astype(bool) & (
        frame["severe_loss20"].astype(bool)
        | pd.to_numeric(frame["ret20_fwd"], errors="coerce").le(0.0)
        | pd.to_numeric(frame["mae20"], errors="coerce").le(MAE_MATERIALLY_BAD_THRESHOLD)
    )
    frame["selected_negative_guard_winner"] = frame["selected_by_prior_policy"] & frame["negative_guard_continuation_winner"]
    frame["selected_negative_guard_severe_loser"] = frame["selected_by_prior_policy"] & frame["negative_guard_blowoff_loser"]
    loser_by_date = frame.groupby("event_date")["negative_guard_blowoff_loser"].transform("any")
    winner_by_date = frame.groupby("event_date")["negative_guard_continuation_winner"].transform("any")
    frame["same_date_negative_guard_oracle_miss"] = frame["selected_negative_guard_severe_loser"] & winner_by_date
    frame["candidate_count_same_day"] = frame.groupby("event_date")["code"].transform("count")
    frame["score_rank_within_date"] = pd.to_numeric(frame["same_date_score_rank"], errors="coerce")
    frame["research_score_decile"] = pd.to_numeric(frame["research_score_bucket"], errors="coerce")
    risk_value = pd.to_numeric(frame["threshold_risk_value"], errors="coerce")
    frame["risk_estimate_bucket"] = pd.qcut(risk_value.rank(method="first"), 5, labels=False, duplicates="drop").astype("float64") + 1.0
    frame["same_day_median_ret20_evaluation_only_for_reporting"] = frame.groupby("event_date")["ret20_fwd"].transform("median")
    frame["time_block"] = frame["event_date"].astype(str).str.slice(0, 4)
    return frame


def extract_negative_guard_features(events: pd.DataFrame, pattern_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    source_db = _source_db_from_pattern(pattern_dir)
    ng_events = events[events["negative_guard_match"].astype(bool)].copy()
    if source_db is None:
        feature_frame = pd.DataFrame(columns=["code", "event_ymd"])
        extraction_status = "source_db_unavailable"
    else:
        min_ymd = int(ng_events["event_ymd"].min())
        max_ymd = int(ng_events["event_ymd"].max())
        load_start = int((pd.to_datetime(str(min_ymd), format="%Y%m%d") - pd.Timedelta(days=430)).strftime("%Y%m%d"))
        daily = _load_daily_rows(source_db, codes=sorted(ng_events["code"].astype(str).unique().tolist()), start_ymd=load_start, end_ymd=max_ymd)
        feature_frame = build_ohlcv_feature_frame(daily)
        extraction_status = "ohlcv_from_source_db" if not feature_frame.empty else "ohlcv_source_returned_no_rows"
    merged = ng_events.merge(feature_frame, on=["code", "event_ymd"], how="left")
    return merged, {
        "source_db": str(source_db) if source_db else None,
        "extraction_status": extraction_status,
        "source_db_available": source_db is not None,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def _feature_columns(frame: pd.DataFrame, previous_shortlist: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    extracted = sorted(column for column in OHLCV_FEATURE_COLUMNS if column in frame.columns)
    previous = sorted({str(row.get("feature_id")) for row in previous_shortlist if row.get("feature_id") in frame.columns})
    return extracted, previous


def build_feature_availability_audit(frame: pd.DataFrame, extracted_features: list[str], previous_features: list[str], extraction_meta: dict[str, Any]) -> dict[str, Any]:
    groups = {
        "negative_guard_continuation_winner": frame["negative_guard_continuation_winner"],
        "negative_guard_blowoff_loser": frame["negative_guard_blowoff_loser"],
        "selected_negative_guard_winner": frame["selected_negative_guard_winner"],
        "selected_negative_guard_severe_loser": frame["selected_negative_guard_severe_loser"],
    }
    rows = []
    for feature in [*extracted_features, *previous_features]:
        present = diagnosis_mod._present_mask(frame[feature])
        rows.append(
            {
                "feature_id": feature,
                "source": "ohlcv_derived_current_or_past_only" if feature in extracted_features else "previous_shortlist_context",
                "feature_present_rate": float(present.mean()) if len(present) else 0.0,
                "feature_missing_rate_by_group": {
                    name: {
                        "feature_present_rate": float(present[mask].mean()) if int(mask.sum()) else None,
                        "feature_missing_rate": float((~present[mask]).mean()) if int(mask.sum()) else None,
                        "group_count": int(mask.sum()),
                    }
                    for name, mask in groups.items()
                },
            }
        )
    unavailable = [feature for feature in sorted(OHLCV_FEATURE_COLUMNS) if feature not in extracted_features]
    usable = [row for row in rows if row["source"] == "ohlcv_derived_current_or_past_only" and row["feature_present_rate"] >= 0.65]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "feature_rows": rows,
        "usable_feature_count": len(usable),
        "unavailable_feature_count": len(unavailable),
        "unavailable_features": unavailable,
        "silently_imputed_feature_count": 0,
        "extraction_meta": extraction_meta,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_leakage_audit(feature_columns: list[str]) -> dict[str, Any]:
    leaked = sorted(set(feature_columns).intersection(FUTURE_LABEL_COLUMNS).difference(EVALUATION_ONLY_REPORT_COLUMNS))
    return {
        "schema_version": f"{SCHEMA_PREFIX}_leakage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "feature_input_columns": sorted(feature_columns),
        "future_label_columns": sorted(FUTURE_LABEL_COLUMNS),
        "leaked_feature_columns": leaked,
        "future_label_used_in_feature_inputs": bool(leaked),
        "future_label_used_in_score_inputs": False,
        "same_period_label_tuning": False,
        "future_labels_used_for_group_definition_only": True,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def _contrast_rows(frame: pd.DataFrame, features: list[str], left_mask: pd.Series, right_mask: pd.Series, left_group: str, right_group: str) -> list[dict[str, Any]]:
    rows = []
    for feature in features:
        row = diagnosis_mod._numeric_contrast(frame[left_mask], frame[right_mask], feature)
        row.update({"left_group": left_group, "right_group": right_group, "source": "ohlcv_derived_current_or_past_only" if feature in OHLCV_FEATURE_COLUMNS else "previous_shortlist_context"})
        rows.append(row)
    return rows


def build_negative_guard_feature_contrast_report(frame: pd.DataFrame, features: list[str]) -> dict[str, Any]:
    rows = _contrast_rows(
        frame,
        features,
        frame["negative_guard_continuation_winner"],
        frame["negative_guard_blowoff_loser"],
        "negative_guard_continuation_winner",
        "negative_guard_blowoff_loser",
    )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_negative_guard_feature_contrast_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "negative_guard_winner_count": int(frame["negative_guard_continuation_winner"].sum()),
        "negative_guard_blowoff_loser_count": int(frame["negative_guard_blowoff_loser"].sum()),
        "rows": rows,
    }


def build_same_date_contrast(frame: pd.DataFrame, features: list[str]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ledger = []
    for event_date, group in frame.groupby("event_date", sort=True):
        winners = group[group["negative_guard_continuation_winner"]].sort_values("ret20_fwd", ascending=False)
        losers = group[group["negative_guard_blowoff_loser"]].sort_values("ret20_fwd", ascending=True)
        if winners.empty or losers.empty:
            continue
        winner = winners.iloc[0]
        for _, loser in losers.iterrows():
            ledger.append(
                {
                    "event_date": event_date,
                    "winner_code": str(winner["code"]),
                    "loser_code": str(loser["code"]),
                    "winner_ret20": float(winner["ret20_fwd"]),
                    "loser_ret20": float(loser["ret20_fwd"]),
                    "winner_minus_loser_feature_delta": {
                        feature: None if pd.isna(winner.get(feature)) or pd.isna(loser.get(feature)) else float(winner.get(feature) - loser.get(feature))
                        for feature in features
                    },
                }
            )
    rows = []
    for feature in features:
        deltas = [row["winner_minus_loser_feature_delta"][feature] for row in ledger if row["winner_minus_loser_feature_delta"].get(feature) is not None]
        positives = len([value for value in deltas if value > 0])
        negatives = len([value for value in deltas if value < 0])
        rows.append(
            {
                "feature_id": feature,
                "same_date_pair_count": len(deltas),
                "winner_minus_loser_feature_delta_avg": float(sum(deltas) / len(deltas)) if deltas else None,
                "winner_feature_dominance_rate": _safe_rate(positives, len(deltas)),
                "same_date_sign_stability": max(_safe_rate(positives, len(deltas)), _safe_rate(negatives, len(deltas))) if deltas else 0.0,
            }
        )
    return (
        {
            "schema_version": f"{SCHEMA_PREFIX}_same_date_negative_guard_contrast_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "same_date_pair_count": len(ledger),
            "rows": rows,
        },
        ledger,
    )


def build_score_bucket_contrast(frame: pd.DataFrame, features: list[str]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ledger = []
    rows = []
    for feature in features:
        effects = []
        for bucket, group in frame.groupby("research_score_decile", sort=True):
            left = group[group["negative_guard_continuation_winner"]]
            right = group[group["negative_guard_blowoff_loser"]]
            if len(left) < 2 or len(right) < 2:
                continue
            contrast = diagnosis_mod._numeric_contrast(left, right, feature)
            effect = diagnosis_mod._feature_effect(contrast)
            if effect is None:
                continue
            effects.append(effect)
            ledger.append(
                {
                    "feature_id": feature,
                    "score_bucket": int(bucket),
                    "score_bucket_pair_count": int(min(len(left), len(right))),
                    "feature_effect_within_score_bucket": effect,
                    "left_count": int(len(left)),
                    "right_count": int(len(right)),
                }
            )
        positives = len([value for value in effects if value > 0])
        negatives = len([value for value in effects if value < 0])
        rows.append(
            {
                "feature_id": feature,
                "score_bucket_pair_count": int(sum(row["score_bucket_pair_count"] for row in ledger if row["feature_id"] == feature)),
                "feature_effect_within_score_bucket": float(sum(effects) / len(effects)) if effects else None,
                "feature_effect_by_score_decile": [row for row in ledger if row["feature_id"] == feature],
                "score_bucket_stability": max(_safe_rate(positives, len(effects)), _safe_rate(negatives, len(effects))) if effects else 0.0,
                "feature_adds_information_beyond_score": bool(effects and max(_safe_rate(positives, len(effects)), _safe_rate(negatives, len(effects))) >= 0.55),
            }
        )
    return (
        {
            "schema_version": f"{SCHEMA_PREFIX}_score_bucket_negative_guard_contrast_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "rows": rows,
        },
        ledger,
    )


def build_time_block_stability(frame: pd.DataFrame, features: list[str]) -> dict[str, Any]:
    rows = []
    for feature in features:
        effects = []
        for block, group in frame.groupby("time_block", sort=True):
            if len(group[group["negative_guard_continuation_winner"]]) < 2 or len(group[group["negative_guard_blowoff_loser"]]) < 2:
                continue
            contrast = diagnosis_mod._numeric_contrast(group[group["negative_guard_continuation_winner"]], group[group["negative_guard_blowoff_loser"]], feature)
            effect = diagnosis_mod._feature_effect(contrast)
            if effect is not None:
                effects.append({"time_block": str(block), "effect_size": effect})
        values = [row["effect_size"] for row in effects]
        positives = len([value for value in values if value > 0])
        negatives = len([value for value in values if value < 0])
        stability = max(_safe_rate(positives, len(values)), _safe_rate(negatives, len(values))) if values else 0.0
        rows.append(
            {
                "feature_id": feature,
                "time_block_effects": effects,
                "time_block_sign_stability": stability,
                "time_block_effect_size_min": min(values) if values else None,
                "time_block_effect_size_max": max(values) if values else None,
                "feature_passes_stability_gate": bool(stability >= 0.60 and len(values) >= 2),
            }
        )
    return {"schema_version": f"{SCHEMA_PREFIX}_time_block_stability_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": rows}


def build_run_maturity_contrast_report(frame: pd.DataFrame) -> dict[str, Any]:
    features = ["prior_run_length", "close_vs_ma20_atr", "close_vs_ma60_atr", "upper_wick_ratio_event_day", "range_expansion_ratio_event_day", "volume_vs_20d_avg"]
    available = [feature for feature in features if feature in frame.columns]
    if not available:
        return {
            "schema_version": f"{SCHEMA_PREFIX}_run_maturity_contrast_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "early_continuation_candidate_count": 0,
            "late_blowoff_candidate_count": 0,
            "extension_without_exhaustion_candidate_count": 0,
            "extension_with_exhaustion_loser_count": 0,
            "rows": [],
        }
    run_length = pd.to_numeric(frame.get("prior_run_length"), errors="coerce")
    upper_wick = pd.to_numeric(frame.get("upper_wick_ratio_event_day"), errors="coerce")
    extension = pd.to_numeric(frame.get("close_vs_ma20_atr"), errors="coerce")
    early_cont = frame["negative_guard_continuation_winner"] & run_length.le(run_length.quantile(0.50))
    late_blowoff = frame["negative_guard_blowoff_loser"] & (run_length.ge(run_length.quantile(0.75)) | upper_wick.ge(upper_wick.quantile(0.75)))
    extension_without_exhaustion = frame["negative_guard_continuation_winner"] & extension.ge(extension.quantile(0.60)) & upper_wick.lt(upper_wick.quantile(0.60))
    extension_with_exhaustion = frame["negative_guard_blowoff_loser"] & extension.ge(extension.quantile(0.60)) & upper_wick.ge(upper_wick.quantile(0.60))
    rows = _contrast_rows(frame, available, early_cont, late_blowoff, "early_continuation_candidate", "late_blowoff_candidate")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_run_maturity_contrast_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "early_continuation_candidate_count": int(early_cont.sum()),
        "late_blowoff_candidate_count": int(late_blowoff.sum()),
        "run_maturity_feature_effect": rows,
        "extension_without_exhaustion_candidate_count": int(extension_without_exhaustion.sum()),
        "extension_with_exhaustion_loser_count": int(extension_with_exhaustion.sum()),
    }


def build_previous_shortlist_retest_report(previous_shortlist: list[dict[str, Any]], contrast: dict[str, Any], same_date: dict[str, Any], score_bucket: dict[str, Any], time_stability: dict[str, Any]) -> dict[str, Any]:
    previous_ids = {str(row.get("feature_id")) for row in previous_shortlist}
    contrast_by_feature = {row["feature_id"]: row for row in contrast["rows"] if row["feature_id"] in previous_ids}
    same_by_feature = {row["feature_id"]: row for row in same_date["rows"] if row["feature_id"] in previous_ids}
    score_by_feature = {row["feature_id"]: row for row in score_bucket["rows"] if row["feature_id"] in previous_ids}
    time_by_feature = {row["feature_id"]: row for row in time_stability["rows"] if row["feature_id"] in previous_ids}
    rows = []
    for feature in sorted(previous_ids):
        effect = diagnosis_mod._feature_effect(contrast_by_feature.get(feature, {})) if feature in contrast_by_feature else None
        passes = bool(
            effect is not None
            and abs(effect) >= 0.15
            and (same_by_feature.get(feature, {}).get("same_date_sign_stability") or 0.0) >= 0.55
            and score_by_feature.get(feature, {}).get("feature_adds_information_beyond_score") is True
            and time_by_feature.get(feature, {}).get("feature_passes_stability_gate") is True
        )
        rows.append({"feature_id": feature, "negative_guard_effect_size": effect, "previous_shortlist_recommended_for_v2": passes})
    return {
        "schema_version": f"{SCHEMA_PREFIX}_previous_shortlist_retest_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "previous_shortlist_feature_count": len(previous_ids),
        "previous_shortlist_negative_guard_effect_count": len([row for row in rows if row["negative_guard_effect_size"] is not None and abs(row["negative_guard_effect_size"]) >= 0.15]),
        "previous_shortlist_recommended_for_v2_count": len([row for row in rows if row["previous_shortlist_recommended_for_v2"]]),
        "rows": rows,
    }


def build_candidate_feature_shortlist_v2(
    *,
    availability: dict[str, Any],
    contrast: dict[str, Any],
    same_date: dict[str, Any],
    score_bucket: dict[str, Any],
    time_stability: dict[str, Any],
    previous_retest: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    availability_by_feature = {row["feature_id"]: row for row in availability["feature_rows"]}
    contrast_by_feature = {row["feature_id"]: row for row in contrast["rows"]}
    same_by_feature = {row["feature_id"]: row for row in same_date["rows"]}
    score_by_feature = {row["feature_id"]: row for row in score_bucket["rows"]}
    time_by_feature = {row["feature_id"]: row for row in time_stability["rows"]}
    recommended = []
    rejected = []
    for feature in sorted(OHLCV_FEATURE_COLUMNS):
        if feature not in contrast_by_feature:
            continue
        effect = diagnosis_mod._feature_effect(contrast_by_feature[feature])
        coverage = float(availability_by_feature.get(feature, {}).get("feature_present_rate", 0.0))
        same_stability = float(same_by_feature.get(feature, {}).get("same_date_sign_stability") or 0.0)
        bucket_stability = float(score_by_feature.get(feature, {}).get("score_bucket_stability") or 0.0)
        time_sign = float(time_by_feature.get(feature, {}).get("time_block_sign_stability") or 0.0)
        passes = {
            "coverage": coverage >= 0.65,
            "global_effect": effect is not None and abs(effect) >= 0.15,
            "same_date_stability": same_stability >= 0.55,
            "score_bucket_added_info": score_by_feature.get(feature, {}).get("feature_adds_information_beyond_score") is True,
            "time_block_stability": time_by_feature.get(feature, {}).get("feature_passes_stability_gate") is True,
            "leakage_safe": feature not in FUTURE_LABEL_COLUMNS,
        }
        payload = {
            "feature_id": feature,
            "source": "ohlcv_derived_current_or_past_only",
            "feature_type": "numeric",
            "intended_use": "soft_boost" if (effect or 0.0) > 0 else "soft_penalty",
            "target_problem": "negative_guard_decomposition",
            "coverage_rate": coverage,
            "effect_size": effect,
            "same_date_stability": same_stability,
            "score_bucket_stability": bucket_stability,
            "time_block_sign_stability": time_sign,
            "leakage_safe": passes["leakage_safe"],
            "recommended_for_next_scorer": all(passes.values()),
            "gate_status": passes,
        }
        if payload["recommended_for_next_scorer"]:
            recommended.append(payload)
        else:
            rejected.append({**payload, "reject_reasons": [key for key, value in passes.items() if not value]})
    return (
        {
            "schema_version": f"{SCHEMA_PREFIX}_candidate_feature_shortlist_v2_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "previous_shortlist_feature_count": int(previous_retest["previous_shortlist_feature_count"]),
            "previous_shortlist_negative_guard_effect_count": int(previous_retest["previous_shortlist_negative_guard_effect_count"]),
            "previous_shortlist_recommended_for_v2_count": int(previous_retest["previous_shortlist_recommended_for_v2_count"]),
            "new_negative_guard_feature_count": len([row for row in availability["feature_rows"] if row["source"] == "ohlcv_derived_current_or_past_only"]),
            "recommended_feature_count": len(recommended),
            "recommended_features": recommended,
            "candidate_scoring_created": False,
        },
        {
            "schema_version": f"{SCHEMA_PREFIX}_rejected_feature_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "rows": rejected,
        },
    )


def build_research_decision(shortlist: dict[str, Any], leakage: dict[str, Any], artifact_complete: bool) -> dict[str, Any]:
    recommended = shortlist["recommended_features"]
    no_leakage = leakage["future_label_used_in_feature_inputs"] is False and leakage["future_label_used_in_score_inputs"] is False
    same_date_pass = any(row["same_date_stability"] >= 0.55 for row in recommended)
    bucket_pass = any(row["score_bucket_stability"] >= 0.55 for row in recommended)
    keep_pass = artifact_complete and no_leakage and len(recommended) >= 2 and same_date_pass and bucket_pass
    hold_pass = artifact_complete and no_leakage and len(recommended) > 0
    if keep_pass:
        decision = "keep_candidate"
        authoritative = "negative_guard_feature_extraction_promising"
    elif hold_pass:
        decision = "hold"
        authoritative = "negative_guard_feature_extraction_hold"
    else:
        decision = "drop"
        authoritative = "negative_guard_feature_extraction_failed"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "negative_guard_missing_feature_extraction_audit",
        "boundary": "TRADEX-only",
        "axis_moved": "negative_guard_missing_feature_extraction_audit",
        "source_feature_diagnosis_decision": "winner_nonwinner_feature_diagnosis_hold",
        "feature_extraction_created": True,
        "feature_diagnosis_created": True,
        "candidate_feature_shortlist_v2_created": True,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "cost_slippage_evaluated": False,
        "cost_slippage_ignored_by_user_intent": True,
        "future_labels_used_for_group_definition_only": True,
        "future_labels_used_in_feature_inputs": False,
        "future_labels_used_in_score_inputs": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "typed_reasons": [
            "feature_extraction_created",
            "candidate_feature_shortlist_v2_created",
            "candidate_scoring_not_created",
            "threshold_policy_not_created",
            "no_future_label_leakage" if no_leakage else "future_label_leakage_detected",
            f"recommended_feature_count_{len(recommended)}",
            f"previous_shortlist_recommended_for_v2_count_{shortlist['previous_shortlist_recommended_for_v2_count']}",
        ],
        "recommended_feature_count": len(recommended),
        "artifact_complete": artifact_complete,
    }


def build_next_axis_recommendation(decision: dict[str, Any]) -> dict[str, Any]:
    if decision["authoritative_research_decision"] == "negative_guard_feature_extraction_promising":
        next_axis = "feature_based_wide_pool_rerank_v2"
    elif decision["authoritative_research_decision"] == "negative_guard_feature_extraction_hold":
        next_axis = "missing_ohlcv_field_volume_atr_ma_feature_extraction_runner"
    else:
        next_axis = "image_assisted_rerank_phase0_1"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "recommended_next_axis": next_axis,
        "one_recommended_next_axis_only": True,
    }


def build_contracts(
    *,
    events: pd.DataFrame,
    feature_columns: list[str],
    extraction_meta: dict[str, Any],
    dirs: dict[str, Path],
    previous_shortlist_count: int,
) -> dict[str, dict[str, Any]]:
    extraction_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_extraction_feature_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "feature_source": "daily_bars and daily_ma from source_db; current event day and prior rows only",
        "additional_feature_candidates": sorted(OHLCV_FEATURE_COLUMNS),
        "evaluation_only_report_columns": sorted(EVALUATION_ONLY_REPORT_COLUMNS),
        "future_labels_used_in_feature_inputs": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "extraction_meta": extraction_meta,
    }
    group_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_group_definition_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "groups": {
            "negative_guard_continuation_winner": "negative_guard_match = true and future_top10_by_ret20 or ret20 >= +10% or MFE20 >= +15%",
            "negative_guard_blowoff_loser": "negative_guard_match = true and severe_loss20 = true or ret20 <= 0 or MAE20 materially bad",
            "selected_negative_guard_winner": "selected_by_prior_policy and negative_guard_continuation_winner",
            "selected_negative_guard_severe_loser": "selected_by_prior_policy and negative_guard_blowoff_loser",
            "same_date_negative_guard_oracle_miss": "same-date negative_guard winner existed while selected negative_guard severe loser was selected",
        },
        "group_counts": {
            "negative_guard_continuation_winner": int(events["negative_guard_continuation_winner"].sum()),
            "negative_guard_blowoff_loser": int(events["negative_guard_blowoff_loser"].sum()),
            "selected_negative_guard_winner": int(events["selected_negative_guard_winner"].sum()),
            "selected_negative_guard_severe_loser": int(events["selected_negative_guard_severe_loser"].sum()),
            "same_date_negative_guard_oracle_miss": int(events["same_date_negative_guard_oracle_miss"].sum()),
        },
        "future_labels_used_for_group_definition_only": True,
        "future_labels_used_in_feature_inputs": False,
        "future_labels_used_in_score_inputs": False,
    }
    source_refs = build_source_artifact_refs(dirs)
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "negative_guard_missing_feature_extraction_audit",
        "boundary": "TRADEX-only",
        "axis_moved": "negative_guard_missing_feature_extraction_audit",
        "source_feature_diagnosis_decision": "winner_nonwinner_feature_diagnosis_hold",
        "artifact_roots": {key: str(value) for key, value in dirs.items()},
        "period": {"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max()))},
        "negative_guard_event_count": int(len(events)),
        "top_k": TOP_K,
        "previous_shortlist_feature_count": previous_shortlist_count,
        "feature_input_columns": sorted(feature_columns),
        "same_condition_controls": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime_condition": "negative_guard_matched_within_wide_pool_pre_strength_event_universe",
            "same_cost_slippage": "ignored_by_user_intent",
            "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }
    for payload in (extraction_contract, group_contract, evaluation_contract):
        payload["contract_hash"] = _stable_hash(payload)
    return {
        "evaluation_contract.json": evaluation_contract,
        "source_artifact_refs.json": source_refs,
        "extraction_feature_contract.json": extraction_contract,
        "group_definition_contract.json": group_contract,
    }


def build_source_artifact_refs(dirs: dict[str, Path]) -> dict[str, Any]:
    refs = []
    names = {
        "pattern": ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "pre_strength_event_ledger.jsonl", "research_decision.json"],
        "guard": ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "research_decision.json"],
        "upside": ["_ARTIFACT_COMPLETE.json", "research_decision.json"],
        "wide": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "score_leaderboard.json"],
        "risk": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "risk_leaderboard.json"],
        "threshold": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "threshold_leaderboard.json"],
        "feature_diagnosis": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "candidate_feature_shortlist.json", "negative_guard_decomposition_report.json"],
    }
    for source, root in dirs.items():
        for name in names[source]:
            path = root / name
            item: dict[str, Any] = {"source": source, "name": name, "path": str(path), "exists": path.exists()}
            if path.exists() and path.suffix == ".json":
                item["content_hash"] = _stable_hash(_load_json(path))
            refs.append(item)
    return {"schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "refs": refs}


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.update({"research_decision.json", "next_axis_recommendation.json"})
    required = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(required.values()),
        "required_artifacts": required,
        "paths": paths,
        "decision": decision.get("decision") if decision else None,
        "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
        "feature_extraction_created": True,
        "candidate_feature_shortlist_v2_created": True,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def run_negative_guard_missing_feature_extraction_audit_v1(
    *,
    source_pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    source_guard_run_id: str = DEFAULT_GUARD_RUN_ID,
    source_upside_run_id: str = DEFAULT_UPSIDE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    source_risk_run_id: str = DEFAULT_RISK_RUN_ID,
    source_threshold_run_id: str = DEFAULT_THRESHOLD_RUN_ID,
    source_feature_diagnosis_run_id: str = DEFAULT_FEATURE_DIAGNOSIS_RUN_ID,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    guard_root: str | Path = DEFAULT_GUARD_ROOT,
    upside_root: str | Path = DEFAULT_UPSIDE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    risk_root: str | Path = DEFAULT_RISK_ROOT,
    threshold_root: str | Path = DEFAULT_THRESHOLD_ROOT,
    feature_diagnosis_root: str | Path = DEFAULT_FEATURE_DIAGNOSIS_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    dirs = {
        "pattern": _run_dir(pattern_root, source_pattern_run_id, DEFAULT_PATTERN_ROOT),
        "guard": _run_dir(guard_root, source_guard_run_id, DEFAULT_GUARD_ROOT),
        "upside": _run_dir(upside_root, source_upside_run_id, DEFAULT_UPSIDE_ROOT),
        "wide": _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT),
        "risk": _run_dir(risk_root, source_risk_run_id, DEFAULT_RISK_ROOT),
        "threshold": _run_dir(threshold_root, source_threshold_run_id, DEFAULT_THRESHOLD_ROOT),
        "feature_diagnosis": _run_dir(feature_diagnosis_root, source_feature_diagnosis_run_id, DEFAULT_FEATURE_DIAGNOSIS_ROOT),
    }
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    loaded = load_source_artifacts(dirs["pattern"], dirs["guard"], dirs["upside"], dirs["wide"], dirs["risk"], dirs["threshold"], dirs["feature_diagnosis"])
    events = add_negative_guard_groups(loaded["events"])
    extracted, extraction_meta = extract_negative_guard_features(events, dirs["pattern"])
    previous_shortlist = loaded["feature_json"]["candidate_feature_shortlist.json"].get("rows", [])
    extracted_features, previous_features = _feature_columns(extracted, previous_shortlist)
    feature_columns = [*extracted_features, *previous_features]
    availability = build_feature_availability_audit(extracted, extracted_features, previous_features, extraction_meta)
    leakage = build_leakage_audit(feature_columns)
    contrast = build_negative_guard_feature_contrast_report(extracted, feature_columns)
    same_date, same_date_ledger = build_same_date_contrast(extracted, feature_columns)
    score_bucket, score_bucket_ledger = build_score_bucket_contrast(extracted, feature_columns)
    run_maturity = build_run_maturity_contrast_report(extracted)
    time_stability = build_time_block_stability(extracted, feature_columns)
    previous_retest = build_previous_shortlist_retest_report(previous_shortlist, contrast, same_date, score_bucket, time_stability)
    shortlist, rejected = build_candidate_feature_shortlist_v2(
        availability=availability,
        contrast=contrast,
        same_date=same_date,
        score_bucket=score_bucket,
        time_stability=time_stability,
        previous_retest=previous_retest,
    )
    contracts_payload = build_contracts(
        events=extracted,
        feature_columns=feature_columns,
        extraction_meta=extraction_meta,
        dirs=dirs,
        previous_shortlist_count=len(previous_shortlist),
    )
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=RANDOM_SEED,
        random_seed=RANDOM_SEED,
        input_artifacts=[
            {"name": f"source_{name}_artifact_root", "path": str(path)}
            for name, path in dirs.items()
        ]
        + [{"name": "evaluation_contract", "contract_hash": contracts_payload["evaluation_contract.json"]["contract_hash"]}],
        asof=str(int(extracted["event_ymd"].max())) if not extracted.empty else "",
        config={"axis_id": AXIS_ID, "top_k": TOP_K, "candidate_scoring_created": False, "threshold_policy_created": False},
        universe=sorted(extracted["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(extracted["event_ymd"].min())) if not extracted.empty else "", "end_date": str(int(extracted["event_ymd"].max())) if not extracted.empty else "", "label": "negative_guard_missing_feature_extraction_audit"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    paths: dict[str, str] = {}
    for name, payload in {
        **contracts_payload,
        "run_manifest.json": run_manifest,
        "feature_availability_audit.json": availability,
        "leakage_audit.json": leakage,
        "negative_guard_feature_contrast_report.json": contrast,
        "same_date_negative_guard_contrast_report.json": same_date,
        "score_bucket_negative_guard_contrast_report.json": score_bucket,
        "run_maturity_contrast_report.json": run_maturity,
        "previous_shortlist_retest_report.json": previous_retest,
        "time_block_stability.json": time_stability,
        "candidate_feature_shortlist_v2.json": shortlist,
        "rejected_feature_report.json": rejected,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    ledger_columns = [
        "event_date",
        "event_ymd",
        "code",
        "ret20_fwd",
        "mfe20",
        "mae20",
        "severe_loss20",
        "negative_guard_continuation_winner",
        "negative_guard_blowoff_loser",
        "selected_negative_guard_winner",
        "selected_negative_guard_severe_loser",
        "same_date_negative_guard_oracle_miss",
        "same_day_median_ret20_evaluation_only_for_reporting",
        *feature_columns,
    ]
    paths["extracted_negative_guard_feature_ledger.jsonl"] = str(_write_jsonl(output_dir / "extracted_negative_guard_feature_ledger.jsonl", extracted[[column for column in ledger_columns if column in extracted.columns]].to_dict(orient="records")))
    paths["negative_guard_same_date_pair_ledger.jsonl"] = str(_write_jsonl(output_dir / "negative_guard_same_date_pair_ledger.jsonl", same_date_ledger))
    paths["negative_guard_score_bucket_pair_ledger.jsonl"] = str(_write_jsonl(output_dir / "negative_guard_score_bucket_pair_ledger.jsonl", score_bucket_ledger))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(shortlist, leakage, bool(pre_complete["complete"]))
    next_axis = build_next_axis_recommendation(decision)
    paths["next_axis_recommendation.json"] = str(_write_json(output_dir / "next_axis_recommendation.json", next_axis))
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "recommended_feature_count": decision["recommended_feature_count"],
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-pattern-run-id", default=DEFAULT_PATTERN_RUN_ID)
    parser.add_argument("--source-guard-run-id", default=DEFAULT_GUARD_RUN_ID)
    parser.add_argument("--source-upside-run-id", default=DEFAULT_UPSIDE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--source-risk-run-id", default=DEFAULT_RISK_RUN_ID)
    parser.add_argument("--source-threshold-run-id", default=DEFAULT_THRESHOLD_RUN_ID)
    parser.add_argument("--source-feature-diagnosis-run-id", default=DEFAULT_FEATURE_DIAGNOSIS_RUN_ID)
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--guard-root", default=str(DEFAULT_GUARD_ROOT))
    parser.add_argument("--upside-root", default=str(DEFAULT_UPSIDE_ROOT))
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--risk-root", default=str(DEFAULT_RISK_ROOT))
    parser.add_argument("--threshold-root", default=str(DEFAULT_THRESHOLD_ROOT))
    parser.add_argument("--feature-diagnosis-root", default=str(DEFAULT_FEATURE_DIAGNOSIS_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_negative_guard_missing_feature_extraction_audit_v1(
        source_pattern_run_id=args.source_pattern_run_id,
        source_guard_run_id=args.source_guard_run_id,
        source_upside_run_id=args.source_upside_run_id,
        source_wide_run_id=args.source_wide_run_id,
        source_risk_run_id=args.source_risk_run_id,
        source_threshold_run_id=args.source_threshold_run_id,
        source_feature_diagnosis_run_id=args.source_feature_diagnosis_run_id,
        pattern_root=args.pattern_root,
        guard_root=args.guard_root,
        upside_root=args.upside_root,
        wide_root=args.wide_root,
        risk_root=args.risk_root,
        threshold_root=args.threshold_root,
        feature_diagnosis_root=args.feature_diagnosis_root,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
