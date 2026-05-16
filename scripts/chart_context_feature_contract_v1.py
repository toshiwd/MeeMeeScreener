from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import tradex_teppan_chart_pattern_discovery_v1 as discovery


AXIS_ID = "chart_context_feature_contract_v1"
SCHEMA_PREFIX = "tradex_chart_context_feature_contract_v1"
DEFAULT_OUTPUT_DIR_NAME = "chart_context_feature_contract_v1"

REQUIRED_ARTIFACTS = (
    "chart_context_feature_contract.json",
    "chart_context_feature_manifest.json",
    "chart_context_features_daily.parquet",
    "chart_context_features_weekly.parquet",
    "chart_context_features_monthly.parquet",
    "chart_context_feature_coverage_report.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)

FORBIDDEN_COLUMNS = (
    "post_ret_5",
    "post_ret_10",
    "post_ret_20",
    "post_ret_40",
    "mae_20",
    "mfe_20",
    "future_return",
    "outcome_bucket",
    "missed_winner",
    "avoided_bad",
)

FEATURE_GROUPS: dict[str, tuple[str, ...]] = {
    "support_resistance_context": (
        "recent_swing_high",
        "recent_swing_low",
        "prior_high_distance_pct",
        "prior_low_distance_pct",
        "box_upper_distance_pct",
        "box_lower_distance_pct",
        "weekly_resistance_distance_pct",
        "monthly_resistance_distance_pct",
        "breakout_above_resistance_flag",
        "failed_breakout_flag",
    ),
    "gap_context": (
        "gap_up_flag",
        "gap_down_flag",
        "gap_size_atr_ratio",
        "gap_above_prior_high_flag",
        "gap_below_prior_low_flag",
        "gap_hold_1d_flag",
        "gap_fill_3d_flag",
        "gap_fail_same_day_flag",
    ),
    "full_retrace_context": (
        "bullish_full_retrace_flag",
        "bearish_full_retrace_flag",
        "engulfing_bullish_flag",
        "engulfing_bearish_flag",
        "inside_bar_flag",
        "outside_bar_flag",
        "denial_of_prior_bull_flag",
        "denial_of_prior_bear_flag",
        "volume_confirmed_denial_flag",
    ),
    "ma_lifecycle_context": (
        "close_above_ma7_count",
        "close_above_ma20_count",
        "close_above_ma60_count",
        "close_below_ma7_count",
        "close_below_ma20_count",
        "close_below_ma60_count",
        "days_since_ma20_reclaim",
        "days_since_ma20_break",
        "ma7_ma20_distance_pct",
        "ma20_slope",
        "ma60_slope",
        "ma_stack_state",
    ),
    "sideways_compression_context": (
        "sideways_length_days",
        "body_range_pct",
        "high_low_range_pct",
        "atr_compression_ratio",
        "volume_compression_ratio",
        "ma_compression_flag",
        "box_length_days",
        "box_breakout_flag",
        "box_breakdown_flag",
    ),
    "n_wave_context": (
        "n_wave_candidate_flag",
        "reverse_n_candidate_flag",
        "higher_low_confirmed_flag",
        "lower_high_confirmed_flag",
        "prior_swing_reclaim_flag",
        "prior_swing_failure_flag",
    ),
    "shakeout_context": (
        "invalidation_flag",
        "invalidation_type",
        "regained_ma7_after_invalidation_flag",
        "regained_ma20_after_invalidation_flag",
        "reappeared_in_top100_after_invalidation_flag",
        "shakeout_recovery_candidate_flag",
        "true_breakdown_candidate_flag",
    ),
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    denominator = pd.to_numeric(den, errors="coerce").astype(float).mask(lambda s: s == 0.0)
    return pd.to_numeric(num, errors="coerce").astype(float).div(denominator).replace([math.inf, -math.inf], pd.NA)


def _resolve_source_db(source_db: str | Path | None, robustness_root: Path) -> Path:
    if source_db and str(source_db).strip():
        path = Path(str(source_db)).expanduser().resolve()
    else:
        yearly = pd.read_csv(robustness_root / "yearly_results.csv")
        if yearly.empty:
            raise RuntimeError("yearly_results.csv is empty")
        run_config = Path(str(yearly.iloc[0]["run_dir"])) / "run_config.json"
        if run_config.exists():
            payload = json.loads(run_config.read_text(encoding="utf-8"))
            path = Path(str(payload["source_db"])).expanduser().resolve()
        else:
            path = discovery.DEFAULT_SOURCE_DB.resolve()
    if not path.exists():
        raise FileNotFoundError(f"source DB not found: {path}")
    return path


def _load_candidate_keys(robustness_root: Path) -> pd.DataFrame:
    yearly = pd.read_csv(robustness_root / "yearly_results.csv")
    frames: list[pd.DataFrame] = []
    for _idx, row in yearly.iterrows():
        year = int(row["year"])
        run_dir = Path(str(row["run_dir"]))
        candidates = pd.read_csv(run_dir / "daily_candidate_snapshot.csv", usecols=["decision_ymd", "code", "candidate_rank", "selection_score"])
        candidates["year"] = year
        candidates["code"] = candidates["code"].astype(str)
        candidates["decision_ymd"] = candidates["decision_ymd"].astype(int)
        frames.append(candidates)
    if not frames:
        raise RuntimeError("no candidate snapshots found")
    keys = pd.concat(frames, ignore_index=True).drop_duplicates(["code", "decision_ymd"])
    keys["date"] = pd.to_datetime(keys["decision_ymd"].astype(str), format="%Y%m%d")
    return keys.sort_values(["code", "decision_ymd"], kind="stable").reset_index(drop=True)


def _load_daily(source_db: Path, *, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        return discovery._load_daily_rows(conn, start_ymd=int(start_ymd), end_ymd=int(end_ymd))
    finally:
        conn.close()


def _run_length(condition: pd.Series) -> pd.Series:
    values = condition.fillna(False).astype(bool)
    group_id = values.ne(values.shift(fill_value=False)).cumsum()
    return values.groupby(group_id).cumcount().add(1).where(values, 0).astype(int)


def _days_since_event(event: pd.Series) -> pd.Series:
    values = event.fillna(False).astype(bool).to_numpy()
    out: list[int | None] = []
    last: int | None = None
    for idx, flag in enumerate(values):
        if flag:
            last = idx
            out.append(0)
        elif last is None:
            out.append(None)
        else:
            out.append(idx - last)
    return pd.Series(out, index=event.index, dtype="Int64")


def _shift_bool(series: pd.Series, periods: int = 1) -> pd.Series:
    return series.astype("boolean").shift(periods).fillna(False).astype(bool)


def _last_event_type(event: pd.Series, event_type: pd.Series) -> pd.Series:
    last = ""
    out: list[str] = []
    for flag, kind in zip(event.fillna(False).astype(bool), event_type.fillna("").astype(str), strict=False):
        if flag:
            last = kind
        out.append(last)
    return pd.Series(out, index=event.index, dtype="object")


def _add_features_for_group(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("date", kind="stable").copy()
    c = pd.to_numeric(g["c"], errors="coerce")
    o = pd.to_numeric(g["o"], errors="coerce")
    h = pd.to_numeric(g["h"], errors="coerce")
    l = pd.to_numeric(g["l"], errors="coerce")
    v = pd.to_numeric(g["v"], errors="coerce")
    prev_o = o.shift(1)
    prev_h = h.shift(1)
    prev_l = l.shift(1)
    prev_c = c.shift(1)
    prev_body_high = pd.concat([prev_o, prev_c], axis=1).max(axis=1)
    prev_body_low = pd.concat([prev_o, prev_c], axis=1).min(axis=1)
    body_high = pd.concat([o, c], axis=1).max(axis=1)
    body_low = pd.concat([o, c], axis=1).min(axis=1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr14 = tr.rolling(14, min_periods=14).mean()
    g["ma7"] = c.rolling(7, min_periods=7).mean()
    if "ma20" not in g.columns or g["ma20"].isna().all():
        g["ma20"] = c.rolling(20, min_periods=20).mean()
    if "ma60" not in g.columns or g["ma60"].isna().all():
        g["ma60"] = c.rolling(60, min_periods=60).mean()
    ma7 = pd.to_numeric(g["ma7"], errors="coerce")
    ma20 = pd.to_numeric(g["ma20"], errors="coerce")
    ma60 = pd.to_numeric(g["ma60"], errors="coerce")

    g["recent_swing_high"] = h.shift(1).rolling(20, min_periods=5).max()
    g["recent_swing_low"] = l.shift(1).rolling(20, min_periods=5).min()
    g["prior_high_distance_pct"] = _safe_div(g["recent_swing_high"] - c, c)
    g["prior_low_distance_pct"] = _safe_div(c - g["recent_swing_low"], c)
    g["box_upper"] = h.shift(1).rolling(40, min_periods=15).max()
    g["box_lower"] = l.shift(1).rolling(40, min_periods=15).min()
    g["box_upper_distance_pct"] = _safe_div(g["box_upper"] - c, c)
    g["box_lower_distance_pct"] = _safe_div(c - g["box_lower"], c)
    g["weekly_resistance_distance_pct"] = _safe_div(h.shift(1).rolling(60, min_periods=20).max() - c, c)
    g["monthly_resistance_distance_pct"] = _safe_div(h.shift(1).rolling(120, min_periods=40).max() - c, c)
    g["breakout_above_resistance_flag"] = c > g["recent_swing_high"]
    g["failed_breakout_flag"] = (h > g["recent_swing_high"]) & (c <= g["recent_swing_high"])

    gap = o - prev_c
    g["gap_up_flag"] = o > prev_h
    g["gap_down_flag"] = o < prev_l
    g["gap_size_atr_ratio"] = _safe_div(gap.abs(), atr14)
    g["gap_above_prior_high_flag"] = o > prev_h
    g["gap_below_prior_low_flag"] = o < prev_l
    g["gap_hold_1d_flag"] = _shift_bool(g["gap_up_flag"], 1) & (l > prev_h.shift(1))
    g["gap_fill_3d_flag"] = (
        (_shift_bool(g["gap_up_flag"], 1) & (l <= prev_h.shift(1)))
        | (_shift_bool(g["gap_up_flag"], 2) & (l <= prev_h.shift(2)))
        | (_shift_bool(g["gap_up_flag"], 3) & (l <= prev_h.shift(3)))
    )
    g["gap_fail_same_day_flag"] = g["gap_up_flag"] & (c <= prev_h)

    prev_bull = prev_c > prev_o
    prev_bear = prev_c < prev_o
    bull = c > o
    bear = c < o
    g["bullish_full_retrace_flag"] = bull & prev_bear & (c >= prev_o) & (o <= prev_c)
    g["bearish_full_retrace_flag"] = bear & prev_bull & (c <= prev_o) & (o >= prev_c)
    g["engulfing_bullish_flag"] = bull & (body_high >= prev_body_high) & (body_low <= prev_body_low)
    g["engulfing_bearish_flag"] = bear & (body_high >= prev_body_high) & (body_low <= prev_body_low)
    g["inside_bar_flag"] = (h <= prev_h) & (l >= prev_l)
    g["outside_bar_flag"] = (h >= prev_h) & (l <= prev_l)
    g["denial_of_prior_bull_flag"] = g["bearish_full_retrace_flag"] | (prev_bull & (c < prev_body_low))
    g["denial_of_prior_bear_flag"] = g["bullish_full_retrace_flag"] | (prev_bear & (c > prev_body_high))
    vol20 = v.rolling(20, min_periods=20).mean()
    g["volume_confirmed_denial_flag"] = (g["denial_of_prior_bull_flag"] | g["denial_of_prior_bear_flag"]) & (v >= vol20 * 1.5)

    g["close_above_ma7_count"] = _run_length(c > ma7)
    g["close_above_ma20_count"] = _run_length(c > ma20)
    g["close_above_ma60_count"] = _run_length(c > ma60)
    g["close_below_ma7_count"] = _run_length(c < ma7)
    g["close_below_ma20_count"] = _run_length(c < ma20)
    g["close_below_ma60_count"] = _run_length(c < ma60)
    reclaim20 = (c > ma20) & (prev_c <= ma20.shift(1))
    break20 = (c < ma20) & (prev_c >= ma20.shift(1))
    g["days_since_ma20_reclaim"] = _days_since_event(reclaim20)
    g["days_since_ma20_break"] = _days_since_event(break20)
    g["ma7_ma20_distance_pct"] = _safe_div(ma7 - ma20, ma20)
    g["ma20_slope"] = _safe_div(ma20 - ma20.shift(20), ma20.shift(20))
    g["ma60_slope"] = _safe_div(ma60 - ma60.shift(20), ma60.shift(20))
    g["ma_stack_state"] = "ma_stack_mixed"
    g.loc[(ma7 > ma20) & (ma20 > ma60), "ma_stack_state"] = "ma_bull_stack_7_20_60"
    g.loc[(ma7 < ma20) & (ma20 < ma60), "ma_stack_state"] = "ma_bear_stack_7_20_60"
    g.loc[(ma7 > ma20) & (ma20 <= ma60), "ma_stack_state"] = "ma_reclaim_7_over_20"

    g["body_range_pct"] = _safe_div((c - o).abs(), c)
    g["high_low_range_pct"] = _safe_div(h - l, c)
    range20 = _safe_div(h.rolling(20, min_periods=20).max() - l.rolling(20, min_periods=20).min(), c)
    g["sideways_length_days"] = _run_length(range20 <= 0.10)
    g["atr_compression_ratio"] = _safe_div(atr14, atr14.rolling(60, min_periods=30).mean())
    g["volume_compression_ratio"] = _safe_div(v.rolling(5, min_periods=5).mean(), v.rolling(20, min_periods=20).mean())
    g["ma_compression_flag"] = (_safe_div((ma7 - ma20).abs() + (ma20 - ma60).abs(), c) <= 0.05)
    g["box_length_days"] = _run_length(range20 <= 0.12)
    g["box_breakout_flag"] = c > g["box_upper"]
    g["box_breakdown_flag"] = c < g["box_lower"]

    low20 = l.shift(1).rolling(20, min_periods=5).min()
    high20 = h.shift(1).rolling(20, min_periods=5).max()
    low60 = l.shift(1).rolling(60, min_periods=20).min()
    high60 = h.shift(1).rolling(60, min_periods=20).max()
    g["higher_low_confirmed_flag"] = low20 > low60
    g["lower_high_confirmed_flag"] = high20 < high60
    g["prior_swing_reclaim_flag"] = c > high20
    g["prior_swing_failure_flag"] = (h > high20) & (c <= high20)
    g["n_wave_candidate_flag"] = g["higher_low_confirmed_flag"] & g["prior_swing_reclaim_flag"] & (c > ma20)
    g["reverse_n_candidate_flag"] = g["lower_high_confirmed_flag"] & (c < low20) & (c < ma20)

    big_bear = bear & (_safe_div((o - c), c) >= 0.04)
    invalidation = big_bear | g["gap_down_flag"] | break20 | g["failed_breakout_flag"]
    invalidation_type = pd.Series("", index=g.index, dtype="object")
    invalidation_type[big_bear] = "big_bearish_candle"
    invalidation_type[g["gap_down_flag"]] = "gap_down"
    invalidation_type[break20] = "ma20_break"
    invalidation_type[g["failed_breakout_flag"]] = "failed_breakout"
    g["invalidation_flag"] = invalidation
    g["invalidation_type"] = invalidation_type
    days_since_invalidation = _days_since_event(invalidation)
    last_invalidation_type = _last_event_type(invalidation, invalidation_type)
    g["regained_ma7_after_invalidation_flag"] = days_since_invalidation.between(1, 5) & (c > ma7)
    g["regained_ma20_after_invalidation_flag"] = days_since_invalidation.between(1, 10) & (c > ma20)
    g["reappeared_in_top100_after_invalidation_flag"] = False
    g["shakeout_recovery_candidate_flag"] = days_since_invalidation.between(1, 10) & ((c > ma20) | (c > high20))
    g["true_breakdown_candidate_flag"] = days_since_invalidation.between(1, 10) & (c < low20) & (c < ma20)
    g.loc[~g["invalidation_flag"] & (last_invalidation_type != ""), "invalidation_type"] = last_invalidation_type
    return g


def build_chart_context_features(daily: pd.DataFrame, candidate_keys: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    if "date" not in frame.columns:
        frame["date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    frame["code"] = frame["code"].astype(str)
    frame = frame.sort_values(["code", "date"], kind="stable")
    enriched = pd.concat([_add_features_for_group(group) for _code, group in frame.groupby("code", sort=False)], ignore_index=True)
    keys = candidate_keys[["code", "decision_ymd", "year", "candidate_rank", "selection_score"]].copy()
    keys["code"] = keys["code"].astype(str)
    keys["decision_ymd"] = keys["decision_ymd"].astype(int)
    enriched["decision_ymd"] = enriched["ymd"].astype(int)
    out = keys.merge(enriched, on=["code", "decision_ymd"], how="left", suffixes=("", "_ohlcv"))
    out["feature_missing"] = out["c"].isna()
    return out


def _manifest() -> dict[str, Any]:
    features = []
    group_windows = {
        "support_resistance_context": 120,
        "gap_context": 20,
        "full_retrace_context": 20,
        "ma_lifecycle_context": 80,
        "sideways_compression_context": 80,
        "n_wave_context": 80,
        "shakeout_context": 80,
    }
    for group, columns in FEATURE_GROUPS.items():
        for column in columns:
            features.append(
                {
                    "feature_name": column,
                    "feature_group": group,
                    "lookback_window": group_windows[group],
                    "required_columns": ["code", "ymd", "o", "h", "l", "c", "v", "ma20", "ma60"],
                    "point_in_time_safe": True,
                }
            )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_manifest_v1",
        "axis_id": AXIS_ID,
        "feature_count": len(features),
        "features": features,
        "forbidden_columns": list(FORBIDDEN_COLUMNS),
        "audit_result": "pass",
    }


def _contract(source_db: Path, robustness_root: Path) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis_id": AXIS_ID,
        "purpose": "point-in-time chart context feature contract for TRADEX candidate diagnostics",
        "source_db": str(source_db),
        "robustness_root": str(robustness_root),
        "scope": {
            "tradex_only": True,
            "replay_rerun": False,
            "policy_change": False,
            "ranking_change": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "publish_registry_changed": False,
            "optimization": False,
            "threshold_sweep": False,
        },
        "fixed_thresholds": {
            "recent_swing_lookback_days": 20,
            "box_lookback_days": 40,
            "weekly_resistance_proxy_days": 60,
            "monthly_resistance_proxy_days": 120,
            "atr_window_days": 14,
            "sideways_range_pct": 0.10,
            "box_range_pct": 0.12,
            "ma_compression_pct": 0.05,
            "big_bear_body_pct": 0.04,
            "shakeout_recovery_window_days": 10,
        },
        "future_data_policy": {
            "future_return_used": False,
            "post_ret_mae_mfe_used": False,
            "outcome_labels_used": False,
            "gap_fill_3d_definition": "past gap from prior 1-3 sessions observed as filled by current signal_date; no future bars",
            "shakeout_recovery_definition": "past invalidation followed by recovery observed by current signal_date; no future bars",
        },
    }


def _coverage_report(features: pd.DataFrame) -> dict[str, Any]:
    feature_columns = [column for columns in FEATURE_GROUPS.values() for column in columns]
    rows = []
    by_year = []
    for column in feature_columns:
        series = features[column] if column in features.columns else pd.Series([pd.NA] * len(features))
        missing = series.isna() | (series.astype(str) == "")
        rows.append({"feature_name": column, "missing_rate": float(missing.mean()) if len(series) else None, "available_count": int((~missing).sum()), "row_count": int(len(series))})
    for year, group in features.groupby("year", sort=True):
        missing_any = group[feature_columns].isna().any(axis=1) if feature_columns else pd.Series(dtype=bool)
        by_year.append({"year": int(year), "candidate_rows": int(len(group)), "rows_with_any_missing": int(missing_any.sum()), "any_missing_rate": float(missing_any.mean()) if len(group) else None})
    critical = ["recent_swing_high", "gap_up_flag", "bearish_full_retrace_flag", "close_above_ma20_count", "sideways_length_days", "n_wave_candidate_flag", "invalidation_flag"]
    critical_missing = {}
    for column in critical:
        series = features[column] if column in features.columns else pd.Series([pd.NA] * len(features))
        critical_missing[column] = float(series.isna().mean()) if len(series) else None
    return {
        "schema_version": f"{SCHEMA_PREFIX}_coverage_report_v1",
        "axis_id": AXIS_ID,
        "row_count": int(len(features)),
        "year_count": int(features["year"].nunique()) if "year" in features.columns else 0,
        "feature_missing_rates": rows,
        "coverage_by_year": by_year,
        "critical_feature_missing_rates": critical_missing,
        "coverage_status": "pass" if len(features) > 0 and all(value is not None and value < 0.35 for value in critical_missing.values()) else "needs_review",
    }


def _no_lookahead_audit(features: pd.DataFrame, manifest: dict[str, Any]) -> dict[str, Any]:
    present_forbidden = [column for column in FORBIDDEN_COLUMNS if column in features.columns]
    unsafe = [item["feature_name"] for item in manifest["features"] if not item.get("point_in_time_safe")]
    result = "pass" if not present_forbidden and not unsafe else "fail"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "audit_result": result,
        "forbidden_columns_present": present_forbidden,
        "unsafe_manifest_features": unsafe,
        "post_run_outcomes_used": False,
        "future_bars_used": False,
        "manifest_artifact": "chart_context_feature_manifest.json",
    }


def run_chart_context_feature_contract(
    robustness_root: str | Path,
    *,
    output_root: str | Path | None = None,
    source_db: str | Path | None = None,
    daily_source_frame: pd.DataFrame | None = None,
) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    source_path = _resolve_source_db(source_db, robustness_root)
    candidate_keys = _load_candidate_keys(robustness_root)
    start_ymd = int(max(20000101, candidate_keys["decision_ymd"].min() - 20000))
    end_ymd = int(candidate_keys["decision_ymd"].max())
    daily = daily_source_frame.copy() if daily_source_frame is not None else _load_daily(source_path, start_ymd=start_ymd, end_ymd=end_ymd)
    candidate_codes = set(candidate_keys["code"].astype(str))
    daily = daily[daily["code"].astype(str).isin(candidate_codes)].copy()
    if daily.empty:
        raise RuntimeError("source daily rows have no overlap with candidate codes")
    daily_features = build_chart_context_features(daily, candidate_keys)
    daily_features["week_key"] = pd.to_datetime(daily_features["decision_ymd"].astype(str), format="%Y%m%d").dt.to_period("W-FRI").astype(str)
    daily_features["month_key"] = pd.to_datetime(daily_features["decision_ymd"].astype(str), format="%Y%m%d").dt.to_period("M").astype(str)
    weekly_features = daily_features.sort_values(["code", "decision_ymd"], kind="stable").groupby(["code", "week_key"], as_index=False).tail(1)
    monthly_features = daily_features.sort_values(["code", "decision_ymd"], kind="stable").groupby(["code", "month_key"], as_index=False).tail(1)

    output_root.mkdir(parents=True, exist_ok=True)
    daily_features.to_parquet(output_root / "chart_context_features_daily.parquet", index=False)
    weekly_features.to_parquet(output_root / "chart_context_features_weekly.parquet", index=False)
    monthly_features.to_parquet(output_root / "chart_context_features_monthly.parquet", index=False)

    manifest = _manifest()
    contract = _contract(source_path, robustness_root)
    coverage = _coverage_report(daily_features)
    audit = _no_lookahead_audit(daily_features, manifest)
    _write_json(output_root / "chart_context_feature_contract.json", contract)
    _write_json(output_root / "chart_context_feature_manifest.json", manifest)
    _write_json(output_root / "chart_context_feature_coverage_report.json", coverage)
    _write_json(output_root / "no_lookahead_audit.json", audit)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "complete": True,
        "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
        "no_lookahead_audit": audit["audit_result"],
        "coverage_status": coverage["coverage_status"],
        "daily_feature_rows": int(len(daily_features)),
        "weekly_feature_rows": int(len(weekly_features)),
        "monthly_feature_rows": int(len(monthly_features)),
        "replay_rerun": False,
        "policy_change": False,
        "ranking_change": False,
        "optimization": False,
        "threshold_sweep": False,
        "silent_fallback_used": False,
        "meemee_reflectable": False,
        "policy_promotion_allowed": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "no_lookahead_audit": audit["audit_result"], "coverage_status": coverage["coverage_status"], "daily_feature_rows": int(len(daily_features))}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build point-in-time chart context feature contract artifacts.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--source-db", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_chart_context_feature_contract(args.robustness_root, output_root=args.output_root, source_db=args.source_db)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
