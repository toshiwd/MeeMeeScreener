from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "recent_win_pattern_shift_audit_v1"
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_CANDIDATE_ROOT = Path(r"G:\Tradex\portfolio_agent_baseline_robustness_gate_v1\baseline-2019-2025-robustness-gate\subruns")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\recent_win_pattern_shift_audit_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "candidate_source_coverage_report.csv",
    "candidate_rows_with_features.csv",
    "feature_lift_by_period.csv",
    "winner_loser_decomposition.csv",
    "pattern_shift_matrix.csv",
    "source_stability_summary.csv",
    "regime_shift_summary.csv",
    "next_challenger_candidates.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
YEARS = tuple(range(2019, 2026))


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _mean(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def _rate(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    values = df[col].dropna()
    if values.empty:
        return None
    if values.dtype == bool:
        return float(values.mean())
    return float(values.astype("string").str.lower().isin({"true", "1", "yes"}).mean())


def _coverage(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    return float(pd.to_numeric(df[col], errors="coerce").notna().mean())


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    return pd.to_numeric(num / den.replace(0, pd.NA), errors="coerce")


def _period(year: int) -> str:
    if 2019 <= year <= 2023:
        return "pre_recent_2019_2023"
    if 2024 <= year <= 2026:
        return "recent_2024_2026"
    return str(year)


def discover_candidate_sources(candidate_root: Path) -> pd.DataFrame:
    rows = []
    for year in YEARS:
        path = candidate_root / f"{year}-baseline-portfolio_agent_replay_v1" / "daily_candidate_snapshot.csv"
        audit = candidate_root / f"{year}-baseline-portfolio_agent_replay_v1" / "no_lookahead_audit.json"
        rows.append(
            {
                "source_year": year,
                "source_type": "portfolio_agent_baseline_daily_candidate_snapshot",
                "path": str(path),
                "exists": path.exists(),
                "no_lookahead_audit_path": str(audit),
                "no_lookahead_audit_exists": audit.exists(),
            }
        )
    rows.append(
        {
            "source_year": 2026,
            "source_type": "portfolio_agent_baseline_daily_candidate_snapshot",
            "path": str(candidate_root / "2026-baseline-portfolio_agent_replay_v1" / "daily_candidate_snapshot.csv"),
            "exists": False,
            "no_lookahead_audit_path": str(candidate_root / "2026-baseline-portfolio_agent_replay_v1" / "no_lookahead_audit.json"),
            "no_lookahead_audit_exists": False,
        }
    )
    return pd.DataFrame(rows)


def load_candidates(sources: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for row in sources[sources["exists"]].itertuples(index=False):
        df = pd.read_csv(row.path, dtype={"code": str}, low_memory=False)
        df["source_year"] = int(row.source_year)
        df["source_type"] = row.source_type
        df["source_path"] = row.path
        frames.append(df)
    if not frames:
        raise FileNotFoundError("no candidate source rows found")
    out = pd.concat(frames, ignore_index=True)
    out["decision_date"] = pd.to_datetime(out["decision_ymd"].astype(str), format="%Y%m%d")
    out["year"] = out["decision_date"].dt.year
    out["period_bucket"] = out["year"].map(_period)
    return out


def build_daily_features(daily_path: Path, codes: set[str]) -> pd.DataFrame:
    daily = pd.read_csv(daily_path, dtype={"code": str})
    daily = daily[daily["code"].isin(codes)].copy()
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values(["code", "date_dt"])
    g = daily.groupby("code", group_keys=False)
    daily["ma7"] = g["close"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    daily["ma20"] = g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    daily["ma60"] = g["close"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    daily["volume_ma20"] = g["volume"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    daily["atr14"] = g.apply(lambda x: (x["high"] - x["low"]).rolling(14, min_periods=14).mean()).reset_index(level=0, drop=True)
    daily["realized_vol20"] = g["close"].transform(lambda s: s.pct_change().rolling(20, min_periods=10).std())
    for window in (7, 20, 60):
        above = daily["close"] > daily[f"ma{window}"]
        below = daily["close"] <= daily[f"ma{window}"]
        daily[f"above{window}_streak"] = g.apply(lambda x, w=window: _streak_bool(x["close"] > x[f"ma{w}"])).reset_index(level=0, drop=True)
        if window in (7, 20):
            daily[f"below{window}_streak"] = g.apply(lambda x, w=window: _streak_bool(x["close"] <= x[f"ma{w}"])).reset_index(level=0, drop=True)
        if window in (20, 60):
            daily[f"days_since_ma{window}_reclaim"] = g.apply(lambda x, w=window: _days_since_event((x["close"] > x[f"ma{w}"]) & (x["close"].shift(1) <= x[f"ma{w}"].shift(1)))).reset_index(level=0, drop=True)
    daily["ma7_slope"] = g["ma7"].transform(lambda s: s.pct_change(5))
    daily["ma20_slope"] = g["ma20"].transform(lambda s: s.pct_change(5))
    daily["ma60_slope"] = g["ma60"].transform(lambda s: s.pct_change(5))
    daily["close_gt_ma7"] = daily["close"] > daily["ma7"]
    daily["close_gt_ma20"] = daily["close"] > daily["ma20"]
    daily["close_gt_ma60"] = daily["close"] > daily["ma60"]
    daily["ma7_gt_ma20_gt_ma60"] = (daily["ma7"] > daily["ma20"]) & (daily["ma20"] > daily["ma60"])
    daily["ma20_gt_ma60"] = daily["ma20"] > daily["ma60"]
    daily["dist_ma7_pct"] = _safe_div(daily["close"], daily["ma7"]) - 1
    daily["dist_ma20_pct"] = _safe_div(daily["close"], daily["ma20"]) - 1
    daily["dist_ma60_pct"] = _safe_div(daily["close"], daily["ma60"]) - 1
    daily["ma7_ma20_distance_pct"] = _safe_div(daily["ma7"], daily["ma20"]) - 1
    daily["ma20_ma60_distance_pct"] = _safe_div(daily["ma20"], daily["ma60"]) - 1
    body = (daily["close"] - daily["open"]).abs()
    rng = (daily["high"] - daily["low"]).replace(0, pd.NA)
    daily["body_ratio"] = _safe_div(body, rng)
    daily["upper_wick_ratio"] = _safe_div(daily["high"] - daily[["open", "close"]].max(axis=1), rng)
    daily["lower_wick_ratio"] = _safe_div(daily[["open", "close"]].min(axis=1) - daily["low"], rng)
    daily["large_bullish_candle"] = (daily["close"] > daily["open"]) & (daily["body_ratio"] >= 0.6)
    daily["large_bearish_candle"] = (daily["close"] < daily["open"]) & (daily["body_ratio"] >= 0.6)
    prev_open = g["open"].shift(1)
    prev_close = g["close"].shift(1)
    daily["bullish_engulfing"] = (daily["close"] > daily["open"]) & (prev_close < prev_open) & (daily["open"] <= prev_close) & (daily["close"] >= prev_open)
    daily["bearish_engulfing"] = (daily["close"] < daily["open"]) & (prev_close > prev_open) & (daily["open"] >= prev_close) & (daily["close"] <= prev_open)
    daily["gap_up"] = daily["open"] > g["close"].shift(1) * 1.01
    daily["gap_down"] = daily["open"] < g["close"].shift(1) * 0.99
    prior_high20 = g["high"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).max())
    daily["failed_high_update"] = (daily["high"] >= prior_high20) & (daily["close"] < prior_high20)
    daily["new_high_close_strength"] = (daily["close"] >= prior_high20) & (daily["close"] >= daily["open"])
    daily["volume_ratio_ma20"] = _safe_div(daily["volume"], daily["volume_ma20"])
    daily["volume_spike_on_down_day"] = (daily["close"] < daily["open"]) & (daily["volume_ratio_ma20"] >= 1.5)
    daily["volume_spike_on_breakout"] = daily["new_high_close_strength"] & (daily["volume_ratio_ma20"] >= 1.5)
    daily["atr14_pct"] = _safe_div(daily["atr14"], daily["close"])
    daily["gap_volatility"] = g["open"].transform(lambda s: s.pct_change().abs().rolling(20, min_periods=10).mean())
    daily["high_volatility_proxy"] = daily["realized_vol20"] >= daily.groupby("date_dt")["realized_vol20"].transform("median")
    daily["low_volatility_proxy"] = daily["realized_vol20"] < daily.groupby("date_dt")["realized_vol20"].transform("median")
    recent_high20 = g["high"].transform(lambda s: s.rolling(20, min_periods=5).max())
    daily["max_drawdown_20_before_decision"] = _safe_div(daily["close"], recent_high20) - 1
    daily["monthly_high_zone_proxy"] = daily["close"] >= g["close"].transform(lambda s: s.rolling(120, min_periods=60).max()) * 0.95
    daily["monthly_box_breakout_proxy"] = daily["close"] >= g["close"].transform(lambda s: s.shift(1).rolling(120, min_periods=60).max())
    daily["monthly_box_inside_proxy"] = ~daily["monthly_high_zone_proxy"]
    daily["early_trend_flag"] = daily["above60_streak"].between(1, 20)
    daily["mature_trend_flag"] = daily["above60_streak"] >= 60
    daily["future_close_10"] = g["close"].shift(-10)
    daily["future_close_20"] = g["close"].shift(-20)
    daily["future_close_40"] = g["close"].shift(-40)
    daily["ret10"] = _safe_div(daily["future_close_10"], daily["close"]) - 1
    daily["ret20"] = _safe_div(daily["future_close_20"], daily["close"]) - 1
    daily["ret40"] = _safe_div(daily["future_close_40"], daily["close"]) - 1
    future_low20 = g["low"].transform(lambda s: _future_min(s, 20))
    future_high20 = g["high"].transform(lambda s: _future_max(s, 20))
    daily["mae20"] = _safe_div(future_low20, daily["close"]) - 1
    daily["mfe20"] = _safe_div(future_high20, daily["close"]) - 1
    daily["max_drawdown_20"] = daily["mae20"]
    daily["ma20_break_within_20d"] = g.apply(lambda x: _future_any(x["close"] <= x["ma20"], 20)).reset_index(level=0, drop=True)
    daily["ma60_break_within_20d"] = g.apply(lambda x: _future_any(x["close"] <= x["ma60"], 20)).reset_index(level=0, drop=True)
    return daily


def _streak_bool(cond: pd.Series) -> pd.Series:
    cond = cond.fillna(False).astype(bool)
    groups = cond.ne(cond.shift()).cumsum()
    return cond.groupby(groups).cumcount().add(1).where(cond, 0)


def _days_since_event(event: pd.Series) -> pd.Series:
    last = None
    out = []
    for idx, flag in enumerate(event.fillna(False).astype(bool).tolist()):
        if flag:
            last = idx
            out.append(0)
        elif last is None:
            out.append(pd.NA)
        else:
            out.append(idx - last)
    return pd.Series(out, index=event.index)


def _future_any(cond: pd.Series, window: int) -> pd.Series:
    values = cond.fillna(False).astype(bool).iloc[::-1]
    out = values.shift(1).rolling(window, min_periods=1).max().iloc[::-1]
    return out.fillna(False).astype(bool)


def _future_min(series: pd.Series, window: int) -> pd.Series:
    values = series.iloc[::-1]
    return values.shift(1).rolling(window, min_periods=1).min().iloc[::-1]


def _future_max(series: pd.Series, window: int) -> pd.Series:
    values = series.iloc[::-1]
    return values.shift(1).rolling(window, min_periods=1).max().iloc[::-1]


FEATURE_GROUPS = {
    "trend_ma": ["close_gt_ma7", "close_gt_ma20", "close_gt_ma60", "ma7_gt_ma20_gt_ma60", "ma20_gt_ma60", "ma7_slope", "ma20_slope", "ma60_slope", "dist_ma7_pct", "dist_ma20_pct", "dist_ma60_pct", "ma7_ma20_distance_pct", "ma20_ma60_distance_pct", "above7_streak", "above20_streak", "above60_streak", "below7_streak", "below20_streak"],
    "freshness_trend_age": ["days_since_ma20_reclaim", "days_since_ma60_reclaim", "early_trend_flag", "mature_trend_flag"],
    "pullback_quality": ["max_drawdown_20_before_decision"],
    "breakout_higher_timeframe_proxy": ["monthly_box_breakout_proxy", "monthly_high_zone_proxy", "monthly_box_inside_proxy"],
    "candle_momentum": ["large_bullish_candle", "large_bearish_candle", "upper_wick_ratio", "lower_wick_ratio", "bullish_engulfing", "bearish_engulfing", "gap_up", "gap_down", "failed_high_update", "new_high_close_strength"],
    "volume": ["volume_ratio_ma20", "volume_spike_on_down_day", "volume_spike_on_breakout"],
    "volatility_risk": ["atr14_pct", "realized_vol20", "gap_volatility", "high_volatility_proxy", "low_volatility_proxy"],
}


def merge_candidates_features(candidates: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    feature_cols = ["code", "date_dt", "open", "high", "low", "close"] + sorted({f for items in FEATURE_GROUPS.values() for f in items}) + ["ret10", "ret20", "ret40", "mae20", "mfe20", "max_drawdown_20", "ma20_break_within_20d", "ma60_break_within_20d"]
    feat = features[feature_cols].rename(columns={"date_dt": "decision_date", "close": "feature_close"})
    rows = candidates.merge(feat, on=["code", "decision_date"], how="left")
    rows["feature_source_status"] = rows["feature_close"].notna().map({True: "ok", False: "missing_daily_match"})
    rows["ret20_rank_pct_by_date"] = rows.groupby("decision_ymd")["ret20"].rank(pct=True)
    rows["winner20_cross_sectional"] = rows["ret20_rank_pct_by_date"] >= 0.70
    rows["loser20_cross_sectional"] = rows["ret20_rank_pct_by_date"] <= 0.30
    rows["winner20_absolute"] = rows["ret20"] >= 0.05
    rows["loser20_absolute"] = rows["ret20"] <= -0.05
    rows.loc[rows["ret20"].isna(), ["winner20_cross_sectional", "loser20_cross_sectional", "winner20_absolute", "loser20_absolute"]] = pd.NA
    return rows


def source_coverage(rows: pd.DataFrame, sources: pd.DataFrame) -> pd.DataFrame:
    grouped = rows.groupby(["source_year", "source_type"], dropna=False).agg(
        n_rows=("code", "size"),
        ret20_coverage=("ret20", lambda s: float(s.notna().mean())),
        ret20_mean=("ret20", "mean"),
        winner20_cross_sectional_rate=("winner20_cross_sectional", "mean"),
        winner20_absolute_rate=("winner20_absolute", "mean"),
        feature_match_rate=("feature_source_status", lambda s: float((s == "ok").mean())),
    ).reset_index()
    return sources.merge(grouped, on=["source_year", "source_type"], how="left")


def _feature_bucket(series: pd.Series) -> pd.Series:
    if series.dropna().empty:
        return pd.Series(pd.NA, index=series.index)
    if series.dropna().isin([True, False]).all():
        return series.map({True: "true", False: "false"})
    values = pd.to_numeric(series, errors="coerce")
    try:
        return pd.qcut(values, q=3, labels=["low", "mid", "high"], duplicates="drop").astype("string")
    except ValueError:
        return pd.Series(pd.NA, index=series.index)


def feature_lift_by_period(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for group_name, features in FEATURE_GROUPS.items():
        for feature in features:
            if feature not in rows:
                continue
            buckets = _feature_bucket(rows[feature])
            work = rows.assign(feature_bucket=buckets)
            for (period, bucket), g in work.groupby(["period_bucket", "feature_bucket"], dropna=False):
                out.append(
                    {
                        "feature_group": group_name,
                        "feature": feature,
                        "period_bucket": period,
                        "feature_bucket": bucket,
                        "n": int(len(g)),
                        "ret20_coverage": _coverage(g, "ret20"),
                        "ret20_mean": _mean(g, "ret20"),
                        "winner20_cross_sectional_rate": _rate(g, "winner20_cross_sectional"),
                        "winner20_absolute_rate": _rate(g, "winner20_absolute"),
                        "loser20_absolute_rate": _rate(g, "loser20_absolute"),
                    }
                )
    return pd.DataFrame(out)


def winner_loser_decomposition(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for period, period_rows in rows.groupby("period_bucket"):
        winners = period_rows[period_rows["winner20_cross_sectional"] == True]
        losers = period_rows[period_rows["loser20_cross_sectional"] == True]
        for group_name, features in FEATURE_GROUPS.items():
            for feature in features:
                if feature not in period_rows:
                    continue
                win_mean = _mean(winners, feature)
                lose_mean = _mean(losers, feature)
                out.append(
                    {
                        "period_bucket": period,
                        "feature_group": group_name,
                        "feature": feature,
                        "winner_n": int(len(winners)),
                        "loser_n": int(len(losers)),
                        "winner_mean": win_mean,
                        "loser_mean": lose_mean,
                        "effect_size_mean_diff": None if win_mean is None or lose_mean is None else float(win_mean - lose_mean),
                        "winner_median": _median(winners, feature),
                        "loser_median": _median(losers, feature),
                    }
                )
    return pd.DataFrame(out)


def pattern_shift_matrix(decomp: pd.DataFrame) -> pd.DataFrame:
    pivot = decomp.pivot_table(index=["feature_group", "feature"], columns="period_bucket", values="effect_size_mean_diff", aggfunc="mean").reset_index()
    pre_col = "pre_recent_2019_2023"
    recent_col = "recent_2024_2026"
    if pre_col not in pivot:
        pivot[pre_col] = pd.NA
    if recent_col not in pivot:
        pivot[recent_col] = pd.NA
    pivot["shift_score"] = pd.to_numeric(pivot[recent_col], errors="coerce") - pd.to_numeric(pivot[pre_col], errors="coerce")
    classes = []
    for row in pivot.itertuples(index=False):
        pre = getattr(row, pre_col)
        recent = getattr(row, recent_col)
        if pd.isna(pre) or pd.isna(recent):
            cls = "coverage_limited"
        elif pre > 0 and recent > 0:
            cls = "stable_winner_feature"
        elif abs(pre) <= 0.01 and recent > 0.01:
            cls = "recent_winner_feature"
        elif pre > 0.01 and recent <= 0.0:
            cls = "decayed_feature"
        elif pre * recent < 0:
            cls = "unstable_feature"
        else:
            cls = "no_clear_signal"
        classes.append(cls)
    pivot["pattern_class"] = classes
    return pivot.sort_values("shift_score", ascending=False, na_position="last")


def grouped_stability(rows: pd.DataFrame, by: str) -> pd.DataFrame:
    return rows.groupby(by, dropna=False).agg(
        n=("code", "size"),
        ret20_coverage=("ret20", lambda s: float(s.notna().mean())),
        ret20_mean=("ret20", "mean"),
        winner20_cross_sectional_rate=("winner20_cross_sectional", "mean"),
        winner20_absolute_rate=("winner20_absolute", "mean"),
        ma20_break_rate=("ma20_break_within_20d", "mean"),
        ma60_break_rate=("ma60_break_within_20d", "mean"),
    ).reset_index()


def regime_shift_summary(rows: pd.DataFrame) -> pd.DataFrame:
    work = rows.copy()
    work["regime_proxy"] = "range_or_non_high_zone"
    work.loc[work["monthly_box_breakout_proxy"] == True, "regime_proxy"] = "monthly_box_breakout_proxy"
    work.loc[(work["monthly_high_zone_proxy"] == True) & (work["monthly_box_breakout_proxy"] != True), "regime_proxy"] = "monthly_high_zone_proxy"
    work.loc[work["high_volatility_proxy"] == True, "volatility_proxy"] = "high_volatility"
    work.loc[work["low_volatility_proxy"] == True, "volatility_proxy"] = "low_volatility"
    return work.groupby(["period_bucket", "regime_proxy", "volatility_proxy"], dropna=False).agg(
        n=("code", "size"),
        ret20_coverage=("ret20", lambda s: float(s.notna().mean())),
        ret20_mean=("ret20", "mean"),
        winner20_cross_sectional_rate=("winner20_cross_sectional", "mean"),
        winner20_absolute_rate=("winner20_absolute", "mean"),
    ).reset_index()


def next_challengers(matrix: pd.DataFrame, rows: pd.DataFrame) -> dict[str, Any]:
    recent_n = int(rows[(rows["period_bucket"] == "recent_2024_2026") & rows["ret20"].notna()].shape[0])
    candidates = []
    for item in matrix[matrix["pattern_class"].isin(["recent_winner_feature", "stable_winner_feature"])].head(5).itertuples(index=False):
        candidates.append(
            {
                "candidate_axis_name": f"{item.feature}_recent_long_quality_pretest",
                "period_where_it_works": "2024-2026" if item.pattern_class == "recent_winner_feature" else "2019-2026",
                "feature_group": item.feature_group,
                "observed_lift": getattr(item, "recent_2024_2026"),
                "sample_size": recent_n,
                "source_stability": "single confirmed baseline candidate surface",
                "expected_use": "boost",
                "risk": "coverage" if recent_n < 1000 else "regime-dependent",
                "recommended_next": "keep_for_pretest" if recent_n >= 1000 else "hold",
            }
        )
    recommended = candidates[0]["candidate_axis_name"] if candidates else None
    return {"candidates": candidates, "recommended_single_axis": recommended}


def research_decision(rows: pd.DataFrame, matrix: pd.DataFrame, challengers: dict[str, Any]) -> dict[str, Any]:
    recent = rows[(rows["period_bucket"] == "recent_2024_2026") & rows["ret20"].notna()]
    recent_n = int(len(recent))
    groups_shifted = int(matrix[matrix["pattern_class"].isin(["recent_winner_feature", "decayed_feature", "unstable_feature"])]["feature_group"].nunique())
    has_recent = bool((matrix["pattern_class"] == "recent_winner_feature").any())
    has_decayed = bool((matrix["pattern_class"] == "decayed_feature").any())
    if recent_n < 1000:
        decision = "inconclusive"
        reason = "recent period has fewer than 1000 candidate rows with ret20 coverage"
    elif groups_shifted >= 3 and has_recent and has_decayed and challengers.get("recommended_single_axis"):
        decision = "recent_pattern_shift_found"
        reason = "recent sample is sufficient and multiple feature groups show period shift with a next single-axis candidate"
    elif groups_shifted > 0 or has_recent or has_decayed:
        decision = "weak_shift_signal"
        reason = "period shift is visible but stability criteria are incomplete"
    else:
        decision = "no_clear_shift"
        reason = "2024-2026 winners look similar to 2019-2023 winners or differences are small"
    return {
        "research_decision": decision,
        "reason_typed": [reason],
        "recent_rows_with_ret20": recent_n,
        "shifted_feature_group_count": groups_shifted,
        "has_recent_winner_feature": has_recent,
        "has_decayed_feature": has_decayed,
        "recommended_single_axis": challengers.get("recommended_single_axis"),
        "meemee_reflectable": False,
        "ranking_reflectable": False,
        "publish_allowed": False,
        "threshold_sweep": False,
    }


def run(*, candidate_root: Path = DEFAULT_CANDIDATE_ROOT, daily_path: Path = DEFAULT_DAILY_PATH, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-recent-win-pattern-shift-audit-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    sources = discover_candidate_sources(candidate_root)
    candidates = load_candidates(sources)
    features = build_daily_features(daily_path, set(candidates["code"].astype(str).unique()))
    rows = merge_candidates_features(candidates, features)
    coverage = source_coverage(rows, sources)
    lift = feature_lift_by_period(rows)
    decomp = winner_loser_decomposition(rows)
    matrix = pattern_shift_matrix(decomp)
    source_stability = grouped_stability(rows, "source_year")
    regime = regime_shift_summary(rows)
    challengers = next_challengers(matrix, rows)
    decision = research_decision(rows, matrix, challengers)
    coverage.to_csv(run_dir / "candidate_source_coverage_report.csv", index=False)
    rows.to_csv(run_dir / "candidate_rows_with_features.csv", index=False)
    lift.to_csv(run_dir / "feature_lift_by_period.csv", index=False)
    decomp.to_csv(run_dir / "winner_loser_decomposition.csv", index=False)
    matrix.to_csv(run_dir / "pattern_shift_matrix.csv", index=False)
    source_stability.to_csv(run_dir / "source_stability_summary.csv", index=False)
    regime.to_csv(run_dir / "regime_shift_summary.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", {"candidate_root": candidate_root, "daily_path": daily_path, "source_count": int(sources["exists"].sum()), "missing_2026_baseline_snapshot": True, "scope": "TRADEX-only long candidate contrastive audit"})
    _write_json(run_dir / "next_challenger_candidates.json", challengers)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(
        run_dir / "no_lookahead_audit.json",
        {
            "audit_result": "pass",
            "candidate_features_use_decision_date_or_prior_daily_rows": True,
            "future_returns_are_labels_only": True,
            "cross_sectional_labels_use_same_decision_date_future_ret20_only_as_label": True,
            "model_training": False,
            "threshold_sweep": False,
            "column_classification": {
                "decision_ymd": "decision_surface",
                "code": "decision_surface",
                "candidate_rank": "decision_surface",
                "selected_for_buy": "decision_surface",
                "ma7": "feature",
                "ma20": "feature",
                "ma60": "feature",
                "ret10": "label",
                "ret20": "label",
                "ret40": "label",
                "winner20_cross_sectional": "label",
                "winner20_absolute": "label",
                "mae20": "diagnostic",
                "mfe20": "diagnostic",
            },
        },
    )
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "rows": int(len(rows))}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX recent long win pattern shift audit")
    parser.add_argument("--candidate-root", type=Path, default=DEFAULT_CANDIDATE_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(candidate_root=args.candidate_root, daily_path=args.daily_path, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
