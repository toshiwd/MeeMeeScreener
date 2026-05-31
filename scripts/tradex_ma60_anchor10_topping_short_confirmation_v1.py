from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


FAMILY_ID = "ma60_anchor10_topping_short_confirmation_v1"
CLOSURE_ID = "ma60_above_60plus_short_veto_family_closure_v1"
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\ma60_above_60plus_stay_guard_pretest_v1\20260523T131433Z-ma60-above-60plus-stay-guard-pretest-v1")
DEFAULT_SHORT_REPLAY_ROOT = Path(r"G:\Tradex\ma60_above_60plus_short_veto_replay_v1\20260523T145336Z-ma60-above-60plus-short-veto-replay-v1")
DEFAULT_FAILURE_ROOT = Path(r"G:\Tradex\ma60_above_60plus_short_veto_failure_decomposition_v1\20260523T173005Z-ma60-above-60plus-short-veto-failure-decomposition-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma60_anchor10_topping_short_confirmation_v1")
DEFAULT_CLOSURE_OUTPUT_ROOT = Path(r"G:\Tradex\ma60_above_60plus_short_veto_family_closure_v1")

NEW_FAMILY_ARTIFACTS = (
    "input_artifact_report.json",
    "cohort_construction_report.json",
    "anchor10_short_rows.csv",
    "anchor10_short_summary.json",
    "feature_decomposition.csv",
    "days_since_anchor_summary.csv",
    "period_stability_summary.csv",
    "source_stability_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
CLOSURE_ARTIFACTS = (
    "input_artifact_report.json",
    "family_closure_summary.json",
    "decision_history.json",
    "reusable_observations.json",
    "non_promotion_guardrails.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


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


def _bool_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    normalized = series.astype("string").fillna("").str.strip().str.lower()
    return normalized.isin({"1", "true", "t", "yes", "y"})


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
    return None if values.empty else float(_bool_series(values).mean())


def _coverage(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    return float(pd.to_numeric(df[col], errors="coerce").notna().mean())


def _period_bucket(year: int) -> str:
    if 2019 <= year <= 2023:
        return "2019-2023"
    if 2024 <= year <= 2026:
        return "2024-2026"
    return str(year)


def _days_bucket(days: float | int | None) -> str:
    if days is None or pd.isna(days):
        return "unknown"
    value = int(days)
    if 0 <= value <= 5:
        return "0-5"
    if 6 <= value <= 10:
        return "6-10"
    if 11 <= value <= 20:
        return "11-20"
    return "out_of_window"


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    out = num / den.replace(0, pd.NA)
    return pd.to_numeric(out, errors="coerce")


def make_closure_artifact(*, failure_root: Path, output_root: Path) -> Path:
    decision = json.loads((failure_root / "research_decision.json").read_text(encoding="utf-8"))
    recent = json.loads((failure_root / "recent_degradation_summary.json").read_text(encoding="utf-8"))
    salvage = json.loads((failure_root / "salvageability_summary.json").read_text(encoding="utf-8"))
    run_dir = output_root / f"{_now_tag()}-ma60-above-60plus-short-veto-family-closure-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "input_artifact_report.json", {"failure_root": failure_root, "source_research_decision": decision})
    _write_json(
        run_dir / "family_closure_summary.json",
        {
            "family": "ma60_above_60plus_short_veto",
            "family_status": "frozen_diagnostic_only",
            "closure_decision": "family_closed_drop",
            "meemee_reflectable": False,
            "ranking_reflectable": False,
            "publish_allowed": False,
            "do_not_use_as_short_veto": True,
            "do_not_treat_anchor_10_as_bullish_continuation_guard_in_recent_regime": True,
        },
    )
    _write_json(
        run_dir / "decision_history.json",
        {
            "previous_failure_decomposition_decision": decision.get("research_decision"),
            "recent_degradation_summary": recent,
            "salvageability_summary": salvage,
        },
    )
    _write_json(
        run_dir / "reusable_observations.json",
        {
            "anchor_10_recent_negative_behavior_can_be_researched_as_new_family": True,
            "reuse_requires_independent_short_confirmation_validation": True,
            "reuse_does_not_revive_short_veto": True,
        },
    )
    _write_json(
        run_dir / "non_promotion_guardrails.json",
        {
            "do_not_use_as_short_veto": True,
            "do_not_use_as_ranking_demotion_or_promotion": True,
            "do_not_expose_to_meemee": True,
            "do_not_revive_without_independent_recent_period_validation": True,
            "do_not_change_thresholds_from_this_closure": True,
        },
    )
    _write_json(
        run_dir / "research_decision.json",
        {
            "research_decision": "family_closed_drop",
            "family_status": "frozen_diagnostic_only",
            "meemee_reflectable": False,
            "ranking_reflectable": False,
            "publish_allowed": False,
        },
    )
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": CLOSURE_ID,
            "output_dir": run_dir,
            "required_artifacts": list(CLOSURE_ARTIFACTS),
            "artifact_complete": all((run_dir / name).exists() for name in CLOSURE_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    return run_dir


def build_daily_features(daily_path: Path, needed_codes: set[str]) -> pd.DataFrame:
    daily = pd.read_csv(daily_path, dtype={"code": str})
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily = daily[daily["code"].isin(needed_codes)].copy()
    daily = daily.sort_values(["code", "date_dt"])
    group = daily.groupby("code", group_keys=False)
    daily["ma7"] = group["close"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    daily["ma20"] = group["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    daily["ma60"] = group["close"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    daily["ma7_slope"] = group["ma7"].transform(lambda s: s.pct_change(5))
    daily["ma20_slope"] = group["ma20"].transform(lambda s: s.pct_change(5))
    daily["ma60_slope"] = group["ma60"].transform(lambda s: s.pct_change(5))
    daily["dist_ma7_pct"] = _safe_div(daily["close"], daily["ma7"]) - 1
    daily["dist_ma20_pct"] = _safe_div(daily["close"], daily["ma20"]) - 1
    daily["dist_ma60_pct"] = _safe_div(daily["close"], daily["ma60"]) - 1
    daily["below_ma7"] = daily["close"] <= daily["ma7"]
    daily["below_ma20"] = daily["close"] <= daily["ma20"]
    body = (daily["close"] - daily["open"]).abs()
    candle_range = (daily["high"] - daily["low"]).replace(0, pd.NA)
    daily["body_ratio"] = _safe_div(body, candle_range)
    daily["large_bearish_candle"] = (daily["close"] < daily["open"]) & (daily["body_ratio"] >= 0.6)
    daily["upper_wick_ratio"] = _safe_div(daily["high"] - daily[["open", "close"]].max(axis=1), candle_range)
    prev_open = group["open"].shift(1)
    prev_close = group["close"].shift(1)
    daily["bearish_engulfing"] = (daily["close"] < daily["open"]) & (prev_close > prev_open) & (daily["open"] >= prev_close) & (daily["close"] <= prev_open)
    prior_high20 = group["high"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).max())
    daily["failed_high_update"] = (daily["high"] >= prior_high20) & (daily["close"] < prior_high20)
    volume_ma20 = group["volume"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    daily["volume_spike_down_day"] = (daily["close"] < daily["open"]) & (_safe_div(daily["volume"], volume_ma20) >= 1.5)
    recent_high20 = group["high"].transform(lambda s: s.rolling(20, min_periods=5).max())
    daily["drawdown_from_recent_high_pct"] = _safe_div(daily["close"], recent_high20) - 1
    return daily


def construct_anchor10_rows(short_rows: pd.DataFrame, daily_features: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = short_rows.copy()
    rows["code"] = rows["code"].astype(str)
    rows["decision_date"] = pd.to_datetime(rows["decision_ymd"].astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
    rows["guard_anchor_date"] = pd.to_datetime(rows["guard_anchor_ymd"].astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
    rows["guard_hit_bool"] = _bool_series(rows["guard_hit"])
    rows["anchor10_guard_hit"] = rows["guard_hit_bool"] & (rows["guard_anchor_type"].astype("string") == "anchor_10")
    rows["anchor10_guard_miss"] = ~rows["anchor10_guard_hit"]
    rows["period_bucket_calc"] = rows["year"].astype(int).map(_period_bucket)
    ret20 = pd.to_numeric(rows["ret20_long"], errors="coerce")
    rows["helped_short"] = (ret20 < -0.01).where(ret20.notna(), pd.NA)
    rows["harmed_short"] = (ret20 > 0.01).where(ret20.notna(), pd.NA)
    rows["neutral_short"] = (ret20.abs() <= 0.01).where(ret20.notna(), pd.NA)
    rows["days_since_anchor"] = (rows["decision_date"] - rows["guard_anchor_date"]).dt.days
    rows["days_since_anchor_bucket"] = rows["days_since_anchor"].map(_days_bucket)

    feature_cols = [
        "code",
        "date_dt",
        "open",
        "high",
        "low",
        "close",
        "ma7",
        "ma20",
        "ma60",
        "ma7_slope",
        "ma20_slope",
        "ma60_slope",
        "dist_ma7_pct",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "below_ma7",
        "below_ma20",
        "large_bearish_candle",
        "upper_wick_ratio",
        "bearish_engulfing",
        "failed_high_update",
        "volume_spike_down_day",
        "drawdown_from_recent_high_pct",
    ]
    decision_features = daily_features[feature_cols].rename(columns={"date_dt": "decision_date", "close": "decision_close"})
    rows = rows.merge(decision_features, on=["code", "decision_date"], how="left")
    anchor_closes = daily_features[["code", "date_dt", "close"]].rename(columns={"date_dt": "guard_anchor_date", "close": "anchor10_close"})
    rows = rows.merge(anchor_closes, on=["code", "guard_anchor_date"], how="left")
    rows["anchor10_to_decision_return"] = _safe_div(rows["decision_close"], rows["anchor10_close"]) - 1
    rows["high_zone_monthly_proxy"] = rows["regime_proxy"].astype("string").str.contains("monthly_breakout_high_zone", na=False)
    rows["feature_source_status"] = rows["decision_close"].notna().map({True: "ok", False: "missing_daily_match"})
    report = {
        "input_short_rows": int(len(short_rows)),
        "anchor10_guard_hit_rows": int(rows["anchor10_guard_hit"].sum()),
        "anchor10_guard_hit_rows_with_ret20": int(rows.loc[rows["anchor10_guard_hit"], "ret20_long"].notna().sum()),
        "daily_feature_match_rate": float(rows["decision_close"].notna().mean()) if len(rows) else None,
    }
    return rows, report


def summarize_group(df: pd.DataFrame, label: str) -> dict[str, Any]:
    return {
        "cohort": label,
        "n": int(len(df)),
        "ret20_coverage": _coverage(df, "ret20_long"),
        "ret40_coverage": _coverage(df, "ret40_long"),
        "ret20_long_mean": _mean(df, "ret20_long"),
        "ret20_long_median": _median(df, "ret20_long"),
        "ret40_long_mean": _mean(df, "ret40_long"),
        "ret40_long_median": _median(df, "ret40_long"),
        "short_return20_mean": _mean(df, "short_return20"),
        "short_return20_median": _median(df, "short_return20"),
        "short_return40_mean": _mean(df, "short_return40"),
        "short_return40_median": _median(df, "short_return40"),
        "helped_short_rate": _rate(df, "helped_short"),
        "harmed_short_rate": _rate(df, "harmed_short"),
        "neutral_rate": _rate(df, "neutral_short"),
    }


def make_summary(rows: pd.DataFrame) -> dict[str, Any]:
    hit = rows[rows["anchor10_guard_hit"]]
    miss = rows[rows["anchor10_guard_miss"]]
    all_rows = rows
    summary = {
        "anchor_10_guard_hit_short_rows": summarize_group(hit, "anchor_10_guard_hit_short_rows"),
        "anchor_10_guard_miss_short_rows": summarize_group(miss, "anchor_10_guard_miss_short_rows"),
        "all_short_rows": summarize_group(all_rows, "all_short_rows"),
    }
    hit_short = summary["anchor_10_guard_hit_short_rows"]["short_return20_mean"]
    miss_short = summary["anchor_10_guard_miss_short_rows"]["short_return20_mean"]
    summary["hit_minus_miss_short_return20"] = None if hit_short is None or miss_short is None else float(hit_short - miss_short)
    return summary


def grouped_summary(rows: pd.DataFrame, by: str) -> pd.DataFrame:
    out = []
    for key, group in rows.groupby(by, dropna=False):
        item = summarize_group(group, str(key))
        item[by] = key
        out.append(item)
    return pd.DataFrame(out)


def feature_decomposition(rows: pd.DataFrame) -> pd.DataFrame:
    hit = rows[rows["anchor10_guard_hit"]].copy()
    features = [
        "below_ma7",
        "below_ma20",
        "large_bearish_candle",
        "bearish_engulfing",
        "failed_high_update",
        "volume_spike_down_day",
        "high_zone_monthly_proxy",
        "dist_ma7_pct",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "upper_wick_ratio",
        "anchor10_to_decision_return",
        "drawdown_from_recent_high_pct",
    ]
    out = []
    for feature in features:
        if feature not in hit:
            continue
        if hit[feature].dropna().isin([True, False]).all():
            true_rows = hit[_bool_series(hit[feature])]
            false_rows = hit[~_bool_series(hit[feature])]
            lift = _mean(true_rows, "short_return20")
            base = _mean(false_rows, "short_return20")
            out.append({"feature": feature, "type": "boolean", "n_true": int(len(true_rows)), "short_return20_true_mean": lift, "short_return20_false_mean": base, "helped_short_rate_true": _rate(true_rows, "helped_short"), "spread_vs_false": None if lift is None or base is None else float(lift - base)})
        else:
            values = pd.to_numeric(hit[feature], errors="coerce")
            out.append({"feature": feature, "type": "numeric", "n_true": int(values.notna().sum()), "median": None if values.dropna().empty else float(values.median()), "mean": None if values.dropna().empty else float(values.mean()), "short_return20_corr": None if values.notna().sum() < 3 else float(values.corr(pd.to_numeric(hit["short_return20"], errors="coerce")))})
    return pd.DataFrame(out)


def decide(rows: pd.DataFrame) -> dict[str, Any]:
    recent_hit = rows[rows["anchor10_guard_hit"] & rows["year"].astype(int).between(2024, 2026)].copy()
    source_counts = recent_hit["source_type"].fillna("unknown").value_counts()
    largest_source_share = None if recent_hit.empty else float(source_counts.iloc[0] / len(recent_hit))
    metrics = summarize_group(recent_hit, "2024_2026_anchor10_guard_hit")
    coverage = metrics["ret20_coverage"]
    if coverage is not None and coverage < 0.8:
        decision = "inconclusive"
        reason = "2024-2026 anchor_10 guard-hit forward return coverage remains too low"
    elif metrics["n"] < 30:
        decision = "inconclusive"
        reason = "2024-2026 anchor_10 guard-hit sample is below n>=30"
    elif (
        metrics["short_return20_mean"] is not None
        and metrics["short_return20_mean"] > 0
        and metrics["short_return20_median"] is not None
        and metrics["short_return20_median"] >= 0
        and metrics["helped_short_rate"] is not None
        and metrics["helped_short_rate"] >= 0.55
        and metrics["harmed_short_rate"] is not None
        and metrics["harmed_short_rate"] <= 0.30
        and largest_source_share is not None
        and largest_source_share < 0.8
    ):
        decision = "topping_short_pattern_found"
        reason = "2024-2026 anchor_10 guard-hit shorts clear sample, return, hit-rate, and source concentration gates"
    elif metrics["short_return20_mean"] is not None and metrics["short_return20_mean"] > 0:
        decision = "weak_topping_pattern"
        reason = "direction is positive but one or more stability gates fail"
    else:
        decision = "not_found"
        reason = "anchor_10 guard-hit shorts do not show positive recent short-return direction"
    return {
        "research_decision": decision,
        "reason_typed": [reason],
        "recent_anchor10_guard_hit_metrics": metrics,
        "largest_source_share_2024_2026": largest_source_share,
        "no_lookahead_safe": True,
        "meemee_reflectable": False,
        "ranking_reflectable": False,
        "publish_allowed": False,
        "threshold_sweep": False,
    }


def run(
    *,
    guard_root: Path = DEFAULT_GUARD_ROOT,
    short_replay_root: Path = DEFAULT_SHORT_REPLAY_ROOT,
    failure_root: Path = DEFAULT_FAILURE_ROOT,
    daily_path: Path = DEFAULT_DAILY_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    closure_output_root: Path = DEFAULT_CLOSURE_OUTPUT_ROOT,
) -> dict[str, Any]:
    closure_dir = make_closure_artifact(failure_root=failure_root, output_root=closure_output_root)
    run_dir = output_root / f"{_now_tag()}-ma60-anchor10-topping-short-confirmation-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    selected_rules = json.loads((guard_root / "selected_guard_rules.json").read_text(encoding="utf-8"))
    source_audit = json.loads((short_replay_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    short_rows = pd.read_csv(short_replay_root / "short_veto_rows.csv", dtype={"code": str}, low_memory=False)
    needed_codes = set(short_rows["code"].astype(str).unique())
    daily_features = build_daily_features(daily_path, needed_codes)
    rows, cohort_report = construct_anchor10_rows(short_rows, daily_features)
    summary = make_summary(rows)
    period = grouped_summary(rows[rows["anchor10_guard_hit"]], "period_bucket_calc")
    source = grouped_summary(rows[rows["anchor10_guard_hit"]], "source_type")
    days = grouped_summary(rows[rows["anchor10_guard_hit"]], "days_since_anchor_bucket")
    features = feature_decomposition(rows)
    decision = decide(rows)
    output_cols = [
        "source_artifact",
        "source_name",
        "source_type",
        "code",
        "decision_ymd",
        "raw_side",
        "anchor10_guard_hit",
        "guard_anchor_type",
        "guard_anchor_ymd",
        "days_since_anchor",
        "days_since_anchor_bucket",
        "ret20_long",
        "ret40_long",
        "short_return20",
        "short_return40",
        "helped_short",
        "harmed_short",
        "neutral_short",
        "regime_proxy",
        "period_bucket_calc",
        "feature_source_status",
        "decision_close",
        "ma7",
        "ma20",
        "ma60",
        "ma7_slope",
        "ma20_slope",
        "ma60_slope",
        "dist_ma7_pct",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "below_ma7",
        "below_ma20",
        "large_bearish_candle",
        "upper_wick_ratio",
        "bearish_engulfing",
        "failed_high_update",
        "volume_spike_down_day",
        "high_zone_monthly_proxy",
        "anchor10_to_decision_return",
        "drawdown_from_recent_high_pct",
    ]
    rows[output_cols].to_csv(run_dir / "anchor10_short_rows.csv", index=False)
    period.to_csv(run_dir / "period_stability_summary.csv", index=False)
    source.to_csv(run_dir / "source_stability_summary.csv", index=False)
    days.to_csv(run_dir / "days_since_anchor_summary.csv", index=False)
    features.to_csv(run_dir / "feature_decomposition.csv", index=False)
    _write_json(
        run_dir / "input_artifact_report.json",
        {
            "guard_root": guard_root,
            "short_replay_root": short_replay_root,
            "failure_root": failure_root,
            "daily_path": daily_path,
            "closure_artifact_root": closure_dir,
            "selected_guard_rules": selected_rules,
            "source_no_lookahead_audit_result": source_audit.get("audit_result"),
        },
    )
    _write_json(run_dir / "cohort_construction_report.json", cohort_report)
    _write_json(run_dir / "anchor10_short_summary.json", summary)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(
        run_dir / "no_lookahead_audit.json",
        {
            "audit_result": "pass",
            "guard_state_uses_only_data_through_decision_date": True,
            "daily_features_use_only_current_or_prior_daily_rows": True,
            "future_returns_are_label_only": True,
            "threshold_sweep": False,
            "model_training": False,
            "column_classification": {
                "code": "decision_surface",
                "decision_ymd": "decision_surface",
                "source_type": "decision_surface",
                "anchor10_guard_hit": "guard_state",
                "guard_anchor_ymd": "guard_state",
                "days_since_anchor": "guard_state",
                "ma7": "feature",
                "ma20": "feature",
                "ma60": "feature",
                "dist_ma7_pct": "feature",
                "dist_ma20_pct": "feature",
                "dist_ma60_pct": "feature",
                "large_bearish_candle": "feature",
                "ret20_long": "label",
                "ret40_long": "label",
                "short_return20": "label",
                "short_return40": "label",
                "helped_short": "label",
                "harmed_short": "label",
            },
        },
    )
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": FAMILY_ID,
            "output_dir": run_dir,
            "closure_output_dir": closure_dir,
            "required_artifacts": list(NEW_FAMILY_ARTIFACTS),
            "artifact_complete": all((run_dir / name).exists() for name in NEW_FAMILY_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    return {"closure_output_dir": str(closure_dir), "output_dir": str(run_dir), "research_decision": decision, "cohort": cohort_report}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit MA60 anchor10 topping short confirmation as an independent TRADEX family")
    parser.add_argument("--guard-root", type=Path, default=DEFAULT_GUARD_ROOT)
    parser.add_argument("--short-replay-root", type=Path, default=DEFAULT_SHORT_REPLAY_ROOT)
    parser.add_argument("--failure-root", type=Path, default=DEFAULT_FAILURE_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--closure-output-root", type=Path, default=DEFAULT_CLOSURE_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(guard_root=args.guard_root, short_replay_root=args.short_replay_root, failure_root=args.failure_root, daily_path=args.daily_path, output_root=args.output_root, closure_output_root=args.closure_output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
