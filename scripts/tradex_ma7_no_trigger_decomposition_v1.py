from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma7_no_trigger_decomposition_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\ma7_pullback_reclaim_entry_overlay_pretest_v1\20260524T062157Z-ma7-pullback-reclaim-entry-overlay-pretest-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma7_no_trigger_decomposition_v1")
REQUIRED_INPUTS = (
    "candidate_entry_overlay_rows.csv",
    "trigger_summary.csv",
    "loser_repair_summary.csv",
    "winner_damage_summary.csv",
    "no_trigger_diagnostics.csv",
    "period_stability_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "no_trigger_outcome_summary.csv",
    "missed_good_vs_skipped_bad_decomposition.csv",
    "no_pullback_momentum_profile.json",
    "skipped_bad_profile.json",
    "fallback_axis_candidates.json",
    "year_stability_summary.csv",
    "concentration_summary.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
YEARS = (2024, 2025, 2026)
TOPK = (5, 10, 20)
FEATURES = (
    "close_gt_ma7",
    "dist_ma7_pct",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "ma7_slope",
    "ma20_slope",
    "ma60_slope",
    "ma7_gt_ma20_gt_ma60",
    "above7_streak",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "days_since_ma60_reclaim",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "volume_ma20_ratio",
    "high_break_volume_count",
    "realized_vol20",
    "atr14_pct",
    "monthly_high_zone_proxy",
    "monthly_box_inside_proxy",
    "monthly_box_breakout_proxy",
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


def _mean(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.median())


def _std(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if len(s) < 2 else float(s.std(ddof=0))


def _rate(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = df[col].dropna()
    if s.empty:
        return None
    return float(s.astype(bool).mean())


def _effect(a: pd.DataFrame, b: pd.DataFrame, col: str) -> float | None:
    diff = None if _mean(a, col) is None or _mean(b, col) is None else _mean(a, col) - _mean(b, col)  # type: ignore[operator]
    sa, sb = _std(a, col), _std(b, col)
    if diff is None or sa is None or sb is None:
        return None
    pooled = math.sqrt((sa * sa + sb * sb) / 2)
    return None if pooled == 0 else float(diff / pooled)


def _streak_from_flag(flag: pd.Series) -> pd.Series:
    values = []
    count = 0
    for value in flag.fillna(False).astype(bool):
        count = count + 1 if value else 0
        values.append(count)
    return pd.Series(values, index=flag.index, dtype="float64")


def _days_since_reclaim(flag: pd.Series) -> pd.Series:
    out = []
    last_reclaim_pos: int | None = None
    prev = False
    for i, value in enumerate(flag.fillna(False).astype(bool).tolist()):
        if value and not prev:
            last_reclaim_pos = i
        out.append(None if last_reclaim_pos is None else i - last_reclaim_pos)
        prev = bool(value)
    return pd.Series(out, index=flag.index, dtype="float64")


def daily_features(daily_path: Path, codes: set[str]) -> pd.DataFrame:
    daily = pd.read_csv(daily_path, dtype={"code": str})
    daily = daily[daily["code"].isin(codes)].copy()
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily["decision_ymd"] = daily["date_dt"].dt.strftime("%Y%m%d").astype(int)
    for col in ("open", "high", "low", "close", "volume"):
        daily[col] = pd.to_numeric(daily[col], errors="coerce")
    daily = daily.sort_values(["code", "date_dt"]).drop_duplicates(["code", "date_dt"], keep="last")
    for win in (7, 20, 60):
        daily[f"ma{win}"] = daily.groupby("code")["close"].transform(lambda s, w=win: s.rolling(w, min_periods=w).mean())
        daily[f"ma{win}_slope"] = daily.groupby("code")[f"ma{win}"].transform(lambda s: s / s.shift(5) - 1)
        daily[f"dist_ma{win}_pct_calc"] = daily["close"] / daily[f"ma{win}"] - 1
        daily[f"above{win}_flag"] = daily["close"] > daily[f"ma{win}"]
    daily["close_gt_ma7_calc"] = daily["close"] > daily["ma7"]
    daily["ma7_gt_ma20_gt_ma60_calc"] = (daily["ma7"] > daily["ma20"]) & (daily["ma20"] > daily["ma60"])
    for win in (7, 20, 60):
        daily[f"above{win}_streak_calc"] = daily.groupby("code")[f"above{win}_flag"].transform(_streak_from_flag)
    daily["days_since_ma20_reclaim_calc"] = daily.groupby("code")["above20_flag"].transform(_days_since_reclaim)
    daily["days_since_ma60_reclaim_calc"] = daily.groupby("code")["above60_flag"].transform(_days_since_reclaim)
    body = (daily["close"] - daily["open"]).abs()
    rng = (daily["high"] - daily["low"]).replace(0, pd.NA)
    daily["upper_wick_ratio_calc"] = (daily["high"] - daily[["open", "close"]].max(axis=1)) / rng
    daily["lower_wick_ratio_calc"] = (daily[["open", "close"]].min(axis=1) - daily["low"]) / rng
    daily["body_ratio"] = body / rng
    daily["large_bullish_candle_calc"] = (daily["close"] > daily["open"]) & (daily["body_ratio"] >= 0.6)
    daily["large_bearish_candle_calc"] = (daily["close"] < daily["open"]) & (daily["body_ratio"] >= 0.6)
    daily["prev20_high"] = daily.groupby("code")["high"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).max())
    daily["failed_high_update_calc"] = (daily["high"] > daily["prev20_high"]) & (daily["close"] < daily["prev20_high"])
    daily["volume_ma20"] = daily.groupby("code")["volume"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    daily["volume_ma20_ratio_calc"] = daily["volume"] / daily["volume_ma20"].replace(0, pd.NA)
    daily["breakout_with_volume"] = (daily["high"] > daily["prev20_high"]) & (daily["volume_ma20_ratio_calc"] > 1.2)
    daily["high_break_volume_count_calc"] = daily.groupby("code")["breakout_with_volume"].transform(lambda s: s.astype(float).rolling(20, min_periods=1).sum())
    daily["realized_vol20_calc"] = daily.groupby("code")["close"].transform(lambda s: s.pct_change().rolling(20, min_periods=10).std())
    tr = pd.concat([(daily["high"] - daily["low"]), (daily["high"] - daily.groupby("code")["close"].shift(1)).abs(), (daily["low"] - daily.groupby("code")["close"].shift(1)).abs()], axis=1).max(axis=1)
    daily["atr14_pct_calc"] = tr.groupby(daily["code"]).transform(lambda s: s.rolling(14, min_periods=7).mean()) / daily["close"]
    return daily[
        [
            "code",
            "decision_ymd",
            "close_gt_ma7_calc",
            "dist_ma7_pct_calc",
            "dist_ma20_pct_calc",
            "dist_ma60_pct_calc",
            "ma7_slope",
            "ma20_slope",
            "ma60_slope",
            "ma7_gt_ma20_gt_ma60_calc",
            "above7_streak_calc",
            "above20_streak_calc",
            "above60_streak_calc",
            "days_since_ma20_reclaim_calc",
            "days_since_ma60_reclaim_calc",
            "upper_wick_ratio_calc",
            "lower_wick_ratio_calc",
            "large_bullish_candle_calc",
            "large_bearish_candle_calc",
            "failed_high_update_calc",
            "volume_ma20_ratio_calc",
            "high_break_volume_count_calc",
            "realized_vol20_calc",
            "atr14_pct_calc",
        ]
    ]


def load_rows(input_root: Path, daily_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = pd.read_csv(input_root / "candidate_entry_overlay_rows.csv", dtype={"code": str}, low_memory=False)
    rows = rows[rows["year"].isin(YEARS)].copy()
    rows["baseline_rank_recalc"] = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows = rows[rows["baseline_rank_recalc"] <= 20].copy()
    features = daily_features(daily_path, set(rows["code"]))
    rows = rows.merge(features, on=["code", "decision_ymd"], how="left")
    rename = {
        "close_gt_ma7_calc": "close_gt_ma7",
        "dist_ma7_pct_calc": "dist_ma7_pct",
        "dist_ma20_pct_calc": "dist_ma20_pct_calc_joined",
        "dist_ma60_pct_calc": "dist_ma60_pct_calc_joined",
        "ma7_gt_ma20_gt_ma60_calc": "ma7_gt_ma20_gt_ma60",
        "above7_streak_calc": "above7_streak",
        "above20_streak_calc": "above20_streak",
        "above60_streak_calc": "above60_streak_calc_joined",
        "days_since_ma20_reclaim_calc": "days_since_ma20_reclaim",
        "days_since_ma60_reclaim_calc": "days_since_ma60_reclaim_calc_joined",
        "upper_wick_ratio_calc": "upper_wick_ratio_calc_joined",
        "lower_wick_ratio_calc": "lower_wick_ratio",
        "large_bullish_candle_calc": "large_bullish_candle",
        "large_bearish_candle_calc": "large_bearish_candle",
        "failed_high_update_calc": "failed_high_update",
        "volume_ma20_ratio_calc": "volume_ma20_ratio",
        "high_break_volume_count_calc": "high_break_volume_count",
        "realized_vol20_calc": "realized_vol20_calc_joined",
        "atr14_pct_calc": "atr14_pct_calc_joined",
    }
    rows = rows.rename(columns=rename)
    for src, dst in (
        ("dist_ma20_pct_calc_joined", "dist_ma20_pct"),
        ("dist_ma60_pct_calc_joined", "dist_ma60_pct"),
        ("above60_streak_calc_joined", "above60_streak"),
        ("days_since_ma60_reclaim_calc_joined", "days_since_ma60_reclaim"),
        ("upper_wick_ratio_calc_joined", "upper_wick_ratio"),
        ("realized_vol20_calc_joined", "realized_vol20"),
        ("atr14_pct_calc_joined", "atr14_pct"),
    ):
        if dst in rows:
            rows[dst] = pd.to_numeric(rows[dst], errors="coerce").fillna(pd.to_numeric(rows.get(src), errors="coerce"))
        elif src in rows:
            rows[dst] = rows[src]
    no_trigger = rows[~rows["ma7_triggered"].astype(bool)].copy()
    return rows, {
        "rows_loaded": int(len(rows)),
        "no_trigger_rows": int(len(no_trigger)),
        "feature_join_coverage_close_gt_ma7": float(rows["close_gt_ma7"].notna().mean()) if "close_gt_ma7" in rows else 0.0,
        "source_research_decision": json.loads((input_root / "research_decision.json").read_text(encoding="utf-8")).get("research_decision"),
    }


def no_trigger_outcome_summary(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for year in YEARS:
        for topk in TOPK:
            g = rows[(rows["year"] == year) & (rows["baseline_rank_recalc"] <= topk) & (~rows["ma7_triggered"].astype(bool))]
            missed = g[g["no_trigger_classification"] == "missed_good_candidate"]
            skipped = g[g["no_trigger_classification"] == "skipped_bad_candidate"]
            neutral = g[g["no_trigger_classification"] == "neutral_no_entry"]
            out.append(
                {
                    "year": year,
                    "topk": topk,
                    "no_trigger_count": int(len(g)),
                    "missed_good_count": int(len(missed)),
                    "missed_good_rate": None if len(g) == 0 else float(len(missed) / len(g)),
                    "skipped_bad_count": int(len(skipped)),
                    "skipped_bad_rate": None if len(g) == 0 else float(len(skipped) / len(g)),
                    "neutral_count": int(len(neutral)),
                    "neutral_rate": None if len(g) == 0 else float(len(neutral) / len(g)),
                    "baseline_ret20_mean": _mean(g, "baseline_ret20_from_t"),
                    "baseline_ret20_median": _median(g, "baseline_ret20_from_t"),
                    "underperformance_contribution_vs_zero": _mean(g, "baseline_ret20_from_t"),
                }
            )
    return pd.DataFrame(out)


def feature_decomposition(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    no_trigger = rows[~rows["ma7_triggered"].astype(bool)].copy()
    for year in YEARS:
        for topk in TOPK:
            g = no_trigger[(no_trigger["year"] == year) & (no_trigger["baseline_rank_recalc"] <= topk)]
            missed = g[g["no_trigger_classification"] == "missed_good_candidate"]
            skipped = g[g["no_trigger_classification"] == "skipped_bad_candidate"]
            for feature in FEATURES:
                if feature not in g:
                    continue
                out.append(
                    {
                        "year": year,
                        "topk": topk,
                        "feature": feature,
                        "missed_good_n": int(len(missed)),
                        "skipped_bad_n": int(len(skipped)),
                        "missed_good_mean": _mean(missed, feature),
                        "skipped_bad_mean": _mean(skipped, feature),
                        "missed_good_median": _median(missed, feature),
                        "skipped_bad_median": _median(skipped, feature),
                        "diff_missed_minus_skipped": None if _mean(missed, feature) is None or _mean(skipped, feature) is None else _mean(missed, feature) - _mean(skipped, feature),  # type: ignore[operator]
                        "effect_size": _effect(missed, skipped, feature),
                        "coverage_missed_good": float(missed[feature].notna().mean()) if len(missed) else None,
                        "coverage_skipped_bad": float(skipped[feature].notna().mean()) if len(skipped) else None,
                    }
                )
    return pd.DataFrame(out)


def profile(rows: pd.DataFrame, cohort: str) -> dict[str, Any]:
    no_trigger = rows[(~rows["ma7_triggered"].astype(bool)) & (rows["no_trigger_classification"] == cohort)].copy()
    return {
        "cohort": cohort,
        "n": int(len(no_trigger)),
        "ret20_mean": _mean(no_trigger, "baseline_ret20_from_t"),
        "ret20_median": _median(no_trigger, "baseline_ret20_from_t"),
        "ret5_proxy_mean": _mean(no_trigger, "baseline_mfe20"),
        "close_gt_ma7_rate": _rate(no_trigger, "close_gt_ma7"),
        "ma7_gt_ma20_gt_ma60_rate": _rate(no_trigger, "ma7_gt_ma20_gt_ma60"),
        "monthly_high_zone_rate": _rate(no_trigger, "monthly_high_zone_proxy"),
        "monthly_box_inside_rate": _rate(no_trigger, "monthly_box_inside_proxy"),
        "monthly_box_breakout_rate": _rate(no_trigger, "monthly_box_breakout_proxy"),
        "dist_ma7_pct_mean": _mean(no_trigger, "dist_ma7_pct"),
        "dist_ma20_pct_mean": _mean(no_trigger, "dist_ma20_pct"),
        "ma7_slope_mean": _mean(no_trigger, "ma7_slope"),
        "ma20_slope_mean": _mean(no_trigger, "ma20_slope"),
        "upper_wick_ratio_mean": _mean(no_trigger, "upper_wick_ratio"),
        "failed_high_update_rate": _rate(no_trigger, "failed_high_update"),
        "realized_vol20_mean": _mean(no_trigger, "realized_vol20"),
    }


def fallback_candidates(decomp: pd.DataFrame, rows: pd.DataFrame) -> list[dict[str, Any]]:
    top = decomp[(decomp["topk"] == 10) & (decomp["effect_size"].notna())].copy()
    if top.empty:
        return []
    agg = top.groupby("feature", as_index=False).agg(
        avg_abs_effect=("effect_size", lambda s: float(s.abs().mean())),
        avg_effect=("effect_size", "mean"),
        years_supported=("year", lambda s: int(s.nunique())),
        sample_size=("missed_good_n", "sum"),
    )
    excluded = {
        "monthly_box_breakout_proxy",
        "above20_streak",
        "above60_streak",
        "monthly_high_zone_proxy",
        "monthly_box_inside_proxy",
    }
    out = []
    for _, row in agg[~agg["feature"].isin(excluded)].sort_values("avg_abs_effect", ascending=False).head(5).iterrows():
        out.append(
            {
                "axis_name": f"{row['feature']}_no_trigger_fallback_context",
                "feature": row["feature"],
                "intended_use": "no_trigger_fallback",
                "what_it_separates": "no_trigger_missed_good vs no_trigger_skipped_bad",
                "years_supported": int(row["years_supported"]),
                "sample_size": int(row["sample_size"]),
                "observed_effect_size_abs": float(row["avg_abs_effect"]),
                "expected_winner_damage": "unknown_until_pretest",
                "recommended_next": "pretest" if row["years_supported"] >= 2 and row["avg_abs_effect"] >= 0.20 else "hold",
            }
        )
    return out


def stability_summary(outcome: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for topk in TOPK:
        g = outcome[outcome["topk"] == topk]
        rows.append(
            {
                "topk": topk,
                "missed_good_rate_min": float(g["missed_good_rate"].min()),
                "missed_good_rate_max": float(g["missed_good_rate"].max()),
                "skipped_bad_rate_min": float(g["skipped_bad_rate"].min()),
                "skipped_bad_rate_max": float(g["skipped_bad_rate"].max()),
                "year_count": int(g["year"].nunique()),
            }
        )
    return pd.DataFrame(rows)


def concentration(rows: pd.DataFrame) -> dict[str, Any]:
    no_trigger = rows[~rows["ma7_triggered"].astype(bool)].copy()
    if no_trigger.empty:
        return {"no_trigger_rows": 0}
    return {
        "no_trigger_rows": int(len(no_trigger)),
        "largest_code_share": float(no_trigger["code"].value_counts(normalize=True).iloc[0]),
        "largest_date_share": float(no_trigger["decision_ymd"].value_counts(normalize=True).iloc[0]),
        "largest_year_share": float(no_trigger["year"].value_counts(normalize=True).iloc[0]),
    }


def decide(outcome: pd.DataFrame, candidates: list[dict[str, Any]], concentration_summary: dict[str, Any]) -> dict[str, Any]:
    top10 = outcome[outcome["topk"] == 10]
    missed_dominates = bool((top10["missed_good_rate"] > top10["skipped_bad_rate"]).all())
    good_candidate = next((c for c in candidates if c["recommended_next"] == "pretest"), None)
    concentrated = concentration_summary.get("largest_year_share", 1.0) > 0.75 or concentration_summary.get("largest_code_share", 1.0) > 0.20
    if good_candidate and not concentrated:
        decision = "fallback_axis_found"
        reasons = [f"best fallback axis: {good_candidate['axis_name']}"]
    elif missed_dominates and not good_candidate:
        decision = "drop_ma7_event_overlay"
        reasons = ["no-trigger missed_good dominates and no stable fallback axis emerged"]
    elif not good_candidate:
        decision = "hold_for_ma20_event_pretest"
        reasons = ["MA7 no-trigger cannot be fixed safely; slower MA event may be more appropriate"]
    else:
        decision = "inconclusive"
        reasons = ["no-trigger decomposition is concentration or coverage limited"]
    return {"research_decision": decision, "reason_typed": reasons, "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(*, input_root: Path = DEFAULT_INPUT_ROOT, daily_path: Path = DEFAULT_DAILY_PATH, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-ma7-no-trigger-decomposition-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    missing_inputs = [name for name in REQUIRED_INPUTS if not (input_root / name).exists()]
    source_audit = json.loads((input_root / "no_lookahead_audit.json").read_text(encoding="utf-8")) if not missing_inputs else {}
    rows, report = load_rows(input_root, daily_path)
    outcome = no_trigger_outcome_summary(rows)
    decomp = feature_decomposition(rows)
    momentum = profile(rows, "missed_good_candidate")
    skipped = profile(rows, "skipped_bad_candidate")
    candidates = fallback_candidates(decomp, rows)
    year_stability = stability_summary(outcome)
    conc = concentration(rows)
    decision = decide(outcome, candidates, conc)

    outcome.to_csv(run_dir / "no_trigger_outcome_summary.csv", index=False)
    decomp.to_csv(run_dir / "missed_good_vs_skipped_bad_decomposition.csv", index=False)
    year_stability.to_csv(run_dir / "year_stability_summary.csv", index=False)
    _write_json(run_dir / "no_pullback_momentum_profile.json", momentum)
    _write_json(run_dir / "skipped_bad_profile.json", skipped)
    _write_json(run_dir / "fallback_axis_candidates.json", {"candidates": candidates})
    _write_json(run_dir / "concentration_summary.json", conc)
    _write_json(run_dir / "input_artifact_report.json", {"input_root": input_root, "daily_path": daily_path, "missing_inputs": missing_inputs, "source_no_lookahead_audit": source_audit.get("audit_result"), **report})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass" if source_audit.get("audit_result") == "pass" and not missing_inputs else "incomplete", "source_audit_result": source_audit.get("audit_result"), "topk_selection_uses_existing_baseline_rows": True, "no_trigger_classification_uses_existing_label_columns": True, "decision_date_features_recomputed_point_in_time": True, "future_returns_used_only_for_cohort_labels": True, "no_new_policy_created": True, "ma20_event_not_used": True})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "report": report}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(input_root=args.input_root, daily_path=args.daily_path, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
