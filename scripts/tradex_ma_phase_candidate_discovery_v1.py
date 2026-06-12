from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma_phase_candidate_discovery_v1"
DEFAULT_FEATURE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_INPUT_PARQUET = DEFAULT_FEATURE_ROOT / "ma_phase_features.parquet"
DEFAULT_INPUT_AUDIT = DEFAULT_FEATURE_ROOT / "input_audit.json"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_phase_candidate_discovery_v1")
HORIZONS = (3, 5, 7, 10, 20)
REQUIRED = (
    "input_audit.json",
    "axis_run_manifest.json",
    "candidate_leaderboard.json",
    "best_candidate_summary.json",
    "drop_hold_reasons.json",
    "final_research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
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
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _metric_row(group: pd.DataFrame, *, axis: str, candidate: str, horizon: int) -> dict[str, Any]:
    ret = f"ret_{horizon}b"
    dd = f"max_drawdown_{horizon}b"
    valid = group[group[ret].notna()]
    ma = "ma7" if "ma7" in candidate or axis.startswith("axis1") or axis.startswith("axis2") else "ma60"
    return {
        "axis": axis,
        "candidate": candidate,
        "horizon": horizon,
        "event_count": int(len(valid)),
        "unique_symbol_count": int(valid["code"].nunique()) if not valid.empty else 0,
        "mean_ret": _mean(valid[ret]),
        "median_ret": _median(valid[ret]),
        "hit_rate": _rate(valid[ret] > 0) if not valid.empty else None,
        "severe_loss_rate": _rate(valid[f"severe_loss_flag_{horizon}b"]) if not valid.empty else None,
        "mean_max_drawdown": _mean(valid[dd]),
        "median_max_drawdown": _median(valid[dd]),
        "higher_high_rate": _rate(valid[f"higher_high_made_{horizon}b"]) if not valid.empty else None,
        "lower_low_rate": _rate(valid[f"lower_low_made_{horizon}b"]) if not valid.empty else None,
        "ma_rebreak_rate": _rate(valid[f"rebreak_{ma}_{horizon}b"]) if f"rebreak_{ma}_{horizon}b" in valid.columns and not valid.empty else None,
        "ma_held_rate": _rate(valid[f"held_above_{ma}_{horizon}b"]) if f"held_above_{ma}_{horizon}b" in valid.columns and not valid.empty else None,
        "pullback_occurred_rate": _rate(valid[f"pullback_occurred_{horizon}b"]) if not valid.empty else None,
        "recovered_after_pullback_rate": _rate(valid[f"recovered_after_pullback_{horizon}b"]) if not valid.empty else None,
    }


def _compare(summary: pd.DataFrame, first: str, second: str, name: str, horizon: int) -> dict[str, Any]:
    a = summary[(summary["candidate"] == first) & (summary["horizon"] == horizon)]
    b = summary[(summary["candidate"] == second) & (summary["horizon"] == horizon)]
    payload = {"name": name, "horizon": horizon, "first": first, "second": second}
    if a.empty or b.empty:
        payload["status"] = "missing_group"
        return payload
    ar = a.iloc[0].to_dict()
    br = b.iloc[0].to_dict()
    deltas = {}
    for metric in ["mean_ret", "hit_rate", "severe_loss_rate", "mean_max_drawdown", "higher_high_rate", "lower_low_rate", "ma_rebreak_rate", "ma_held_rate"]:
        if ar.get(metric) is not None and br.get(metric) is not None:
            deltas[f"{metric}_delta"] = ar[metric] - br[metric]
    payload.update({"status": "ready" if ar["event_count"] >= 500 and br["event_count"] >= 500 else "insufficient_sample", "first_metrics": ar, "second_metrics": br, "deltas": deltas})
    return payload


def _read_common(path: Path, ma: str) -> pd.DataFrame:
    cols = [
        "code", "ymd", "c",
        f"close_above_{ma}", f"cross_above_{ma}_today", f"bars_since_cross_above_{ma}", f"above_{ma}_run_bucket", f"consecutive_bars_above_{ma}",
        "close_above_ma20",
        "upper_resistance_bucket", "lower_support_bucket", "nearest_upper_ma", "nearest_lower_ma",
        "ma7_gt_ma20", "ma20_gt_ma60", "ma_stack_state",
        "ma20_slope_20d_bucket", "ma60_slope_20d_bucket",
        "is_large_bull_body", "is_large_bear_body", "is_small_body", "is_doji_like", "is_upper_shadow_long", "is_lower_shadow_long", "is_hammer_like", "is_shooting_star_like", "is_engulfing_bull", "is_engulfing_bear",
    ]
    if ma == "ma60":
        cols.extend(["ma60_slope_20d_bucket"])
    for h in HORIZONS:
        cols.extend([
            f"ret_{h}b", f"max_drawdown_{h}b", f"higher_high_made_{h}b", f"lower_low_made_{h}b",
            f"held_above_{ma}_{h}b", f"rebreak_{ma}_{h}b",
            f"pullback_occurred_{h}b", f"recovered_after_pullback_{h}b", f"severe_loss_flag_{h}b",
        ])
    frame = pd.read_parquet(path, columns=list(dict.fromkeys(cols)))
    frame = frame[frame[f"close_above_{ma}"].fillna(False) & frame["ret_20b"].notna()].copy()
    frame["event_year"] = frame["ymd"].astype(str).str.slice(0, 4).astype(int)
    frame[f"bars_since_cross_above_{ma}"] = pd.to_numeric(frame[f"bars_since_cross_above_{ma}"], errors="coerce")
    frame["no_light_upper_resistance"] = frame["upper_resistance_bucket"].isin(["none_near", "light_resistance"])
    frame["heavy_upper_resistance"] = frame["upper_resistance_bucket"].isin(["medium_resistance", "heavy_resistance"])
    frame["has_lower_support"] = frame["lower_support_bucket"].isin(["light_support", "medium_support", "heavy_support"])
    frame["ma20_slope_up"] = frame["ma20_slope_20d_bucket"].isin(["weak_up", "strong_up"])
    frame["ma60_not_down"] = ~frame["ma60_slope_20d_bucket"].isin(["weak_down", "strong_down"])
    return frame


def _axis2_ma7_pullback_reentry(path: Path, out_dir: Path) -> dict[str, Any]:
    frame = _read_common(path, "ma7")
    extra = pd.read_parquet(path, columns=["code", "ymd", "bars_since_cross_below_ma7"])
    extra["code"] = extra["code"].astype(str)
    frame["code"] = frame["code"].astype(str)
    frame = frame.merge(extra, on=["code", "ymd"], how="left", validate="one_to_one")
    frame["bars_since_cross_below_ma7"] = pd.to_numeric(frame["bars_since_cross_below_ma7"], errors="coerce")
    frame["first_cross"] = frame["cross_above_ma7_today"].fillna(False)
    frame["pullback_reclaim"] = frame["first_cross"] & frame["bars_since_cross_below_ma7"].between(1, 5, inclusive="both") & frame["close_above_ma20"].fillna(False)
    frame["reclaim_bull_candle"] = frame["pullback_reclaim"] & (frame["is_lower_shadow_long"].fillna(False) | frame["is_hammer_like"].fillna(False) | frame["is_large_bull_body"].fillna(False) | frame["is_engulfing_bull"].fillna(False))
    variants = {
        "ma7_first_cross_all": frame[frame["first_cross"]],
        "ma7_pullback_reclaim": frame[frame["pullback_reclaim"]],
        "ma7_pullback_reclaim_no_light_upper": frame[frame["pullback_reclaim"] & frame["no_light_upper_resistance"]],
        "ma7_pullback_reclaim_support_bull": frame[frame["reclaim_bull_candle"] & frame["has_lower_support"] & frame["no_light_upper_resistance"]],
        "ma7_pullback_reclaim_heavy_upper": frame[frame["pullback_reclaim"] & frame["heavy_upper_resistance"]],
    }
    summary = pd.DataFrame([_metric_row(g, axis="axis2_ma7_pullback_reentry", candidate=k, horizon=h) for k, g in variants.items() for h in HORIZONS])
    contrasts = [
        _compare(summary, "ma7_pullback_reclaim", "ma7_first_cross_all", "ma7_pullback_reclaim_vs_first_cross_h7", 7),
        _compare(summary, "ma7_pullback_reclaim_no_light_upper", "ma7_pullback_reclaim_heavy_upper", "ma7_reclaim_no_light_vs_heavy_h7", 7),
        _compare(summary, "ma7_pullback_reclaim_support_bull", "ma7_pullback_reclaim", "ma7_reclaim_support_bull_vs_plain_h7", 7),
    ]
    yearly = _yearly(frame, variants, "axis2_ma7_pullback_reentry", ["ma7_pullback_reclaim", "ma7_pullback_reclaim_no_light_upper", "ma7_pullback_reclaim_support_bull"], 7)
    axis_dir = out_dir / "axis2_ma7_pullback_reentry"
    axis_dir.mkdir()
    summary.to_csv(axis_dir / "variant_summary.csv", index=False, encoding="utf-8")
    yearly.to_csv(axis_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    _write_json(axis_dir / "contrast_summary.json", {"axis": "axis2_ma7_pullback_reentry", "contrasts": contrasts})
    frame[frame["pullback_reclaim"]].head(5000).to_csv(axis_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    lookup = {c["name"]: c for c in contrasts if c.get("status") == "ready"}
    c_first = lookup.get("ma7_pullback_reclaim_vs_first_cross_h7", {})
    c_res = lookup.get("ma7_reclaim_no_light_vs_heavy_h7", {})
    d_first = c_first.get("deltas", {})
    d_res = c_res.get("deltas", {})
    stable = _stable_years(yearly, "ma7_pullback_reclaim_support_bull", "mean_ret", 4)
    if d_first.get("mean_ret_delta", 0) > 0.1 and d_first.get("ma_rebreak_rate_delta", 1) <= 0 and stable:
        decision, reason = "keep_for_candidate_pretest_next", "ma7_pullback_reclaim_beats_first_cross_with_stability"
    elif d_res.get("ma_rebreak_rate_delta", 0) < -0.05 or d_res.get("mean_ret_delta", 0) > 0.1:
        decision, reason = "keep_as_context_feature", "ma7_pullback_reclaim_context_separates_resistance"
    else:
        decision, reason = "drop", "ma7_pullback_reentry_does_not_beat_first_cross_or_context_enough"
    return {"axis": "axis2_ma7_pullback_reentry", "status": "completed", "decision": decision, "reason": reason, "artifact_dir": str(axis_dir), "row_count": int(len(frame)), "best_candidate": "ma7_pullback_reclaim_support_bull" if decision != "drop" else None, "contrasts": contrasts}


def _axis1_ma7(path: Path, out_dir: Path) -> dict[str, Any]:
    frame = _read_common(path, "ma7")
    frame["phase_0_5"] = frame["bars_since_cross_above_ma7"].between(0, 5, inclusive="both")
    frame["phase_3_5"] = frame["bars_since_cross_above_ma7"].between(3, 5, inclusive="both")
    frame["phase_6_7"] = frame["bars_since_cross_above_ma7"].between(6, 7, inclusive="both")
    variants = {
        "ma7_baseline_all_above": frame,
        "ma7_cross_day": frame[frame["cross_above_ma7_today"].fillna(False)],
        "ma7_phase_0_5": frame[frame["phase_0_5"]],
        "ma7_phase_3_5": frame[frame["phase_3_5"]],
        "ma7_phase_6_7": frame[frame["phase_6_7"]],
        "ma7_phase_3_5_no_light_upper": frame[frame["phase_3_5"] & frame["no_light_upper_resistance"]],
        "ma7_phase_3_5_support_slope": frame[frame["phase_3_5"] & frame["has_lower_support"] & frame["ma20_slope_up"] & frame["ma60_not_down"]],
        "ma7_phase_3_5_heavy_upper_negative_control": frame[frame["phase_3_5"] & frame["heavy_upper_resistance"]],
    }
    rows = [_metric_row(g, axis="axis1_ma7_phase_continuation_pullback", candidate=k, horizon=h) for k, g in variants.items() for h in HORIZONS]
    summary = pd.DataFrame(rows)
    contrasts = [
        _compare(summary, "ma7_phase_3_5", "ma7_baseline_all_above", "ma7_phase_3_5_vs_baseline_h5", 5),
        _compare(summary, "ma7_phase_3_5", "ma7_phase_6_7", "ma7_phase_3_5_vs_6_7_h7", 7),
        _compare(summary, "ma7_phase_3_5_no_light_upper", "ma7_phase_3_5_heavy_upper_negative_control", "ma7_no_light_vs_heavy_h7", 7),
        _compare(summary, "ma7_phase_3_5_support_slope", "ma7_phase_3_5", "ma7_support_slope_vs_plain_h7", 7),
    ]
    yearly = _yearly(frame, variants, "axis1_ma7_phase_continuation_pullback", ["ma7_phase_3_5", "ma7_phase_3_5_no_light_upper", "ma7_phase_3_5_heavy_upper_negative_control"], 7)
    axis_dir = out_dir / "axis1_ma7_phase_continuation_pullback"
    axis_dir.mkdir()
    summary.to_csv(axis_dir / "variant_summary.csv", index=False, encoding="utf-8")
    yearly.to_csv(axis_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    _write_json(axis_dir / "contrast_summary.json", {"axis": "axis1_ma7_phase_continuation_pullback", "contrasts": contrasts})
    frame[frame["phase_3_5"]].head(5000).to_csv(axis_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    decision, reason = _axis1_decision(summary, contrasts, yearly)
    return {
        "axis": "axis1_ma7_phase_continuation_pullback",
        "status": "completed",
        "decision": decision,
        "reason": reason,
        "artifact_dir": str(axis_dir),
        "row_count": int(len(frame)),
        "best_candidate": "ma7_phase_3_5_no_light_upper" if decision != "drop" else None,
        "summary_20": summary[(summary["horizon"] == 7)].to_dict("records"),
        "contrasts": contrasts,
    }


def _axis1_decision(summary: pd.DataFrame, contrasts: list[dict[str, Any]], yearly: pd.DataFrame) -> tuple[str, str]:
    lookup = {c["name"]: c for c in contrasts if c.get("status") == "ready"}
    c_base = lookup.get("ma7_phase_3_5_vs_baseline_h5", {})
    c_67 = lookup.get("ma7_phase_3_5_vs_6_7_h7", {})
    c_res = lookup.get("ma7_no_light_vs_heavy_h7", {})
    if not c_base or not c_67:
        return "hold", "insufficient_sample_or_missing_required_contrast"
    d_base = c_base.get("deltas", {})
    d_67 = c_67.get("deltas", {})
    d_res = c_res.get("deltas", {})
    phase_positive = d_base.get("mean_ret_delta", 0) > 0.05 and d_base.get("ma_rebreak_rate_delta", 1) <= 0.03
    pullback_separation = d_67.get("mean_ret_delta", 0) >= -0.05 and d_67.get("ma_rebreak_rate_delta", 0) <= -0.02
    resistance_separates = d_res.get("ma_rebreak_rate_delta", 0) <= -0.05 or d_res.get("mean_ret_delta", 0) > 0.1
    stable = _stable_years(yearly, "ma7_phase_3_5_no_light_upper", "mean_ret", min_years=5)
    if phase_positive and pullback_separation and resistance_separates and stable:
        return "keep_for_candidate_pretest_next", "ma7_phase_3_5_context_has_positive_same_condition_separation"
    if resistance_separates or phase_positive:
        return "keep_as_context_feature", "ma7_phase_context_has_partial_value_but_not_enough_for_candidate"
    return "drop", "ma7_phase_does_not_improve_continuation_or_rebreak_enough"


def _axis3_ma60(path: Path, out_dir: Path) -> dict[str, Any]:
    frame = _read_common(path, "ma60")
    run = pd.to_numeric(frame["consecutive_bars_above_ma60"], errors="coerce")
    frame["run_20_39"] = run.between(20, 39, inclusive="both")
    frame["run_40_59"] = run.between(40, 59, inclusive="both")
    frame["run_60_plus"] = run.ge(60)
    variants = {
        "ma60_baseline_all_above": frame,
        "ma60_run_20_39": frame[frame["run_20_39"]],
        "ma60_run_40_59": frame[frame["run_40_59"]],
        "ma60_run_60_plus": frame[frame["run_60_plus"]],
        "ma60_run_20_plus_no_light_upper": frame[(run.ge(20)) & frame["no_light_upper_resistance"]],
        "ma60_run_20_plus_heavy_upper": frame[(run.ge(20)) & frame["heavy_upper_resistance"]],
        "ma60_run_20_plus_support": frame[(run.ge(20)) & frame["has_lower_support"]],
    }
    summary = pd.DataFrame([_metric_row(g, axis="axis3_ma60_continuation_path", candidate=k, horizon=h) for k, g in variants.items() for h in HORIZONS])
    contrasts = [
        _compare(summary, "ma60_run_20_plus_no_light_upper", "ma60_baseline_all_above", "ma60_run20_no_light_vs_baseline_h20", 20),
        _compare(summary, "ma60_run_20_plus_no_light_upper", "ma60_run_20_plus_heavy_upper", "ma60_no_light_vs_heavy_h20", 20),
        _compare(summary, "ma60_run_60_plus", "ma60_run_20_39", "ma60_run60plus_vs_20_39_h20", 20),
    ]
    yearly = _yearly(frame, variants, "axis3_ma60_continuation_path", ["ma60_run_20_plus_no_light_upper", "ma60_run_20_plus_heavy_upper"], 20)
    axis_dir = out_dir / "axis3_ma60_continuation_path"
    axis_dir.mkdir()
    summary.to_csv(axis_dir / "variant_summary.csv", index=False, encoding="utf-8")
    yearly.to_csv(axis_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    _write_json(axis_dir / "contrast_summary.json", {"axis": "axis3_ma60_continuation_path", "contrasts": contrasts})
    frame[run.ge(20)].head(5000).to_csv(axis_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    decision, reason = _axis3_decision(contrasts, yearly)
    return {
        "axis": "axis3_ma60_continuation_path",
        "status": "completed",
        "decision": decision,
        "reason": reason,
        "artifact_dir": str(axis_dir),
        "row_count": int(len(frame)),
        "best_candidate": "ma60_run_20_plus_no_light_upper" if decision != "drop" else None,
        "contrasts": contrasts,
    }


def _axis3_decision(contrasts: list[dict[str, Any]], yearly: pd.DataFrame) -> tuple[str, str]:
    lookup = {c["name"]: c for c in contrasts if c.get("status") == "ready"}
    c_base = lookup.get("ma60_run20_no_light_vs_baseline_h20", {})
    c_heavy = lookup.get("ma60_no_light_vs_heavy_h20", {})
    if not c_base or not c_heavy:
        return "hold", "insufficient_sample_or_missing_required_contrast"
    d_base = c_base.get("deltas", {})
    d_heavy = c_heavy.get("deltas", {})
    positive = d_base.get("mean_ret_delta", 0) > 0 and d_base.get("ma_rebreak_rate_delta", 1) <= 0
    separates = d_heavy.get("mean_ret_delta", 0) > 0.2 or d_heavy.get("ma_rebreak_rate_delta", 0) < -0.05
    stable = _stable_years(yearly, "ma60_run_20_plus_no_light_upper", "mean_ret", 5)
    if positive and separates and stable:
        return "keep_for_candidate_pretest_next", "ma60_continuation_context_has_positive_resistance_separation"
    if positive or separates:
        return "keep_as_context_feature", "ma60_continuation_has_context_value_but_not_candidate_ready"
    return "drop", "ma60_continuation_path_does_not_improve_enough"


def _axis4_ma60_failure_guard(path: Path, out_dir: Path) -> dict[str, Any]:
    frame = _read_common(path, "ma60")
    run = pd.to_numeric(frame["consecutive_bars_above_ma60"], errors="coerce")
    frame = frame[run.ge(20)].copy()
    frame["weak_candle"] = frame["is_upper_shadow_long"].fillna(False) | frame["is_small_body"].fillna(False) | frame["is_doji_like"].fillna(False) | frame["is_shooting_star_like"].fillna(False) | frame["is_large_bear_body"].fillna(False)
    frame["ma60_slope_weakening"] = frame["ma60_slope_20d_bucket"].isin(["flat", "weak_down", "strong_down"])
    variants = {
        "ma60_continuation_attempt_all": frame,
        "ma60_failure_guard_heavy_upper": frame[frame["heavy_upper_resistance"]],
        "ma60_failure_guard_slope_weak": frame[frame["ma60_slope_weakening"]],
        "ma60_failure_guard_weak_candle": frame[frame["weak_candle"]],
        "ma60_failure_guard_heavy_weak_candle": frame[frame["heavy_upper_resistance"] & frame["weak_candle"]],
        "ma60_positive_control_no_light_support": frame[frame["no_light_upper_resistance"] & frame["has_lower_support"] & ~frame["weak_candle"]],
    }
    summary = pd.DataFrame([_metric_row(g, axis="axis4_ma60_continuation_failure_guard", candidate=k, horizon=h) for k, g in variants.items() for h in HORIZONS])
    contrasts = [
        _compare(summary, "ma60_failure_guard_heavy_weak_candle", "ma60_positive_control_no_light_support", "ma60_heavy_weak_vs_positive_control_h20", 20),
        _compare(summary, "ma60_failure_guard_heavy_upper", "ma60_continuation_attempt_all", "ma60_heavy_upper_vs_all_h20", 20),
        _compare(summary, "ma60_failure_guard_slope_weak", "ma60_continuation_attempt_all", "ma60_slope_weak_vs_all_h20", 20),
    ]
    yearly = _yearly(frame, variants, "axis4_ma60_continuation_failure_guard", ["ma60_failure_guard_heavy_weak_candle", "ma60_positive_control_no_light_support"], 20)
    axis_dir = out_dir / "axis4_ma60_continuation_failure_guard"
    axis_dir.mkdir()
    summary.to_csv(axis_dir / "variant_summary.csv", index=False, encoding="utf-8")
    yearly.to_csv(axis_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    _write_json(axis_dir / "contrast_summary.json", {"axis": "axis4_ma60_continuation_failure_guard", "contrasts": contrasts})
    frame[frame["heavy_upper_resistance"] & frame["weak_candle"]].head(5000).to_csv(axis_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    lookup = {c["name"]: c for c in contrasts if c.get("status") == "ready"}
    c = lookup.get("ma60_heavy_weak_vs_positive_control_h20", {})
    d = c.get("deltas", {})
    if d.get("mean_ret_delta", 0) < -0.5 and (d.get("ma_rebreak_rate_delta", 0) > 0.05 or d.get("severe_loss_rate_delta", 0) > 0.02):
        decision, reason = "keep_for_candidate_pretest_next", "ma60_failure_guard_heavy_weak_candle_separates_bad_continuation"
    elif d.get("mean_ret_delta", 0) < 0 or d.get("ma_rebreak_rate_delta", 0) > 0.03:
        decision, reason = "keep_as_context_feature", "ma60_failure_guard_has_warning_value"
    else:
        decision, reason = "drop", "ma60_failure_guard_does_not_separate_failures_enough"
    return {"axis": "axis4_ma60_continuation_failure_guard", "status": "completed", "decision": decision, "reason": reason, "artifact_dir": str(axis_dir), "row_count": int(len(frame)), "best_candidate": "ma60_failure_guard_heavy_weak_candle" if decision != "drop" else None, "contrasts": contrasts}


def _yearly(frame: pd.DataFrame, variants: dict[str, pd.DataFrame], axis: str, names: list[str], horizon: int) -> pd.DataFrame:
    rows = []
    for name in names:
        source = variants[name]
        for year, group in source.groupby("event_year", sort=False):
            row = _metric_row(group, axis=axis, candidate=name, horizon=horizon)
            row["event_year"] = int(year)
            row["sample_status"] = "sufficient" if row["event_count"] >= 100 else "insufficient_sample"
            rows.append(row)
    return pd.DataFrame(rows)


def _stable_years(yearly: pd.DataFrame, candidate: str, metric: str, min_years: int) -> bool:
    subset = yearly[(yearly["candidate"] == candidate) & (yearly["sample_status"] == "sufficient")]
    if subset.empty:
        return False
    if metric == "mean_ret":
        supporting = subset[pd.to_numeric(subset["mean_ret"], errors="coerce") > 0]
    else:
        supporting = subset
    return subset["event_year"].nunique() >= min_years and len(supporting) >= max(3, subset["event_year"].nunique() - 1)


def _leaderboard(axis_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rank = {"promote_to_research_champion_candidate": 5, "keep_for_candidate_pretest_next": 4, "keep_as_context_feature": 3, "hold": 2, "drop": 1}
    rows = []
    for result in axis_results:
        rows.append({
            "axis": result["axis"],
            "decision": result["decision"],
            "rank_score": rank.get(result["decision"], 0),
            "best_candidate": result.get("best_candidate"),
            "reason": result["reason"],
            "artifact_dir": result["artifact_dir"],
            "row_count": result["row_count"],
        })
    return sorted(rows, key=lambda r: r["rank_score"], reverse=True)


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_audit = json.loads(args.input_audit.read_text(encoding="utf-8"))
    axis_results: list[dict[str, Any]] = []
    axis1 = _axis1_ma7(args.input_parquet, out_dir)
    axis_results.append(axis1)
    stop_reason = None
    if axis1["decision"] in {"promote_to_research_champion_candidate", "keep_for_candidate_pretest_next"}:
        stop_reason = "axis1_reached_stop_condition"
    else:
        if axis1["decision"] in {"keep_as_context_feature", "hold"}:
            axis2 = _axis2_ma7_pullback_reentry(args.input_parquet, out_dir)
            axis_results.append(axis2)
            if axis2["decision"] in {"promote_to_research_champion_candidate", "keep_for_candidate_pretest_next"}:
                stop_reason = "axis2_reached_stop_condition"
        if stop_reason is None:
            axis3 = _axis3_ma60(args.input_parquet, out_dir)
            axis_results.append(axis3)
            if axis3["decision"] in {"promote_to_research_champion_candidate", "keep_for_candidate_pretest_next"}:
                stop_reason = "axis3_reached_stop_condition"
            elif axis3["decision"] in {"keep_as_context_feature", "hold"}:
                axis4 = _axis4_ma60_failure_guard(args.input_parquet, out_dir)
                axis_results.append(axis4)
                if axis4["decision"] in {"promote_to_research_champion_candidate", "keep_for_candidate_pretest_next"}:
                    stop_reason = "axis4_reached_stop_condition"
                else:
                    stop_reason = "planned_axes_completed_until_no_candidate_ready"
            else:
                stop_reason = "axis1_and_axis3_completed_without_candidate"
    if stop_reason is None:
        axis3 = _axis3_ma60(args.input_parquet, out_dir)
        axis_results.append(axis3)
        stop_reason = "fallback_axis3_completed"
    leaderboard = _leaderboard(axis_results)
    best = leaderboard[0] if leaderboard else None
    final_decision = best["decision"] if best else "hold"
    if final_decision == "keep_as_context_feature":
        final_rollup = "keep_as_context_feature"
    elif final_decision == "drop" and all(r["decision"] == "drop" for r in axis_results):
        final_rollup = "drop"
    else:
        final_rollup = final_decision
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_parquet": str(args.input_parquet),
        "input_audit": str(args.input_audit),
        "source_axis_id": source_audit.get("axis_id"),
        "confirmed_bars_only_inherited": bool(source_audit.get("confirmed_bars_only")),
        "runtime_db_write": False,
        "meemee_reflection": False,
        "ranking_change": False,
        "publish": False,
        "silent_fallback_used": False,
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "axis_run_manifest.json", {"axis_id": AXIS_ID, "stop_reason": stop_reason, "axis_results": axis_results})
    _write_json(out_dir / "candidate_leaderboard.json", {"axis_id": AXIS_ID, "leaderboard": leaderboard})
    _write_json(out_dir / "best_candidate_summary.json", {"axis_id": AXIS_ID, "best_candidate": best})
    _write_json(out_dir / "drop_hold_reasons.json", {"axis_id": AXIS_ID, "reasons": [{"axis": r["axis"], "decision": r["decision"], "reason": r["reason"]} for r in axis_results]})
    final = {
        "axis_id": AXIS_ID,
        "candidate_local_decision": final_rollup,
        "session_aggregate_decision": final_rollup,
        "authoritative_rollup_decision": final_rollup,
        "best_candidate": best,
        "stop_reason": stop_reason,
        "non_scope": ["no MeeMee reflection", "no runtime DB write", "no ranking change", "no publish", "no production candidate generation change", "no live buy/sell rule", "no exit threshold tuning", "no synthetic lifecycle promotion", "no frozen exit champion changes"],
    }
    _write_json(out_dir / "final_research_decision.json", final)
    missing = [name for name in REQUIRED if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete" if not missing else "incomplete", "missing_artifacts": missing, "authoritative_result": str(out_dir / "final_research_decision.json"), "generated_at_utc": datetime.now(timezone.utc).isoformat()})
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX MA phase candidate discovery loop.")
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT_PARQUET)
    parser.add_argument("--input-audit", type=Path, default=DEFAULT_INPUT_AUDIT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
