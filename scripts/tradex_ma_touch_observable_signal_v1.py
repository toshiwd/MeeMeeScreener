from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma_touch_observable_signal_v1"
DEFAULT_FEATURE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_INPUT_PARQUET = DEFAULT_FEATURE_ROOT / "ma_phase_features.parquet"
DEFAULT_INPUT_AUDIT = DEFAULT_FEATURE_ROOT / "input_audit.json"
DEFAULT_PRIOR_ROOT = Path("G:/Tradex/ma_touch_reaction_context_v1/20260603T152130Z-ma-touch-reaction-context-v1")
DEFAULT_PRIOR_EVENTS = DEFAULT_PRIOR_ROOT / "ma_touch_reaction_events.csv"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_touch_observable_signal_v1")
TARGET_MAS = ("MA60", "MA100", "MA200")
REACTIONS = ("touch_rejection", "failed_breakout", "breakout_continuation", "touch_pullback_reacceleration")
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "observable_signal_definition.json",
    "ma_touch_observable_signal_events.csv",
    "observable_signal_summary.csv",
    "observable_signal_by_context.csv",
    "contrast_summary.json",
    "yearly_stability_summary.csv",
    "candidate_examples.csv",
    "research_decision.json",
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


def _load_events(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["event_year"] = frame["ymd"].astype(str).str.slice(0, 4).astype(int)
    bool_cols = [
        "is_large_bull_body",
        "is_large_bear_body",
        "is_small_body",
        "is_doji_like",
        "is_upper_shadow_long",
        "is_lower_shadow_long",
        "is_hammer_like",
        "is_shooting_star_like",
        "is_engulfing_bull",
        "is_engulfing_bear",
        "weak_close_position",
        "gap_up",
        "gap_down",
        "ma20_gt_ma60",
        "target_ma_rebreak_20b",
        "rebreak_ma20_20b",
        "higher_high_made_20b",
        "lower_low_made_20b",
        "severe_loss_flag_20b",
    ]
    for col in bool_cols:
        if col in frame.columns:
            frame[col] = frame[col].fillna(False).astype(bool)
    frame["close_position_strong"] = pd.to_numeric(frame["close_position_in_range"], errors="coerce").ge(0.6)
    frame["positive_body"] = pd.to_numeric(frame["c"], errors="coerce").gt(pd.to_numeric(frame["o"], errors="coerce"))
    frame["target_ma_slope_context"] = frame["target_ma_slope_bucket"].map(_slope_context).fillna("unknown")
    frame["ma20_phase_context"] = frame["above_ma20_run_bucket"].map(_phase_context).fillna("other")
    frame["has_lower_support"] = frame["lower_support_bucket"].isin(["light_support", "medium_support", "heavy_support"])
    frame["signal_close_break_above_strong"] = (
        frame["touch_method"].eq("close_break_above")
        & frame["close_position_strong"]
        & ~frame["is_upper_shadow_long"]
        & (frame["is_large_bull_body"] | frame["positive_body"])
    )
    frame["signal_high_touch_only_weak"] = (
        frame["touch_method"].eq("high_touch_only")
        & (frame["is_upper_shadow_long"] | frame["weak_close_position"])
    )
    frame["signal_close_break_above_weak"] = (
        frame["touch_method"].eq("close_break_above")
        & (frame["is_upper_shadow_long"] | frame["is_small_body"] | frame["is_doji_like"] | frame["weak_close_position"])
    )
    frame["signal_gap_touch_fade"] = frame["gap_up"] & (frame["weak_close_position"] | frame["is_upper_shadow_long"])
    frame["signal_touch_with_lower_support"] = frame["has_lower_support"]
    frame["signal_touch_without_lower_support"] = frame["lower_support_bucket"].eq("none_near")
    return frame


def _slope_context(value: Any) -> str:
    text = str(value)
    if text in {"weak_up", "strong_up"}:
        return "up"
    if text == "flat":
        return "flat"
    if text in {"weak_down", "strong_down"}:
        return "down"
    return "unknown"


def _phase_context(value: Any) -> str:
    text = str(value)
    if text in {"1-5", "6-10"}:
        return "1-10"
    if text in {"15-18", "19-20"}:
        return "15-20"
    if text == "21-30":
        return "21-30"
    return text


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _metric_row(group: pd.DataFrame, *, target_ma: str, observable_signal: str, denominator: int | None = None, context_name: str | None = None, context_value: str | None = None) -> dict[str, Any]:
    valid = group[group["ret_20b"].notna()].copy()
    row = {
        "target_ma": target_ma,
        "observable_signal": observable_signal,
        "event_count": int(len(valid)),
        "unique_symbol_count": int(valid["code"].nunique()) if not valid.empty else 0,
        "signal_rate": None if not denominator else float(len(valid) / denominator),
        "touch_rejection_rate": _rate(valid["reaction_type"].eq("touch_rejection")) if not valid.empty else None,
        "failed_breakout_rate": _rate(valid["reaction_type"].eq("failed_breakout")) if not valid.empty else None,
        "breakout_continuation_rate": _rate(valid["reaction_type"].eq("breakout_continuation")) if not valid.empty else None,
        "pullback_reacceleration_rate": _rate(valid["reaction_type"].eq("touch_pullback_reacceleration")) if not valid.empty else None,
        "mean_ret20": _mean(valid["ret_20b"]),
        "median_ret20": _median(valid["ret_20b"]),
        "hit_rate20": _rate(valid["ret_20b"] > 0) if not valid.empty else None,
        "severe_loss20": _rate(valid["severe_loss_flag_20b"]) if not valid.empty else None,
        "mean_dd20": _mean(valid["max_drawdown_20b"]),
        "median_dd20": _median(valid["max_drawdown_20b"]),
        "ma20_rebreak20": _rate(valid["rebreak_ma20_20b"]) if not valid.empty else None,
        "target_ma_rebreak20": _rate(valid["target_ma_rebreak_20b"]) if not valid.empty else None,
        "higher_high20": _rate(valid["higher_high_made_20b"]) if not valid.empty else None,
        "lower_low20": _rate(valid["lower_low_made_20b"]) if not valid.empty else None,
    }
    if context_name is not None:
        row["context_name"] = context_name
        row["context_value"] = context_value
    return row


def _signal_masks(frame: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "all_touch_events": pd.Series(True, index=frame.index),
        "close_break_above_strong": frame["signal_close_break_above_strong"],
        "high_touch_only_weak": frame["signal_high_touch_only_weak"],
        "close_break_above_weak": frame["signal_close_break_above_weak"],
        "gap_touch_fade": frame["signal_gap_touch_fade"],
        "touch_with_lower_support": frame["signal_touch_with_lower_support"],
        "touch_without_lower_support": frame["signal_touch_without_lower_support"],
        "target_ma_slope_up": frame["target_ma_slope_context"].eq("up"),
        "target_ma_slope_down": frame["target_ma_slope_context"].eq("down"),
        "ma20_phase_1_10": frame["ma20_phase_context"].eq("1-10"),
        "ma20_phase_15_20": frame["ma20_phase_context"].eq("15-20"),
        "ma20_phase_21_30": frame["ma20_phase_context"].eq("21-30"),
    }


def _summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    masks = _signal_masks(frame)
    for target_ma, target_group in frame.groupby("target_ma", sort=False):
        denom = len(target_group)
        for signal, mask in masks.items():
            group = target_group[mask.loc[target_group.index]]
            rows.append(_metric_row(group, target_ma=target_ma, observable_signal=signal, denominator=denom))
    return pd.DataFrame(rows)


def _context_summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    contexts = [
        "target_ma_slope_context",
        "ma20_phase_context",
        "upper_resistance_bucket",
        "lower_support_bucket",
        "ma_stack_state",
        "touch_method",
        "is_large_bull_body",
        "is_upper_shadow_long",
        "weak_close_position",
    ]
    for context in contexts:
        for (target_ma, value), group in frame.groupby(["target_ma", context], dropna=False, sort=False):
            denom = len(frame[frame["target_ma"].eq(target_ma)])
            rows.append(_metric_row(group, target_ma=target_ma, observable_signal=f"context_{context}", denominator=denom, context_name=context, context_value=str(value)))
    return pd.DataFrame(rows)


def _compare(summary: pd.DataFrame, *, target_ma: str, first: str, second: str, name: str) -> dict[str, Any]:
    a = summary[(summary["target_ma"] == target_ma) & (summary["observable_signal"] == first)]
    b = summary[(summary["target_ma"] == target_ma) & (summary["observable_signal"] == second)]
    payload: dict[str, Any] = {"name": f"{target_ma}_{name}", "target_ma": target_ma, "first": first, "second": second}
    if a.empty or b.empty:
        payload["status"] = "missing_group"
        return payload
    ar = a.iloc[0].to_dict()
    br = b.iloc[0].to_dict()
    deltas = {}
    for metric in [
        "touch_rejection_rate",
        "failed_breakout_rate",
        "breakout_continuation_rate",
        "pullback_reacceleration_rate",
        "mean_ret20",
        "hit_rate20",
        "severe_loss20",
        "mean_dd20",
        "ma20_rebreak20",
        "target_ma_rebreak20",
        "higher_high20",
        "lower_low20",
    ]:
        if ar.get(metric) is not None and br.get(metric) is not None:
            deltas[f"{metric}_delta"] = ar[metric] - br[metric]
    payload.update({"status": "ready" if ar["event_count"] >= 200 and br["event_count"] >= 200 else "insufficient_sample", "first_metrics": ar, "second_metrics": br, "deltas": deltas})
    return payload


def _contrast(summary: pd.DataFrame) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    pairs = [
        ("close_break_above_strong", "high_touch_only_weak", "strong_break_vs_high_touch_weak"),
        ("close_break_above_strong", "close_break_above_weak", "strong_break_vs_weak_break"),
        ("high_touch_only_weak", "all_touch_events", "high_touch_weak_vs_all"),
        ("gap_touch_fade", "close_break_above_strong", "gap_touch_fade_vs_strong_break"),
        ("touch_with_lower_support", "touch_without_lower_support", "lower_support_vs_without"),
        ("target_ma_slope_up", "target_ma_slope_down", "target_slope_up_vs_down"),
        ("ma20_phase_1_10", "ma20_phase_15_20", "ma20_phase_1_10_vs_15_20"),
        ("ma20_phase_15_20", "ma20_phase_21_30", "ma20_phase_15_20_vs_21_30"),
    ]
    for target_ma in TARGET_MAS:
        for first, second, name in pairs:
            comparisons.append(_compare(summary, target_ma=target_ma, first=first, second=second, name=name))
    return {"axis_id": AXIS_ID, "required_contrasts": comparisons}


def _yearly(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    masks = _signal_masks(frame)
    for (year, target_ma), group in frame.groupby(["event_year", "target_ma"], sort=False):
        denom = len(group)
        for signal in ["all_touch_events", "close_break_above_strong", "high_touch_only_weak", "close_break_above_weak", "gap_touch_fade"]:
            local = group[masks[signal].loc[group.index]]
            row = _metric_row(local, target_ma=target_ma, observable_signal=signal, denominator=denom)
            row["event_year"] = int(year)
            row["sample_status"] = "sufficient" if row["event_count"] >= 100 else "insufficient_sample"
            rows.append(row)
    return pd.DataFrame(rows).sort_values(["target_ma", "observable_signal", "event_year"], kind="stable")


def _yearly_support(yearly: pd.DataFrame, signal: str, target_ma: str) -> dict[str, Any]:
    sig = yearly[(yearly["target_ma"] == target_ma) & (yearly["observable_signal"] == signal) & (yearly["sample_status"] == "sufficient")]
    all_rows = yearly[(yearly["target_ma"] == target_ma) & (yearly["observable_signal"] == "all_touch_events") & (yearly["sample_status"] == "sufficient")]
    supports = 0
    comparable = 0
    for year in sorted(set(sig["event_year"]).intersection(set(all_rows["event_year"]))):
        sr = sig[sig["event_year"] == year].iloc[0]
        ar = all_rows[all_rows["event_year"] == year].iloc[0]
        comparable += 1
        if signal in {"high_touch_only_weak", "gap_touch_fade", "close_break_above_weak"}:
            if sr["failed_breakout_rate"] + sr["touch_rejection_rate"] >= ar["failed_breakout_rate"] + ar["touch_rejection_rate"]:
                supports += 1
        else:
            if sr["mean_ret20"] >= ar["mean_ret20"] and sr["target_ma_rebreak20"] <= ar["target_ma_rebreak20"]:
                supports += 1
    return {"signal": signal, "target_ma": target_ma, "comparable_years": comparable, "supporting_years": supports, "stable": comparable >= 4 and supports >= max(3, comparable - 1)}


def _decision(contrast: dict[str, Any], yearly: pd.DataFrame) -> dict[str, Any]:
    weak_reasons: list[dict[str, Any]] = []
    buy_reasons: list[dict[str, Any]] = []
    for row in contrast["required_contrasts"]:
        if row.get("status") != "ready":
            continue
        d = row["deltas"]
        name = row["name"]
        target = row["target_ma"]
        if "high_touch_weak_vs_all" in name or "gap_touch_fade_vs_strong_break" in name:
            danger_delta = d.get("failed_breakout_rate_delta", 0) + d.get("touch_rejection_rate_delta", 0)
            if danger_delta > 0.05 or d.get("ma20_rebreak20_delta", 0) > 0.05 or d.get("mean_dd20_delta", 0) < -0.5:
                weak_reasons.append({"typed_reason": "observable_weak_touch_signal_identifies_risk", "contrast": name, "target_ma": target, "deltas": d, "yearly": _yearly_support(yearly, row["first"], target)})
        if "strong_break_vs_high_touch_weak" in name or "strong_break_vs_weak_break" in name:
            if d.get("mean_ret20_delta", 0) > 0.5 and d.get("target_ma_rebreak20_delta", 0) < -0.05 and d.get("failed_breakout_rate_delta", 0) < -0.05:
                buy_reasons.append({"typed_reason": "observable_strong_break_signal_improves_continuation", "contrast": name, "target_ma": target, "deltas": d, "yearly": _yearly_support(yearly, row["first"], target)})
    buy_stable = any(reason["yearly"]["stable"] for reason in buy_reasons)
    weak_stable = any(reason["yearly"]["stable"] for reason in weak_reasons)
    if buy_reasons and buy_stable:
        decision = "keep_for_buy_timing_pretest_next"
        reason = "observable_strong_break_signal_stably_improves_continuation"
    elif weak_reasons and weak_stable:
        decision = "keep_for_entry_guard_pretest_next"
        reason = "observable_weak_touch_signal_stably_identifies_risk"
    elif weak_reasons:
        decision = "keep_as_risk_warning"
        reason = "observable_weak_touch_signal_identifies_danger_but_stability_is_incomplete"
    else:
        decision = "drop"
        reason = "observable_touch_signals_do_not_separate_outcomes_enough"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "buy_timing_reasons": buy_reasons,
        "risk_warning_reasons": weak_reasons,
        "future_defined_reaction_type_used_as_input_signal": False,
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB write",
            "no ranking change",
            "no publish",
            "no candidate generation change",
            "no buy/sell rule promotion",
            "no bad-pick removal implementation",
            "no score tuning",
            "no threshold optimization",
            "no MA7 diagnostic",
            "no MA60 continuation diagnostic",
        ],
    }


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "target_events": "same MA60/MA100/MA200 upper-touch events from ma_touch_reaction_context_v1",
        "input_signal_rule": "observable signals use only event-day candle/MA/support/slope/phase fields; reaction_type is used only as an outcome label",
        "signals": {
            "close_break_above_strong": "close breaks above target MA, close position >= 0.6, not upper-shadow-heavy, and large bull or positive body",
            "high_touch_only_weak": "high touches target MA, close remains below, and upper shadow or weak close position",
            "close_break_above_weak": "close breaks above target MA but upper shadow/small body/doji/weak close",
            "gap_touch_fade": "gap up with weak close or upper shadow",
            "touch_with_lower_support": "lower support bucket light/medium/heavy",
            "touch_without_lower_support": "lower support bucket none_near",
            "target_ma_slope_context": "target MA slope bucket mapped to up/flat/down",
            "ma20_phase_context": "MA20 above-run 1-10 / 15-20 / 21-30 and raw buckets",
        },
        "outcome_labels": list(REACTIONS),
        "authoritative_input": str(DEFAULT_INPUT_PARQUET),
        "prior_reaction_events": str(DEFAULT_PRIOR_EVENTS),
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_audit = json.loads(args.input_audit.read_text(encoding="utf-8"))
    frame = _load_events(args.prior_events)
    summary = _summary(frame)
    context = _context_summary(frame)
    contrast = _contrast(summary)
    yearly = _yearly(frame)
    decision = _decision(contrast, yearly)
    examples = frame[
        frame["signal_close_break_above_strong"]
        | frame["signal_high_touch_only_weak"]
        | frame["signal_close_break_above_weak"]
        | frame["signal_gap_touch_fade"]
    ].head(5000)
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_parquet": str(args.input_parquet),
        "input_audit": str(args.input_audit),
        "prior_reaction_events": str(args.prior_events),
        "source_axis_id": source_audit.get("axis_id"),
        "confirmed_bars_only_inherited": bool(source_audit.get("confirmed_bars_only")),
        "runtime_db_write": False,
        "meemee_reflection": False,
        "ranking_change": False,
        "publish": False,
        "events_loaded": int(len(frame)),
        "unique_symbol_count": int(frame["code"].nunique()),
        "min_ymd": int(frame["ymd"].min()),
        "max_ymd": int(frame["ymd"].max()),
        "target_ma_counts": frame["target_ma"].value_counts().to_dict(),
        "reaction_type_counts": frame["reaction_type"].value_counts().to_dict(),
        "signal_counts": {col.replace("signal_", ""): int(frame[col].sum()) for col in frame.columns if col.startswith("signal_")},
        "future_defined_reaction_type_used_as_input_signal": False,
        "event_count_matches_prior_touch_event_count": int(len(frame)) == 64333,
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "observable_signal_definition.json", _definition())
    frame.to_csv(out_dir / "ma_touch_observable_signal_events.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "observable_signal_summary.csv", index=False, encoding="utf-8")
    context.to_csv(out_dir / "observable_signal_by_context.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "contrast_summary.json", contrast)
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    examples.to_csv(out_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete" if not missing else "incomplete", "missing_artifacts": missing, "authoritative_result": str(out_dir / "research_decision.json"), "generated_at_utc": datetime.now(timezone.utc).isoformat()})
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only observable upper MA touch signal diagnostic.")
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT_PARQUET)
    parser.add_argument("--input-audit", type=Path, default=DEFAULT_INPUT_AUDIT)
    parser.add_argument("--prior-events", type=Path, default=DEFAULT_PRIOR_EVENTS)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
