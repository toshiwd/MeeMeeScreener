from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma_touch_reaction_context_v1"
DEFAULT_FEATURE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_INPUT_PARQUET = DEFAULT_FEATURE_ROOT / "ma_phase_features.parquet"
DEFAULT_INPUT_AUDIT = DEFAULT_FEATURE_ROOT / "input_audit.json"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma_touch_reaction_context_v1")
TARGET_MAS = ("ma60", "ma100", "ma200")
HORIZONS = (5, 10, 20)
MA20_BUCKETS = ("1-5", "6-10", "10-14", "15-18", "19-20", "21-30")
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "touch_definition.json",
    "ma_touch_reaction_events.csv",
    "touch_reaction_summary.csv",
    "touch_reaction_by_context.csv",
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


def _base_columns() -> list[str]:
    cols = [
        "code",
        "ymd",
        "o",
        "h",
        "l",
        "c",
        "ma20",
        "ma60",
        "ma100",
        "ma200",
        "close_above_ma20",
        "above_ma20_run_bucket",
        "consecutive_bars_above_ma20",
        "upper_resistance_bucket",
        "nearest_upper_ma",
        "nearest_upper_ma_distance_pct",
        "lower_support_bucket",
        "nearest_lower_ma",
        "nearest_lower_ma_distance_pct",
        "ma20_slope_20d_bucket",
        "ma60_slope_20d_bucket",
        "ma100_slope_20d_bucket",
        "ma200_slope_20d_bucket",
        "ma20_gt_ma60",
        "ma_stack_state",
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
        "gap_up_pct",
        "gap_down_pct",
    ]
    for horizon in HORIZONS:
        cols.extend(
            [
                f"ret_{horizon}b",
                f"max_drawdown_{horizon}b",
                f"higher_high_made_{horizon}b",
                f"lower_low_made_{horizon}b",
                f"rebreak_ma20_{horizon}b",
                f"pullback_occurred_{horizon}b",
                f"recovered_after_pullback_{horizon}b",
                f"severe_loss_flag_{horizon}b",
            ]
        )
        for ma in TARGET_MAS:
            cols.extend([f"held_above_{ma}_{horizon}b", f"rebreak_{ma}_{horizon}b", f"recovered_{ma}_after_pullback_{horizon}b"])
    return cols


def _prior_below_within_5(frame: pd.DataFrame, target_ma: str) -> pd.Series:
    below = frame["c"].lt(frame[target_ma])
    return (
        below.groupby(frame["code"], sort=False)
        .transform(lambda s: s.shift(1).rolling(5, min_periods=1).max())
        .fillna(False)
        .astype(bool)
    )


def _load_events(path: Path) -> pd.DataFrame:
    base = pd.read_parquet(path, columns=_base_columns()).sort_values(["code", "ymd"], kind="stable")
    base = base[base["close_above_ma20"].fillna(False) & base["above_ma20_run_bucket"].isin(MA20_BUCKETS)].copy()
    base = base[base["ret_20b"].notna() & base["max_drawdown_20b"].notna()].copy()
    base["event_year"] = base["ymd"].astype(str).str.slice(0, 4).astype(int)
    body_range = pd.to_numeric(base["h"] - base["l"], errors="coerce").replace(0, float("nan"))
    base["close_position_in_range"] = pd.to_numeric((base["c"] - base["l"]) / body_range, errors="coerce")
    base["weak_close_position"] = base["close_position_in_range"].lt(0.4).fillna(False)
    base["gap_up"] = pd.to_numeric(base["gap_up_pct"], errors="coerce").fillna(0) > 0
    base["gap_down"] = pd.to_numeric(base["gap_down_pct"], errors="coerce").fillna(0) < 0

    parts: list[pd.DataFrame] = []
    for target_ma in TARGET_MAS:
        prior_below = _prior_below_within_5(base, target_ma)
        target_value = base[target_ma]
        high_touch = base["h"].ge(target_value)
        close_break = base["c"].ge(target_value)
        event = base[prior_below & (high_touch | close_break) & target_value.notna()].copy()
        event["target_ma"] = target_ma.upper()
        event["target_ma_value"] = event[target_ma]
        event["target_ma_slope_bucket"] = event[f"{target_ma}_slope_20d_bucket"]
        event["touch_method"] = "high_touch_only"
        event.loc[event["c"].ge(event[target_ma]), "touch_method"] = "close_break_above"
        event["target_ma_distance_pct"] = (event["c"] / event[target_ma] - 1.0) * 100.0
        for horizon in HORIZONS:
            event[f"target_ma_held_above_{horizon}b"] = event[f"held_above_{target_ma}_{horizon}b"]
            event[f"target_ma_rebreak_{horizon}b"] = event[f"rebreak_{target_ma}_{horizon}b"]
            event[f"target_ma_recovered_after_pullback_{horizon}b"] = event[f"recovered_{target_ma}_after_pullback_{horizon}b"]
        parts.append(event)
    events = pd.concat(parts, ignore_index=True)
    events["reaction_type"] = _reaction_type(events)
    return events.sort_values(["target_ma", "code", "ymd"], kind="stable")


def _reaction_type(events: pd.DataFrame) -> pd.Series:
    close_break = events["touch_method"].eq("close_break_above")
    weak_touch = events["is_upper_shadow_long"].fillna(False) | events["weak_close_position"].fillna(False) | events["is_shooting_star_like"].fillna(False)
    touch_rejection = (
        events["touch_method"].eq("high_touch_only")
        & weak_touch
        & ((events["ret_5b"] < 0) | (events["ret_10b"] < 0) | events["rebreak_ma20_10b"].fillna(False) | events["rebreak_ma20_20b"].fillna(False))
    )
    breakout_continuation = (
        close_break
        & events["target_ma_held_above_5b"].fillna(False)
        & events["target_ma_held_above_10b"].fillna(False)
        & events["target_ma_held_above_20b"].fillna(False)
        & (events["ret_10b"] > 0)
        & (events["ret_20b"] > 0)
        & ~events["severe_loss_flag_20b"].fillna(False)
    )
    failed_breakout = (
        close_break
        & (events["target_ma_rebreak_5b"].fillna(False) | events["target_ma_rebreak_10b"].fillna(False))
        & ((events["ret_20b"] < 0) | events["rebreak_ma20_20b"].fillna(False))
    )
    pullback_reaccel = (
        (events["pullback_occurred_10b"].fillna(False) | events["pullback_occurred_20b"].fillna(False))
        & (
            events["recovered_after_pullback_20b"].fillna(False)
            | events["target_ma_recovered_after_pullback_20b"].fillna(False)
            | (events["ret_20b"] > 0)
            | events["higher_high_made_20b"].fillna(False)
        )
    )
    result = pd.Series("unresolved", index=events.index, dtype="object")
    result.loc[touch_rejection] = "touch_rejection"
    result.loc[pullback_reaccel] = "touch_pullback_reacceleration"
    result.loc[failed_breakout] = "failed_breakout"
    result.loc[breakout_continuation] = "breakout_continuation"
    return result


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _metric_row(group: pd.DataFrame, *, target_ma: str, reaction_type: str, horizon: int, context_name: str | None = None, context_value: str | None = None, denominator: int | None = None) -> dict[str, Any]:
    ret = f"ret_{horizon}b"
    dd = f"max_drawdown_{horizon}b"
    valid = group[group[ret].notna()].copy()
    row = {
        "target_ma": target_ma,
        "reaction_type": reaction_type,
        "horizon": int(horizon),
        "event_count": int(len(valid)),
        "unique_symbol_count": int(valid["code"].nunique()) if not valid.empty else 0,
        "reaction_rate": None if not denominator else float(len(valid) / denominator),
        "mean_ret": _mean(valid[ret]),
        "median_ret": _median(valid[ret]),
        "hit_rate": _rate(valid[ret] > 0) if not valid.empty else None,
        "severe_loss_rate": _rate(valid[f"severe_loss_flag_{horizon}b"]) if not valid.empty else None,
        "mean_max_drawdown": _mean(valid[dd]),
        "median_max_drawdown": _median(valid[dd]),
        "ma20_rebreak_rate": _rate(valid[f"rebreak_ma20_{horizon}b"]) if not valid.empty else None,
        "target_ma_rebreak_rate": _rate(valid[f"target_ma_rebreak_{horizon}b"]) if not valid.empty else None,
        "higher_high_rate": _rate(valid[f"higher_high_made_{horizon}b"]) if not valid.empty else None,
        "lower_low_rate": _rate(valid[f"lower_low_made_{horizon}b"]) if not valid.empty else None,
        "pullback_occurred_rate": _rate(valid[f"pullback_occurred_{horizon}b"]) if not valid.empty else None,
        "recovered_after_pullback_rate": _rate(valid[f"recovered_after_pullback_{horizon}b"]) if not valid.empty else None,
    }
    if context_name is not None:
        row["context_name"] = context_name
        row["context_value"] = context_value
    return row


def _summary(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for target_ma, target_group in events.groupby("target_ma", sort=False):
        denom_by_target = len(target_group)
        for reaction_type, group in target_group.groupby("reaction_type", sort=False):
            for horizon in HORIZONS:
                rows.append(_metric_row(group, target_ma=target_ma, reaction_type=reaction_type, horizon=horizon, denominator=denom_by_target))
    return pd.DataFrame(rows)


def _context_summary(events: pd.DataFrame) -> pd.DataFrame:
    contexts = [
        "touch_method",
        "above_ma20_run_bucket",
        "upper_resistance_bucket",
        "lower_support_bucket",
        "ma20_slope_20d_bucket",
        "target_ma_slope_bucket",
        "ma20_gt_ma60",
        "ma_stack_state",
        "is_large_bull_body",
        "is_large_bear_body",
        "is_small_body",
        "is_doji_like",
        "is_upper_shadow_long",
        "is_lower_shadow_long",
        "is_shooting_star_like",
        "is_hammer_like",
        "is_engulfing_bull",
        "is_engulfing_bear",
        "gap_up",
        "gap_down",
    ]
    rows: list[dict[str, Any]] = []
    for context in contexts:
        for (target_ma, value, reaction_type), group in events.groupby(["target_ma", context, "reaction_type"], dropna=False, sort=False):
            denom = len(events[(events["target_ma"] == target_ma) & (events[context].astype(str) == str(value))])
            for horizon in HORIZONS:
                rows.append(_metric_row(group, target_ma=target_ma, reaction_type=reaction_type, horizon=horizon, context_name=context, context_value=str(value), denominator=denom))
    return pd.DataFrame(rows)


def _group_metric(events: pd.DataFrame, mask: pd.Series, name: str, horizon: int = 20) -> dict[str, Any]:
    group = events[mask]
    row = _metric_row(group, target_ma="ALL", reaction_type=name, horizon=horizon, denominator=len(events))
    row["variant"] = name
    return row


def _compare(a: dict[str, Any], b: dict[str, Any], name: str) -> dict[str, Any]:
    deltas = {}
    for metric in [
        "mean_ret",
        "hit_rate",
        "severe_loss_rate",
        "mean_max_drawdown",
        "ma20_rebreak_rate",
        "target_ma_rebreak_rate",
        "higher_high_rate",
        "lower_low_rate",
        "pullback_occurred_rate",
        "recovered_after_pullback_rate",
    ]:
        if a.get(metric) is not None and b.get(metric) is not None:
            deltas[f"{metric}_delta"] = a[metric] - b[metric]
    return {
        "name": name,
        "horizon": a["horizon"],
        "status": "ready" if a["event_count"] >= 200 and b["event_count"] >= 200 else "insufficient_sample",
        "first_metrics": a,
        "second_metrics": b,
        "deltas": deltas,
    }


def _contrast(events: pd.DataFrame) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    for target in ("MA60", "MA100", "MA200"):
        target_mask = events["target_ma"].eq(target)
        comparisons.append(
            _compare(
                _group_metric(events, target_mask & events["touch_method"].eq("close_break_above"), f"{target}_close_break_above"),
                _group_metric(events, target_mask & events["touch_method"].eq("high_touch_only"), f"{target}_high_touch_only"),
                f"{target}_close_break_above_vs_high_touch_only",
            )
        )
    ma60 = events["target_ma"].eq("MA60")
    comparisons.extend(
        [
            _compare(
                _group_metric(events, ma60 & events["is_large_bull_body"].fillna(False), "MA60_large_bull_close"),
                _group_metric(events, ma60 & (events["is_upper_shadow_long"].fillna(False) | events["weak_close_position"].fillna(False)), "MA60_upper_shadow_or_weak_close"),
                "MA60_large_bull_close_vs_upper_shadow_weak_close",
            ),
            _compare(
                _group_metric(events, ma60 & events["above_ma20_run_bucket"].eq("1-5"), "MA60_run_1_5"),
                _group_metric(events, ma60 & events["above_ma20_run_bucket"].isin(["15-18", "19-20"]), "MA60_run_15_20"),
                "MA60_run_1_5_vs_15_20",
            ),
            _compare(
                _group_metric(events, ma60 & events["above_ma20_run_bucket"].isin(["15-18", "19-20"]), "MA60_run_15_20"),
                _group_metric(events, ma60 & events["above_ma20_run_bucket"].eq("21-30"), "MA60_run_21_30"),
                "MA60_run_15_20_vs_21_30",
            ),
        ]
    )
    ma100 = events["target_ma"].eq("MA100")
    comparisons.extend(
        [
            _compare(
                _group_metric(events, ma100 & events["target_ma_slope_bucket"].isin(["weak_up", "strong_up"]), "MA100_slope_up"),
                _group_metric(events, ma100 & events["target_ma_slope_bucket"].isin(["flat", "weak_down", "strong_down"]), "MA100_slope_flat_down"),
                "MA100_target_slope_up_vs_flat_down",
            ),
            _compare(
                _group_metric(events, ma100 & events["lower_support_bucket"].isin(["light_support", "medium_support", "heavy_support"]), "MA100_lower_support_present"),
                _group_metric(events, ma100 & events["lower_support_bucket"].eq("none_near"), "MA100_lower_support_none"),
                "MA100_lower_support_present_vs_none",
            ),
        ]
    )
    ma200 = events["target_ma"].eq("MA200")
    for first, second in [("bullish_stack", "mixed_stack"), ("mixed_stack", "bearish_stack")]:
        comparisons.append(
            _compare(
                _group_metric(events, ma200 & events["ma_stack_state"].eq(first), f"MA200_{first}"),
                _group_metric(events, ma200 & events["ma_stack_state"].eq(second), f"MA200_{second}"),
                f"MA200_{first}_vs_{second}",
            )
        )
    heavy = events["upper_resistance_bucket"].isin(["medium_resistance", "heavy_resistance"])
    comparisons.extend(
        [
            _compare(
                _group_metric(events, heavy & events["reaction_type"].eq("touch_rejection"), "heavy_touch_rejection"),
                _group_metric(events, heavy & events["reaction_type"].eq("breakout_continuation"), "heavy_breakout_continuation"),
                "heavy_touch_rejection_vs_breakout_continuation",
            ),
            _compare(
                _group_metric(events, heavy & events["reaction_type"].eq("failed_breakout"), "heavy_failed_breakout"),
                _group_metric(events, heavy & events["reaction_type"].eq("touch_pullback_reacceleration"), "heavy_touch_pullback_reacceleration"),
                "heavy_failed_breakout_vs_touch_pullback_reacceleration",
            ),
        ]
    )
    return {"axis_id": AXIS_ID, "required_contrasts": comparisons}


def _yearly(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (year, target_ma, reaction_type), group in events.groupby(["event_year", "target_ma", "reaction_type"], sort=False):
        denom = len(events[(events["event_year"] == year) & (events["target_ma"] == target_ma)])
        for horizon in HORIZONS:
            row = _metric_row(group, target_ma=target_ma, reaction_type=reaction_type, horizon=horizon, denominator=denom)
            row["event_year"] = int(year)
            row["sample_status"] = "sufficient" if row["event_count"] >= 100 else "insufficient_sample"
            rows.append(row)
    return pd.DataFrame(rows).sort_values(["target_ma", "reaction_type", "event_year", "horizon"], kind="stable")


def _yearly_ok(yearly: pd.DataFrame, reaction_type: str) -> bool:
    subset = yearly[(yearly["reaction_type"] == reaction_type) & (yearly["horizon"] == 20) & (yearly["sample_status"] == "sufficient")]
    return subset["event_year"].nunique() >= 4


def _decision(contrast: dict[str, Any], yearly: pd.DataFrame) -> dict[str, Any]:
    ready = {row["name"]: row for row in contrast["required_contrasts"] if row.get("status") == "ready"}
    keep_buy: list[dict[str, Any]] = []
    keep_context: list[dict[str, Any]] = []
    keep_risk: list[dict[str, Any]] = []
    for name, row in ready.items():
        d = row["deltas"]
        if "close_break_above_vs_high_touch_only" in name:
            if d.get("mean_ret_delta", 0) > 0.2 and d.get("target_ma_rebreak_rate_delta", 0) < -0.05 and d.get("severe_loss_rate_delta", 0) <= 0.005:
                keep_buy.append({"typed_reason": "close_break_above_beats_high_touch_only", "contrast": name, "deltas": d})
            elif d.get("target_ma_rebreak_rate_delta", 0) < -0.05 or d.get("mean_ret_delta", 0) > 0.2:
                keep_context.append({"typed_reason": "touch_method_separates_outcomes", "contrast": name, "deltas": d})
        if "touch_rejection_vs_breakout_continuation" in name and d.get("mean_ret_delta", 0) is not None:
            if d.get("mean_ret_delta", 0) < -1.0 or d.get("ma20_rebreak_rate_delta", 0) > 0.05 or d.get("target_ma_rebreak_rate_delta", 0) > 0.05:
                keep_risk.append({"typed_reason": "touch_rejection_is_materially_worse_than_breakout_continuation", "contrast": name, "deltas": d})
        if "failed_breakout_vs_touch_pullback_reacceleration" in name:
            if d.get("mean_ret_delta", 0) < -1.0 or d.get("ma20_rebreak_rate_delta", 0) > 0.05:
                keep_risk.append({"typed_reason": "failed_breakout_is_materially_worse_than_pullback_reacceleration", "contrast": name, "deltas": d})
    breakout_stable = _yearly_ok(yearly, "breakout_continuation")
    reaccel_stable = _yearly_ok(yearly, "touch_pullback_reacceleration")
    if keep_buy and (breakout_stable or reaccel_stable):
        decision = "keep_for_buy_timing_pretest_next"
        reason = "touch_breakout_or_reacceleration_passes_pretest_gates"
    elif keep_risk:
        decision = "keep_as_risk_warning"
        reason = "touch_rejection_or_failed_breakout_materially_worsens_outcomes"
    elif keep_context:
        decision = "keep_as_context_feature"
        reason = "touch_reaction_separates_rebreak_or_return_but_not_enough_for_buy_signal"
    else:
        decision = "drop"
        reason = "touch_reaction_does_not_separate_outcomes_enough"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "buy_timing_reasons": keep_buy,
        "context_feature_reasons": keep_context,
        "risk_warning_reasons": keep_risk,
        "yearly_stability": {"breakout_continuation": breakout_stable, "touch_pullback_reacceleration": reaccel_stable},
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB write",
            "no ranking change",
            "no publish",
            "no candidate generation change",
            "no buy/sell rule promotion",
            "no bad-pick removal",
            "no MA7 phase diagnostic",
            "no MA60 continuation diagnostic",
            "no score tuning",
            "no threshold optimization",
        ],
    }


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "target_event": "MA20-positive rows in selected above-run buckets where price touched MA60/MA100/MA200 from below or near-below; prior close below target MA within previous 1-5 bars and event high >= target MA or close >= target MA",
        "reaction_type_priority": [
            "breakout_continuation overrides failed_breakout, pullback_reacceleration, touch_rejection when all conditions overlap",
            "failed_breakout overrides pullback_reacceleration and touch_rejection",
            "touch_pullback_reacceleration overrides touch_rejection",
            "otherwise touch_rejection or unresolved",
        ],
        "reaction_definitions": {
            "touch_rejection": "high_touch_only, weak candle/close, and ret5/ret10 negative or MA20 rebreak within 10/20 bars",
            "breakout_continuation": "close_break_above, held above target MA at 5/10/20 bars, ret10/ret20 positive, no severe loss at 20 bars",
            "failed_breakout": "close_break_above, target MA rebreak within 5/10 bars, and ret20 negative or MA20 rebreak at 20 bars",
            "touch_pullback_reacceleration": "pullback within 10/20 bars followed by MA20/target recovery, ret20 positive, or higher high by 20 bars",
            "unresolved": "conflicting or weak state under these definitions",
        },
        "target_mas": list(TARGET_MAS),
        "ma20_above_run_buckets": list(MA20_BUCKETS),
        "horizons": list(HORIZONS),
        "authoritative_input": str(DEFAULT_INPUT_PARQUET),
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_audit = json.loads(args.input_audit.read_text(encoding="utf-8"))
    events = _load_events(args.input_parquet)
    summary = _summary(events)
    context = _context_summary(events)
    contrast = _contrast(events)
    yearly = _yearly(events)
    decision = _decision(contrast, yearly)
    examples = events[events["reaction_type"].isin(["breakout_continuation", "failed_breakout", "touch_rejection", "touch_pullback_reacceleration"])].head(5000)
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
        "events_loaded": int(len(events)),
        "unique_symbol_count": int(events["code"].nunique()),
        "min_ymd": int(events["ymd"].min()) if not events.empty else None,
        "max_ymd": int(events["ymd"].max()) if not events.empty else None,
        "target_ma_counts": events["target_ma"].value_counts().to_dict(),
        "reaction_type_counts": events["reaction_type"].value_counts().to_dict(),
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "touch_definition.json", _definition())
    events.to_csv(out_dir / "ma_touch_reaction_events.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "touch_reaction_summary.csv", index=False, encoding="utf-8")
    context.to_csv(out_dir / "touch_reaction_by_context.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "contrast_summary.json", contrast)
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
    examples.to_csv(out_dir / "candidate_examples.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(
        out_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "status": "complete" if not missing else "incomplete",
            "missing_artifacts": missing,
            "authoritative_result": str(out_dir / "research_decision.json"),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only upper MA touch reaction diagnostic.")
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT_PARQUET)
    parser.add_argument("--input-audit", type=Path, default=DEFAULT_INPUT_AUDIT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
