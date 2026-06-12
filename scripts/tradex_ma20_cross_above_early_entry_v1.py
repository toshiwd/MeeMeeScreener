from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma20_cross_above_early_entry_v1"
DEFAULT_FEATURE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_INPUT_PARQUET = DEFAULT_FEATURE_ROOT / "ma_phase_features.parquet"
DEFAULT_INPUT_AUDIT = DEFAULT_FEATURE_ROOT / "input_audit.json"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma20_cross_above_early_entry_v1")
HORIZONS = (3, 5, 7, 10, 20)
MATURE_BUCKETS = ("10-14", "15-18", "19-20", "21-30")
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "entry_definition.json",
    "ma20_cross_above_events.csv",
    "variant_summary.csv",
    "context_summary.csv",
    "candle_context_summary.csv",
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
    cols = [
        "code",
        "ymd",
        "c",
        "ma20",
        "close_above_ma20",
        "cross_above_ma20_today",
        "bars_since_cross_above_ma20",
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
        "ma7_gt_ma20",
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
                f"held_above_ma20_{horizon}b",
                f"rebreak_ma20_{horizon}b",
                f"pullback_occurred_{horizon}b",
                f"recovered_after_pullback_{horizon}b",
                f"severe_loss_flag_{horizon}b",
            ]
        )
    frame = pd.read_parquet(path, columns=cols)
    frame = frame[frame["close_above_ma20"].fillna(False)].copy()
    frame = frame[frame["ret_20b"].notna() & frame["max_drawdown_20b"].notna()].copy()
    frame["event_year"] = frame["ymd"].astype(str).str.slice(0, 4).astype(int)
    frame["bars_since_cross_above_ma20"] = pd.to_numeric(frame["bars_since_cross_above_ma20"], errors="coerce")
    frame["phase_bucket"] = frame["above_ma20_run_bucket"].astype(str)
    frame["bars_since_cross_bucket"] = pd.cut(
        frame["bars_since_cross_above_ma20"],
        bins=[-0.5, 0.5, 2.5, 5.5, 7.5, 10.5, float("inf")],
        labels=["0", "1-2", "3-5", "6-7", "8-10", "11+"],
    ).astype("string")
    frame["early_0_5"] = frame["bars_since_cross_above_ma20"].between(0, 5, inclusive="both")
    frame["mature_15_20"] = frame["phase_bucket"].isin(["15-18", "19-20"])
    frame["mature_comparison_bucket"] = frame["phase_bucket"].isin(MATURE_BUCKETS)
    frame["no_light_upper_resistance"] = frame["upper_resistance_bucket"].isin(["none_near", "light_resistance"])
    frame["medium_heavy_upper_resistance"] = frame["upper_resistance_bucket"].isin(["medium_resistance", "heavy_resistance"])
    frame["has_lower_support"] = frame["lower_support_bucket"].isin(["light_support", "medium_support", "heavy_support"])
    frame["ma20_slope_up"] = frame["ma20_slope_20d_bucket"].isin(["weak_up", "strong_up"])
    frame["ma60_not_strong_down"] = ~frame["ma60_slope_20d_bucket"].eq("strong_down")
    frame["ma60_strong_down"] = frame["ma60_slope_20d_bucket"].eq("strong_down")
    frame["cross_day_weak_close"] = frame["is_upper_shadow_long"].fillna(False) | frame["is_small_body"].fillna(False) | frame["is_doji_like"].fillna(False)
    frame["gap_up"] = pd.to_numeric(frame["gap_up_pct"], errors="coerce").fillna(0) > 0
    frame["gap_down"] = pd.to_numeric(frame["gap_down_pct"], errors="coerce").fillna(0) < 0
    frame["variant_baseline_all_ma20_above"] = True
    frame["variant_cross_day_all"] = frame["cross_above_ma20_today"].fillna(False)
    frame["variant_cross_early_1_5"] = frame["early_0_5"]
    frame["variant_cross_early_1_5_no_light_upper_resistance"] = frame["early_0_5"] & frame["no_light_upper_resistance"]
    frame["variant_cross_early_1_5_no_light_upper_resistance_slope_up"] = frame["variant_cross_early_1_5_no_light_upper_resistance"] & frame["ma20_slope_up"] & frame["ma60_not_strong_down"]
    frame["variant_cross_early_1_5_heavy_resistance_negative_control"] = frame["early_0_5"] & frame["medium_heavy_upper_resistance"]
    frame["variant_mature_15_20_no_light_upper_resistance"] = frame["mature_15_20"] & frame["no_light_upper_resistance"]
    return frame


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    return float(valid.astype(bool).mean())


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _metric_row(group: pd.DataFrame, *, variant: str, horizon: int, context_name: str | None = None, context_value: str | None = None) -> dict[str, Any]:
    ret = f"ret_{horizon}b"
    dd = f"max_drawdown_{horizon}b"
    valid = group[group[ret].notna()].copy()
    row = {
        "variant": variant,
        "horizon": int(horizon),
        "event_count": int(len(valid)),
        "unique_symbol_count": int(valid["code"].nunique()) if not valid.empty else 0,
        "mean_ret": _mean(valid[ret]),
        "median_ret": _median(valid[ret]),
        "hit_rate": _rate(valid[ret] > 0) if not valid.empty else None,
        "severe_loss_rate": _rate(valid[f"severe_loss_flag_{horizon}b"]) if not valid.empty else None,
        "mean_max_drawdown": _mean(valid[dd]),
        "median_max_drawdown": _median(valid[dd]),
        "held_above_ma20_rate": _rate(valid[f"held_above_ma20_{horizon}b"]) if not valid.empty else None,
        "rebreak_ma20_rate": _rate(valid[f"rebreak_ma20_{horizon}b"]) if not valid.empty else None,
        "pullback_occurred_rate": _rate(valid[f"pullback_occurred_{horizon}b"]) if not valid.empty else None,
        "recovered_after_pullback_rate": _rate(valid[f"recovered_after_pullback_{horizon}b"]) if not valid.empty else None,
        "higher_high_rate": _rate(valid[f"higher_high_made_{horizon}b"]) if not valid.empty else None,
        "lower_low_rate": _rate(valid[f"lower_low_made_{horizon}b"]) if not valid.empty else None,
    }
    if context_name is not None:
        row["context_name"] = context_name
        row["context_value"] = context_value
    return row


def _variant_summary(frame: pd.DataFrame) -> pd.DataFrame:
    variants = {
        "baseline_all_ma20_above": frame,
        "cross_day_all": frame[frame["variant_cross_day_all"]],
        "cross_early_1_5": frame[frame["variant_cross_early_1_5"]],
        "cross_early_1_5_no_light_upper_resistance": frame[frame["variant_cross_early_1_5_no_light_upper_resistance"]],
        "cross_early_1_5_no_light_upper_resistance_slope_up": frame[frame["variant_cross_early_1_5_no_light_upper_resistance_slope_up"]],
        "cross_early_1_5_heavy_resistance_negative_control": frame[frame["variant_cross_early_1_5_heavy_resistance_negative_control"]],
        "mature_15_20_no_light_upper_resistance": frame[frame["variant_mature_15_20_no_light_upper_resistance"]],
    }
    return pd.DataFrame([_metric_row(group, variant=variant, horizon=horizon) for variant, group in variants.items() for horizon in HORIZONS])


def _context_summary(frame: pd.DataFrame) -> pd.DataFrame:
    early = frame[frame["variant_cross_early_1_5"]]
    contexts = [
        "bars_since_cross_bucket",
        "upper_resistance_bucket",
        "nearest_upper_ma",
        "lower_support_bucket",
        "nearest_lower_ma",
        "ma20_slope_20d_bucket",
        "ma60_slope_20d_bucket",
        "ma7_gt_ma20",
        "ma20_gt_ma60",
        "ma_stack_state",
        "has_lower_support",
        "ma20_slope_up",
        "ma60_not_strong_down",
    ]
    rows: list[dict[str, Any]] = []
    for context in contexts:
        for value, group in early.groupby(context, dropna=False):
            for horizon in HORIZONS:
                rows.append(_metric_row(group, variant="cross_early_1_5", horizon=horizon, context_name=context, context_value=str(value)))
    return pd.DataFrame(rows)


def _candle_context_summary(frame: pd.DataFrame) -> pd.DataFrame:
    cross_day = frame[frame["variant_cross_day_all"]]
    contexts = [
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
        "gap_up",
        "gap_down",
        "cross_day_weak_close",
    ]
    rows: list[dict[str, Any]] = []
    for context in contexts:
        for value, group in cross_day.groupby(context, dropna=False):
            for horizon in HORIZONS:
                rows.append(_metric_row(group, variant="cross_day_all", horizon=horizon, context_name=context, context_value=str(value)))
    return pd.DataFrame(rows)


def _compare(summary: pd.DataFrame, *, first: str, second: str, name: str, horizon: int = 20) -> dict[str, Any]:
    a = summary[(summary["variant"] == first) & (summary["horizon"] == horizon)]
    b = summary[(summary["variant"] == second) & (summary["horizon"] == horizon)]
    payload: dict[str, Any] = {"name": name, "horizon": horizon, "first": first, "second": second}
    if a.empty or b.empty:
        payload["status"] = "missing_group"
        return payload
    ar = a.iloc[0].to_dict()
    br = b.iloc[0].to_dict()
    deltas = {}
    for metric in [
        "mean_ret",
        "hit_rate",
        "severe_loss_rate",
        "mean_max_drawdown",
        "held_above_ma20_rate",
        "rebreak_ma20_rate",
        "pullback_occurred_rate",
        "recovered_after_pullback_rate",
        "higher_high_rate",
        "lower_low_rate",
    ]:
        if ar.get(metric) is not None and br.get(metric) is not None:
            deltas[f"{metric}_delta"] = ar[metric] - br[metric]
    payload.update({"status": "ready" if ar["event_count"] >= 200 and br["event_count"] >= 200 else "insufficient_sample", "first_metrics": ar, "second_metrics": br, "deltas": deltas})
    return payload


def _contrast(frame: pd.DataFrame, summary: pd.DataFrame) -> dict[str, Any]:
    comparisons = [
        _compare(summary, first="cross_early_1_5", second="baseline_all_ma20_above", name="cross_early_1_5_vs_baseline", horizon=20),
        _compare(summary, first="cross_early_1_5", second="mature_15_20_no_light_upper_resistance", name="cross_early_1_5_vs_mature_15_20_no_light", horizon=20),
        _compare(summary, first="cross_early_1_5_no_light_upper_resistance", second="cross_early_1_5_heavy_resistance_negative_control", name="early_no_light_vs_heavy_resistance", horizon=20),
        _compare(summary, first="cross_early_1_5_no_light_upper_resistance_slope_up", second="cross_early_1_5_no_light_upper_resistance", name="early_no_light_slope_up_vs_no_light", horizon=20),
    ]
    local_groups = {
        "cross_day_large_bull_body": frame[frame["variant_cross_day_all"] & frame["is_large_bull_body"].fillna(False)],
        "cross_day_upper_shadow_or_weak_close": frame[frame["variant_cross_day_all"] & frame["cross_day_weak_close"]],
        "early_ma20_lt_ma60": frame[frame["variant_cross_early_1_5"] & ~frame["ma20_gt_ma60"].fillna(False)],
        "early_ma20_gt_ma60": frame[frame["variant_cross_early_1_5"] & frame["ma20_gt_ma60"].fillna(False)],
        "early_lower_support_present": frame[frame["variant_cross_early_1_5"] & frame["has_lower_support"]],
        "early_lower_support_absent": frame[frame["variant_cross_early_1_5"] & ~frame["has_lower_support"]],
    }
    local_summary = pd.DataFrame([_metric_row(group, variant=name, horizon=20) for name, group in local_groups.items()])
    comparisons.extend(
        [
            _compare(local_summary, first="cross_day_large_bull_body", second="cross_day_upper_shadow_or_weak_close", name="cross_day_large_bull_vs_upper_shadow_or_weak_close", horizon=20),
            _compare(local_summary, first="early_ma20_lt_ma60", second="early_ma20_gt_ma60", name="cross_early_ma20_lt_ma60_vs_gt_ma60", horizon=20),
            _compare(local_summary, first="early_lower_support_present", second="early_lower_support_absent", name="cross_early_lower_support_present_vs_absent", horizon=20),
        ]
    )
    return {"axis_id": AXIS_ID, "required_contrasts": comparisons}


def _yearly(frame: pd.DataFrame) -> pd.DataFrame:
    variants = {
        "baseline_all_ma20_above": frame,
        "cross_early_1_5": frame[frame["variant_cross_early_1_5"]],
        "cross_early_1_5_no_light_upper_resistance": frame[frame["variant_cross_early_1_5_no_light_upper_resistance"]],
        "cross_early_1_5_heavy_resistance_negative_control": frame[frame["variant_cross_early_1_5_heavy_resistance_negative_control"]],
        "mature_15_20_no_light_upper_resistance": frame[frame["variant_mature_15_20_no_light_upper_resistance"]],
    }
    rows: list[dict[str, Any]] = []
    for variant, source in variants.items():
        for year, group in source.groupby("event_year"):
            for horizon in HORIZONS:
                row = _metric_row(group, variant=variant, horizon=horizon)
                row["event_year"] = int(year)
                row["sample_status"] = "sufficient" if row["event_count"] >= 100 else "insufficient_sample"
                rows.append(row)
    return pd.DataFrame(rows).sort_values(["variant", "event_year", "horizon"], kind="stable")


def _yearly_ok(yearly: pd.DataFrame) -> bool:
    early = yearly[(yearly["variant"] == "cross_early_1_5") & (yearly["horizon"] == 20) & (yearly["sample_status"] == "sufficient")]
    base = yearly[(yearly["variant"] == "baseline_all_ma20_above") & (yearly["horizon"] == 20) & (yearly["sample_status"] == "sufficient")]
    supports = 0
    comparable = 0
    for year in sorted(set(early["event_year"].astype(int)).intersection(set(base["event_year"].astype(int)))):
        er = early[early["event_year"] == year].iloc[0]
        br = base[base["event_year"] == year].iloc[0]
        comparable += 1
        if (er["mean_ret"] >= br["mean_ret"]) and (er["severe_loss_rate"] <= br["severe_loss_rate"] + 0.005):
            supports += 1
    return comparable >= 3 and supports >= max(2, comparable - 1)


def _decision(contrast: dict[str, Any], yearly: pd.DataFrame) -> dict[str, Any]:
    comps = {row["name"]: row for row in contrast["required_contrasts"] if row.get("status") == "ready"}
    early_base = comps.get("cross_early_1_5_vs_baseline")
    early_mature = comps.get("cross_early_1_5_vs_mature_15_20_no_light")
    no_light_heavy = comps.get("early_no_light_vs_heavy_resistance")
    slope_add = comps.get("early_no_light_slope_up_vs_no_light")
    yearly_ok = _yearly_ok(yearly)
    buy_reasons: list[dict[str, Any]] = []
    context_reasons: list[dict[str, Any]] = []
    negative_reasons: list[dict[str, Any]] = []
    if early_base and early_mature:
        d_base = early_base["deltas"]
        d_mature = early_mature["deltas"]
        ret_improves = d_base.get("mean_ret_delta", 0) > 0 and d_mature.get("mean_ret_delta", 0) > 0
        risk_ok = d_base.get("severe_loss_rate_delta", 0) <= 0.005 and d_base.get("mean_max_drawdown_delta", 0) >= -0.2
        rebreak_ok = d_base.get("rebreak_ma20_rate_delta", 0) <= 0.03
        if ret_improves and risk_ok and rebreak_ok and yearly_ok:
            buy_reasons.append({"typed_reason": "early_cross_outperforms_baseline_and_mature_with_acceptable_risk", "early_vs_baseline": d_base, "early_vs_mature": d_mature, "yearly_ok": yearly_ok})
        elif ret_improves or rebreak_ok:
            context_reasons.append({"typed_reason": "early_cross_has_partial_entry_context_value_but_risk_or_stability_is_weak", "early_vs_baseline": d_base, "early_vs_mature": d_mature, "yearly_ok": yearly_ok})
    if no_light_heavy:
        d = no_light_heavy["deltas"]
        if d.get("rebreak_ma20_rate_delta", 0) <= -0.05 or d.get("mean_ret_delta", 0) >= 0.2:
            context_reasons.append({"typed_reason": "upper_resistance_filter_separates_early_cross_outcomes", "no_light_vs_heavy": d})
        if d.get("mean_ret_delta", 0) > 0.2 and d.get("severe_loss_rate_delta", 0) <= 0.005:
            negative_reasons.append({"typed_reason": "heavy_resistance_is_materially_worse_than_no_light_context", "no_light_vs_heavy": d})
    if slope_add:
        d = slope_add["deltas"]
        if d.get("mean_ret_delta", 0) > 0 or d.get("severe_loss_rate_delta", 0) < 0:
            context_reasons.append({"typed_reason": "slope_filter_adds_some_context_value", "slope_vs_no_light": d})
    if buy_reasons:
        decision = "keep_for_buy_timing_candidate_next"
        reason = "early_cross_phase_passes_buy_timing_candidate_gates"
    elif negative_reasons:
        decision = "keep_as_negative_filter"
        reason = "early_cross_heavy_resistance_or_weak_context_is_materially_worse"
    elif context_reasons:
        decision = "keep_as_entry_context_feature"
        reason = "early_cross_context_has_partial_value_but_not_enough_for_buy_signal"
    else:
        decision = "drop"
        reason = "early_cross_phase_does_not_outperform_baseline_or_mature_context"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "buy_timing_reasons": buy_reasons,
        "entry_context_reasons": context_reasons,
        "negative_filter_reasons": negative_reasons,
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB write",
            "no ranking change",
            "no publish",
            "no candidate generation change",
            "no buy/sell rule promotion",
            "no bad-pick removal",
            "no MA7 phase diagnostic",
            "no MA60 phase diagnostic",
            "no score tuning",
            "no threshold optimization",
            "no MA touch reaction diagnostic",
        ],
    }


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "target_events": {
            "baseline_all_ma20_above": "all rows where close_above_ma20 is true with valid ret20 and max_drawdown20",
            "cross_day_all": "cross_above_ma20_today true",
            "cross_early_1_5": "bars_since_cross_above_ma20 between 0 and 5 inclusive",
            "mature_comparison": "consecutive bars above MA20 buckets 10-14, 15-18, 19-20, 21-30; required mature variant uses 15-18/19-20 no/light upper resistance",
        },
        "context_splits": [
            "upper_resistance_bucket",
            "nearest_upper_ma",
            "lower_support_bucket",
            "nearest_lower_ma",
            "ma20_slope_20d_bucket",
            "ma60_slope_20d_bucket",
            "ma7_gt_ma20",
            "ma20_gt_ma60",
            "ma_stack_state",
            "candle_context_on_cross_day",
            "gap_up_pct/gap_down_pct",
        ],
        "horizons": list(HORIZONS),
        "authoritative_input": str(DEFAULT_INPUT_PARQUET),
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_audit = json.loads(args.input_audit.read_text(encoding="utf-8"))
    frame = _load_events(args.input_parquet)
    summary = _variant_summary(frame)
    context = _context_summary(frame)
    candle_context = _candle_context_summary(frame)
    contrast = _contrast(frame, summary)
    yearly = _yearly(frame)
    decision = _decision(contrast, yearly)
    examples = frame[
        frame["variant_cross_early_1_5_no_light_upper_resistance"]
        | frame["variant_cross_early_1_5_heavy_resistance_negative_control"]
        | frame["variant_mature_15_20_no_light_upper_resistance"]
    ].head(5000)
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
        "events_loaded": int(len(frame)),
        "unique_symbol_count": int(frame["code"].nunique()),
        "min_ymd": int(frame["ymd"].min()),
        "max_ymd": int(frame["ymd"].max()),
        "variant_counts": {col.replace("variant_", ""): int(frame[col].sum()) for col in frame.columns if col.startswith("variant_")},
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "entry_definition.json", _definition())
    frame.to_csv(out_dir / "ma20_cross_above_events.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "variant_summary.csv", index=False, encoding="utf-8")
    context.to_csv(out_dir / "context_summary.csv", index=False, encoding="utf-8")
    candle_context.to_csv(out_dir / "candle_context_summary.csv", index=False, encoding="utf-8")
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
    parser = argparse.ArgumentParser(description="TRADEX read-only MA20 cross-above early entry diagnostic.")
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT_PARQUET)
    parser.add_argument("--input-audit", type=Path, default=DEFAULT_INPUT_AUDIT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
