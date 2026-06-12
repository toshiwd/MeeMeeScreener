from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma20_above_run_phase_timing_v1"
DEFAULT_FEATURE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_INPUT_PARQUET = DEFAULT_FEATURE_ROOT / "ma_phase_features.parquet"
DEFAULT_INPUT_AUDIT = DEFAULT_FEATURE_ROOT / "input_audit.json"
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma20_above_run_phase_timing_v1")
PHASE_BUCKETS = ("1-2", "3-4", "5-6", "7-9", "10-14", "15-18", "19-20", "21-30", "31-40", "41-60", "61+")
HORIZONS = (3, 5, 7, 10, 20)
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "phase_definition.json",
    "ma20_phase_events.csv",
    "ma20_phase_summary_by_bucket.csv",
    "ma20_phase_by_resistance_support.csv",
    "ma20_phase_by_slope_stack.csv",
    "ma20_phase_by_candle.csv",
    "contrast_summary.json",
    "yearly_stability_summary.csv",
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


def _load_feature_rows(path: Path) -> pd.DataFrame:
    base_cols = [
        "code",
        "ymd",
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
        "ma20_slope_5d_bucket",
        "ma20_slope_20d_bucket",
        "ma60_slope_20d_bucket",
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
        "is_inside_bar",
        "is_outside_bar",
    ]
    outcome_cols: list[str] = []
    for horizon in HORIZONS:
        outcome_cols.extend(
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
    columns = base_cols + outcome_cols
    frame = pd.read_parquet(path, columns=columns)
    frame = frame[frame["close_above_ma20"].fillna(False) & frame["ma20"].notna()].copy()
    frame = frame[frame["above_ma20_run_bucket"].isin([b for b in PHASE_BUCKETS if b != "61+"] + ["41-60", "61-80", "81-100", "101+"])]
    frame["phase_bucket"] = frame["above_ma20_run_bucket"].astype(str)
    frame.loc[frame["phase_bucket"].isin(["61-80", "81-100", "101+"]), "phase_bucket"] = "61+"
    frame["event_year"] = frame["ymd"].astype(str).str.slice(0, 4).astype(int)
    frame["upper_resistance_group"] = frame["upper_resistance_bucket"].map(
        {
            "none_near": "no_low_upper_resistance",
            "light_resistance": "no_low_upper_resistance",
            "medium_resistance": "medium_heavy_upper_resistance",
            "heavy_resistance": "medium_heavy_upper_resistance",
        }
    ).fillna("unknown")
    frame["lower_support_group"] = frame["lower_support_bucket"].map(
        {
            "none_near": "no_lower_support",
            "light_support": "lower_support",
            "medium_support": "lower_support",
            "heavy_support": "lower_support",
        }
    ).fillna("unknown")
    frame["ma20_slope_up"] = frame["ma20_slope_20d_bucket"].isin(["weak_up", "strong_up"])
    frame["ma60_slope_up"] = frame["ma60_slope_20d_bucket"].isin(["weak_up", "strong_up"])
    frame["ma20_vs_ma60"] = frame["ma20_gt_ma60"].map({True: "ma20_gt_ma60", False: "ma20_lt_ma60"}).fillna("unknown")
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


def _metric_row(group: pd.DataFrame, *, split_axis: str, split_value: str, phase_bucket: str, horizon: int) -> dict[str, Any]:
    ret_col = f"ret_{horizon}b"
    dd_col = f"max_drawdown_{horizon}b"
    valid = group[group[ret_col].notna()].copy()
    return {
        "split_axis": split_axis,
        "split_value": split_value,
        "phase_bucket": phase_bucket,
        "horizon": int(horizon),
        "event_count": int(len(valid)),
        "unique_symbol_count": int(valid["code"].nunique()) if not valid.empty else 0,
        "mean_ret_horizon": _mean(valid[ret_col]),
        "median_ret_horizon": _median(valid[ret_col]),
        "hit_rate": _rate(valid[ret_col] > 0) if not valid.empty else None,
        "severe_loss_rate": _rate(valid[f"severe_loss_flag_{horizon}b"]) if not valid.empty else None,
        "mean_max_drawdown": _mean(valid[dd_col]),
        "median_max_drawdown": _median(valid[dd_col]),
        "higher_high_rate": _rate(valid[f"higher_high_made_{horizon}b"]) if not valid.empty else None,
        "lower_low_rate": _rate(valid[f"lower_low_made_{horizon}b"]) if not valid.empty else None,
        "held_above_ma20_rate": _rate(valid[f"held_above_ma20_{horizon}b"]) if not valid.empty else None,
        "rebreak_ma20_rate": _rate(valid[f"rebreak_ma20_{horizon}b"]) if not valid.empty else None,
        "pullback_occurred_rate": _rate(valid[f"pullback_occurred_{horizon}b"]) if not valid.empty else None,
        "recovered_after_pullback_rate": _rate(valid[f"recovered_after_pullback_{horizon}b"]) if not valid.empty else None,
    }


def _summarize(frame: pd.DataFrame, split_axis: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for phase_bucket, phase_group in frame.groupby("phase_bucket", sort=False):
        if split_axis == "all":
            for horizon in HORIZONS:
                rows.append(_metric_row(phase_group, split_axis="all", split_value="all", phase_bucket=str(phase_bucket), horizon=horizon))
        else:
            for split_value, group in phase_group.groupby(split_axis, dropna=False):
                for horizon in HORIZONS:
                    rows.append(_metric_row(group, split_axis=split_axis, split_value=str(split_value), phase_bucket=str(phase_bucket), horizon=horizon))
    return pd.DataFrame(rows).sort_values(["split_axis", "split_value", "phase_bucket", "horizon"], kind="stable")


def _phase_group(frame: pd.DataFrame, buckets: set[str], extra_mask: pd.Series | None = None) -> pd.DataFrame:
    mask = frame["phase_bucket"].isin(buckets)
    if extra_mask is not None:
        mask &= extra_mask
    return frame[mask]


def _compare_groups(frame: pd.DataFrame, *, name: str, first: pd.DataFrame, second: pd.DataFrame, horizon: int = 20) -> dict[str, Any]:
    a = _metric_row(first, split_axis="contrast", split_value=name + ":first", phase_bucket="custom", horizon=horizon)
    b = _metric_row(second, split_axis="contrast", split_value=name + ":second", phase_bucket="custom", horizon=horizon)
    deltas = {}
    for metric in [
        "mean_ret_horizon",
        "hit_rate",
        "severe_loss_rate",
        "mean_max_drawdown",
        "higher_high_rate",
        "held_above_ma20_rate",
        "rebreak_ma20_rate",
        "pullback_occurred_rate",
        "recovered_after_pullback_rate",
    ]:
        if a.get(metric) is not None and b.get(metric) is not None:
            deltas[f"{metric}_delta"] = a[metric] - b[metric]
    status = "ready" if a["event_count"] >= 200 and b["event_count"] >= 200 else "insufficient_sample"
    return {"name": name, "horizon": horizon, "status": status, "first_metrics": a, "second_metrics": b, "deltas": deltas}


def _contrast(frame: pd.DataFrame) -> dict[str, Any]:
    b15_18 = _phase_group(frame, {"15-18"})
    b1_6 = _phase_group(frame, {"1-2", "3-4", "5-6"})
    b19_20 = _phase_group(frame, {"19-20"})
    b21_30 = _phase_group(frame, {"21-30"})
    no_low_res = frame["upper_resistance_group"].eq("no_low_upper_resistance")
    med_heavy_res = frame["upper_resistance_group"].eq("medium_heavy_upper_resistance")
    lower_support = frame["lower_support_group"].eq("lower_support")
    no_support = frame["lower_support_group"].eq("no_lower_support")
    comparisons = [
        _compare_groups(frame, name="phase_15_18_vs_1_6", first=b15_18, second=b1_6),
        _compare_groups(frame, name="phase_15_18_vs_19_20", first=b15_18, second=b19_20),
        _compare_groups(frame, name="phase_19_20_vs_21_30", first=b19_20, second=b21_30),
        _compare_groups(frame, name="phase_15_18_no_low_resistance_vs_medium_heavy_resistance", first=_phase_group(frame, {"15-18"}, no_low_res), second=_phase_group(frame, {"15-18"}, med_heavy_res)),
        _compare_groups(frame, name="phase_15_18_lower_support_vs_no_lower_support", first=_phase_group(frame, {"15-18"}, lower_support), second=_phase_group(frame, {"15-18"}, no_support)),
        _compare_groups(frame, name="phase_19_20_upper_resistance_vs_no_upper_resistance", first=_phase_group(frame, {"19-20"}, med_heavy_res), second=_phase_group(frame, {"19-20"}, no_low_res)),
    ]
    for split_name, col in [
        ("ma20_slope_up", "ma20_slope_up"),
        ("ma20_vs_ma60", "ma20_vs_ma60"),
    ]:
        for split_value, group in frame.groupby(col, dropna=False):
            comparisons.append(
                _compare_groups(
                    frame,
                    name=f"{split_name}_{split_value}_phase_15_18_vs_19_20",
                    first=_phase_group(group, {"15-18"}),
                    second=_phase_group(group, {"19-20"}),
                )
            )
    candle_cols = [
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
        "is_inside_bar",
        "is_outside_bar",
    ]
    for candle in candle_cols:
        group = frame[frame[candle].fillna(False)]
        if len(group) >= 200:
            comparisons.append(
                _compare_groups(
                    frame,
                    name=f"candle_{candle}_phase_15_18_vs_19_20",
                    first=_phase_group(group, {"15-18"}),
                    second=_phase_group(group, {"19-20"}),
                )
            )
    return {"axis_id": AXIS_ID, "required_contrasts": comparisons}


def _yearly(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for year, year_group in frame.groupby("event_year"):
        for phase_bucket in ("15-18", "19-20", "1-2", "3-4", "5-6", "21-30"):
            group = year_group[year_group["phase_bucket"].eq(phase_bucket)]
            for horizon in (5, 10, 20):
                row = _metric_row(group, split_axis="year", split_value=str(year), phase_bucket=phase_bucket, horizon=horizon)
                row["event_year"] = int(year)
                row["sample_status"] = "sufficient" if row["event_count"] >= 100 else "insufficient_sample"
                rows.append(row)
    return pd.DataFrame(rows).sort_values(["event_year", "phase_bucket", "horizon"], kind="stable")


def _decision(contrast: dict[str, Any], yearly: pd.DataFrame) -> dict[str, Any]:
    comparisons = {row["name"]: row for row in contrast["required_contrasts"] if row["status"] == "ready"}
    reasons: list[dict[str, Any]] = []
    context_reasons: list[dict[str, Any]] = []
    c1 = comparisons.get("phase_15_18_vs_1_6")
    c2 = comparisons.get("phase_15_18_vs_19_20")
    c3 = comparisons.get("phase_19_20_vs_21_30")
    if c1 and c2:
        d1 = c1["deltas"]
        d2 = c2["deltas"]
        phase_good = (
            d1.get("mean_ret_horizon_delta", 0) > 0
            and d1.get("held_above_ma20_rate_delta", 0) >= 0
            and d2.get("rebreak_ma20_rate_delta", 0) <= 0
        )
        yearly_ok = _yearly_supports(yearly)
        if phase_good and yearly_ok:
            reasons.append({"typed_reason": "phase_15_18_has_better_continuation_than_early_or_19_20", "phase_15_18_vs_1_6": d1, "phase_15_18_vs_19_20": d2, "yearly_ok": yearly_ok})
    for name in [
        "phase_15_18_no_low_resistance_vs_medium_heavy_resistance",
        "phase_15_18_lower_support_vs_no_lower_support",
        "phase_19_20_upper_resistance_vs_no_upper_resistance",
    ]:
        comp = comparisons.get(name)
        if not comp:
            continue
        d = comp["deltas"]
        if (
            abs(d.get("mean_ret_horizon_delta", 0)) >= 0.3
            or abs(d.get("rebreak_ma20_rate_delta", 0)) >= 0.02
            or abs(d.get("pullback_occurred_rate_delta", 0)) >= 0.02
        ):
            context_reasons.append({"typed_reason": name, "deltas": d})
    if reasons:
        decision = "keep_for_buy_timing_pretest_next"
        reason = "phase_15_18_timing_has_continuation_edge_with_yearly_support"
    elif context_reasons:
        decision = "keep_as_context_feature"
        reason = "phase_bucket_alone_is_weak_but_resistance_support_or_slope_context_adds_separation"
    else:
        decision = "drop"
        reason = "phase_buckets_and_context_do_not_separate_outcomes"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "buy_timing_reasons": reasons,
        "context_feature_reasons": context_reasons,
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
        ],
    }


def _yearly_supports(yearly: pd.DataFrame) -> bool:
    supports = 0
    comparable = 0
    for year in sorted(yearly["event_year"].unique()):
        y = yearly[(yearly["event_year"] == year) & (yearly["horizon"] == 20)]
        a = y[y["phase_bucket"].eq("15-18")]
        b = y[y["phase_bucket"].eq("19-20")]
        if a.empty or b.empty or a.iloc[0]["sample_status"] != "sufficient" or b.iloc[0]["sample_status"] != "sufficient":
            continue
        comparable += 1
        if (a.iloc[0]["mean_ret_horizon"] or 0) >= (b.iloc[0]["mean_ret_horizon"] or 0) or (a.iloc[0]["rebreak_ma20_rate"] or 0) <= (b.iloc[0]["rebreak_ma20_rate"] or 0):
            supports += 1
    return comparable >= 3 and supports >= max(2, comparable - 1)


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "input": str(DEFAULT_INPUT_PARQUET),
        "target_population": "close above MA20 with valid MA20",
        "phase_buckets": list(PHASE_BUCKETS),
        "horizons": list(HORIZONS),
        "primary_contrasts": [
            "15-18 vs 1-6",
            "15-18 vs 19-20",
            "19-20 vs 21-30",
            "15-18 no/low upper resistance vs medium/heavy",
            "15-18 lower support vs no support",
            "19-20 upper resistance vs no upper resistance",
        ],
        "outcome_metrics": [
            "mean_ret_horizon",
            "median_ret_horizon",
            "hit_rate",
            "severe_loss_rate",
            "mean_max_drawdown",
            "median_max_drawdown",
            "higher_high_rate",
            "lower_low_rate",
            "held_above_ma20_rate",
            "rebreak_ma20_rate",
            "pullback_occurred_rate",
            "recovered_after_pullback_rate",
        ],
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_audit = json.loads(args.input_audit.read_text(encoding="utf-8"))
    frame = _load_feature_rows(args.input_parquet)
    summary_bucket = _summarize(frame, "all")
    summary_res_sup = pd.concat(
        [
            _summarize(frame, "upper_resistance_bucket"),
            _summarize(frame, "nearest_upper_ma"),
            _summarize(frame, "lower_support_bucket"),
            _summarize(frame, "nearest_lower_ma"),
        ],
        ignore_index=True,
    )
    summary_slope_stack = pd.concat(
        [
            _summarize(frame, "ma20_slope_20d_bucket"),
            _summarize(frame, "ma60_slope_20d_bucket"),
            _summarize(frame, "ma20_slope_up"),
            _summarize(frame, "ma60_slope_up"),
            _summarize(frame, "ma20_vs_ma60"),
            _summarize(frame, "ma_stack_state"),
        ],
        ignore_index=True,
    )
    candle_cols = [
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
        "is_inside_bar",
        "is_outside_bar",
    ]
    summary_candle = pd.concat([_summarize(frame, col) for col in candle_cols], ignore_index=True)
    contrast = _contrast(frame)
    yearly = _yearly(frame)
    decision = _decision(contrast, yearly)
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
        "feature_rows_loaded": int(len(frame)),
        "unique_symbol_count": int(frame["code"].nunique()),
        "min_ymd": int(frame["ymd"].min()),
        "max_ymd": int(frame["ymd"].max()),
        "phase_distribution": frame["phase_bucket"].value_counts(dropna=False).to_dict(),
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "phase_definition.json", _definition())
    frame.to_csv(out_dir / "ma20_phase_events.csv", index=False, encoding="utf-8")
    summary_bucket.to_csv(out_dir / "ma20_phase_summary_by_bucket.csv", index=False, encoding="utf-8")
    summary_res_sup.to_csv(out_dir / "ma20_phase_by_resistance_support.csv", index=False, encoding="utf-8")
    summary_slope_stack.to_csv(out_dir / "ma20_phase_by_slope_stack.csv", index=False, encoding="utf-8")
    summary_candle.to_csv(out_dir / "ma20_phase_by_candle.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "contrast_summary.json", contrast)
    yearly.to_csv(out_dir / "yearly_stability_summary.csv", index=False, encoding="utf-8")
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
    parser = argparse.ArgumentParser(description="TRADEX read-only MA20 above-run phase timing diagnostic.")
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT_PARQUET)
    parser.add_argument("--input-audit", type=Path, default=DEFAULT_INPUT_AUDIT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
