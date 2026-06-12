from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma20_context_filtered_buy_timing_pretest_v1"
DEFAULT_FEATURE_ROOT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1")
DEFAULT_INPUT_PARQUET = DEFAULT_FEATURE_ROOT / "ma_phase_features.parquet"
DEFAULT_INPUT_AUDIT = DEFAULT_FEATURE_ROOT / "input_audit.json"
DEFAULT_PHASE_DEFINITION = Path("G:/Tradex/ma20_above_run_phase_timing_v1/20260603T121810Z-ma20-above-run-phase-timing-v1/phase_definition.json")
DEFAULT_OUT_ROOT = Path("G:/Tradex/ma20_context_filtered_buy_timing_pretest_v1")
HORIZONS = (5, 10, 20)
FOCUS_BUCKETS = ("10-14", "15-18", "19-20", "21-30")
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "filter_definition.json",
    "ma20_context_filtered_events.csv",
    "variant_summary.csv",
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
    frame["phase_bucket"] = frame["above_ma20_run_bucket"].astype(str)
    frame["focus_bucket"] = frame["phase_bucket"].isin(FOCUS_BUCKETS)
    frame["no_light_upper_resistance"] = frame["upper_resistance_bucket"].isin(["none_near", "light_resistance"])
    frame["medium_heavy_upper_resistance"] = frame["upper_resistance_bucket"].isin(["medium_resistance", "heavy_resistance"])
    frame["has_lower_support"] = frame["lower_support_bucket"].isin(["light_support", "medium_support", "heavy_support"])
    frame["ma20_slope_up"] = frame["ma20_slope_20d_bucket"].isin(["weak_up", "strong_up"])
    frame["ma60_not_strong_down"] = ~frame["ma60_slope_20d_bucket"].eq("strong_down")
    frame["phase_15_20"] = frame["phase_bucket"].isin(["15-18", "19-20"])
    frame["variant_baseline_all_ma20_above"] = True
    frame["variant_phase_15_20_only"] = frame["phase_15_20"]
    frame["variant_phase_15_20_no_light_upper_resistance"] = frame["phase_15_20"] & frame["no_light_upper_resistance"]
    frame["variant_phase_15_20_no_light_upper_resistance_with_support"] = frame["variant_phase_15_20_no_light_upper_resistance"] & frame["has_lower_support"]
    frame["variant_phase_15_20_no_light_upper_resistance_slope_up"] = frame["variant_phase_15_20_no_light_upper_resistance"] & frame["ma20_slope_up"] & frame["ma60_not_strong_down"]
    frame["variant_negative_control_heavy_resistance"] = frame["phase_15_20"] & frame["medium_heavy_upper_resistance"]
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


def _metric_row(group: pd.DataFrame, *, variant: str, horizon: int) -> dict[str, Any]:
    ret = f"ret_{horizon}b"
    dd = f"max_drawdown_{horizon}b"
    valid = group[group[ret].notna()].copy()
    return {
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


def _variant_summary(frame: pd.DataFrame) -> pd.DataFrame:
    variants = {
        "baseline_all_ma20_above": frame,
        "phase_15_20_only": frame[frame["variant_phase_15_20_only"]],
        "phase_15_20_no_light_upper_resistance": frame[frame["variant_phase_15_20_no_light_upper_resistance"]],
        "phase_15_20_no_light_upper_resistance_with_support": frame[frame["variant_phase_15_20_no_light_upper_resistance_with_support"]],
        "phase_15_20_no_light_upper_resistance_slope_up": frame[frame["variant_phase_15_20_no_light_upper_resistance_slope_up"]],
        "negative_control_heavy_resistance": frame[frame["variant_negative_control_heavy_resistance"]],
    }
    rows: list[dict[str, Any]] = []
    for variant, group in variants.items():
        for horizon in HORIZONS:
            rows.append(_metric_row(group, variant=variant, horizon=horizon))
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
        _compare(summary, first="phase_15_20_no_light_upper_resistance", second="baseline_all_ma20_above", name="C_vs_baseline_all_ma20_above"),
        _compare(summary, first="phase_15_20_no_light_upper_resistance", second="phase_15_20_only", name="C_vs_phase_15_20_only"),
        _compare(summary, first="phase_15_20_no_light_upper_resistance", second="negative_control_heavy_resistance", name="C_vs_F_heavy_resistance"),
        _compare(summary, first="phase_15_20_no_light_upper_resistance_with_support", second="phase_15_20_no_light_upper_resistance", name="D_vs_C_support_add"),
        _compare(summary, first="phase_15_20_no_light_upper_resistance_slope_up", second="phase_15_20_no_light_upper_resistance", name="E_vs_C_slope_add"),
    ]
    c = frame[frame["variant_phase_15_20_no_light_upper_resistance"]]
    f = frame[frame["variant_negative_control_heavy_resistance"]]
    for source_name, source in [("within_C", c), ("within_F", f)]:
        local_summary = pd.DataFrame(
            [
                _metric_row(source[source["phase_bucket"].eq("15-18")], variant=f"{source_name}_15_18", horizon=20),
                _metric_row(source[source["phase_bucket"].eq("19-20")], variant=f"{source_name}_19_20", horizon=20),
            ]
        )
        comparisons.append(_compare(local_summary, first=f"{source_name}_15_18", second=f"{source_name}_19_20", name=f"15_18_vs_19_20_{source_name}"))
    return {"axis_id": AXIS_ID, "required_contrasts": comparisons}


def _yearly(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    variants = {
        "baseline_all_ma20_above": frame,
        "phase_15_20_no_light_upper_resistance": frame[frame["variant_phase_15_20_no_light_upper_resistance"]],
        "negative_control_heavy_resistance": frame[frame["variant_negative_control_heavy_resistance"]],
    }
    for variant, source in variants.items():
        for year, group in source.groupby("event_year"):
            for horizon in HORIZONS:
                row = _metric_row(group, variant=variant, horizon=horizon)
                row["event_year"] = int(year)
                row["sample_status"] = "sufficient" if row["event_count"] >= 100 else "insufficient_sample"
                rows.append(row)
    return pd.DataFrame(rows).sort_values(["variant", "event_year", "horizon"], kind="stable")


def _decision(contrast: dict[str, Any], yearly: pd.DataFrame) -> dict[str, Any]:
    comps = {row["name"]: row for row in contrast["required_contrasts"] if row.get("status") == "ready"}
    c_base = comps.get("C_vs_baseline_all_ma20_above")
    c_phase = comps.get("C_vs_phase_15_20_only")
    c_heavy = comps.get("C_vs_F_heavy_resistance")
    reasons: list[dict[str, Any]] = []
    context: list[dict[str, Any]] = []
    if c_base and c_phase and c_heavy:
        d_base = c_base["deltas"]
        d_phase = c_phase["deltas"]
        d_heavy = c_heavy["deltas"]
        yearly_ok = _yearly_ok(yearly)
        improves_rebreak = d_base.get("rebreak_ma20_rate_delta", 0) <= -0.03 or d_phase.get("rebreak_ma20_rate_delta", 0) <= -0.03
        risk_ok = d_base.get("severe_loss_rate_delta", 0) <= 0.005 and d_base.get("mean_max_drawdown_delta", 0) >= -0.3
        ret_ok = d_base.get("mean_ret_delta", 0) >= 0
        heavy_worse = d_heavy.get("rebreak_ma20_rate_delta", 0) <= -0.05 or d_heavy.get("held_above_ma20_rate_delta", 0) >= 0.05
        if improves_rebreak and risk_ok and ret_ok and heavy_worse and yearly_ok:
            reasons.append({"typed_reason": "no_light_resistance_filter_passes_buy_timing_candidate_gates", "C_vs_baseline": d_base, "C_vs_phase": d_phase, "C_vs_heavy": d_heavy, "yearly_ok": yearly_ok})
        elif improves_rebreak or heavy_worse:
            context.append({"typed_reason": "resistance_filter_improves_rebreak_or_separates_heavy_resistance_but_return_or_stability_is_weak", "C_vs_baseline": d_base, "C_vs_phase": d_phase, "C_vs_heavy": d_heavy, "yearly_ok": yearly_ok})
    if reasons:
        decision = "keep_for_buy_timing_candidate_next"
        reason = "no_light_resistance_context_passes_buy_timing_pretest"
    elif context:
        decision = "keep_as_context_feature"
        reason = "resistance_filter_improves_rebreak_or_drawdown_but_ret20_or_yearly_stability_is_weak"
    else:
        decision = "drop"
        reason = "context_filter_does_not_improve_outcomes"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "buy_timing_reasons": reasons,
        "context_feature_reasons": context,
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


def _yearly_ok(yearly: pd.DataFrame) -> bool:
    c = yearly[(yearly["variant"] == "phase_15_20_no_light_upper_resistance") & (yearly["horizon"] == 20) & (yearly["sample_status"] == "sufficient")]
    b = yearly[(yearly["variant"] == "baseline_all_ma20_above") & (yearly["horizon"] == 20) & (yearly["sample_status"] == "sufficient")]
    supports = 0
    comparable = 0
    for year in sorted(set(c["event_year"].astype(int)).intersection(set(b["event_year"].astype(int)))):
        cr = c[c["event_year"] == year].iloc[0]
        br = b[b["event_year"] == year].iloc[0]
        comparable += 1
        if (cr["rebreak_ma20_rate"] <= br["rebreak_ma20_rate"]) or (cr["mean_ret"] >= br["mean_ret"]):
            supports += 1
    return comparable >= 3 and supports >= max(2, comparable - 1)


def _definition() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "target_population": "baseline uses all close-above-MA20 rows with valid ret20 and max drawdown; focused diagnostics retain 10-14/15-18/19-20/21-30 buckets",
        "variants": {
            "baseline_all_ma20_above": "all close-above-MA20 rows",
            "phase_15_20_only": "MA20 above-run 15-18 or 19-20",
            "phase_15_20_no_light_upper_resistance": "phase 15-20 and upper resistance none_near/light_resistance",
            "phase_15_20_no_light_upper_resistance_with_support": "C plus lower support light/medium/heavy",
            "phase_15_20_no_light_upper_resistance_slope_up": "C plus MA20 slope weak_up/strong_up and MA60 not strong_down",
            "negative_control_heavy_resistance": "phase 15-20 and upper resistance medium/heavy",
        },
        "horizons": list(HORIZONS),
        "authoritative_input": str(DEFAULT_INPUT_PARQUET),
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_audit = json.loads(args.input_audit.read_text(encoding="utf-8"))
    phase_definition = json.loads(args.phase_definition.read_text(encoding="utf-8"))
    frame = _load_events(args.input_parquet)
    summary = _variant_summary(frame)
    contrast = _contrast(frame, summary)
    yearly = _yearly(frame)
    decision = _decision(contrast, yearly)
    examples = frame[
        frame["variant_phase_15_20_no_light_upper_resistance"]
        | frame["variant_negative_control_heavy_resistance"]
    ].head(5000)
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_parquet": str(args.input_parquet),
        "input_audit": str(args.input_audit),
        "phase_definition": str(args.phase_definition),
        "source_axis_id": source_audit.get("axis_id"),
        "phase_definition_axis_id": phase_definition.get("axis_id"),
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
    _write_json(out_dir / "filter_definition.json", _definition())
    frame.to_csv(out_dir / "ma20_context_filtered_events.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "variant_summary.csv", index=False, encoding="utf-8")
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
    parser = argparse.ArgumentParser(description="TRADEX read-only MA20 context-filtered buy timing pretest.")
    parser.add_argument("--input-parquet", type=Path, default=DEFAULT_INPUT_PARQUET)
    parser.add_argument("--input-audit", type=Path, default=DEFAULT_FEATURE_ROOT / "input_audit.json")
    parser.add_argument("--phase-definition", type=Path, default=DEFAULT_PHASE_DEFINITION)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
