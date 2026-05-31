from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "pattern_family_selectivity_pretest_v1"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\pattern_family_source_rows_v1\20260525T101220Z-pattern-family-source-rows-v1")
DEFAULT_EVALUATION_ROOT = Path(r"G:\Tradex\pattern_family_candidate_evaluation_v1\20260525T101613Z-pattern-family-candidate-evaluation-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\pattern_family_selectivity_pretest_v1")
PROMISING_FAMILIES = [
    "constructive_pullback_support_bullish_confirmation_reference_match",
    "monthly_weekly_supportive_daily_confirmation_candidate",
    "volatility_compression_breakout_preparation_candidate",
]
BROAD_METADATA_FLAGS = [
    "high_upside_reserve_reference_match",
    "early_trend_reclaim_controlled_extension_candidate",
]
VARIANTS = ["variant_a_quality_score_bucket", "variant_b_risk_filtered_subset", "variant_c_overlap_adjusted_subset"]
REQUIRED_ARTIFACTS = (
    "selectivity_pretest_summary.json",
    "selectivity_pretest_rows.csv",
    "feature_contract.json",
    "family_selectivity_metrics.json",
    "selected_vs_unselected_quality.json",
    "overlap_adjusted_family_metrics.json",
    "period_stability_metrics.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
POINT_IN_TIME_FEATURES = [
    "close_vs_ma7_pct",
    "close_vs_ma20_pct",
    "close_vs_ma60_pct",
    "ma7_slope_5d",
    "ma20_slope_10d",
    "ma60_slope_20d",
    "close_above_ma7",
    "close_above_ma20",
    "close_above_ma60",
    "ma7_above_ma20",
    "ma20_above_ma60",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "bullish_body_flag",
    "bearish_body_flag",
    "failed_high_flag",
    "recent_high_distance_pct",
    "recent_low_distance_pct",
    "volume_vs_20d_avg",
    "gap_up_flag",
    "gap_down_flag",
    "atr14_pct",
    "realized_vol20",
    "weekly_close_vs_ma7_pct",
    "weekly_close_vs_ma20_pct",
    "weekly_ma7_slope",
    "weekly_ma20_slope",
    "weekly_supportive_flag",
    "weekly_failed_high_flag",
    "monthly_close_vs_ma7_pct",
    "monthly_close_vs_ma20_pct",
    "monthly_ma7_slope",
    "monthly_ma20_slope",
    "monthly_supportive_flag",
    "monthly_box_position",
    "monthly_box_width_pct",
]
OUTCOME_COLUMNS = ["ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"]


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
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


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    if col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, col: str) -> float | None:
    if col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return None if values.empty else float(values.median())


def period_half(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(int(value))
    return f"{text[:4]}{'H1' if int(text[4:6]) <= 6 else 'H2'}" if len(text) >= 6 else None


def feature_contract(source_contract: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, dict[str, str]] = {"as_of_date": {"classification": "identifier"}, "code": {"classification": "identifier"}}
    for col in POINT_IN_TIME_FEATURES + PROMISING_FAMILIES + BROAD_METADATA_FLAGS:
        source_cls = source_contract.get("fields", {}).get(col, {}).get("classification")
        fields[col] = {"classification": "point_in_time_feature" if source_cls == "point_in_time_feature" else "unavailable"}
    for col in OUTCOME_COLUMNS:
        fields[col] = {"classification": "offline_outcome_only"}
    fields["ret20_derived_tags"] = {"classification": "forbidden_future_leak"}
    fields["liquidity_event_fields"] = {"classification": "unavailable"}
    return {"axis_id": AXIS_ID, "fields": fields}


def load_inputs(source_root: Path, evaluation_root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    rows = pd.read_parquet(source_root / "pattern_family_source_rows.parquet")
    source_contract = json.loads((source_root / "feature_contract.json").read_text(encoding="utf-8"))
    source_no_lookahead = json.loads((source_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    decisions = json.loads((evaluation_root / "family_candidate_decisions.json").read_text(encoding="utf-8"))
    return rows, source_contract, source_no_lookahead, decisions


def candidate_population(rows: pd.DataFrame) -> pd.DataFrame:
    pop = rows[rows[PROMISING_FAMILIES].any(axis=1)].copy()
    pop["promising_family_count"] = pop[PROMISING_FAMILIES].sum(axis=1)
    pop["broad_proxy_overlap_count"] = pop[BROAD_METADATA_FLAGS].sum(axis=1)
    pop["period_half"] = pop["as_of_date"].map(period_half)
    pop["quality_score"] = (
        pop["weekly_supportive_flag"].astype(int)
        + pop["monthly_supportive_flag"].astype(int)
        + pop["close_above_ma20"].astype(int)
        + pop["ma7_above_ma20"].astype(int)
        + pop["ma20_above_ma60"].astype(int)
        + (pop["ma7_slope_5d"] > 0).astype(int)
        + (pop["ma20_slope_10d"] > 0).astype(int)
        + pop["bullish_body_flag"].astype(int)
        + (pop["lower_wick_ratio"] >= 0.20).astype(int)
        + (pop["volume_vs_20d_avg"].between(0.8, 1.8, inclusive="both")).astype(int)
    )
    return pop


def variant_masks(pop: pd.DataFrame, family: str) -> dict[str, pd.Series]:
    base = pop[family].astype(bool)
    risk_clean = (
        ~pop["failed_high_flag"].astype(bool)
        & ~pop["bearish_body_flag"].astype(bool)
        & (pop["upper_wick_ratio"] <= 0.35)
        & (pop["close_vs_ma20_pct"] <= 0.10)
        & (pop["close_vs_ma60_pct"] <= 0.30)
        & (pop["atr14_pct"] <= 0.05)
        & (pop["realized_vol20"] <= 0.04)
    )
    unique_to_family = base & (pop[PROMISING_FAMILIES].sum(axis=1) == 1)
    supportive_overlap = base & (pop[PROMISING_FAMILIES].sum(axis=1) >= 2) & (pop["broad_proxy_overlap_count"] < 2)
    return {
        "baseline_family": base,
        "variant_a_quality_score_bucket": base & (pop["quality_score"] >= 8),
        "variant_b_risk_filtered_subset": base & risk_clean,
        "variant_c_overlap_adjusted_subset": unique_to_family | supportive_overlap,
    }


def metric(frame: pd.DataFrame, all_dates: set[Any], base_count: int | None = None) -> dict[str, Any]:
    per_date = frame.groupby("as_of_date").size() if not frame.empty else pd.Series(dtype=float)
    present_dates = set(frame["as_of_date"].dropna().unique().tolist()) if not frame.empty else set()
    bad = frame["bad_ret20_lt_minus_5pct"] if "bad_ret20_lt_minus_5pct" in frame else pd.Series(dtype=bool)
    severe = frame["severe_ret20_lt_minus_10pct"] if "severe_ret20_lt_minus_10pct" in frame else pd.Series(dtype=bool)
    winner = frame["winner_ret20_gt_10pct"] if "winner_ret20_gt_10pct" in frame else pd.Series(dtype=bool)
    bad_rate = _rate(bad)
    winner_rate = _rate(winner)
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["as_of_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "median_candidates_per_date": None if per_date.empty else float(per_date.median()),
        "zero_candidate_date_count": int(len(all_dates - present_dates)),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "hit_rate_ret20_gt_0": _rate(frame["ret20"] > 0) if not frame.empty else None,
        "winner_rate_ret20_gt_10pct": winner_rate,
        "bad_rate_ret20_lt_minus_5pct": bad_rate,
        "severe_rate_ret20_lt_minus_10pct": _rate(severe),
        "downside_to_upside_ratio": None if not winner_rate else (bad_rate or 0) / winner_rate,
        "selected_share": None if not base_count else len(frame) / base_count,
    }


def selected_unselected_delta(selected: pd.DataFrame, unselected: pd.DataFrame) -> dict[str, Any]:
    selected_winner = _rate(selected["winner_ret20_gt_10pct"]) if not selected.empty else None
    unselected_winner = _rate(unselected["winner_ret20_gt_10pct"]) if not unselected.empty else None
    selected_bad = _rate(selected["bad_ret20_lt_minus_5pct"]) if not selected.empty else None
    unselected_bad = _rate(unselected["bad_ret20_lt_minus_5pct"]) if not unselected.empty else None
    selected_severe = _rate(selected["severe_ret20_lt_minus_10pct"]) if not selected.empty else None
    unselected_severe = _rate(unselected["severe_ret20_lt_minus_10pct"]) if not unselected.empty else None
    return {
        "selected_vs_unselected_delta_ret20": None if _mean(selected, "ret20") is None or _mean(unselected, "ret20") is None else _mean(selected, "ret20") - _mean(unselected, "ret20"),
        "selected_vs_unselected_delta_winner_rate": None if selected_winner is None or unselected_winner is None else selected_winner - unselected_winner,
        "selected_vs_unselected_delta_bad_rate": None if selected_bad is None or unselected_bad is None else selected_bad - unselected_bad,
        "selected_vs_unselected_delta_severe_rate": None if selected_severe is None or unselected_severe is None else selected_severe - unselected_severe,
    }


def evaluate(pop: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    all_dates = set(pop["as_of_date"].dropna().unique().tolist())
    metrics: dict[str, Any] = {}
    deltas: dict[str, Any] = {}
    for family in PROMISING_FAMILIES:
        masks = variant_masks(pop, family)
        base = pop[masks["baseline_family"]]
        metrics[family] = {}
        deltas[family] = {}
        for variant, mask in masks.items():
            selected = pop[mask]
            unselected = base.loc[~base.index.isin(selected.index)]
            m = metric(selected, all_dates, len(base))
            d = selected_unselected_delta(selected, unselected)
            metrics[family][variant] = {**m, **d}
            deltas[family][variant] = d
    return metrics, deltas


def overlap_adjusted_metrics(pop: pd.DataFrame) -> dict[str, Any]:
    all_dates = set(pop["as_of_date"].dropna().unique().tolist())
    out: dict[str, Any] = {}
    for family in PROMISING_FAMILIES:
        unique = pop[pop[family] & (pop[PROMISING_FAMILIES].sum(axis=1) == 1)]
        out[f"unique_{family}"] = metric(unique, all_dates)
    overlap = pop[pop[PROMISING_FAMILIES].sum(axis=1) >= 2]
    out["overlapping_rows_among_promising_families"] = metric(overlap, all_dates)
    return out


def period_stability(pop: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for family in PROMISING_FAMILIES:
        out[family] = {}
        for variant, mask in variant_masks(pop, family).items():
            selected = pop[mask]
            out[family][variant] = {}
            for period, group in selected.groupby("period_half", dropna=True):
                out[family][variant][str(period)] = metric(group, set(group["as_of_date"].dropna().unique().tolist()))
    return out


def best_variant(metrics: dict[str, Any]) -> tuple[str | None, str | None, dict[str, Any] | None]:
    best_family = None
    best_name = None
    best_metric = None
    for family, variants in metrics.items():
        for name, m in variants.items():
            if name == "baseline_family":
                continue
            if best_metric is None or (m.get("mean_ret20") or -999) > (best_metric.get("mean_ret20") or -999):
                best_family, best_name, best_metric = family, name, m
    return best_family, best_name, best_metric


def decide(metrics: dict[str, Any], overlap_metrics: dict[str, Any]) -> tuple[str, list[str], dict[str, Any]]:
    family, variant, m = best_variant(metrics)
    if m is None:
        return "blocked_missing_point_in_time_features", ["no_selectivity_metrics_computed"], {}
    unique_edges = [v for k, v in overlap_metrics.items() if k.startswith("unique_") and (v.get("mean_ret20") or 0) > 0.02 and (v.get("winner_rate_ret20_gt_10pct") or 0) > 0.13]
    edge_survives_overlap = bool(unique_edges)
    mean_ok = (m["mean_ret20"] or 0) >= 0.025
    winner_ok = (m["winner_rate_ret20_gt_10pct"] or 0) >= 0.15
    risk_ok = (m["bad_rate_ret20_lt_minus_5pct"] or 1) <= 0.20 and (m["severe_rate_ret20_lt_minus_10pct"] or 1) <= 0.08
    breadth_ok = (m["sample_count"] or 0) >= 500 and (m["date_count"] or 0) >= 200
    delta_ok = (m.get("selected_vs_unselected_delta_ret20") or 0) > 0 and (m.get("selected_vs_unselected_delta_winner_rate") or 0) > 0
    if mean_ok and winner_ok and risk_ok and breadth_ok and delta_ok and edge_survives_overlap:
        return "selectivity_keep_for_family_pretest", [f"{family}.{variant}_passes_selectivity_return_risk_breadth_and_overlap_gate"], {"best_family": family, "best_variant": variant}
    if mean_ok and winner_ok and delta_ok and edge_survives_overlap:
        return "selectivity_promising_but_underpowered", [f"{family}.{variant}_direction_positive_but_risk_or_breadth_gate_not_met"], {"best_family": family, "best_variant": variant}
    if not edge_survives_overlap:
        return "selectivity_edge_overlap_driven_close_or_redesign", ["unique_family_rows_do_not_preserve_edge_after_overlap_adjustment"], {"best_family": family, "best_variant": variant}
    return "selectivity_no_edge_close_family_contract", [f"{family}.{variant}_does_not_improve_return_winner_risk_enough"], {"best_family": family, "best_variant": variant}


def run(source_root: Path, evaluation_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-pattern-family-selectivity-pretest-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows, source_contract, source_no_lookahead, decisions = load_inputs(source_root, evaluation_root)
    missing = [f for f in POINT_IN_TIME_FEATURES + PROMISING_FAMILIES + OUTCOME_COLUMNS if f not in rows.columns]
    if missing:
        metrics: dict[str, Any] = {}
        deltas: dict[str, Any] = {}
        overlap_metrics: dict[str, Any] = {}
        stability: dict[str, Any] = {}
        decision = "blocked_missing_point_in_time_features"
        reasons = [f"missing_columns:{','.join(missing)}"]
        extras: dict[str, Any] = {}
        pop = pd.DataFrame()
    else:
        pop = candidate_population(rows)
        metrics, deltas = evaluate(pop)
        overlap_metrics = overlap_adjusted_metrics(pop)
        stability = period_stability(pop)
        decision, reasons, extras = decide(metrics, overlap_metrics)
    sample_cols = ["as_of_date", "code", *PROMISING_FAMILIES, *BROAD_METADATA_FLAGS, "quality_score", "promising_family_count", "broad_proxy_overlap_count", "ret20"]
    if not pop.empty:
        pop[sample_cols].head(25_000).to_csv(out / "selectivity_pretest_rows.csv", index=False)
    else:
        pd.DataFrame(columns=sample_cols).to_csv(out / "selectivity_pretest_rows.csv", index=False)
    _write_json(out / "feature_contract.json", feature_contract(source_contract))
    _write_json(out / "family_selectivity_metrics.json", metrics)
    _write_json(out / "selected_vs_unselected_quality.json", deltas)
    _write_json(out / "overlap_adjusted_family_metrics.json", overlap_metrics)
    _write_json(out / "period_stability_metrics.json", stability)
    _write_json(out / "selectivity_pretest_summary.json", {"axis_id": AXIS_ID, "source_root": source_root, "evaluation_root": evaluation_root, "candidate_population_rows": int(len(pop)), "candidate_population_dates": int(pop["as_of_date"].nunique()) if not pop.empty else 0, "decision": decision, "reason_typed": reasons, **extras})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass" if source_no_lookahead.get("audit_result") == "pass" and decision != "blocked_missing_point_in_time_features" else "blocked", "source_no_lookahead_audit": source_no_lookahead.get("audit_result"), "point_in_time_features_only": True, "outcomes_used_evaluation_only": True, "thresholds_retuned": False, "new_family_definitions_added": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_rows": int(len(rows)), "candidate_population_rows": int(len(pop)), "candidate_population_date_count": int(pop["as_of_date"].nunique()) if not pop.empty else 0, "promising_families": PROMISING_FAMILIES, "input_family_decisions": decisions, "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "reason_typed": reasons, **extras, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--evaluation-root", type=Path, default=DEFAULT_EVALUATION_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.source_root, args.evaluation_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
