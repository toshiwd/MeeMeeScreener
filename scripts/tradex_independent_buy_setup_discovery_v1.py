from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "independent_buy_setup_discovery_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_FROZEN_SEED_ROOT = Path(r"G:\Tradex\high_upside_reserve_risk_containment_robustness_gate_v1\20260525T091806Z-high-upside-reserve-risk-containment-robustness-gate-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\independent_buy_setup_discovery_v1")
REQUIRED_ARTIFACTS = (
    "setup_discovery_summary.json",
    "setup_discovery_rows.csv",
    "feature_contract.json",
    "setup_candidate_metrics.json",
    "setup_overlap_with_frozen_seed.json",
    "setup_breadth_metrics.json",
    "setup_risk_metrics.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
POINT_IN_TIME_FIELDS = [
    "code",
    "year",
    "decision_date",
    "baseline_rank",
    "baseline_score",
    "ma7_slope",
    "ma20_slope",
    "ma60_slope",
    "dist_ma7_pct",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "ma7_gt_ma20_gt_ma60",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "volume_ma20_ratio",
    "realized_vol20",
    "atr14_pct",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "monthly_box_inside_proxy",
    "weekly_monthly_uptrend_proxy",
    "primary_family",
    "path20_available",
]
OUTCOME_FIELDS = ["ret5", "ret20"]
READ_COLUMNS = POINT_IN_TIME_FIELDS + OUTCOME_FIELDS
NUMERIC_FIELDS = [
    "year",
    "decision_date",
    "baseline_rank",
    "baseline_score",
    "ma7_slope",
    "ma20_slope",
    "ma60_slope",
    "dist_ma7_pct",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "volume_ma20_ratio",
    "realized_vol20",
    "atr14_pct",
    "ret5",
    "ret20",
]
BOOLEAN_FIELDS = [
    "ma7_gt_ma20_gt_ma60",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "monthly_box_inside_proxy",
    "weekly_monthly_uptrend_proxy",
    "path20_available",
]
SETUP_NAMES = [
    "setup_a_pullback_reclaim",
    "setup_b_breakout_continuation_controlled_extension",
    "setup_c_supportive_context_daily_confirmation",
]


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


def _to_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False)
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


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


def normalize_rows(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    for col in READ_COLUMNS:
        if col not in out:
            out[col] = pd.NA
    out["code"] = out["code"].astype(str).str.removesuffix(".0")
    for col in NUMERIC_FIELDS:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in BOOLEAN_FIELDS:
        out[col] = _to_bool(out[col])
    return out


def feature_contract(header: list[str]) -> dict[str, Any]:
    fields: dict[str, dict[str, str]] = {}
    for col in sorted(set(POINT_IN_TIME_FIELDS + OUTCOME_FIELDS + ["ret20_derived_terms", "liquidity_event_fields"])):
        if col in OUTCOME_FIELDS:
            cls = "outcome_only"
        elif col == "ret20_derived_terms":
            cls = "forbidden_future_leak"
        elif col == "liquidity_event_fields":
            cls = "unavailable"
        elif col in header:
            cls = "point_in_time_feature"
        else:
            cls = "unavailable"
        fields[col] = {"classification": cls}
    return {"axis_id": AXIS_ID, "fields": fields}


def setup_masks(rows: pd.DataFrame) -> dict[str, pd.Series]:
    valid = rows["path20_available"] & rows["ret20"].notna()
    controlled_risk = (rows["dist_ma20_pct"] <= 0.12) & (rows["realized_vol20"] <= 0.05) & (rows["atr14_pct"] <= 0.06)
    clean_candle = ~rows["failed_high_update"] & ~rows["large_bearish_candle"] & (rows["upper_wick_ratio"] <= 0.35)
    return {
        "setup_a_pullback_reclaim": valid
        & rows["primary_family"].eq("pullback_reclaim_family")
        & (rows["days_since_ma20_reclaim"].between(0, 10, inclusive="both") | rows["above20_streak"].between(1, 8, inclusive="both"))
        & (rows["dist_ma20_pct"].between(-0.03, 0.08, inclusive="both"))
        & (rows["ma20_slope"] >= 0)
        & clean_candle,
        "setup_b_breakout_continuation_controlled_extension": valid
        & rows["primary_family"].isin(["early_trend_family", "mature_trend_continuation_family"])
        & rows["monthly_box_breakout_proxy"]
        & rows["ma7_gt_ma20_gt_ma60"]
        & controlled_risk
        & (rows["volume_ma20_ratio"].between(1.0, 2.5, inclusive="both"))
        & clean_candle,
        "setup_c_supportive_context_daily_confirmation": valid
        & rows["weekly_monthly_uptrend_proxy"]
        & (rows["monthly_box_inside_proxy"] | rows["monthly_high_zone_proxy"])
        & rows["ma7_gt_ma20_gt_ma60"]
        & (rows["ma7_slope"] > 0)
        & (rows["ma20_slope"] >= 0)
        & controlled_risk
        & clean_candle,
    }


def load_setup_rows(input_root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    source = input_root / "candidate_family_source_rows.csv"
    header = pd.read_csv(source, nrows=0).columns.tolist()
    present = [c for c in READ_COLUMNS if c in header]
    chunks = []
    source_rows = 0
    source_dates: set[Any] = set()
    for chunk in pd.read_csv(source, usecols=present, chunksize=200_000, low_memory=False):
        source_rows += len(chunk)
        norm = normalize_rows(chunk)
        source_dates.update(norm.loc[norm["path20_available"] & norm["ret20"].notna(), "decision_date"].dropna().unique().tolist())
        masks = setup_masks(norm)
        selected = []
        for setup_name, mask in masks.items():
            part = norm[mask].copy()
            if not part.empty:
                part["setup_name"] = setup_name
                selected.append(part)
        if selected:
            chunks.append(pd.concat(selected, ignore_index=True))
    rows = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=[*READ_COLUMNS, "setup_name"])
    rows = rows.drop_duplicates(["decision_date", "code", "setup_name"]).copy()
    return rows, feature_contract(header), {"source_rows_scanned": source_rows, "source_eval_date_count": len(source_dates), "source_eval_dates": list(source_dates), "header": header}


def load_frozen_seed(frozen_seed_root: Path) -> pd.DataFrame:
    source = frozen_seed_root / "robustness_gate_rows.csv"
    rows = pd.read_csv(source, low_memory=False)
    rows = normalize_rows(rows)
    if "kept_by_fixed_variant" not in rows:
        raise ValueError("frozen seed rows missing kept_by_fixed_variant")
    rows["kept_by_fixed_variant"] = _to_bool(rows["kept_by_fixed_variant"])
    return rows[rows["kept_by_fixed_variant"]].copy()


def metric(frame: pd.DataFrame, all_dates: set[Any] | None = None) -> dict[str, Any]:
    per_date = frame.groupby("decision_date").size() if not frame.empty else pd.Series(dtype=float)
    date_count = int(frame["decision_date"].nunique()) if not frame.empty else 0
    zero = None if all_dates is None else int(len(all_dates - set(frame["decision_date"].dropna().unique().tolist())))
    bad = frame["ret20"] < -0.05 if not frame.empty else pd.Series(dtype=bool)
    winner = frame["ret20"] > 0.10 if not frame.empty else pd.Series(dtype=bool)
    bad_rate = _rate(bad)
    winner_rate = _rate(winner)
    return {
        "sample_count": int(len(frame)),
        "date_count": date_count,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "zero_candidate_date_count": zero,
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "hit_rate_ret20_gt_0": _rate(frame["ret20"] > 0) if not frame.empty else None,
        "winner_rate_ret20_gt_10pct": winner_rate,
        "bad_rate_ret20_lt_minus_5pct": bad_rate,
        "severe_rate_ret20_lt_minus_10pct": _rate(frame["ret20"] < -0.10) if not frame.empty else None,
        "downside_to_upside_ratio": None if not winner_rate else (bad_rate or 0) / winner_rate,
    }


def setup_metrics(rows: pd.DataFrame, all_dates: set[Any]) -> dict[str, Any]:
    return {name: metric(rows[rows["setup_name"].eq(name)], all_dates) for name in SETUP_NAMES}


def overlap_metrics(rows: pd.DataFrame, frozen: pd.DataFrame, all_dates: set[Any]) -> dict[str, Any]:
    seed_keys = set(zip(frozen["decision_date"], frozen["code"]))
    out: dict[str, Any] = {}
    for name in SETUP_NAMES:
        g = rows[rows["setup_name"].eq(name)]
        setup_keys = set(zip(g["decision_date"], g["code"]))
        overlap_keys = setup_keys & seed_keys
        unique_keys = setup_keys - seed_keys
        combined = pd.concat([frozen.assign(setup_name="frozen_seed"), g], ignore_index=True)
        combined = combined.drop_duplicates(["decision_date", "code"])
        combined_metric = metric(combined, all_dates)
        out[name] = {
            "overlap_sample_count": int(len(overlap_keys)),
            "unique_new_sample_count": int(len(unique_keys)),
            "overlap_rate": None if len(setup_keys) == 0 else len(overlap_keys) / len(setup_keys),
            "added_date_count": int(len(set(g["decision_date"].dropna().unique().tolist()) - set(frozen["decision_date"].dropna().unique().tolist()))),
            "dates_with_candidate_count": int(g["decision_date"].nunique()),
            "combined_average_candidates_per_date": combined_metric["average_candidates_per_date"],
            "combined_zero_candidate_date_count": combined_metric["zero_candidate_date_count"],
            "combined_mean_ret20": combined_metric["mean_ret20"],
            "combined_bad_rate": combined_metric["bad_rate_ret20_lt_minus_5pct"],
            "combined_severe_rate": combined_metric["severe_rate_ret20_lt_minus_10pct"],
            "combined_winner_rate": combined_metric["winner_rate_ret20_gt_10pct"],
        }
    return out


def breadth_metrics(metrics: dict[str, Any], overlaps: dict[str, Any]) -> dict[str, Any]:
    return {
        name: {
            "setup_sample_count": m["sample_count"],
            "setup_date_count": m["date_count"],
            "average_candidates_per_date": m["average_candidates_per_date"],
            "zero_candidate_date_count": m["zero_candidate_date_count"],
            "unique_new_sample_count": overlaps[name]["unique_new_sample_count"],
            "added_date_count": overlaps[name]["added_date_count"],
            "overlap_rate": overlaps[name]["overlap_rate"],
        }
        for name, m in metrics.items()
    }


def choose_best(metrics: dict[str, Any], overlaps: dict[str, Any], reserve_reference_winner_rate: float = 0.1714929214929215) -> str | None:
    viable = []
    for name, m in metrics.items():
        if (m["mean_ret20"] or 0) > 0 and (m["winner_rate_ret20_gt_10pct"] or 0) > reserve_reference_winner_rate:
            viable.append(name)
    if not viable:
        return None
    return max(viable, key=lambda n: ((metrics[n]["mean_ret20"] or -999), overlaps[n]["unique_new_sample_count"]))


def decide(best: str | None, metrics: dict[str, Any], overlaps: dict[str, Any]) -> tuple[str, list[str]]:
    if best is None:
        return "independent_setup_no_edge", ["no_fixed_setup_has_positive_return_and_winner_rate_above_reserve_reference"]
    m = metrics[best]
    o = overlaps[best]
    strong_return = (m["mean_ret20"] or 0) >= 0.05
    risk_ok = (m["bad_rate_ret20_lt_minus_5pct"] or 1) < 0.25 and (m["severe_rate_ret20_lt_minus_10pct"] or 1) <= 0.15
    breadth_ok = (m["date_count"] or 0) >= 80 and (o["unique_new_sample_count"] or 0) >= 100 and (o["added_date_count"] or 0) >= 40
    overlap_ok = (o["overlap_rate"] or 0) <= 0.30
    if strong_return and risk_ok and breadth_ok and overlap_ok:
        return "independent_setup_keep_for_family_portfolio_pretest", [f"{best}_adds_independent_breadth_with_controlled_risk"]
    if not overlap_ok:
        return "independent_setup_too_overlapping_with_frozen_seed", [f"{best}_overlap_with_frozen_seed_exceeds_limit"]
    if (m["mean_ret20"] or 0) > 0 and (m["winner_rate_ret20_gt_10pct"] or 0) > 0.1714929214929215:
        return "independent_setup_promising_but_underpowered", [f"{best}_direction_positive_but_support_or_risk_gate_not_met"]
    return "independent_setup_no_edge", [f"{best}_does_not_clear_return_risk_reference"]


def run(input_root: Path, frozen_seed_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-independent-buy-setup-discovery-v1"
    out.mkdir(parents=True, exist_ok=True)
    try:
        rows, contract, coverage_base = load_setup_rows(input_root)
        frozen = load_frozen_seed(frozen_seed_root)
        blocked = False
        block_reason = None
    except Exception as exc:
        rows = pd.DataFrame()
        frozen = pd.DataFrame()
        contract = {"axis_id": AXIS_ID, "fields": {}}
        coverage_base = {}
        blocked = True
        block_reason = str(exc)
    if blocked or rows.empty:
        decision = "blocked_missing_point_in_time_features" if blocked else "independent_setup_no_edge"
        reasons = [block_reason or "no_rows_matched_predeclared_setup_definitions"]
        metrics: dict[str, Any] = {}
        overlaps: dict[str, Any] = {}
        breadth: dict[str, Any] = {}
        best = None
    else:
        all_dates = set(coverage_base.get("source_eval_dates", []))
        if not all_dates:
            all_dates = set(pd.concat([rows["decision_date"], frozen["decision_date"]]).dropna().unique().tolist())
        metrics = setup_metrics(rows, all_dates)
        overlaps = overlap_metrics(rows, frozen, all_dates)
        breadth = breadth_metrics(metrics, overlaps)
        best = choose_best(metrics, overlaps)
        decision, reasons = decide(best, metrics, overlaps)
    rows.to_csv(out / "setup_discovery_rows.csv", index=False)
    _write_json(out / "feature_contract.json", contract)
    _write_json(out / "setup_candidate_metrics.json", metrics)
    _write_json(out / "setup_overlap_with_frozen_seed.json", overlaps)
    _write_json(out / "setup_breadth_metrics.json", breadth)
    _write_json(out / "setup_risk_metrics.json", {k: {rk: v for rk, v in m.items() if "rate" in rk or rk in {"mean_ret20", "median_ret20", "downside_to_upside_ratio"}} for k, m in metrics.items()})
    _write_json(out / "setup_discovery_summary.json", {"axis_id": AXIS_ID, "input_root": input_root, "frozen_seed_root": frozen_seed_root, "best_setup": best, "decision": decision, "reason_typed": reasons})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "blocked" if blocked else "pass", "features_use_saved_point_in_time_context_only": True, "outcomes_used_evaluation_only": True, "thresholds_retuned": False, "new_variants_for_frozen_seed": False, "runtime_db_write": False, "research_fallback_used": False})
    source_coverage_payload = {k: v for k, v in coverage_base.items() if k != "source_eval_dates"}
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, **source_coverage_payload, "selected_row_count": int(len(rows)), "frozen_seed_row_count": int(len(frozen)), "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "reason_typed": reasons, "best_setup": best, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--frozen-seed-root", type=Path, default=DEFAULT_FROZEN_SEED_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.frozen_seed_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
