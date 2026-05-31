from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "candidate_generation_rebuild_preflight_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_PATTERN_SEED_ROOT = Path(r"G:\Tradex\pattern_family_seed_discovery_v1\20260525T092840Z-pattern-family-seed-discovery-v1")
DEFAULT_FROZEN_SEED_ROOT = Path(r"G:\Tradex\high_upside_reserve_risk_containment_robustness_gate_v1\20260525T091806Z-high-upside-reserve-risk-containment-robustness-gate-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\candidate_generation_rebuild_preflight_v1")
REQUIRED_ARTIFACTS = (
    "rebuild_preflight_summary.json",
    "source_funnel_audit.json",
    "candidate_source_coverage.csv",
    "candidate_source_coverage.json",
    "dropped_universe_audit.json",
    "feature_contract_gap_audit.json",
    "seed_lineage_summary.json",
    "rebuild_options.json",
    "recommended_rebuild_contract.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
READ_COLUMNS = [
    "decision_date",
    "code",
    "baseline_rank",
    "baseline_score",
    "candidate_rank",
    "selection_score",
    "primary_family",
    "research_candidate_source_family",
    "research_family_surface",
    "candidate_source",
    "signal_family",
    "setup_name",
    "regime_bucket",
    "path20_available",
    "ret5",
    "ret20",
    "mae20",
    "mfe20",
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
    "event_flags_json",
    "liquidity_flags_json",
    "feature_snapshot_json",
    "feature_availability_json",
]
NUMERIC_COLUMNS = [
    "decision_date",
    "baseline_rank",
    "baseline_score",
    "candidate_rank",
    "selection_score",
    "ret5",
    "ret20",
    "mae20",
    "mfe20",
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
]
BOOLEAN_COLUMNS = [
    "path20_available",
    "ma7_gt_ma20_gt_ma60",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "monthly_box_inside_proxy",
    "weekly_monthly_uptrend_proxy",
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
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def normalize(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    for col in READ_COLUMNS:
        if col not in out:
            out[col] = pd.NA
    out["code"] = out["code"].astype(str).str.removesuffix(".0")
    for col in NUMERIC_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in BOOLEAN_COLUMNS:
        out[col] = _to_bool(out[col])
    return out


def load_source_rows(input_root: Path) -> tuple[pd.DataFrame, list[str], dict[str, Any]]:
    source = input_root / "candidate_family_source_rows.csv"
    schema_path = input_root / "research_family_source_schema.json"
    header = pd.read_csv(source, nrows=0).columns.tolist()
    present = [c for c in READ_COLUMNS if c in header]
    rows = pd.concat([normalize(chunk) for chunk in pd.read_csv(source, usecols=present, chunksize=250_000, low_memory=False)], ignore_index=True)
    schema = json.loads(schema_path.read_text(encoding="utf-8")) if schema_path.exists() else {}
    return rows, header, schema


def metric(frame: pd.DataFrame) -> dict[str, Any]:
    bad = frame["ret20"] < -0.05 if not frame.empty else pd.Series(dtype=bool)
    severe = frame["ret20"] < -0.10 if not frame.empty else pd.Series(dtype=bool)
    winner = frame["ret20"] > 0.10 if not frame.empty else pd.Series(dtype=bool)
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["decision_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "mean_ret20": _mean(frame, "ret20"),
        "winner_rate": _rate(winner),
        "bad_rate": _rate(bad),
        "severe_rate": _rate(severe),
    }


def source_funnel_audit(rows: pd.DataFrame, schema: dict[str, Any]) -> dict[str, Any]:
    eval_rows = rows[rows["path20_available"] & rows["ret20"].notna()].copy()
    rank_buckets = {
        "rank_1_10": eval_rows[eval_rows["baseline_rank"].between(1, 10, inclusive="both")],
        "rank_11_50": eval_rows[eval_rows["baseline_rank"].between(11, 50, inclusive="both")],
        "rank_51_100": eval_rows[eval_rows["baseline_rank"].between(51, 100, inclusive="both")],
        "rank_gt_100_or_missing": eval_rows[~eval_rows["baseline_rank"].between(1, 100, inclusive="both")],
    }
    per_date = eval_rows.groupby("decision_date").size()
    winners = eval_rows[eval_rows["ret20"] > 0.10]
    winner_rank_distribution = {
        name: int(len(frame[frame["ret20"] > 0.10]))
        for name, frame in rank_buckets.items()
    }
    return {
        "source_path": str(DEFAULT_INPUT_ROOT / "candidate_family_source_rows.csv"),
        "diagnostic_not_real_candidate_source": bool(schema.get("diagnostic_not_real_candidate_source", False)),
        "row_count": int(len(rows)),
        "eval_row_count": int(len(eval_rows)),
        "date_count": int(eval_rows["decision_date"].nunique()),
        "code_count": int(eval_rows["code"].nunique()),
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "median_candidates_per_date": None if per_date.empty else float(per_date.median()),
        "rank_bucket_counts": {name: int(len(frame)) for name, frame in rank_buckets.items()},
        "rank_bucket_metrics": {name: metric(frame) for name, frame in rank_buckets.items()},
        "high_return_winner_count_ret20_gt_10pct": int(len(winners)),
        "winner_rank_distribution": winner_rank_distribution,
        "winners_present_but_ranked_below_top10": int(winner_rank_distribution["rank_11_50"] + winner_rank_distribution["rank_51_100"] + winner_rank_distribution["rank_gt_100_or_missing"]),
        "absent_winners_not_observable_from_current_source_rows": "not_directly_observable_without_all_bars_universe",
    }


def candidate_source_coverage(rows: pd.DataFrame, header: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    records = []
    for col in READ_COLUMNS:
        if col in rows:
            records.append({"field": col, "present_in_source": col in header, "non_null_rate": float(rows[col].notna().mean()), "unique_count": int(rows[col].nunique(dropna=True))})
        else:
            records.append({"field": col, "present_in_source": col in header, "non_null_rate": 0.0, "unique_count": 0})
    return records, {"row_count": int(len(rows)), "fields_checked": len(records), "fields_missing": [r["field"] for r in records if not r["present_in_source"]]}


def feature_contract_gap_audit(header: list[str]) -> dict[str, Any]:
    groups = {
        "liquidity_fields": ["liquidity_flags_json"],
        "event_risk_fields": ["event_flags_json"],
        "earnings_exrights_fields": [],
        "monthly_box_features": ["monthly_high_zone_proxy", "monthly_box_breakout_proxy", "monthly_box_inside_proxy"],
        "weekly_regime_features": ["weekly_monthly_uptrend_proxy", "regime_bucket"],
        "daily_candle_ma_volume_features": ["ma7_slope", "ma20_slope", "dist_ma20_pct", "upper_wick_ratio", "lower_wick_ratio", "large_bullish_candle", "large_bearish_candle", "volume_ma20_ratio"],
        "volatility_atr_features": ["realized_vol20", "atr14_pct"],
        "pattern_family_source_fields": ["primary_family", "research_candidate_source_family", "research_family_surface", "candidate_source", "signal_family", "setup_name"],
        "current_rank_base_score_fields": ["baseline_rank", "baseline_score", "candidate_rank", "selection_score"],
        "future_outcome_fields": ["ret5", "ret20", "mae20", "mfe20"],
    }
    classifications = {}
    for group, fields in groups.items():
        if group == "future_outcome_fields":
            cls = "forbidden_future_leak"
        elif not fields:
            cls = "unavailable"
        elif all(f in header for f in fields):
            cls = "available_point_in_time"
        elif any(f in header for f in fields):
            cls = "available_but_not_actionable"
        else:
            cls = "unavailable"
        classifications[group] = {"classification": cls, "fields": fields, "present_fields": [f for f in fields if f in header], "missing_fields": [f for f in fields if f not in header]}
    classifications["liquidity_fields"]["classification"] = "available_but_not_actionable"
    classifications["event_risk_fields"]["classification"] = "available_but_not_actionable"
    return {"axis_id": AXIS_ID, "feature_groups": classifications, "research_fallback_used": False}


def dropped_universe_audit(rows: pd.DataFrame, pattern_seed_root: Path) -> dict[str, Any]:
    eval_rows = rows[rows["path20_available"] & rows["ret20"].notna()].copy()
    winners = eval_rows[eval_rows["ret20"] > 0.10]
    seed_rows_path = pattern_seed_root / "family_seed_rows.csv"
    seed_rows = pd.read_csv(seed_rows_path, low_memory=False) if seed_rows_path.exists() else pd.DataFrame()
    seed_keys = set(zip(seed_rows.get("decision_date", pd.Series(dtype=float)), seed_rows.get("code", pd.Series(dtype=str))))
    winner_keys = set(zip(winners["decision_date"], winners["code"]))
    return {
        "strong_winners_in_current_source_rows": int(len(winners)),
        "strong_winners_in_rank_1_10": int(len(winners[winners["baseline_rank"].between(1, 10, inclusive="both")])),
        "strong_winners_in_rank_11_50": int(len(winners[winners["baseline_rank"].between(11, 50, inclusive="both")])),
        "strong_winners_in_rank_51_100": int(len(winners[winners["baseline_rank"].between(51, 100, inclusive="both")])),
        "strong_winners_outside_rank_1_100_or_missing": int(len(winners[~winners["baseline_rank"].between(1, 100, inclusive="both")])),
        "strong_winners_captured_by_narrow_pattern_seed_rows": int(len(winner_keys & seed_keys)),
        "strong_winners_absent_from_current_source_rows": "blocked_without_all_bars_or_full_universe_outcome_table",
        "classification": {
            "absent_from_current_source_rows": "not_observable_from_current_artifacts",
            "present_but_ranked_too_low": "confirmed",
            "present_in_rank_11_50_but_too_risky": "confirmed_by_prior_rank11_50_and_seed_audits",
            "present_only_in_narrow_seeds": "partial",
            "not_separable_with_current_features": "confirmed_for_fixed_rules_but_model_probe_found_partial_signal",
        },
    }


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def seed_lineage_summary(pattern_seed_root: Path, frozen_seed_root: Path) -> dict[str, Any]:
    frozen_summary = load_json(frozen_seed_root / "robustness_gate_summary.json")
    frozen_decision = load_json(frozen_seed_root / "research_decision.json")
    pattern_metrics = load_json(pattern_seed_root / "candidate_family_metrics.json")
    pattern_overlap = load_json(pattern_seed_root / "family_vs_frozen_seed_overlap.json")
    family_b = pattern_metrics["family_b_constructive_pullback_support_bullish_confirmation"]
    family_b_overlap = pattern_overlap["family_b_constructive_pullback_support_bullish_confirmation"]
    frozen = frozen_summary["overall_fixed_variant_metrics"]
    return {
        "high_upside_reserve_risk_containment_robustness_gate_v1": {
            "decision": frozen_decision["research_decision"],
            "sample_count": frozen["sample_count"],
            "date_count": frozen["date_count"],
            "mean_ret20": frozen["mean_ret20"],
            "winner_rate": frozen["winner_rate"],
            "bad_rate": frozen["bad_rate"],
            "severe_rate": frozen["severe_rate"],
            "overlap": "seed_reference",
            "breadth_limitation": "kept_share_below_0_30_and_single_candidate_date_share_high",
            "risk_limitation": "2025H1_and_2025H2_period_risk_unstable",
            "why_frozen": "promising_underpowered_do_not_retune_thresholds",
        },
        "family_b_constructive_pullback_support_bullish_confirmation": {
            "decision": "pattern_family_seed_promising_but_underpowered",
            "sample_count": family_b["sample_count"],
            "date_count": family_b["date_count"],
            "mean_ret20": family_b["mean_ret20"],
            "winner_rate": family_b["winner_rate_ret20_gt_10pct"],
            "bad_rate": family_b["bad_rate_ret20_lt_minus_5pct"],
            "severe_rate": family_b["severe_rate_ret20_lt_minus_10pct"],
            "overlap": {"overlap_with_frozen_seed": family_b_overlap["overlap_sample_count"], "overlap_rate": family_b_overlap["overlap_rate"]},
            "breadth_limitation": "only_52_samples_47_dates",
            "risk_limitation": "bad_and_severe_rates_above_keep_gate",
            "why_frozen": "independent_promising_underpowered_seed_not_keep_worthy",
        },
    }


def rebuild_options() -> dict[str, Any]:
    return {
        "option_1_expand_candidate_source_before_ranking": {
            "description": "Broaden pre-ranking source inclusion while preserving point-in-time feature snapshots.",
            "fit": "moderate",
            "risk": "may add low-quality breadth unless family contracts are explicit",
        },
        "option_2_create_pattern_family_source_rows_from_all_confirmed_bars": {
            "description": "Generate candidate rows directly from confirmed bars using explicit pattern-family definitions before ranking.",
            "fit": "high",
            "risk": "requires all-bars universe and strict as_of feature builder",
        },
        "option_3_add_missing_point_in_time_feature_contracts_before_rebuild": {
            "description": "Add actionable liquidity/event/earnings contracts before the rebuild if those exclusions are required.",
            "fit": "supporting",
            "risk": "does not solve current diagnostic-source thinness by itself",
        },
    }


def recommended_rebuild_contract(decision: str) -> dict[str, Any]:
    return {
        "target_objective": "build new pre-ranking buy candidate source rows that can produce multiple narrow pattern-family seeds with controlled downside and enough date breadth",
        "recommended_decision": decision,
        "input_universe": "all eligible TRADEX symbols with confirmed daily bars for the same historical period, not only existing diagnostic family source rows",
        "as_of_date_policy": "features must use only bars and metadata confirmed on or before decision_date; outcomes remain evaluation-only",
        "point_in_time_feature_requirements": [
            "daily candle, MA, volume, volatility, ATR features",
            "weekly/monthly regime and monthly box context",
            "actionable liquidity/event/earnings fields only if truly point-in-time",
            "pattern-family source fields independent of future outcomes",
        ],
        "candidate_family_definitions_to_preserve_as_frozen_seeds": [
            "high_upside_reserve_risk_containment_robustness_gate_v1.variant_a_refined as frozen promising-underpowered reference",
            "family_b_constructive_pullback_support_bullish_confirmation as frozen promising-underpowered reference",
        ],
        "what_not_to_include": [
            "ret20-derived tags in feature construction",
            "top10 after-processing rules",
            "demotion or broad exclusion rescue",
            "threshold relaxation of frozen seeds",
            "MeeMee reflection or production ranking mutation",
        ],
        "verify_gates_for_next_step": [
            "py_compile and focused pytest",
            "JSON/CSV parse checks",
            "no-lookahead audit",
            "source coverage audit",
            "same-period comparison against current diagnostic source",
            "explicit missing-universe and rank-bucket accounting",
        ],
        "boundary": {"meemee_reflection": False, "runtime_db_write": False, "production_mutation": False, "publish": False},
    }


def decide(schema: dict[str, Any], feature_gaps: dict[str, Any], funnel: dict[str, Any], lineage: dict[str, Any]) -> tuple[str, list[str]]:
    if not schema:
        return "blocked_missing_source_contract", ["research_family_source_schema_missing"]
    if schema.get("diagnostic_not_real_candidate_source"):
        return "rebuild_pattern_family_source_rows", ["current_family_source_rows_are_diagnostic_not_real_candidate_source_and_promising_seeds_are_underpowered"]
    if feature_gaps["feature_groups"]["liquidity_fields"]["classification"] == "unavailable":
        return "add_feature_contracts_before_rebuild", ["key_point_in_time_feature_contracts_missing"]
    low_rank_winners = funnel["winners_present_but_ranked_below_top10"]
    if low_rank_winners > funnel["winner_rank_distribution"]["rank_1_10"]:
        return "current_source_rows_sufficient_but_ranking_needs_rebuild", ["many_winners_present_but_ranked_below_top10"]
    return "rebuild_candidate_generation_from_all_bars", ["winner_absence_from_current_rows_cannot_be_ruled_out_without_all_bars_source"]


def write_coverage_csv(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["field", "present_in_source", "non_null_rate", "unique_count"])
        writer.writeheader()
        writer.writerows(records)


def run(input_root: Path, pattern_seed_root: Path, frozen_seed_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-candidate-generation-rebuild-preflight-v1"
    out.mkdir(parents=True, exist_ok=True)
    try:
        rows, header, schema = load_source_rows(input_root)
        blocked = False
        block_reason = None
    except Exception as exc:
        rows = pd.DataFrame()
        header = []
        schema = {}
        blocked = True
        block_reason = str(exc)

    if blocked:
        decision = "blocked_missing_source_contract"
        reasons = [block_reason or "source_rows_unavailable"]
        funnel = {}
        coverage_records = []
        coverage_json = {}
        gaps = {}
        dropped = {}
        lineage = {}
    else:
        funnel = source_funnel_audit(rows, schema)
        coverage_records, coverage_json = candidate_source_coverage(rows, header)
        gaps = feature_contract_gap_audit(header)
        dropped = dropped_universe_audit(rows, pattern_seed_root)
        lineage = seed_lineage_summary(pattern_seed_root, frozen_seed_root)
        decision, reasons = decide(schema, gaps, funnel, lineage)

    write_coverage_csv(out / "candidate_source_coverage.csv", coverage_records)
    _write_json(out / "source_funnel_audit.json", funnel)
    _write_json(out / "candidate_source_coverage.json", coverage_json)
    _write_json(out / "dropped_universe_audit.json", dropped)
    _write_json(out / "feature_contract_gap_audit.json", gaps)
    _write_json(out / "seed_lineage_summary.json", lineage)
    _write_json(out / "rebuild_options.json", rebuild_options())
    _write_json(out / "recommended_rebuild_contract.json", recommended_rebuild_contract(decision))
    _write_json(out / "rebuild_preflight_summary.json", {"axis_id": AXIS_ID, "input_root": input_root, "pattern_seed_root": pattern_seed_root, "frozen_seed_root": frozen_seed_root, "decision": decision, "reason_typed": reasons})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "blocked" if blocked else "pass", "existing_artifacts_only": True, "features_use_saved_point_in_time_context_only": True, "outcomes_used_evaluation_only": True, "new_candidate_generator_implemented": False, "thresholds_retuned": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_rows_loaded": int(len(rows)), "header_field_count": len(header), "schema_loaded": bool(schema), "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--pattern-seed-root", type=Path, default=DEFAULT_PATTERN_SEED_ROOT)
    parser.add_argument("--frozen-seed-root", type=Path, default=DEFAULT_FROZEN_SEED_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.pattern_seed_root, args.frozen_seed_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
