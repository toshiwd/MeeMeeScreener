from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "family_definition_v2_source_rows"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\pattern_family_source_rows_v1\20260525T101220Z-pattern-family-source-rows-v1")
DEFAULT_DESIGN_ROOT = Path(r"G:\Tradex\family_definition_contract_v2_design_audit\20260525T132026Z-family-definition-contract-v2-design-audit")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\family_definition_v2_source_rows")
V2_FLAGS = [
    "high_upside_contained_reserve_family_v2",
    "constructive_pullback_confirmation_family_v2",
    "volatility_compression_pre_breakout_family_v2",
]
OFFLINE_OUTCOME_COLUMNS = ["ret5", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"]
IDENTIFIER_COLUMNS = ["as_of_date", "code"]
SOURCE_COLUMNS = ["source_db", "source_bar_status", "source_lineage"]
REQUIRED_ARTIFACTS = (
    "family_v2_source_summary.json",
    "family_v2_source_rows.parquet",
    "family_v2_source_rows_sample.csv",
    "family_v2_definition_contract.json",
    "feature_contract.json",
    "as_of_policy.json",
    "family_v2_flag_counts.json",
    "family_v2_overlap_matrix.json",
    "offline_outcome_audit.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
LIVE_FEATURE_COLUMNS = [
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
    "gap_up_flag",
    "gap_down_flag",
    "volume_vs_20d_avg",
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
    "monthly_box_month_count",
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


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def load_source(source_root: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    rows = pd.read_parquet(source_root / "pattern_family_source_rows.parquet")
    feature_contract = json.loads((source_root / "feature_contract.json").read_text(encoding="utf-8"))
    source_coverage = json.loads((source_root / "source_coverage.json").read_text(encoding="utf-8"))
    return rows, feature_contract, source_coverage


def required_missing(rows: pd.DataFrame) -> list[str]:
    return [c for c in LIVE_FEATURE_COLUMNS + OFFLINE_OUTCOME_COLUMNS if c not in rows.columns]


def add_v2_flags(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    clean_candle = ~out["failed_high_flag"].astype(bool) & ~out["bearish_body_flag"].astype(bool) & (out["upper_wick_ratio"] <= 0.28)
    contained_vol = (out["atr14_pct"] <= 0.045) & (out["realized_vol20"] <= 0.035)
    no_extension = (out["close_vs_ma20_pct"] <= 0.08) & (out["close_vs_ma60_pct"] <= 0.22)
    constructive_trend = (out["ma7_slope_5d"] > 0) & (out["ma20_slope_10d"] >= 0) & out["close_above_ma20"].astype(bool)
    turnover_ok = out["volume_vs_20d_avg"].between(0.75, 2.0, inclusive="both")

    out["high_upside_contained_reserve_family_v2"] = (
        constructive_trend
        & out["weekly_supportive_flag"].astype(bool)
        & (out["monthly_box_position"].between(0.35, 0.90, inclusive="both"))
        & contained_vol
        & no_extension
        & clean_candle
        & turnover_ok
        & (out["recent_high_distance_pct"] >= -0.12)
    )
    out["constructive_pullback_confirmation_family_v2"] = (
        out["weekly_supportive_flag"].astype(bool)
        & out["monthly_box_position"].between(0.15, 0.65, inclusive="both")
        & out["close_vs_ma20_pct"].between(-0.035, 0.035, inclusive="both")
        & out["bullish_body_flag"].astype(bool)
        & (out["body_ratio"] >= 0.35)
        & (out["lower_wick_ratio"] >= 0.22)
        & clean_candle
        & contained_vol
        & (out["volume_vs_20d_avg"].between(0.9, 1.8, inclusive="both"))
    )
    out["volatility_compression_pre_breakout_family_v2"] = (
        out["monthly_supportive_flag"].astype(bool)
        & out["monthly_box_position"].between(0.45, 0.82, inclusive="both")
        & (out["monthly_box_width_pct"] >= 0.08)
        & (out["realized_vol20"] <= 0.020)
        & (out["atr14_pct"] <= 0.030)
        & (out["close_vs_ma20_pct"].between(-0.025, 0.045, inclusive="both"))
        & (out["recent_high_distance_pct"].between(-0.08, 0.0, inclusive="both"))
        & (out["ma7_slope_5d"] > 0)
        & (out["ma20_slope_10d"] >= 0)
        & (out["volume_vs_20d_avg"].between(0.8, 1.6, inclusive="both"))
        & clean_candle
    )
    return out


def family_contract(design_contract: dict[str, Any]) -> dict[str, Any]:
    concepts = {f["family_id"]: f for f in design_contract.get("families", [])}
    out: dict[str, Any] = {"axis_id": AXIS_ID, "source_design_contract": design_contract.get("contract_id"), "families": {}}
    for flag in V2_FLAGS:
        concept = concepts.get(flag, {})
        out["families"][flag] = {
            "family_id": flag,
            "hypothesis": concept.get("hypothesis"),
            "inclusion_conditions": concept.get("inclusion_conditions", []),
            "exclusion_conditions": concept.get("exclusion_risk_conditions", []),
            "required_features": concept.get("required_point_in_time_features", []),
            "optional_features": ["actionable liquidity/event fields", "earnings/ex-rights fields"],
            "unavailable_features": ["actionable liquidity/event fields", "earnings/ex-rights fields"],
            "expected_failure_mode": concept.get("expected_failure_mode"),
            "forbidden_features": concept.get("forbidden_terms_features", []),
            "live_feature_columns": LIVE_FEATURE_COLUMNS,
            "offline_evaluation_columns": OFFLINE_OUTCOME_COLUMNS,
        }
    return out


def feature_contract(rows: pd.DataFrame) -> dict[str, Any]:
    fields = {}
    for col in rows.columns:
        if col in IDENTIFIER_COLUMNS:
            cls = "identifier"
        elif col in SOURCE_COLUMNS:
            cls = "source_metadata"
        elif col in OFFLINE_OUTCOME_COLUMNS:
            cls = "offline_outcome_only"
        elif col in V2_FLAGS or col in LIVE_FEATURE_COLUMNS:
            cls = "point_in_time_feature"
        elif col == "ret20_derived_tags":
            cls = "forbidden_future_leak"
        else:
            cls = "source_metadata"
        fields[col] = {"classification": cls}
    fields["liquidity_event_fields"] = {"classification": "unavailable"}
    fields["earnings_exrights_fields"] = {"classification": "unavailable"}
    fields["ret20_derived_tags"] = {"classification": "forbidden_future_leak"}
    return {"axis_id": AXIS_ID, "fields": fields}


def flag_counts(rows: pd.DataFrame) -> dict[str, Any]:
    per_date = {}
    out = {"rows_per_family": {}, "dates_per_family": {}, "average_candidates_per_date_per_family": {}, "median_candidates_per_date_per_family": {}, "zero_candidate_dates_per_family": {}}
    all_dates = int(rows["as_of_date"].nunique())
    for flag in V2_FLAGS:
        selected = rows[rows[flag]]
        counts = selected.groupby("as_of_date").size() if not selected.empty else pd.Series(dtype=float)
        per_date[flag] = counts
        out["rows_per_family"][flag] = int(len(selected))
        out["dates_per_family"][flag] = int(selected["as_of_date"].nunique()) if not selected.empty else 0
        out["average_candidates_per_date_per_family"][flag] = None if counts.empty else float(counts.mean())
        out["median_candidates_per_date_per_family"][flag] = None if counts.empty else float(counts.median())
        out["zero_candidate_dates_per_family"][flag] = int(all_dates - out["dates_per_family"][flag])
    return out


def overlap_matrix(rows: pd.DataFrame) -> dict[str, Any]:
    out = {}
    keys = {flag: set(zip(rows.loc[rows[flag], "as_of_date"], rows.loc[rows[flag], "code"])) for flag in V2_FLAGS}
    for left in V2_FLAGS:
        out[left] = {}
        for right in V2_FLAGS:
            inter = keys[left] & keys[right]
            out[left][right] = {
                "overlap_count": int(len(inter)),
                "left_count": int(len(keys[left])),
                "right_count": int(len(keys[right])),
                "left_overlap_rate": None if not keys[left] else len(inter) / len(keys[left]),
            }
    return out


def outcome_audit(rows: pd.DataFrame) -> dict[str, Any]:
    out = {"outcomes_are_offline_only": True, "outcome_coverage_rate": float(rows["ret20"].notna().mean()) if "ret20" in rows else 0.0, "family_metrics": {}}
    for flag in V2_FLAGS:
        selected = rows[rows[flag]]
        out["family_metrics"][flag] = {
            "sample_count": int(len(selected)),
            "date_count": int(selected["as_of_date"].nunique()) if not selected.empty else 0,
            "mean_ret5": _mean(selected, "ret5"),
            "mean_ret20": _mean(selected, "ret20"),
            "median_ret20": _median(selected, "ret20"),
            "winner_rate_ret20_gt_10pct": _rate(selected["winner_ret20_gt_10pct"]) if not selected.empty else None,
            "bad_rate_ret20_lt_minus_5pct": _rate(selected["bad_ret20_lt_minus_5pct"]) if not selected.empty else None,
            "severe_rate_ret20_lt_minus_10pct": _rate(selected["severe_ret20_lt_minus_10pct"]) if not selected.empty else None,
        }
    return out


def summary(rows: pd.DataFrame, counts: dict[str, Any]) -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "total_rows": int(len(rows)),
        "date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0,
        "code_count": int(rows["code"].nunique()) if not rows.empty else 0,
        **counts,
    }


def decide(rows: pd.DataFrame, missing: list[str], no_lookahead_ok: bool) -> tuple[str, list[str]]:
    if missing:
        return "family_v2_source_rows_created_but_feature_gaps", [f"missing_required_columns:{','.join(missing)}"]
    if not no_lookahead_ok:
        return "blocked_no_lookahead_violation", ["source_no_lookahead_audit_not_pass"]
    if rows.empty:
        return "blocked_missing_confirmed_bar_source", ["no_source_rows_loaded"]
    if not any(int(rows[flag].sum()) > 0 for flag in V2_FLAGS):
        return "family_v2_source_rows_created_but_feature_gaps", ["all_family_v2_flags_zero"]
    return "family_v2_source_rows_ready_for_evaluation", ["all_family_v2_flags_generated_with_point_in_time_features_and_offline_outcomes_separated"]


def run(source_root: Path, design_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-family-definition-v2-source-rows"
    out.mkdir(parents=True, exist_ok=True)
    source_rows, source_contract, source_coverage = load_source(source_root)
    source_no_lookahead = json.loads((source_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    design_contract = json.loads((design_root / "proposed_family_definition_contract_v2.json").read_text(encoding="utf-8"))
    missing = required_missing(source_rows)
    if missing:
        rows = source_rows.head(0).copy()
    else:
        rows = add_v2_flags(source_rows)
        rows = rows.rename(columns={"source_db_path": "source_db"})
        rows["source_bar_status"] = "confirmed_non_yahoo_daily_bars"
        rows["source_lineage"] = AXIS_ID
        keep_cols = IDENTIFIER_COLUMNS + SOURCE_COLUMNS + V2_FLAGS + LIVE_FEATURE_COLUMNS + OFFLINE_OUTCOME_COLUMNS
        rows = rows[keep_cols].copy()
    counts = flag_counts(rows) if not rows.empty else {"rows_per_family": {f: 0 for f in V2_FLAGS}, "dates_per_family": {f: 0 for f in V2_FLAGS}, "average_candidates_per_date_per_family": {f: None for f in V2_FLAGS}, "median_candidates_per_date_per_family": {f: None for f in V2_FLAGS}, "zero_candidate_dates_per_family": {f: 0 for f in V2_FLAGS}}
    decision, reasons = decide(rows, missing, source_no_lookahead.get("audit_result") == "pass")
    rows.to_parquet(out / "family_v2_source_rows.parquet", index=False)
    rows.head(1000).to_csv(out / "family_v2_source_rows_sample.csv", index=False)
    _write_json(out / "family_v2_definition_contract.json", family_contract(design_contract))
    _write_json(out / "feature_contract.json", feature_contract(rows))
    _write_json(out / "as_of_policy.json", {"row_key": ["as_of_date", "code"], "source_policy": "inherits pattern_family_source_rows_v1 confirmed-bar as-of policy", "family_flags_use_offline_outcomes": False, "offline_outcomes_in_live_features": False})
    _write_json(out / "family_v2_flag_counts.json", counts)
    _write_json(out / "family_v2_overlap_matrix.json", overlap_matrix(rows) if not rows.empty else {})
    _write_json(out / "offline_outcome_audit.json", outcome_audit(rows) if not rows.empty else {"outcomes_are_offline_only": True, "outcome_coverage_rate": 0.0, "family_metrics": {}})
    _write_json(out / "family_v2_source_summary.json", {**summary(rows, counts), "decision": decision, "reason_typed": reasons})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass" if decision != "blocked_no_lookahead_violation" else "blocked", "source_no_lookahead_audit": source_no_lookahead.get("audit_result"), "family_flags_use_point_in_time_features_only": True, "offline_outcomes_used_in_family_flags": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_root": source_root, "source_rows": int(len(source_rows)), "generated_rows": int(len(rows)), "date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0, "code_count": int(rows["code"].nunique()) if not rows.empty else 0, "source_coverage": source_coverage, "missing_required_columns": missing, "research_fallback_used": False})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--design-root", type=Path, default=DEFAULT_DESIGN_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.source_root, args.design_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
