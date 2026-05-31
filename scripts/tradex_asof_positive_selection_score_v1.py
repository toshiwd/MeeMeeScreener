from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "asof_positive_selection_score_v1"
SCORE_VERSION = "asof_positive_selection_score_v1_transparent_weighted_20260525"
DEFAULT_V2_SOURCE_ROOT = Path(r"G:\Tradex\family_definition_v2_source_rows\20260525T132408Z-family-definition-v2-source-rows")
DEFAULT_V1_SOURCE_ROOT = Path(r"G:\Tradex\pattern_family_source_rows_v1\20260525T101220Z-pattern-family-source-rows-v1")
DEFAULT_BLOCKER_ROOT = Path(r"G:\Tradex\buy_candidate_generation_surface_closure_v1\20260525T133246Z-buy-candidate-generation-surface-closure-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\asof_positive_selection_score_v1")

IDENTIFIER_COLUMNS = {"as_of_date", "code"}
SOURCE_METADATA_COLUMNS = {"source_db", "source_bar_status", "source_lineage"}
OFFLINE_OUTCOME_COLUMNS = {
    "ret5",
    "ret20",
    "winner_ret20_gt_10pct",
    "bad_ret20_lt_minus_5pct",
    "severe_ret20_lt_minus_10pct",
}
FORBIDDEN_TERMS = ("ret5", "ret10", "ret20", "future", "winner", "bad", "severe", "trigger", "invalidation")
FAMILY_FLAG_COLUMNS = [
    "high_upside_contained_reserve_family_v2",
    "constructive_pullback_confirmation_family_v2",
    "volatility_compression_pre_breakout_family_v2",
]
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
REQUIRED_ARTIFACTS = (
    "asof_positive_selection_score_summary.json",
    "asof_positive_selection_score_rows.parquet",
    "asof_positive_selection_score_rows_sample.csv",
    "score_contract.json",
    "feature_contract.json",
    "model_probe_contract.json",
    "score_bucket_metrics.json",
    "score_stability_metrics.json",
    "offline_outcome_audit.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
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


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_input(v2_root: Path, v1_root: Path) -> tuple[Path, str]:
    v2 = v2_root / "family_v2_source_rows.parquet"
    if v2.exists():
        return v2, "family_definition_v2_source_rows"
    v1 = v1_root / "pattern_family_source_rows.parquet"
    if v1.exists():
        return v1, "pattern_family_source_rows_v1"
    raise FileNotFoundError("no usable all-bars source rows parquet found")


def validate_live_features(frame: pd.DataFrame) -> tuple[list[str], list[str]]:
    available = [col for col in LIVE_FEATURE_COLUMNS if col in frame.columns]
    missing = [col for col in LIVE_FEATURE_COLUMNS if col not in frame.columns]
    return available, missing


def forbidden_live_feature_violations(columns: list[str]) -> list[str]:
    violations = []
    for col in columns:
        lower = col.lower()
        if any(term in lower for term in FORBIDDEN_TERMS):
            violations.append(col)
    return violations


def _pct_by_date(frame: pd.DataFrame, column: str, *, ascending: bool = True) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    return values.groupby(frame["as_of_date"]).rank(pct=True, ascending=ascending)


def _bool_score(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(0.0, index=frame.index)
    return frame[column].fillna(False).astype(bool).astype(float)


def build_score(frame: pd.DataFrame, live_features: list[str]) -> pd.DataFrame:
    missing_reason = []
    for _, row in frame[live_features].isna().iterrows():
        missing = [col for col, is_missing in row.items() if bool(is_missing)]
        missing_reason.append(",".join(missing[:8]) if missing else "")

    scored = frame.copy()
    components = {
        "daily_trend": (
            0.18 * _pct_by_date(scored, "close_vs_ma20_pct")
            + 0.10 * _pct_by_date(scored, "ma20_slope_10d")
            + 0.07 * _bool_score(scored, "ma7_above_ma20")
            + 0.05 * _bool_score(scored, "close_above_ma20")
        ),
        "multi_timeframe_support": (
            0.12 * _bool_score(scored, "weekly_supportive_flag")
            + 0.10 * _bool_score(scored, "monthly_supportive_flag")
            + 0.08 * _pct_by_date(scored, "monthly_close_vs_ma20_pct")
        ),
        "constructive_confirmation": (
            0.10 * _bool_score(scored, "bullish_body_flag")
            + 0.07 * _pct_by_date(scored, "body_ratio")
            + 0.06 * _pct_by_date(scored, "volume_vs_20d_avg")
            + 0.05 * _pct_by_date(scored, "lower_wick_ratio")
        ),
        "controlled_extension": (
            0.08 * _pct_by_date(scored, "atr14_pct", ascending=False)
            + 0.07 * _pct_by_date(scored, "realized_vol20", ascending=False)
            + 0.05 * _pct_by_date(scored, "recent_high_distance_pct", ascending=False)
        ),
        "risk_penalty": (
            -0.12 * _bool_score(scored, "failed_high_flag")
            -0.08 * _bool_score(scored, "weekly_failed_high_flag")
            -0.08 * _bool_score(scored, "bearish_body_flag")
            -0.07 * _pct_by_date(scored, "upper_wick_ratio")
            -0.04 * _bool_score(scored, "gap_down_flag")
        ),
    }
    raw = sum(components.values())
    scored["asof_positive_selection_score_v1"] = raw.fillna(raw.groupby(scored["as_of_date"]).transform("median")).fillna(0.0)
    scored["score_rank_by_date"] = scored.groupby("as_of_date")["asof_positive_selection_score_v1"].rank(method="first", ascending=False).astype(int)
    scored["score_percentile_by_date"] = scored.groupby("as_of_date")["asof_positive_selection_score_v1"].rank(pct=True, ascending=True)
    scored["score_bucket"] = scored["score_percentile_by_date"].map(score_bucket)
    scored["score_source"] = "transparent_point_in_time_weighted_score"
    scored["score_contract_version"] = SCORE_VERSION
    scored["live_feature_available_flag"] = scored[live_features].notna().all(axis=1)
    scored["feature_missing_reason"] = missing_reason
    return scored


def score_bucket(percentile: float) -> str:
    if pd.isna(percentile):
        return "missing_score"
    if percentile >= 0.99:
        return "top_1pct"
    if percentile >= 0.97:
        return "top_3pct"
    if percentile >= 0.95:
        return "top_5pct"
    if percentile >= 0.90:
        return "top_10pct"
    if percentile >= 0.80:
        return "top_20pct"
    return "remaining"


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def metric(frame: pd.DataFrame) -> dict[str, Any]:
    per_date = frame.groupby("as_of_date").size() if not frame.empty else pd.Series(dtype=float)
    winner_rate = _rate(frame["winner_ret20_gt_10pct"]) if "winner_ret20_gt_10pct" in frame else None
    bad_rate = _rate(frame["bad_ret20_lt_minus_5pct"]) if "bad_ret20_lt_minus_5pct" in frame else None
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["as_of_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "hit_rate_ret20_gt_0": _rate(frame["ret20"] > 0) if "ret20" in frame and not frame.empty else None,
        "winner_rate_ret20_gt_10pct": winner_rate,
        "bad_rate_ret20_lt_minus_5pct": bad_rate,
        "severe_rate_ret20_lt_minus_10pct": _rate(frame["severe_ret20_lt_minus_10pct"]) if "severe_ret20_lt_minus_10pct" in frame else None,
        "downside_to_upside_ratio": None if not winner_rate else (bad_rate or 0.0) / winner_rate,
        "outcome_coverage_rate": None if frame.empty or "ret20" not in frame else float(frame["ret20"].notna().mean()),
    }


def bucket_metrics(scored: pd.DataFrame) -> dict[str, Any]:
    order = ["top_1pct", "top_3pct", "top_5pct", "top_10pct", "top_20pct", "remaining"]
    return {bucket: metric(scored[scored["score_bucket"] == bucket]) for bucket in order}


def period_half(value: Any) -> str | None:
    if pd.isna(value):
        return None
    text = str(int(value))
    if len(text) < 6:
        return None
    return f"{text[:4]}{'H1' if int(text[4:6]) <= 6 else 'H2'}"


def stability_metrics(scored: pd.DataFrame) -> dict[str, Any]:
    frame = scored.copy()
    frame["period_half"] = frame["as_of_date"].map(period_half)
    out: dict[str, Any] = {"period_bucket_metrics": {}, "date_concentration_audit": {}, "code_concentration_audit": {}, "high_score_overlap": {}}
    high = frame[frame["score_bucket"].isin(["top_1pct", "top_3pct", "top_5pct"])]
    for period, group in high.groupby("period_half", dropna=True):
        out["period_bucket_metrics"][str(period)] = metric(group)
    per_date = high.groupby("as_of_date").size() if not high.empty else pd.Series(dtype=float)
    out["date_concentration_audit"] = {
        "sample_count": int(len(high)),
        "date_count": int(high["as_of_date"].nunique()) if not high.empty else 0,
        "average_candidates_per_date": None if per_date.empty else float(per_date.mean()),
        "median_candidates_per_date": None if per_date.empty else float(per_date.median()),
        "max_candidates_per_date": None if per_date.empty else int(per_date.max()),
        "top_10_dates_share_of_samples": None if per_date.empty or len(high) == 0 else float(per_date.sort_values(ascending=False).head(10).sum() / len(high)),
    }
    per_code = high.groupby("code").size() if not high.empty else pd.Series(dtype=float)
    out["code_concentration_audit"] = {
        "code_count": int(high["code"].nunique()) if not high.empty else 0,
        "top_10_codes_share_of_samples": None if per_code.empty or len(high) == 0 else float(per_code.sort_values(ascending=False).head(10).sum() / len(high)),
    }
    for flag in FAMILY_FLAG_COLUMNS:
        out["high_score_overlap"][flag] = {
            "overlap_count": int(high[flag].fillna(False).astype(bool).sum()) if flag in high else 0,
            "overlap_rate": None if high.empty or flag not in high else float(high[flag].fillna(False).astype(bool).mean()),
        }
    return out


def feature_contract(source_contract: dict[str, Any], live_features: list[str], missing_live_features: list[str]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    all_columns = set(IDENTIFIER_COLUMNS) | set(SOURCE_METADATA_COLUMNS) | set(live_features) | set(OFFLINE_OUTCOME_COLUMNS) | set(FAMILY_FLAG_COLUMNS)
    score_columns = {
        "asof_positive_selection_score_v1",
        "score_bucket",
        "score_percentile_by_date",
        "score_rank_by_date",
        "score_source",
        "score_contract_version",
        "live_feature_available_flag",
        "feature_missing_reason",
    }
    for col in sorted(all_columns | score_columns):
        if col in IDENTIFIER_COLUMNS:
            cls = "identifier"
        elif col in SOURCE_METADATA_COLUMNS:
            cls = "source_metadata"
        elif col in OFFLINE_OUTCOME_COLUMNS:
            cls = "offline_outcome_only"
        elif col in score_columns:
            cls = "point_in_time_feature"
        elif col in live_features or col in FAMILY_FLAG_COLUMNS:
            cls = "point_in_time_feature"
        else:
            cls = "unavailable"
        fields[col] = {"classification": cls}
    for col in missing_live_features:
        fields[col] = {"classification": "unavailable"}
    fields["liquidity_event_fields"] = {"classification": "unavailable", "downstream_dependency": "actionable_event_liquidity_risk_v1"}
    fields["earnings_exrights_fields"] = {"classification": "unavailable", "downstream_dependency": "actionable_event_liquidity_risk_v1"}
    fields["ret20_derived_tags"] = {"classification": "forbidden_future_leak"}
    return {
        "axis_id": AXIS_ID,
        "source_feature_contract_axis": source_contract.get("axis_id"),
        "fields": fields,
    }


def offline_outcome_audit(scored: pd.DataFrame) -> dict[str, Any]:
    return {
        "outcomes_are_offline_only": True,
        "outcome_columns": sorted(OFFLINE_OUTCOME_COLUMNS),
        "outcome_coverage_rate": float(scored["ret20"].notna().mean()) if "ret20" in scored else 0.0,
        "overall_metrics": metric(scored),
        "bucket_metrics_reference": "score_bucket_metrics.json",
    }


def decide(metrics: dict[str, Any], source_coverage: dict[str, Any], no_lookahead_pass: bool) -> tuple[str, str, list[str]]:
    if not no_lookahead_pass:
        return "blocked_no_lookahead_violation", "BLOCKED", ["no_lookahead_audit_failed"]
    if source_coverage.get("missing_required_live_features"):
        return "blocked_missing_point_in_time_features", "BLOCKED", ["required_point_in_time_features_missing"]

    top5 = metrics["top_5pct"]
    remaining = metrics["remaining"]
    top5_mean = top5.get("mean_ret20") or 0.0
    remaining_mean = remaining.get("mean_ret20") or 0.0
    top5_winner = top5.get("winner_rate_ret20_gt_10pct") or 0.0
    remaining_winner = remaining.get("winner_rate_ret20_gt_10pct") or 0.0
    top5_bad = top5.get("bad_rate_ret20_lt_minus_5pct") or 1.0
    remaining_bad = remaining.get("bad_rate_ret20_lt_minus_5pct") or 0.0
    top5_severe = top5.get("severe_rate_ret20_lt_minus_10pct") or 1.0
    remaining_severe = remaining.get("severe_rate_ret20_lt_minus_10pct") or 0.0
    broad = (top5.get("sample_count") or 0) >= 10000 and (top5.get("date_count") or 0) >= 1000
    upside_edge = top5_mean >= remaining_mean + 0.01 and top5_winner >= remaining_winner + 0.03
    risk_controlled = top5_bad <= remaining_bad + 0.025 and top5_severe <= remaining_severe + 0.02

    if upside_edge and risk_controlled and broad:
        return "asof_positive_score_ready_for_family_source_integration", "KEEP", ["high_score_buckets_separate_upside_with_acceptable_risk_and_breadth"]
    if upside_edge and broad:
        return "asof_positive_score_signal_exists_but_risk_uncontrolled", "HOLD_UNDERPOWERED", ["upside_signal_separates_but_bad_or_severe_rate_requires_actionable_risk_contract"]
    if upside_edge:
        return "asof_positive_score_promising_but_underpowered", "HOLD_UNDERPOWERED", ["upside_signal_positive_but_sample_or_date_breadth_thin"]
    return "asof_positive_score_no_edge", "DROP", ["high_score_buckets_do_not_separate_return_or_winner_quality"]


def output_columns(frame: pd.DataFrame, live_features: list[str]) -> list[str]:
    cols = [
        "as_of_date",
        "code",
        "asof_positive_selection_score_v1",
        "score_bucket",
        "score_percentile_by_date",
        "score_rank_by_date",
        "score_source",
        "score_contract_version",
        "live_feature_available_flag",
        "feature_missing_reason",
    ]
    cols.extend(col for col in SOURCE_METADATA_COLUMNS if col in frame.columns)
    cols.extend(col for col in live_features if col in frame.columns)
    cols.extend(col for col in FAMILY_FLAG_COLUMNS if col in frame.columns)
    cols.extend(col for col in OFFLINE_OUTCOME_COLUMNS if col in frame.columns)
    return cols


def run(v2_source_root: Path = DEFAULT_V2_SOURCE_ROOT, v1_source_root: Path = DEFAULT_V1_SOURCE_ROOT, blocker_root: Path = DEFAULT_BLOCKER_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    source_path, source_kind = resolve_input(v2_source_root, v1_source_root)
    source_root = source_path.parent
    source_contract = _load_json(source_root / "feature_contract.json")
    source_no_lookahead = _load_json(source_root / "no_lookahead_audit.json")
    blocker_decision = _load_json(blocker_root / "research_decision.json")
    frame = pd.read_parquet(source_path)
    live_features, missing_features = validate_live_features(frame)
    violations = forbidden_live_feature_violations(live_features)
    if violations:
        raise RuntimeError(f"forbidden live feature terms detected: {violations}")
    if not live_features:
        raise RuntimeError("no point-in-time live features available")

    scored = build_score(frame, live_features)
    metrics = bucket_metrics(scored)
    stability = stability_metrics(scored)
    no_lookahead_pass = source_no_lookahead.get("audit_result") == "pass"
    source_coverage = {
        "axis_id": AXIS_ID,
        "input_source_kind": source_kind,
        "input_source_path": str(source_path),
        "source_db": str(scored["source_db"].dropna().iloc[0]) if "source_db" in scored and scored["source_db"].notna().any() else None,
        "row_count": int(len(scored)),
        "date_count": int(scored["as_of_date"].nunique()),
        "code_count": int(scored["code"].nunique()),
        "live_feature_count": int(len(live_features)),
        "missing_optional_live_features": missing_features,
        "missing_required_live_features": [],
        "live_feature_complete_rate": float(scored["live_feature_available_flag"].mean()),
        "research_fallback_used": False,
    }
    decision, decision_class, reasons = decide(metrics, source_coverage, no_lookahead_pass)

    out = output_root / f"{_now_tag()}-asof-positive-selection-score-v1"
    out.mkdir(parents=True, exist_ok=True)
    cols = output_columns(scored, live_features)
    scored[cols].to_parquet(out / "asof_positive_selection_score_rows.parquet", index=False)
    scored[cols].head(25000).to_csv(out / "asof_positive_selection_score_rows_sample.csv", index=False)
    score_contract = {
        "axis_id": AXIS_ID,
        "score_id": "asof_positive_selection_score_v1",
        "score_contract_version": SCORE_VERSION,
        "score_type": "transparent_weighted_point_in_time_score",
        "diagnostic_only": True,
        "production_model": False,
        "live_feature_columns": live_features,
        "offline_evaluation_columns": sorted(OFFLINE_OUTCOME_COLUMNS),
        "forbidden_features": list(FORBIDDEN_TERMS),
        "score_columns": [
            "asof_positive_selection_score_v1",
            "score_bucket",
            "score_percentile_by_date",
            "score_rank_by_date",
            "score_source",
            "score_contract_version",
            "live_feature_available_flag",
            "feature_missing_reason",
        ],
        "construction": {
            "normalization": "same_as_of_date_percentile_ranks_for_numeric_features",
            "weights_are_predeclared": True,
            "outcomes_used_in_score": False,
            "family_v2_flags_used_as_score_targets": False,
            "family_v2_flags_in_output_as_metadata": True,
        },
    }
    model_probe_contract = {
        "axis_id": AXIS_ID,
        "model_probe_used": False,
        "reason": "transparent deterministic score chosen to keep every row as-of safe without production training",
        "dependency_required": False,
        "chronological_split_required_if_future_model_probe_is_added": True,
        "diagnostic_only": True,
    }
    summary = {
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "input_source_kind": source_kind,
        "input_source_path": source_path,
        "row_count": int(len(scored)),
        "date_count": int(scored["as_of_date"].nunique()),
        "code_count": int(scored["code"].nunique()),
        "top_5pct_metrics": metrics["top_5pct"],
        "remaining_metrics": metrics["remaining"],
        "downstream_dependency": "actionable_event_liquidity_risk_v1",
    }
    no_lookahead = {
        "audit_result": "pass" if no_lookahead_pass else "blocked",
        "no_lookahead_pass": bool(no_lookahead_pass),
        "source_no_lookahead_audit": source_no_lookahead.get("audit_result"),
        "score_uses_point_in_time_features_only": True,
        "offline_outcomes_used_in_score": False,
        "forbidden_live_feature_violations": violations,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }
    research_decision = {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "meemee_reflectable_candidate": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "production_candidate_generator_changed": False,
        "publish_allowed": False,
        "validated_buy_count": 0,
        "active_gate_created": False,
        "research_fallback_used": False,
    }

    _write_json(out / "asof_positive_selection_score_summary.json", summary)
    _write_json(out / "score_contract.json", score_contract)
    _write_json(out / "feature_contract.json", feature_contract(source_contract, live_features, missing_features))
    _write_json(out / "model_probe_contract.json", model_probe_contract)
    _write_json(out / "score_bucket_metrics.json", metrics)
    _write_json(out / "score_stability_metrics.json", stability)
    _write_json(out / "offline_outcome_audit.json", offline_outcome_audit(scored))
    _write_json(out / "no_lookahead_audit.json", no_lookahead)
    _write_json(out / "source_coverage.json", {**source_coverage, "blocker_artifact": str(blocker_root), "blocker_decision": blocker_decision})
    _write_json(out / "research_decision.json", research_decision)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--v2-source-root", type=Path, default=DEFAULT_V2_SOURCE_ROOT)
    parser.add_argument("--v1-source-root", type=Path, default=DEFAULT_V1_SOURCE_ROOT)
    parser.add_argument("--blocker-root", type=Path, default=DEFAULT_BLOCKER_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.v2_source_root, args.v1_source_root, args.blocker_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
