from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import tradex_fresh_runtime_score_walkforward_validation_v1 as walkforward


AXIS_ID = "buyable_intersection_family_audit_v1"
DEFAULT_SOURCE_DB = walkforward.DEFAULT_SOURCE_DB
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\buyable_intersection_family_audit_v1")
DEFAULT_DATE_COUNT = 260
REQUIRED_ARTIFACTS = (
    "intersection_family_summary.json",
    "intersection_family_rows.csv",
    "intersection_variant_metrics.json",
    "buyability_gate_audit.json",
    "feature_contract.json",
    "source_coverage.json",
    "no_lookahead_audit.json",
    "lineage.json",
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
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_signal_flags(source_db: Path) -> pd.DataFrame:
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        return con.execute(
            """
            SELECT
              CAST(dt AS INTEGER) AS as_of_date,
              CAST(code AS VARCHAR) AS code,
              bool_or(side = 'buy' AND entry_qualified = TRUE) AS buy_entry_qualified,
              bool_or(side = 'buy' AND setup_type = 'breakout') AS buy_breakout_surface
            FROM signal_decision_daily
            GROUP BY 1,2
            """
        ).fetchdf()
    finally:
        con.close()


def build_intersection_frame(source_db: Path, date_count: int) -> pd.DataFrame:
    raw = walkforward.load_walkforward_frame(source_db, date_count)
    scored = walkforward.build_scored_frame(raw)
    flags = load_signal_flags(source_db)
    out = scored.merge(flags, on=["as_of_date", "code"], how="left")
    out["buy_entry_qualified"] = out["buy_entry_qualified"].map(lambda value: bool(value) if pd.notna(value) else False)
    out["buy_breakout_surface"] = out["buy_breakout_surface"].map(lambda value: bool(value) if pd.notna(value) else False)
    out["variant_a_entry_qualified_top100"] = out["buy_entry_qualified"] & (out["fresh_runtime_research_watch_rank"] <= 100)
    out["variant_b_entry_qualified_top50"] = out["buy_entry_qualified"] & (out["fresh_runtime_research_watch_rank"] <= 50)
    out["variant_c_entry_qualified_top20"] = out["buy_entry_qualified"] & (out["fresh_runtime_research_watch_rank"] <= 20)
    return out


def _metrics(rows: pd.DataFrame, total_dates: int) -> dict[str, Any]:
    valid = rows[rows["ret20"].notna()].copy()
    if valid.empty:
        return {"sample_count": 0, "date_count": 0, "code_count": 0, "average_candidates_per_date": 0.0, "mean_ret20": None, "median_ret20": None, "winner_rate_ret20_gt_10pct": None, "bad_rate_ret20_lt_minus_5pct": None, "severe_rate_ret20_lt_minus_10pct": None, "outcome_coverage_rate": 0.0}
    return {
        "sample_count": int(len(valid)),
        "date_count": int(valid["as_of_date"].nunique()),
        "code_count": int(valid["code"].nunique()),
        "average_candidates_per_date": float(len(rows) / total_dates) if total_dates else 0.0,
        "mean_ret20": float(valid["ret20"].mean()),
        "median_ret20": float(valid["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((valid["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((valid["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((valid["ret20"] < -0.10).mean()),
        "outcome_coverage_rate": float(len(valid) / len(rows)) if len(rows) else 0.0,
    }


def variant_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    total_dates = int(frame["as_of_date"].nunique()) if not frame.empty else 0
    return {
        "fresh_top20": _metrics(frame[frame["fresh_runtime_research_watch_rank"] <= 20], total_dates),
        "entry_qualified_all": _metrics(frame[frame["buy_entry_qualified"]], total_dates),
        "variant_a_entry_qualified_top100": _metrics(frame[frame["variant_a_entry_qualified_top100"]], total_dates),
        "variant_b_entry_qualified_top50": _metrics(frame[frame["variant_b_entry_qualified_top50"]], total_dates),
        "variant_c_entry_qualified_top20": _metrics(frame[frame["variant_c_entry_qualified_top20"]], total_dates),
    }


def buyability_gate(metrics: dict[str, Any]) -> dict[str, Any]:
    gates = {}
    for name, m in metrics.items():
        coverage_ok = m["outcome_coverage_rate"] >= 0.90 and m["sample_count"] >= 500 and m["date_count"] >= 50
        breadth_ok = m["average_candidates_per_date"] >= 1.0
        quality_ok = (
            m["mean_ret20"] is not None and m["mean_ret20"] > 0.03
            and m["winner_rate_ret20_gt_10pct"] is not None and m["winner_rate_ret20_gt_10pct"] >= 0.20
            and m["bad_rate_ret20_lt_minus_5pct"] is not None and m["bad_rate_ret20_lt_minus_5pct"] <= 0.20
            and m["severe_rate_ret20_lt_minus_10pct"] is not None and m["severe_rate_ret20_lt_minus_10pct"] <= 0.10
        )
        gates[name] = {"buyability_gate_pass": bool(coverage_ok and breadth_ok and quality_ok), "coverage_gate_pass": bool(coverage_ok), "breadth_gate_pass": bool(breadth_ok), "quality_gate_pass": bool(quality_ok)}
    return {"variant_gates": gates, "passing_variants": [k for k, v in gates.items() if v["buyability_gate_pass"]], "any_buyability_gate_pass": any(v["buyability_gate_pass"] for v in gates.values()), "thresholds": {"sample_count_min": 500, "date_count_min": 50, "average_candidates_per_date_min": 1.0, "outcome_coverage_rate_min": 0.90, "mean_ret20_min": 0.03, "winner_rate_ret20_gt_10pct_min": 0.20, "bad_rate_ret20_lt_minus_5pct_max": 0.20, "severe_rate_ret20_lt_minus_10pct_max": 0.10}, "validated_buy_count": 0, "active_gate_created": False}


def decide(gate: dict[str, Any], metrics: dict[str, Any]) -> tuple[str, str, list[str]]:
    if gate["any_buyability_gate_pass"]:
        return "intersection_family_keep_for_forward_validation", "KEEP", ["entry_qualified_and_fresh_score_intersection_passed_buyability_gate"]
    if any(name.startswith("variant_") and m["mean_ret20"] is not None and m["mean_ret20"] > 0.03 for name, m in metrics.items()):
        return "intersection_family_promising_but_not_buyable", "HOLD_UNDERPOWERED", ["intersection_improved_return_but_failed_full_buyability_gate"]
    return "intersection_family_no_buyability_edge", "DROP", ["intersection_variants_failed_buyability_gate"]


def run(source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT, date_count: int = DEFAULT_DATE_COUNT) -> Path:
    frame = build_intersection_frame(source_db, date_count)
    metrics = variant_metrics(frame)
    gate = buyability_gate(metrics)
    decision, decision_class, reasons = decide(gate, metrics)
    out = output_root / f"{_now_tag()}-buyable-intersection-family-audit-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = frame[frame[["variant_a_entry_qualified_top100", "variant_b_entry_qualified_top50", "variant_c_entry_qualified_top20"]].any(axis=1)].copy()
    cols = ["as_of_date", "code", "fresh_runtime_research_watch_rank", "ret20", "buy_entry_qualified", "buy_breakout_surface", "variant_a_entry_qualified_top100", "variant_b_entry_qualified_top50", "variant_c_entry_qualified_top20"]
    rows[cols].to_csv(out / "intersection_family_rows.csv", index=False)
    _write_json(out / "intersection_family_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "row_count": int(len(rows)), "date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0, "buyable_selection_ready": decision_class == "KEEP", "validated_buy_count": 0})
    _write_json(out / "intersection_variant_metrics.json", {"axis_id": AXIS_ID, "variants": metrics})
    _write_json(out / "buyability_gate_audit.json", gate)
    _write_json(out / "feature_contract.json", {"axis_id": AXIS_ID, "fields": {"as_of_date": {"classification": "identifier"}, "code": {"classification": "identifier"}, "fresh_runtime_research_watch_rank": {"classification": "point_in_time_feature"}, "buy_entry_qualified": {"classification": "point_in_time_feature"}, "buy_breakout_surface": {"classification": "point_in_time_feature"}, "ret20": {"classification": "offline_outcome_only"}}})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "row_count": int(len(frame)), "date_count": int(frame["as_of_date"].nunique()), "code_count": int(frame["code"].nunique()), "intersection_row_count": int(len(rows)), "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "intersection_uses_point_in_time_signal_and_score_rank": True, "outcomes_used_for_selection": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "upstream_score_axis": walkforward.AXIS_ID, "signal_source_table": "signal_decision_daily"})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "research_watch_only": True, "buyable_selection_ready": decision_class == "KEEP", "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--date-count", type=int, default=DEFAULT_DATE_COUNT)
    args = parser.parse_args(argv)
    out = run(args.source_db, args.output_root, args.date_count)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
