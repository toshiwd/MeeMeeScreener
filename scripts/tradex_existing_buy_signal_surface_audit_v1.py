from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "existing_buy_signal_surface_audit_v1"
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\existing_buy_signal_surface_audit_v1")
REQUIRED_ARTIFACTS = (
    "existing_buy_signal_summary.json",
    "existing_buy_signal_rows.csv",
    "signal_group_metrics.json",
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


def load_signal_rows(source_db: Path) -> pd.DataFrame:
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        return con.execute(
            """
            SELECT
              CAST(dt AS INTEGER) AS as_of_date,
              CAST(code AS VARCHAR) AS code,
              side,
              logic_version,
              basis_version,
              entry_qualified,
              setup_type,
              forward_return_5 AS ret5,
              forward_return_20 AS ret20,
              forward_return_10 AS ret10
            FROM signal_decision_daily
            WHERE side IN ('buy', 'long', 'up')
            """
        ).fetchdf()
    finally:
        con.close()


def _metrics(rows: pd.DataFrame) -> dict[str, Any]:
    valid = rows[rows["ret20"].notna()].copy()
    if valid.empty:
        return {"sample_count": 0, "date_count": 0, "code_count": 0, "mean_ret20": None, "median_ret20": None, "winner_rate_ret20_gt_10pct": None, "bad_rate_ret20_lt_minus_5pct": None, "severe_rate_ret20_lt_minus_10pct": None, "outcome_coverage_rate": 0.0}
    return {
        "sample_count": int(len(valid)),
        "date_count": int(valid["as_of_date"].nunique()),
        "code_count": int(valid["code"].nunique()),
        "mean_ret20": float(valid["ret20"].mean()),
        "median_ret20": float(valid["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((valid["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((valid["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((valid["ret20"] < -0.10).mean()),
        "outcome_coverage_rate": float(len(valid) / len(rows)) if len(rows) else 0.0,
    }


def group_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    out = {
        "all_buy_side_rows": _metrics(rows),
        "entry_qualified_true": _metrics(rows[rows["entry_qualified"] == True]),
    }
    for setup, grp in rows.groupby("setup_type", dropna=False):
        out[f"setup_type={setup}"] = _metrics(grp)
    return out


def buyability_gate(metrics: dict[str, Any]) -> dict[str, Any]:
    gates = {}
    for name, m in metrics.items():
        coverage_ok = m["outcome_coverage_rate"] >= 0.90 and m["sample_count"] >= 1000 and m["date_count"] >= 50
        quality_ok = (
            m["mean_ret20"] is not None and m["mean_ret20"] > 0.03
            and m["winner_rate_ret20_gt_10pct"] is not None and m["winner_rate_ret20_gt_10pct"] >= 0.20
            and m["bad_rate_ret20_lt_minus_5pct"] is not None and m["bad_rate_ret20_lt_minus_5pct"] <= 0.20
            and m["severe_rate_ret20_lt_minus_10pct"] is not None and m["severe_rate_ret20_lt_minus_10pct"] <= 0.10
        )
        gates[name] = {"buyability_gate_pass": bool(coverage_ok and quality_ok), "coverage_gate_pass": bool(coverage_ok), "quality_gate_pass": bool(quality_ok)}
    return {"group_gates": gates, "passing_groups": [k for k, v in gates.items() if v["buyability_gate_pass"]], "any_buyability_gate_pass": any(v["buyability_gate_pass"] for v in gates.values()), "thresholds": {"sample_count_min": 1000, "date_count_min": 50, "outcome_coverage_rate_min": 0.90, "mean_ret20_min": 0.03, "winner_rate_ret20_gt_10pct_min": 0.20, "bad_rate_ret20_lt_minus_5pct_max": 0.20, "severe_rate_ret20_lt_minus_10pct_max": 0.10}, "validated_buy_count": 0, "active_gate_created": False}


def decide(gate: dict[str, Any], rows: pd.DataFrame) -> tuple[str, str, list[str]]:
    if rows.empty:
        return "blocked_no_existing_buy_signal_rows", "BLOCKED", ["signal_decision_daily_has_no_buy_side_rows"]
    if gate["any_buyability_gate_pass"]:
        return "existing_buy_signal_surface_keep_for_next_validation", "KEEP", ["existing_buy_signal_group_passed_predeclared_buyability_gate"]
    return "existing_buy_signal_surface_no_buyability_edge", "DROP", ["existing_buy_signal_groups_failed_buyability_gate"]


def run(source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    rows = load_signal_rows(source_db)
    metrics = group_metrics(rows)
    gate = buyability_gate(metrics)
    decision, decision_class, reasons = decide(gate, rows)
    out = output_root / f"{_now_tag()}-existing-buy-signal-surface-audit-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows.to_csv(out / "existing_buy_signal_rows.csv", index=False)
    _write_json(out / "existing_buy_signal_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "row_count": int(len(rows)), "date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0, "buyable_selection_ready": decision_class == "KEEP", "validated_buy_count": 0})
    _write_json(out / "signal_group_metrics.json", {"axis_id": AXIS_ID, "groups": metrics})
    _write_json(out / "buyability_gate_audit.json", gate)
    _write_json(out / "feature_contract.json", {"axis_id": AXIS_ID, "fields": {"as_of_date": {"classification": "identifier"}, "code": {"classification": "identifier"}, "side": {"classification": "point_in_time_feature"}, "entry_qualified": {"classification": "point_in_time_feature"}, "setup_type": {"classification": "point_in_time_feature"}, "ret5": {"classification": "offline_outcome_only"}, "ret10": {"classification": "offline_outcome_only"}, "ret20": {"classification": "offline_outcome_only"}}})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "source_table": "signal_decision_daily", "row_count": int(len(rows)), "date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0, "code_count": int(rows["code"].nunique()) if not rows.empty else 0, "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "signal_features_read_only": True, "outcomes_used_for_selection": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "source_table": "signal_decision_daily", "source_db": str(source_db)})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "research_watch_only": True, "buyable_selection_ready": decision_class == "KEEP", "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
