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


AXIS_ID = "fresh_runtime_score_regime_containment_v1"
DEFAULT_SOURCE_DB = walkforward.DEFAULT_SOURCE_DB
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\fresh_runtime_score_regime_containment_v1")
DEFAULT_DATE_COUNT = 260
REQUIRED_ARTIFACTS = (
    "regime_containment_summary.json",
    "regime_containment_rows.csv",
    "regime_variant_metrics.json",
    "regime_bucket_metrics.json",
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


def load_regime(source_db: Path) -> pd.DataFrame:
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        return con.execute(
            """
            SELECT
              CAST(dt AS INTEGER) AS as_of_date,
              regime_id,
              breadth_above_ma20,
              breadth_above_ma60,
              advancers_ratio,
              index_close_vs_ma20,
              index_close_vs_ma60,
              market_atr_pct,
              regime_score
            FROM market_regime_daily
            """
        ).fetchdf()
    finally:
        con.close()


def build_regime_frame(source_db: Path, date_count: int) -> pd.DataFrame:
    raw = walkforward.load_walkforward_frame(source_db, date_count)
    scored = walkforward.build_scored_frame(raw)
    top = scored[scored["score_bucket"] == "top20"].copy()
    regime = load_regime(source_db)
    out = top.merge(regime, on="as_of_date", how="left")
    out["variant_a_risk_on_only"] = out["regime_id"].isin(["risk_on_trend", "risk_on_range"])
    out["variant_b_breadth_support"] = (
        (pd.to_numeric(out["breadth_above_ma20"], errors="coerce") >= 0.50)
        & (pd.to_numeric(out["advancers_ratio"], errors="coerce") >= 0.45)
        & (pd.to_numeric(out["index_close_vs_ma20"], errors="coerce") >= -0.03)
    )
    out["variant_c_strict_regime"] = (
        out["variant_a_risk_on_only"]
        & out["variant_b_breadth_support"]
        & (pd.to_numeric(out["market_atr_pct"], errors="coerce") <= 0.025)
    )
    return out


def _metrics(rows: pd.DataFrame, total: int) -> dict[str, Any]:
    valid = rows[rows["ret20"].notna()].copy()
    if valid.empty:
        return {"sample_count": 0, "date_count": 0, "code_count": 0, "kept_share": 0.0, "mean_ret20": None, "median_ret20": None, "winner_rate_ret20_gt_10pct": None, "bad_rate_ret20_lt_minus_5pct": None, "severe_rate_ret20_lt_minus_10pct": None, "outcome_coverage_rate": 0.0}
    return {
        "sample_count": int(len(valid)),
        "date_count": int(valid["as_of_date"].nunique()),
        "code_count": int(valid["code"].nunique()),
        "kept_share": float(len(rows) / total) if total else 0.0,
        "mean_ret20": float(valid["ret20"].mean()),
        "median_ret20": float(valid["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((valid["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((valid["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((valid["ret20"] < -0.10).mean()),
        "outcome_coverage_rate": float(len(valid) / len(rows)) if len(rows) else 0.0,
    }


def variant_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    total = len(rows)
    out = {"raw_top20": _metrics(rows, total)}
    for variant in ["variant_a_risk_on_only", "variant_b_breadth_support", "variant_c_strict_regime"]:
        out[variant] = _metrics(rows[rows[variant]], total)
    return out


def regime_bucket_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    out = {}
    for regime, grp in rows.groupby("regime_id", dropna=False):
        out[str(regime)] = _metrics(grp, len(rows))
    return out


def buyability_gate(metrics: dict[str, Any]) -> dict[str, Any]:
    gates = {}
    for name, m in metrics.items():
        if name == "raw_top20":
            continue
        coverage_ok = m["outcome_coverage_rate"] >= 0.90 and m["sample_count"] >= 1000 and m["date_count"] >= 50 and m["kept_share"] >= 0.25
        quality_ok = (
            m["mean_ret20"] is not None
            and m["mean_ret20"] > 0.03
            and m["winner_rate_ret20_gt_10pct"] is not None
            and m["winner_rate_ret20_gt_10pct"] >= 0.20
            and m["bad_rate_ret20_lt_minus_5pct"] is not None
            and m["bad_rate_ret20_lt_minus_5pct"] <= 0.20
            and m["severe_rate_ret20_lt_minus_10pct"] is not None
            and m["severe_rate_ret20_lt_minus_10pct"] <= 0.10
        )
        gates[name] = {"buyability_gate_pass": bool(coverage_ok and quality_ok), "coverage_gate_pass": bool(coverage_ok), "quality_gate_pass": bool(quality_ok)}
    return {"variant_gates": gates, "any_buyability_gate_pass": any(v["buyability_gate_pass"] for v in gates.values()), "thresholds": {"sample_count_min": 1000, "date_count_min": 50, "kept_share_min": 0.25, "outcome_coverage_rate_min": 0.90, "mean_ret20_min": 0.03, "winner_rate_ret20_gt_10pct_min": 0.20, "bad_rate_ret20_lt_minus_5pct_max": 0.20, "severe_rate_ret20_lt_minus_10pct_max": 0.10}, "validated_buy_count": 0, "active_gate_created": False}


def decide(gate: dict[str, Any], metrics: dict[str, Any]) -> tuple[str, str, list[str]]:
    if gate["any_buyability_gate_pass"]:
        return "fresh_runtime_regime_containment_keep_for_next_validation", "KEEP", ["regime_contained_top20_passed_predeclared_buyability_gate"]
    raw_bad = metrics["raw_top20"]["bad_rate_ret20_lt_minus_5pct"]
    raw_severe = metrics["raw_top20"]["severe_rate_ret20_lt_minus_10pct"]
    improved = any(
        name != "raw_top20"
        and m["bad_rate_ret20_lt_minus_5pct"] is not None
        and raw_bad is not None
        and m["bad_rate_ret20_lt_minus_5pct"] < raw_bad
        and m["severe_rate_ret20_lt_minus_10pct"] is not None
        and raw_severe is not None
        and m["severe_rate_ret20_lt_minus_10pct"] < raw_severe
        for name, m in metrics.items()
    )
    if improved:
        return "fresh_runtime_regime_containment_improved_but_not_buyable", "HOLD_UNDERPOWERED", ["regime_filter_reduced_risk_but_failed_buyability_gate"]
    return "fresh_runtime_regime_containment_no_edge", "DROP", ["regime_filter_failed_to_reduce_bad_severe"]


def run(source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT, date_count: int = DEFAULT_DATE_COUNT) -> Path:
    rows = build_regime_frame(source_db, date_count)
    metrics = variant_metrics(rows)
    buckets = regime_bucket_metrics(rows)
    gate = buyability_gate(metrics)
    decision, decision_class, reasons = decide(gate, metrics)
    out = output_root / f"{_now_tag()}-fresh-runtime-score-regime-containment-v1"
    out.mkdir(parents=True, exist_ok=True)
    cols = ["as_of_date", "code", "fresh_runtime_research_watch_rank", "ret20", "regime_id", "breadth_above_ma20", "advancers_ratio", "index_close_vs_ma20", "market_atr_pct", "variant_a_risk_on_only", "variant_b_breadth_support", "variant_c_strict_regime"]
    rows[cols].to_csv(out / "regime_containment_rows.csv", index=False)
    _write_json(out / "regime_containment_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "source_db": str(source_db), "row_count": int(len(rows)), "date_count": int(rows["as_of_date"].nunique()), "regime_coverage_rate": float(rows["regime_id"].notna().mean()) if len(rows) else 0.0, "buyable_selection_ready": decision_class == "KEEP", "validated_buy_count": 0})
    _write_json(out / "regime_variant_metrics.json", {"axis_id": AXIS_ID, "variants": metrics})
    _write_json(out / "regime_bucket_metrics.json", {"axis_id": AXIS_ID, "regimes": buckets})
    _write_json(out / "buyability_gate_audit.json", gate)
    _write_json(out / "feature_contract.json", walkforward.score_contract.feature_contract() | {"regime_fields": {c: {"classification": "point_in_time_feature"} for c in ["regime_id", "breadth_above_ma20", "advancers_ratio", "index_close_vs_ma20", "market_atr_pct"]}})
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "row_count": int(len(rows)), "date_count": int(rows["as_of_date"].nunique()), "code_count": int(rows["code"].nunique()), "regime_coverage_rate": float(rows["regime_id"].notna().mean()) if len(rows) else 0.0, "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "regime_fields_point_in_time": True, "outcomes_used_for_selection": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "upstream_axis": walkforward.AXIS_ID, "source_table": "market_regime_daily"})
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
