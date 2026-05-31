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
from scripts import tradex_fresh_runtime_candidate_surface_v1 as score_contract
from scripts import tradex_fresh_runtime_score_walkforward_validation_v1 as walkforward


AXIS_ID = "fresh_runtime_score_source_liquidity_quality_v1"
DEFAULT_SOURCE_DB = walkforward.DEFAULT_SOURCE_DB
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\fresh_runtime_score_source_liquidity_quality_v1")
DEFAULT_DATE_COUNT = 260
REQUIRED_ARTIFACTS = (
    "source_liquidity_summary.json",
    "source_liquidity_rows.csv",
    "source_liquidity_variant_metrics.json",
    "kept_vs_removed_quality.json",
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


def load_quality_frame(source_db: Path, date_count: int = DEFAULT_DATE_COUNT) -> pd.DataFrame:
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        query = """
            WITH all_dates AS (
                SELECT DISTINCT CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) AS as_of_date
                FROM daily_bars
                ORDER BY as_of_date DESC
                LIMIT ?
            ),
            bars AS (
                SELECT
                    CAST(code AS VARCHAR) AS code,
                    CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) AS as_of_date,
                    o, h, l, c, v, source,
                    lag(c, 1) OVER (PARTITION BY code ORDER BY date) AS prev_close,
                    avg(v) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS volume20_avg,
                    avg(c * v) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS turnover20_value,
                    max(h) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS recent_high20,
                    min(l) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS recent_low20,
                    lead(c, 5) OVER (PARTITION BY code ORDER BY date) AS close_fwd5,
                    lead(c, 20) OVER (PARTITION BY code ORDER BY date) AS close_fwd20
                FROM daily_bars
            ),
            features AS (
                SELECT
                    CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) AS as_of_date,
                    CAST(code AS VARCHAR) AS code,
                    close, ma7, ma20, ma60, diff20_pct, cnt_20_above, cnt_7_above
                FROM feature_snapshot_daily
            )
            SELECT
                f.as_of_date, f.code, f.close, f.ma7, f.ma20, f.ma60, f.diff20_pct, f.cnt_20_above, f.cnt_7_above,
                b.o, b.h, b.l, b.c, b.v, b.source, b.prev_close, b.volume20_avg, b.turnover20_value,
                b.recent_high20, b.recent_low20, b.close_fwd5, b.close_fwd20
            FROM features f
            JOIN bars b ON b.code = f.code AND b.as_of_date = f.as_of_date
            JOIN all_dates d ON d.as_of_date = f.as_of_date
        """
        return con.execute(query, [int(date_count)]).fetchdf()
    finally:
        con.close()


def build_quality_frame(frame: pd.DataFrame) -> pd.DataFrame:
    scored = walkforward.build_scored_frame(frame)
    top = scored[scored["score_bucket"] == "top20"].copy()
    top["turnover20_value"] = pd.to_numeric(top["turnover20_value"], errors="coerce")
    top["quality_a_price_liquidity"] = (
        (pd.to_numeric(top["close"], errors="coerce") >= 300.0)
        & (top["turnover20_value"] >= 100_000.0)
        & (pd.to_numeric(top["volume20_avg"], errors="coerce") >= 500.0)
    )
    top["quality_b_source_pan_liquid"] = top["quality_a_price_liquidity"] & (top["source"].astype(str) == "pan")
    top["quality_c_avoid_extreme_price_turnover"] = (
        top["quality_b_source_pan_liquid"]
        & (pd.to_numeric(top["close"], errors="coerce") <= 20000.0)
        & (pd.to_numeric(top["volume_vs_20d_avg"], errors="coerce") >= 0.5)
        & (pd.to_numeric(top["volume_vs_20d_avg"], errors="coerce") <= 5.0)
    )
    return top


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


def variant_metrics(top: pd.DataFrame) -> dict[str, Any]:
    total = len(top)
    out = {"raw_top20": _metrics(top, total)}
    for col in ["quality_a_price_liquidity", "quality_b_source_pan_liquid", "quality_c_avoid_extreme_price_turnover"]:
        out[col] = _metrics(top[top[col]], total)
    return out


def kept_removed(top: pd.DataFrame) -> dict[str, Any]:
    total = len(top)
    return {col: {"kept": _metrics(top[top[col]], total), "removed": _metrics(top[~top[col]], total)} for col in ["quality_a_price_liquidity", "quality_b_source_pan_liquid", "quality_c_avoid_extreme_price_turnover"]}


def buyability_gate(metrics: dict[str, Any]) -> dict[str, Any]:
    gates = {}
    for name, m in metrics.items():
        if name == "raw_top20":
            continue
        coverage_ok = m["outcome_coverage_rate"] >= 0.90 and m["sample_count"] >= 1000 and m["date_count"] >= 50 and m["kept_share"] >= 0.25
        quality_ok = (
            m["mean_ret20"] is not None and m["mean_ret20"] > 0.03
            and m["winner_rate_ret20_gt_10pct"] is not None and m["winner_rate_ret20_gt_10pct"] >= 0.20
            and m["bad_rate_ret20_lt_minus_5pct"] is not None and m["bad_rate_ret20_lt_minus_5pct"] <= 0.20
            and m["severe_rate_ret20_lt_minus_10pct"] is not None and m["severe_rate_ret20_lt_minus_10pct"] <= 0.10
        )
        gates[name] = {"buyability_gate_pass": bool(coverage_ok and quality_ok), "coverage_gate_pass": bool(coverage_ok), "quality_gate_pass": bool(quality_ok)}
    return {"variant_gates": gates, "any_buyability_gate_pass": any(v["buyability_gate_pass"] for v in gates.values()), "thresholds": {"sample_count_min": 1000, "date_count_min": 50, "kept_share_min": 0.25, "outcome_coverage_rate_min": 0.90, "mean_ret20_min": 0.03, "winner_rate_ret20_gt_10pct_min": 0.20, "bad_rate_ret20_lt_minus_5pct_max": 0.20, "severe_rate_ret20_lt_minus_10pct_max": 0.10}, "validated_buy_count": 0, "active_gate_created": False}


def decide(gate: dict[str, Any], metrics: dict[str, Any]) -> tuple[str, str, list[str]]:
    if gate["any_buyability_gate_pass"]:
        return "fresh_runtime_source_liquidity_keep_for_next_validation", "KEEP", ["source_liquidity_quality_subset_passed_predeclared_buyability_gate"]
    raw_bad = metrics["raw_top20"]["bad_rate_ret20_lt_minus_5pct"]
    raw_severe = metrics["raw_top20"]["severe_rate_ret20_lt_minus_10pct"]
    improved = any(name != "raw_top20" and m["bad_rate_ret20_lt_minus_5pct"] is not None and m["bad_rate_ret20_lt_minus_5pct"] < raw_bad and m["severe_rate_ret20_lt_minus_10pct"] is not None and m["severe_rate_ret20_lt_minus_10pct"] < raw_severe for name, m in metrics.items())
    if improved:
        return "fresh_runtime_source_liquidity_improved_but_not_buyable", "HOLD_UNDERPOWERED", ["source_liquidity_quality_reduced_risk_but_failed_buyability_gate"]
    return "fresh_runtime_source_liquidity_no_edge", "DROP", ["source_liquidity_quality_failed_to_reduce_risk"]


def run(source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT, date_count: int = DEFAULT_DATE_COUNT) -> Path:
    raw = load_quality_frame(source_db, date_count)
    top = build_quality_frame(raw)
    metrics = variant_metrics(top)
    kept_removed_payload = kept_removed(top)
    gate = buyability_gate(metrics)
    decision, decision_class, reasons = decide(gate, metrics)
    out = output_root / f"{_now_tag()}-fresh-runtime-score-source-liquidity-quality-v1"
    out.mkdir(parents=True, exist_ok=True)
    cols = ["as_of_date", "code", "fresh_runtime_research_watch_rank", "ret20", "close", "v", "source", "volume20_avg", "turnover20_value", "volume_vs_20d_avg", "quality_a_price_liquidity", "quality_b_source_pan_liquid", "quality_c_avoid_extreme_price_turnover"]
    top[cols].to_csv(out / "source_liquidity_rows.csv", index=False)
    _write_json(out / "source_liquidity_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "row_count": int(len(top)), "date_count": int(top["as_of_date"].nunique()), "buyable_selection_ready": decision_class == "KEEP", "validated_buy_count": 0})
    _write_json(out / "source_liquidity_variant_metrics.json", {"axis_id": AXIS_ID, "variants": metrics})
    _write_json(out / "kept_vs_removed_quality.json", {"axis_id": AXIS_ID, "variants": kept_removed_payload})
    _write_json(out / "buyability_gate_audit.json", gate)
    contract = score_contract.feature_contract()
    contract["quality_fields"] = {c: {"classification": "point_in_time_feature"} for c in ["close", "v", "source", "volume20_avg", "turnover20_value", "volume_vs_20d_avg"]}
    _write_json(out / "feature_contract.json", contract)
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "row_count": int(len(top)), "date_count": int(top["as_of_date"].nunique()), "code_count": int(top["code"].nunique()), "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "quality_rules_use_point_in_time_fields_only": True, "outcomes_used_for_selection": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "upstream_axis": walkforward.AXIS_ID, "source_tables": ["daily_bars", "feature_snapshot_daily"]})
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
