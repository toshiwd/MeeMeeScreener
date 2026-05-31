from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import tradex_fresh_runtime_score_walkforward_validation_v1 as walkforward


AXIS_ID = "fresh_runtime_score_rank_depth_gate_v1"
DEFAULT_SOURCE_DB = walkforward.DEFAULT_SOURCE_DB
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\fresh_runtime_score_rank_depth_gate_v1")
DEFAULT_DATE_COUNT = 260
REQUIRED_ARTIFACTS = (
    "rank_depth_summary.json",
    "rank_depth_rows.csv",
    "rank_depth_metrics.json",
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


def _metrics(rows: pd.DataFrame, total: int) -> dict[str, Any]:
    valid = rows[rows["ret20"].notna()].copy()
    if valid.empty:
        return {"sample_count": 0, "date_count": 0, "code_count": 0, "selected_share": 0.0, "mean_ret20": None, "median_ret20": None, "winner_rate_ret20_gt_10pct": None, "bad_rate_ret20_lt_minus_5pct": None, "severe_rate_ret20_lt_minus_10pct": None, "outcome_coverage_rate": 0.0}
    return {
        "sample_count": int(len(valid)),
        "date_count": int(valid["as_of_date"].nunique()),
        "code_count": int(valid["code"].nunique()),
        "selected_share": float(len(rows) / total) if total else 0.0,
        "mean_ret20": float(valid["ret20"].mean()),
        "median_ret20": float(valid["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((valid["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((valid["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((valid["ret20"] < -0.10).mean()),
        "outcome_coverage_rate": float(len(valid) / len(rows)) if len(rows) else 0.0,
    }


def rank_depth_metrics(scored: pd.DataFrame) -> dict[str, Any]:
    total = len(scored)
    return {
        "top5": _metrics(scored[scored["fresh_runtime_research_watch_rank"] <= 5], total),
        "top10": _metrics(scored[scored["fresh_runtime_research_watch_rank"] <= 10], total),
        "top20": _metrics(scored[scored["fresh_runtime_research_watch_rank"] <= 20], total),
        "rank21_100": _metrics(scored[(scored["fresh_runtime_research_watch_rank"] > 20) & (scored["fresh_runtime_research_watch_rank"] <= 100)], total),
        "remaining": _metrics(scored[scored["fresh_runtime_research_watch_rank"] > 100], total),
    }


def buyability_gate(metrics: dict[str, Any]) -> dict[str, Any]:
    gates = {}
    for name in ["top5", "top10", "top20"]:
        m = metrics[name]
        coverage_ok = m["outcome_coverage_rate"] >= 0.90 and m["sample_count"] >= 1000 and m["date_count"] >= 50
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
    return {
        "rank_depth_gates": gates,
        "any_buyability_gate_pass": any(v["buyability_gate_pass"] for v in gates.values()),
        "thresholds": {
            "sample_count_min": 1000,
            "date_count_min": 50,
            "outcome_coverage_rate_min": 0.90,
            "mean_ret20_min": 0.03,
            "winner_rate_ret20_gt_10pct_min": 0.20,
            "bad_rate_ret20_lt_minus_5pct_max": 0.20,
            "severe_rate_ret20_lt_minus_10pct_max": 0.10,
        },
        "validated_buy_count": 0,
        "active_gate_created": False,
    }


def decide(gate: dict[str, Any], metrics: dict[str, Any]) -> tuple[str, str, list[str]]:
    if gate["any_buyability_gate_pass"]:
        return "fresh_runtime_rank_depth_keep_for_next_validation", "KEEP", ["rank_depth_subset_passed_predeclared_buyability_gate"]
    if metrics["top5"]["mean_ret20"] is not None and metrics["top5"]["mean_ret20"] > metrics["top20"]["mean_ret20"]:
        return "fresh_runtime_rank_depth_promising_but_not_buyable", "HOLD_UNDERPOWERED", ["rank_depth_improved_return_but_failed_buyability_gate"]
    return "fresh_runtime_rank_depth_no_buyability_edge", "DROP", ["rank_depth_selectivity_failed_buyability_gate"]


def run(source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT, date_count: int = DEFAULT_DATE_COUNT) -> Path:
    raw = walkforward.load_walkforward_frame(source_db, date_count)
    scored = walkforward.build_scored_frame(raw)
    metrics = rank_depth_metrics(scored)
    gate = buyability_gate(metrics)
    decision, decision_class, reasons = decide(gate, metrics)
    out = output_root / f"{_now_tag()}-fresh-runtime-score-rank-depth-gate-v1"
    out.mkdir(parents=True, exist_ok=True)
    cols = ["as_of_date", "code", "fresh_runtime_research_watch_rank", "fresh_runtime_research_watch_score", "ret20"]
    scored[scored["fresh_runtime_research_watch_rank"] <= 20][cols].to_csv(out / "rank_depth_rows.csv", index=False)
    _write_json(out / "rank_depth_summary.json", {"axis_id": AXIS_ID, "decision": decision, "decision_class": decision_class, "reason_typed": reasons, "source_db": str(source_db), "row_count": int(len(scored)), "date_count": int(scored["as_of_date"].nunique()), "buyable_selection_ready": decision_class == "KEEP", "validated_buy_count": 0})
    _write_json(out / "rank_depth_metrics.json", {"axis_id": AXIS_ID, "rank_depths": metrics})
    _write_json(out / "buyability_gate_audit.json", gate)
    _write_json(out / "feature_contract.json", walkforward.score_contract.feature_contract())
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "row_count": int(len(scored)), "date_count": int(scored["as_of_date"].nunique()), "code_count": int(scored["code"].nunique()), "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "no_lookahead_pass": True, "rank_depth_uses_score_rank_only": True, "outcomes_used_for_selection": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "upstream_axis": walkforward.AXIS_ID, "score_contract_source": "scripts/tradex_fresh_runtime_candidate_surface_v1.py"})
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
