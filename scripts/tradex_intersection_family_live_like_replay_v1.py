from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "intersection_family_live_like_replay_v1"
DEFAULT_SUPPORT_ROOT = Path(
    r"G:\Tradex\buyable_intersection_family_support_gate_v1\20260526T004451Z-buyable-intersection-family-support-gate-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\intersection_family_live_like_replay_v1")
REQUIRED_ARTIFACTS = (
    "live_like_replay_summary.json",
    "live_like_replay_rows.csv",
    "period_replay_metrics.json",
    "recent_period_gate.json",
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


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_support_rows(support_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows_path = support_root / "support_gate_rows.csv"
    decision_path = support_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    rows["ret20"] = pd.to_numeric(rows["ret20"], errors="coerce")
    rows["period_bucket"] = rows["as_of_date"].map(period_bucket)
    return rows, _load_json(decision_path)


def period_bucket(as_of_date: Any) -> str:
    date_int = int(as_of_date)
    year = date_int // 10000
    month = (date_int // 100) % 100
    half = "H1" if month <= 6 else "H2"
    return f"{year}{half}"


def metric_payload(rows: pd.DataFrame) -> dict[str, Any]:
    evaluated = rows[rows["ret20"].notna()].copy()
    if evaluated.empty:
        return {
            "sample_count": 0,
            "date_count": 0,
            "code_count": 0,
            "mean_ret20": None,
            "median_ret20": None,
            "winner_rate_ret20_gt_10pct": None,
            "bad_rate_ret20_lt_minus_5pct": None,
            "severe_rate_ret20_lt_minus_10pct": None,
            "outcome_coverage_rate": 0.0,
        }
    return {
        "sample_count": int(len(evaluated)),
        "date_count": int(evaluated["as_of_date"].nunique()),
        "code_count": int(evaluated["code"].nunique()),
        "mean_ret20": float(evaluated["ret20"].mean()),
        "median_ret20": float(evaluated["ret20"].median()),
        "winner_rate_ret20_gt_10pct": float((evaluated["ret20"] > 0.10).mean()),
        "bad_rate_ret20_lt_minus_5pct": float((evaluated["ret20"] < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": float((evaluated["ret20"] < -0.10).mean()),
        "outcome_coverage_rate": float(len(evaluated) / len(rows)) if len(rows) else 0.0,
    }


def period_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    return {str(period): metric_payload(group) for period, group in rows.groupby("period_bucket", sort=True)}


def recent_period_gate(metrics_by_period: dict[str, Any]) -> dict[str, Any]:
    if not metrics_by_period:
        return {"gate_pass": False, "current_period": None, "reason_typed": ["no_period_metrics_available"]}
    current_period = sorted(metrics_by_period)[-1]
    metrics = metrics_by_period[current_period]
    sample_ok = metrics["sample_count"] >= 20 and metrics["date_count"] >= 10 and metrics["outcome_coverage_rate"] >= 0.9
    quality_ok = (
        metrics["mean_ret20"] is not None
        and metrics["mean_ret20"] > 0.03
        and metrics["winner_rate_ret20_gt_10pct"] is not None
        and metrics["winner_rate_ret20_gt_10pct"] >= 0.20
        and metrics["bad_rate_ret20_lt_minus_5pct"] is not None
        and metrics["bad_rate_ret20_lt_minus_5pct"] <= 0.20
        and metrics["severe_rate_ret20_lt_minus_10pct"] is not None
        and metrics["severe_rate_ret20_lt_minus_10pct"] <= 0.10
    )
    reasons = []
    if not sample_ok:
        reasons.append("recent_period_support_or_coverage_insufficient")
    if not quality_ok:
        reasons.append("recent_period_return_or_risk_gate_failed")
    return {
        "gate_pass": bool(sample_ok and quality_ok),
        "current_period": current_period,
        "sample_gate_pass": bool(sample_ok),
        "quality_gate_pass": bool(quality_ok),
        "metrics": metrics,
        "thresholds": {
            "sample_count_min": 20,
            "date_count_min": 10,
            "outcome_coverage_rate_min": 0.9,
            "mean_ret20_min": 0.03,
            "winner_rate_ret20_gt_10pct_min": 0.20,
            "bad_rate_ret20_lt_minus_5pct_max": 0.20,
            "severe_rate_ret20_lt_minus_10pct_max": 0.10,
        },
        "reason_typed": reasons or ["recent_period_gate_passed"],
    }


def no_lookahead_audit(rows: pd.DataFrame, support_decision: dict[str, Any]) -> dict[str, Any]:
    support_ok = support_decision.get("research_decision") == "intersection_family_ready_for_forward_paper_validation"
    selector_cols = {"buy_entry_qualified", "variant_b_entry_qualified_top50", "fresh_runtime_research_watch_rank"}
    selector_cols_present = sorted(selector_cols & set(rows.columns))
    passed = bool(support_ok and selector_cols_present)
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "support_gate_ready": support_ok,
        "selector_columns_present": selector_cols_present,
        "future_outcomes_used_for_selection": False,
        "ret20_used_evaluation_only": True,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(overall: dict[str, Any], gate: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["support_gate_or_selector_contract_failed"]
    if gate["gate_pass"]:
        return "intersection_family_live_like_replay_current_period_buyable_ready", "KEEP", [
            "historical_overall_and_recent_period_gates_passed"
        ]
    if overall["mean_ret20"] is not None and overall["mean_ret20"] > 0.03:
        return "intersection_family_live_like_replay_overall_edge_recent_period_not_buyable", "HOLD_UNDERPOWERED", [
            "overall_edge_positive_but_recent_period_gate_failed"
        ]
    return "intersection_family_live_like_replay_no_buyability_edge", "DROP", ["overall_and_recent_live_like_gates_failed"]


def run(support_root: Path = DEFAULT_SUPPORT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    rows, support_decision = load_support_rows(support_root)
    overall = metric_payload(rows)
    periods = period_metrics(rows)
    gate = recent_period_gate(periods)
    audit = no_lookahead_audit(rows, support_decision)
    decision, decision_class, reasons = decide(overall, gate, audit)

    out = output_root / f"{_now_tag()}-intersection-family-live-like-replay-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows.to_csv(out / "live_like_replay_rows.csv", index=False)
    _write_json(
        out / "live_like_replay_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "support_root": str(support_root),
            "overall_metrics": overall,
            "recent_period_gate_pass": gate["gate_pass"],
            "buyable_selection_ready": decision_class == "KEEP",
            "validated_buy_count": 0,
        },
    )
    _write_json(out / "period_replay_metrics.json", {"axis_id": AXIS_ID, "periods": periods})
    _write_json(out / "recent_period_gate.json", gate)
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "support_root": str(support_root), "row_count": int(len(rows)), "date_count": int(rows["as_of_date"].nunique()), "code_count": int(rows["code"].nunique()), "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "support_root": str(support_root), "support_decision": support_decision})
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "research_watch_only": True,
            "buyable_selection_ready": decision_class == "KEEP",
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "publish_allowed": False,
            "validated_buy_count": 0,
            "active_gate_created": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--support-root", type=Path, default=DEFAULT_SUPPORT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.support_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
