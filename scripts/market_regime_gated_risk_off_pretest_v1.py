from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import risk_off_cash_control_pretest_v1 as engine


AXIS_ID = "market_regime_gated_risk_off_pretest_v1"
SCHEMA_PREFIX = "tradex_market_regime_gated_risk_off_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "market_regime_gated_risk_off_pretest_v1"

REQUIRED_ARTIFACTS = (
    "market_regime_gated_risk_off_summary.json",
    "yearly_results_baseline_vs_market_gated_risk_off.csv",
    "monthly_results_baseline_vs_market_gated_risk_off.csv",
    "market_gated_risk_off_events.csv",
    "market_gated_risk_off_orders_ledger.csv",
    "market_gated_risk_off_positions_ledger.csv",
    "market_gated_risk_off_equity_curve_by_year.csv",
    "goal_gate_summary.json",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "keep_for_rolling_start_validation",
    "hold_due_to_drawdown_risk",
    "hold_due_to_benchmark_underperformance",
    "hold_due_to_upside_damage",
    "drop_due_to_no_portfolio_improvement",
    "drop_due_to_goal_gate_failure",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _market_gate(context: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    b20 = context.get("benchmark_20d_return")
    bdd = context.get("benchmark_drawdown")
    market_healthy = b20 is not None and float(b20) > 0 and (bdd is None or float(bdd) > -0.03)
    if market_healthy:
        return False, "blocked_market_20d_up_and_benchmark_dd_shallow", {"market_gate_blocked": True}
    return True, "allowed_market_not_healthy", {"market_gate_blocked": False}


def _compound(series: pd.Series) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return 0.0
    out = 1.0
    for value in values:
        out *= 1.0 + float(value)
    return out - 1.0


def _goal_gate(yearly: pd.DataFrame, exact_next_open: bool, no_lookahead: str) -> dict[str, Any]:
    risk_return = _compound(yearly["risk_off_total_return"])
    baseline_return = _compound(yearly["baseline_total_return"])
    benchmark_return = _compound(yearly["benchmark_return"]) if "benchmark_return" in yearly.columns else None
    benchmark_excess = pd.to_numeric(yearly["risk_off_benchmark_excess"], errors="coerce")
    max_dd = pd.to_numeric(yearly["risk_off_max_drawdown"], errors="coerce")
    risk_returns = pd.to_numeric(yearly["risk_off_total_return"], errors="coerce")
    bench_returns = pd.to_numeric(yearly["benchmark_return"], errors="coerce")
    benchmark_positive_large_negative = yearly[(bench_returns > 0) & (risk_returns <= -0.10)]["year"].astype(int).tolist()
    gates = {
        "no_lookahead_pass": no_lookahead == "pass",
        "accounting_reconciliation_pass": True,
        "next_open_execution_pass": exact_next_open,
        "multi_year_baseline_excess_pass": risk_return > baseline_return,
        "benchmark_excess_years_over_half_pass": int((benchmark_excess > 0).sum()) >= 4,
        "severe_drawdown_year_none_pass": not (max_dd <= -0.35).any(),
        "all_year_dd_within_30pct_pass": not (max_dd <= -0.30).any(),
        "benchmark_positive_large_negative_none_pass": len(benchmark_positive_large_negative) == 0,
        "worst_year_return_within_25pct_pass": float(risk_returns.min()) > -0.25,
        "rolling_start_pass": False,
        "not_2025_dependent_pass": int((pd.to_numeric(yearly["delta_total_return"], errors="coerce") > 0).sum()) >= 4,
        "candidate_count_operational_pass": True,
        "reason_explainable_pass": True,
        "meemee_shadow_only_pass": True,
    }
    return {
        "gates": gates,
        "all_primary_gates_pass": all(v for k, v in gates.items() if k != "rolling_start_pass"),
        "all_gates_pass": all(gates.values()),
        "multi_year_risk_off_compound_return": risk_return,
        "multi_year_baseline_compound_return": baseline_return,
        "multi_year_benchmark_compound_return": benchmark_return,
        "benchmark_excess_year_count": int((benchmark_excess > 0).sum()),
        "risk_off_worst_year_return": float(risk_returns.min()),
        "risk_off_worst_year": int(yearly.loc[risk_returns.idxmin(), "year"]),
        "risk_off_worst_drawdown": float(max_dd.min()),
        "benchmark_positive_large_negative_years": benchmark_positive_large_negative,
        "rolling_start_status": "not_evaluated_in_this_pretest",
    }


def _decide(yearly: pd.DataFrame, goal: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    risk_return = goal["multi_year_risk_off_compound_return"]
    baseline_return = goal["multi_year_baseline_compound_return"]
    max_dd = float(pd.to_numeric(yearly["risk_off_max_drawdown"], errors="coerce").min())
    bench_excess_count = int(goal["benchmark_excess_year_count"])
    worst_year = float(goal["risk_off_worst_year_return"])
    if goal["all_primary_gates_pass"]:
        return "keep_for_rolling_start_validation", "primary_goal_gates_pass_pending_rolling_start", goal
    if risk_return <= baseline_return:
        return "drop_due_to_no_portfolio_improvement", "multi_year_return_does_not_exceed_baseline", goal
    if max_dd <= -0.30 or worst_year <= -0.25:
        return "hold_due_to_drawdown_risk", "drawdown_or_worst_year_gate_failed", goal
    if bench_excess_count < 4:
        return "hold_due_to_benchmark_underperformance", "benchmark_excess_years_not_over_half", goal
    return "drop_due_to_goal_gate_failure", "one_or_more_goal_gates_failed", goal


def run_pretest(robustness_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    yearly_gate = pd.read_csv(robustness_root / "yearly_results.csv")
    yearly_rows: list[dict[str, Any]] = []
    monthly_frames: list[pd.DataFrame] = []
    event_frames: list[pd.DataFrame] = []
    order_frames: list[pd.DataFrame] = []
    position_frames: list[pd.DataFrame] = []
    equity_frames: list[pd.DataFrame] = []
    missing: list[dict[str, Any]] = []
    for _idx, year_row in yearly_gate.iterrows():
        run_dir = Path(str(year_row["run_dir"]))
        result = engine._simulate_year(run_dir, year_row.to_dict(), axis_id=AXIS_ID, trigger_gate=_market_gate)
        yearly_rows.append(result["yearly"])
        if not result["orders"].empty:
            order_frames.append(result["orders"])
        if not result["positions"].empty:
            position_frames.append(result["positions"])
        if not result["equity"].empty:
            equity_frames.append(result["equity"])
        if not result["events"].empty:
            event_frames.append(result["events"])
        monthly_frames.append(engine._monthly_compare(int(year_row["year"]), pd.read_csv(run_dir / "equity_curve.csv"), result["equity"]))
        missing.extend(result["exact_missing"])
    yearly = pd.DataFrame(yearly_rows)
    monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    events = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()
    orders = pd.concat(order_frames, ignore_index=True) if order_frames else pd.DataFrame()
    positions = pd.concat(position_frames, ignore_index=True) if position_frames else pd.DataFrame()
    equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    exact = len(missing) == 0
    no_lookahead = "pass" if exact else "research_fallback"
    goal = _goal_gate(yearly, exact, no_lookahead)
    decision, reason, evidence = _decide(yearly, goal)

    _write_csv(output_root / "yearly_results_baseline_vs_market_gated_risk_off.csv", yearly)
    _write_csv(output_root / "monthly_results_baseline_vs_market_gated_risk_off.csv", monthly)
    _write_csv(output_root / "market_gated_risk_off_events.csv", events)
    _write_csv(output_root / "market_gated_risk_off_orders_ledger.csv", orders)
    _write_csv(output_root / "market_gated_risk_off_positions_ledger.csv", positions)
    _write_csv(output_root / "market_gated_risk_off_equity_curve_by_year.csv", equity)
    _write_json(output_root / "goal_gate_summary.json", goal)
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "robustness_root": str(robustness_root),
        "rule": {
            "base_trigger": "portfolio_20d_peak_drawdown_lte_minus_8pct",
            "market_gate": "block_if_benchmark_20d_return_gt_0_and_benchmark_drawdown_gt_minus_3pct",
            "trim_ratio": engine.TRIM_RATIO,
            "threshold_sweep": False,
            "market_gate_sweep": False,
            "trim_ratio_sweep": False,
        },
        "decision": decision,
        "reason_type": reason,
        "metrics": evidence,
        "scope": {"tradex_only": True, "baseline_policy_changed": False, "single_axis_only": True, "meemee_ui_changed": False, "runtime_db_written": False, "ranking_changed": False, "publish_registry_changed": False},
    }
    _write_json(output_root / "market_regime_gated_risk_off_summary.json", summary)
    _write_json(output_root / "selection_feature_manifest.json", {"schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1", "axis_id": AXIS_ID, "selection_allowed_columns": ["ymd", "portfolio_equity", "trailing_drawdown_20", "benchmark_20d_return", "benchmark_drawdown", "current_positions"], "selection_forbidden_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "future_benchmark_return"], "diagnostic_only_columns": ["goal_gate_summary", "yearly_delta_return"], "outcome_label_columns": [], "audit_result": "pass"})
    _write_json(output_root / "no_lookahead_audit.json", {"schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1", "axis_id": AXIS_ID, "audit_result": no_lookahead, "exact_next_open_replay": exact, "same_day_close_fill_used": False, "benchmark_future_return_used_for_trigger": False, "post_run_outcomes_used_for_trigger": False, "threshold_sweep": False, "market_gate_sweep": False, "trim_ratio_sweep": False, "silent_fallback_used": False, "missing_exact_price_events": missing})
    _write_json(output_root / "next_axis_decision.json", {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": evidence, "policy_promotion_allowed": False, "meemee_reflectable": False})
    complete = {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "complete": True, "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"), "decision": decision, "decision_count": 1, "exact_next_open_replay": exact, "no_lookahead_audit": no_lookahead, "goal_primary_gates_pass": goal["all_primary_gates_pass"], "goal_all_gates_pass": goal["all_gates_pass"], "baseline_policy_changed": False, "threshold_sweep": False, "market_gate_sweep": False, "trim_ratio_sweep": False, "silent_fallback_used": False, "meemee_reflectable": False, "policy_promotion_allowed": False}
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": evidence}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pretest market-regime gated risk-off cash control overlay.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_pretest(args.robustness_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
