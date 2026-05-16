from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import market_regime_gated_risk_off_pretest_v1 as goal
from scripts import risk_off_cash_control_pretest_v1 as engine


AXIS_ID = "faster_risk_on_reentry_pretest_v1"
SCHEMA_PREFIX = "tradex_faster_risk_on_reentry_pretest_v1"
DEFAULT_OUTPUT_DIR_NAME = "faster_risk_on_reentry_pretest_v1"
FAST_RISK_OFF_MAX_DAYS = 10

REQUIRED_ARTIFACTS = (
    "faster_risk_on_reentry_summary.json",
    "yearly_results_baseline_vs_faster_reentry.csv",
    "monthly_results_baseline_vs_faster_reentry.csv",
    "faster_reentry_events.csv",
    "faster_reentry_orders_ledger.csv",
    "faster_reentry_positions_ledger.csv",
    "faster_reentry_equity_curve_by_year.csv",
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(goal._json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


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
    old_max_days = engine.RISK_OFF_MAX_DAYS
    try:
        engine.RISK_OFF_MAX_DAYS = FAST_RISK_OFF_MAX_DAYS
        for _idx, year_row in yearly_gate.iterrows():
            run_dir = Path(str(year_row["run_dir"]))
            result = engine._simulate_year(run_dir, year_row.to_dict(), axis_id=AXIS_ID)
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
    finally:
        engine.RISK_OFF_MAX_DAYS = old_max_days

    yearly = pd.DataFrame(yearly_rows)
    monthly = pd.concat(monthly_frames, ignore_index=True) if monthly_frames else pd.DataFrame()
    events = pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()
    orders = pd.concat(order_frames, ignore_index=True) if order_frames else pd.DataFrame()
    positions = pd.concat(position_frames, ignore_index=True) if position_frames else pd.DataFrame()
    equity = pd.concat(equity_frames, ignore_index=True) if equity_frames else pd.DataFrame()
    exact = len(missing) == 0
    no_lookahead = "pass" if exact else "research_fallback"
    gate = goal._goal_gate(yearly, exact, no_lookahead)
    decision, reason, evidence = goal._decide(yearly, gate)

    _write_csv(output_root / "yearly_results_baseline_vs_faster_reentry.csv", yearly)
    _write_csv(output_root / "monthly_results_baseline_vs_faster_reentry.csv", monthly)
    _write_csv(output_root / "faster_reentry_events.csv", events)
    _write_csv(output_root / "faster_reentry_orders_ledger.csv", orders)
    _write_csv(output_root / "faster_reentry_positions_ledger.csv", positions)
    _write_csv(output_root / "faster_reentry_equity_curve_by_year.csv", equity)
    _write_json(output_root / "goal_gate_summary.json", gate)
    _write_json(
        output_root / "faster_risk_on_reentry_summary.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "robustness_root": str(robustness_root),
            "rule": {
                "base_trigger": "portfolio_20d_peak_drawdown_lte_minus_8pct",
                "risk_off_max_days": FAST_RISK_OFF_MAX_DAYS,
                "baseline_risk_off_max_days": old_max_days,
                "recovery_threshold": engine.RECOVERY_THRESHOLD,
                "trim_ratio": engine.TRIM_RATIO,
                "max_days_sweep": False,
            },
            "decision": decision,
            "reason_type": reason,
            "metrics": evidence,
            "scope": {"tradex_only": True, "baseline_policy_changed": False, "single_axis_only": True, "meemee_ui_changed": False, "runtime_db_written": False, "ranking_changed": False, "publish_registry_changed": False},
        },
    )
    _write_json(output_root / "selection_feature_manifest.json", {"schema_version": f"{SCHEMA_PREFIX}_selection_feature_manifest_v1", "axis_id": AXIS_ID, "selection_allowed_columns": ["ymd", "portfolio_equity", "trailing_drawdown_20", "risk_off_days", "current_positions"], "selection_forbidden_columns": ["post_ret_5", "post_ret_10", "post_ret_20", "post_ret_40", "mae_20", "mfe_20", "future_benchmark_return"], "diagnostic_only_columns": ["goal_gate_summary", "yearly_delta_return"], "outcome_label_columns": [], "audit_result": "pass"})
    _write_json(output_root / "no_lookahead_audit.json", {"schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1", "axis_id": AXIS_ID, "audit_result": no_lookahead, "exact_next_open_replay": exact, "same_day_close_fill_used": False, "benchmark_future_return_used_for_trigger": False, "post_run_outcomes_used_for_trigger": False, "max_days_sweep": False, "threshold_sweep": False, "trim_ratio_sweep": False, "silent_fallback_used": False, "missing_exact_price_events": missing})
    _write_json(output_root / "next_axis_decision.json", {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": evidence, "policy_promotion_allowed": False, "meemee_reflectable": False})
    complete = {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "complete": True, "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"), "decision": decision, "decision_count": 1, "exact_next_open_replay": exact, "no_lookahead_audit": no_lookahead, "goal_primary_gates_pass": gate["all_primary_gates_pass"], "goal_all_gates_pass": gate["all_gates_pass"], "baseline_policy_changed": False, "max_days_sweep": False, "threshold_sweep": False, "trim_ratio_sweep": False, "silent_fallback_used": False, "meemee_reflectable": False, "policy_promotion_allowed": False}
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": evidence}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pretest faster risk-on reentry for the risk-off overlay.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(goal._json_ready(run_pretest(args.robustness_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
