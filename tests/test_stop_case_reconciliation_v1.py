from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import stop_case_reconciliation_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _make_run(tmp_path: Path) -> Path:
    root = tmp_path / "run"
    stop = root / "stop_too_wide_pretest_v1"
    lifecycle = root / "candidate_lifecycle_audit_v1"
    (root / "diagnosis_v1").mkdir(parents=True)
    (root / "trade_reflection_audit_v1").mkdir()
    stop.mkdir()
    lifecycle.mkdir()
    metrics = {
        "baseline_final_equity": 1_000_000.0,
        "stop_final_equity": 1_030_000.0,
        "delta_final_equity": 30_000.0,
        "baseline_max_drawdown": -0.15,
        "stop_max_drawdown": -0.10,
        "delta_max_drawdown": 0.05,
        "stop_trigger_count": 1,
        "saved_loss_total": 0.0,
        "missed_profit_total": 10_000.0,
        "false_stop_recovery_count": 1,
        "cost_delta": -1000.0,
        "invalidation_count": 1,
        "shakeout_recovery_count": 1,
        "true_breakdown_count": 0,
    }
    _write_json(stop / "baseline_vs_stop_comparison.json", {"metrics": metrics})
    _write_json(stop / "next_axis_decision.json", {"decision": "hold_due_to_recovery_risk"})
    _write_json(stop / "stop_too_wide_pretest_summary.json", {"decision": "hold_due_to_recovery_risk", "decision_reason_type": "test"})
    _write_json(lifecycle / "candidate_lifecycle_summary.json", {"selected_next_axis": "stop_too_wide_pretest", "metrics": {"invalidation_cases": 1}})
    pd.DataFrame(
        [
            {"position_id": "stop-p1", "baseline_position_id": "p1", "code": "7001", "stop_trigger_date": 20250403, "stop_exit_date": 20250404, "stop_realized_pnl": -20_000.0, "baseline_pnl": -10_000.0, "baseline_exit_ymd": 20250405, "baseline_exit_reason": "profit_target", "delta_vs_baseline": -10_000.0, "post_stop_ret5": 0.04, "post_stop_ret10": 0.08, "post_stop_ret20": 0.12, "post_stop_max_up20": 0.15, "recovery_type": "strong_shakeout_recovery"},
        ]
    ).to_csv(stop / "stop_triggered_cases.csv", index=False)
    pd.DataFrame(
        [
            {"position_id": "stop-p1", "baseline_position_id": "p1", "code": "7001", "stop_trigger_date": 20250403, "stop_exit_date": 20250404, "stop_realized_pnl": -20_000.0, "baseline_pnl": -10_000.0, "baseline_exit_ymd": 20250405, "baseline_exit_reason": "profit_target", "delta_vs_baseline": -10_000.0, "post_stop_ret5": 0.04, "post_stop_ret10": 0.08, "post_stop_ret20": 0.12, "post_stop_max_up20": 0.15, "recovery_type": "strong_shakeout_recovery"},
        ]
    ).to_csv(stop / "false_stop_recovery_cases.csv", index=False)
    pd.DataFrame(columns=["position_id", "baseline_position_id", "delta_vs_baseline"]).to_csv(stop / "saved_loss_cases.csv", index=False)
    pd.DataFrame(
        [
            {"position_id": "stop-p1", "baseline_position_id": "p1", "code": "7001", "delta_vs_baseline": -10_000.0, "post_stop_max_up20": 0.15, "post_stop_ret20": 0.12, "recovery_type": "strong_shakeout_recovery"},
        ]
    ).to_csv(stop / "missed_profit_after_stop_cases.csv", index=False)
    pd.DataFrame(
        [
            {"order_id": "b1", "decision_ymd": 20250401, "execution_ymd": 20250402, "action": "buy", "code": "7001", "order_status": "filled", "execution_price": 100.0, "shares": 100, "notional": 10_000.0, "cost_amount": 30.0, "position_id": "p1"},
            {"order_id": "e1", "decision_ymd": 20250405, "execution_ymd": 20250406, "action": "exit", "code": "7001", "order_status": "filled", "execution_price": 99.0, "shares": 100, "notional": 9_900.0, "cost_amount": 29.7, "position_id": "p1", "realized_pnl": -10_000.0, "reason_type": "profit_target"},
            {"order_id": "b2", "decision_ymd": 20250402, "execution_ymd": 20250403, "action": "buy", "code": "7002", "order_status": "filled", "execution_price": 100.0, "shares": 100, "notional": 10_000.0, "cost_amount": 30.0, "position_id": "p2"},
            {"order_id": "e2", "decision_ymd": 20250408, "execution_ymd": 20250409, "action": "exit", "code": "7002", "order_status": "filled", "execution_price": 100.0, "shares": 100, "notional": 10_000.0, "cost_amount": 30.0, "position_id": "p2", "realized_pnl": 0.0, "reason_type": "time_stop"},
        ]
    ).to_csv(root / "orders_ledger.csv", index=False)
    pd.DataFrame([{"ymd": 20250406, "position_id": "p1", "unrealized_pnl": -10_000.0}, {"ymd": 20250409, "position_id": "p2", "unrealized_pnl": 0.0}]).to_csv(root / "positions_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"order_id": "ob1", "decision_ymd": 20250401, "execution_ymd": 20250402, "action": "buy", "code": "7001", "order_status": "filled", "execution_price": 100.0, "shares": 100, "notional": 10_000.0, "cost_amount": 30.0, "position_id": "op1", "baseline_position_id": "p1"},
            {"order_id": "oe1", "decision_ymd": 20250403, "execution_ymd": 20250404, "action": "stop", "code": "7001", "order_status": "filled", "execution_price": 80.0, "shares": 100, "notional": 8_000.0, "cost_amount": 24.0, "position_id": "op1", "baseline_position_id": "p1", "realized_pnl": -20_000.0, "reason_type": "fixed_stop_minus_8pct"},
            {"order_id": "ob2", "decision_ymd": 20250402, "execution_ymd": 20250403, "action": "buy", "code": "7002", "order_status": "filled", "execution_price": 100.0, "shares": 200, "notional": 20_000.0, "cost_amount": 60.0, "position_id": "op2", "baseline_position_id": "p2"},
            {"order_id": "oe2", "decision_ymd": 20250408, "execution_ymd": 20250409, "action": "exit", "code": "7002", "order_status": "filled", "execution_price": 200.0, "shares": 200, "notional": 40_000.0, "cost_amount": 120.0, "position_id": "op2", "baseline_position_id": "p2", "realized_pnl": 40_000.0, "reason_type": "time_stop"},
        ]
    ).to_csv(stop / "stop_overlay_orders_ledger.csv", index=False)
    pd.DataFrame([{"ymd": 20250404, "position_id": "op1", "baseline_position_id": "p1", "unrealized_pnl": -20_000.0}, {"ymd": 20250409, "position_id": "op2", "baseline_position_id": "p2", "unrealized_pnl": 40_000.0}]).to_csv(stop / "stop_overlay_positions_ledger.csv", index=False)
    pd.DataFrame([{"ymd": 20250401, "equity": 1_000_000.0}, {"ymd": 20250409, "equity": 1_000_000.0}]).to_csv(root / "equity_curve.csv", index=False)
    pd.DataFrame([{"ymd": 20250401, "equity": 1_000_000.0}, {"ymd": 20250409, "equity": 1_030_000.0}]).to_csv(stop / "stop_overlay_equity_curve.csv", index=False)
    return root


def test_stop_case_reconciliation_outputs_attribution(tmp_path: Path) -> None:
    root = _make_run(tmp_path)

    result = mod.run_stop_case_reconciliation_v1(root)

    out = root / "stop_case_reconciliation_v1"
    assert result["complete"] is True
    for artifact in mod.OUTPUT_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    attribution = json.loads((out / "equity_delta_attribution.json").read_text(encoding="utf-8"))
    assert attribution["direct_stop_pnl_delta"] == -10_000.0
    assert attribution["freed_cash_redeployment_delta"] == 40_000.0
    assert abs(attribution["residual_unexplained_delta"]) < 1e-6
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["replay_rerun"] is False
    assert complete["stop_threshold_changed"] is False
    assert complete["threshold_sweep"] is False
    false_diag = pd.read_csv(out / "false_stop_recovery_diagnosis.csv")
    assert false_diag.iloc[0]["false_stop_classification"] == "recovered_strongly_after_stop"
    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
