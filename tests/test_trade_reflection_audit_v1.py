from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import trade_reflection_audit_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _make_run(tmp_path: Path) -> Path:
    root = tmp_path / "run"
    root.mkdir()
    _write_json(root / "failure_diagnosis_summary.json", {"metrics": {"bought_weak_candidate_count": 3, "total_cost": 9000.0}})
    _write_json(root / "no_lookahead_audit.json", {"audit_result": "pass"})
    _write_json(root / "selection_feature_manifest.json", {"audit_result": "pass"})
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "candidate_rank": 1, "selection_score": 15, "selected_for_buy": True, "next_execution_ymd": 20250402, "close": 1000.0, "score_components_json": "[]"},
            {"decision_ymd": 20250401, "code": "7002", "candidate_rank": 2, "selection_score": 14, "selected_for_buy": False, "next_execution_ymd": 20250402, "close": 1000.0, "score_components_json": "[]"},
            {"decision_ymd": 20250402, "code": "7003", "candidate_rank": 1, "selection_score": 14, "selected_for_buy": False, "next_execution_ymd": 20250403, "close": 1000.0, "score_components_json": "[]"},
            {"decision_ymd": 20250403, "code": "7003", "candidate_rank": 1, "selection_score": 15, "selected_for_buy": False, "next_execution_ymd": 20250404, "close": 1050.0, "score_components_json": "[]"},
            {"decision_ymd": 20250404, "code": "7003", "candidate_rank": 1, "selection_score": 16, "selected_for_buy": True, "next_execution_ymd": 20250407, "close": 1100.0, "score_components_json": "[]"},
            {"decision_ymd": 20250407, "code": "7001", "candidate_rank": 4, "selection_score": 11, "selected_for_buy": False, "next_execution_ymd": 20250408, "close": 1080.0, "score_components_json": "[]"},
            {"decision_ymd": 20250408, "code": "7001", "candidate_rank": 8, "selection_score": 10, "selected_for_buy": False, "next_execution_ymd": 20250409, "close": 1150.0, "score_components_json": "[]"},
            {"decision_ymd": 20250409, "code": "7001", "candidate_rank": 9, "selection_score": 10, "selected_for_buy": False, "next_execution_ymd": 20250410, "close": 1160.0, "score_components_json": "[]"},
            {"decision_ymd": 20250410, "code": "7001", "candidate_rank": 10, "selection_score": 10, "selected_for_buy": False, "next_execution_ymd": 20250411, "close": 1170.0, "score_components_json": "[]"},
            {"decision_ymd": 20250411, "code": "7001", "candidate_rank": 10, "selection_score": 10, "selected_for_buy": False, "next_execution_ymd": 20250414, "close": 1180.0, "score_components_json": "[]"},
            {"decision_ymd": 20250414, "code": "7001", "candidate_rank": 10, "selection_score": 10, "selected_for_buy": False, "next_execution_ymd": 20250415, "close": 1190.0, "score_components_json": "[]"},
        ]
    ).to_csv(root / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7002", "candidate_rank": 2, "selection_score": 14, "reject_reason": "max_positions_full", "selected_best_score": 15, "selected_best_rank": 1},
        ]
    ).to_csv(root / "rejected_candidates.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20250401, "code": "7001", "was_selected": True, "diagnostic_only": True, "post_ret_20": 0.02, "mae_20": -0.08, "mfe_20": 0.04, "outcome_bucket": "flat"},
            {"decision_ymd": 20250401, "code": "7002", "was_selected": False, "diagnostic_only": True, "post_ret_20": 0.22, "mae_20": -0.01, "mfe_20": 0.25, "outcome_bucket": "winner"},
            {"decision_ymd": 20250404, "code": "7003", "was_selected": True, "diagnostic_only": True, "post_ret_20": -0.03, "mae_20": -0.07, "mfe_20": 0.03, "outcome_bucket": "loser"},
        ]
    ).to_csv(root / "post_run_outcome_labels.csv", index=False)
    pd.DataFrame(
        [
            {"order_id": "o1", "decision_ymd": 20250401, "execution_ymd": 20250402, "action": "buy", "code": "7001", "order_status": "filled", "execution_price": 1000.0, "shares": 100, "notional": 100000.0, "cost_amount": 300.0, "position_id": "p1", "reason_type": "entry_score_passed"},
            {"order_id": "o2", "decision_ymd": 20250404, "execution_ymd": 20250407, "action": "exit", "code": "7001", "order_status": "filled", "execution_price": 1070.0, "shares": 100, "notional": 107000.0, "cost_amount": 321.0, "position_id": "p1", "reason_type": "profit_target", "realized_pnl": 6379.0, "realized_return": 0.063},
            {"order_id": "o3", "decision_ymd": 20250404, "execution_ymd": 20250407, "action": "buy", "code": "7003", "order_status": "filled", "execution_price": 1100.0, "shares": 100, "notional": 110000.0, "cost_amount": 330.0, "position_id": "p2", "reason_type": "entry_score_passed"},
            {"order_id": "o4", "decision_ymd": 20250410, "execution_ymd": 20250411, "action": "stop", "code": "7003", "order_status": "filled", "execution_price": 990.0, "shares": 100, "notional": 99000.0, "cost_amount": 297.0, "position_id": "p2", "reason_type": "stop_loss", "realized_pnl": -11627.0, "realized_return": -0.105},
        ]
    ).to_csv(root / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250402, "position_id": "p1", "code": "7001", "entry_ymd": 20250402, "entry_price": 1000.0, "close_price": 960.0, "market_value": 96000.0, "cost_basis": 100300.0, "unrealized_pnl": -4300.0, "holding_days": 1},
            {"ymd": 20250403, "position_id": "p1", "code": "7001", "entry_ymd": 20250402, "entry_price": 1000.0, "close_price": 1030.0, "market_value": 103000.0, "cost_basis": 100300.0, "unrealized_pnl": 2700.0, "holding_days": 2},
            {"ymd": 20250407, "position_id": "p2", "code": "7003", "entry_ymd": 20250407, "entry_price": 1100.0, "close_price": 1040.0, "market_value": 104000.0, "cost_basis": 110330.0, "unrealized_pnl": -6330.0, "holding_days": 1},
            {"ymd": 20250408, "position_id": "p2", "code": "7003", "entry_ymd": 20250407, "entry_price": 1100.0, "close_price": 1000.0, "market_value": 100000.0, "cost_basis": 110330.0, "unrealized_pnl": -10330.0, "holding_days": 2},
            {"ymd": 20250409, "position_id": "p2", "code": "7003", "entry_ymd": 20250407, "entry_price": 1100.0, "close_price": 1010.0, "market_value": 101000.0, "cost_basis": 110330.0, "unrealized_pnl": -9330.0, "holding_days": 3},
            {"ymd": 20250410, "position_id": "p2", "code": "7003", "entry_ymd": 20250407, "entry_price": 1100.0, "close_price": 1005.0, "market_value": 100500.0, "cost_basis": 110330.0, "unrealized_pnl": -9830.0, "holding_days": 4},
        ]
    ).to_csv(root / "positions_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250401, "cash": 1000000.0, "positions_market_value": 0.0, "equity": 1000000.0},
            {"ymd": 20250402, "cash": 899700.0, "positions_market_value": 96000.0, "equity": 995700.0},
            {"ymd": 20250407, "cash": 896079.0, "positions_market_value": 104000.0, "equity": 1000079.0},
            {"ymd": 20250411, "cash": 984452.0, "positions_market_value": 0.0, "equity": 984452.0},
        ]
    ).to_csv(root / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {"code": "7001", "net_contribution": 6379.0, "realized_pnl": 6379.0, "total_cost": 621.0},
            {"code": "7003", "net_contribution": -11627.0, "realized_pnl": -11627.0, "total_cost": 627.0},
        ]
    ).to_csv(root / "trade_contribution.csv", index=False)
    _write_jsonl(
        root / "daily_action_ledger.jsonl",
        [
            {"decision_ymd": 20250401, "action": "buy", "code": "7001"},
            {"decision_ymd": 20250401, "action": "reject", "code": "7002", "reason_type": "max_positions_full"},
            {"decision_ymd": 20250404, "action": "exit", "code": "7001"},
            {"decision_ymd": 20250404, "action": "buy", "code": "7003"},
            {"decision_ymd": 20250410, "action": "stop", "code": "7003"},
        ],
    )
    return root


def test_trade_reflection_audit_outputs_required_artifacts(tmp_path: Path) -> None:
    root = _make_run(tmp_path)

    result = mod.run_trade_reflection_audit_v1(root)

    out = root / "trade_reflection_audit_v1"
    assert result["complete"] is True
    for artifact in mod.OUTPUT_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["replay_rerun"] is False
    assert complete["rule_changed"] is False
    assert complete["selected_next_axis_count"] == 1
    missed = pd.read_csv(out / "missed_entry_cases.csv")
    assert missed.iloc[0]["missed_entry_class"] == "position_limit"
    late_entry = pd.read_csv(out / "late_entry_cases.csv")
    assert late_entry["late_entry_flag"].astype(str).str.lower().isin(["true"]).any()
    early_entry = pd.read_csv(out / "early_entry_cases.csv")
    assert set(early_entry["code"].astype(str)) == {"7001", "7003"}
    early_profit = pd.read_csv(out / "early_profit_take_cases.csv")
    assert early_profit["early_profit_take_flag"].astype(str).str.lower().isin(["true"]).any()
    late_exit = pd.read_csv(out / "late_exit_cases.csv")
    assert not late_exit.empty
    hold_worked = pd.read_csv(out / "hold_would_have_worked_cases.csv")
    assert set(hold_worked["code"].astype(str)) == {"7001"}
    decision = json.loads((out / "reflection_priority_decision.json").read_text(encoding="utf-8"))
    assert decision["selected_next_axis"] in mod.PRIORITY_CANDIDATES
    assert decision["policy"]["post_run_diagnostics_not_used_as_trading_rules"] is True
