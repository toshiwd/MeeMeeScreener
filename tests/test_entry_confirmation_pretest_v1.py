from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import entry_confirmation_pretest_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _make_gate(tmp_path: Path) -> Path:
    gate = tmp_path / "gate"
    run = gate / "subruns" / "2021-baseline-portfolio_agent_replay_v1"
    run.mkdir(parents=True)
    db = tmp_path / "stocks.duckdb"
    conn = duckdb.connect(str(db))
    try:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date DATE, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)")
        rows = [
            ("1001", "2021-01-04", 100.0, 101.0, 99.0, 100.0, 1000.0, "pan"),
            ("1001", "2021-01-05", 101.0, 102.0, 100.0, 101.0, 1000.0, "pan"),
            ("1001", "2021-01-06", 102.0, 103.0, 101.0, 102.0, 1000.0, "pan"),
            ("1001", "2021-01-07", 103.0, 104.0, 102.0, 103.0, 1000.0, "pan"),
            ("1002", "2021-01-04", 100.0, 101.0, 99.0, 100.0, 1000.0, "pan"),
            ("1002", "2021-01-05", 99.0, 100.0, 98.0, 99.0, 1000.0, "pan"),
            ("1002", "2021-01-06", 98.0, 99.0, 97.0, 98.0, 1000.0, "pan"),
            ("1002", "2021-01-07", 97.0, 98.0, 96.0, 97.0, 1000.0, "pan"),
        ]
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()
    _write_json(
        run / "run_config.json",
        {
            "source_db": str(db),
            "period": {"start_ymd": 20210104, "end_ymd": 20210107},
            "portfolio": {"initial_cash_jpy": 1_000_000.0, "per_symbol_cap_jpy": 333_333.0, "max_positions": 3},
            "cost_model": {"commission_bps": 0.0, "slippage_bps": 0.0, "tax_or_fee_bps": 0.0, "min_fee": 0.0},
            "exit_rules": {"profit_target": 0.50, "max_holding_trading_days": 20, "stop_loss": -0.50},
        },
    )
    pd.DataFrame(
        [
            {"decision_ymd": 20210104, "code": "1001", "candidate_rank": 1, "selection_score": 12, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": True, "reject_reason": "", "next_execution_ymd": 20210105},
            {"decision_ymd": 20210104, "code": "1002", "candidate_rank": 2, "selection_score": 12, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": True, "reject_reason": "", "next_execution_ymd": 20210105},
            {"decision_ymd": 20210105, "code": "1001", "candidate_rank": 1, "selection_score": 12, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": True, "reject_reason": "", "next_execution_ymd": 20210106},
            {"decision_ymd": 20210105, "code": "1002", "candidate_rank": 20, "selection_score": 8, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": False, "reject_reason": "test", "next_execution_ymd": 20210106},
        ]
    ).to_csv(run / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20210104, "code": "1001", "was_selected": True, "post_ret_20": 0.02, "mae_20": -0.01, "mfe_20": 0.03},
            {"decision_ymd": 20210104, "code": "1002", "was_selected": True, "post_ret_20": -0.10, "mae_20": -0.12, "mfe_20": 0.01},
        ]
    ).to_csv(run / "post_run_outcome_labels.csv", index=False)
    pd.DataFrame(
        [
            {"order_id": "b1", "decision_ymd": 20210104, "execution_ymd": 20210105, "action": "buy", "code": "1001", "order_status": "filled", "cost_amount": 0, "position_id": "p1"},
            {"order_id": "b2", "decision_ymd": 20210104, "execution_ymd": 20210105, "action": "buy", "code": "1002", "order_status": "filled", "cost_amount": 0, "position_id": "p2"},
        ]
    ).to_csv(run / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20210104, "cash": 1_000_000.0, "positions_market_value": 0.0, "equity": 1_000_000.0, "market_benchmark_equity": 1_000_000.0, "benchmark_code": "1306"},
            {"ymd": 20210105, "cash": 333_333.0, "positions_market_value": 666_000.0, "equity": 999_333.0, "market_benchmark_equity": 1_010_000.0, "benchmark_code": "1306"},
            {"ymd": 20210106, "cash": 333_333.0, "positions_market_value": 666_000.0, "equity": 999_333.0, "market_benchmark_equity": 1_020_000.0, "benchmark_code": "1306"},
            {"ymd": 20210107, "cash": 333_333.0, "positions_market_value": 666_000.0, "equity": 999_333.0, "market_benchmark_equity": 1_030_000.0, "benchmark_code": "1306"},
        ]
    ).to_csv(run / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {"year": 2021, "run_dir": str(run), "final_equity": 999_333.0, "total_return": -0.000667, "benchmark_return": 0.03, "excess_return": -0.030667, "max_drawdown": -0.01, "order_count": 2, "win_rate": 0, "stop_count": 0, "exit_count": 0, "primary_failure_mode": "test"}
        ]
    ).to_csv(gate / "yearly_results.csv", index=False)
    return gate


def test_entry_confirmation_pretest_outputs_and_cancels_weak_entry(tmp_path: Path) -> None:
    gate = _make_gate(tmp_path)

    result = mod.run_pretest(gate)

    out = gate / "entry_confirmation_pretest_v1"
    assert result["complete"] is True
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["exact_next_open_replay"] is True
    assert complete["confirmation_days_sweep"] is False
    assert complete["rank_threshold_sweep"] is False

    orders = pd.read_csv(out / "entry_confirmation_orders_ledger.csv")
    buy = orders[orders["action"] == "buy"].iloc[0]
    assert buy["decision_ymd"] == 20210105
    assert buy["execution_ymd"] == 20210106
    assert buy["code"] == 1001 or str(buy["code"]) == "1001"

    cancelled = pd.read_csv(out / "cancelled_entries.csv")
    assert "1002" in set(cancelled["code"].astype(str))
    outcome = pd.read_csv(out / "entry_confirmation_outcome_analysis.csv")
    assert "avoided_bad_entry" in set(outcome["entry_confirmation_outcome_class"])

    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    assert audit["audit_result"] == "pass"
    assert audit["post_run_outcomes_used_for_confirmation"] is False

    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
