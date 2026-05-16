from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import risk_off_cash_control_pretest_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _make_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE daily_bars(code VARCHAR, date DATE, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)"
        )
        rows = [
            ("7001", "2021-01-01", 100.0, 101.0, 99.0, 100.0, 1000.0, "pan"),
            ("7001", "2021-01-04", 100.0, 101.0, 99.0, 100.0, 1000.0, "pan"),
            ("7001", "2021-01-05", 100.0, 101.0, 89.0, 90.0, 1200.0, "pan"),
            ("7001", "2021-01-06", 90.0, 92.0, 88.0, 91.0, 1200.0, "pan"),
            ("7001", "2021-01-07", 91.0, 93.0, 90.0, 92.0, 1200.0, "pan"),
            ("7001", "2021-01-08", 92.0, 96.0, 91.0, 95.0, 1200.0, "pan"),
        ]
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def _make_baseline_gate(tmp_path: Path) -> Path:
    gate = tmp_path / "baseline_gate"
    run = gate / "subruns" / "2021-baseline-portfolio_agent_replay_v1"
    run.mkdir(parents=True)
    db = tmp_path / "stocks.duckdb"
    _make_db(db)

    _write_json(
        run / "run_config.json",
        {
            "source_db": str(db),
            "period": {"start_ymd": 20210101, "end_ymd": 20210108},
            "portfolio": {"initial_cash_jpy": 1_000_000.0, "per_symbol_cap_jpy": 1_000_000.0, "max_positions": 1},
            "cost_model": {"commission_bps": 0.0, "slippage_bps": 0.0, "tax_or_fee_bps": 0.0, "min_fee": 0.0},
            "exit_rules": {"profit_target": 0.50, "max_holding_trading_days": 30, "stop_loss": -0.50},
        },
    )
    _write_jsonl(
        run / "daily_action_ledger.jsonl",
        [
            {
                "action": "buy",
                "decision_ymd": 20210101,
                "execution_ymd": 20210104,
                "code": "7001",
                "order_id": "baseline-buy-1",
                "selection_score": 15,
                "candidate_rank": 1,
            },
            {
                "action": "buy",
                "decision_ymd": 20210105,
                "execution_ymd": 20210106,
                "code": "7001",
                "order_id": "baseline-buy-skipped-during-risk-off",
                "selection_score": 16,
                "candidate_rank": 1,
            },
        ],
    )
    pd.DataFrame(
        [
            {
                "order_id": "baseline-buy-1",
                "decision_ymd": 20210101,
                "execution_ymd": 20210104,
                "action": "buy",
                "code": "7001",
                "order_status": "filled",
                "execution_price": 100.0,
                "shares": 10000,
                "notional": 1_000_000.0,
                "cost_amount": 0.0,
                "position_id": "p1",
            }
        ]
    ).to_csv(run / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {
                "ymd": 20210101,
                "cash": 1_000_000.0,
                "positions_market_value": 0.0,
                "equity": 1_000_000.0,
                "market_benchmark_equity": 1_000_000.0,
                "open_position_count": 0,
            },
            {
                "ymd": 20210104,
                "cash": 0.0,
                "positions_market_value": 1_000_000.0,
                "equity": 1_000_000.0,
                "market_benchmark_equity": 1_010_000.0,
                "open_position_count": 1,
            },
            {
                "ymd": 20210105,
                "cash": 0.0,
                "positions_market_value": 900_000.0,
                "equity": 900_000.0,
                "market_benchmark_equity": 1_020_000.0,
                "open_position_count": 1,
            },
            {
                "ymd": 20210106,
                "cash": 0.0,
                "positions_market_value": 910_000.0,
                "equity": 910_000.0,
                "market_benchmark_equity": 1_030_000.0,
                "open_position_count": 1,
            },
            {
                "ymd": 20210107,
                "cash": 0.0,
                "positions_market_value": 920_000.0,
                "equity": 920_000.0,
                "market_benchmark_equity": 1_040_000.0,
                "open_position_count": 1,
            },
            {
                "ymd": 20210108,
                "cash": 0.0,
                "positions_market_value": 950_000.0,
                "equity": 950_000.0,
                "market_benchmark_equity": 1_050_000.0,
                "open_position_count": 1,
            },
        ]
    ).to_csv(run / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {
                "year": 2021,
                "run_dir": str(run),
                "final_equity": 950_000.0,
                "total_return": -0.05,
                "benchmark_return": 0.05,
                "excess_return": -0.10,
                "max_drawdown": -0.10,
                "order_count": 1,
                "win_rate": 0.0,
                "stop_count": 0,
                "exit_count": 0,
                "primary_failure_mode": "test",
                "no_lookahead_audit": "pass",
                "accounting_reconciliation": "pass",
                "next_open_execution": "pass",
            }
        ]
    ).to_csv(gate / "yearly_results.csv", index=False)
    return gate


def test_risk_off_cash_control_pretest_outputs_exact_next_open(tmp_path: Path) -> None:
    gate = _make_baseline_gate(tmp_path)

    result = mod.run_risk_off_pretest(gate)

    out = gate / "risk_off_cash_control_pretest_v1"
    assert result["complete"] is True
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact

    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["exact_next_open_replay"] is True
    assert complete["no_lookahead_audit"] == "pass"
    assert complete["threshold_sweep"] is False
    assert complete["trim_ratio_sweep"] is False
    assert complete["lookback_sweep"] is False

    orders = pd.read_csv(out / "risk_off_orders_ledger.csv")
    trim = orders[orders["action"] == "risk_off_trim"].iloc[0]
    assert trim["decision_ymd"] == 20210105
    assert trim["execution_ymd"] == 20210106
    assert trim["execution_price"] == 90.0
    assert trim["shares"] == 5000
    assert not (orders["order_id"] == "baseline-buy-skipped-during-risk-off").any()

    events = pd.read_csv(out / "risk_off_events.csv")
    assert events.iloc[0]["event_type"] == "risk_off"
    assert events.iloc[0]["decision_ymd"] == 20210105
    assert events.iloc[0]["execution_ymd"] == 20210106

    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    assert audit["audit_result"] == "pass"
    assert audit["benchmark_future_return_used_for_trigger"] is False
    assert audit["post_run_outcomes_used_for_trigger"] is False

    manifest = json.loads((out / "selection_feature_manifest.json").read_text(encoding="utf-8"))
    assert manifest["audit_result"] == "pass"
    assert "post_ret_20" in manifest["selection_forbidden_columns"]

    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
