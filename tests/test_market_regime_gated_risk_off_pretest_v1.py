from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import market_regime_gated_risk_off_pretest_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _make_gate(tmp_path: Path) -> Path:
    gate = tmp_path / "gate"
    run = gate / "subruns" / "2021-baseline-portfolio_agent_replay_v1"
    run.mkdir(parents=True)
    db = tmp_path / "stocks.duckdb"
    dates = pd.bdate_range("2021-01-01", periods=25)
    rows = []
    equity_rows = []
    for idx, date in enumerate(dates):
        ymd = int(date.strftime("%Y%m%d"))
        close = 100.0 if idx < 21 else 90.0
        open_price = 100.0 if idx <= 21 else 90.0
        rows.append(("7001", date.strftime("%Y-%m-%d"), open_price, max(open_price, close), min(open_price, close), close, 1000.0, "pan"))
        if idx == 0:
            cash = 1_000_000.0
            mv = 0.0
            equity = 1_000_000.0
            open_count = 0
        else:
            cash = 0.0
            mv = 10_000 * close
            equity = mv
            open_count = 1
        equity_rows.append(
            {
                "ymd": ymd,
                "cash": cash,
                "positions_market_value": mv,
                "equity": equity,
                "market_benchmark_equity": 1_000_000.0 + idx * 10_000.0,
                "benchmark_code": "1306",
                "open_position_count": open_count,
            }
        )
    conn = duckdb.connect(str(db))
    try:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date DATE, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)")
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()
    _write_json(
        run / "run_config.json",
        {
            "source_db": str(db),
            "period": {"start_ymd": int(dates[0].strftime("%Y%m%d")), "end_ymd": int(dates[-1].strftime("%Y%m%d"))},
            "portfolio": {"initial_cash_jpy": 1_000_000.0, "per_symbol_cap_jpy": 1_000_000.0, "max_positions": 1},
            "cost_model": {"commission_bps": 0.0, "slippage_bps": 0.0, "tax_or_fee_bps": 0.0, "min_fee": 0.0},
            "exit_rules": {"profit_target": 0.50, "max_holding_trading_days": 100, "stop_loss": -0.50},
        },
    )
    first_ymd = int(dates[0].strftime("%Y%m%d"))
    second_ymd = int(dates[1].strftime("%Y%m%d"))
    _write_jsonl(
        run / "daily_action_ledger.jsonl",
        [{"action": "buy", "decision_ymd": first_ymd, "execution_ymd": second_ymd, "code": "7001", "order_id": "buy-1", "selection_score": 10, "candidate_rank": 1}],
    )
    pd.DataFrame(
        [
            {
                "order_id": "buy-1",
                "decision_ymd": first_ymd,
                "execution_ymd": second_ymd,
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
    pd.DataFrame(equity_rows).to_csv(run / "equity_curve.csv", index=False)
    pd.DataFrame(
        [
            {
                "year": 2021,
                "run_dir": str(run),
                "final_equity": 900_000.0,
                "total_return": -0.10,
                "benchmark_return": 0.24,
                "excess_return": -0.34,
                "max_drawdown": -0.10,
                "order_count": 1,
                "win_rate": 0.0,
                "stop_count": 0,
                "exit_count": 0,
                "primary_failure_mode": "test",
            }
        ]
    ).to_csv(gate / "yearly_results.csv", index=False)
    return gate


def test_market_regime_gate_blocks_risk_off_when_benchmark_is_healthy(tmp_path: Path) -> None:
    gate = _make_gate(tmp_path)

    result = mod.run_pretest(gate)

    out = gate / "market_regime_gated_risk_off_pretest_v1"
    assert result["complete"] is True
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["exact_next_open_replay"] is True
    assert complete["market_gate_sweep"] is False

    events = pd.read_csv(out / "market_gated_risk_off_events.csv")
    assert "risk_off_blocked_by_market_gate" in set(events["event_type"])
    orders = pd.read_csv(out / "market_gated_risk_off_orders_ledger.csv")
    assert "risk_off_trim" not in set(orders["action"])

    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    assert audit["audit_result"] == "pass"
    assert audit["benchmark_future_return_used_for_trigger"] is False

    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
