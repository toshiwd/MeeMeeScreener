from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import partial_stop_at_minus8_pretest_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _make_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date DATE, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)")
        rows = [
            ("7001", "2025-04-01", 100.0, 101.0, 99.0, 100.0, 1000.0, "pan"),
            ("7001", "2025-04-02", 100.0, 102.0, 99.0, 100.0, 1000.0, "pan"),
            ("7001", "2025-04-03", 99.0, 100.0, 90.0, 91.0, 1200.0, "pan"),
            ("7001", "2025-04-04", 90.0, 92.0, 88.0, 89.0, 1300.0, "pan"),
            ("7001", "2025-04-07", 88.0, 101.0, 87.0, 100.0, 1400.0, "pan"),
            ("7001", "2025-04-08", 101.0, 105.0, 99.0, 104.0, 1100.0, "pan"),
            ("7001", "2025-04-09", 104.0, 108.0, 103.0, 107.0, 1100.0, "pan"),
        ]
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def _make_run(tmp_path: Path) -> Path:
    root = tmp_path / "run"
    root.mkdir()
    db = tmp_path / "stocks.duckdb"
    _make_db(db)
    _write_json(
        root / "run_config.json",
        {
            "source_db": str(db),
            "period": {"start_ymd": 20250401, "end_ymd": 20250409},
            "portfolio": {"initial_cash_jpy": 1_000_000.0, "per_symbol_cap_jpy": 333_333.0, "max_positions": 3},
            "cost_model": {"commission_bps": 15.0, "slippage_bps": 15.0, "tax_or_fee_bps": 0.0, "min_fee": 0.0},
            "exit_rules": {"profit_target": 0.50, "max_holding_trading_days": 20, "stop_loss": -0.08},
        },
    )
    _write_jsonl(
        root / "daily_action_ledger.jsonl",
        [
            {
                "action": "buy",
                "decision_ymd": 20250401,
                "execution_ymd": 20250402,
                "code": "7001",
                "order_id": "baseline-buy-1",
                "selection_score": 15,
                "candidate_rank": 1,
            }
        ],
    )
    pd.DataFrame(
        [
            {
                "order_id": "baseline-buy-1",
                "decision_ymd": 20250401,
                "execution_ymd": 20250402,
                "action": "buy",
                "code": "7001",
                "order_status": "filled",
                "execution_price": 100.0,
                "shares": 3300,
                "notional": 330000.0,
                "cost_amount": 990.0,
                "position_id": "p1",
                "reason_type": "entry_score_passed",
            }
        ]
    ).to_csv(root / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250402, "position_id": "p1", "code": "7001", "shares": 3300, "entry_ymd": 20250402, "entry_price": 100.0, "close_price": 100.0, "market_value": 330000.0, "cost_basis": 330990.0, "unrealized_pnl": -990.0, "holding_days": 1},
            {"ymd": 20250403, "position_id": "p1", "code": "7001", "shares": 3300, "entry_ymd": 20250402, "entry_price": 100.0, "close_price": 91.0, "market_value": 300300.0, "cost_basis": 330990.0, "unrealized_pnl": -30690.0, "holding_days": 2},
        ]
    ).to_csv(root / "positions_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250401, "equity": 1_000_000.0},
            {"ymd": 20250402, "equity": 999_010.0},
            {"ymd": 20250403, "equity": 969_310.0},
            {"ymd": 20250404, "equity": 962_710.0},
            {"ymd": 20250407, "equity": 999_010.0},
            {"ymd": 20250408, "equity": 1_012_210.0},
            {"ymd": 20250409, "equity": 1_022_110.0},
        ]
    ).to_csv(root / "equity_curve.csv", index=False)
    _write_json(
        root / "stop_too_wide_pretest_v1" / "baseline_vs_stop_comparison.json",
        {"metrics": {"stop_final_equity": 990_000.0, "stop_max_drawdown": -0.04, "missed_profit_total": 10_000.0}},
    )
    _write_json(root / "stop_case_reconciliation_v1" / "equity_delta_attribution.json", {"exposure_delta": 12_345.0})
    _write_json(
        root / "hedged_stop_at_minus8_pretest_v1" / "baseline_vs_hedge_comparison.json",
        {"metrics": {"hedge_final_equity": 980_000.0, "hedge_max_drawdown": -0.06}},
    )
    _write_json(
        root / "candidate_lifecycle_audit_v1" / "candidate_lifecycle_summary.json",
        {"metrics": {"invalidation_cases": 1, "false_invalidation_recovery_cases": 1}},
    )
    (root / "diagnosis_v1").mkdir()
    return root


def test_partial_stop_at_minus8_pretest_outputs_exact_next_open(tmp_path: Path) -> None:
    root = _make_run(tmp_path)

    result = mod.run_partial_stop_at_minus8_pretest_v1(root)

    out = root / "partial_stop_at_minus8_pretest_v1"
    assert result["complete"] is True
    for artifact in mod.OUTPUT_ARTIFACTS:
        assert (out / artifact).exists(), artifact

    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["exact_next_open_replay"] is True
    assert complete["no_lookahead_audit"] == "pass"
    assert complete["threshold_sweep"] is False
    assert complete["trim_ratio_sweep"] is False

    orders = pd.read_csv(out / "partial_stop_orders_ledger.csv")
    partial = orders[orders["action"] == "partial_stop"].iloc[0]
    assert partial["decision_ymd"] == 20250403
    assert partial["execution_ymd"] == 20250404
    assert partial["execution_price"] == 90.0
    assert partial["shares"] == 1600

    positions = pd.read_csv(out / "partial_stop_positions_ledger.csv")
    after_trim = positions[positions["ymd"] == 20250404].iloc[0]
    assert after_trim["shares"] == 1700
    assert after_trim["partial_stop_triggered"] is True or str(after_trim["partial_stop_triggered"]).lower() == "true"

    cases = pd.read_csv(out / "partial_stop_triggered_cases.csv")
    assert len(cases) == 1
    assert cases.iloc[0]["partial_stop_trigger_date"] == 20250403
    assert cases.iloc[0]["partial_stop_exit_date"] == 20250404

    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    assert audit["post_run_outcomes_used_for_exit_condition"] is False
    assert audit["future_recovery_used_for_exit_condition"] is False

    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
