from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import same_day_selection_filter_pretest_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _components(volume: str, candle: str) -> str:
    return json.dumps(
        [
            {"feature": "daily_volume_state", "value": volume},
            {"feature": "daily_candle_state", "value": candle},
            {"feature": "weekly_trend_state", "value": "weekly_uptrend"},
            {"feature": "monthly_trend_state", "value": "monthly_uptrend"},
        ]
    )


def _make_gate(tmp_path: Path) -> Path:
    gate = tmp_path / "gate"
    run = gate / "subruns" / "2021-baseline-portfolio_agent_replay_v1"
    run.mkdir(parents=True)
    db = tmp_path / "stocks.duckdb"
    conn = duckdb.connect(str(db))
    try:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date DATE, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)")
        rows = []
        for code, base in [("1001", 100.0), ("1002", 200.0)]:
            rows.extend(
                [
                    (code, "2021-01-04", base, base + 1, base - 1, base, 1000.0, "pan"),
                    (code, "2021-01-05", base, base + 1, base - 1, base, 1000.0, "pan"),
                    (code, "2021-01-06", base + 1, base + 2, base, base + 1, 1000.0, "pan"),
                ]
            )
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()
    _write_json(
        run / "run_config.json",
        {
            "source_db": str(db),
            "period": {"start_ymd": 20210104, "end_ymd": 20210106},
            "portfolio": {"initial_cash_jpy": 1_000_000.0, "per_symbol_cap_jpy": 333_333.0, "max_positions": 1},
            "cost_model": {"commission_bps": 0.0, "slippage_bps": 0.0, "tax_or_fee_bps": 0.0, "min_fee": 0.0},
            "exit_rules": {"profit_target": 0.50, "max_holding_trading_days": 20, "stop_loss": -0.50},
        },
    )
    pd.DataFrame(
        [
            {"decision_ymd": 20210104, "code": "1001", "candidate_rank": 1, "selection_score": 15, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": True, "reject_reason": "", "next_execution_ymd": 20210105, "next_open_available": True, "score_components_json": _components("daily_volume_normal", "daily_upper_wick")},
            {"decision_ymd": 20210104, "code": "1002", "candidate_rank": 2, "selection_score": 14, "entry_allowed_by_score": True, "downside_guard_blocked": False, "selected_for_buy": False, "reject_reason": "max_positions_full", "next_execution_ymd": 20210105, "next_open_available": True, "score_components_json": _components("daily_volume_expansion", "daily_strong_bull")},
        ]
    ).to_csv(run / "daily_candidate_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {"decision_ymd": 20210104, "code": "1001", "post_ret_20": -0.05, "mae_20": -0.08, "mfe_20": 0.01},
            {"decision_ymd": 20210104, "code": "1002", "post_ret_20": 0.10, "mae_20": -0.01, "mfe_20": 0.12},
        ]
    ).to_csv(run / "post_run_outcome_labels.csv", index=False)
    pd.DataFrame([{"order_id": "b1", "decision_ymd": 20210104, "execution_ymd": 20210105, "action": "buy", "code": "1001", "order_status": "filled", "cost_amount": 0, "position_id": "p1"}]).to_csv(run / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20210104, "cash": 1_000_000.0, "positions_market_value": 0.0, "equity": 1_000_000.0, "market_benchmark_equity": 1_000_000.0, "benchmark_code": "1306"},
            {"ymd": 20210105, "cash": 666_667.0, "positions_market_value": 333_333.0, "equity": 1_000_000.0, "market_benchmark_equity": 1_010_000.0, "benchmark_code": "1306"},
            {"ymd": 20210106, "cash": 666_667.0, "positions_market_value": 333_333.0, "equity": 1_000_000.0, "market_benchmark_equity": 1_020_000.0, "benchmark_code": "1306"},
        ]
    ).to_csv(run / "equity_curve.csv", index=False)
    pd.DataFrame([{"year": 2021, "run_dir": str(run), "total_return": 0.0, "benchmark_return": 0.02, "max_drawdown": -0.01}]).to_csv(gate / "yearly_results.csv", index=False)
    return gate


def test_same_day_selection_filter_outputs_contract_and_changes_selection(tmp_path: Path) -> None:
    gate = _make_gate(tmp_path)

    result = mod.run_pretest(gate)

    out = gate / "same_day_selection_filter_pretest_v1"
    assert result["complete"] is True
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    contract = json.loads((out / "selection_filter_contract.json").read_text(encoding="utf-8"))
    assert contract["post_run_outcome_used_for_selection"] is False
    assert "post_ret_20" in contract["forbidden_diagnostic_columns"]
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["required_artifacts_all_present"] is True
    assert complete["exact_next_open_replay"] is True
    assert complete["threshold_sweep"] is False

    changes = pd.read_csv(out / "selected_candidate_changes.csv")
    assert len(changes) == 1
    assert changes.iloc[0]["baseline_selected_codes"] == 1001 or str(changes.iloc[0]["baseline_selected_codes"]) == "1001"
    assert changes.iloc[0]["same_day_selected_codes"] == 1002 or str(changes.iloc[0]["same_day_selected_codes"]) == "1002"

    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    assert audit["post_run_outcome_used_for_selection"] is False
    assert audit["same_day_alternative_future_result_used_for_selection"] is False

    decision = json.loads((out / "next_axis_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in mod.DECISIONS
    assert decision["decision_count"] == 1
