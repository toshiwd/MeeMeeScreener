from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import portfolio_exit_parameter_optimizer_v1 as mod


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
            ("7001", "2025-04-03", 99.0, 110.0, 98.0, 109.0, 1200.0, "pan"),
            ("7001", "2025-04-04", 110.0, 112.0, 108.0, 111.0, 1300.0, "pan"),
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
            "period": {"start_ymd": 20250401, "end_ymd": 20250404},
            "portfolio": {"initial_cash_jpy": 1_000_000.0, "per_symbol_cap_jpy": 333_333.0, "max_positions": 3},
            "cost_model": {"commission_bps": 15.0, "slippage_bps": 15.0, "tax_or_fee_bps": 0.0, "min_fee": 0.0},
            "exit_rules": {"profit_target": 0.08, "max_holding_trading_days": 20, "stop_loss": -0.06},
        },
    )
    _write_jsonl(
        root / "daily_action_ledger.jsonl",
        [{"action": "buy", "decision_ymd": 20250401, "execution_ymd": 20250402, "code": "7001", "order_id": "baseline-buy-1"}],
    )
    pd.DataFrame(
        [{"order_id": "baseline-buy-1", "decision_ymd": 20250401, "execution_ymd": 20250402, "action": "buy", "code": "7001", "order_status": "filled", "execution_price": 100.0, "shares": 3300, "notional": 330000.0, "cost_amount": 990.0, "position_id": "p1"}]
    ).to_csv(root / "orders_ledger.csv", index=False)
    pd.DataFrame(
        [
            {"ymd": 20250401, "equity": 1_000_000.0},
            {"ymd": 20250402, "equity": 999_010.0},
            {"ymd": 20250403, "equity": 1_028_710.0},
            {"ymd": 20250404, "equity": 1_035_310.0},
        ]
    ).to_csv(root / "equity_curve.csv", index=False)
    return root


def test_portfolio_exit_parameter_optimizer_artifacts(tmp_path: Path, monkeypatch) -> None:
    root = _make_run(tmp_path)
    monkeypatch.setattr(
        mod,
        "_build_grid",
        lambda: [
            mod.CandidateConfig("c0001", "none", None, None, 0.08, 20),
            mod.CandidateConfig("c0002", "full_stop", -0.07, None, None, 30),
        ],
    )

    result = mod.run_optimizer(root)

    out = root / "portfolio_exit_parameter_optimizer_v1"
    assert result["complete"] is True
    for artifact in mod.OUTPUT_ARTIFACTS:
        assert (out / artifact).exists(), artifact
    complete = json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["optimization_mode"] is True
    assert complete["in_sample_only"] is True
    assert complete["not_promotable"] is True
    audit = json.loads((out / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    assert audit["same_day_close_fill_used"] is False
    assert audit["post_run_outcomes_used_for_exit_condition"] is False
    grid = pd.read_csv(out / "optimization_grid_results.csv")
    assert len(grid) == 2
