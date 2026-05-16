from __future__ import annotations

from pathlib import Path

from scripts import portfolio_agent_replay_c1506_fixed_v1 as mod
from scripts import portfolio_agent_replay_v1 as replay


def test_c1506_wrapper_applies_fixed_exit_rules_and_restores_globals(tmp_path: Path, monkeypatch) -> None:
    observed: dict[str, float | int | str] = {}
    original_profit_target = replay.PROFIT_TARGET
    original_stop_loss = replay.STOP_LOSS
    original_max_holding = replay.MAX_HOLDING_TRADING_DAYS

    def fake_run_portfolio_agent_replay_v1(**kwargs):
        observed["profit_target"] = replay.PROFIT_TARGET
        observed["stop_loss"] = replay.STOP_LOSS
        observed["max_holding"] = replay.MAX_HOLDING_TRADING_DAYS
        out = tmp_path / "out"
        out.mkdir()
        return {
            "output_dir": str(out),
            "final_equity": 1_000_000.0,
            "total_return": -0.9,
            "no_lookahead_audit": "pass",
            "accounting_reconciliation": "pass",
            "next_open_execution": "pass",
        }

    monkeypatch.setattr(replay, "run_portfolio_agent_replay_v1", fake_run_portfolio_agent_replay_v1)

    result = mod.run_c1506_fixed_replay(output_root=tmp_path, run_id="test", start_ymd=20210101, end_ymd=20220101)

    assert Path(result["c1506_fixed_summary"]).exists()
    assert observed["profit_target"] == float("inf")
    assert observed["stop_loss"] == -0.07
    assert observed["max_holding"] == 30
    assert replay.PROFIT_TARGET == original_profit_target
    assert replay.STOP_LOSS == original_stop_loss
    assert replay.MAX_HOLDING_TRADING_DAYS == original_max_holding
