from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest


def test_simulate_policy_prefers_better_exit_threshold() -> None:
    import scripts.tradex_actual_trade_short_exit_size_control_grid_v1 as mod

    path = [
        {
            "path_day_index": 0,
            "entry_price": "100.0",
            "close": "100.0",
            "actual_exit_price": "120.0",
            "quantity": "10",
            "gross_pnl_actual": "-200.0",
            "holding_days_actual": "30",
            "actual_exit_date": "2020-01-31",
            "entry_date": "2020-01-01",
            "symbol": "1111",
        },
        {
            "path_day_index": 25,
            "close": "90.0",
            "actual_exit_price": "120.0",
            "quantity": "10",
            "gross_pnl_actual": "-200.0",
            "holding_days_actual": "30",
            "actual_exit_date": "2020-01-31",
            "entry_date": "2020-01-01",
            "symbol": "1111",
        },
        {
            "path_day_index": 27,
            "close": "80.0",
            "actual_exit_price": "120.0",
            "quantity": "10",
            "gross_pnl_actual": "-200.0",
            "holding_days_actual": "30",
            "actual_exit_date": "2020-01-31",
            "entry_date": "2020-01-01",
            "symbol": "1111",
        },
    ]
    policy_25 = mod.simulate_policy(policy_id="exit_25_close", path=path, final_exit_day=25)
    policy_27 = mod.simulate_policy(policy_id="exit_27_close", path=path, final_exit_day=27)
    assert policy_27["sim_gross_pnl"] > policy_25["sim_gross_pnl"]
    assert policy_25["actions_json"]


def test_simulate_policy_profit_take_triggers() -> None:
    import scripts.tradex_actual_trade_short_exit_size_control_grid_v1 as mod

    path = [
        {
            "path_day_index": 0,
            "entry_price": "100.0",
            "close": "100.0",
            "actual_exit_price": "90.0",
            "quantity": "10",
            "gross_pnl_actual": "100.0",
            "holding_days_actual": "3",
            "actual_exit_date": "2020-01-31",
            "entry_date": "2020-01-01",
            "symbol": "1111",
        },
        {
            "path_day_index": 1,
            "close": "96.0",
            "actual_exit_price": "90.0",
            "quantity": "10",
            "gross_pnl_actual": "100.0",
            "holding_days_actual": "3",
            "actual_exit_date": "2020-01-31",
            "entry_date": "2020-01-01",
            "symbol": "1111",
        },
        {
            "path_day_index": 2,
            "close": "90.0",
            "actual_exit_price": "90.0",
            "quantity": "10",
            "gross_pnl_actual": "100.0",
            "holding_days_actual": "3",
            "actual_exit_date": "2020-01-31",
            "entry_date": "2020-01-01",
            "symbol": "1111",
        },
    ]
    policy = mod.simulate_policy(policy_id="takeprofit_0p04_close", path=path, take_profit_return_pct=0.04)
    assert "take_profit" in policy["actions_json"]
    assert policy["final_exit_day"] == 1
    assert policy["sim_gross_pnl"] == 40.0


def test_run_smoke_with_frozen_subset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import scripts.tradex_actual_trade_short_exit_size_control_grid_v1 as mod

    contract = {
        "approved_for_subset_replay": True,
        "included_trade_count": 2,
        "excluded_trade_ids": [],
        "excluded_missing_trade_count": 0,
        "excluded_reason_counts": {},
        "available_vs_missing_bias_summary": {"bias_classification_after_repair": "missing_paths_low_bias"},
    }
    kept = [
        {
            "normalized_trade_id": "T1",
            "side": "short",
            "counterfactual_action": "keep",
            "tainted_excluded_flag": "false",
            "symbol": "1111",
            "entry_date": "2020-01-01",
            "exit_date": "2020-01-04",
            "entry_price": "100.0",
            "exit_price": "120.0",
            "gross_pnl": "-200.0",
            "quantity": "10",
            "holding_days": "3",
        },
        {
            "normalized_trade_id": "T2",
            "side": "short",
            "counterfactual_action": "keep",
            "tainted_excluded_flag": "false",
            "symbol": "2222",
            "entry_date": "2020-01-01",
            "exit_date": "2020-01-31",
            "entry_price": "100.0",
            "exit_price": "120.0",
            "gross_pnl": "-200.0",
            "quantity": "10",
            "holding_days": "30",
        },
    ]
    path_rows = [
        {
            "normalized_trade_id": "T1",
            "symbol": "1111",
            "path_day_index": "0",
            "entry_price": "100.0",
            "close": "100.0",
            "open": "101.0",
            "actual_exit_price": "120.0",
            "quantity": "10",
            "gross_pnl_actual": "-200.0",
            "holding_days_actual": "3",
            "entry_date": "2020-01-01",
            "actual_exit_date": "2020-01-04",
        },
        {
            "normalized_trade_id": "T2",
            "symbol": "2222",
            "path_day_index": "0",
            "entry_price": "100.0",
            "close": "100.0",
            "open": "101.0",
            "actual_exit_price": "120.0",
            "quantity": "10",
            "gross_pnl_actual": "-200.0",
            "holding_days_actual": "30",
            "entry_date": "2020-01-01",
            "actual_exit_date": "2020-01-31",
        },
        {
            "normalized_trade_id": "T2",
            "symbol": "2222",
            "path_day_index": "25",
            "entry_price": "100.0",
            "close": "85.0",
            "open": "86.0",
            "actual_exit_price": "120.0",
            "quantity": "10",
            "gross_pnl_actual": "-200.0",
            "holding_days_actual": "30",
            "entry_date": "2020-01-01",
            "actual_exit_date": "2020-01-31",
        },
        {
            "normalized_trade_id": "T2",
            "symbol": "2222",
            "path_day_index": "27",
            "entry_price": "100.0",
            "close": "80.0",
            "open": "81.0",
            "actual_exit_price": "120.0",
            "quantity": "10",
            "gross_pnl_actual": "-200.0",
            "holding_days_actual": "30",
            "entry_date": "2020-01-01",
            "actual_exit_date": "2020-01-31",
        },
    ]

    runtime_status = {"freshness_state": "fresh"}
    freshness = {"freshness_state": "fresh", "current_candidate_available": True}

    monkeypatch.setattr(mod, "OUT_BASE", tmp_path)
    monkeypatch.setattr(mod, "APPROVED_SUBSET_ROOT", tmp_path)
    monkeypatch.setattr(mod, "FEASIBILITY_ROOT", tmp_path)
    monkeypatch.setattr(mod, "COUNTERFACTUAL_ROOT", tmp_path)
    monkeypatch.setattr(mod, "SEGMENATION_ROOT", tmp_path)
    monkeypatch.setattr(mod, "load_inputs", lambda: (kept, contract, {"pass": True}, {"decision": "test_holding_duration_exit_rule_next"}, {"rows": path_rows}))
    monkeypatch.setattr(mod, "get_runtime_stock_db_status", lambda: dict(runtime_status), raising=False)
    monkeypatch.setattr(mod, "get_rankings_freshness", lambda **_kwargs: dict(freshness), raising=False)

    mod.main()

    out_dirs = sorted(tmp_path.glob(f"*-{mod.AXIS_ID}"))
    assert out_dirs, "run root not created"
    out_dir = out_dirs[-1]
    for name in mod.REQUIRED_OUTPUTS:
        assert (out_dir / name).exists(), name
    decision = json.loads((out_dir / "short_exit_size_control_decision.json").read_text(encoding="utf-8"))
    assert decision["no_lookahead_pass"] is True
    assert decision["production_candidate"] is False


def test_policy_grid_names_are_stable() -> None:
    import scripts.tradex_actual_trade_short_exit_size_control_grid_v1 as mod

    assert mod.AXIS_ID == "tradex_actual_trade_short_exit_size_control_grid_v1"
    assert "short_exit_size_control_decision.json" in mod.REQUIRED_OUTPUTS
