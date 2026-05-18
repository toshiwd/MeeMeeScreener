from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest


def _load_module():
    import scripts.tradex_actual_trade_short_exit_execution_convention_compare_v1 as mod

    return mod


def _sample_path_rows() -> list[dict[str, object]]:
    return [
        {
            "path_day_index": 0,
            "open": 100.0,
            "close": 100.0,
            "entry_price": 100.0,
            "actual_exit_price": 90.0,
            "quantity": 100.0,
            "gross_pnl_actual": 1000.0,
            "holding_days_actual": 2,
            "symbol": "1111",
            "entry_date": "2026-01-05",
            "actual_exit_date": "2026-01-07",
        },
        {
            "path_day_index": 1,
            "open": 95.5,
            "close": 97.0,
            "entry_price": 100.0,
            "actual_exit_price": 90.0,
            "quantity": 100.0,
            "gross_pnl_actual": 1000.0,
            "holding_days_actual": 2,
            "symbol": "1111",
            "entry_date": "2026-01-05",
            "actual_exit_date": "2026-01-07",
        },
        {
            "path_day_index": 2,
            "open": 93.0,
            "close": 96.0,
            "entry_price": 100.0,
            "actual_exit_price": 90.0,
            "quantity": 100.0,
            "gross_pnl_actual": 1000.0,
            "holding_days_actual": 2,
            "symbol": "1111",
            "entry_date": "2026-01-05",
            "actual_exit_date": "2026-01-07",
        },
    ]


def test_next_open_policy_uses_next_bar_open_and_records_fallback():
    mod = _load_module()
    row = mod.simulate_next_open_policy(
        policy_id="takeprofit_0p025_close",
        path=_sample_path_rows(),
        take_profit_return_pct=0.025,
    )
    assert row["execution_convention"] == "next_session_open"
    assert row["decision_day_index"] == 1
    assert row["fill_day_index"] == 2
    assert row["fallback_count"] == 0
    assert row["sim_gross_pnl"] == pytest.approx((100.0 - 93.0) * 100.0)
    assert row["sim_return_pct"] == pytest.approx(((100.0 - 93.0) * 100.0) / 10000.0)

    last_bar_row = mod.simulate_next_open_policy(
        policy_id="takeprofit_0p025_close",
        path=[
            {
                "path_day_index": 0,
                "open": 100.0,
                "close": 100.0,
                "entry_price": 100.0,
                "actual_exit_price": 90.0,
                "quantity": 100.0,
                "gross_pnl_actual": 1000.0,
                "holding_days_actual": 2,
                "symbol": "1111",
                "entry_date": "2026-01-05",
                "actual_exit_date": "2026-01-07",
            },
            {
                "path_day_index": 1,
                "open": 95.5,
                "close": 97.0,
                "entry_price": 100.0,
                "actual_exit_price": 90.0,
                "quantity": 100.0,
                "gross_pnl_actual": 1000.0,
                "holding_days_actual": 2,
                "symbol": "1111",
                "entry_date": "2026-01-05",
                "actual_exit_date": "2026-01-07",
            }
        ],
        take_profit_return_pct=0.025,
    )
    assert last_bar_row["fallback_count"] == 1
    assert last_bar_row["fill_day_index"] == 1


def test_real_run_keeps_shadow_paper_replay_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "OUT_BASE", tmp_path)
    result = mod.run()
    decision = result["decision"]
    assert decision["decision"] == "keep_for_shadow_paper_replay"
    assert decision["best_execution_convention"] == "next_session_open"
    assert decision["paper_replay_ready"] is True
    assert decision["production_candidate"] is False
    assert decision["no_lookahead_pass"] is True
    assert decision["best_policy_id"] in {"takeprofit_0p025_close", "takeprofit_0p03_close", "takeprofit_0p0325_close", "takeprofit_0p035_close", "takeprofit_0p0375_close"}
    out_root = Path(result["run_root"])
    assert (out_root / "short_exit_execution_convention_decision.json").exists()
    payload = json.loads((out_root / "short_exit_execution_convention_decision.json").read_text(encoding="utf-8"))
    assert payload["decision"] == "keep_for_shadow_paper_replay"
    assert payload["best_execution_convention"] == "next_session_open"
    assert payload["next_gate"] == "paper_execution_replay"


def test_required_artifact_names_are_stable():
    mod = _load_module()
    assert "short_exit_execution_convention_contract.json" in mod.REQUIRED_OUTPUTS
    assert "short_exit_execution_convention_decision.json" in mod.REQUIRED_OUTPUTS
    assert mod.AXIS_ID == "tradex_actual_trade_short_exit_execution_convention_compare_v1"
