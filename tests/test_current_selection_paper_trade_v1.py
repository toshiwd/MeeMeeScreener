from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import current_selection_paper_trade_v1 as paper


def test_paper_orders_use_next_open_policy_and_lot_sizing() -> None:
    candidates = [
        {"rank": 1, "code": "1111", "name": "A", "close": 1000, "trade_priority_score": 0.9, "entry_score": 0.8, "prob_side": 0.7, "setup_type": "breakout", "trade_entry_class": "box", "market_regime": "risk_on", "market_risk_off": False, "decision_reasons": "r1", "risk_watch": ""},
        {"rank": 2, "code": "2222", "name": "B", "close": 2000, "trade_priority_score": 0.8, "entry_score": 0.7, "prob_side": 0.6, "setup_type": "pullback", "trade_entry_class": "ma", "market_regime": "risk_on", "market_risk_off": False, "decision_reasons": "r2", "risk_watch": ""},
    ]

    orders, sizing = paper._paper_orders(
        candidates,
        initial_cash=1_000_000,
        max_positions=2,
        lot_size=100,
        one_way_cost_bps=30,
        signal_date="2026-05-14",
        planned_execution_date="2026-05-15",
    )

    assert [order["code"] for order in orders] == ["1111", "2222"]
    assert orders[0]["quantity"] == 500
    assert orders[1]["quantity"] == 200
    assert orders[0]["execution_policy"] == "next_session_open_planned"
    assert orders[0]["paper_status"] == "planned_not_submitted"
    assert sizing["all_orders_planned"] is True


def test_current_selection_paper_trade_writes_required_artifacts_with_injected_services(tmp_path: Path, monkeypatch) -> None:
    runtime_payload = {
        "freshness_state": "fresh",
        "stale": False,
        "selected_runtime_db_path": str(tmp_path / "stocks.duckdb"),
        "latest_available_global_date_iso": "2026-05-14",
    }
    db_path = Path(runtime_payload["selected_runtime_db_path"])
    db_path.write_bytes(b"db")
    freshness_payload = {
        "freshness_state": "fresh",
        "stale": False,
        "snapshot_as_of": "2026-05-14",
    }
    ranking_payload = {
        "snapshot_as_of": "2026-05-14",
        "items": [
            {
                "code": "1111",
                "name": "A",
                "asOf": "2026-05-14",
                "close": 1000,
                "tradePriorityScore": 0.9,
                "entryScore": 0.8,
                "probSide": 0.7,
                "entryQualified": True,
                "tradeDecisionReasons": ["reason"],
            }
        ],
    }

    class Bridge:
        @staticmethod
        def get_runtime_stock_db_status() -> dict:
            return dict(runtime_payload)

        @staticmethod
        def get_rankings_freshness(**_kwargs) -> dict:
            return dict(freshness_payload)

    class Cache:
        @staticmethod
        def get_rankings(*_args, **_kwargs) -> dict:
            return dict(ranking_payload)

    monkeypatch.setitem(__import__("sys").modules, "app.backend.services.codex_bridge_service", Bridge)
    monkeypatch.setitem(__import__("sys").modules, "app.backend.services.ml.rankings_cache", Cache)

    result = paper.run_current_selection_paper_trade_v1(
        output_parent=tmp_path / "out",
        run_id="paper-run",
        initial_cash=1_000_000,
        max_positions=3,
        lot_size=100,
        limit=20,
    )
    output = Path(result["output_root"])
    complete = json.loads((output / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    summary = json.loads((output / "paper_trade_summary.json").read_text(encoding="utf-8"))
    orders = pd.read_csv(output / "paper_trade_orders.csv")

    assert complete["complete"] is True
    assert complete["required_artifacts_all_present"] is True
    assert complete["broker_api_called"] is False
    assert summary["actual_orders_submitted"] is False
    assert summary["same_day_close_fill"] is False
    assert orders.iloc[0]["paper_status"] == "planned_not_submitted"
