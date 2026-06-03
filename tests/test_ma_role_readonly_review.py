from __future__ import annotations

import json
from pathlib import Path

from app.backend.services.ma_role_readonly_review import build_ma_role_review_payload
from scripts import tradex_ma_role_transition_research_phase15 as phase15


def _row(index: int, close: float) -> tuple:
    ma7 = close
    ma20 = close
    ma60 = close
    ma100 = close
    ma200 = close
    return (20260101 + index, close, close + 1.0, close - 1.0, close, 1000.0, ma7, ma20, ma60, ma100, ma200, "pan")


def test_build_payload_degrades_when_catalog_missing(tmp_path: Path) -> None:
    payload = build_ma_role_review_payload([_row(i, 100.0 + i) for i in range(25)], catalog_root=tmp_path)
    assert payload["available"] is False
    assert payload["reason"] == "catalog_missing"
    assert payload["chart_markers"] == []
    assert payload["ranking_effect"] is False


def test_build_payload_matches_catalog_rule(tmp_path: Path) -> None:
    rows = [_row(i, 100.0 + i) for i in range(25)]
    state = phase15._state(
        {
            "open": rows[-1][1],
            "high": rows[-1][2],
            "low": rows[-1][3],
            "close": rows[-1][4],
            "ma7": rows[-1][6],
            "ma20": rows[-1][7],
            "ma60": rows[-1][8],
            "ma100": rows[-1][9],
            "ma200": rows[-1][10],
            "open_prev1": rows[-2][1],
            "high_prev1": rows[-2][2],
            "low_prev1": rows[-2][3],
            "close_prev1": rows[-2][4],
            "open_prev2": rows[-3][1],
            "high_prev2": rows[-3][2],
            "low_prev2": rows[-3][3],
            "close_prev2": rows[-3][4],
            "ma7_prev5": rows[-6][6],
            "ma20_prev5": rows[-6][7],
            "ma60_prev5": rows[-6][8],
            "ma100_prev20": rows[-21][9],
            "ma200_prev20": rows[-21][10],
        }
    )
    run_root = tmp_path / "run"
    run_root.mkdir()
    (tmp_path / "latest_research_decision.json").write_text(json.dumps({"run_root": str(run_root)}), encoding="utf-8")
    (run_root / "meemee_readonly_signal_catalog.json").write_text(
        json.dumps(
            {
                "rules": [
                    {
                        "rule_id": "rule-1",
                        "display_label": "demo",
                        "entry_exit": state["entry_exit"],
                        "trend": state["trend"],
                        "environment": state["environment"],
                        "evidence": {"minimum_split_count": 200},
                        "actionability": "watch_context_not_trade_signal",
                        "meemee_display_mode": "review_only",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = build_ma_role_review_payload(rows, catalog_root=tmp_path)

    assert payload["available"] is True
    assert payload["matches"][0]["rule_id"] == "rule-1"
    assert payload["chart_markers"]
    assert payload["chart_markers"][-1]["kind"] == "ranking-up"
    assert payload["chart_markers"][-1]["label"] == "MA"
    assert payload["chart_markers"][-1]["rule_id"] == "rule-1"
    assert payload["automatic_trade_action"] is False
