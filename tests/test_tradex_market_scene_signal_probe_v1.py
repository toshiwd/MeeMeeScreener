from __future__ import annotations

import duckdb

from scripts.tradex_market_scene_signal_probe_v1 import run_probe
from scripts.tradex_market_scene_signal_probe_v1 import _judge_groups


def test_market_scene_decision_does_not_keep_wait_none_for_short() -> None:
    groups = {
        "sideways_b_phase|wait_for_breakout_or_breakdown|none": {
            "count": 50,
            "mean_ret20": 0.05,
            "bad_loser_rate_20": 0.0,
        },
        "downtrend_a_phase|sell_rebound_rejection_or_lower_low|short": {
            "count": 50,
            "mean_ret20": 0.02,
            "bad_loser_rate_20": 0.0,
        },
    }
    decision = _judge_groups(groups, {"mean_ret20": 0.0, "bad_loser_rate_20": 0.0}, side="short")

    assert "sideways_b_phase|wait_for_breakout_or_breakdown|none" in decision["drop"]
    assert "downtrend_a_phase|sell_rebound_rejection_or_lower_low|short" in decision["keep"]


def test_market_scene_decision_drops_negative_sparse_groups() -> None:
    groups = {
        "uptrend_c_phase|hold_or_add_long|long": {
            "count": 10,
            "mean_ret20": -0.03,
            "bad_loser_rate_20": 0.4,
        }
    }
    decision = _judge_groups(groups, {"mean_ret20": 0.01, "bad_loser_rate_20": 0.03}, side="long")

    assert "uptrend_c_phase|hold_or_add_long|long" in decision["drop"]


def test_market_scene_signal_probe_writes_scene_artifacts(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE daily_bars (code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR)")
        rows = []
        for code in ["1001", "1002"]:
            price = 1000.0
            for index in range(240):
                if index < 170:
                    price += 0.3
                elif index < 210:
                    price += (index % 4 - 1.5) * 0.6
                else:
                    price += 1.2
                ymd = 20260101 + index
                rows.append((code, ymd, price, price + 5.0, price - 5.0, price, 1000, "pan"))
        con.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

    result = run_probe(db_path=db_path, output_root=tmp_path / "out", start_dt=20260270, end_dt=20260320)

    assert result["scope"]["tradex_only"] is True
    assert result["scope"]["silent_fallback_used"] is False
    assert result["meemee_reflectable"] is False
    assert result["artifacts"]["compare_json"].endswith("market_scene_signal_probe_compare.json")
    assert "long_by_scene_action_side" in result["compare"]
    assert "short_by_scene_action_side" in result["compare"]
    assert result["authoritative_rollup_decision"] in {"hold", "drop"}
