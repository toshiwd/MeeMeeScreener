from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_origin5541_dynamic_hedge_replay_v1 as mod


def test_build_compare_reproduces_expected_policy_tradeoff() -> None:
    dates = pd.bdate_range("2025-09-19", "2025-12-26")
    prices = pd.DataFrame(
        {
            "trade_date": dates.strftime("%Y-%m-%d"),
            "o": range(1000, 1000 + len(dates)),
            "h": range(1001, 1001 + len(dates)),
            "l": range(999, 999 + len(dates)),
            "c": range(1000, 1000 + len(dates)),
            "v": [100] * len(dates),
            "source": ["pan"] * len(dates),
        }
    )
    compare, curve = mod.build_compare(prices)
    assert set(compare["authoritative_results"]) == {
        "article_dynamic_hedge",
        "no_hedge",
        "fixed_50pct_hedge",
        "risk_equivalent_long_reduction",
    }
    assert len(curve) == len(prices) * 4
    assert compare["fixed_conditions"]["selection_logic_changed"] is False
    assert compare["observed_branching"]["article_vs_no_hedge_changed_position_dates"] > 0
    assert compare["capacity_retention_analysis"]["zero_cost_net_exposure_equivalence_confirmed"] is True
    assert compare["capacity_retention_analysis"]["breakout_preserved_core_long_units"] == 6
    assert compare["capacity_retention_analysis"]["breakout_article_long_units"] == 27
    assert compare["capacity_retention_analysis"]["breakout_risk_equivalent_long_units"] == 21
    assert compare["judgment"]["authoritative_rollup_decision"].startswith("hold_review_only")


def test_run_writes_complete_authoritative_artifact(tmp_path: Path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source VARCHAR)")
        dates = pd.bdate_range("2025-09-19", "2025-12-26")
        rows = [
            ("5541", int(date.timestamp()), 2000.0, 2010.0, 1990.0, 2000.0 + index, 100, "pan")
            for index, date in enumerate(dates)
        ]
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

    output = tmp_path / "output"
    result = mod.run(db_path, output)
    compare = json.loads((output / "compare.json").read_text(encoding="utf-8"))
    complete = json.loads((output / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert result["output"] == str(output.resolve())
    assert compare["review_only"] is True
    assert compare["not_changed"] == ["MeeMee", "ranking", "runtime DB", "production trading logic", "candidate selection"]
    assert complete["complete"] is True
    assert complete["authoritative"] == "compare.json"
    assert (output / "policy_equity_curve.csv").exists()
    assert (output / "article_position_schedule.csv").exists()
