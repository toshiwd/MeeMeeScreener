from __future__ import annotations

import pandas as pd

from scripts import tradex_decisive_trigger_higher_timeframe_state_v1 as mod


def test_pit_weekly_average_uses_current_close_and_prior_completed_weeks() -> None:
    dates = pd.bdate_range("2024-01-01", periods=40)
    group = pd.DataFrame({"code": "1000", "trade_date": dates.strftime("%Y-%m-%d"), "c": range(100, 140), "source": "pan"})
    enriched = mod.add_pit_higher_timeframe(group)
    row = enriched.iloc[-1]
    current_week = dates[-1].to_period("W-FRI")
    prior_closes = group.assign(period=dates.to_period("W-FRI").astype(str)).groupby("period")["c"].last()
    prior_closes.index = pd.PeriodIndex(prior_closes.index, freq="W-FRI")
    expected = (prior_closes[prior_closes.index < current_week].tail(6).sum() + row["c"]) / 7
    assert row["wma7_pit"] == expected


def test_state_table_classifies_strong_and_pullback() -> None:
    states = pd.DataFrame(
        {
            "code": ["1", "2"],
            "trade_date": ["2025-01-01", "2025-01-01"],
            "c": [120.0, 105.0],
            "mma20_pit": [100.0, 100.0],
            "wma7_pit": [110.0, 110.0],
            "wma20_pit": [105.0, 100.0],
        }
    )
    known = states[["mma20_pit", "wma7_pit", "wma20_pit"]].notna().all(axis=1)
    monthly_up = states.c > states.mma20_pit
    strong = known & monthly_up & (states.c > states.wma7_pit) & (states.wma7_pit > states.wma20_pit)
    pullback = known & monthly_up & ~(states.c > states.wma7_pit) & (states.c > states.wma20_pit)
    assert strong.tolist() == [True, False]
    assert pullback.tolist() == [False, True]
