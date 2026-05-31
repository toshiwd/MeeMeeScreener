from __future__ import annotations

import pandas as pd

from scripts import tradex_fresh_runtime_candidate_surface_v1 as mod


def _features() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260522, "code": "1001", "close": 120.0, "ma7": 115.0, "ma20": 110.0, "ma60": 100.0, "atr14": 2.0, "diff20_pct": 0.09, "cnt_20_above": 15, "cnt_7_above": 7, "day_count": 80},
            {"as_of_date": 20260522, "code": "1002", "close": 90.0, "ma7": 92.0, "ma20": 95.0, "ma60": 100.0, "atr14": 2.0, "diff20_pct": -0.04, "cnt_20_above": 2, "cnt_7_above": 1, "day_count": 80},
        ]
    )


def _bars() -> pd.DataFrame:
    rows = []
    for code, base in [("1001", 100.0), ("1002", 100.0)]:
        for idx in range(25):
            date = 20260498 + idx
            close = base + idx if code == "1001" else base - idx * 0.2
            rows.append({"bar_date": date, "code": code, "o": close - 1, "h": close + 2, "l": close - 2, "c": close, "v": 1000 + idx * (20 if code == "1001" else 1), "source": "pan"})
    return pd.DataFrame(rows)


def test_build_surface_creates_current_watch_rank_without_outcomes() -> None:
    surface = mod.build_surface(_features(), _bars())
    assert "fresh_runtime_research_watch_rank" in surface.columns
    assert "ret20" not in surface.columns
    assert surface["fresh_runtime_live_feature_available_flag"].all()
    assert surface.sort_values("fresh_runtime_research_watch_rank").iloc[0]["code"] == "1001"


def test_feature_contract_forbids_future_outcomes() -> None:
    contract = mod.feature_contract()
    assert contract["fields"]["ret20"]["classification"] == "forbidden_future_leak"
    assert contract["fields"]["volume_vs_20d_avg"]["classification"] == "point_in_time_feature"


def test_decide_ready_when_feature_and_bar_dates_match() -> None:
    surface = mod.build_surface(_features(), _bars())
    decision, decision_class, reasons = mod.decide(surface, 20260522, 20260522)
    assert decision == "fresh_runtime_surface_ready_for_research_watch_pretest"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert reasons


def test_decide_holds_when_feature_and_bar_dates_mismatch() -> None:
    surface = mod.build_surface(_features(), _bars())
    decision, decision_class, reasons = mod.decide(surface, 20260521, 20260522)
    assert decision == "fresh_runtime_surface_created_but_feature_bar_date_mismatch"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert "feature_snapshot_date_differs_from_latest_daily_bar_date" in reasons
