from __future__ import annotations

import pandas as pd

from scripts import tradex_forward_current_candidate_surface_v1 as mod


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": 20260422,
                "code": "1001",
                "available_at": "2026-04-22 09:00:00+09:00",
                "source_presence_flag": "{}",
                "close": 100,
                "ma7": 99,
                "ma20": 95,
                "ma60": 90,
                "atr14_pct": 0.02,
                "gap_pct": 0.01,
                "vol_ratio5_20": 1.2,
                "turnover20": 100000,
                "turnover_z20": 1.0,
                "high20_dist": -0.02,
                "low20_dist": 0.20,
                "candle_body_ratio": 0.8,
                "candle_upper_wick_ratio": 0.1,
                "candle_lower_wick_ratio": 0.1,
                "diff20_pct": 0.05,
                "cnt_20_above": 10,
                "cnt_7_above": 5,
                "weekly_breakout_up_prob": 0.8,
                "monthly_breakout_up_prob": 0.7,
                "monthly_range_prob": 0.6,
            },
            {
                "as_of_date": 20260422,
                "code": "1002",
                "available_at": "2026-04-22 09:00:00+09:00",
                "source_presence_flag": "{}",
                "close": 100,
                "ma7": 100,
                "ma20": 100,
                "ma60": 100,
                "atr14_pct": 0.08,
                "gap_pct": -0.01,
                "vol_ratio5_20": 0.7,
                "turnover20": 1000,
                "turnover_z20": -1.0,
                "high20_dist": -0.10,
                "low20_dist": 0.05,
                "candle_body_ratio": 0.1,
                "candle_upper_wick_ratio": 0.9,
                "candle_lower_wick_ratio": 0.0,
                "diff20_pct": 0.0,
                "cnt_20_above": 1,
                "cnt_7_above": 1,
                "weekly_breakout_up_prob": 0.2,
                "monthly_breakout_up_prob": 0.1,
                "monthly_range_prob": 0.2,
            },
        ]
    )


def test_build_surface_adds_watch_rank_without_outcomes() -> None:
    surface = mod.build_surface(_frame())
    assert "forward_research_watch_rank" in surface.columns
    assert surface["forward_surface_live_feature_available_flag"].all()
    assert surface.sort_values("forward_research_watch_rank").iloc[0]["code"] == "1001"


def test_no_lookahead_checks_available_at() -> None:
    assert mod.no_lookahead(_frame())["no_lookahead_pass"] is True
    bad = _frame()
    bad.loc[0, "available_at"] = "2026-04-23 09:00:00+09:00"
    assert mod.no_lookahead(bad)["no_lookahead_pass"] is False


def test_decide_ready_for_research_watch_not_buy() -> None:
    surface = mod.build_surface(_frame())
    decision, cls, reasons = mod.decide(surface, {"no_lookahead_pass": True})
    assert decision == "forward_current_surface_ready_for_research_watch_pretest"
    assert cls == "HOLD_UNDERPOWERED"
    assert reasons


def test_feature_contract_forbids_future_outcomes() -> None:
    contract = mod.feature_contract()
    assert contract["fields"]["ret20"]["classification"] == "forbidden_future_leak"
    assert contract["fields"]["forward_research_watch_score"]["classification"] == "point_in_time_feature"
