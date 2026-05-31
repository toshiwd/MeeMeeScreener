from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_entry_daily_timing_quality_v1 as mod


def test_score_axis_demotes_candle_reject_and_boosts_constructive_pullback() -> None:
    rows = pd.DataFrame(
        [
            {"decision_date": 20240101, "code": "A", "baseline_rank": 1, "baseline_score": 10, "days_since_ma20_reclaim": 1, "ma7_slope": -0.01, "failed_high_update": False, "large_bearish_candle": False, "upper_wick_ratio": 0.1, "monthly_box_inside_proxy": False, "weekly_monthly_uptrend_proxy": False, "dist_ma20_pct": 0.1, "lower_wick_ratio": 0.0, "volume_ma20_ratio": 1.0},
            {"decision_date": 20240101, "code": "B", "baseline_rank": 2, "baseline_score": 9, "days_since_ma20_reclaim": 10, "ma7_slope": 0.01, "failed_high_update": False, "large_bearish_candle": False, "upper_wick_ratio": 0.1, "monthly_box_inside_proxy": True, "weekly_monthly_uptrend_proxy": True, "dist_ma20_pct": 0.0, "lower_wick_ratio": 0.3, "volume_ma20_ratio": 1.0},
        ]
    )
    scored = mod.score_axis(rows)
    assert scored.loc[scored["code"] == "A", "daily_timing_action"].iloc[0] == "demote_weak_reclaim_or_candle_reject"
    assert scored.loc[scored["code"] == "B", "daily_timing_action"].iloc[0] == "boost_constructive_pullback"
    assert int(scored.loc[scored["code"] == "B", "daily_timing_rank"].iloc[0]) == 1


def test_decide_keeps_when_full_gates_pass() -> None:
    comp = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.01, "delta_bad_pick_rate": -0.01, "delta_severe_loss_rate": 0.0}])
    repl = {"rows": [{"period": "2024_2026_combined", "topk": 10, "replacement_delta_ret20": 0.02}]}
    decision = mod.decide(comp, repl, {"changed_top10_members_count": 20})
    assert decision["research_decision"] == "keep_for_next_stage"


def test_decide_drops_negative_replacement_quality() -> None:
    comp = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": -0.01, "delta_bad_pick_rate": -0.01, "delta_severe_loss_rate": -0.01}])
    repl = {"rows": [{"period": "2024_2026_combined", "topk": 10, "replacement_delta_ret20": -0.02}]}
    decision = mod.decide(comp, repl, {"changed_top10_members_count": 20})
    assert decision["research_decision"] == "drop"
