from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_entry_failed_breakout_avoidance_v1 as mod


def test_score_axis_demotes_failed_breakout_zone() -> None:
    rows = pd.DataFrame(
        [
            {"decision_date": 20240101, "code": "A", "baseline_rank": 1, "baseline_score": 10, "monthly_high_zone_proxy": True, "monthly_box_breakout_proxy": False, "failed_high_update": True, "large_bearish_candle": False, "upper_wick_ratio": 0.1, "volume_ma20_ratio": 1.0, "large_bullish_candle": False},
            {"decision_date": 20240101, "code": "B", "baseline_rank": 2, "baseline_score": 9, "monthly_high_zone_proxy": True, "monthly_box_breakout_proxy": False, "failed_high_update": False, "large_bearish_candle": False, "upper_wick_ratio": 0.0, "volume_ma20_ratio": 1.0, "large_bullish_candle": True},
        ]
    )
    scored = mod.score_axis(rows)
    assert scored.loc[scored["code"] == "A", "failed_breakout_action"].iloc[0] == "demote_failed_breakout_high_reject"
    assert int(scored.loc[scored["code"] == "B", "failed_breakout_rank"].iloc[0]) == 1


def test_decide_keep_gate() -> None:
    comp = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.01, "delta_bad_pick_rate": -0.01, "delta_severe_loss_rate": 0.0}])
    repl = {"rows": [{"period": "2024_2026_combined", "topk": 10, "replacement_delta_ret20": 0.02}]}
    assert mod.decide(comp, repl, {"changed_top10_members_count": 20})["research_decision"] == "keep_for_next_stage"


def test_decide_closes_when_boundary_does_not_move() -> None:
    comp = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.0, "delta_bad_pick_rate": 0.0, "delta_severe_loss_rate": 0.0}])
    repl = {"rows": [{"period": "2024_2026_combined", "topk": 10, "replacement_delta_ret20": 0.0}]}
    assert mod.decide(comp, repl, {"changed_top10_members_count": 0})["research_decision"] == "close_branch_no_reusable_signal"
