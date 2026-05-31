from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_entry_monthly_box_regime_interaction_v1 as mod


def test_score_variants_demotes_overextended_and_boosts_support() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_date": 20240101,
                "code": "A",
                "year": 2024,
                "baseline_rank": 1,
                "baseline_score": 10,
                "monthly_overextended_flag": True,
                "monthly_pullback_context_flag": False,
                "weekly_monthly_uptrend_proxy": False,
                "monthly_box_inside_proxy": False,
            },
            {
                "decision_date": 20240101,
                "code": "B",
                "year": 2024,
                "baseline_rank": 2,
                "baseline_score": 9,
                "monthly_overextended_flag": False,
                "monthly_pullback_context_flag": True,
                "weekly_monthly_uptrend_proxy": True,
                "monthly_box_inside_proxy": False,
            },
        ]
    )
    scored = mod.score_variants(rows)
    assert scored.loc[scored["code"] == "A", "monthly_box_regime_action"].iloc[0] == "demote_overextended_box_high"
    assert scored.loc[scored["code"] == "B", "monthly_box_regime_action"].iloc[0] == "boost_constructive_support"
    assert int(scored.loc[scored["code"] == "B", "monthly_box_regime_rank"].iloc[0]) == 1


def test_decide_blocks_when_monthly_contract_coverage_is_low() -> None:
    comp = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.0, "delta_bad_pick_rate": 0.0, "delta_severe_loss_rate": 0.0}])
    repl = {"rows": [{"period": "2024_2026_combined", "topk": 10, "replacement_delta_ret20": 0.0}]}
    boundary = {"changed_top10_members_count": 20}
    rows = pd.DataFrame({col: [pd.NA] for col in mod.MONTHLY_FEATURES})
    rows["year"] = [2024]
    rows["decision_date"] = [20240101]
    decision = mod.decide(comp, repl, boundary, rows)
    assert decision["research_decision"] == "blocked_missing_contract"


def test_decide_keeps_positive_same_condition_edge() -> None:
    comp = pd.DataFrame(
        [
            {
                "period": "2024_2026_combined",
                "topk": 10,
                "delta_mean_ret20": 0.01,
                "delta_bad_pick_rate": -0.01,
                "delta_severe_loss_rate": 0.0,
            }
        ]
    )
    repl = {"rows": [{"period": "2024_2026_combined", "topk": 10, "replacement_delta_ret20": 0.02}]}
    boundary = {"changed_top10_members_count": 20}
    rows = pd.DataFrame({col: [1.0] * 120 for col in mod.MONTHLY_FEATURES})
    rows["monthly_box_position_bucket"] = ["box_mid"] * 120
    rows["monthly_regime_bucket"] = ["monthly_neutral"] * 120
    rows["monthly_supportive_flag"] = [True] * 120
    rows["monthly_overextended_flag"] = [False] * 120
    rows["monthly_breakout_context_flag"] = [False] * 120
    rows["monthly_pullback_context_flag"] = [True] * 120
    rows["year"] = [2024] * 120
    rows["decision_date"] = [20240101 + i for i in range(120)]
    decision = mod.decide(comp, repl, boundary, rows)
    assert decision["research_decision"] == "keep_for_next_stage"
