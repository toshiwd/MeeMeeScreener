from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_entry_volatility_extension_demotion_v1 as mod


def _base_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "decision_date": 20240101,
                "code": "A",
                "year": 2024,
                "baseline_rank": 1,
                "baseline_score": 10,
                "dist_ma20_top_quartile": True,
                "dist_ma60_top_quartile": True,
                "realized_vol20_top_quartile": True,
                "atr14_pct": 0.03,
                "upper_wick_ratio": 0.1,
                "path20_available": True,
                "ret20": -0.08,
                "mae20": -0.1,
                "mfe20": 0.01,
                "starter_good": False,
                "starter_bad": True,
                "selected_loser": True,
                "selected_winner": False,
                "immediate_adverse_entry": True,
                "same_date_ret20_rank_pct": 0.1,
            },
            {
                "decision_date": 20240101,
                "code": "B",
                "year": 2024,
                "baseline_rank": 2,
                "baseline_score": 9,
                "dist_ma20_top_quartile": False,
                "dist_ma60_top_quartile": False,
                "realized_vol20_top_quartile": False,
                "atr14_pct": 0.02,
                "upper_wick_ratio": 0.0,
                "path20_available": True,
                "ret20": 0.07,
                "mae20": -0.01,
                "mfe20": 0.09,
                "starter_good": True,
                "starter_bad": False,
                "selected_loser": False,
                "selected_winner": True,
                "immediate_adverse_entry": False,
                "same_date_ret20_rank_pct": 0.9,
            },
        ]
    )


def test_score_axis_demotes_volatility_extension_risk() -> None:
    scored = mod.score_axis(_base_rows())
    assert bool(scored.loc[scored["code"] == "A", "volatility_extension_risk_flag"].iloc[0])
    assert int(scored.loc[scored["code"] == "A", "volatility_extension_rank"].iloc[0]) == 2
    assert int(scored.loc[scored["code"] == "B", "volatility_extension_rank"].iloc[0]) == 1


def test_comparison_summary_uses_outcomes_only_after_ranking() -> None:
    scored = mod.score_axis(_base_rows())
    comp = mod.comparison_summary(scored)
    row = comp[(comp["period"] == "2024") & (comp["topk"] == 5)].iloc[0]
    assert row["baseline_n"] == 2
    assert row["challenger_n"] == 2
    assert row["delta_mean_ret20"] == 0


def test_decide_closes_when_boundary_does_not_move() -> None:
    comp = pd.DataFrame(
        [{"period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.0, "delta_starter_bad_rate": 0.0, "delta_selected_loser_rate": 0.0}]
    )
    repl = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "added_minus_removed_ret20": 0.0}])
    decision = mod.decide(comp, repl, {"changed_top10_members_count": 0})
    assert decision["research_decision"] == "drop"
    assert decision["meemee_reflectable_candidate"] is False


def test_decide_closes_when_bad_rate_improves_but_replacements_are_negative() -> None:
    comp = pd.DataFrame(
        [
            {
                "period": "2024_2026_combined",
                "topk": 10,
                "delta_mean_ret20": -0.002,
                "delta_starter_bad_rate": -0.03,
                "delta_selected_loser_rate": -0.02,
            }
        ]
    )
    repl = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "added_minus_removed_ret20": -0.01}])
    decision = mod.decide(comp, repl, {"changed_top10_members_count": 20})
    assert decision["research_decision"] == "close_branch_no_reusable_signal"
