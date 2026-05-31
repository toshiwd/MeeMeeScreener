from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_entry_family_split_v1 as mod


def test_assign_family_prioritizes_overextension_risk() -> None:
    row = pd.Series(
        {
            "dist_ma20_top_quartile": True,
            "dist_ma60_top_quartile": False,
            "ma7_slope_top_quartile": False,
            "upper_wick_ratio": 0.1,
            "failed_high_update": False,
            "large_bearish_candle": False,
            "dist_ma7_pct": 0.0,
            "dist_ma20_pct": 0.0,
            "above20_streak": 10,
            "above60_streak": 80,
            "ma20_slope": 0.01,
            "ma60_slope": 0.01,
        }
    )

    family, tags, _, _ = mod.assign_family_row(row)

    assert family == "overextension_risk_family"
    assert "overextension_context" in tags


def test_assign_family_detects_pullback_reclaim() -> None:
    row = pd.Series(
        {
            "dist_ma20_top_quartile": False,
            "dist_ma60_top_quartile": False,
            "ma7_slope_top_quartile": False,
            "upper_wick_ratio": 0.1,
            "failed_high_update": False,
            "large_bearish_candle": False,
            "dist_ma7_pct": 0.01,
            "dist_ma20_pct": 0.03,
            "above20_streak": 12,
            "days_since_ma20_reclaim": 8,
            "ma20_slope": 0.01,
            "ma60_slope": 0.0,
        }
    )

    family, tags, _, _ = mod.assign_family_row(row)

    assert family == "pullback_reclaim_family"
    assert "near_ma_pullback_context" in tags


def test_quality_summary_calculates_family_metrics() -> None:
    rows = pd.DataFrame(
        [
            {"year": 2024, "path20_available": True, "primary_family": "pullback_reclaim_family", "ret20": 0.1, "starter_good": True, "starter_bad": False, "selected_loser": False, "selected_winner": True, "immediate_adverse_entry": False, "mae20": -0.01, "mfe20": 0.12},
            {"year": 2024, "path20_available": True, "primary_family": "pullback_reclaim_family", "ret20": -0.1, "starter_good": False, "starter_bad": True, "selected_loser": True, "selected_winner": False, "immediate_adverse_entry": True, "mae20": -0.12, "mfe20": 0.01},
        ]
    )

    out = mod.quality_summary(rows)
    row = out[(out["period"] == "2024") & (out["primary_family"] == "pullback_reclaim_family")].iloc[0]

    assert row["n"] == 2
    assert row["starter_good_rate"] == 0.5
    assert row["starter_bad_rate"] == 0.5


def test_decide_allows_family_pretest_for_large_better_family() -> None:
    score = pd.DataFrame(
        [
            {
                "primary_family": "pullback_reclaim_family",
                "sample_size": 2000,
                "starter_good_minus_bad_spread": -0.2,
            }
        ]
    )

    decision = mod.decide(score)

    assert decision["research_decision"] == "starter_specific_family_pretest_allowed"
    assert decision["meemee_reflectable_candidate"] is False
