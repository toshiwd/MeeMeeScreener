from __future__ import annotations

import pandas as pd

from scripts import tradex_family_surface_freshness_repair_v1 as mod


def test_build_current_surface_rows_assigns_family_without_labels() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_date": 20260508,
                "code": "A",
                "baseline_score": 10,
                "dist_ma20_top_quartile": False,
                "dist_ma60_top_quartile": False,
                "ma7_slope_top_quartile": False,
                "upper_wick_ratio": 0.1,
                "failed_high_update": False,
                "large_bearish_candle": False,
                "dist_ma7_pct": 0.01,
                "dist_ma20_pct": 0.02,
                "above20_streak": 10,
                "above60_streak": 5,
                "days_since_ma20_reclaim": 3,
                "days_since_ma60_reclaim": 50,
                "ma20_slope": 0.01,
                "ma60_slope": 0.0,
                "monthly_high_zone_proxy": False,
                "monthly_box_breakout_proxy": False,
                "monthly_box_inside_proxy": False,
            }
        ]
    )

    out = mod.build_current_surface_rows(rows)

    assert out.loc[0, "research_candidate_source_family"] == "pullback_reclaim_source"
    assert bool(out.loc[0, "labels_required_for_current_review"]) is False
    assert bool(out.loc[0, "current_review_no_lookahead_mode"]) is True


def test_build_current_surface_rows_marks_future_labels_unavailable() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_date": 20260508,
                "code": "B",
                "baseline_score": 9,
                "dist_ma20_top_quartile": False,
                "dist_ma60_top_quartile": False,
                "ma7_slope_top_quartile": False,
                "upper_wick_ratio": 0.1,
                "failed_high_update": False,
                "large_bearish_candle": False,
                "dist_ma7_pct": 0.2,
                "dist_ma20_pct": 0.2,
                "above20_streak": 1,
                "above60_streak": 1,
                "days_since_ma20_reclaim": 99,
                "days_since_ma60_reclaim": 99,
                "ma20_slope": 0.0,
                "ma60_slope": 0.0,
                "monthly_high_zone_proxy": False,
                "monthly_box_breakout_proxy": False,
                "monthly_box_inside_proxy": True,
            }
        ]
    )

    out = mod.build_current_surface_rows(rows)

    assert bool(out.loc[0, "future_label_fields_available"]) is False
    assert out.loc[0, "research_candidate_source_family"] == "range_reversal_source"
