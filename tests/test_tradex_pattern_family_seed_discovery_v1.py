from __future__ import annotations

import pandas as pd

from scripts import tradex_pattern_family_seed_discovery_v1 as mod


def _rows() -> pd.DataFrame:
    rows = pd.DataFrame(
        [
            {
                "code": "1001",
                "decision_date": 20250101,
                "path20_available": True,
                "ret20": 0.12,
                "ret5": 0.01,
                "primary_family": "early_trend_family",
                "days_since_ma20_reclaim": 3,
                "above20_streak": 5,
                "dist_ma7_pct": 0.02,
                "dist_ma20_pct": 0.04,
                "dist_ma60_pct": 0.10,
                "ma7_slope": 0.01,
                "ma20_slope": 0.01,
                "ma7_gt_ma20_gt_ma60": True,
                "failed_high_update": False,
                "large_bearish_candle": False,
                "large_bullish_candle": True,
                "upper_wick_ratio": 0.2,
                "lower_wick_ratio": 0.25,
                "realized_vol20": 0.02,
                "atr14_pct": 0.03,
                "monthly_box_breakout_proxy": False,
                "monthly_box_inside_proxy": True,
                "monthly_high_zone_proxy": False,
                "weekly_monthly_uptrend_proxy": True,
                "volume_ma20_ratio": 1.2,
            },
            {
                "code": "1002",
                "decision_date": 20250102,
                "path20_available": True,
                "ret20": -0.08,
                "ret5": -0.01,
                "primary_family": "pullback_reclaim_family",
                "days_since_ma20_reclaim": 12,
                "above20_streak": 4,
                "dist_ma7_pct": 0.01,
                "dist_ma20_pct": 0.01,
                "dist_ma60_pct": 0.08,
                "ma7_slope": 0.01,
                "ma20_slope": 0.01,
                "ma7_gt_ma20_gt_ma60": True,
                "failed_high_update": False,
                "large_bearish_candle": False,
                "large_bullish_candle": True,
                "upper_wick_ratio": 0.15,
                "lower_wick_ratio": 0.3,
                "realized_vol20": 0.02,
                "atr14_pct": 0.03,
                "monthly_box_breakout_proxy": False,
                "monthly_box_inside_proxy": True,
                "monthly_high_zone_proxy": False,
                "weekly_monthly_uptrend_proxy": True,
                "volume_ma20_ratio": 1.0,
            },
        ]
    )
    return mod.normalize_rows(rows)


def test_family_masks_identify_narrow_predeclared_families() -> None:
    masks = mod.family_masks(_rows())
    assert masks["family_a_early_trend_reclaim_controlled_extension"].tolist() == [True, False]
    assert masks["family_b_constructive_pullback_support_bullish_confirmation"].tolist() == [False, True]
    assert masks["family_c_weekly_monthly_supportive_daily_not_overextended"].tolist() == [True, True]
    assert masks["family_e_volatility_compressed_breakout_preparation"].tolist() == [True, True]


def test_metric_reports_quality_and_zero_dates() -> None:
    m = mod.metric(_rows(), {20250101, 20250102, 20250103})
    assert m["sample_count"] == 2
    assert m["zero_candidate_date_count"] == 1
    assert m["winner_rate_ret20_gt_10pct"] == 0.5
    assert m["bad_rate_ret20_lt_minus_5pct"] == 0.5


def test_overlap_matrix_counts_pairwise_family_overlap() -> None:
    rows = pd.concat(
        [
            _rows().iloc[[0]].assign(family_name="family_a_early_trend_reclaim_controlled_extension"),
            _rows().iloc[[0]].assign(family_name="family_c_weekly_monthly_supportive_daily_not_overextended"),
        ],
        ignore_index=True,
    )
    matrix = mod.family_overlap_matrix(rows)
    assert matrix["family_a_early_trend_reclaim_controlled_extension"]["family_c_weekly_monthly_supportive_daily_not_overextended"]["overlap_sample_count"] == 1


def test_decide_keep_for_clear_family_seed() -> None:
    metrics = {
        "family": {
            "mean_ret20": 0.06,
            "winner_rate_ret20_gt_10pct": 0.30,
            "bad_rate_ret20_lt_minus_5pct": 0.20,
            "severe_rate_ret20_lt_minus_10pct": 0.10,
            "sample_count": 150,
            "date_count": 80,
            "average_candidates_per_date": 1.8,
        }
    }
    overlaps = {"family": {"overlap_rate": 0.1, "unique_new_sample_count": 140, "added_date_count": 50}}
    decision, _ = mod.decide("family", metrics, overlaps)
    assert decision == "pattern_family_seed_keep_for_portfolio_pretest"


def test_decide_no_edge_without_reference_winner_rate() -> None:
    metrics = {
        "family": {
            "mean_ret20": 0.01,
            "winner_rate_ret20_gt_10pct": 0.12,
            "bad_rate_ret20_lt_minus_5pct": 0.20,
            "severe_rate_ret20_lt_minus_10pct": 0.10,
            "sample_count": 150,
            "date_count": 80,
            "average_candidates_per_date": 1.8,
        }
    }
    overlaps = {"family": {"overlap_rate": 0.1, "unique_new_sample_count": 140, "added_date_count": 50}}
    decision, _ = mod.decide("family", metrics, overlaps)
    assert decision == "no_new_family_edge"
