from __future__ import annotations

import pandas as pd

from scripts import tradex_pattern_family_selectivity_pretest_v1 as mod


def _rows() -> pd.DataFrame:
    base = {
        "as_of_date": 20250101,
        "code": "1001",
        "close_vs_ma7_pct": 0.02,
        "close_vs_ma20_pct": 0.03,
        "close_vs_ma60_pct": 0.08,
        "ma7_slope_5d": 0.01,
        "ma20_slope_10d": 0.01,
        "ma60_slope_20d": 0.01,
        "close_above_ma7": True,
        "close_above_ma20": True,
        "close_above_ma60": True,
        "ma7_above_ma20": True,
        "ma20_above_ma60": True,
        "body_ratio": 0.5,
        "upper_wick_ratio": 0.1,
        "lower_wick_ratio": 0.3,
        "bullish_body_flag": True,
        "bearish_body_flag": False,
        "failed_high_flag": False,
        "recent_high_distance_pct": -0.02,
        "recent_low_distance_pct": 0.1,
        "volume_vs_20d_avg": 1.2,
        "gap_up_flag": False,
        "gap_down_flag": False,
        "atr14_pct": 0.03,
        "realized_vol20": 0.02,
        "weekly_close_vs_ma7_pct": 0.02,
        "weekly_close_vs_ma20_pct": 0.04,
        "weekly_ma7_slope": 0.01,
        "weekly_ma20_slope": 0.01,
        "weekly_supportive_flag": True,
        "weekly_failed_high_flag": False,
        "monthly_close_vs_ma7_pct": 0.02,
        "monthly_close_vs_ma20_pct": 0.04,
        "monthly_ma7_slope": 0.01,
        "monthly_ma20_slope": 0.01,
        "monthly_supportive_flag": True,
        "monthly_box_position": 0.5,
        "monthly_box_width_pct": 0.2,
        "high_upside_reserve_reference_match": False,
        "early_trend_reclaim_controlled_extension_candidate": False,
        "constructive_pullback_support_bullish_confirmation_reference_match": True,
        "monthly_weekly_supportive_daily_confirmation_candidate": False,
        "volatility_compression_breakout_preparation_candidate": False,
        "ret5": 0.01,
        "ret20": 0.12,
        "winner_ret20_gt_10pct": True,
        "bad_ret20_lt_minus_5pct": False,
        "severe_ret20_lt_minus_10pct": False,
    }
    weak = {**base, "code": "1002", "as_of_date": 20250102, "ret20": -0.08, "winner_ret20_gt_10pct": False, "bad_ret20_lt_minus_5pct": True, "upper_wick_ratio": 0.5}
    return pd.DataFrame([base, weak])


def test_candidate_population_adds_quality_score() -> None:
    pop = mod.candidate_population(_rows())
    assert len(pop) == 2
    assert pop["quality_score"].max() >= 8
    assert "promising_family_count" in pop


def test_variant_masks_select_quality_and_risk() -> None:
    pop = mod.candidate_population(_rows())
    masks = mod.variant_masks(pop, "constructive_pullback_support_bullish_confirmation_reference_match")
    assert masks["variant_a_quality_score_bucket"].sum() == 2
    assert masks["variant_b_risk_filtered_subset"].sum() == 1


def test_metric_and_delta() -> None:
    pop = mod.candidate_population(_rows())
    selected = pop.iloc[[0]]
    unselected = pop.iloc[[1]]
    metric = mod.metric(selected, set(pop["as_of_date"]), len(pop))
    delta = mod.selected_unselected_delta(selected, unselected)
    assert metric["selected_share"] == 0.5
    assert delta["selected_vs_unselected_delta_ret20"] > 0
    assert delta["selected_vs_unselected_delta_bad_rate"] < 0


def test_overlap_adjusted_unique_metrics() -> None:
    pop = mod.candidate_population(_rows())
    metrics = mod.overlap_adjusted_metrics(pop)
    key = "unique_constructive_pullback_support_bullish_confirmation_reference_match"
    assert metrics[key]["sample_count"] == 2


def test_decide_no_edge_when_best_is_weak() -> None:
    metrics = {
        "fam": {
            "variant_a_quality_score_bucket": {
                "mean_ret20": 0.01,
                "winner_rate_ret20_gt_10pct": 0.1,
                "bad_rate_ret20_lt_minus_5pct": 0.2,
                "severe_rate_ret20_lt_minus_10pct": 0.08,
                "sample_count": 1000,
                "date_count": 300,
                "selected_vs_unselected_delta_ret20": 0.001,
                "selected_vs_unselected_delta_winner_rate": 0.001,
            }
        }
    }
    overlap = {"unique_fam": {"mean_ret20": 0.03, "winner_rate_ret20_gt_10pct": 0.2}}
    decision, _, _ = mod.decide(metrics, overlap)
    assert decision == "selectivity_no_edge_close_family_contract"
