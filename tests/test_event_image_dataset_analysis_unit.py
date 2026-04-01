from __future__ import annotations

import pandas as pd

from external_analysis.event_image_dataset.analysis import (
    _apply_combo_rule,
    _apply_gate_mask,
    _apply_rebound_playbook_variant,
    _apply_rebound_thin_liquidity_threshold,
    _apply_rebound_veto_ablation,
    _compute_micro_features,
    _decorate_rebound_playbook_frame,
    _determine_selection_contract_leak,
    _summarize_selection_contract_stage,
)


def test_compute_micro_features_reads_only_latest_window_shape() -> None:
    frame = pd.DataFrame(
        [
            {
                "trade_date": 20260120,
                "o": 100.0,
                "h": 106.0,
                "l": 99.0,
                "c": 104.0,
                "v": 1000.0,
                "ma20": 102.0,
                "ma60": 100.0,
                "ma120": 98.0,
                "volume_mean5": 900.0,
                "volume_mean20": 800.0,
                "turnover20": 82000.0,
                "realized_vol20": 0.025,
                "rolling_high20": 108.0,
                "rolling_high60": 112.0,
                "range_pct": 0.0673076923,
                "range_median20": 0.08,
            },
            {
                "trade_date": 20260121,
                "o": 105.0,
                "h": 105.0,
                "l": 101.0,
                "c": 102.0,
                "v": 1100.0,
                "ma20": 101.0,
                "ma60": 99.0,
                "ma120": 97.0,
                "volume_mean5": 950.0,
                "volume_mean20": 850.0,
                "turnover20": 83000.0,
                "realized_vol20": 0.026,
                "rolling_high20": 108.0,
                "rolling_high60": 112.0,
                "range_pct": 0.0392156863,
                "range_median20": 0.08,
            },
            {
                "trade_date": 20260122,
                "o": 101.0,
                "h": 107.0,
                "l": 100.0,
                "c": 106.0,
                "v": 1300.0,
                "ma20": 102.0,
                "ma60": 100.0,
                "ma120": 98.0,
                "volume_mean5": 1000.0,
                "volume_mean20": 900.0,
                "turnover20": 85000.0,
                "realized_vol20": 0.027,
                "rolling_high20": 108.0,
                "rolling_high60": 112.0,
                "range_pct": 0.0660377358,
                "range_median20": 0.08,
            },
            {
                "trade_date": 20260123,
                "o": 105.0,
                "h": 108.0,
                "l": 103.0,
                "c": 107.0,
                "v": 1400.0,
                "ma20": 103.0,
                "ma60": 101.0,
                "ma120": 99.0,
                "volume_mean5": 1100.0,
                "volume_mean20": 950.0,
                "turnover20": 86000.0,
                "realized_vol20": 0.028,
                "rolling_high20": 109.0,
                "rolling_high60": 113.0,
                "range_pct": 0.0467289719,
                "range_median20": 0.08,
            },
            {
                "trade_date": 20260126,
                "o": 108.0,
                "h": 110.0,
                "l": 104.0,
                "c": 105.0,
                "v": 1500.0,
                "ma20": 104.0,
                "ma60": 102.0,
                "ma120": 100.0,
                "volume_mean5": 1200.0,
                "volume_mean20": 1000.0,
                "turnover20": 88000.0,
                "realized_vol20": 0.029,
                "rolling_high20": 110.0,
                "rolling_high60": 114.0,
                "range_pct": 0.0571428571,
                "range_median20": 0.08,
            },
            {
                "trade_date": 20260127,
                "o": 103.0,
                "h": 109.0,
                "l": 102.0,
                "c": 108.0,
                "v": 1600.0,
                "ma20": 105.0,
                "ma60": 103.0,
                "ma120": 101.0,
                "volume_mean5": 1300.0,
                "volume_mean20": 1100.0,
                "turnover20": 90000.0,
                "realized_vol20": 0.03,
                "rolling_high20": 111.0,
                "rolling_high60": 115.0,
                "range_pct": 0.0648148148,
                "range_median20": 0.08,
            },
            {
                "trade_date": 20260128,
                "o": 106.0,
                "h": 112.0,
                "l": 105.0,
                "c": 111.0,
                "v": 1700.0,
                "ma20": 106.0,
                "ma60": 104.0,
                "ma120": 102.0,
                "volume_mean5": 1400.0,
                "volume_mean20": 1200.0,
                "turnover20": 93000.0,
                "realized_vol20": 0.031,
                "rolling_high20": 112.0,
                "rolling_high60": 116.0,
                "range_pct": 0.0630630631,
                "range_median20": 0.08,
            },
        ]
    )
    frame["daily_return"] = frame["c"].pct_change()
    frame["ma20"] = frame["ma20"].astype(float)
    frame["ma60"] = frame["ma60"].astype(float)
    frame["ma120"] = frame["ma120"].astype(float)

    features = _compute_micro_features(frame)

    assert round(float(features["body_pct_last"]), 6) == round(abs(111.0 - 106.0) / (112.0 - 105.0), 6)
    assert round(float(features["upper_wick_pct_last"]), 6) == round((112.0 - 111.0) / (112.0 - 105.0), 6)
    assert round(float(features["lower_wick_pct_last"]), 6) == round((106.0 - 105.0) / (112.0 - 105.0), 6)
    assert int(features["is_bull_last"]) == 1
    assert int(features["is_bear_last"]) == 0
    assert round(float(features["gap_from_prev_close_pct"]), 6) == round((106.0 / 108.0) - 1.0, 6)
    assert int(features["inside_bar_last"]) == 0
    assert int(features["outside_bar_last"]) == 0
    assert int(features["bull_streak_3"]) == 2
    assert int(features["bear_streak_3"]) == 0
    assert int(features["narrow_range_count_5"]) == 5
    assert int(features["higher_close_count_5"]) == 3
    assert int(features["lower_close_count_5"]) == 1
    assert round(float(features["price_vs_ma20"]), 6) == round((111.0 / 106.0) - 1.0, 6)
    assert round(float(features["price_vs_ma60"]), 6) == round((111.0 / 104.0) - 1.0, 6)
    assert round(float(features["price_vs_ma120"]), 6) == round((111.0 / 102.0) - 1.0, 6)
    assert round(float(features["ma20_vs_ma60"]), 6) == round((106.0 / 104.0) - 1.0, 6)
    assert round(float(features["ma60_vs_ma120"]), 6) == round((104.0 / 102.0) - 1.0, 6)
    assert round(float(features["distance_from_20d_high"]), 6) == round((111.0 / 112.0) - 1.0, 6)
    assert round(float(features["distance_from_60d_high"]), 6) == round((111.0 / 116.0) - 1.0, 6)
    assert round(float(features["pullback_from_recent_high_pct"]), 6) == round((112.0 - 111.0) / 112.0, 6)
    assert int(features["reclaim_ma20_flag"]) == 0
    assert int(features["reclaim_ma60_flag"]) == 0
    assert round(float(features["volume_spike_ratio_5_20"]), 6) == round(1400.0 / 1200.0, 6)
    assert round(float(features["volume_last_vs_20"]), 6) == round(1700.0 / 1200.0, 6)
    assert round(float(features["turnover20"]), 6) == 93000.0
    assert round(float(features["realized_vol20"]), 6) == 0.031
    assert int(features["is_bull_prev1"]) == 1
    assert int(features["is_bull_prev2"]) == 0
    assert round(float(features["lower_wick_pct_prev1"]), 6) == round((103.0 - 102.0) / (109.0 - 102.0), 6)
    assert round(float(features["upper_wick_pct_prev2"]), 6) == round((110.0 - 108.0) / (110.0 - 104.0), 6)
    assert int(features["close_above_prev1_body_mid_flag"]) == 1
    assert int(features["engulfed_by_next_bear_flag"]) == 0
    assert int(features["two_red_then_green_flag"]) == 0
    assert int(features["narrow_range_then_bull_flag"]) == 1
    assert int(features["higher_low_3_flag"]) == 0
    assert int(features["higher_close_3_flag"]) == 1


def test_apply_gate_mask_uses_expected_thresholds() -> None:
    frame = pd.DataFrame(
        [
            {
                "dist_ma120": 0.05,
                "position_from_60d_high": -0.15,
                "realized_vol20": 0.033,
                "volume_change20": -0.10,
                "dist_ma60": 0.01,
            },
            {
                "dist_ma120": 0.01,
                "position_from_60d_high": -0.14,
                "realized_vol20": 0.034,
                "volume_change20": -0.20,
                "dist_ma60": -0.02,
            },
            {
                "dist_ma120": -0.02,
                "position_from_60d_high": -0.08,
                "realized_vol20": 0.024,
                "volume_change20": 0.25,
                "dist_ma60": -0.03,
            },
        ]
    )

    primary_mask = _apply_gate_mask(
        frame,
        price_vs_ma120_min=0.030385945180089302,
        distance_from_60d_high_range=(-0.17672449574186516, -0.13623486558311215),
        realized_vol20_min=0.031597770852671736,
        volume_change20_max=None,
    )
    comparison_mask = _apply_gate_mask(
        frame,
        price_vs_ma120_min=-0.04641842281821093,
        price_vs_ma60_min=-0.06900447803551038,
        realized_vol20_max=0.026231553179427828,
        volume_change20_min=0.1492348699427547,
    )

    assert primary_mask.tolist() == [True, False, False]
    assert comparison_mask.tolist() == [False, False, True]


def test_apply_combo_rule_uses_expected_conditions() -> None:
    frame = pd.DataFrame(
        [
            {
                "is_bull_last": 1.0,
                "lower_wick_pct_last": 0.30,
                "lower_close_count_5": 3.0,
                "price_vs_ma120": 0.06,
                "distance_from_60d_high": -0.15,
                "realized_vol20": 0.034,
            },
            {
                "is_bull_last": 1.0,
                "lower_wick_pct_last": 0.10,
                "lower_close_count_5": 3.0,
                "price_vs_ma120": 0.06,
                "distance_from_60d_high": -0.15,
                "realized_vol20": 0.034,
            },
            {
                "is_bull_last": 0.0,
                "lower_wick_pct_last": 0.35,
                "lower_close_count_5": 4.0,
                "price_vs_ma120": 0.08,
                "distance_from_60d_high": -0.16,
                "realized_vol20": 0.036,
            },
        ]
    )
    conditions = [
        {"column": "is_bull_last", "operator": "eq", "value": 1.0, "enabled": True},
        {"column": "lower_wick_pct_last", "operator": "gte", "value": 0.20, "enabled": True},
        {"column": "lower_close_count_5", "operator": "gte", "value": 3.0, "enabled": True},
        {"column": "price_vs_ma120", "operator": "gte", "value": 0.030385945180089302, "enabled": True},
        {"column": "distance_from_60d_high", "operator": "between", "value": (-0.17672449574186516, -0.13623486558311215), "enabled": True},
        {"column": "realized_vol20", "operator": "gte", "value": 0.031597770852671736, "enabled": True},
    ]

    mask = _apply_combo_rule(frame, conditions=conditions)

    assert mask.tolist() == [True, False, False]


def test_rebound_playbook_variant_scores_and_veto_flags() -> None:
    frame = pd.DataFrame(
        [
            {
                "price_vs_ma120": 0.06,
                "distance_from_60d_high": -0.14,
                "ma60_slope_5": 0.02,
                "ma20_slope_5": 0.03,
                "realized_vol20": 0.02,
                "is_bull_last": 1,
                "lower_wick_pct_last": 0.35,
                "bull_streak_3": 2,
                "narrow_range_count_5": 3,
                "higher_close_count_5": 3,
                "close_above_prev1_body_mid_flag": 1,
                "engulfed_by_next_bear_flag": 0,
                "two_red_then_green_flag": 0,
                "narrow_range_then_bull_flag": 1,
                "higher_low_3_flag": 1,
                "turnover20": 120000.0,
            },
            {
                "price_vs_ma120": 0.05,
                "distance_from_60d_high": -0.01,
                "ma60_slope_5": -0.01,
                "ma20_slope_5": -0.02,
                "realized_vol20": 0.06,
                "is_bull_last": 1,
                "lower_wick_pct_last": 0.40,
                "bull_streak_3": 2,
                "narrow_range_count_5": 3,
                "higher_close_count_5": 3,
                "close_above_prev1_body_mid_flag": 1,
                "engulfed_by_next_bear_flag": 0,
                "two_red_then_green_flag": 0,
                "narrow_range_then_bull_flag": 1,
                "higher_low_3_flag": 1,
                "turnover20": 40000.0,
            },
        ]
    )
    thresholds = {
        "price_vs_ma120_env_min": 0.03,
        "distance_from_60d_high_low": -0.18,
        "distance_from_60d_high_high": -0.12,
        "bull_streak_floor": 1,
        "narrow_range_floor": 2,
        "higher_close_floor": 2,
        "lower_wick_median": 0.30,
        "realized_vol20_veto_max": 0.04,
        "turnover20_veto_min": 80000.0,
        "distance_from_60d_high_veto_max": -0.02,
    }

    decorated = _decorate_rebound_playbook_frame(frame, thresholds=thresholds)

    assert decorated["sequence_flags"].iloc[0] == ["tail_hold", "narrow_range_then_bull", "higher_low_3"]
    assert decorated["veto_flags"].iloc[0] == []
    assert "extreme_volatility" in decorated["veto_flags"].iloc[1]
    assert "thin_liquidity" in decorated["veto_flags"].iloc[1]
    assert bool(decorated["veto_blocked"].iloc[1]) is True

    selected = _apply_rebound_playbook_variant(
        frame,
        variant_name="balanced_playbook",
        thresholds=thresholds,
    )

    assert len(selected) == 1
    assert float(selected["playbook_score"].iloc[0]) >= 0.55


def test_rebound_veto_ablation_disables_one_rule_only() -> None:
    frame = pd.DataFrame(
        [
            {
                "veto_flags": ["thin_liquidity"],
                "veto_blocked": True,
            },
            {
                "veto_flags": ["trend_shape_conflict"],
                "veto_blocked": True,
            },
            {
                "veto_flags": [],
                "veto_blocked": False,
            },
        ]
    )

    selected = _apply_rebound_veto_ablation(frame, disabled_rules=("thin_liquidity",))

    assert len(selected) == 2
    assert selected["effective_veto_flags"].iloc[0] == []
    assert selected["effective_veto_flags"].iloc[1] == []


def test_rebound_thin_liquidity_threshold_only_changes_thin_liquidity_rule() -> None:
    frame = pd.DataFrame(
        [
            {
                "code": "1111",
                "turnover20": 70000.0,
                "veto_flags": ["thin_liquidity"],
            },
            {
                "code": "2222",
                "turnover20": 70000.0,
                "veto_flags": ["thin_liquidity", "trend_shape_conflict"],
            },
        ]
    )

    current = _apply_rebound_thin_liquidity_threshold(frame, thin_liquidity_turnover20_min=80000.0)
    weak = _apply_rebound_thin_liquidity_threshold(frame, thin_liquidity_turnover20_min=60000.0)
    off = _apply_rebound_thin_liquidity_threshold(frame, thin_liquidity_turnover20_min=None)

    assert current.empty
    assert len(weak) == 1
    assert weak.loc[0, "code"] == "1111"
    assert weak.loc[0, "effective_veto_flags"] == []
    assert len(off) == 1
    assert off.loc[0, "code"] == "1111"
    assert off.loc[0, "effective_veto_flags"] == []


def test_selection_contract_stage_summary_computes_attrition_and_retention() -> None:
    core_gate = pd.DataFrame(
        [
            {"code": "1111", "as_of_date": 20250131, "label_id": 1, "image_pred_prob_up": 0.7, "numeric_pred_prob_up": 0.6, "image_pred_label": 1, "numeric_pred_label": 1, "forward_return_1m": 0.08},
            {"code": "2222", "as_of_date": 20250131, "label_id": 0, "image_pred_prob_up": 0.6, "numeric_pred_prob_up": 0.5, "image_pred_label": 1, "numeric_pred_label": 1, "forward_return_1m": -0.04},
        ]
    )
    previous = core_gate.copy()
    current = core_gate.iloc[[0]].copy().reset_index(drop=True)

    summary = _summarize_selection_contract_stage(
        stage_name="core_gate_plus_veto_current",
        stage_frame=current,
        previous_frame=previous,
        core_gate_frame=core_gate,
    )

    assert summary["sample_count"] == 1
    assert summary["positive_count"] == 1
    assert summary["false_positive_count"] == 0
    assert summary["sample_attrition_vs_previous"] == 1
    assert summary["sample_attrition_vs_core_gate"] == 1
    assert summary["positive_retention_vs_core_gate"] == 1.0
    assert summary["false_positive_reduction_vs_core_gate"] == 1
    assert summary["saved_false_positive_codes"] == ["2222"]


def test_determine_selection_contract_leak_prefers_thin_liquidity_policy_when_winners_return_with_fp() -> None:
    leak_stage, reason = _determine_selection_contract_leak(
        current_veto_stage={"lost_winner_codes": ["1111"], "false_positive_count": 0},
        weak_stage={
            "lost_winner_codes": [],
            "false_positive_count": 1,
            "monthly_long_short_spread_delta_vs_numeric": 0.0,
            "monthly_top10_precision_up_delta_vs_numeric": 0.0,
            "sample_count": 1,
        },
        ranking_top20_stage={"lost_winner_codes": []},
        entry_stage={"lost_winner_codes": []},
        full_validation_summary={"decision": "fix_holdings_before_policy_change"},
    )

    assert leak_stage == "thin_liquidity_policy"
    assert "weak_1" in reason
