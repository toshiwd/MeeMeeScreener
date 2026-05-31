from __future__ import annotations

import pandas as pd

from scripts import tradex_independent_buy_setup_discovery_v1 as mod


def _base_rows() -> pd.DataFrame:
    rows = pd.DataFrame(
        [
            {
                "code": "1001",
                "decision_date": 20250101,
                "path20_available": True,
                "ret20": 0.12,
                "ret5": 0.01,
                "primary_family": "pullback_reclaim_family",
                "days_since_ma20_reclaim": 3,
                "above20_streak": 2,
                "dist_ma20_pct": 0.02,
                "dist_ma60_pct": 0.06,
                "ma20_slope": 0.01,
                "ma7_slope": 0.01,
                "ma7_gt_ma20_gt_ma60": True,
                "failed_high_update": False,
                "large_bearish_candle": False,
                "upper_wick_ratio": 0.2,
                "realized_vol20": 0.03,
                "atr14_pct": 0.04,
                "monthly_box_breakout_proxy": False,
                "monthly_box_inside_proxy": True,
                "monthly_high_zone_proxy": False,
                "weekly_monthly_uptrend_proxy": True,
                "volume_ma20_ratio": 1.5,
            },
            {
                "code": "1002",
                "decision_date": 20250102,
                "path20_available": True,
                "ret20": -0.08,
                "ret5": -0.01,
                "primary_family": "early_trend_family",
                "days_since_ma20_reclaim": 20,
                "above20_streak": 10,
                "dist_ma20_pct": 0.04,
                "dist_ma60_pct": 0.10,
                "ma20_slope": 0.01,
                "ma7_slope": 0.01,
                "ma7_gt_ma20_gt_ma60": True,
                "failed_high_update": False,
                "large_bearish_candle": False,
                "upper_wick_ratio": 0.2,
                "realized_vol20": 0.03,
                "atr14_pct": 0.04,
                "monthly_box_breakout_proxy": True,
                "monthly_box_inside_proxy": False,
                "monthly_high_zone_proxy": False,
                "weekly_monthly_uptrend_proxy": True,
                "volume_ma20_ratio": 1.5,
            },
        ]
    )
    return mod.normalize_rows(rows)


def test_setup_masks_identify_predeclared_families() -> None:
    masks = mod.setup_masks(_base_rows())
    assert masks["setup_a_pullback_reclaim"].tolist() == [True, False]
    assert masks["setup_b_breakout_continuation_controlled_extension"].tolist() == [False, True]
    assert masks["setup_c_supportive_context_daily_confirmation"].tolist() == [True, False]


def test_metric_reports_required_rates() -> None:
    rows = _base_rows()
    m = mod.metric(rows, {20250101, 20250102, 20250103})
    assert m["sample_count"] == 2
    assert m["zero_candidate_date_count"] == 1
    assert m["winner_rate_ret20_gt_10pct"] == 0.5
    assert m["bad_rate_ret20_lt_minus_5pct"] == 0.5


def test_overlap_metrics_counts_unique_new_samples() -> None:
    rows = _base_rows().assign(setup_name="setup_a_pullback_reclaim")
    frozen = rows.iloc[[0]].copy()
    overlaps = mod.overlap_metrics(rows, frozen, {20250101, 20250102})
    item = overlaps["setup_a_pullback_reclaim"]
    assert item["overlap_sample_count"] == 1
    assert item["unique_new_sample_count"] == 1
    assert item["overlap_rate"] == 0.5


def test_decide_keep_when_edge_breadth_and_overlap_pass() -> None:
    metrics = {
        "setup": {
            "mean_ret20": 0.06,
            "winner_rate_ret20_gt_10pct": 0.30,
            "bad_rate_ret20_lt_minus_5pct": 0.20,
            "severe_rate_ret20_lt_minus_10pct": 0.10,
            "date_count": 100,
        }
    }
    overlaps = {"setup": {"unique_new_sample_count": 150, "added_date_count": 50, "overlap_rate": 0.10}}
    decision, _ = mod.decide("setup", metrics, overlaps)
    assert decision == "independent_setup_keep_for_family_portfolio_pretest"


def test_decide_promising_when_risk_or_support_fails() -> None:
    metrics = {
        "setup": {
            "mean_ret20": 0.04,
            "winner_rate_ret20_gt_10pct": 0.25,
            "bad_rate_ret20_lt_minus_5pct": 0.30,
            "severe_rate_ret20_lt_minus_10pct": 0.16,
            "date_count": 20,
        }
    }
    overlaps = {"setup": {"unique_new_sample_count": 30, "added_date_count": 10, "overlap_rate": 0.10}}
    decision, _ = mod.decide("setup", metrics, overlaps)
    assert decision == "independent_setup_promising_but_underpowered"
