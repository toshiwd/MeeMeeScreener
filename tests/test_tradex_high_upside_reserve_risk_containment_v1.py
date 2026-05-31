from __future__ import annotations

import pandas as pd

from scripts import tradex_high_upside_reserve_risk_containment_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "decision_date": 20250101,
                "code": "1001",
                "ret5": 0.02,
                "ret20": 0.15,
                "dist_ma20_pct": 0.05,
                "dist_ma60_pct": 0.10,
                "realized_vol20": 0.03,
                "atr14_pct": 0.04,
                "volume_ma20_ratio": 1.2,
                "failed_high_update": False,
                "upper_wick_ratio": 0.10,
                "large_bearish_candle": False,
                "weekly_monthly_uptrend_proxy": True,
                "monthly_high_zone_proxy": True,
                "monthly_box_inside_proxy": False,
                "baseline_rank": 12,
                "primary_family": "alpha",
            },
            {
                "decision_date": 20250101,
                "code": "1002",
                "ret5": -0.01,
                "ret20": -0.12,
                "dist_ma20_pct": 0.20,
                "dist_ma60_pct": 0.35,
                "realized_vol20": 0.07,
                "atr14_pct": 0.08,
                "volume_ma20_ratio": 3.0,
                "failed_high_update": True,
                "upper_wick_ratio": 0.50,
                "large_bearish_candle": True,
                "weekly_monthly_uptrend_proxy": True,
                "monthly_high_zone_proxy": True,
                "monthly_box_inside_proxy": False,
                "baseline_rank": 25,
                "primary_family": "beta",
            },
            {
                "decision_date": 20250102,
                "code": "1003",
                "ret5": 0.00,
                "ret20": -0.03,
                "dist_ma20_pct": 0.08,
                "dist_ma60_pct": 0.12,
                "realized_vol20": 0.04,
                "atr14_pct": 0.05,
                "volume_ma20_ratio": 1.5,
                "failed_high_update": False,
                "upper_wick_ratio": 0.20,
                "large_bearish_candle": False,
                "weekly_monthly_uptrend_proxy": False,
                "monthly_high_zone_proxy": False,
                "monthly_box_inside_proxy": True,
                "baseline_rank": 35,
                "primary_family": "alpha",
            },
        ]
    )


def test_feature_contract_classifies_outcomes_and_future_leaks() -> None:
    contract = mod.feature_contract(["code", "ret20", "winner_probability"])
    assert contract["fields"]["code"]["classification"] == "point_in_time_feature"
    assert contract["fields"]["winner_probability"]["classification"] == "point_in_time_feature"
    assert contract["fields"]["ret20"]["classification"] == "outcome_only"
    assert contract["fields"]["ret20_derived_terms"]["classification"] == "forbidden_future_leak"
    assert contract["fields"]["liquidity_event_fields"]["classification"] == "unavailable"


def test_metric_computes_required_rates() -> None:
    m = mod.metric(_rows())
    assert m["sample_count"] == 3
    assert m["date_count"] == 2
    assert m["winner_rate_ret20_gt_10pct"] == 1 / 3
    assert m["bad_rate_ret20_lt_minus_5pct"] == 1 / 3
    assert m["severe_rate_ret20_lt_minus_10pct"] == 1 / 3
    assert m["downside_to_upside_ratio"] == 1.0


def test_variant_a_refined_removes_extension_and_volatility_risk() -> None:
    rows = _rows()
    masks = mod.variant_masks(rows)
    kept = rows[masks["variant_a_refined"]]
    assert kept["code"].tolist() == ["1001", "1003"]


def test_decide_keeps_only_when_thresholds_are_met() -> None:
    metrics = {
        "raw_top_5pct": {
            "bad_rate_ret20_lt_minus_5pct": 0.32,
            "severe_rate_ret20_lt_minus_10pct": 0.21,
        },
        "variant_a_refined": {
            "mean_ret20": 0.08,
            "winner_rate_ret20_gt_10pct": 0.42,
            "bad_rate_ret20_lt_minus_5pct": 0.20,
            "severe_rate_ret20_lt_minus_10pct": 0.12,
            "average_candidates_per_date": 1.2,
            "date_count": 80,
            "kept_share": 0.35,
        },
    }
    remaining = {"winner_rate_ret20_gt_10pct": 0.17}
    decision, reasons = mod.decide(metrics, remaining, "variant_a_refined")
    assert decision == "risk_containment_keep_for_pattern_portfolio_pretest"
    assert reasons


def test_decide_freezes_when_bad_rate_threshold_fails() -> None:
    metrics = {
        "raw_top_5pct": {
            "bad_rate_ret20_lt_minus_5pct": 0.32,
            "severe_rate_ret20_lt_minus_10pct": 0.21,
        },
        "variant_a_refined": {
            "mean_ret20": 0.09,
            "winner_rate_ret20_gt_10pct": 0.45,
            "bad_rate_ret20_lt_minus_5pct": 0.251,
            "severe_rate_ret20_lt_minus_10pct": 0.14,
            "average_candidates_per_date": 1.1,
            "date_count": 90,
            "kept_share": 0.34,
        },
    }
    remaining = {"winner_rate_ret20_gt_10pct": 0.17}
    decision, _ = mod.decide(metrics, remaining, "variant_a_refined")
    assert decision == "upside_signal_remains_risky_freeze_family_seed"
