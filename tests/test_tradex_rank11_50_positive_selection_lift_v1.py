from __future__ import annotations

import pandas as pd

from scripts import tradex_rank11_50_positive_selection_lift_v1 as mod


def test_feature_contract_classifies_outcomes_and_forbidden_terms() -> None:
    c = mod.feature_contract(["ma20_slope", "ret20"])
    assert c["fields"]["ma20_slope"]["classification"] == "point_in_time_feature"
    assert c["fields"]["ret20"]["classification"] == "outcome_only"
    assert c["fields"]["ret20_derived_terms"]["classification"] == "forbidden_future_leak"


def test_add_variant_flags_only_lifts_rank11_50() -> None:
    rows = pd.DataFrame(
        [
            {"baseline_rank": 10, "weekly_monthly_uptrend_proxy": True, "ma20_slope": 0.1, "ma60_slope": 0.1, "dist_ma20_pct": 0.0, "large_bullish_candle": True, "large_bearish_candle": False, "lower_wick_ratio": 0.3, "upper_wick_ratio": 0.1, "volume_ma20_ratio": 1.2, "failed_high_update": False, "monthly_box_inside_proxy": True, "monthly_high_zone_proxy": False},
            {"baseline_rank": 20, "weekly_monthly_uptrend_proxy": True, "ma20_slope": 0.1, "ma60_slope": 0.1, "dist_ma20_pct": 0.0, "large_bullish_candle": True, "large_bearish_candle": False, "lower_wick_ratio": 0.3, "upper_wick_ratio": 0.1, "volume_ma20_ratio": 1.2, "failed_high_update": False, "monthly_box_inside_proxy": True, "monthly_high_zone_proxy": False},
        ]
    )
    out = mod.add_variant_flags(rows)
    assert not bool(out.loc[0, "variant_a_lift"])
    assert bool(out.loc[1, "variant_a_lift"])
    assert bool(out.loc[1, "variant_c_lift"])


def test_score_variant_lifts_reserve_candidate() -> None:
    rows = pd.DataFrame(
        [
            {"decision_date": 20240101, "code": "A", "baseline_rank": 10, "baseline_score": 10, "variant_a_lift": False},
            {"decision_date": 20240101, "code": "B", "baseline_rank": 20, "baseline_score": 8, "variant_a_lift": True},
        ]
    )
    out = mod.score_variant(rows, "variant_a")
    assert int(out.loc[out["code"] == "B", "variant_a_rank"].iloc[0]) == 1


def test_decide_keep_when_positive_lift_gates_pass() -> None:
    topk = pd.DataFrame(
        [
            {"variant": "variant_a", "period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.01, "delta_bad_pick_rate": 0.0, "delta_severe_loss_rate": 0.0},
            {"variant": "variant_b", "period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.0, "delta_bad_pick_rate": 0.0, "delta_severe_loss_rate": 0.0},
            {"variant": "variant_c", "period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.0, "delta_bad_pick_rate": 0.0, "delta_severe_loss_rate": 0.0},
        ]
    )
    promoted = {v: {} for v in ["variant_a", "variant_b", "variant_c"]}
    displaced = {v: {} for v in ["variant_a", "variant_b", "variant_c"]}
    lift = {
        "variant_a": {"promoted_minus_displaced_ret20": 0.02, "accidental_promotion_bad_rate": 0.1},
        "variant_b": {"promoted_minus_displaced_ret20": 0.0, "accidental_promotion_bad_rate": 0.0},
        "variant_c": {"promoted_minus_displaced_ret20": 0.0, "accidental_promotion_bad_rate": 0.0},
    }
    bounds = {v: {"changed_top10_members_count": 10} for v in ["variant_a", "variant_b", "variant_c"]}
    decision, _, variant = mod.decide(topk, promoted, displaced, lift, bounds)
    assert decision == "keep_for_next_stage"
    assert variant == "variant_a"
