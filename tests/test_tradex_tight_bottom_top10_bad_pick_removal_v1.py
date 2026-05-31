from __future__ import annotations

import pandas as pd

from scripts import tradex_tight_bottom_top10_bad_pick_removal_v1 as mod


def test_feature_contract_marks_missing_event_liquidity_unavailable() -> None:
    contract = mod.feature_contract(False, False, ["volume_ma20_ratio", "ret20"])
    assert contract["fields"]["event_flags_json"]["classification"] == "unavailable"
    assert contract["fields"]["liquidity_flags_json"]["classification"] == "unavailable"
    assert contract["fields"]["volume_ma20_ratio"]["classification"] == "point_in_time_feature"
    assert contract["fields"]["ret20"]["classification"] == "outcome_only"


def test_add_flags_variant_b_only_targets_bottom_top10() -> None:
    rows = pd.DataFrame(
        [
            {"baseline_rank": 5, "ret20": -0.1, "volume_ma20_ratio": 0.5, "realized_vol20": 0.04, "atr14_pct": 0.02, "failed_high_update": False, "large_bearish_candle": False, "upper_wick_ratio": 0.0, "days_since_ma20_reclaim": 9, "dist_ma20_pct": 0.0, "weekly_monthly_uptrend_proxy": True},
            {"baseline_rank": 8, "ret20": -0.1, "volume_ma20_ratio": 0.5, "realized_vol20": 0.04, "atr14_pct": 0.02, "failed_high_update": False, "large_bearish_candle": False, "upper_wick_ratio": 0.0, "days_since_ma20_reclaim": 9, "dist_ma20_pct": 0.0, "weekly_monthly_uptrend_proxy": True},
        ]
    )
    out = mod.add_flags(rows)
    assert not bool(out.loc[0, "variant_b_demote"])
    assert bool(out.loc[1, "variant_b_demote"])


def test_score_variant_moves_demoted_bottom_top10_below_replacement() -> None:
    rows = pd.DataFrame(
        [
            {"decision_date": 20240101, "code": "A", "baseline_rank": 8, "baseline_score": 10, "variant_b_demote": True},
            {"decision_date": 20240101, "code": "B", "baseline_rank": 11, "baseline_score": 8, "variant_b_demote": False},
        ]
    )
    out = mod.score_variant(rows, "variant_b")
    assert int(out.loc[out["code"] == "B", "variant_b_rank"].iloc[0]) == 1


def test_decide_keep_when_gates_pass() -> None:
    topk = pd.DataFrame(
        [
            {"variant": "variant_b", "period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.01, "delta_bad_pick_rate": -0.01, "delta_severe_loss_rate": 0.0},
            {"variant": "variant_c", "period": "2024_2026_combined", "topk": 10, "delta_mean_ret20": 0.0, "delta_bad_pick_rate": 0.0, "delta_severe_loss_rate": 0.0},
        ]
    )
    removed = {"variant_b": {}, "variant_c": {}}
    repl = {"variant_b": {"replacement_delta_vs_removed": 0.02}, "variant_c": {"replacement_delta_vs_removed": 0.0}}
    winner = {"variant_b": {"winner_accidental_removal_rate": 0.1}, "variant_c": {"winner_accidental_removal_rate": 0.0}}
    bounds = {"variant_b": {"changed_top10_members_count": 10}, "variant_c": {"changed_top10_members_count": 10}}
    decision, _, variant = mod.decide(topk, removed, repl, winner, bounds)
    assert decision == "keep_for_next_stage"
    assert variant == "variant_b"
