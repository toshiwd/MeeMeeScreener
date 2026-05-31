from __future__ import annotations

import pandas as pd

from scripts import tradex_recent_ex_rights_supportive_seed_gate_v1 as mod


def test_supportive_slice_keeps_rights_without_earnings() -> None:
    rows = pd.DataFrame(
        [
            {"ex_rights_nearby_flag": True, "earnings_nearby_flag": False},
            {"ex_rights_nearby_flag": True, "earnings_nearby_flag": True},
            {"ex_rights_nearby_flag": False, "earnings_nearby_flag": False},
        ]
    )
    assert len(mod.supportive_slice(rows)) == 1


def test_decide_hold_when_positive_but_underpowered() -> None:
    metrics = {
        "sample_count": 1000,
        "date_count": 20,
        "mean_ret20": 0.04,
        "winner_rate_ret20_gt_10pct": 0.20,
        "bad_rate_ret20_lt_minus_5pct": 0.15,
        "severe_rate_ret20_lt_minus_10pct": 0.04,
    }
    decision, cls, reasons = mod.decide(metrics, {"top_10_dates_share_of_samples": 0.5}, {"p": {"mean_ret20": 0.04}})
    assert decision == "ex_rights_seed_promising_but_underpowered"
    assert cls == "HOLD_UNDERPOWERED"
    assert reasons


def test_decide_keep_when_breadth_and_stability_ok() -> None:
    metrics = {
        "sample_count": 5000,
        "date_count": 60,
        "mean_ret20": 0.04,
        "winner_rate_ret20_gt_10pct": 0.20,
        "bad_rate_ret20_lt_minus_5pct": 0.15,
        "severe_rate_ret20_lt_minus_10pct": 0.04,
    }
    periods = {f"p{i}": {"mean_ret20": 0.02} for i in range(6)}
    decision, cls, _ = mod.decide(metrics, {"top_10_dates_share_of_samples": 0.5}, periods)
    assert decision == "ex_rights_seed_keep_for_current_buyability_pretest"
    assert cls == "KEEP"


def test_feature_contract_marks_outcomes_offline() -> None:
    contract = mod.feature_contract()
    assert contract["fields"]["ret20"]["classification"] == "offline_outcome_only"
    assert contract["fields"]["ex_rights_nearby_flag"]["classification"] == "point_in_time_feature"
