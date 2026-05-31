from scripts.tradex_crash_distribution_short_pullback_additive_oos_v1 import (
    BASE_SIGNAL_KEY,
    _decision,
    _select_one_per_date,
)


def _compare(top5_mean, top10_mean=0.0, bad=0.0, severe=0.0, changed=25):
    return {
        "top5": {
            "changed_member_count_total": changed,
            "additive_delta": {
                "forward_return_20_mean": top5_mean,
                "bad_loser_rate_20": bad,
                "severe_loser_rate_20": severe,
            },
        },
        "top10": {
            "additive_delta": {
                "forward_return_20_mean": top10_mean,
            },
        },
    }


def test_base_signal_key_is_crash_distribution_short_pullback_probe():
    assert (
        BASE_SIGNAL_KEY
        == "crash_or_distribution_phase|wait_for_short_trigger|watch|pullback_probe_candidate"
    )


def test_decision_keeps_when_top5_improves_top10_stable_and_adverse_not_worse():
    decision = _decision(
        _compare(top5_mean=0.01, top10_mean=0.0, bad=-0.01, severe=0.0),
        [
            {"month": "202501", "changed_top5_members_count": 3, "top5_delta_mean": 0.01},
            {"month": "202502", "changed_top5_members_count": 2, "top5_delta_mean": -0.002},
        ],
        {"oos_selected_additive_date_count": 10},
    )

    assert decision == {
        "judgment": "keep_for_next_probe",
        "reason_type": "oos_top5_improves_top10_stable_adverse_not_worse",
    }


def test_decision_drops_when_top5_improves_but_adverse_worsens():
    decision = _decision(
        _compare(top5_mean=0.01, top10_mean=0.0, bad=0.02, severe=0.0),
        [{"month": "202501", "changed_top5_members_count": 3, "top5_delta_mean": 0.01}],
        {"oos_selected_additive_date_count": 10},
    )

    assert decision == {
        "judgment": "drop",
        "reason_type": "top5_improves_but_adverse_move_worsens",
    }


def test_decision_holds_when_branching_is_insufficient():
    decision = _decision(
        _compare(top5_mean=0.01, changed=19),
        [{"month": "202501", "changed_top5_members_count": 1, "top5_delta_mean": 0.01}],
        {"oos_selected_additive_date_count": 10},
    )

    assert decision == {
        "judgment": "hold",
        "reason_type": "insufficient_oos_branching_or_breadth",
    }


def test_select_one_per_date_prefers_lower_ma20_slope_then_higher_position():
    selected = _select_one_per_date(
        [
            {"dt": 20250110, "code": "1111", "ma20_slope_10": -0.02, "latest_price_position_pct": 0.8},
            {"dt": 20250110, "code": "2222", "ma20_slope_10": -0.03, "latest_price_position_pct": 0.2},
            {"dt": 20250111, "code": "3333", "ma20_slope_10": -0.01, "latest_price_position_pct": 0.4},
            {"dt": 20250111, "code": "4444", "ma20_slope_10": -0.01, "latest_price_position_pct": 0.6},
        ]
    )

    assert selected[20250110]["code"] == "2222"
    assert selected[20250111]["code"] == "4444"
