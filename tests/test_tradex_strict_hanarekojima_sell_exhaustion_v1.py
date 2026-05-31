from scripts.tradex_strict_hanarekojima_sell_exhaustion_v1 import (
    ENTRY_CONVENTIONS,
    PATTERN_LABEL,
    _convention_decision,
    _entry_index,
    _rollup_decision,
    _strict_features,
)


def _row(open_: float, high: float, low: float, close: float, ymd: int = 20250101) -> dict[str, float | int]:
    return {"ymd": ymd, "o": open_, "h": high, "l": low, "c": close, "v": 1000}


def _compare(top5_mean: float, hit: float = 0.0, bad: float = 0.0, severe: float = 0.0, top10_mean: float = 0.0, changed: int = 25):
    return {
        "top5": {
            "changed_member_count_total": changed,
            "additive_delta": {
                "forward_return_20_mean": top5_mean,
                "hit_rate_20": hit,
                "bad_loser_rate_20": bad,
                "severe_loser_rate_20": severe,
            },
        },
        "top10": {"additive_delta": {"forward_return_20_mean": top10_mean}},
    }


def test_strict_features_match_text_contract():
    rows = [_row(120 - index, 121 - index, 119 - index, 120 - index) for index in range(30)]
    rows.append(_row(88, 91, 85, 90))
    rows.append(_row(83, 88, 80, 86))

    features = _strict_features(rows)

    assert PATTERN_LABEL == "strict_hanarekojima_sell_exhaustion_v1"
    assert features["matched"] is True
    assert features["ma_down"] is True
    assert features["ma_stack_down"] is True
    assert features["day1_gap_down"] is True
    assert features["day1_wick_guard"] is True
    assert features["day2_open_below_day1_low"] is True
    assert features["day2_wick_guard"] is True


def test_strict_features_reject_extreme_wick_and_coma_body():
    base = [_row(120 - index, 121 - index, 119 - index, 120 - index) for index in range(30)]
    extreme = base + [_row(88, 91, 85, 90), _row(83, 104, 80, 86)]
    coma = base + [_row(88, 91, 85, 90), _row(83, 95, 80, 83.5)]

    assert _strict_features(extreme)["matched"] is False
    assert _strict_features(coma)["matched"] is False


def test_entry_index_conventions():
    bars = [_row(100, 101, 99, 100, 20250101 + index) for index in range(10)]
    bars[4] = _row(80, 86, 79, 82, 20250105)
    bars[5] = _row(83, 85, 82, 84, 20250106)
    bars[6] = _row(84, 88, 83, 87, 20250107)

    assert ENTRY_CONVENTIONS == (
        "signal_day_close_entry",
        "next_close_above_signal_high_entry",
        "ma5_reclaim_entry",
    )
    assert _entry_index(bars + bars * 4, 4, "signal_day_close_entry", 86) == 4
    assert _entry_index(bars + bars * 4, 4, "next_close_above_signal_high_entry", 86) == 6


def test_convention_decision_keep_drop_and_hold():
    assert _convention_decision(_compare(0.01, hit=0.01, bad=-0.01, severe=0.0), {"oos_selected_additive_date_count": 10})[
        "judgment"
    ] == "keep_for_next_probe"
    assert _convention_decision(_compare(0.01, hit=-0.01, bad=0.0, severe=0.0), {"oos_selected_additive_date_count": 10})[
        "reason_type"
    ] == "top5_mean_improves_but_hit_or_adverse_worsens"
    assert _convention_decision(_compare(0.01, changed=19), {"oos_selected_additive_date_count": 10})["judgment"] == "hold"


def test_rollup_keeps_only_confirmation_entry():
    per_entry = {
        "signal_day_close_entry": {"decision": {"judgment": "drop"}},
        "next_close_above_signal_high_entry": {"decision": {"judgment": "keep_for_next_probe"}},
        "ma5_reclaim_entry": {"decision": {"judgment": "drop"}},
    }

    assert _rollup_decision(per_entry) == {
        "judgment": "keep_confirmation_entry",
        "reason_type": "confirmation_entry_passes_while_broad_route_remains_drop",
    }
