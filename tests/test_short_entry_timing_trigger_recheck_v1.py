from scripts.tradex_short_entry_timing_trigger_recheck_v1 import _evaluate


def _row():
    return {
        "trigger_plan": {
            "entry_review_trigger_break_low": 100.0,
            "entry_review_trigger_close_below": 103.0,
            "invalidate_if_high_breaks": 110.0,
            "hard_invalidate_if_above": 115.0,
        }
    }


def _bar(*, high, low, close):
    return {
        "ymd": 20260703,
        "source": "yahoo",
        "open": 105.0,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1000.0,
    }


def test_evaluate_waits_when_no_future_bar():
    result = _evaluate(_row(), [])

    assert result["trigger_status"] == "waiting_next_bar"
    assert result["evaluated_bar"] is None


def test_evaluate_hard_invalidates_before_other_conditions():
    result = _evaluate(_row(), [_bar(high=116.0, low=99.0, close=102.0)])

    assert result["trigger_status"] == "hard_invalidated"


def test_evaluate_invalidates_on_provisional_high_break():
    result = _evaluate(_row(), [_bar(high=111.0, low=99.0, close=102.0)])

    assert result["trigger_status"] == "invalidated"


def test_evaluate_strong_rejection_requires_low_break_and_close_confirm():
    result = _evaluate(_row(), [_bar(high=109.0, low=99.0, close=102.0)])

    assert result["trigger_status"] == "triggered_strong_rejection"


def test_evaluate_low_break_without_close_confirm_is_intraday_only():
    result = _evaluate(_row(), [_bar(high=109.0, low=99.0, close=104.0)])

    assert result["trigger_status"] == "triggered_intraday_low_break_only"


def test_evaluate_close_confirm_without_low_break_is_close_only():
    result = _evaluate(_row(), [_bar(high=109.0, low=100.5, close=102.0)])

    assert result["trigger_status"] == "triggered_close_rejection_only"


def test_evaluate_still_waiting_when_no_boundary_crossed():
    result = _evaluate(_row(), [_bar(high=109.0, low=100.5, close=104.0)])

    assert result["trigger_status"] == "still_waiting"
