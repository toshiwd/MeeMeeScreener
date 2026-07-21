from scripts.tradex_long_fresh_operational_board_v1 import evaluate_managed_entry, new_entry_action


def bars(closes):
    return [{"date": 1700000000 + index * 86400, "c": close} for index, close in enumerate(closes)]


def entry(high=True):
    return {"code": "6724", "signal_date": "2026-07-17", "entry_date": "2026-07-21", "entry_price": 100.0, "high_tail_risk": high}


def holding(issue=False):
    return {"long_qty": 100.0, "has_issue": issue, "issue_note": "bad" if issue else None}


def test_high_risk_day5_minus2_exits_at_close():
    result = evaluate_managed_entry(entry(), bars([100, 99, 99, 99, 98.3]), holding())
    assert result["action"] == "EXIT_REVIEW_CLOSE"
    assert result["day5_return_after_cost_pct"] <= -2.0


def test_low_risk_same_path_keeps_monitoring():
    result = evaluate_managed_entry(entry(False), bars([100, 99, 99, 99, 98.3]), holding())
    assert result["action"] == "HOLD_MONITOR"


def test_missed_day5_action_fails_closed():
    result = evaluate_managed_entry(entry(), bars([100, 99, 99, 99, 98.3, 101]), holding())
    assert result["action"] == "STOP_MISSED_EXIT_REVIEW"


def test_twenty_sessions_requests_exit_review():
    result = evaluate_managed_entry(entry(False), bars([101] * 20), holding())
    assert result["action"] == "EXIT_REVIEW_CLOSE"
    assert result["reason"] == "maximum_20_sessions_reached"


def test_position_mismatch_stops():
    result = evaluate_managed_entry(entry(), bars([100]), None)
    assert result["action"] == "STOP"
    assert result["reason"] == "registered_entry_not_found_in_positions_live"


def test_missing_entry_price_stops():
    broken = entry(); broken.pop("entry_price")
    result = evaluate_managed_entry(broken, bars([100]), holding())
    assert result["action"] == "STOP"
    assert result["reason"] == "entry_ledger_missing_fields"


def test_entry_action_requires_same_day_chart_review():
    assert new_entry_action(held=False, chart_usable=False, chart_status=None) == "CHART_REVIEW_REQUIRED"
    assert new_entry_action(held=False, chart_usable=True, chart_status="Starter") == "ENTRY_REVIEW_NEXT_OPEN"
    assert new_entry_action(held=False, chart_usable=True, chart_status="Avoid") == "AVOID_NO_ENTRY"


def test_held_name_never_becomes_new_entry():
    assert new_entry_action(held=True, chart_usable=True, chart_status="Starter") == "HOLDING_REVIEW_NO_NEW_ENTRY"
