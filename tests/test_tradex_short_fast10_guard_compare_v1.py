import pandas as pd

from scripts.tradex_short_fast10_guard_compare_v1 import decide, fast10_guard, oracle_instrumentation, route


def _events(returns):
    rows = []
    for index, ret in enumerate(returns):
        signal = pd.Timestamp("2025-01-01") + pd.Timedelta(days=index * 2)
        rows.append({"code": str(2000 + index), "signal_date": signal, "entry_date": signal, "ret": ret, "rule": "r", "outcome_known_date": signal + pd.Timedelta(days=1)})
    return pd.DataFrame(rows)


def test_fast10_guard_triggers_at_eight_known_losses():
    result = fast10_guard(_events([-0.01] * 8))
    assert result["fast10_n"] == 8
    assert result["fast10_triggered"] is True


def test_fast10_guard_does_not_trigger_before_eight():
    assert fast10_guard(_events([-0.08] * 7))["fast10_triggered"] is False


def test_route_excludes_same_day_outcome_from_guard():
    events = _events([0.05] * 20)
    target_date = events.signal_date.max() + pd.Timedelta(days=2)
    future_loss = {"code": "9999", "signal_date": target_date, "entry_date": target_date, "ret": -0.08, "rule": "r", "outcome_known_date": target_date}
    events = pd.concat([events, pd.DataFrame([future_loss])], ignore_index=True)
    snapshots, _ = route(events, True)
    row = snapshots[(snapshots.signal_date == target_date) & (snapshots.rule == "r")].iloc[0]
    assert row.fast10_expectancy == 0.05
    assert bool(row.fast10_triggered) is False


def test_keep_is_blocked_when_2026_trade_days_are_below_twelve():
    dev0 = {"daily_profit_factor": 1.20, "daily_expectancy": .006, "utilization_vs_b0": 1.0, "trade_days": 159, "unique_codes": 149, "top_code_share": .03}
    dev1 = {"daily_profit_factor": 1.33, "daily_expectancy": .009, "utilization_vs_b0": .82, "trade_days": 138, "unique_codes": 126, "top_code_share": .03}
    diag0 = {"daily_profit_factor": 1.21, "daily_expectancy": .006, "utilization_vs_b0": 1.0, "trade_days": 13, "unique_codes": 16, "top_code_share": .07}
    diag1 = {"daily_profit_factor": 1.52, "daily_expectancy": .015, "utilization_vs_b0": .875, "trade_days": 11, "unique_codes": 14, "top_code_share": .08}
    inst = {"development_2019_2025": {"false_stop_rate": 0, "reaction_lag_median": 1, "reaction_lag_p90": 2}, "diagnostic_2026": {"false_stop_rate": 0, "reaction_lag_median": 1, "reaction_lag_p90": 2}}
    result, reasons = decide(dev0, dev1, diag0, diag1, inst)
    assert result == "hold"
    assert "diagnostic_keep_gate=False" in reasons


def test_keep_is_blocked_when_required_instrumentation_is_missing():
    good = {"daily_profit_factor": 1.30, "daily_expectancy": .01, "utilization_vs_b0": .80, "trade_days": 50, "unique_codes": 30, "top_code_share": .10}
    baseline = {**good, "daily_profit_factor": 1.20, "daily_expectancy": .005}
    result, reasons = decide(baseline, good, baseline, good, {})
    assert result == "hold"
    assert "required_instrumentation_complete=False" in reasons


def test_future_return_change_cannot_change_pre_outcome_state_or_selection():
    events = _events([0.05] * 20)
    future = pd.Timestamp("2025-03-01")
    row = {"code": "9999", "signal_date": future, "entry_date": future, "ret": -0.08, "rule": "r", "outcome_known_date": future + pd.Timedelta(days=20)}
    left = pd.concat([events, pd.DataFrame([row])], ignore_index=True)
    right = left.copy(); right.loc[right.code == "9999", "ret"] = 0.50
    ls, lr = route(left, True); rs, rr = route(right, True)
    cutoff = row["outcome_known_date"]
    pd.testing.assert_frame_equal(ls[ls.signal_date <= cutoff].reset_index(drop=True), rs[rs.signal_date <= cutoff].reset_index(drop=True))
    pd.testing.assert_frame_equal(lr[lr.signal_date < future].reset_index(drop=True), rr[rr.signal_date < future].reset_index(drop=True))


def test_oracle_excludes_incomplete_next_five_tail():
    events = _events([0.05] * 4)
    snapshots = pd.DataFrame({"signal_date": events.signal_date, "rule": "r", "state": ["Active", "Watch", "Active", "Watch"]})
    result = oracle_instrumentation(events, snapshots, "2025-01-01", "2025-12-31")
    assert result["eligible_stop_count"] == 0
    assert result["false_stop_rate"] is None
