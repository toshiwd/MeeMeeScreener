import pandas as pd

from scripts.tradex_short_dual5of10_guard_compare_v1 import dual5of10_guard, route


def _events(returns):
    rows = []
    for i, ret in enumerate(returns):
        signal = pd.Timestamp("2025-01-01") + pd.Timedelta(days=i * 2)
        rows.append({"code": str(3000 + i), "signal_date": signal, "entry_date": signal, "ret": ret, "rule": "r", "outcome_known_date": signal + pd.Timedelta(days=1)})
    return pd.DataFrame(rows)


def test_guard_requires_all_ten_results():
    assert dual5of10_guard(_events([-0.01] * 9))["dual5of10_triggered"] is False


def test_guard_requires_three_recent_losses_and_pf_below_one():
    result = dual5of10_guard(_events([.01] * 5 + [-.03, -.03, -.03, .01, .01]))
    assert result["loss_count5"] == 3
    assert result["dual10_pf"] < 1
    assert result["dual5of10_triggered"] is True


def test_guard_does_not_trigger_when_pf_is_at_least_one():
    result = dual5of10_guard(_events([.10] * 5 + [-.01, -.01, -.01, .01, .01]))
    assert result["loss_count5"] == 3
    assert result["dual10_pf"] >= 1
    assert result["dual5of10_triggered"] is False


def test_future_return_change_cannot_change_pre_outcome_state_or_selection():
    events = _events([.05] * 20)
    future = pd.Timestamp("2025-03-01")
    row = {"code": "9999", "signal_date": future, "entry_date": future, "ret": -.08, "rule": "r", "outcome_known_date": future + pd.Timedelta(days=20)}
    left = pd.concat([events, pd.DataFrame([row])], ignore_index=True)
    right = left.copy(); right.loc[right.code == "9999", "ret"] = .50
    ls, lr = route(left, True); rs, rr = route(right, True)
    pd.testing.assert_frame_equal(ls[ls.signal_date <= row["outcome_known_date"]].reset_index(drop=True), rs[rs.signal_date <= row["outcome_known_date"]].reset_index(drop=True))
    pd.testing.assert_frame_equal(lr[lr.signal_date < future].reset_index(drop=True), rr[rr.signal_date < future].reset_index(drop=True))
