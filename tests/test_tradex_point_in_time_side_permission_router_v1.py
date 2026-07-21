import pandas as pd

from scripts.tradex_point_in_time_side_permission_router_v1 import permission_table, route


def test_permission_uses_only_outcomes_past_embargo():
    rows = []
    for i in range(35):
        signal = 20240101 + i
        rows.append({"side": "buy", "signal_ymd": signal, "side_return": 0.02, "outcome_known_date": signal + 1, "eligible_from_date": signal + 2})
    frame = pd.DataFrame(rows)
    result = permission_table(frame)
    at_30 = result[result.signal_ymd == 20240131].iloc[0]
    assert at_30.latest_used_outcome_known_date < at_30.signal_ymd
    assert at_30.permission_event_n < 30
    assert not at_30.permission_active


def test_inactive_means_no_candidates_and_no_fallback():
    events = pd.DataFrame([{"side": "sell", "signal_ymd": 20250102, "side_return": 0.1}])
    permissions = pd.DataFrame([{"side": "sell", "signal_ymd": 20250102, "permission_active": False, "permission_status": "INACTIVE_INSUFFICIENT_HISTORY"}])
    assert route(events, permissions).empty


def test_active_requires_fixed_pf_and_tail_gates():
    rows = []
    for i in range(35):
        ret = 0.03 if i % 4 else -0.05
        rows.append({"side": "buy", "signal_ymd": 20240101 + i, "side_return": ret, "outcome_known_date": 20230101 + i, "eligible_from_date": 20230103 + i})
    result = permission_table(pd.DataFrame(rows))
    assert result.iloc[-1].permission_active
