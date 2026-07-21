import pandas as pd

from scripts.tradex_point_in_time_side_priority_top3_v1 import _interleave, select_top3


def _events():
    rows = []
    for side in ("buy", "sell"):
        for rank in (1, 2, 3):
            rows.append({"signal_ymd": 20250102, "side": side, "rank": rank, "code": f"{side}{rank}", "split": "validation", "side_return": 0.01})
    return pd.DataFrame(rows)


def test_fixed_baseline_interleave_is_buy_sell_buy():
    result = _interleave(_events(), "buy")
    assert result.code.tolist() == ["buy1", "sell1", "buy2"]


def test_health_priority_changes_order_without_suppressing_candidates():
    permissions = pd.DataFrame([
        {"signal_ymd": 20250102, "side": "buy", "permission_pf": 1.1, "permission_expectancy": 0.01, "permission_cvar10": -0.05},
        {"signal_ymd": 20250102, "side": "sell", "permission_pf": 1.5, "permission_expectancy": 0.02, "permission_cvar10": -0.05},
    ])
    baseline, challenger, _ = select_top3(_events(), permissions)
    assert baseline.code.tolist() == ["buy1", "sell1", "buy2"]
    assert challenger.code.tolist() == ["sell1", "buy1", "sell2"]
    assert len(challenger) == 3
