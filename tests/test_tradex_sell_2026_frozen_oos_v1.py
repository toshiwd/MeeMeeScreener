import pandas as pd

from scripts.tradex_sell_2026_frozen_oos_v1 import evaluate, metrics


def test_metrics_include_pf_payoff_tail_and_frequency():
    rows = pd.DataFrame({"signal_ymd": [20260102, 20260102, 20260109], "ret": [.10, -.05, .10]})
    result = metrics(rows)
    assert result["n"] == 3
    assert result["profit_factor"] == 4.0
    assert result["win_rate"] == 2 / 3
    assert result["payoff_ratio"] == 2.0
    assert result["p05_ret"] < 0


def test_2026_is_hold_when_n_is_below_fixed_gate():
    rows = []
    for split, n, ymd in (("train", 88, 20210104), ("validation", 65, 20230104), ("test", 89, 20250106)):
        rows.extend({"split": split, "signal_ymd": ymd, "ret": .01} for _ in range(n))
    rows.extend({"split": None, "signal_ymd": 20260130, "ret": .10} for _ in range(14))
    result = evaluate(pd.DataFrame(rows))
    assert result["prior_split_reproduction"]["all_match"] is True
    assert result["shadow_2026"]["n"] == 14
    assert result["decision"] == "hold_insufficient_n"
