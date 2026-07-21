import pandas as pd

from scripts.tradex_frozen_two_sided_union_2026_v1 import branching, metrics


def test_metrics_uses_zero_return_on_inactive_ranking_dates():
    rows = pd.DataFrame({"signal_ymd": [20260105, 20260105], "net_ret": [0.10, -0.05]})
    result = metrics(rows, [20260105, 20260106])
    assert result["n"] == 2
    assert result["expectancy"] == 0.025
    assert result["calendar_expectancy"] == 0.0125
    assert result["daily_profit_factor"] is None


def test_metrics_reports_normalized_drawdown_and_tail():
    rows = pd.DataFrame({"signal_ymd": [20260105, 20260106, 20260107], "net_ret": [0.10, -0.051, 0.02]})
    result = metrics(rows, [20260105, 20260106, 20260107])
    assert result["profit_factor"] > 2
    assert result["p05"] < -0.04
    assert result["max_drawdown_return"] == -0.051000000000000045


def test_branching_labels_missing_preselection_boards():
    candidate = pd.DataFrame({"signal_ymd": [20260105], "side": ["buy"], "source_rank": [1], "code": ["1"]})
    baseline = pd.DataFrame({"signal_ymd": [20260105], "side": ["buy"], "rank": [1], "code": ["2"]})
    result = branching(candidate, baseline, [20260105])
    assert result["changed_top3_members_count"] == 2
    assert result["changed_top5_members_count"] is None
    assert result["changed_date_side_count"] == 1
