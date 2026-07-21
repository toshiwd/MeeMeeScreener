import pandas as pd
import pytest

from scripts.tradex_two_sided_buy_topk_pruning_v1 import _june_metrics


def test_june_metrics_uses_equal_weight_daily_returns() -> None:
    rows = pd.DataFrame({
        "signal_ymd": [20260601, 20260601, 20260602, 20260529],
        "trade_return_h10": [0.10, -0.02, -0.03, 1.0],
    })
    result = _june_metrics(rows)
    assert result["trade_count"] == 3
    assert result["signal_days"] == 2
    assert result["daily_expectancy"] == pytest.approx(0.005)
    assert result["daily_win_rate"] == 0.5
