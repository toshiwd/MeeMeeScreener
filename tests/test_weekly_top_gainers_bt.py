from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime

import pandas as pd

from app.backend.tools import weekly_top_gainers_bt as bt
from app.backend.tools.weekly_top_gainers_study import build_weekly_top_gainers_study_frame


def _synthetic_daily_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2024-01-02", periods=120)
    for code, start, drift, vol_base in (("A", 100.0, 0.012, 2000), ("B", 100.0, 0.000, 1200)):
        price = float(start)
        for i, dt in enumerate(dates):
            if code == "A":
                open_ = price
                close = price * (1.0 + drift)
                high = max(open_, close) * 1.01
                low = min(open_, close) * 0.99
                volume = vol_base + i * 15
            else:
                open_ = price
                close = price * (1.0 + drift)
                high = max(open_, close) * 1.002
                low = min(open_, close) * 0.998
                volume = vol_base
            rows.append(
                {
                    "code": code,
                    "ymd": int(dt.strftime("%Y%m%d")),
                    "date_dt": dt.to_pydatetime(),
                    "o": open_,
                    "h": high,
                    "l": low,
                    "c": close,
                    "v": volume,
                }
            )
            price = close
    return pd.DataFrame(rows)


def test_simulate_trade_takes_profit_before_max_hold() -> None:
    daily = _synthetic_daily_frame()
    study = build_weekly_top_gainers_study_frame(daily, top_n=1)
    histories = bt._build_histories(daily)
    trade = None
    for _, row in study.loc[study["code"] == "A"].sort_values("week_last_ymd").iterrows():
        entry_idx = bt._next_entry_idx(histories["A"], int(row["week_last_ymd"]))
        if entry_idx is None or entry_idx + 6 >= len(histories["A"].ymds):
            continue
        trade = bt._simulate_trade(
            histories["A"],
            entry_idx,
            tp=0.02,
            sl=0.05,
            max_hold_days=10,
            cost=0.0,
        )
        if trade is not None:
            break
    assert trade is not None
    assert trade is not None
    assert trade["exit_reason"] == "take_profit"
    assert trade["net_ret"] > 0
    assert trade["hold_days"] <= 10


def test_portfolio_backtest_grows_capital_on_synthetic_uptrend(monkeypatch) -> None:
    daily = _synthetic_daily_frame()

    class _Result:
        def __init__(self, value: int) -> None:
            self._value = value

        def fetchone(self):
            return (self._value,)

    class _Conn:
        def execute(self, *args, **kwargs):
            return _Result(1)

    @contextmanager
    def _fake_get_conn():
        yield _Conn()

    monkeypatch.setattr(bt, "get_conn", _fake_get_conn)
    monkeypatch.setattr(bt, "_load_daily_frame", lambda conn, lookback_days: daily)

    result = bt.run_weekly_top_gainers_portfolio_backtest(
        config=bt.BacktestConfig(
            lookback_days=3650,
            initial_capital=1_000_000.0,
            train_end_ymd=20240430,
            transaction_cost_rate=0.0,
        ),
        score_thresholds=(0,),
        take_profit_pcts=(0.03,),
        stop_loss_pcts=(0.05,),
        max_hold_days=(5,),
        max_positions=(1,),
    )

    assert result["ok"] is True
    assert result["best_train"] is not None
    assert result["best_test"] is not None
    assert float(result["best_train"]["final_capital"]) > 1_000_000.0
    assert float(result["best_test"]["final_capital"]) > 1_000_000.0
    assert float(result["best_test"]["win_rate"]) > 0.5
