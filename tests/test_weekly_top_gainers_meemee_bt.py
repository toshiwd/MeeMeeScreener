from __future__ import annotations

from contextlib import contextmanager

import pandas as pd

from app.backend.tools import weekly_top_gainers_meemee_bt as meemee_bt


def _synthetic_daily_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    dates = pd.bdate_range("2024-01-02", periods=60)
    for code, drift in (("A", 0.015), ("B", -0.005)):
        price = 100.0
        for dt in dates:
            open_ = price
            close = price * (1.0 + drift)
            high = max(open_, close) * (1.01 if code == "A" else 1.002)
            low = min(open_, close) * (0.99 if code == "A" else 0.998)
            rows.append(
                {
                    "code": code,
                    "ymd": int(dt.strftime("%Y%m%d")),
                    "date_dt": dt.to_pydatetime(),
                    "o": open_,
                    "h": high,
                    "l": low,
                    "c": close,
                    "v": 2000 if code == "A" else 1200,
                }
            )
            price = close
    return pd.DataFrame(rows)


def test_meemee_blend_filters_candidates_by_surface(monkeypatch) -> None:
    daily = _synthetic_daily_frame()
    signal_a = int(daily.iloc[20]["ymd"])
    signal_b = int(daily.iloc[40]["ymd"])
    entry_a = int(daily.iloc[21]["ymd"])
    entry_b = int(daily.iloc[41]["ymd"])

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

    surface = pd.DataFrame(
        [
            {
                "as_of_date": str(signal_a),
                "as_of_ymd": signal_a,
                "code": "A",
                "side": "long",
                "action_state": "enter",
                "direction_prob": 0.8,
                "expected_upside": 0.12,
                "expected_downside": 0.03,
                "invalidation_price": 98.0,
                "opportunity_score": 1.5,
                "freshness_state": "fresh",
            },
            {
                "as_of_date": str(signal_a),
                "as_of_ymd": signal_a,
                "code": "B",
                "side": "long",
                "action_state": "wait",
                "direction_prob": 0.3,
                "expected_upside": 0.02,
                "expected_downside": 0.08,
                "invalidation_price": 92.0,
                "opportunity_score": -0.5,
                "freshness_state": "fresh",
            },
            {
                "as_of_date": str(signal_b),
                "as_of_ymd": signal_b,
                "code": "A",
                "side": "long",
                "action_state": "enter",
                "direction_prob": 0.85,
                "expected_upside": 0.15,
                "expected_downside": 0.02,
                "invalidation_price": 102.0,
                "opportunity_score": 1.8,
                "freshness_state": "fresh",
            },
            {
                "as_of_date": str(signal_b),
                "as_of_ymd": signal_b,
                "code": "B",
                "side": "long",
                "action_state": "wait",
                "direction_prob": 0.25,
                "expected_upside": 0.01,
                "expected_downside": 0.10,
                "invalidation_price": 90.0,
                "opportunity_score": -0.8,
                "freshness_state": "fresh",
            },
        ]
    )

    candidates = [
        {
            "code": "A",
            "signal_week_last_ymd": signal_a,
            "entry_ymd": entry_a,
            "entry_idx": 21,
            "candidate_score": 9.0,
            "trend_4w": 0.1,
            "trend_12w": 0.2,
        },
        {
            "code": "B",
            "signal_week_last_ymd": signal_a,
            "entry_ymd": entry_a,
            "entry_idx": 21,
            "candidate_score": 9.0,
            "trend_4w": 0.05,
            "trend_12w": 0.1,
        },
        {
            "code": "A",
            "signal_week_last_ymd": signal_b,
            "entry_ymd": entry_b,
            "entry_idx": 41,
            "candidate_score": 9.0,
            "trend_4w": 0.12,
            "trend_12w": 0.25,
        },
        {
            "code": "B",
            "signal_week_last_ymd": signal_b,
            "entry_ymd": entry_b,
            "entry_idx": 41,
            "candidate_score": 9.0,
            "trend_4w": 0.04,
            "trend_12w": 0.08,
        },
    ]

    monkeypatch.setattr(meemee_bt, "get_conn", _fake_get_conn)
    monkeypatch.setattr(meemee_bt, "_load_daily_frame", lambda conn, lookback_days: daily)
    monkeypatch.setattr(meemee_bt, "_load_latest_surface_frame", lambda *args, **kwargs: surface)
    monkeypatch.setattr(meemee_bt, "_build_candidates", lambda study, histories, threshold: candidates)

    result = meemee_bt.run_weekly_top_gainers_meemee_backtest(
        config=meemee_bt.MeeMeeBlendConfig(
            lookback_days=3650,
            initial_capital=1_000_000.0,
            train_end_ymd=int(daily.iloc[30]["ymd"]),
            transaction_cost_rate=0.0,
            surface_min_opportunity_score=0.0,
            surface_min_direction_prob=0.6,
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
    assert int(result["surface_date_count"]) == 2
