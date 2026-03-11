from __future__ import annotations

from contextlib import contextmanager

import pandas as pd

from app.backend.services import strategy_backtest_service


@contextmanager
def _dummy_conn():
    yield object()


def test_run_strategy_walkforward_truncates_when_probe_drawdown_breaks_gate(monkeypatch) -> None:
    market = pd.DataFrame(
        [
            {"dt": 20200101, "code": "2413", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.0, "v": 1000.0},
            {"dt": 20200102, "code": "2413", "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.0, "v": 1000.0},
        ]
    )
    features = pd.DataFrame(
        [
            {"dt": 20200101, "code": "2413", "signal_ready": True, "dt_date": strategy_backtest_service._dt_to_date(20200101)},
            {"dt": 20200102, "code": "2413", "signal_ready": True, "dt_date": strategy_backtest_service._dt_to_date(20200102)},
        ]
    )
    segments = [
        {"month": "2020-01", "start_dt": 20200101, "end_dt": 20200101},
        {"month": "2020-02", "start_dt": 20200102, "end_dt": 20200102},
        {"month": "2020-03", "start_dt": 20200102, "end_dt": 20200102},
    ]

    monkeypatch.setattr(strategy_backtest_service, "get_conn", lambda: _dummy_conn())
    monkeypatch.setattr(strategy_backtest_service, "_ensure_backtest_schema", lambda conn: None)
    monkeypatch.setattr(strategy_backtest_service, "_load_market_frame", lambda *args, **kwargs: market.copy())
    monkeypatch.setattr(strategy_backtest_service, "_prepare_feature_frame", lambda df, cfg: features.copy())
    monkeypatch.setattr(strategy_backtest_service, "_build_month_segments", lambda frame: list(segments))
    monkeypatch.setattr(strategy_backtest_service, "_load_event_rows", lambda conn: ([], []))

    call_state = {"count": 0}

    def fake_simulate(frame, cfg, event_block_set):
        call_state["count"] += 1
        max_dd = -0.05 if call_state["count"] % 2 == 1 else -0.20
        return {
            "metrics": {
                "days": 1,
                "trade_events": 10,
                "win_rate": 0.5,
                "avg_ret_net": 0.1,
                "profit_factor": 1.0,
                "max_drawdown_unit": max_dd,
                "total_realized_unit_pnl": -0.1,
                "final_equity_unit": -0.1,
                "side_breakdown": {},
                "setup_breakdown": {},
                "code_breakdown": {},
                "sector_breakdown": {},
                "hedge_breakdown": {},
            },
            "monthly": [],
            "yearly_daily": [],
            "yearly_trades": [],
            "entry_monthly": [],
            "daily": [],
            "sample_trades": [],
        }

    monkeypatch.setattr(strategy_backtest_service, "_simulate", fake_simulate)

    report = strategy_backtest_service.run_strategy_walkforward(
        dry_run=True,
        max_codes=10,
        train_months=1,
        test_months=1,
        step_months=1,
        min_windows=1,
        stop_on_oos_worst_max_drawdown_below=-0.12,
    )

    assert report["execution"]["truncated"] is True
    assert report["execution"]["truncated_reason"] == "oos_worst_max_drawdown_below_threshold"
    assert report["summary"]["executed_windows"] == 1
    assert len(report["windows"]) == 1
