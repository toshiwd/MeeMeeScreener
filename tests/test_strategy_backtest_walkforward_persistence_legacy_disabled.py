from __future__ import annotations

import os
import sys
from contextlib import contextmanager

import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.services.analysis import strategy_backtest_service


@contextmanager
def _dummy_conn():
    yield object()


def test_run_strategy_walkforward_skips_persist_when_legacy_disabled(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    saved = {"runs": 0}

    monkeypatch.setattr(strategy_backtest_service, "get_conn", _dummy_conn)
    monkeypatch.setattr(strategy_backtest_service, "_ensure_backtest_schema", lambda conn: None)
    monkeypatch.setattr(
        strategy_backtest_service,
        "_load_market_frame",
        lambda conn, start_dt=None, end_dt=None, max_codes=None: pd.DataFrame(
            [{"code": "1301", "dt": 20260313, "signal_ready": True}]
        ),
    )
    monkeypatch.setattr(strategy_backtest_service, "_prepare_feature_frame", lambda market, cfg: market.copy())
    monkeypatch.setattr(
        strategy_backtest_service,
        "_build_month_segments",
        lambda features: [
            {"month": "2025-01", "start_dt": 20250106, "end_dt": 20250131},
            {"month": "2025-02", "start_dt": 20250203, "end_dt": 20250228},
        ],
    )
    monkeypatch.setattr(strategy_backtest_service, "_load_event_rows", lambda conn: ([], []))
    monkeypatch.setattr(strategy_backtest_service, "_build_event_block_set", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        strategy_backtest_service,
        "_simulate",
        lambda *args, **kwargs: {"ret_net": 0.1, "max_drawdown_unit": -0.01, "trade_events": []},
    )
    monkeypatch.setattr(strategy_backtest_service, "_compact_metrics", lambda result: result)
    monkeypatch.setattr(
        strategy_backtest_service,
        "_summarize_walkforward_windows",
        lambda windows: {"oos_total_realized_unit_pnl": 0.1},
    )
    monkeypatch.setattr(strategy_backtest_service, "_build_walkforward_attribution", lambda windows: {})
    monkeypatch.setattr(
        strategy_backtest_service,
        "_save_walkforward_run",
        lambda *args, **kwargs: saved.__setitem__("runs", saved["runs"] + 1),
    )

    report = strategy_backtest_service.run_strategy_walkforward(
        train_months=1,
        test_months=1,
        step_months=1,
        dry_run=False,
    )

    assert report["status"] == "success"
    assert saved["runs"] == 0


def test_run_strategy_walkforward_gate_skips_persist_when_legacy_disabled(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    saved = {"gates": 0}

    monkeypatch.setattr(strategy_backtest_service, "_save_walkforward_gate_report", lambda *args, **kwargs: saved.__setitem__("gates", saved["gates"] + 1))

    report = strategy_backtest_service.run_strategy_walkforward_gate(
        dry_run=False,
        source_run_id="swf_test",
        source_status="success",
        source_report={
            "summary": {
                "oos_total_realized_unit_pnl": 0.2,
                "oos_mean_profit_factor": 1.2,
                "oos_positive_window_ratio": 0.6,
                "oos_worst_max_drawdown_unit": -0.05,
            },
            "windowing": {"train_months": 24, "test_months": 3, "step_months": 12},
        },
    )

    assert report["status"] == "pass"
    assert saved["gates"] == 0
