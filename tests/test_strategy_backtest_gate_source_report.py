from __future__ import annotations

import os
import sys
from contextlib import contextmanager

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.services.analysis import strategy_backtest_service


@contextmanager
def _dummy_conn():
    yield object()


def test_walkforward_gate_uses_source_report_without_loading_latest_run(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "0")
    called = {"conn": 0, "saved": 0}

    @contextmanager
    def _conn_ctx():
        called["conn"] += 1
        yield object()

    monkeypatch.setattr(strategy_backtest_service, "get_conn", _conn_ctx)
    monkeypatch.setattr(strategy_backtest_service, "_ensure_walkforward_schema", lambda conn: None)
    monkeypatch.setattr(strategy_backtest_service, "_save_walkforward_gate_report", lambda *args, **kwargs: called.__setitem__("saved", called["saved"] + 1))

    result = strategy_backtest_service.run_strategy_walkforward_gate(
        min_oos_total_realized_unit_pnl=0.0,
        min_oos_mean_profit_factor=1.0,
        min_oos_positive_window_ratio=0.0,
        min_oos_worst_max_drawdown_unit=-1.0,
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

    assert called["conn"] == 1
    assert called["saved"] == 1
    assert result["passed"] is True
    assert result["source"]["run_id"] == "swf_test"
