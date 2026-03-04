from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services import strategy_backtest_service


def _build_source_summary(worst_dd: float) -> dict[str, float]:
    return {
        "oos_total_realized_unit_pnl": 1.2,
        "oos_mean_profit_factor": 1.20,
        "oos_positive_window_ratio": 0.70,
        "oos_worst_max_drawdown_unit": worst_dd,
    }


def test_walkforward_gate_fails_when_worst_drawdown_is_below_threshold() -> None:
    report = strategy_backtest_service._build_walkforward_gate_report(  # type: ignore[attr-defined]
        gate_id="gate-1",
        created_at=datetime(2026, 3, 4, tzinfo=timezone.utc),
        source_run_id="run-1",
        source_finished_at=datetime(2026, 3, 4, tzinfo=timezone.utc),
        source_status="success",
        source_summary=_build_source_summary(-0.15),
        source_windowing={},
        min_oos_total_realized_unit_pnl=0.0,
        min_oos_mean_profit_factor=1.05,
        min_oos_positive_window_ratio=0.40,
        min_oos_worst_max_drawdown_unit=-0.12,
        note=None,
    )

    assert report["passed"] is False
    assert report["checks"]["oos_worst_max_drawdown_unit"]["pass"] is False
    assert report["checks"]["oos_worst_max_drawdown_unit"]["threshold"] == -0.12


def test_walkforward_gate_passes_when_worst_drawdown_meets_threshold() -> None:
    report = strategy_backtest_service._build_walkforward_gate_report(  # type: ignore[attr-defined]
        gate_id="gate-2",
        created_at=datetime(2026, 3, 4, tzinfo=timezone.utc),
        source_run_id="run-2",
        source_finished_at=datetime(2026, 3, 4, tzinfo=timezone.utc),
        source_status="success",
        source_summary=_build_source_summary(-0.10),
        source_windowing={},
        min_oos_total_realized_unit_pnl=0.0,
        min_oos_mean_profit_factor=1.05,
        min_oos_positive_window_ratio=0.40,
        min_oos_worst_max_drawdown_unit=-0.12,
        note=None,
    )

    assert report["passed"] is True
    assert report["checks"]["oos_worst_max_drawdown_unit"]["pass"] is True

