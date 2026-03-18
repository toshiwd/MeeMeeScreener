import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.services.analysis import strategy_backtest_service


def test_walkforward_latest_read_short_circuits_when_legacy_disabled(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    called = {"conn": 0}

    class _ConnCtx:
        def __enter__(self):
            called["conn"] += 1
            return object()

        def __exit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(strategy_backtest_service, "get_conn", lambda: _ConnCtx())

    latest_run = strategy_backtest_service.get_latest_strategy_walkforward()
    latest_gate = strategy_backtest_service.get_latest_strategy_walkforward_gate()

    assert called["conn"] == 0
    assert latest_run == {
        "has_run": False,
        "disabled_reason": "legacy_analysis_disabled",
        "latest": None,
    }
    assert latest_gate == {
        "has_run": False,
        "disabled_reason": "legacy_analysis_disabled",
        "latest": None,
    }
