import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.services.analysis import strategy_backtest_service


def test_walkforward_research_snapshot_short_circuits_when_legacy_disabled(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    called = {"conn": 0}

    class _ConnCtx:
        def __enter__(self):
            called["conn"] += 1
            return object()

        def __exit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setattr(strategy_backtest_service, "get_conn", lambda: _ConnCtx())

    save_result = strategy_backtest_service.save_daily_walkforward_research_snapshot(snapshot_date=20260313)
    latest_result = strategy_backtest_service.get_latest_strategy_walkforward_research_snapshot()
    prune_result = strategy_backtest_service.prune_strategy_walkforward_history()

    assert called["conn"] == 0
    assert save_result == {
        "saved": False,
        "snapshot_date": 20260313,
        "source_run_id": None,
        "reason": "legacy_analysis_disabled",
    }
    assert latest_result == {
        "has_snapshot": False,
        "disabled_reason": "legacy_analysis_disabled",
        "latest": None,
    }
    assert prune_result["deleted_total"] == 0
    assert prune_result["skipped_reason"] == "legacy_analysis_disabled"
