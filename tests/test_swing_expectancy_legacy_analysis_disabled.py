from __future__ import annotations

from app.backend.services.analysis import swing_expectancy_service


def test_refresh_swing_setup_stats_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    def _raise():
        raise AssertionError("get_conn should not be called")

    monkeypatch.setattr(swing_expectancy_service, "get_conn", _raise)

    result = swing_expectancy_service.refresh_swing_setup_stats(as_of_ymd=20260313)

    assert result == {
        "ok": False,
        "reason": "legacy_analysis_disabled",
        "as_of_ymd": None,
        "rows": 0,
    }


def test_ensure_latest_swing_setup_stats_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    def _raise():
        raise AssertionError("get_conn should not be called")

    monkeypatch.setattr(swing_expectancy_service, "get_conn", _raise)

    result = swing_expectancy_service.ensure_latest_swing_setup_stats()

    assert result == {
        "ok": False,
        "reason": "legacy_analysis_disabled",
        "as_of_ymd": None,
        "rows": 0,
    }
