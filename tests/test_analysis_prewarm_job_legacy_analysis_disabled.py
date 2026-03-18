from app.backend.core.analysis_prewarm_job import (
    schedule_analysis_prewarm_if_needed,
    start_analysis_prewarm_scheduler,
)


def test_schedule_analysis_prewarm_returns_none_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    monkeypatch.setenv("MEEMEE_ANALYSIS_PREWARM_ENABLED", "1")
    assert schedule_analysis_prewarm_if_needed(source="test") is None


def test_start_analysis_prewarm_scheduler_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    monkeypatch.setenv("MEEMEE_ANALYSIS_PREWARM_ENABLED", "1")
    start_analysis_prewarm_scheduler()
