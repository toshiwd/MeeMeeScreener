from app.backend.services.ml.ranking_analysis_quality import (
    compute_ranking_analysis_quality_snapshot,
    get_latest_prob_up_gates,
    get_ranking_analysis_review,
)


def test_compute_snapshot_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    result = compute_ranking_analysis_quality_snapshot(as_of_ymd=20260313, persist=True)
    assert result["as_of"] == 20260313
    assert result["disabled_reason"] == "legacy_analysis_disabled"
    assert result["table_health"] == []
    assert result["kpi_snapshot"] == {}
    assert result["alerts"] == []


def test_prob_up_gates_returns_none_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    assert get_latest_prob_up_gates() is None


def test_review_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    result = get_ranking_analysis_review(days=14, min_occurrence=3)
    assert result["as_of"] is None
    assert result["disabled_reason"] == "legacy_analysis_disabled"
    assert result["windowDays"] == 14
    assert result["minOccurrence"] == 3
    assert result["snapshots"] == []
    assert result["reviewTargets"] == []
    assert result["alertsFrequency"] == []
