from app.backend.services.ml.ml_service import (
    enforce_live_guard,
    get_latest_live_guard_status,
    get_ml_status,
)


def test_enforce_live_guard_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    result = enforce_live_guard()
    assert result["checked"] is False
    assert result["passed"] is True
    assert result["action"] == "disabled"
    assert result["reason"] == "legacy_analysis_disabled"


def test_get_latest_live_guard_status_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    result = get_latest_live_guard_status()
    assert result["has_check"] is False
    assert result["disabled_reason"] == "legacy_analysis_disabled"
    assert result["latest"] is None


def test_get_ml_status_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    result = get_ml_status()
    assert result["has_active_model"] is False
    assert result["disabled_reason"] == "legacy_analysis_disabled"
    assert result["active_model"] is None
    assert result["latest_prediction"] is None
    assert result["latest_training_audit"] is None
    assert result["latest_live_guard_audit"] is None
