from __future__ import annotations

from app.backend.services.analysis import analysis_backfill_service
from app.backend.services.ml import ml_service


def test_predict_functions_do_not_regrow_ml_features_when_legacy_disabled(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    def _raise(*_args, **_kwargs):
        raise AssertionError("refresh_ml_feature_table should not be called")

    monkeypatch.setattr(ml_service, "refresh_ml_feature_table", _raise)

    monthly = ml_service.predict_monthly_for_dt(20260313)
    assert monthly["disabled"] is True
    assert monthly["rows"] == 0

    bulk = ml_service.predict_for_dates_bulk(dates=[20260312, 20260313], chunk_size_days=10)
    assert bulk["disabled"] is True
    assert bulk["rows_total"] == 0
    assert bulk["predicted_dates"] == []

    daily = ml_service.predict_for_dt(20260313)
    assert daily["disabled"] is True
    assert daily["rows"] == 0
    assert daily["monthly"]["disabled"] is True


def test_backfill_service_does_not_regrow_ml_features_when_legacy_disabled(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    def _raise(*_args, **_kwargs):
        raise AssertionError("refresh_ml_feature_table should not be called")

    monkeypatch.setattr(ml_service, "refresh_ml_feature_table", _raise)

    coverage = analysis_backfill_service.inspect_analysis_backfill_coverage(lookback_days=30)
    assert coverage["disabled"] is True
    assert coverage["target_dates"] == []
    assert coverage["covered"] is True

    result = analysis_backfill_service.backfill_missing_analysis_history(lookback_days=30)
    assert result["disabled"] is True
    assert result["ok"] is True
    assert result["predicted_rows_total"] == 0
