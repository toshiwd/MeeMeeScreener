from __future__ import annotations

from app.desktop import launcher


def test_seed_ml_models_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    def _raise(*args, **kwargs):
        raise AssertionError("resolve_path should not be called")

    monkeypatch.setattr(launcher, "resolve_path", _raise)

    launcher._seed_ml_models({"data_dir": "C:/tmp/data", "stocks_db": "C:/tmp/stocks.duckdb"})


def test_has_active_ml_model_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    assert launcher._has_active_ml_model("C:/tmp/stocks.duckdb") is False


def test_register_seed_model_short_circuits_when_legacy_analysis_disabled(monkeypatch, tmp_path):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    launcher._register_seed_model(str(tmp_path / "stocks.duckdb"), model_dir, "v1")
