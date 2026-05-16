from __future__ import annotations

from app.desktop import launcher


def test_packaged_defaults_keep_ranking_warmup_enabled(monkeypatch):
    monkeypatch.setattr(launcher.sys, "frozen", True, raising=False)
    env = {"APP_ENV": "production"}

    launcher._apply_packaged_backend_defaults(env)

    assert env["MEEMEE_RANKINGS_WARMUP_ENABLED"] == "1"
    assert env["MEEMEE_RANKINGS_RESULT_WARMUP_DELAY_SEC"] == "0"
    assert env["MEEMEE_RANKINGS_WARMUP_DELAY_SEC"] == "120"
    assert env["MEEMEE_EDINET_AUTO_START_ENABLED"] == "0"
    assert env["MEEMEE_ANALYSIS_PREWARM_ENABLED"] == "0"


def test_packaged_defaults_respect_ranking_warmup_opt_out(monkeypatch):
    monkeypatch.setattr(launcher.sys, "frozen", True, raising=False)
    env = {
        "APP_ENV": "production",
        "MEEMEE_RANKINGS_WARMUP_ENABLED": "0",
        "MEEMEE_RANKINGS_RESULT_WARMUP_DELAY_SEC": "5",
        "MEEMEE_RANKINGS_WARMUP_DELAY_SEC": "30",
    }

    launcher._apply_packaged_backend_defaults(env)

    assert env["MEEMEE_RANKINGS_WARMUP_ENABLED"] == "0"
    assert env["MEEMEE_RANKINGS_RESULT_WARMUP_DELAY_SEC"] == "5"
    assert env["MEEMEE_RANKINGS_WARMUP_DELAY_SEC"] == "30"


def test_desktop_stocks_db_path_stays_under_selected_data_dir(tmp_path):
    data_dir = tmp_path / "MeeMeeScreener" / "data"

    assert launcher._desktop_stocks_db_path(data_dir) == data_dir / "stocks.duckdb"


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
