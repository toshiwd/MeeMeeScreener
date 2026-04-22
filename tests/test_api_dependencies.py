from __future__ import annotations

import importlib

import pytest


def _reload_dependencies_module():
    import app.backend.api.dependencies as dependencies_module

    return importlib.reload(dependencies_module)


def test_get_config_repo_requires_explicit_init_outside_dev_test(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("MEEMEE_ALLOW_IMPLICIT_REPO_INIT", raising=False)
    monkeypatch.setenv("MEEMEE_DATA_DIR", str(tmp_path / "data"))

    dependencies_module = _reload_dependencies_module()

    with pytest.raises(RuntimeError, match="init_resources"):
        dependencies_module.get_config_repo()

    dependencies_module.init_resources(str(tmp_path / "data"))
    assert dependencies_module.get_config_repo() is not None


def test_get_config_repo_allows_implicit_init_in_test_env(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("MEEMEE_DATA_DIR", str(tmp_path / "data"))

    dependencies_module = _reload_dependencies_module()

    assert dependencies_module.get_config_repo() is not None
