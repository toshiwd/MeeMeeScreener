from __future__ import annotations

from pathlib import Path

from app.desktop import launcher
from app.desktop import runtime_paths


def test_normal_launch_uses_user_data_dir(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("MEEMEE_DEV", raising=False)
    monkeypatch.delenv("MEEMEE_DEV_MODE", raising=False)
    monkeypatch.delenv("MEEMEE_SELFTEST", raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("MEEMEE_DATA_DIR", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    monkeypatch.setattr(runtime_paths, "is_portable_mode", lambda: False)

    paths = launcher._prepare_appdata()

    expected_root = Path(tmp_path) / "MeeMeeScreener"
    expected_data_dir = expected_root / "data"
    assert Path(paths["data_dir"]) == expected_data_dir
    assert Path(paths["txt_dir"]) == expected_data_dir / "txt"
    assert Path(paths["config_dir"]) == expected_data_dir / "config"

    launcher._configure_environment(paths)
    assert launcher.os.environ["APP_ENV"] == "prod"
    assert launcher.os.environ["MEEMEE_DATA_DIR"] == str(expected_data_dir)


def test_dev_launch_keeps_dev_data_dir(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_DEV", "1")
    monkeypatch.delenv("MEEMEE_DEV_MODE", raising=False)
    monkeypatch.delenv("MEEMEE_SELFTEST", raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("MEEMEE_DATA_DIR", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    monkeypatch.setattr(runtime_paths, "is_portable_mode", lambda: False)

    paths = launcher._prepare_appdata()

    expected_root = Path(tmp_path) / "MeeMeeScreener-dev"
    expected_data_dir = expected_root / "data"
    assert Path(paths["data_dir"]) == expected_data_dir

    launcher._configure_environment(paths)
    assert launcher.os.environ["APP_ENV"] == "dev"
    assert launcher.os.environ["MEEMEE_DATA_DIR"] == str(expected_data_dir)
