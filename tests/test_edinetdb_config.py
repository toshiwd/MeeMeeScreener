from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.edinetdb.config import JST, load_config


class _FakeSecretStore:
    def __init__(self, value: str | None) -> None:
        self.value = value

    def read_secret(self, credential_name: str) -> str | None:
        return self.value


def test_default_budget_before_cutover(monkeypatch):
    monkeypatch.delenv("EDINETDB_DAILY_BUDGET", raising=False)
    cfg = load_config(datetime(2026, 3, 7, 12, 0, 0, tzinfo=JST))
    assert cfg.daily_budget == 1000


def test_default_budget_after_cutover(monkeypatch):
    monkeypatch.delenv("EDINETDB_DAILY_BUDGET", raising=False)
    cfg = load_config(datetime(2026, 3, 8, 0, 0, 0, tzinfo=JST))
    assert cfg.daily_budget == 100


def test_budget_override(monkeypatch):
    monkeypatch.setenv("EDINETDB_DAILY_BUDGET", "321")
    cfg = load_config(datetime(2026, 3, 8, 0, 0, 0, tzinfo=JST))
    assert cfg.daily_budget == 321


def test_public_and_official_defaults(monkeypatch):
    monkeypatch.delenv("EDINET_PUBLIC_COMPANY_MAP_ENABLED", raising=False)
    monkeypatch.delenv("EDINET_PUBLIC_COMPANY_MAP_URL", raising=False)
    monkeypatch.delenv("EDINET_OFFICIAL_API_ENABLED", raising=False)
    monkeypatch.delenv("EDINET_OFFICIAL_API_KEY", raising=False)
    monkeypatch.delenv("EDINET_OFFICIAL_RECENT_DAYS", raising=False)
    monkeypatch.setattr(
        "app.backend.edinetdb.config.get_default_edinet_secret_store",
        lambda: _FakeSecretStore(None),
    )
    cfg = load_config(datetime(2026, 3, 8, 0, 0, 0, tzinfo=JST))
    assert cfg.public_company_map_enabled is True
    assert cfg.public_company_map_url.endswith("Edinetcode.zip")
    assert cfg.official_api_enabled is True
    assert cfg.official_api_credential_name == "official"
    assert cfg.official_api_key is None
    assert cfg.official_recent_days == 3


def test_official_api_key_prefers_secret_store(monkeypatch):
    monkeypatch.setenv("EDINET_OFFICIAL_API_KEY", "env-secret")
    monkeypatch.setenv("EDINET_OFFICIAL_API_CREDENTIAL_NAME", "official")
    monkeypatch.setattr(
        "app.backend.edinetdb.config.get_default_edinet_secret_store",
        lambda: _FakeSecretStore("stored-secret"),
    )
    cfg = load_config(datetime(2026, 3, 8, 0, 0, 0, tzinfo=JST))
    assert cfg.official_api_key == "stored-secret"


def test_official_api_key_falls_back_to_env_when_store_missing(monkeypatch):
    monkeypatch.setenv("EDINET_OFFICIAL_API_KEY", "env-secret")
    monkeypatch.setattr(
        "app.backend.edinetdb.config.get_default_edinet_secret_store",
        lambda: _FakeSecretStore(None),
    )
    cfg = load_config(datetime(2026, 3, 8, 0, 0, 0, tzinfo=JST))
    assert cfg.official_api_key == "env-secret"
