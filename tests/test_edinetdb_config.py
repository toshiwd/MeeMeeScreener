from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.edinetdb.config import JST, load_config


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
