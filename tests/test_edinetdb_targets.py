from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.edinetdb import targets


def test_normalize_sec_code():
    assert targets.normalize_sec_code("7203") == "7203"
    assert targets.normalize_sec_code(" 7203.T ") == "7203"
    assert targets.normalize_sec_code("abc") is None


def test_load_ranking_codes_fallback(monkeypatch):
    monkeypatch.setattr(
        targets,
        "load_ranking_codes_from_stock_scores",
        lambda _db_path, _limit: [],
    )
    monkeypatch.setattr(
        targets,
        "load_ranking_codes_from_rankings_cache",
        lambda _limit: ["1301", "7203", "1301"],
    )
    out = targets.load_ranking_codes("dummy.duckdb", 10)
    assert out == ["1301", "7203"]


def test_load_ranking_codes_keeps_order(monkeypatch):
    monkeypatch.setattr(
        targets,
        "load_ranking_codes_from_stock_scores",
        lambda _db_path, _limit: ["7203", "1301", "7203", "6758"],
    )
    monkeypatch.setattr(
        targets,
        "load_ranking_codes_from_rankings_cache",
        lambda _limit: ["0000"],
    )
    out = targets.load_ranking_codes("dummy.duckdb", 10)
    assert out == ["7203", "1301", "6758"]
