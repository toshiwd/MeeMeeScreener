from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.edinetdb.config import JST, load_config
from app.backend.edinetdb.jobs import (
    DailyBudget,
    _build_daily_watch_analysis_candidates,
    _resolve_daily_watch_analysis_cap,
)


def test_build_daily_watch_analysis_candidates_prioritizes_new_then_p0():
    result = _build_daily_watch_analysis_candidates(
        ordered_new=["E5938", "E1306"],
        p0=["1306", "2413", "5938"],
        mapped={
            "1306": "E1306",
            "2413": "E2413",
            "5938": "E5938",
        },
    )

    assert result == ["E5938", "E1306", "E2413"]


def test_resolve_daily_watch_analysis_cap_leaves_reserve(monkeypatch):
    monkeypatch.delenv("EDINETDB_DAILY_WATCH_ANALYSIS_ENABLED", raising=False)
    monkeypatch.delenv("EDINETDB_DAILY_WATCH_ANALYSIS_RESERVE", raising=False)
    monkeypatch.delenv("EDINETDB_DAILY_WATCH_ANALYSIS_MAX_CALLS", raising=False)
    cfg = load_config(datetime(2026, 3, 12, 9, 0, 0, tzinfo=JST))
    budget = DailyBudget(100)

    budget.used = 86
    assert _resolve_daily_watch_analysis_cap(cfg=cfg, budget=budget, candidate_count=5) == 2

    budget.used = 89
    assert _resolve_daily_watch_analysis_cap(cfg=cfg, budget=budget, candidate_count=5) == 0


def test_resolve_daily_watch_analysis_cap_can_be_disabled(monkeypatch):
    monkeypatch.setenv("EDINETDB_DAILY_WATCH_ANALYSIS_ENABLED", "0")
    cfg = load_config(datetime(2026, 3, 12, 9, 0, 0, tzinfo=JST))
    budget = DailyBudget(100)

    assert _resolve_daily_watch_analysis_cap(cfg=cfg, budget=budget, candidate_count=4) == 0
