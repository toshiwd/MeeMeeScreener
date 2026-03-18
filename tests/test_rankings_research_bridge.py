from __future__ import annotations

import json
import os
from pathlib import Path
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services import rankings_cache
from app.core.config import config as core_config


def _reset_research_prior_cache() -> None:
    with rankings_cache._RESEARCH_PRIOR_CACHE_LOCK:  # type: ignore[attr-defined]
        rankings_cache._RESEARCH_PRIOR_CACHE["loaded_at"] = None  # type: ignore[attr-defined]
        rankings_cache._RESEARCH_PRIOR_CACHE["payload"] = None  # type: ignore[attr-defined]


def test_load_research_prior_snapshot_returns_empty_without_bridge(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MEEMEE_RESEARCH_BRIDGE_DIR", str(tmp_path / "bridge"))
    _reset_research_prior_cache()

    payload = rankings_cache._load_research_prior_snapshot()

    assert payload["run_id"] is None
    assert payload["up"] == {"asof": None, "codes": [], "rank_map": {}}
    assert payload["down"] == {"asof": None, "codes": [], "rank_map": {}}


def test_load_research_prior_snapshot_prefers_bridge_over_repo_published(monkeypatch, tmp_path: Path) -> None:
    bridge_latest = tmp_path / "bridge" / "latest"
    bridge_latest.mkdir(parents=True, exist_ok=True)
    (bridge_latest / "research_prior_snapshot.json").write_text(
        json.dumps(
            {
                "run_id": "bridge_run",
                "up": {"asof": "2026-03-12", "codes": ["1001", "2002"], "rank_map": {"1001": 1, "2002": 2}},
                "down": {"asof": "2026-03-12", "codes": ["3003"], "rank_map": {"3003": 1}},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (bridge_latest / "bridge_manifest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-12T00:00:00Z",
                "artifacts": {
                    "research_prior_snapshot.json": {
                        "source_type": "run",
                        "source_id": "bridge_run",
                        "generated_at": "2026-03-12T00:00:00Z",
                        "filename": "research_prior_snapshot.json",
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    fake_repo = tmp_path / "repo"
    fake_published = fake_repo / "published" / "latest"
    fake_published.mkdir(parents=True, exist_ok=True)
    (fake_published / "research_prior_snapshot.json").write_text(
        json.dumps(
            {
                "run_id": "repo_run",
                "up": {"asof": "2025-01-01", "codes": ["9999"], "rank_map": {"9999": 1}},
                "down": {"asof": "2025-01-01", "codes": ["8888"], "rank_map": {"8888": 1}},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("MEEMEE_RESEARCH_BRIDGE_DIR", str(tmp_path / "bridge"))
    monkeypatch.setattr(core_config, "REPO_ROOT", fake_repo)
    _reset_research_prior_cache()

    payload = rankings_cache._load_research_prior_snapshot()

    assert payload["run_id"] == "bridge_run"
    assert payload["up"]["codes"] == ["1001", "2002"]
    assert payload["down"]["rank_map"] == {"3003": 1}
