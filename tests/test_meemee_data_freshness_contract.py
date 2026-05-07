from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.backend.api.routers.rankings as rankings_router


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(rankings_router.router)
    return TestClient(app)


def test_rankings_multi_attaches_read_only_data_freshness_contract(monkeypatch) -> None:
    def _fake_get_rankings(tf, which, dir, limit, mode="trade", risk_mode="balanced"):
        return {
            "items": [{"code": f"{tf}001"}],
            "freshness_state": "fresh",
            "freshness_days": 0,
            "snapshot_as_of": "2026-04-20",
            "current_candidate_available": True,
            "stale": False,
        }

    monkeypatch.setattr(rankings_router.rankings_cache, "get_rankings", _fake_get_rankings)

    response = _client().get(
        "/api/rankings/multi",
        params={"which": "latest", "dir": "up", "mode": "trade", "risk_mode": "balanced", "limit": 10},
    )

    assert response.status_code == 200
    contract = response.json()["data_freshness_contract"]
    assert contract["contract_version"] == "meemee_data_freshness_v1"
    assert contract["ranking"]["snapshot_as_of"] == "2026-04-20"
    assert contract["ranking"]["snapshot_id"] == "ranking:multi:latest:up:trade:balanced:2026-04-20"
    assert contract["ranking"]["freshness_state"] == "fresh"
    assert contract["ranking"]["source"] == "rankings_cache.get_rankings_multi"
    assert contract["ranking"]["classification"] == "confirmed"
    assert contract["research"]["normal_ui_exposure_allowed"] is False


def test_rankings_session_attaches_confirmed_snapshot_contract(monkeypatch) -> None:
    def _fake_get_rankings_session_bundle(tf, which, dir, limit, mode="trade", risk_mode="balanced"):
        return {
            "confirmed": {"items": [{"code": "7203"}]},
            "provisional": {"items": []},
            "confirmed_snapshot_as_of": "2026-04-19",
            "provisional_snapshot_as_of": "2026-04-20",
            "freshness_state": "stale",
            "stale": True,
        }

    monkeypatch.setattr(
        rankings_router.rankings_cache,
        "get_rankings_session_bundle",
        _fake_get_rankings_session_bundle,
    )

    response = _client().get(
        "/api/rankings/session",
        params={"tf": "D", "which": "latest", "dir": "up", "mode": "trade", "risk_mode": "balanced", "limit": 10},
    )

    assert response.status_code == 200
    contract = response.json()["data_freshness_contract"]
    assert contract["ranking"]["snapshot_as_of"] == "2026-04-19"
    assert contract["ranking"]["snapshot_id"] == "ranking:D:latest:up:trade:balanced:2026-04-19"
    assert contract["ranking"]["freshness_state"] == "stale"
    assert contract["ranking"]["source"] == "rankings_cache.get_rankings_session_bundle"
    assert contract["ranking"]["status"] == "ready"
