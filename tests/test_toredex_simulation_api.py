from __future__ import annotations

import os
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.backend.api.routers import toredex as toredex_router


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(toredex_router.router)
    return TestClient(app)


def test_validate_simulation_api_success_and_bounds(monkeypatch) -> None:
    calls: list[tuple[int, int]] = []

    def fake_service(*, principal_jpy: int, limit: int) -> dict:
        calls.append((principal_jpy, limit))
        return {
            "principal_jpy": principal_jpy,
            "filters": {"limit": limit},
            "summary": {
                "count": 1,
                "avg": {"season_id": None, "net_cum_return_pct": 1.0, "final_jpy": 101, "gain_jpy": 1},
                "median": {"season_id": None, "net_cum_return_pct": 1.0, "final_jpy": 101, "gain_jpy": 1},
                "best": {"season_id": "validate_a", "net_cum_return_pct": 1.0, "final_jpy": 101, "gain_jpy": 1},
                "worst": {"season_id": "validate_a", "net_cum_return_pct": 1.0, "final_jpy": 101, "gain_jpy": 1},
            },
            "items": [],
        }

    monkeypatch.setattr(
        toredex_router.toredex_simulation_service,
        "get_validate_simulation",
        fake_service,
    )
    client = _build_client()

    res_default = client.get("/api/toredex/simulation/validate")
    assert res_default.status_code == 200
    assert res_default.json()["principal_jpy"] == 10_000_000
    assert calls[-1] == (10_000_000, 30)

    res_min = client.get("/api/toredex/simulation/validate", params={"limit": 1})
    assert res_min.status_code == 200
    assert calls[-1] == (10_000_000, 1)

    res_max = client.get("/api/toredex/simulation/validate", params={"limit": 200})
    assert res_max.status_code == 200
    assert calls[-1] == (10_000_000, 200)


def test_validate_simulation_api_invalid_limits() -> None:
    client = _build_client()

    assert client.get("/api/toredex/simulation/validate", params={"limit": 0}).status_code == 422
    assert client.get("/api/toredex/simulation/validate", params={"limit": 201}).status_code == 422
    assert client.get("/api/toredex/simulation/validate", params={"limit": "abc"}).status_code == 422
