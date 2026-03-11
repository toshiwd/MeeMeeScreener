from __future__ import annotations

import os
import sys
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.backend.api.routers import jobs as jobs_router


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(jobs_router.router)
    return TestClient(app)


def test_submit_toredex_live_accepts_config_override(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any] | None]] = []

    def fake_submit(job_type: str, payload: dict | None = None) -> dict[str, Any]:
        calls.append((job_type, payload))
        return {"ok": True, "job_id": "job-1"}

    monkeypatch.setattr(jobs_router, "_submit_job", fake_submit)
    client = _build_client()

    response = client.post(
        "/api/jobs/toredex/live",
        json={
            "season_id": "toredex_live_short_hybrid_shadow_20260304",
            "asOf": "2026-03-04",
            "dry_run": True,
            "operating_mode": "champion",
            "config_override": {
                "rankingMode": "hybrid",
                "sides": {"longEnabled": True, "shortEnabled": True},
                "thresholds": {
                    "entryMinUpProb": 0.56,
                    "entryMinEv": -0.01,
                    "entryMaxRevRisk": 0.70,
                    "maxNewEntriesPerDay": 2.0,
                    "newEntryMaxRank": 10.0,
                },
            },
        },
    )
    assert response.status_code == 200
    assert response.json() == {"ok": True, "job_id": "job-1"}

    assert len(calls) == 1
    job_type, payload = calls[0]
    assert job_type == "toredex_live"
    assert payload is not None
    assert payload["season_id"] == "toredex_live_short_hybrid_shadow_20260304"
    assert payload["asOf"] == "2026-03-04"
    assert payload["dry_run"] is True
    assert payload["operating_mode"] == "champion"
    assert isinstance(payload["config_override"], dict)
    assert payload["config_override"]["rankingMode"] == "hybrid"


def test_submit_toredex_live_rejects_non_object_config_override() -> None:
    client = _build_client()
    response = client.post(
        "/api/jobs/toredex/live",
        json={
            "season_id": "toredex_live_short_hybrid_shadow_20260304",
            "config_override": ["bad"],
        },
    )
    assert response.status_code == 400
    assert response.json().get("error") == "config_override must be an object"
