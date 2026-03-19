from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient


def _build_app() -> FastAPI:
    import app.backend.api.routers.ticker as ticker_router

    app = FastAPI()
    app.include_router(ticker_router.router)
    return app


def test_tradex_list_summary_endpoint_returns_batch_snapshot(monkeypatch) -> None:
    import app.backend.api.routers.ticker as ticker_router

    monkeypatch.setattr(
        ticker_router,
        "build_tradex_list_summary_snapshot",
        lambda **kwargs: {
            "available": True,
            "reason": None,
            "scope": kwargs.get("scope"),
            "items": [
                {
                    "code": "7203",
                    "asof": "2026-03-19",
                    "available": True,
                    "reason": None,
                    "dominant_tone": "buy",
                    "confidence": 0.84,
                    "publish_readiness": {
                        "ready": True,
                        "status": "ready",
                        "reasons": [],
                        "candidate_key": None,
                        "approved": True,
                    },
                    "reasons": ["tone=up", "pattern=breakout"],
                }
            ],
        },
    )

    with TestClient(_build_app()) as client:
        response = client.post(
            "/api/ticker/tradex/summary",
            json={"scope": "visible", "items": [{"code": "7203", "asof": "2026-03-19"}]},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["available"] is True
        assert payload["scope"] == "visible"
        assert payload["items"][0]["code"] == "7203"
        assert payload["items"][0]["dominant_tone"] == "buy"

