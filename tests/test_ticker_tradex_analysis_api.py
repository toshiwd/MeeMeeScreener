from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.backend.api import dependencies
from app.backend.services import tradex_analysis_service as service


def _build_app() -> FastAPI:
    import app.backend.api.routers.ticker as ticker_router

    app = FastAPI()
    app.include_router(ticker_router.router)
    return app


class _FakeRepo:
    def get_ml_analysis_pred(self, code: str, asof_dt: int | None):
        return (
            20260319,
            0.81,
            0.07,
            0.63,
            0.31,
            0.54,
            0.23,
            None,
            None,
            None,
            None,
            None,
            None,
            0.012,
            0.010,
            None,
            None,
            "v1",
        )

    def get_sell_analysis_snapshot(self, code: str, asof_dt: int | None):
        return None


def test_tradex_detail_analysis_endpoint_rejects_when_feature_flag_disabled(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_ENABLE_TRADEX_DETAIL_ANALYSIS", "0")
    dependencies._stock_repo = _FakeRepo()

    with TestClient(_build_app()) as client:
        response = client.get("/api/ticker/tradex/analysis", params={"code": "7203"})
        assert response.status_code == 200
        payload = response.json()
        assert payload == {"available": False, "reason": "feature flag disabled", "analysis": None}


def test_tradex_detail_analysis_endpoint_returns_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("MEEMEE_ENABLE_TRADEX_DETAIL_ANALYSIS", "1")
    dependencies._stock_repo = _FakeRepo()
    import app.backend.api.routers.ticker as ticker_router

    monkeypatch.setattr(
        ticker_router,
        "build_tradex_detail_analysis_snapshot",
        lambda **kwargs: {
            "available": True,
            "reason": None,
            "analysis": {
                "symbol": kwargs["code"],
                "asof": "2026-03-19",
                "side_ratios": {"buy": 0.81, "neutral": 0.12, "sell": 0.07},
                "confidence": 0.72,
                "reasons": ["tone=up"],
                "candidate_comparisons": [],
                "publish_readiness": {"ready": False, "status": "not_evaluated", "reasons": []},
                "override_state": {"present": False, "source": None, "logic_key": None, "logic_version": None, "reason": None},
                "source": "tradex_analysis",
                "schema_version": "tradex_analysis_output_v1",
            },
        },
    )

    with TestClient(_build_app()) as client:
        response = client.get("/api/ticker/tradex/analysis", params={"code": "7203"})
        assert response.status_code == 200
        payload = response.json()
        assert payload["available"] is True
        assert payload["analysis"]["symbol"] == "7203"

