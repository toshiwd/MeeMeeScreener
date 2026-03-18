from __future__ import annotations

import importlib

from fastapi.testclient import TestClient


class _NoopThread:
    def __init__(self, *args, **kwargs):
        pass

    def start(self) -> None:
        return None


class _FakeStockRepo:
    def get_analysis_timeline(self, code: str, asof_dt: int | None, *, limit: int = 400):
        assert code == "2413"
        assert limit >= 400
        return [
            {
                "dt": 20260312,
                "pUp": 0.71,
                "pDown": 0.29,
                "pTurnUp": 0.62,
                "pTurnDown": 0.38,
                "ev20Net": 0.021,
                "sellPDown": 0.31,
                "sellPTurnDown": 0.36,
                "trendDown": False,
                "trendDownStrict": False,
            },
            {
                "dt": 20260313,
                "pUp": 0.32,
                "pDown": 0.68,
                "pTurnUp": 0.41,
                "pTurnDown": 0.67,
                "ev20Net": -0.018,
                "sellPDown": 0.71,
                "sellPTurnDown": 0.72,
                "trendDown": True,
                "trendDownStrict": True,
            },
        ]


def test_ticker_analysis_decisions_uses_cached_timeline(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(tmp_path / "result.duckdb"))

    import app.main as main_module
    from app.backend.api import dependencies as deps_module

    main_module = importlib.reload(main_module)

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    app = main_module.create_app()
    app.dependency_overrides[deps_module.get_stock_repo] = lambda: _FakeStockRepo()
    client = TestClient(app)

    response = client.get(
        "/api/ticker/analysis/decisions",
        params={
            "code": "2413",
            "start_dt": 20260312,
            "end_dt": 20260313,
            "risk_mode": "balanced",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [item["dt"] for item in payload["items"]] == [20260312, 20260313]
    assert [item["decision"]["tone"] for item in payload["items"]] == ["up", "down"]
