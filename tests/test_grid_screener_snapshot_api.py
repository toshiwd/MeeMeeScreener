from __future__ import annotations

import importlib

from fastapi.testclient import TestClient


class _NoopThread:
    def __init__(self, *args, **kwargs):
        pass

    def start(self) -> None:
        return None


def test_grid_screener_returns_snapshot_payload(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(tmp_path / "result.duckdb"))

    import app.main as main_module
    from app.backend.api import dependencies as deps_module
    from app.backend.api.routers import grid as grid_router

    main_module = importlib.reload(main_module)

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_screener_snapshot_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_screener_snapshot_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    expected_payload = {
        "items": [{"code": "9432", "name": "NTT", "stage": "WATCH", "score": 12, "reason": ""}],
        "stale": True,
        "asOf": "2026-03-13",
        "updatedAt": "2026-03-13T04:00:00Z",
        "generation": "g1",
        "lastError": "forced_build_failure",
    }
    monkeypatch.setattr(
        grid_router.screener_snapshot_service,
        "get_screener_snapshot_response",
        lambda **_kwargs: expected_payload,
    )

    app = main_module.create_app()
    app.dependency_overrides[deps_module.get_screener_repo] = lambda: object()
    app.dependency_overrides[deps_module.get_stock_repo] = lambda: object()
    client = TestClient(app)

    response = client.get("/api/grid/screener")

    assert response.status_code == 200
    assert response.json() == expected_payload
