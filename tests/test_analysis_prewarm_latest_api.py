from __future__ import annotations

import importlib

from fastapi.testclient import TestClient


class _NoopThread:
    def __init__(self, *args, **kwargs):
        pass

    def start(self) -> None:
        return None


def test_analysis_prewarm_latest_endpoint_is_available_when_legacy_analysis_is_disabled(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(tmp_path / "result.duckdb"))

    import app.main as main_module
    import app.backend.api.routers.jobs as jobs_module

    main_module = importlib.reload(main_module)
    jobs_module = importlib.reload(jobs_module)

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    monkeypatch.setattr(
        "app.backend.services.analysis.analysis_backfill_service.backfill_missing_analysis_history",
        lambda **_kwargs: {
            "ok": True,
            "message": "ml=1/1 sell=1 phase=skip",
            "predicted_dates": [20260313],
            "sell_refreshed_dates": [20260313],
            "errors": [],
        },
    )

    client = TestClient(main_module.create_app())

    response = client.post("/api/jobs/analysis/prewarm-latest")
    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "mode": "sync",
        "message": "ml=1/1 sell=1 phase=skip",
        "predicted_dates": [20260313],
        "sell_refreshed_dates": [20260313],
        "errors": [],
    }
