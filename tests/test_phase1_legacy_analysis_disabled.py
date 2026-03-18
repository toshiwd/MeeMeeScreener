from __future__ import annotations

import importlib

from fastapi.testclient import TestClient


class _NoopThread:
    def __init__(self, *args, **kwargs):
        pass

    def start(self) -> None:
        return None


def test_legacy_analysis_endpoints_are_disabled_in_phase1(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(tmp_path / "result.duckdb"))

    import app.main as main_module

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

    client = TestClient(main_module.create_app())

    for path in (
        "/api/phase/rebuild",
        "/api/jobs/ml/train",
        "/api/jobs/ml/predict",
        "/api/jobs/analysis/backfill-missing",
        "/api/jobs/ml/live-guard",
    ):
        response = client.post(path)
        assert response.status_code == 410
        payload = response.json()
        assert payload["disabled"] is True
        assert payload["error"] == "legacy_analysis_disabled"

    status_response = client.get("/api/jobs/ml/status")
    assert status_response.status_code == 200
    assert status_response.json()["disabled"] is True

