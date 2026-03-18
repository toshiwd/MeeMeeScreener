from __future__ import annotations

import importlib

from fastapi.testclient import TestClient

from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import ensure_result_db


class _NoopThread:
    def __init__(self, *args, **kwargs):
        pass

    def start(self) -> None:
        return None


def test_parallel_guard_keeps_bridge_alive_while_legacy_analysis_is_disabled(monkeypatch, tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    ensure_result_db(str(result_db))
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-12_20260312T120000Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
    )

    import app.main as main_module

    main_module = importlib.reload(main_module)
    prewarm_calls = {"start": 0}
    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: prewarm_calls.__setitem__("start", prewarm_calls["start"] + 1))
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)

    client = TestClient(main_module.create_app())

    bridge = client.get("/api/analysis-bridge/status")
    assert bridge.status_code == 200
    assert bridge.json()["degraded"] is False
    assert bridge.json()["publish"]["publish_id"] == "pub_2026-03-12_20260312T120000Z_01"

    disabled = client.post("/api/jobs/ml/predict")
    assert disabled.status_code == 410
    assert disabled.json()["disabled"] is True
    assert prewarm_calls["start"] == 0
