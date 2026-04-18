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
    called: dict[str, object] = {}

    def _fake_get_screener_snapshot_response(**kwargs):
        called.update(kwargs)
        return expected_payload

    monkeypatch.setattr(
        grid_router.screener_snapshot_service,
        "get_screener_snapshot_response",
        _fake_get_screener_snapshot_response,
    )

    app = main_module.create_app()
    app.dependency_overrides[deps_module.get_screener_repo] = lambda: object()
    app.dependency_overrides[deps_module.get_stock_repo] = lambda: object()
    client = TestClient(app)

    response = client.get("/api/grid/screener")

    assert response.status_code == 200
    assert response.json() == expected_payload
    assert called["limit"] == 0
    assert called["force_refresh"] is False


def test_grid_ranking_fallback_exposes_display_score_sources(monkeypatch) -> None:
    from app.backend.api.routers import grid as grid_router

    def _fake_get_rankings(tf, which, direction, limit, **_kwargs):
        if direction == "up":
            return {
                "items": [
                    {
                        "code": "1111",
                        "name": "Alpha",
                        "entryScore": 0.55,
                        "hybridScore": 0.72,
                        "changePct": 0.05,
                        "asOf": "2026-03-13",
                    },
                    {
                        "code": "2222",
                        "name": "Beta",
                        "hybridScore": 0.54,
                        "changePct": 0.04,
                        "asOf": "2026-03-13",
                    },
                    {
                        "code": "3333",
                        "name": "Gamma",
                        "changePct": 0.03,
                        "asOf": "2026-03-13",
                    },
                ]
            }
        return {
            "items": [
                {
                    "code": "4444",
                    "name": "Delta",
                    "entryScore": 0.61,
                    "hybridScore": 0.51,
                    "changePct": -0.04,
                    "asOf": "2026-03-13",
                }
            ]
        }

    monkeypatch.setattr(grid_router.rankings_cache, "get_rankings", _fake_get_rankings)

    rows = grid_router._build_grid_rankings_fallback(10)  # type: ignore[attr-defined]
    by_code = {row["code"]: row for row in rows}

    assert by_code["1111"]["displayScore"] == 0.55
    assert by_code["1111"]["displayScoreSource"] == "ranking_entry"
    assert by_code["2222"]["displayScore"] == 0.54
    assert by_code["2222"]["displayScoreSource"] == "ranking_hybrid"
    assert by_code["3333"]["displayScore"] is None
    assert by_code["3333"]["displayScoreSource"] == "none"
    assert by_code["4444"]["displayScore"] == 0.61
    assert by_code["4444"]["displayScoreSource"] == "ranking_entry"


def test_grid_ranking_response_includes_display_score_contract(monkeypatch) -> None:
    from app.backend.api.routers import grid as grid_router

    def _fake_get_rankings(tf, which, direction, limit, **_kwargs):
        if direction == "up":
            return {
                "items": [
                    {
                        "code": "1111",
                        "name": "Alpha",
                        "entryScore": 0.55,
                        "hybridScore": 0.72,
                        "changePct": 0.05,
                        "asOf": "2026-03-13",
                    },
                    {
                        "code": "2222",
                        "name": "Beta",
                        "hybridScore": 0.54,
                        "changePct": 0.04,
                        "asOf": "2026-03-13",
                    },
                ]
            }
        return {
            "items": [
                {
                    "code": "4444",
                    "name": "Delta",
                    "entryScore": 0.61,
                    "hybridScore": 0.51,
                    "changePct": -0.04,
                    "asOf": "2026-03-13",
                }
            ]
        }

    monkeypatch.setattr(grid_router.rankings_cache, "get_rankings", _fake_get_rankings)

    payload = grid_router.get_ranking(limit=3)

    assert payload["up"][0]["displayScore"] == 0.55
    assert payload["up"][0]["displayScoreSource"] == "ranking_entry"
    assert payload["up"][1]["displayScore"] == 0.54
    assert payload["up"][1]["displayScoreSource"] == "ranking_hybrid"
    assert payload["down"][0]["displayScore"] == 0.61
    assert payload["down"][0]["displayScoreSource"] == "ranking_entry"
