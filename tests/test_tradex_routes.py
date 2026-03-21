from __future__ import annotations

from fastapi.testclient import TestClient

import app.backend.api.routers.tradex as tradex_router
from app.main import create_app


def _build_client() -> TestClient:
    app = create_app()
    return TestClient(app)


def _make_candidate_bundle() -> dict[str, object]:
    return {
        "candidate_id": "candidate-001",
        "logic_key": "logic-a:v1",
        "logic_id": "logic-a",
        "logic_version": "v1",
        "logic_family": "family_a",
        "status": "approved",
        "validation_state": "ok",
        "created_at": "2026-03-20T09:00:00Z",
        "updated_at": "2026-03-20T09:00:00Z",
        "source_publish_id": "pub-001",
        "validation_summary": {
            "metrics": {
                "readiness_pass": True,
                "sample_count": 96,
                "expectancy_delta": 0.217,
                "improved_expectancy": True,
                "mae_non_worse": True,
                "adverse_move_non_worse": True,
                "stable_window": True,
                "alignment_ok": True,
                "total_score_delta": 0.31,
                "max_drawdown_delta": -0.08,
                "sample_count_delta": 12,
                "win_rate_delta": 0.045,
                "expected_value_delta": 0.217,
            },
            "notes": ["sample-note"],
        },
        "published_logic_manifest": {"logic_id": "logic-a", "logic_version": "v1"},
        "published_logic_artifact": {"path": "artifact.json"},
        "published_ranking_snapshot": {"items": []},
    }


def test_tradex_bootstrap_returns_structured_candidates(monkeypatch) -> None:
    bundle = _make_candidate_bundle()
    monkeypatch.setattr(tradex_router, "get_analysis_bridge_snapshot", lambda: {"publish": {"publish_id": "pub-001", "as_of_date": "2026-03-20", "freshness_state": "fresh"}})
    monkeypatch.setattr(
        tradex_router,
        "build_runtime_selection_snapshot",
        lambda **_kwargs: {"selected_logic_id": "logic-a", "selected_logic_version": "v1"},
    )
    monkeypatch.setattr(
        tradex_router,
        "build_publish_promotion_snapshot",
        lambda **_kwargs: {
            "champion_logic_key": "logic-a:v1",
            "default_logic_pointer": "logic-a:v1",
            "last_sync_time": "2026-03-20T09:00:00Z",
        },
    )
    monkeypatch.setattr(tradex_router, "get_internal_replay_progress", lambda: {"current_run": {"status": "running", "current_phase": "review"}})
    monkeypatch.setattr(tradex_router, "get_internal_state_eval_action_queue", lambda: {"actions": [{}, {}]})
    monkeypatch.setattr(tradex_router, "list_publish_candidate_bundles", lambda **_kwargs: [bundle])

    client = _build_client()
    response = client.get("/api/tradex/bootstrap")

    assert response.status_code == 200
    payload = response.json()
    assert payload["baseline"]["logic_id"] == "logic-a"
    assert payload["summary"]["attention_count"] == 2
    assert payload["summary"]["candidate_count"] == 1
    candidate = payload["candidates"][0]
    assert candidate["comparison_snapshot"]["metric_deltas"]["total_score_delta"] == 0.31
    assert candidate["comparison_snapshot"]["comparison_snapshot_id"]
    assert candidate["validation_result"]["status"] == "ok"


def test_tradex_adopt_rejects_snapshot_mismatch(monkeypatch) -> None:
    bundle = _make_candidate_bundle()
    baseline_publish_id = "pub-001"
    comparison = tradex_router._build_comparison_snapshot(bundle, baseline_publish_id)

    monkeypatch.setattr(tradex_router, "get_analysis_bridge_snapshot", lambda: {"publish": {"publish_id": baseline_publish_id, "as_of_date": "2026-03-20", "freshness_state": "fresh"}})
    monkeypatch.setattr(
        tradex_router,
        "build_runtime_selection_snapshot",
        lambda **_kwargs: {"selected_logic_id": "logic-a", "selected_logic_version": "v1"},
    )
    monkeypatch.setattr(
        tradex_router,
        "build_publish_promotion_snapshot",
        lambda **_kwargs: {"champion_logic_key": "logic-a:v1", "default_logic_pointer": "logic-a:v1", "last_sync_time": "2026-03-20T09:00:00Z"},
    )
    monkeypatch.setattr(tradex_router, "load_publish_candidate_bundle", lambda **_kwargs: bundle)
    monkeypatch.setattr(tradex_router, "_run_operator_mutation", lambda _action, fn: fn())
    monkeypatch.setattr(tradex_router, "promote_logic_key", lambda **_kwargs: {"ok": True, "reason": "promoted", "snapshot": {"publish_id": "pub-002"}})

    client = _build_client()
    response = client.post(
        "/api/tradex/adopt",
        json={
            "candidate_id": "candidate-001",
            "baseline_publish_id": baseline_publish_id,
            "comparison_snapshot_id": f"{comparison['comparison_snapshot_id']}-mismatch",
        },
    )

    assert response.status_code == 409
    payload = response.json()
    assert payload["detail"]["reason"] == "comparison_snapshot_mismatch"
    assert payload["detail"]["expected_comparison_snapshot_id"] == comparison["comparison_snapshot_id"]


def test_tradex_adopt_accepts_matching_contract(monkeypatch) -> None:
    bundle = _make_candidate_bundle()
    baseline_publish_id = "pub-001"
    comparison = tradex_router._build_comparison_snapshot(bundle, baseline_publish_id)

    monkeypatch.setattr(tradex_router, "get_analysis_bridge_snapshot", lambda: {"publish": {"publish_id": baseline_publish_id, "as_of_date": "2026-03-20", "freshness_state": "fresh"}})
    monkeypatch.setattr(
        tradex_router,
        "build_runtime_selection_snapshot",
        lambda **_kwargs: {"selected_logic_id": "logic-a", "selected_logic_version": "v1"},
    )
    monkeypatch.setattr(
        tradex_router,
        "build_publish_promotion_snapshot",
        lambda **_kwargs: {"champion_logic_key": "logic-a:v1", "default_logic_pointer": "logic-a:v1", "last_sync_time": "2026-03-20T09:00:00Z"},
    )
    monkeypatch.setattr(tradex_router, "load_publish_candidate_bundle", lambda **_kwargs: bundle)
    monkeypatch.setattr(tradex_router, "_run_operator_mutation", lambda _action, fn: fn())
    called = {}

    def fake_promote_logic_key(**kwargs):
        called.update(kwargs)
        return {"ok": True, "reason": "promoted", "snapshot": {"publish_id": "pub-002"}}

    monkeypatch.setattr(tradex_router, "promote_logic_key", fake_promote_logic_key)

    client = _build_client()
    response = client.post(
        "/api/tradex/adopt",
        json={
            "candidate_id": "candidate-001",
            "baseline_publish_id": baseline_publish_id,
            "comparison_snapshot_id": comparison["comparison_snapshot_id"],
            "reason": "compare-first adoption",
            "actor": "tradex-ui",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["candidate_id"] == "candidate-001"
    assert payload["baseline_publish_id"] == baseline_publish_id
    assert payload["comparison_snapshot_id"] == comparison["comparison_snapshot_id"]
    assert called["logic_key"] == "logic-a:v1"
    assert called["source"] == "api.tradex.adopt"
