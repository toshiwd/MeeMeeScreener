from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

from app.backend.infra.files.config_repo import ConfigRepository, PUBLISH_REGISTRY_SCHEMA_VERSION
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import ensure_result_db


def _reset_repo_singletons() -> None:
    import app.backend.api.dependencies as dependencies

    dependencies._stock_repo = None
    dependencies._favorites_repo = None
    dependencies._config_repo = None
    dependencies._screener_repo = None


def _write_artifact(path: Path, content: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = content.encode("utf-8")
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def _seed_publish_state(tmp_path: Path) -> tuple[Path, Path, str, str]:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    ensure_result_db(str(result_db))
    ensure_ops_db(str(ops_db))

    champion_artifact = tmp_path / "artifacts" / "logic_family_a_v1.json"
    challenger_artifact = tmp_path / "artifacts" / "logic_family_a_v2.json"
    champion_checksum = _write_artifact(champion_artifact, "{\"logic\":\"family_a:v1\"}\n")
    challenger_checksum = _write_artifact(challenger_artifact, "{\"logic\":\"family_a:v2\"}\n")

    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-19_20260319T010000Z_01",
        as_of_date="2026-03-19",
        freshness_state="fresh",
        default_logic_pointer="logic_family_a:v1",
        bootstrap_champion=True,
        logic_artifact_uri=str(champion_artifact),
        logic_artifact_checksum=champion_checksum,
        logic_manifest={
            "logic_id": "logic_family_a",
            "logic_version": "v1",
            "logic_family": "family_a",
            "status": "published",
            "input_schema_version": "v3",
            "output_schema_version": "v3",
            "feature_spec_version": "v3",
            "required_inputs": ["confirmed_market_bars"],
            "scorer_type": "ranking",
            "params": {},
            "thresholds": {},
            "weights": {},
            "output_spec": {"rank_fields": ["code", "score"]},
            "trained_at": "2026-03-18T00:00:00Z",
            "published_at": "2026-03-19T01:00:00Z",
            "artifact_uri": str(champion_artifact),
            "checksum": champion_checksum,
        },
    )
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-19_20260319T020000Z_01",
        as_of_date="2026-03-19",
        freshness_state="fresh",
        default_logic_pointer="logic_family_a:v2",
        logic_artifact_uri=str(challenger_artifact),
        logic_artifact_checksum=challenger_checksum,
        logic_manifest={
            "logic_id": "logic_family_a",
            "logic_version": "v2",
            "logic_family": "family_a",
            "status": "published",
            "input_schema_version": "v3",
            "output_schema_version": "v3",
            "feature_spec_version": "v3",
            "required_inputs": ["confirmed_market_bars"],
            "scorer_type": "ranking",
            "params": {},
            "thresholds": {},
            "weights": {},
            "output_spec": {"rank_fields": ["code", "score"]},
            "trained_at": "2026-03-18T12:00:00Z",
            "published_at": "2026-03-19T02:00:00Z",
            "artifact_uri": str(challenger_artifact),
            "checksum": challenger_checksum,
        },
    )
    third_artifact = tmp_path / "artifacts" / "logic_family_a_v3.json"
    third_checksum = _write_artifact(third_artifact, "{\"logic\":\"family_a:v3\"}\n")
    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-19_20260319T030000Z_01",
        as_of_date="2026-03-19",
        freshness_state="fresh",
        default_logic_pointer="logic_family_a:v3",
        logic_artifact_uri=str(third_artifact),
        logic_artifact_checksum=third_checksum,
        logic_manifest={
            "logic_id": "logic_family_a",
            "logic_version": "v3",
            "logic_family": "family_a",
            "status": "published",
            "input_schema_version": "v3",
            "output_schema_version": "v3",
            "feature_spec_version": "v3",
            "required_inputs": ["confirmed_market_bars"],
            "scorer_type": "ranking",
            "params": {},
            "thresholds": {},
            "weights": {},
            "output_spec": {"rank_fields": ["code", "score"]},
            "trained_at": "2026-03-18T18:00:00Z",
            "published_at": "2026-03-19T03:00:00Z",
            "artifact_uri": str(third_artifact),
            "checksum": third_checksum,
        },
    )

    ops_conn = duckdb.connect(str(ops_db))
    try:
        ops_conn.execute(
            """
            INSERT INTO external_state_eval_readiness VALUES (
                'pub_2026-03-19_20260319T020000Z_01:readiness',
                'pub_2026-03-19_20260319T020000Z_01',
                DATE '2026-03-19',
                'logic_family_a:v1',
                'logic_family_a:v2',
                64,
                0.035,
                TRUE,
                TRUE,
                TRUE,
                TRUE,
                TRUE,
                TRUE,
                '[]',
                '{"champion_selected":18,"challenger_selected":21}',
                TIMESTAMP '2026-03-19 02:30:00'
            )
            """
        )
    finally:
        ops_conn.close()

    repo = ConfigRepository(str(data_dir))
    repo.save_publish_registry_state(
        {
            "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
            "default_logic_pointer": "logic_family_a:v1",
            "champion": {
                "logic_id": "logic_family_a",
                "logic_version": "v1",
                "logic_key": "logic_family_a:v1",
                "logic_family": "family_a",
                "artifact_uri": str(champion_artifact),
                "artifact_checksum": champion_checksum,
                "published_at": "2026-03-19T01:00:00Z",
                "status": "champion",
                "role": "champion",
            },
            "challenger": {
                "logic_id": "logic_family_a",
                "logic_version": "v2",
                "logic_key": "logic_family_a:v2",
                "logic_family": "family_a",
                "artifact_uri": str(challenger_artifact),
                "artifact_checksum": challenger_checksum,
                "published_at": "2026-03-19T02:00:00Z",
                "status": "challenger",
                "role": "challenger",
                "source_publish_id": "pub_2026-03-19_20260319T020000Z_01",
            },
            "previous_champion_logic_key": "logic_family_a:v0",
            "retired_logic_keys": ["logic_family_a:v0"],
            "promotion_history": [],
        }
    )
    return data_dir, result_db, ops_db, "logic_family_a:v1", "logic_family_a:v2"


def _load_app(monkeypatch, data_dir: Path, result_db: Path, ops_db: Path):
    monkeypatch.setenv("MEEMEE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    monkeypatch.setenv("MEEMEE_OPS_DB_PATH", str(ops_db))
    _reset_repo_singletons()

    import app.main as main_module

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    return main_module


def test_publish_promotion_updates_champion_and_runtime_selection(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)

    with TestClient(main_module.create_app()) as client:
        before = client.get("/api/system/runtime-selection")
        assert before.status_code == 200
        before_payload = before.json()
        assert before_payload["source_of_truth"] == "external_analysis"
        assert before_payload["default_logic_pointer"] == champion_key
        assert before_payload["publish_registry_state"]["champion_logic_key"] == champion_key
        assert before_payload["publish_registry_state"]["source_of_truth"] == "external_analysis"

        response = client.post(
            "/api/system/publish/promote",
            json={"logicKey": challenger_key, "reason": "validation passed", "actor": "codex_test"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["action"] == "promote"
        assert payload["champion"]["logic_key"] == challenger_key
        assert payload["validation"]["gate_pass"] is True

        after = client.get("/api/system/runtime-selection")
        assert after.status_code == 200
        after_payload = after.json()
        assert after_payload["source_of_truth"] == "external_analysis"
        assert after_payload["default_logic_pointer"] == challenger_key
        assert after_payload["publish_registry_state"]["champion_logic_key"] == challenger_key
        assert after_payload["publish_registry_state"]["source_of_truth"] == "external_analysis"
        assert after_payload["resolved_source"] == "default_logic_pointer"
        assert after_payload["selected_logic_key"] == challenger_key

    audit_path = data_dir / "runtime_selection" / "publish_promotion_audit.jsonl"
    audit_lines = [line for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert audit_lines
    last_event = json.loads(audit_lines[-1])
    assert last_event["action"] == "promote"
    assert last_event["previous_logic_key"] == champion_key
    assert last_event["new_logic_key"] == challenger_key


def test_publish_demotion_retires_non_champion(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)

    with TestClient(main_module.create_app()) as client:
        response = client.post(
            "/api/system/publish/demote",
            json={"logicKey": challenger_key, "reason": "manual retire", "actor": "codex_test"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["action"] == "demote"

        state = client.get("/api/system/publish/state")
        assert state.status_code == 200
        state_payload = state.json()
        assert state_payload["source_of_truth"] == "external_analysis"
        assert state_payload["champion_logic_key"] == champion_key
        assert challenger_key in state_payload["retired_logic_keys"]

    audit_path = data_dir / "runtime_selection" / "publish_promotion_audit.jsonl"
    audit_lines = [line for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(json.loads(line)["action"] == "demote" for line in audit_lines)


def test_publish_rollback_restores_previous_champion(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)

    with TestClient(main_module.create_app()) as client:
        promote_response = client.post(
            "/api/system/publish/promote",
            json={"logicKey": challenger_key, "reason": "validation passed", "actor": "codex_test"},
        )
        assert promote_response.status_code == 200

        rollback_response = client.post(
            "/api/system/publish/rollback",
            json={"reason": "rollback to stable", "actor": "codex_test"},
        )
        assert rollback_response.status_code == 200
        rollback_payload = rollback_response.json()
        assert rollback_payload["ok"] is True
        assert rollback_payload["action"] == "rollback"

        state = client.get("/api/system/runtime-selection")
        assert state.status_code == 200
        payload = state.json()
        assert payload["source_of_truth"] == "external_analysis"
        assert payload["default_logic_pointer"] == champion_key
        assert payload["publish_registry_state"]["champion_logic_key"] == champion_key


def test_runtime_selection_uses_local_mirror_when_external_registry_unavailable(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, _ = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)
    mirror_path = data_dir / "config" / "publish_registry.json"
    mirror_state = json.loads(mirror_path.read_text(encoding="utf-8"))
    mirror_state["source_of_truth"] = "local_mirror"
    mirror_state["registry_sync_state"] = "mirror_fallback"
    mirror_state["degraded"] = True
    mirror_path.write_text(json.dumps(mirror_state, ensure_ascii=False, indent=2), encoding="utf-8")

    import app.backend.services.runtime_selection_service as runtime_selection_service

    monkeypatch.setattr(
        runtime_selection_service,
        "load_external_publish_registry_state",
        lambda **_kwargs: {"source_of_truth": "external_analysis", "degraded": True, "registry_sync_state": "unavailable"},
    )

    with TestClient(main_module.create_app()) as client:
        response = client.get("/api/system/runtime-selection")
        assert response.status_code == 200
        payload = response.json()
        assert payload["source_of_truth"] == "local_mirror"
        assert payload["registry_sync_state"] == "mirror_fallback"
        assert payload["degraded"] is True
        assert payload["default_logic_pointer"] == champion_key


def test_publish_promotion_fails_when_external_registry_write_fails(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, _, challenger_key = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)

    import app.backend.services.publish_promotion_service as promotion_service

    monkeypatch.setattr(
        promotion_service,
        "save_external_publish_registry_state",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("external registry unavailable")),
    )

    with TestClient(main_module.create_app()) as client:
        response = client.post(
            "/api/system/publish/promote",
            json={"logicKey": challenger_key, "reason": "validation passed", "actor": "codex_test"},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["reason"] == "external_registry_write_failed"


def test_publish_promotion_rejects_invalid_logic_key(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, _, _ = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)

    with TestClient(main_module.create_app()) as client:
        response = client.post(
            "/api/system/publish/promote",
            json={"logicKey": "missing_logic:v9", "reason": "invalid target", "actor": "codex_test"},
        )
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["reason"] == "logic_key_not_available"


def test_publish_queue_supports_multiple_challengers(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)
    third_key = "logic_family_a:v3"

    with TestClient(main_module.create_app()) as client:
        enqueue_first = client.post(
            "/api/system/publish/challenger/enqueue",
            json={"logicKey": challenger_key, "reason": "queue first", "actor": "codex_test"},
        )
        assert enqueue_first.status_code == 200
        enqueue_second = client.post(
            "/api/system/publish/challenger/enqueue",
            json={"logicKey": third_key, "reason": "queue second", "actor": "codex_test"},
        )
        assert enqueue_second.status_code == 200

        queue_response = client.get("/api/system/publish/queue")
        assert queue_response.status_code == 200
        queue_payload = queue_response.json()
        assert queue_payload["bootstrap_rule"] == "explicit_champion_flag"
        assert queue_payload["champion"]["logic_key"] == champion_key
        assert queue_payload["challenger_logic_keys"] == [challenger_key, third_key]
        assert [item["queue_order"] for item in queue_payload["challengers"]] == [1, 2]
