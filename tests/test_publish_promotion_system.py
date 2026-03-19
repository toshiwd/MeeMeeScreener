from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

import app.backend.api.routers.system as system_router
from app.backend.infra.files.config_repo import ConfigRepository, PUBLISH_REGISTRY_SCHEMA_VERSION
from app.backend.core.publish_candidate_maintenance_job import run_publish_candidate_maintenance_cycle
from app.backend.services.operator_mutation_lock import OperatorMutationBusyError
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.results.publish_candidates import (
    backfill_publish_candidate_bundles,
    cleanup_publish_candidate_maintenance_state,
    build_publish_candidate_bundle,
    load_publish_candidate_maintenance_state,
    sweep_publish_candidate_snapshots,
)
from external_analysis.results.publish import publish_result
from external_analysis.results.publish_registry import save_publish_registry_state as save_external_publish_registry_state
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
    mirror_state = {
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
    repo.save_publish_registry_state(mirror_state)
    save_external_publish_registry_state(
        db_path=str(result_db),
        state={
            **mirror_state,
            "source_revision": "seed:publish_registry",
            "bootstrap_rule": "explicit_champion_flag",
            "registry_sync_state": "synced",
            "sync_state": "synced",
            "sync_message": "seeded_external_registry",
            "challengers": [
                {
                    "logic_id": "logic_family_a",
                    "logic_version": "v2",
                    "logic_key": "logic_family_a:v2",
                    "logic_family": "family_a",
                    "artifact_uri": str(challenger_artifact),
                    "artifact_checksum": challenger_checksum,
                    "queued_at": "2026-03-19T02:00:00Z",
                    "promotion_state": "queued",
                    "queue_order": 1,
                    "validation_state": "approved",
                    "status": "challenger",
                    "role": "challenger",
                    "source_publish_id": "pub_2026-03-19_20260319T020000Z_01",
                }
            ],
            "challenger": {
                "logic_id": "logic_family_a",
                "logic_version": "v2",
                "logic_key": "logic_family_a:v2",
                "logic_family": "family_a",
                "artifact_uri": str(challenger_artifact),
                "artifact_checksum": challenger_checksum,
                "queued_at": "2026-03-19T02:00:00Z",
                "promotion_state": "queued",
                "queue_order": 1,
                "validation_state": "approved",
                "status": "challenger",
                "role": "challenger",
                "source_publish_id": "pub_2026-03-19_20260319T020000Z_01",
            },
            "challenger_logic_keys": ["logic_family_a:v2"],
        },
        sync_state="synced",
        degraded=False,
        source_revision="seed:publish_registry",
        sync_message="seeded_external_registry",
    )
    readiness_payload = {
        "source": "external_analysis_shadow",
        "as_of_date": "2026-03-19",
        "champion_version": "logic_family_a:v1",
        "challenger_version": "logic_family_a:v2",
        "sample_count": 64,
        "expectancy_delta": 0.035,
        "improved_expectancy": True,
        "mae_non_worse": True,
        "adverse_move_non_worse": True,
        "stable_window": True,
        "alignment_ok": True,
        "readiness_pass": True,
        "reason_codes": [],
        "summary": {"champion_selected": 18, "challenger_selected": 21},
        "created_at": "2026-03-19T02:30:00Z",
    }
    build_publish_candidate_bundle(
        db_path=str(result_db),
        publish_id="pub_2026-03-19_20260319T020000Z_01",
        readiness=readiness_payload,
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
        approve = client.post(
            f"/api/system/publish/candidates/{challenger_key}/approve",
            json={"reason": "validated", "actor": "codex_test"},
        )
        assert approve.status_code == 200
        approve_payload = approve.json()
        assert approve_payload["ok"] is True
        assert approve_payload["bundle"]["status"] == "approved"

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

        candidate_after_promote = client.get(f"/api/system/publish/candidates/{challenger_key}")
        assert candidate_after_promote.status_code == 200
        assert candidate_after_promote.json()["candidate"]["status"] == "promoted"

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
        approve = client.post(
            f"/api/system/publish/candidates/{challenger_key}/approve",
            json={"reason": "validated", "actor": "codex_test"},
        )
        assert approve.status_code == 200
        response = client.post(
            "/api/system/publish/demote",
            json={"logicKey": challenger_key, "reason": "manual retire", "actor": "codex_test"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["action"] == "demote"

        candidate_after_demote = client.get(f"/api/system/publish/candidates/{challenger_key}")
        assert candidate_after_demote.status_code == 200
        assert candidate_after_demote.json()["candidate"]["status"] == "retired"

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
        approve = client.post(
            f"/api/system/publish/candidates/{challenger_key}/approve",
            json={"reason": "validated", "actor": "codex_test"},
        )
        assert approve.status_code == 200
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


def test_publish_and_runtime_reads_use_cached_snapshots_during_operator_mutation(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)

    with TestClient(main_module.create_app()) as client:
        client.app.state.publish_promotion_snapshot = {
            "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
            "source_of_truth": "external_analysis",
            "registry_sync_state": "in_sync",
            "degraded": False,
            "last_sync_time": "2026-03-19T02:45:00Z",
            "bootstrap_rule": "explicit_champion_flag",
            "champion_logic_key": champion_key,
            "challenger_logic_keys": [challenger_key],
            "challengers": [],
            "default_logic_pointer": champion_key,
        }
        client.app.state.runtime_selection_snapshot = {
            "schema_version": "logic_selection_v1",
            "snapshot_created_at": "2026-03-19T02:45:00Z",
            "source_of_truth": "external_analysis",
            "registry_sync_state": "in_sync",
            "resolved_source": "default_logic_pointer",
            "selected_logic_key": champion_key,
            "selected_logic_id": "logic_family_a",
            "selected_logic_version": "v1",
            "logic_key": champion_key,
            "artifact_uri": "artifact.json",
            "bootstrap_rule": "explicit_champion_flag",
            "degraded": False,
            "override_present": False,
            "last_known_good_present": False,
        }
        monkeypatch.setattr(system_router, "is_operator_mutation_active", lambda: True)
        monkeypatch.setattr(
            system_router,
            "build_publish_promotion_snapshot",
            lambda **_kwargs: (_ for _ in ()).throw(AssertionError("publish snapshot should use cache")),
        )
        monkeypatch.setattr(
            system_router,
            "build_runtime_selection_snapshot",
            lambda **_kwargs: (_ for _ in ()).throw(AssertionError("runtime snapshot should use cache")),
        )

        publish_state = client.get("/api/system/publish/state")
        assert publish_state.status_code == 200
        publish_payload = publish_state.json()
        assert publish_payload["champion_logic_key"] == champion_key
        assert "operator_mutation_observability" in publish_payload

        runtime_state = client.get("/api/system/runtime-selection")
        assert runtime_state.status_code == 200
        runtime_payload = runtime_state.json()
        assert runtime_payload["selected_logic_key"] == champion_key
        assert "operator_mutation_observability" in runtime_payload


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
        assert payload["registry_sync_state"] == "external_unreachable"
        assert payload["degraded"] is True
        assert payload["default_logic_pointer"] == champion_key


def test_publish_mirror_normalize_repairs_legacy_mirror_from_external(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    mirror_path = data_dir / "config" / "publish_registry.json"
    mirror_state = json.loads(mirror_path.read_text(encoding="utf-8"))
    mirror_state["schema_version"] = "publish_registry_legacy"
    mirror_state["challenger"].pop("queue_order", None)
    mirror_state["challengers"] = [mirror_state["challenger"]]
    mirror_state["challengers_json"] = [mirror_state["challenger"]]
    mirror_path.write_text(json.dumps(mirror_state, ensure_ascii=False, indent=2), encoding="utf-8")

    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)
    with TestClient(main_module.create_app()) as client:
        list_response = client.get("/api/system/publish/candidates")
        assert list_response.status_code == 200
        list_payload = list_response.json()
        assert list_payload["ok"] is True
        assert list_payload["count"] >= 1

        candidate_response = client.get(f"/api/system/publish/candidates/{challenger_key}")
        assert candidate_response.status_code == 200
        candidate_payload = candidate_response.json()
        assert candidate_payload["ok"] is True
        assert candidate_payload["candidate"]["logic_key"] == challenger_key
        assert candidate_payload["candidate"]["metadata"]["ranking_snapshot_policy"] == "creation_time_if_rows_present"
        assert candidate_payload["candidate"]["published_ranking_snapshot"] is None

        response = client.post(
            "/api/system/publish/mirror/normalize",
            json={"reason": "repair legacy mirror", "actor": "codex_test"},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["source_of_truth"] == "external_analysis"
        assert payload["mirror_normalized"] is True

    repaired_state = json.loads(mirror_path.read_text(encoding="utf-8"))
    assert repaired_state["schema_version"] == PUBLISH_REGISTRY_SCHEMA_VERSION
    assert repaired_state["source_of_truth"] == "local_mirror"
    assert repaired_state["mirror_of"] == "external_analysis"
    assert repaired_state["challenger_logic_keys"] == [challenger_key]
    assert repaired_state["challengers"][0]["queue_order"] == 1


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
        approve = client.post(
            f"/api/system/publish/candidates/{challenger_key}/approve",
            json={"reason": "validated", "actor": "codex_test"},
        )
        assert approve.status_code == 200
        response = client.post(
            "/api/system/publish/promote",
            json={"logicKey": challenger_key, "reason": "validation passed", "actor": "codex_test"},
        )
        assert response.status_code == 503
        assert response.json()["detail"]["reason"] == "external_registry_write_conflict"


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
        assert detail["reason"] in {"candidate_bundle_not_found", "logic_key_not_available"}


def test_publish_candidate_reject_blocks_promote(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)

    with TestClient(main_module.create_app()) as client:
        reject = client.post(
            f"/api/system/publish/candidates/{challenger_key}/reject",
            json={"reason": "validation blocked", "actor": "codex_test"},
        )
        assert reject.status_code == 200
        reject_payload = reject.json()
        assert reject_payload["ok"] is True
        assert reject_payload["bundle"]["status"] == "rejected"

        candidate_after_reject = client.get(f"/api/system/publish/candidates/{challenger_key}")
        assert candidate_after_reject.status_code == 200
        assert candidate_after_reject.json()["candidate"]["status"] == "rejected"

        promote = client.post(
            "/api/system/publish/promote",
            json={"logicKey": challenger_key, "reason": "should fail", "actor": "codex_test"},
        )
        assert promote.status_code == 400
        assert promote.json()["detail"]["reason"] in {"candidate_not_approved", "candidate_bundle_invalid"}


def test_publish_candidate_validation_summary_required(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    conn = duckdb.connect(str(result_db))
    try:
        conn.execute(
            "UPDATE publish_candidate_bundle SET validation_summary = '{}' WHERE candidate_id = ?",
            [challenger_key],
        )
    finally:
        conn.close()
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)

    with TestClient(main_module.create_app()) as client:
        approve = client.post(
            f"/api/system/publish/candidates/{challenger_key}/approve",
            json={"reason": "validation incomplete", "actor": "codex_test"},
        )
        assert approve.status_code == 400
        detail = approve.json()["detail"]
        assert detail["reason"] in {"candidate_bundle_invalid", "candidate_validation_invalid"}


def test_publish_candidate_backfill_leaves_missing_validation_summary_non_promotable(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    conn = duckdb.connect(str(result_db))
    try:
        conn.execute(
            "UPDATE publish_candidate_bundle SET validation_summary = '{}' WHERE candidate_id = ?",
            [challenger_key],
        )
    finally:
        conn.close()

    result = backfill_publish_candidate_bundles(db_path=str(result_db))
    assert result["ok"] is True
    assert result["repaired"] == 0
    assert result["failed"] >= 1

    conn = duckdb.connect(str(result_db))
    try:
        row = conn.execute(
            "SELECT candidate_status, validation_summary FROM publish_candidate_bundle WHERE candidate_id = ?",
            [challenger_key],
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == "candidate"
    assert isinstance(json.loads(row[1]), dict)
    assert json.loads(row[1]) == {}

    maintenance = load_publish_candidate_maintenance_state(db_path=str(result_db))
    assert maintenance["candidate_backfill_last_run"] is not None
    assert maintenance["candidate_backfill_summary"]["updated"] == 0
    assert maintenance["candidate_backfill_summary"]["failed"] >= 1
    assert maintenance["non_promotable_legacy_count"] >= 1


def test_publish_candidate_maintenance_dry_run_tracks_state_without_mutation(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    conn = duckdb.connect(str(result_db))
    try:
        conn.execute(
            "UPDATE publish_candidate_bundle SET validation_summary = '{}' WHERE candidate_id = ?",
            [challenger_key],
        )
    finally:
        conn.close()

    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)
    with TestClient(main_module.create_app()) as client:
        response = client.post(
            "/api/system/publish/maintenance/backfill",
            json={"dryRun": True},
        )
        assert response.status_code == 200
        result = response.json()
        assert result["ok"] is True
        assert result["dry_run"] is True
        assert result["updated"] == 0
        assert result["failed"] >= 1

    conn = duckdb.connect(str(result_db))
    try:
        row = conn.execute(
            "SELECT validation_summary FROM publish_candidate_bundle WHERE candidate_id = ?",
            [challenger_key],
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert json.loads(row[0]) == {}

    with TestClient(main_module.create_app()) as client:
        state_response = client.get("/api/system/publish/state")
        assert state_response.status_code == 200
        state_payload = state_response.json()
        assert "candidate_backfill_last_run" in state_payload
        assert "non_promotable_legacy_count" in state_payload
        assert "maintenance_degraded" in state_payload


def test_publish_candidate_non_promotable_legacy_count_is_observable(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)
    conn = duckdb.connect(str(result_db))
    try:
        conn.execute(
            "UPDATE publish_candidate_bundle SET validation_summary = '{}' WHERE candidate_id = ?",
            [challenger_key],
        )
    finally:
        conn.close()

    result = backfill_publish_candidate_bundles(db_path=str(result_db))
    assert result["ok"] is True
    assert result["failed"] >= 1

    with TestClient(main_module.create_app()) as client:
        state_response = client.get("/api/system/publish/state")
        assert state_response.status_code == 200
        state_payload = state_response.json()
        assert state_payload["non_promotable_legacy_count"] >= 1
        assert state_payload["maintenance_state"]["non_promotable_legacy_count"] >= 1
        assert state_payload["maintenance_degraded"] is True
        assert state_payload["registry_sync_state"] in {"in_sync", "mirror_stale", "mirror_legacy", "external_unreachable", "external_invalid"}


def test_publish_candidate_maintenance_cycle_records_last_run(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    conn = duckdb.connect(str(result_db))
    try:
        conn.execute(
            "UPDATE publish_candidate_bundle SET validation_summary = '{}' WHERE candidate_id = ?",
            [challenger_key],
        )
    finally:
        conn.close()

    result = run_publish_candidate_maintenance_cycle(
        result_db_path=str(result_db),
        dry_run=True,
        source="test_cycle",
    )
    assert result["ok"] is True
    assert result["dry_run"] is True
    assert result["backfill"]["dry_run"] is True
    assert result["snapshot_sweep"]["dry_run"] is True

    maintenance = load_publish_candidate_maintenance_state(db_path=str(result_db))
    assert maintenance["candidate_backfill_last_run"] is not None
    assert maintenance["snapshot_sweep_last_run"] is not None
    assert maintenance["non_promotable_legacy_count"] >= 0
    assert maintenance["details_json"]["last_cycle"]["source"] == "test_cycle"


def test_publish_candidate_cleanup_removes_deprecated_ops_residue(tmp_path) -> None:
    _, result_db, _, _, challenger_key = _seed_publish_state(tmp_path)
    conn = duckdb.connect(str(result_db))
    try:
        conn.execute(
            "UPDATE publish_candidate_bundle SET validation_summary = '{}' WHERE candidate_id = ?",
            [challenger_key],
        )
    finally:
        conn.close()
    conn = duckdb.connect(str(result_db))
    try:
        conn.execute("ALTER TABLE publish_maintenance_state ADD COLUMN IF NOT EXISTS ops_fallback_enabled BOOLEAN")
        conn.execute("ALTER TABLE publish_maintenance_state ADD COLUMN IF NOT EXISTS ops_fallback_hit_count BIGINT")
        conn.execute("ALTER TABLE publish_maintenance_state ADD COLUMN IF NOT EXISTS ops_fallback_last_used_at TIMESTAMP")
        conn.execute("ALTER TABLE publish_maintenance_state ADD COLUMN IF NOT EXISTS ops_fallback_last_target TEXT")
        conn.execute(
            """
            INSERT OR REPLACE INTO publish_maintenance_state (
                maintenance_name, schema_version, candidate_backfill_last_run, candidate_backfill_summary,
                snapshot_sweep_last_run, snapshot_sweep_summary, non_promotable_legacy_count,
                maintenance_degraded, updated_at, details_json, ops_fallback_enabled,
                ops_fallback_hit_count, ops_fallback_last_used_at, ops_fallback_last_target
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "publish_candidates",
                "publish_candidate_bundle_v1",
                None,
                "{}",
                None,
                "{}",
                1,
                False,
                "2026-03-19T00:00:00Z",
                json.dumps({"ops_fallback_enabled": True, "ops_fallback_hit_count": 3}),
                True,
                3,
                "2026-03-19T00:00:00Z",
                challenger_key,
            ],
        )
    finally:
        conn.close()

    result = cleanup_publish_candidate_maintenance_state(db_path=str(result_db))
    assert result["ok"] is True
    assert result["updated"] == 1
    assert "ops_fallback_enabled" in result["dropped_columns"] or result["failed_columns"] == []


def test_publish_candidate_maintenance_cycle_skips_when_operator_busy(monkeypatch, tmp_path) -> None:
    _, result_db, _, _, _ = _seed_publish_state(tmp_path)

    class _BusyScope:
        def __init__(self, *args, **kwargs) -> None:
            self.action = args[0] if args else "unknown"

        def __enter__(self):
            raise OperatorMutationBusyError(self.action, holder_action="operator_test", holder_since="2026-03-19T00:00:00Z")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        "app.backend.core.publish_candidate_maintenance_job.operator_mutation_scope",
        lambda action, timeout_sec=0.0: _BusyScope(action, timeout_sec=timeout_sec),
    )

    result = run_publish_candidate_maintenance_cycle(
        result_db_path=str(result_db),
        dry_run=True,
        source="test_cycle",
        acquire_timeout_sec=0.0,
        skip_if_busy=True,
    )
    assert result["ok"] is True
    assert result["skipped"] is True
    assert result["skip_reason"] == "operator_mutation_busy"


def test_publish_approve_returns_operator_busy_reason(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, _, _ = _seed_publish_state(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)

    class _BusyScope:
        def __init__(self, *args, **kwargs) -> None:
            self.action = args[0] if args else "unknown"

        def __enter__(self):
            raise OperatorMutationBusyError(self.action, holder_action="snapshot_refresh", holder_since="2026-03-19T00:00:00Z")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(system_router, "operator_mutation_scope", lambda action, timeout_sec=6.0: _BusyScope(action, timeout_sec=timeout_sec))

    with TestClient(main_module.create_app()) as client:
        response = client.post(
            "/api/system/publish/candidates/logic_family_a:v2/approve",
            json={"reason": "smoke"},
            headers={"X-MeeMee-Operator-Mode": "operator"},
        )
    assert response.status_code == 503
    payload = response.json()
    assert payload["detail"]["reason"] == "operator_mutation_busy"

    conn = duckdb.connect(str(result_db), read_only=True)
    try:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info('publish_maintenance_state')").fetchall()}
        assert "ops_fallback_enabled" not in columns
        assert "ops_fallback_hit_count" not in columns
        assert "ops_fallback_last_used_at" not in columns
        assert "ops_fallback_last_target" not in columns
    finally:
        conn.close()

    maintenance = load_publish_candidate_maintenance_state(db_path=str(result_db))
    assert maintenance["details_json"] == {}
    assert maintenance["non_promotable_legacy_count"] >= 0


def test_publish_candidate_snapshot_retention_sweeps_old_rejected_snapshot(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    conn = duckdb.connect(str(result_db))
    try:
        conn.execute(
            """
            UPDATE publish_candidate_bundle
            SET candidate_status = 'rejected',
                published_ranking_snapshot = ?,
                created_at = TIMESTAMP '2025-12-01 00:00:00'
            WHERE candidate_id = ?
            """,
            [json.dumps({"snapshot": True}), challenger_key],
        )
    finally:
        conn.close()

    result = sweep_publish_candidate_snapshots(db_path=str(result_db), keep_approved_days=90, keep_rejected_days=14, keep_retired_days=14)
    assert result["ok"] is True
    assert result["deleted"] >= 1

    conn = duckdb.connect(str(result_db))
    try:
        row = conn.execute(
            "SELECT published_ranking_snapshot, metadata FROM publish_candidate_bundle WHERE candidate_id = ?",
            [challenger_key],
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] is None
    metadata = json.loads(row[1])
    assert metadata["ranking_snapshot_retained_days"] == 14


def test_publish_candidate_snapshot_sweep_dry_run_does_not_mutate(monkeypatch, tmp_path) -> None:
    data_dir, result_db, ops_db, champion_key, challenger_key = _seed_publish_state(tmp_path)
    conn = duckdb.connect(str(result_db))
    try:
        conn.execute(
            """
            UPDATE publish_candidate_bundle
            SET candidate_status = 'rejected',
                published_ranking_snapshot = ?,
                created_at = TIMESTAMP '2025-12-01 00:00:00'
            WHERE candidate_id = ?
            """,
            [json.dumps({"snapshot": True}), challenger_key],
        )
    finally:
        conn.close()

    main_module = _load_app(monkeypatch, data_dir, result_db, ops_db)
    with TestClient(main_module.create_app()) as client:
        response = client.post(
            "/api/system/publish/maintenance/snapshot-sweep",
            json={"dryRun": True, "keepApprovedDays": 90, "keepRejectedDays": 14, "keepRetiredDays": 14},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["ok"] is True
        assert payload["dry_run"] is True
        assert payload["deleted"] >= 1

    conn = duckdb.connect(str(result_db))
    try:
        row = conn.execute(
            "SELECT published_ranking_snapshot FROM publish_candidate_bundle WHERE candidate_id = ?",
            [challenger_key],
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] is not None


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
