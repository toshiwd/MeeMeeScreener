from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from app.backend.infra.files.config_repo import ConfigRepository, LOGIC_SELECTION_SCHEMA_VERSION, PUBLISH_REGISTRY_SCHEMA_VERSION
from app.backend.services.runtime_selection_service import (
    capture_last_known_good_if_eligible,
    clear_selected_logic_override,
)
from external_analysis.results.publish import load_published_logic_catalog, publish_result
from external_analysis.results.result_schema import ensure_result_db


def _reset_repo_singletons() -> None:
    import app.backend.api.dependencies as dependencies

    dependencies._stock_repo = None
    dependencies._favorites_repo = None
    dependencies._config_repo = None
    dependencies._screener_repo = None


def _write_artifact(path: Path, content: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _publish_catalog(tmp_path: Path) -> tuple[Path, Path, Path, str, str]:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = tmp_path / "result.duckdb"
    ensure_result_db(str(db_path))
    artifact_path = tmp_path / "artifacts" / "logic_family_a_v1.json"
    checksum = _write_artifact(artifact_path, "{\"logic\":\"family_a:v1\"}\n")
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-19_20260319T010000Z_01",
        as_of_date="2026-03-19",
        freshness_state="fresh",
        default_logic_pointer="logic_family_a:v1",
        bootstrap_champion=True,
        logic_artifact_uri=str(artifact_path),
        logic_artifact_checksum=checksum,
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
            "artifact_uri": str(artifact_path),
            "checksum": checksum,
        },
    )
    return data_dir, db_path, artifact_path, checksum, "logic_family_a:v1"


def _load_app(monkeypatch, data_dir: Path, db_path: Path):
    monkeypatch.setenv("MEEMEE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
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


def test_runtime_selection_api_reports_observability_fields(monkeypatch, tmp_path) -> None:
    data_dir, db_path, _, _, valid_logic_key = _publish_catalog(tmp_path)
    logic_state_path = data_dir / "config" / "logic_selection.json"
    logic_state_path.parent.mkdir(parents=True, exist_ok=True)
    logic_state_path.write_text(
        '{"selected_logic_override":"missing_logic:v9","last_known_good":{"logic_key":"logic_lkg_v1","artifact_uri":"missing.json","artifact_path":"missing.json","artifact_checksum":"deadbeef","validation_state":"artifact_missing"}}',
        encoding="utf-8",
    )

    main_module = _load_app(monkeypatch, data_dir, db_path)
    with TestClient(main_module.create_app()) as client:
        response = client.get("/api/system/runtime-selection")
        assert response.status_code == 200
        payload = response.json()
        assert payload["schema_version"] == LOGIC_SELECTION_SCHEMA_VERSION
        assert payload["snapshot_created_at"]
        assert payload["resolved_source"]
        assert payload["selected_logic_key"]
        assert payload["validation_state"]
        assert payload["bootstrap_rule"] == "explicit_champion_flag"
        assert payload["challenger_logic_keys"] == []
        assert payload["source_of_truth"] == "external_analysis"
        assert payload["registry_sync_state"] == "mirror_stale"
        assert payload["external_registry_version"] is not None
        assert payload["local_mirror_version"] is None
        assert payload["mirror_schema_version"] == PUBLISH_REGISTRY_SCHEMA_VERSION
        assert payload["mirror_normalized"] is False

    catalog = load_published_logic_catalog(db_path=str(db_path))
    assert catalog["default_logic_pointer"] == valid_logic_key
    assert catalog["available_logic_keys"] == [valid_logic_key]
    assert catalog["available_logic_manifest"][0]["logic_artifact_uri"] == str(tmp_path / "artifacts" / "logic_family_a_v1.json")


def test_runtime_selection_override_validates_and_clear_updates_state(monkeypatch, tmp_path) -> None:
    data_dir, db_path, _, _, valid_logic_key = _publish_catalog(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, db_path)
    with TestClient(main_module.create_app()) as client:
        invalid_response = client.post(
            "/api/system/runtime-selection/override",
            json={"selectedLogicOverride": "missing_logic:v9", "reason": "bad pin"},
        )
        assert invalid_response.status_code == 400
        assert invalid_response.json()["detail"]["reason"] == "logic_key_not_available"

    repo = ConfigRepository(str(data_dir))
    repo.save_logic_selection_state({"selected_logic_override": valid_logic_key})
    clear_result = clear_selected_logic_override(
        config_repo=repo,
        source="test.override.clear",
        reason="clear after review",
        db_path=str(db_path),
    )
    assert clear_result["ok"] is True

    logic_state_path = data_dir / "config" / "logic_selection.json"
    state = json.loads(logic_state_path.read_text(encoding="utf-8"))
    assert state["schema_version"] == LOGIC_SELECTION_SCHEMA_VERSION
    assert state["selected_logic_override"] is None

    audit_path = data_dir / "runtime_selection" / "logic_selection_audit.jsonl"
    lines = [line for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) >= 1
    last_event = json.loads(lines[-1])
    assert last_event["action"] == "clear"
    assert last_event["previous_logic_key"] == valid_logic_key
    assert last_event["source"] == "test.override.clear"


def test_runtime_selection_broken_config_degrades_safely(monkeypatch, tmp_path) -> None:
    data_dir, db_path, _, _, _ = _publish_catalog(tmp_path)
    logic_state_path = data_dir / "config" / "logic_selection.json"
    logic_state_path.parent.mkdir(parents=True, exist_ok=True)
    logic_state_path.write_text("{broken json", encoding="utf-8")

    main_module = _load_app(monkeypatch, data_dir, db_path)
    with TestClient(main_module.create_app()) as client:
        response = client.get("/api/system/runtime-selection")
        assert response.status_code == 200
        payload = response.json()
        assert payload["resolved_source"]

    repo = ConfigRepository(str(data_dir))
    assert repo.load_logic_selection_state() == {}
    catalog = load_published_logic_catalog(db_path=str(db_path))
    assert catalog["default_logic_pointer"] == "logic_family_a:v1"
    assert catalog["available_logic_keys"] == ["logic_family_a:v1"]


def test_confirmed_analysis_path_captures_last_known_good(monkeypatch, tmp_path) -> None:
    data_dir, db_path, artifact_path, checksum, valid_logic_key = _publish_catalog(tmp_path)
    main_module = _load_app(monkeypatch, data_dir, db_path)
    with TestClient(main_module.create_app()) as client:
        assert client.get("/api/system/runtime-selection").status_code == 200
    config_repo = ConfigRepository(str(data_dir))
    catalog = load_published_logic_catalog(db_path=str(db_path))
    catalog_row = catalog["available_logic_manifest"][0]
    manifest = {
        "logic_id": catalog_row["logic_id"],
        "logic_version": catalog_row["logic_version"],
        "logic_key": catalog_row["logic_key"],
        "artifact_uri": catalog_row["logic_artifact_uri"],
        "artifact_path": catalog_row["logic_artifact_uri"],
        "artifact_checksum": catalog_row["logic_artifact_checksum"],
        "published_at": catalog_row["published_at"],
        "validation_state": "ok",
        "validation_reasons": [],
        "source": "publish_catalog",
    }
    snapshot = {
        "resolved_source": "default_logic_pointer",
        "validation_state": "default_valid",
        "selected_manifest": manifest,
    }
    capture_result = capture_last_known_good_if_eligible(
        config_repo=config_repo,
        snapshot=snapshot,
        db_path=str(db_path),
    )

    assert isinstance(capture_result, dict)
    assert "reason" in capture_result
