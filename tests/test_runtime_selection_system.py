from __future__ import annotations

from fastapi.testclient import TestClient

from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import ensure_result_db
from app.backend.infra.files.config_repo import LOGIC_SELECTION_SCHEMA_VERSION


def _reset_repo_singletons() -> None:
    import app.backend.api.dependencies as dependencies

    dependencies._stock_repo = None
    dependencies._favorites_repo = None
    dependencies._config_repo = None
    dependencies._screener_repo = None


def test_system_runtime_selection_uses_bootstrap_snapshot(monkeypatch, tmp_path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = tmp_path / "result.duckdb"
    ensure_result_db(str(db_path))
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-19_20260319T010000Z_01",
        as_of_date="2026-03-19",
        freshness_state="fresh",
        default_logic_pointer="logic_family_a:v1",
        logic_artifact_uri="artifacts/logic_family_a/v1.json",
        logic_artifact_checksum="sha256:test",
        logic_manifest={
            "logic_id": "logic_family_a",
            "logic_version": "v1",
            "logic_family": "family_a",
            "status": "published",
            "input_schema_version": "v3",
            "output_schema_version": "v3",
            "trained_at": "2026-03-18T00:00:00Z",
            "published_at": "2026-03-19T01:00:00Z",
            "artifact_uri": "artifacts/logic_family_a/v1.json",
            "checksum": "sha256:test",
        },
    )
    logic_state_path = data_dir / "config" / "logic_selection.json"
    logic_state_path.parent.mkdir(parents=True, exist_ok=True)
    logic_state_path.write_text(
        '{"selected_logic_override":"logic_override_v9","last_known_good":"logic_lkg_v1"}',
        encoding="utf-8",
    )
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

    client = TestClient(main_module.create_app())
    response = client.get("/api/system/runtime-selection")

    assert response.status_code == 200
    payload = response.json()
    assert payload["schema_version"] == LOGIC_SELECTION_SCHEMA_VERSION
    assert payload["snapshot_created_at"]
    assert payload["selected_logic_override"] == "logic_override_v9"
    assert payload["default_logic_pointer"] == "logic_family_a:v1"
    assert payload["resolved_source"] == "default_logic_pointer"
    assert payload["selected_source"] == "default_logic_pointer"
    assert payload["selected_logic_key"] == "logic_family_a:v1"
    assert payload["selected_logic_id"] == "logic_family_a"
    assert payload["selected_logic_version"] == "v1"
    assert payload["artifact_uri"] == "artifacts/logic_family_a/v1.json"
    assert payload["last_known_good_artifact_uri"] is None
    assert payload["available_logic_keys"][0] == "logic_family_a:v1"
    assert "logic_lkg_v1" in payload["available_logic_keys"]
    assert payload["available_logic_manifest"][0]["logic_key"] == "logic_family_a:v1"


def test_system_runtime_selection_override_updates_state(monkeypatch, tmp_path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = tmp_path / "result.duckdb"
    ensure_result_db(str(db_path))
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-19_20260319T010000Z_01",
        as_of_date="2026-03-19",
        freshness_state="fresh",
        default_logic_pointer="logic_family_a:v1",
        logic_artifact_uri="artifacts/logic_family_a/v1.json",
        logic_artifact_checksum="sha256:test",
        logic_manifest={
            "logic_id": "logic_family_a",
            "logic_version": "v1",
            "logic_family": "family_a",
            "status": "published",
            "input_schema_version": "v3",
            "output_schema_version": "v3",
            "trained_at": "2026-03-18T00:00:00Z",
            "published_at": "2026-03-19T01:00:00Z",
            "artifact_uri": "artifacts/logic_family_a/v1.json",
            "checksum": "sha256:test",
        },
    )
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

    client = TestClient(main_module.create_app())
    response = client.post(
        "/api/system/runtime-selection/override",
        json={"selectedLogicOverride": "logic_override_v9"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["schema_version"] == LOGIC_SELECTION_SCHEMA_VERSION
    assert payload["selected_logic_override"] == "logic_override_v9"
    assert payload["snapshot"]["selected_logic_override"] == "logic_override_v9"
    logic_state_path = data_dir / "config" / "logic_selection.json"
    assert logic_state_path.exists()
    assert logic_state_path.read_text(encoding="utf-8").find('"schema_version": "logic_selection_v1"') >= 0
