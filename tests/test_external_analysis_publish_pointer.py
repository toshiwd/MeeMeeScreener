from __future__ import annotations

import duckdb

from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import ensure_result_db


def test_publish_result_updates_pointer_and_manifest(tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    ensure_result_db(str(db_path))
    payload = publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T120000Z_01",
        as_of_date="2026-03-12",
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
            "trained_at": "2026-03-11T00:00:00Z",
            "published_at": "2026-03-12T12:00:00Z",
            "artifact_uri": "artifacts/logic_family_a/v1.json",
            "checksum": "sha256:test",
        },
    )
    assert payload["ok"] is True
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        pointer = conn.execute("SELECT publish_id, freshness_state FROM publish_pointer WHERE pointer_name='latest_successful'").fetchone()
        manifest = conn.execute(
            "SELECT publish_id, status, logic_id, logic_version, logic_family, default_logic_pointer, logic_artifact_uri, logic_artifact_checksum FROM publish_manifest"
        ).fetchone()
    finally:
        conn.close()
    assert pointer == ("pub_2026-03-12_20260312T120000Z_01", "fresh")
    assert manifest == (
        "pub_2026-03-12_20260312T120000Z_01",
        "published",
        "logic_family_a",
        "v1",
        "family_a",
        "logic_family_a:v1",
        "artifacts/logic_family_a/v1.json",
        "sha256:test",
    )
