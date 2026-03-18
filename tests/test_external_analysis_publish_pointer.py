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
    )
    assert payload["ok"] is True
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        pointer = conn.execute("SELECT publish_id, freshness_state FROM publish_pointer WHERE pointer_name='latest_successful'").fetchone()
        manifest = conn.execute("SELECT publish_id, status FROM publish_manifest").fetchone()
    finally:
        conn.close()
    assert pointer == ("pub_2026-03-12_20260312T120000Z_01", "fresh")
    assert manifest == ("pub_2026-03-12_20260312T120000Z_01", "published")

