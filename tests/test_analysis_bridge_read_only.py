from __future__ import annotations

from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import ensure_result_db
from app.backend.services.analysis_bridge.reader import get_analysis_bridge_snapshot


def test_bridge_reads_latest_successful_publish_from_pointer(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T120000Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
    )

    payload = get_analysis_bridge_snapshot()

    assert payload["degraded"] is False
    assert payload["publish"]["publish_id"] == "pub_2026-03-12_20260312T120000Z_01"
    assert "candidate_component_scores" not in payload["public_table_counts"]

