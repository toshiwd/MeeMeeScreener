from __future__ import annotations

import duckdb

from external_analysis.results.publish import publish_result
from external_analysis.results.result_schema import ensure_result_db
from app.backend.services.analysis_bridge.reader import get_analysis_bridge_snapshot


def test_bridge_degrades_when_result_db_missing(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "missing.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))

    payload = get_analysis_bridge_snapshot()

    assert payload["degraded"] is True
    assert payload["degrade_reason"] == "result_db_missing"
    assert payload["app_continues"] is True


def test_bridge_degrades_without_latest_successful_publish(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))

    payload = get_analysis_bridge_snapshot()

    assert payload["degraded"] is True
    assert payload["degrade_reason"] == "no_latest_successful_publish"


def test_bridge_degrades_for_warning_and_hard_stale(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T120000Z_01",
        as_of_date="2026-03-12",
        freshness_state="warning",
    )
    payload_warning = get_analysis_bridge_snapshot()
    assert payload_warning["degraded"] is True
    assert payload_warning["degrade_reason"] == "warning_stale"
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T120000Z_02",
        as_of_date="2026-03-12",
        freshness_state="hard",
    )
    payload_hard = get_analysis_bridge_snapshot()
    assert payload_hard["degraded"] is True
    assert payload_hard["degrade_reason"] == "hard_stale"
    assert payload_hard["show_state_evaluation"] is False


def test_bridge_degrades_for_pointer_corruption(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE publish_pointer (pointer_name TEXT)")
    finally:
        conn.close()

    payload = get_analysis_bridge_snapshot()

    assert payload["degraded"] is True
    assert payload["degrade_reason"] == "pointer_corruption"


def test_bridge_degrades_when_publish_pointer_has_multiple_rows(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO publish_pointer (
                pointer_name, publish_id, as_of_date, published_at, schema_version, contract_version, freshness_state
            ) VALUES
                ('latest_successful', 'pub_1', DATE '2026-03-12', NOW(), 'phase1-v1', 'phase1-v1', 'fresh'),
                ('other_pointer', 'pub_2', DATE '2026-03-12', NOW(), 'phase1-v1', 'phase1-v1', 'fresh')
            """
        )
    finally:
        conn.close()

    payload = get_analysis_bridge_snapshot()

    assert payload["degraded"] is True
    assert payload["degrade_reason"] == "pointer_corruption"


def test_bridge_degrades_for_manifest_mismatch(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T120000Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
    )
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("DELETE FROM publish_manifest")
    finally:
        conn.close()

    payload = get_analysis_bridge_snapshot()

    assert payload["degraded"] is True
    assert payload["degrade_reason"] == "manifest_mismatch"


def test_bridge_degrades_for_schema_mismatch(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(db_path))
    ensure_result_db(str(db_path))
    publish_result(
        db_path=str(db_path),
        publish_id="pub_2026-03-12_20260312T120000Z_01",
        as_of_date="2026-03-12",
        freshness_state="fresh",
    )
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("UPDATE publish_pointer SET schema_version='broken' WHERE pointer_name='latest_successful'")
    finally:
        conn.close()

    payload = get_analysis_bridge_snapshot()

    assert payload["degraded"] is True
    assert payload["degrade_reason"] == "schema_mismatch"
