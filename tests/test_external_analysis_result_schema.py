from __future__ import annotations

import duckdb

from external_analysis.results.result_schema import ALL_RESULT_TABLES, ensure_result_db


def test_ensure_result_db_creates_all_tables(tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    payload = ensure_result_db(str(db_path))
    assert payload["ok"] is True
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
    finally:
        conn.close()
    names = {str(row[0]) for row in rows}
    for table_name in ALL_RESULT_TABLES:
        assert table_name in names


def test_public_empty_schema_has_expected_columns_and_publish_id(tmp_path) -> None:
    db_path = tmp_path / "result.duckdb"
    ensure_result_db(str(db_path))
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        expected = {
            "publish_pointer": {"pointer_name": "VARCHAR", "publish_id": "VARCHAR", "as_of_date": "DATE"},
            "publish_manifest": {
                "publish_id": "VARCHAR",
                "as_of_date": "DATE",
                "published_at": "TIMESTAMP",
                "logic_id": "VARCHAR",
                "logic_version": "VARCHAR",
                "logic_family": "VARCHAR",
                "default_logic_pointer": "VARCHAR",
                "logic_artifact_uri": "VARCHAR",
                "logic_artifact_checksum": "VARCHAR",
            },
            "candidate_daily": {"publish_id": "VARCHAR", "as_of_date": "DATE", "code": "VARCHAR"},
            "candidate_component_scores": {"publish_id": "VARCHAR", "as_of_date": "DATE", "code": "VARCHAR"},
            "nightly_candidate_metrics": {"run_id": "VARCHAR", "publish_id": "VARCHAR", "as_of_date": "DATE", "baseline_version": "VARCHAR"},
            "state_eval_daily": {
                "publish_id": "VARCHAR",
                "as_of_date": "DATE",
                "code": "VARCHAR",
                "side": "VARCHAR",
                "holding_band": "VARCHAR",
                "strategy_tags": "JSON",
                "decision_3way": "VARCHAR",
                "reason_text_top3": "JSON",
            },
            "similar_cases_daily": {"publish_id": "VARCHAR", "as_of_date": "DATE", "code": "VARCHAR"},
            "similar_case_paths": {"publish_id": "VARCHAR", "as_of_date": "DATE", "code": "VARCHAR"},
            "regime_daily": {"publish_id": "VARCHAR", "as_of_date": "DATE", "regime_tag": "VARCHAR"},
        }
        for table_name, columns in expected.items():
            rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            by_name = {str(row[1]): str(row[2]).upper() for row in rows}
            for column_name, column_type in columns.items():
                assert by_name[column_name] == column_type
    finally:
        conn.close()
