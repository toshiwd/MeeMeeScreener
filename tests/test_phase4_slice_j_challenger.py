from __future__ import annotations

import sys

import duckdb

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.similarity import baseline as similarity_baseline_module
from external_analysis.similarity.baseline import run_similarity_baseline, run_similarity_challenger_shadow
from tests.test_phase3_similarity_nightly_pipeline import _make_client, _prepare_similarity_inputs


def test_challenger_shadow_persists_multi_version_embeddings_and_metrics(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    as_of_date = _prepare_similarity_inputs(
        monkeypatch,
        source_db=str(source_db),
        export_db=str(export_db),
        label_db=str(label_db),
        result_db=str(result_db),
        ops_db=str(ops_db),
        similarity_db=str(similarity_db),
    )
    run_similarity_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=str(as_of_date),
        publish_id="pub_2026-03-14_20260314T010000Z_01",
    )

    payload = run_similarity_challenger_shadow(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=str(as_of_date),
        publish_id="pub_2026-03-14_20260314T010000Z_01",
    )

    conn = duckdb.connect(str(similarity_db), read_only=True)
    try:
        embedding_versions = conn.execute(
            """
            SELECT embedding_version, embedding_role, COUNT(*)
            FROM case_embedding_store
            GROUP BY embedding_version, embedding_role
            ORDER BY embedding_version
            """
        ).fetchall()
        shadow_count = conn.execute(
            "SELECT COUNT(*) FROM similarity_shadow_cases WHERE publish_id = ? AND embedding_version = ?",
            ["pub_2026-03-14_20260314T010000Z_01", "future_path_challenger_v1"],
        ).fetchone()
        metric_row = conn.execute(
            """
            SELECT engine_role, embedding_version, comparison_target_version, overlap_at_k
            FROM similarity_quality_metrics
            WHERE publish_id = ? AND engine_role = 'challenger'
            """,
            ["pub_2026-03-14_20260314T010000Z_01"],
        ).fetchone()
    finally:
        conn.close()

    assert payload["ok"] is True
    assert payload["engine_role"] == "challenger"
    assert ("deterministic_similarity_v1", "champion", embedding_versions[0][2]) in embedding_versions
    assert any(row[0] == "future_path_challenger_v1" and row[1] == "challenger" for row in embedding_versions)
    assert int(shadow_count[0]) > 0
    assert metric_row[0] == "challenger"
    assert metric_row[1] == "future_path_challenger_v1"
    assert metric_row[2] == "deterministic_similarity_v1"
    assert metric_row[3] is not None


def test_challenger_shadow_keeps_public_publish_and_api_contract_unchanged(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    as_of_date = _prepare_similarity_inputs(
        monkeypatch,
        source_db=str(source_db),
        export_db=str(export_db),
        label_db=str(label_db),
        result_db=str(result_db),
        ops_db=str(ops_db),
        similarity_db=str(similarity_db),
    )
    publish_id = "pub_2026-03-14_20260314T020000Z_01"
    run_similarity_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=str(as_of_date),
        publish_id=publish_id,
    )
    before_conn = duckdb.connect(str(result_db), read_only=True)
    try:
        before_counts = before_conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM similar_cases_daily WHERE publish_id = ?),
                (SELECT COUNT(*) FROM similar_case_paths WHERE publish_id = ?),
                (SELECT COUNT(*) FROM candidate_daily WHERE publish_id = ?),
                (SELECT COUNT(*) FROM regime_daily WHERE publish_id = ?)
            """,
            [publish_id, publish_id, publish_id, publish_id],
        ).fetchone()
    finally:
        before_conn.close()

    run_similarity_challenger_shadow(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=str(as_of_date),
        publish_id=publish_id,
    )

    client = _make_client(monkeypatch)
    candidates = client.get("/api/analysis-bridge/candidates")
    regime = client.get("/api/analysis-bridge/regime")
    similar_cases = client.get("/api/analysis-bridge/similar-cases", params={"code": "1301", "limit": 5})

    after_conn = duckdb.connect(str(result_db), read_only=True)
    try:
        after_counts = after_conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM similar_cases_daily WHERE publish_id = ?),
                (SELECT COUNT(*) FROM similar_case_paths WHERE publish_id = ?),
                (SELECT COUNT(*) FROM candidate_daily WHERE publish_id = ?),
                (SELECT COUNT(*) FROM regime_daily WHERE publish_id = ?)
            """,
            [publish_id, publish_id, publish_id, publish_id],
        ).fetchone()
        pointer_row = after_conn.execute("SELECT publish_id FROM publish_pointer WHERE pointer_name = 'latest_successful'").fetchone()
    finally:
        after_conn.close()

    assert before_counts == after_counts
    assert pointer_row == (publish_id,)
    assert candidates.status_code == 200
    assert candidates.json()["degraded"] is False
    assert regime.status_code == 200
    assert regime.json()["degraded"] is False
    assert similar_cases.status_code == 200
    assert similar_cases.json()["degraded"] is False
    assert all("embedding_version" not in row for row in similar_cases.json()["rows"])


def test_slice_j_shadow_smoke_runs_cli_end_to_end(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    as_of_date = _prepare_similarity_inputs(
        monkeypatch,
        source_db=str(source_db),
        export_db=str(export_db),
        label_db=str(label_db),
        result_db=str(result_db),
        ops_db=str(ops_db),
        similarity_db=str(similarity_db),
    )
    publish_id = "pub_2026-03-14_20260314T030000Z_01"
    run_similarity_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=str(as_of_date),
        publish_id=publish_id,
    )

    argv = [
        "external_analysis",
        "similarity-challenger-run",
        "--export-db-path",
        str(export_db),
        "--label-db-path",
        str(label_db),
        "--result-db-path",
        str(result_db),
        "--similarity-db-path",
        str(similarity_db),
        "--as-of-date",
        str(as_of_date),
        "--publish-id",
        publish_id,
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0

    conn = duckdb.connect(str(similarity_db), read_only=True)
    try:
        metric_rows = conn.execute(
            """
            SELECT engine_role, embedding_version
            FROM similarity_quality_metrics
            WHERE publish_id = ?
            ORDER BY engine_role, embedding_version
            """,
            [publish_id],
        ).fetchall()
        shadow_rows = conn.execute(
            "SELECT COUNT(*) FROM similarity_shadow_cases WHERE publish_id = ?",
            [publish_id],
        ).fetchone()
    finally:
        conn.close()

    assert ("challenger", "future_path_challenger_v1") in metric_rows
    assert ("champion", "deterministic_similarity_v1") in metric_rows
    assert int(shadow_rows[0]) > 0


def test_challenger_metrics_failure_does_not_touch_public_rows(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    as_of_date = _prepare_similarity_inputs(
        monkeypatch,
        source_db=str(source_db),
        export_db=str(export_db),
        label_db=str(label_db),
        result_db=str(result_db),
        ops_db=str(ops_db),
        similarity_db=str(similarity_db),
    )
    publish_id = "pub_2026-03-14_20260314T040000Z_01"
    run_similarity_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=str(as_of_date),
        publish_id=publish_id,
    )

    def _raise_metrics_failure(*, similarity_db_path, metrics, max_attempts=3):
        return {"saved": False, "attempts": max_attempts, "run_id": None, "error_class": "RuntimeError"}

    monkeypatch.setattr(similarity_baseline_module, "persist_similarity_quality_metrics_with_retry", _raise_metrics_failure)
    payload = run_similarity_challenger_shadow(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=str(as_of_date),
        publish_id=publish_id,
    )

    result_conn = duckdb.connect(str(result_db), read_only=True)
    similarity_conn = duckdb.connect(str(similarity_db), read_only=True)
    try:
        public_count = result_conn.execute("SELECT COUNT(*) FROM similar_cases_daily WHERE publish_id = ?", [publish_id]).fetchone()
        challenger_metric_count = similarity_conn.execute(
            """
            SELECT COUNT(*) FROM similarity_quality_metrics
            WHERE publish_id = ? AND engine_role = 'challenger'
            """,
            [publish_id],
        ).fetchone()
    finally:
        result_conn.close()
        similarity_conn.close()

    assert payload["metrics_saved"] is False
    assert int(public_count[0]) > 0
    assert int(challenger_metric_count[0]) == 0
