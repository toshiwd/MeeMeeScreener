from __future__ import annotations

import sys

import duckdb

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.runtime import nightly_similarity_challenger_pipeline as challenger_pipeline_module
from external_analysis.runtime.nightly_similarity_challenger_pipeline import run_nightly_similarity_challenger_pipeline
from external_analysis.similarity.baseline import run_similarity_baseline
from tests.test_phase3_similarity_nightly_pipeline import _make_client, _prepare_similarity_inputs


def _prepare_champion_publish(monkeypatch, tmp_path, publish_id: str) -> tuple[str, str, str, str, str, str, int]:
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
        publish_id=publish_id,
    )
    return str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db), str(similarity_db), as_of_date


def test_nightly_similarity_challenger_is_idempotent_for_same_publish(monkeypatch, tmp_path) -> None:
    publish_id = "pub_2026-03-14_20260314T230000Z_01"
    _source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_champion_publish(monkeypatch, tmp_path, publish_id)

    payload_1 = run_nightly_similarity_challenger_pipeline(
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        as_of_date=str(as_of_date),
        publish_id=publish_id,
    )
    payload_2 = run_nightly_similarity_challenger_pipeline(
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        as_of_date=str(as_of_date),
        publish_id=publish_id,
    )

    conn = duckdb.connect(similarity_db, read_only=True)
    try:
        challenger_metric_count = conn.execute(
            """
            SELECT COUNT(*) FROM similarity_quality_metrics
            WHERE publish_id = ? AND engine_role = 'challenger' AND embedding_version = 'future_path_challenger_v1'
            """,
            [publish_id],
        ).fetchone()
        promotion_count = conn.execute(
            "SELECT COUNT(*) FROM similarity_promotion_reviews WHERE publish_id = ?",
            [publish_id],
        ).fetchone()
        summary_count = conn.execute(
            "SELECT COUNT(*) FROM similarity_nightly_summaries WHERE publish_id = ?",
            [publish_id],
        ).fetchone()
    finally:
        conn.close()

    assert payload_1["status"] == "success"
    assert payload_2["status"] == "success"
    assert int(payload_1["challenger"]["query_case_limit"]) > 0
    assert int(payload_1["challenger"]["query_case_count"]) <= int(payload_1["challenger"]["query_case_limit"])
    assert int(payload_1["challenger"]["candidate_pool_limit"]) > int(payload_1["challenger"]["top_k"])
    assert int(payload_1["challenger"]["focus_setup_count"]) > 0
    assert int(challenger_metric_count[0]) == 1
    assert int(promotion_count[0]) == 1
    assert int(summary_count[0]) == 1


def test_nightly_similarity_challenger_records_ops_and_keeps_public_api_unchanged(monkeypatch, tmp_path) -> None:
    publish_id = "pub_2026-03-14_20260314T231000Z_01"
    _source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_champion_publish(monkeypatch, tmp_path, publish_id)

    argv = [
        "external_analysis",
        "nightly-similarity-challenger-run",
        "--export-db-path",
        export_db,
        "--label-db-path",
        label_db,
        "--result-db-path",
        result_db,
        "--similarity-db-path",
        similarity_db,
        "--ops-db-path",
        ops_db,
        "--as-of-date",
        str(as_of_date),
        "--publish-id",
        publish_id,
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0

    ops_conn = duckdb.connect(ops_db, read_only=True)
    similarity_conn = duckdb.connect(similarity_db, read_only=True)
    try:
        run_row = ops_conn.execute(
            """
            SELECT status, publish_id, details_json
            FROM external_job_runs
            WHERE job_type = 'nightly_similarity_challenger_pipeline'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        review_row = similarity_conn.execute(
            """
            SELECT pass_gate, required_streak, observed_streak
            FROM similarity_promotion_reviews
            WHERE publish_id = ?
            """,
            [publish_id],
        ).fetchone()
        summary_row = similarity_conn.execute(
            "SELECT publish_id FROM similarity_nightly_summaries WHERE publish_id = ?",
            [publish_id],
        ).fetchone()
        shadow_count = similarity_conn.execute(
            "SELECT COUNT(*) FROM similarity_shadow_cases WHERE publish_id = ?",
            [publish_id],
        ).fetchone()
        work_item_row = ops_conn.execute(
            """
            SELECT status
            FROM external_work_items
            WHERE work_type = 'review_build' AND scope_type = 'nightly' AND scope_id = 'nightly'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        ops_conn.close()
        similarity_conn.close()

    client = _make_client(monkeypatch)
    candidates = client.get("/api/analysis-bridge/candidates")
    regime = client.get("/api/analysis-bridge/regime")
    similar_cases = client.get("/api/analysis-bridge/similar-cases", params={"code": "1301", "limit": 5})

    assert run_row[0] == "success"
    assert run_row[1] == publish_id
    assert "\"challenger_query_case_limit\"" in str(run_row[2])
    assert "\"challenger_candidate_pool_limit\"" in str(run_row[2])
    assert work_item_row == ("pending",)
    assert review_row is not None
    assert int(review_row[1]) == 3
    assert int(review_row[2]) >= 0
    assert summary_row == (publish_id,)
    assert int(shadow_count[0]) == 0
    assert candidates.status_code == 200
    assert candidates.json()["degraded"] is False
    assert regime.status_code == 200
    assert regime.json()["degraded"] is False
    assert similar_cases.status_code == 200
    assert similar_cases.json()["degraded"] is False


def test_nightly_similarity_challenger_failure_quarantines_without_breaking_champion(monkeypatch, tmp_path) -> None:
    publish_id = "pub_2026-03-14_20260314T232000Z_01"
    _source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_champion_publish(monkeypatch, tmp_path, publish_id)

    def _raise_shadow(*args, **kwargs):
        raise RuntimeError("forced_challenger_failure")

    monkeypatch.setattr(challenger_pipeline_module, "run_challenger_eval", _raise_shadow)

    payload = run_nightly_similarity_challenger_pipeline(
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        as_of_date=str(as_of_date),
        publish_id=publish_id,
    )

    ops_conn = duckdb.connect(ops_db, read_only=True)
    result_conn = duckdb.connect(result_db, read_only=True)
    try:
        run_row = ops_conn.execute(
            """
            SELECT status, error_class
            FROM external_job_runs
            WHERE job_type = 'nightly_similarity_challenger_pipeline'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        quarantine_row = ops_conn.execute(
            """
            SELECT reason, publish_id
            FROM external_job_quarantine
            WHERE job_type = 'nightly_similarity_challenger_pipeline'
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        public_count = result_conn.execute(
            "SELECT COUNT(*) FROM similar_cases_daily WHERE publish_id = ?",
            [publish_id],
        ).fetchone()
    finally:
        ops_conn.close()
        result_conn.close()

    client = _make_client(monkeypatch)
    similar_cases = client.get("/api/analysis-bridge/similar-cases", params={"code": "1301", "limit": 5})

    assert payload["status"] == "failed"
    assert run_row == ("failed", "RuntimeError")
    assert quarantine_row == ("challenger_eval_failed", publish_id)
    assert int(public_count[0]) > 0
    assert similar_cases.status_code == 200
    assert similar_cases.json()["degraded"] is False
