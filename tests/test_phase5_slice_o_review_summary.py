from __future__ import annotations

import duckdb

from external_analysis.runtime.challenger_eval import run_challenger_eval
from external_analysis.runtime.historical_replay import run_historical_replay
from external_analysis.runtime.nightly_similarity_challenger_pipeline import run_nightly_similarity_challenger_pipeline
from external_analysis.runtime.review_build import run_review_build
from tests.test_phase5_slice_n_rolling import _prepare_public_publish
from tests.test_phase3_similarity_nightly_pipeline import _make_client


def test_slice_o_replay_creates_review_summary_and_preserves_public_contract(monkeypatch, tmp_path) -> None:
    public_publish_id = "pub_2026-03-16_20260316T030000Z_01"
    source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_public_publish(
        monkeypatch, tmp_path, public_publish_id
    )

    payload = run_historical_replay(
        source_db_path=source_db,
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        start_as_of_date=str(as_of_date - 4),
        end_as_of_date=str(as_of_date),
        replay_id="replay_review",
        max_days=3,
        max_codes=3,
    )
    challenger_payload = run_challenger_eval(
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        work_id=payload["queued_work_ids"][0],
    )
    review_payload = run_review_build(
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        work_id=challenger_payload["review_work_id"],
    )

    ops_conn = duckdb.connect(ops_db, read_only=True)
    result_conn = duckdb.connect(result_db, read_only=True)
    try:
        review_row = ops_conn.execute(
            """
            SELECT review_id, replay_scope_id, combined_scope_id, top_reason_codes_json, summary_json
            FROM external_review_artifacts
            WHERE review_id = 'weekly_review_latest'
            """
        ).fetchone()
        pointer_row = result_conn.execute(
            "SELECT publish_id FROM publish_pointer WHERE pointer_name = 'latest_successful'"
        ).fetchone()
    finally:
        ops_conn.close()
        result_conn.close()

    client = _make_client(monkeypatch)
    candidates = client.get("/api/analysis-bridge/candidates")

    assert payload["status"] == "success"
    assert challenger_payload["status"] == "success"
    assert review_payload["status"] == "success"
    assert review_row is not None
    assert review_row[0] == "weekly_review_latest"
    assert review_row[1] == "replay_review"
    assert review_row[2] == "global"
    assert "reason_code" in str(review_row[3]) or str(review_row[3]) == "[]"
    assert "scope_comparison" in str(review_row[4])
    assert pointer_row == (public_publish_id,)
    assert candidates.status_code == 200
    assert candidates.json()["publish_id"] == public_publish_id


def test_slice_o_nightly_updates_review_summary_without_changing_public_api(monkeypatch, tmp_path) -> None:
    public_publish_id = "pub_2026-03-16_20260316T040000Z_01"
    _source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_public_publish(
        monkeypatch, tmp_path, public_publish_id
    )

    payload = run_nightly_similarity_challenger_pipeline(
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        as_of_date=str(as_of_date),
        publish_id=public_publish_id,
    )
    review_payload = run_review_build(
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        work_id=payload["review_work_id"],
    )

    ops_conn = duckdb.connect(ops_db, read_only=True)
    try:
        review_row = ops_conn.execute(
            """
            SELECT review_id, nightly_scope_id, combined_readiness_20, recent_failure_rate, recent_quarantine_count
            FROM external_review_artifacts
            WHERE review_id = 'weekly_review_latest'
            """
        ).fetchone()
    finally:
        ops_conn.close()

    client = _make_client(monkeypatch)
    regime = client.get("/api/analysis-bridge/regime")
    similar_cases = client.get("/api/analysis-bridge/similar-cases", params={"code": "1301", "limit": 5})

    assert payload["status"] in {"success", "shadow_with_metrics_failure"}
    assert review_payload["status"] == "success"
    assert review_row is not None
    assert review_row[0] == "weekly_review_latest"
    assert review_row[1] == "nightly"
    assert isinstance(review_row[2], (bool, type(None)))
    assert float(review_row[3]) >= 0.0
    assert int(review_row[4]) >= 0
    assert regime.status_code == 200
    assert regime.json()["publish_id"] == public_publish_id
    assert similar_cases.status_code == 200
    assert similar_cases.json()["publish_id"] == public_publish_id
