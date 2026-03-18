from __future__ import annotations

import duckdb

from external_analysis.runtime.challenger_eval import run_challenger_eval
from external_analysis.runtime.historical_replay import run_historical_replay
from external_analysis.runtime.nightly_similarity_challenger_pipeline import run_nightly_similarity_challenger_pipeline
from external_analysis.runtime.review_build import run_review_build
from external_analysis.similarity.baseline import run_similarity_baseline
from tests.test_phase3_similarity_nightly_pipeline import _make_client, _prepare_similarity_inputs


def _prepare_public_publish(monkeypatch, tmp_path, publish_id: str) -> tuple[str, str, str, str, str, str, int]:
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


def test_slice_n_replay_builds_rollups_and_readiness_without_touching_public_api(monkeypatch, tmp_path) -> None:
    public_publish_id = "pub_2026-03-16_20260316T010000Z_01"
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
        replay_id="replay_rollup",
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
        replay_rollups = ops_conn.execute(
            "SELECT COUNT(*) FROM external_comparison_rollups WHERE scope_type = 'replay' AND scope_id = 'replay_rollup'"
        ).fetchone()
        combined_rollups = ops_conn.execute(
            "SELECT COUNT(*) FROM external_comparison_rollups WHERE scope_type = 'combined' AND scope_id = 'global'"
        ).fetchone()
        readiness = ops_conn.execute(
            """
            SELECT COUNT(*)
            FROM external_promotion_readiness
            WHERE scope_type = 'replay' AND scope_id = 'replay_rollup' AND summary_json IS NOT NULL
            """
        ).fetchone()
        daily_summaries = ops_conn.execute(
            "SELECT COUNT(*) FROM external_metric_daily_summaries WHERE scope_type = 'replay' AND scope_id = 'replay_rollup'"
        ).fetchone()
        pointer_row = result_conn.execute(
            "SELECT publish_id FROM publish_pointer WHERE pointer_name = 'latest_successful'"
        ).fetchone()
    finally:
        ops_conn.close()
        result_conn.close()

    client = _make_client(monkeypatch)
    candidates = client.get("/api/analysis-bridge/candidates")
    similar_cases = client.get("/api/analysis-bridge/similar-cases", params={"code": "1301", "limit": 5})

    assert payload["status"] == "success"
    assert challenger_payload["status"] == "success"
    assert review_payload["status"] == "success"
    assert int(replay_rollups[0]) >= 1
    assert int(combined_rollups[0]) >= 1
    assert int(readiness[0]) >= 1
    assert int(daily_summaries[0]) == 3
    assert pointer_row == (public_publish_id,)
    assert candidates.status_code == 200
    assert candidates.json()["publish_id"] == public_publish_id
    assert similar_cases.status_code == 200
    assert similar_cases.json()["publish_id"] == public_publish_id


def test_slice_n_nightly_challenger_updates_nightly_and_combined_rollups(monkeypatch, tmp_path) -> None:
    public_publish_id = "pub_2026-03-16_20260316T020000Z_01"
    source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_public_publish(
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
        nightly_rollups = ops_conn.execute(
            "SELECT COUNT(*) FROM external_comparison_rollups WHERE scope_type = 'nightly' AND scope_id = 'nightly'"
        ).fetchone()
        combined_readiness = ops_conn.execute(
            "SELECT COUNT(*) FROM external_promotion_readiness WHERE scope_type = 'combined' AND scope_id = 'global'"
        ).fetchone()
        nightly_daily_summary = ops_conn.execute(
            "SELECT COUNT(*) FROM external_metric_daily_summaries WHERE scope_type = 'nightly' AND scope_id = 'nightly'"
        ).fetchone()
    finally:
        ops_conn.close()

    client = _make_client(monkeypatch)
    regime = client.get("/api/analysis-bridge/regime")

    assert payload["status"] in {"success", "shadow_with_metrics_failure"}
    assert review_payload["status"] == "success"
    assert int(nightly_rollups[0]) >= 1
    assert int(combined_readiness[0]) >= 1
    assert int(nightly_daily_summary[0]) >= 1
    assert regime.status_code == 200
    assert regime.json()["publish_id"] == public_publish_id
