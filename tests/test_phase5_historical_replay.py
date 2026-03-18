from __future__ import annotations

import sys

import duckdb

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.runtime import historical_replay as historical_replay_module
from external_analysis.runtime.historical_replay import run_historical_replay
from external_analysis.similarity.baseline import run_similarity_baseline
from tests.test_phase3_similarity_nightly_pipeline import _make_client, _prepare_similarity_inputs


def _prepare_public_baseline(monkeypatch, tmp_path, publish_id: str) -> tuple[str, str, str, str, str, str, int]:
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


def test_historical_replay_smoke_preserves_public_pointer_and_accumulates_metrics(monkeypatch, tmp_path) -> None:
    public_publish_id = "pub_2026-03-15_20260315T010000Z_01"
    source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_public_baseline(monkeypatch, tmp_path, public_publish_id)
    start_as_of_date = str(as_of_date - 4)
    end_as_of_date = str(as_of_date)

    payload = run_historical_replay(
        source_db_path=source_db,
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
        replay_id="replay_smoke",
        max_days=3,
        max_codes=3,
    )

    result_conn = duckdb.connect(result_db, read_only=True)
    similarity_conn = duckdb.connect(similarity_db, read_only=True)
    ops_conn = duckdb.connect(ops_db, read_only=True)
    try:
        pointer_row = result_conn.execute("SELECT publish_id FROM publish_pointer WHERE pointer_name = 'latest_successful'").fetchone()
        replay_candidate_metrics = result_conn.execute(
            "SELECT COUNT(*) FROM nightly_candidate_metrics WHERE publish_id LIKE 'replay_replay_smoke_%'"
        ).fetchone()
        replay_similarity_metrics = similarity_conn.execute(
            "SELECT COUNT(*) FROM similarity_quality_metrics WHERE publish_id LIKE 'replay_replay_smoke_%'"
        ).fetchone()
        replay_summary = ops_conn.execute(
            "SELECT success_days, failed_days, skipped_days FROM external_replay_summaries WHERE replay_id = 'replay_smoke'"
        ).fetchone()
        queued_work_count = ops_conn.execute(
            "SELECT COUNT(*) FROM external_work_items WHERE work_type = 'challenger_eval' AND scope_type = 'replay' AND scope_id = 'replay_smoke'"
        ).fetchone()
    finally:
        result_conn.close()
        similarity_conn.close()
        ops_conn.close()

    client = _make_client(monkeypatch)
    candidates = client.get("/api/analysis-bridge/candidates")
    similar_cases = client.get("/api/analysis-bridge/similar-cases", params={"code": "1301", "limit": 5})

    assert payload["status"] == "success"
    assert "similarity_case_library" in payload["bootstrap"]
    assert pointer_row == (public_publish_id,)
    assert int(replay_candidate_metrics[0]) > 0
    assert int(replay_similarity_metrics[0]) > 0
    assert replay_summary is not None
    assert int(queued_work_count[0]) == 1
    assert payload["queued_work_ids"] == ["challenger_eval_replay_replay_smoke"]
    assert candidates.status_code == 200
    assert candidates.json()["publish_id"] == public_publish_id
    assert similar_cases.status_code == 200
    assert similar_cases.json()["publish_id"] == public_publish_id


def test_historical_replay_is_idempotent_and_resume_safe(monkeypatch, tmp_path) -> None:
    public_publish_id = "pub_2026-03-15_20260315T020000Z_01"
    source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_public_baseline(monkeypatch, tmp_path, public_publish_id)
    start_as_of_date = str(as_of_date - 3)
    end_as_of_date = str(as_of_date)

    payload_1 = run_historical_replay(
        source_db_path=source_db,
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
        replay_id="replay_resume",
        max_days=2,
        max_codes=2,
    )
    payload_2 = run_historical_replay(
        source_db_path=source_db,
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        start_as_of_date=start_as_of_date,
        end_as_of_date=end_as_of_date,
        replay_id="replay_resume",
        max_days=2,
        max_codes=2,
    )

    ops_conn = duckdb.connect(ops_db, read_only=True)
    result_conn = duckdb.connect(result_db, read_only=True)
    similarity_conn = duckdb.connect(similarity_db, read_only=True)
    try:
        replay_days = ops_conn.execute(
            "SELECT COUNT(*) FROM external_replay_days WHERE replay_id = 'replay_resume'"
        ).fetchone()
        replay_summary = ops_conn.execute(
            "SELECT success_days, skipped_days FROM external_replay_summaries WHERE replay_id = 'replay_resume'"
        ).fetchone()
        work_items = ops_conn.execute(
            "SELECT COUNT(*) FROM external_work_items WHERE work_type = 'challenger_eval' AND scope_type = 'replay' AND scope_id = 'replay_resume'"
        ).fetchone()
        replay_candidate_metrics = result_conn.execute(
            "SELECT COUNT(*) FROM nightly_candidate_metrics WHERE publish_id LIKE 'replay_replay_resume_%'"
        ).fetchone()
        replay_challenger_metrics = similarity_conn.execute(
            """
            SELECT COUNT(*) FROM similarity_quality_metrics
            WHERE publish_id LIKE 'replay_replay_resume_%' AND engine_role = 'challenger'
            """
        ).fetchone()
    finally:
        ops_conn.close()
        result_conn.close()
        similarity_conn.close()

    assert payload_1["status"] == "success"
    assert payload_2["status"] == "success"
    assert int(replay_days[0]) == 2
    assert int(replay_summary[0]) == 2
    assert int(replay_summary[1]) >= 0
    assert int(work_items[0]) == 1
    assert int(replay_candidate_metrics[0]) == 2
    assert int(replay_challenger_metrics[0]) == 0


def test_historical_replay_quarantines_failed_day_and_keeps_public_publish(monkeypatch, tmp_path) -> None:
    public_publish_id = "pub_2026-03-15_20260315T030000Z_01"
    source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_public_baseline(monkeypatch, tmp_path, public_publish_id)
    def _failing_similarity(**kwargs):
        raise RuntimeError("forced_replay_failure")

    monkeypatch.setattr(historical_replay_module, "run_similarity_baseline", _failing_similarity)

    payload = run_historical_replay(
        source_db_path=source_db,
        export_db_path=export_db,
        label_db_path=label_db,
        result_db_path=result_db,
        similarity_db_path=similarity_db,
        ops_db_path=ops_db,
        start_as_of_date=str(as_of_date - 2),
        end_as_of_date=str(as_of_date),
        replay_id="replay_fail",
        max_days=3,
        max_codes=2,
    )

    ops_conn = duckdb.connect(ops_db, read_only=True)
    result_conn = duckdb.connect(result_db, read_only=True)
    try:
        run_row = ops_conn.execute(
            "SELECT status FROM external_replay_runs WHERE replay_id = 'replay_fail'"
        ).fetchone()
        failed_days = ops_conn.execute(
            "SELECT COUNT(*) FROM external_replay_days WHERE replay_id = 'replay_fail' AND status = 'failed'"
        ).fetchone()
        quarantine_row = ops_conn.execute(
            "SELECT reason FROM external_job_quarantine WHERE job_type = 'historical_replay_runner' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        pointer_row = result_conn.execute(
            "SELECT publish_id FROM publish_pointer WHERE pointer_name = 'latest_successful'"
        ).fetchone()
    finally:
        ops_conn.close()
        result_conn.close()

    client = _make_client(monkeypatch)
    regime = client.get("/api/analysis-bridge/regime")

    assert payload["status"] == "partial_failure"
    assert run_row == ("partial_failure",)
    assert int(failed_days[0]) == 1
    assert quarantine_row == ("historical_replay_day_failed",)
    assert pointer_row == (public_publish_id,)
    assert regime.status_code == 200
    assert regime.json()["publish_id"] == public_publish_id


def test_historical_replay_cli_smoke(monkeypatch, tmp_path) -> None:
    public_publish_id = "pub_2026-03-15_20260315T040000Z_01"
    source_db, export_db, label_db, result_db, ops_db, similarity_db, as_of_date = _prepare_public_baseline(monkeypatch, tmp_path, public_publish_id)
    argv = [
        "external_analysis",
        "historical-replay-run",
        "--source-db-path",
        source_db,
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
        "--start-as-of-date",
        str(as_of_date - 2),
        "--end-as-of-date",
        str(as_of_date),
        "--replay-id",
        "replay_cli",
        "--max-days",
        "2",
        "--max-codes",
        "2",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0
