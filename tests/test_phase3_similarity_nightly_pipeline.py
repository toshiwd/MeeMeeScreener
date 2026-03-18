from __future__ import annotations

import sys

import duckdb
from fastapi.testclient import TestClient

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.runtime.nightly_similarity_pipeline import run_nightly_similarity_pipeline
from external_analysis.similarity import baseline as similarity_baseline_module
from tests.test_phase3_similarity_baseline import _seed_source_db


def _prepare_similarity_inputs(monkeypatch, *, source_db: str, export_db: str, label_db: str, result_db: str, ops_db: str, similarity_db: str) -> int:
    dates = _seed_source_db(source_db)
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    commands = [
        ["external_analysis", "init-result-db", "--db-path", str(result_db)],
        ["external_analysis", "init-export-db", "--db-path", str(export_db)],
        ["external_analysis", "init-label-db", "--db-path", str(label_db)],
        ["external_analysis", "init-ops-db", "--db-path", str(ops_db)],
        ["external_analysis", "init-similarity-db", "--db-path", str(similarity_db)],
        ["external_analysis", "export-sync", "--source-db-path", str(source_db), "--export-db-path", str(export_db)],
        ["external_analysis", "label-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
        ["external_analysis", "anchor-window-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
        [
            "external_analysis",
            "candidate-baseline-run",
            "--export-db-path",
            str(export_db),
            "--label-db-path",
            str(label_db),
            "--result-db-path",
            str(result_db),
            "--similarity-db-path",
            str(similarity_db),
            "--as-of-date",
            str(dates[50]),
            "--publish-id",
            "pub_2026-03-13_20260313T220000Z_01",
        ],
    ]
    for argv in commands:
        monkeypatch.setattr(sys, "argv", argv)
        assert external_analysis_main() == 0
    return dates[50]


def _make_client(monkeypatch):
    import app.main as main_module

    class _NoopThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self) -> None:
            return None

    monkeypatch.setattr(main_module, "init_resources", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(main_module, "start_yf_daily_ingest_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_yf_daily_ingest_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_ranking_analysis_quality_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_ranking_analysis_quality_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module, "start_analysis_prewarm_scheduler", lambda: None)
    monkeypatch.setattr(main_module, "stop_analysis_prewarm_scheduler", lambda timeout_sec=1.0: None)
    monkeypatch.setattr(main_module.threading, "Thread", _NoopThread)
    return TestClient(main_module.create_app())


def test_similarity_nightly_metrics_is_idempotent_for_same_publish_id(monkeypatch, tmp_path) -> None:
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

    payload_1 = run_nightly_similarity_pipeline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(as_of_date),
        publish_id="pub_2026-03-13_20260313T220000Z_01",
    )
    payload_2 = run_nightly_similarity_pipeline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(as_of_date),
        publish_id="pub_2026-03-13_20260313T220000Z_01",
    )

    conn = duckdb.connect(str(similarity_db), read_only=True)
    try:
        metric_count = conn.execute(
            "SELECT COUNT(*) FROM similarity_quality_metrics WHERE publish_id = ?",
            ["pub_2026-03-13_20260313T220000Z_01"],
        ).fetchone()
    finally:
        conn.close()
    assert payload_1["status"] == "success"
    assert payload_2["status"] == "success"
    assert int(metric_count[0]) == 1


def test_phase3_similarity_nightly_smoke_keeps_candidate_api_and_limits_payload(monkeypatch, tmp_path) -> None:
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

    argv = [
        "external_analysis",
        "nightly-similarity-run",
        "--export-db-path",
        str(export_db),
        "--label-db-path",
        str(label_db),
        "--result-db-path",
        str(result_db),
        "--similarity-db-path",
        str(similarity_db),
        "--ops-db-path",
        str(ops_db),
        "--as-of-date",
        str(as_of_date),
        "--publish-id",
        "pub_2026-03-13_20260313T220000Z_01",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0

    client = _make_client(monkeypatch)
    candidates = client.get("/api/analysis-bridge/candidates")
    regime = client.get("/api/analysis-bridge/regime")
    similar_cases = client.get("/api/analysis-bridge/similar-cases", params={"code": "1301", "limit": 99})
    first_case_id = similar_cases.json()["rows"][0]["case_id"]
    similar_paths = client.get("/api/analysis-bridge/similar-case-paths", params={"code": "1301", "case_id": first_case_id})

    similarity_conn = duckdb.connect(str(similarity_db), read_only=True)
    ops_conn = duckdb.connect(str(ops_db), read_only=True)
    try:
        metric_row = similarity_conn.execute(
            "SELECT publish_id, top_k, returned_case_count FROM similarity_quality_metrics WHERE publish_id = ?",
            ["pub_2026-03-13_20260313T220000Z_01"],
        ).fetchone()
        run_row = ops_conn.execute(
            "SELECT status, publish_id FROM external_job_runs WHERE job_type = 'nightly_similarity_pipeline' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        similarity_conn.close()
        ops_conn.close()

    assert candidates.status_code == 200
    assert candidates.json()["degraded"] is False
    assert regime.status_code == 200
    assert regime.json()["degraded"] is False
    assert similar_cases.status_code == 200
    assert similar_cases.json()["degraded"] is False
    assert len(similar_cases.json()["rows"]) <= 5
    assert similar_paths.status_code == 200
    assert similar_paths.json()["degraded"] is False
    assert len(similar_paths.json()["rows"]) <= 20
    assert metric_row == ("pub_2026-03-13_20260313T220000Z_01", 5, metric_row[2])
    assert int(metric_row[2]) > 0
    assert run_row == ("success", "pub_2026-03-13_20260313T220000Z_01")


def test_similarity_nightly_quarantines_metrics_failure_without_breaking_publish(monkeypatch, tmp_path) -> None:
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

    def _raise_metrics_failure(*, similarity_db_path, metrics):
        raise RuntimeError("forced_similarity_metrics_failure")

    monkeypatch.setattr(similarity_baseline_module, "_persist_similarity_quality_metrics", _raise_metrics_failure)

    payload = run_nightly_similarity_pipeline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(as_of_date),
        publish_id="pub_2026-03-13_20260313T220000Z_01",
    )

    result_conn = duckdb.connect(str(result_db), read_only=True)
    similarity_conn = duckdb.connect(str(similarity_db), read_only=True)
    ops_conn = duckdb.connect(str(ops_db), read_only=True)
    try:
        similar_count = result_conn.execute(
            "SELECT COUNT(*) FROM similar_cases_daily WHERE publish_id = ?",
            ["pub_2026-03-13_20260313T220000Z_01"],
        ).fetchone()
        metric_count = similarity_conn.execute(
            "SELECT COUNT(*) FROM similarity_quality_metrics WHERE publish_id = ?",
            ["pub_2026-03-13_20260313T220000Z_01"],
        ).fetchone()
        run_row = ops_conn.execute(
            "SELECT status, error_class FROM external_job_runs WHERE job_type = 'nightly_similarity_pipeline' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        quarantine_row = ops_conn.execute(
            "SELECT reason, publish_id FROM external_job_quarantine WHERE job_type = 'nightly_similarity_pipeline' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        result_conn.close()
        similarity_conn.close()
        ops_conn.close()

    client = _make_client(monkeypatch)
    candidates = client.get("/api/analysis-bridge/candidates")
    similar_cases = client.get("/api/analysis-bridge/similar-cases", params={"code": "1301", "limit": 5})

    assert payload["status"] == "published_with_metrics_failure"
    assert int(similar_count[0]) > 0
    assert int(metric_count[0]) == 0
    assert run_row == ("published_with_metrics_failure", "RuntimeError")
    assert quarantine_row == ("similarity_metrics_persist_failed", "pub_2026-03-13_20260313T220000Z_01")
    assert candidates.status_code == 200
    assert candidates.json()["degraded"] is False
    assert similar_cases.status_code == 200
    assert similar_cases.json()["degraded"] is False
    assert len(similar_cases.json()["rows"]) > 0


def test_similarity_nightly_throttled_mode_reduces_top_k(monkeypatch, tmp_path) -> None:
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

    payload = run_nightly_similarity_pipeline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(as_of_date),
        publish_id="pub_2026-03-13_20260313T220100Z_01",
        load_control={"mode": "throttled", "reason": "meemee_foreground_active"},
    )

    similarity_conn = duckdb.connect(str(similarity_db), read_only=True)
    try:
        metric_row = similarity_conn.execute(
            "SELECT top_k FROM similarity_quality_metrics WHERE publish_id = ?",
            ["pub_2026-03-13_20260313T220100Z_01"],
        ).fetchone()
    finally:
        similarity_conn.close()

    assert payload["ok"] is True
    assert payload["runtime_budget"]["similarity_top_k"] == 3
    assert payload["similarity"]["top_k"] == 3
    assert metric_row == (3,)
