from __future__ import annotations

import sys
from datetime import date, timedelta

import duckdb

from app.backend.services.analysis_bridge.reader import get_analysis_bridge_snapshot
from external_analysis.__main__ import main as external_analysis_main
from external_analysis.models import candidate_baseline as candidate_baseline_module
from external_analysis.models.candidate_baseline import run_candidate_baseline
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.runtime.nightly_pipeline import run_nightly_candidate_pipeline


def _weekday_ints(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def _seed_source_db(source_db: str) -> list[int]:
    conn = duckdb.connect(source_db)
    dates = _weekday_ints(date(2026, 1, 5), 70)
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source TEXT)")
        conn.execute("CREATE TABLE daily_ma (code TEXT, date INTEGER, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE)")
        conn.execute(
            "CREATE TABLE feature_snapshot_daily (dt INTEGER, code TEXT, close DOUBLE, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE, atr14 DOUBLE, diff20_pct DOUBLE, diff20_atr DOUBLE, cnt_20_above INTEGER, cnt_7_above INTEGER, day_count INTEGER, candle_flags TEXT)"
        )
        conn.execute("CREATE TABLE monthly_bars (code TEXT, month INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT)")
        conn.execute("CREATE TABLE positions_live (symbol TEXT, spot_qty DOUBLE, margin_long_qty DOUBLE, margin_short_qty DOUBLE, buy_qty DOUBLE, sell_qty DOUBLE, opened_at TIMESTAMP, updated_at TIMESTAMP, has_issue BOOLEAN, issue_note TEXT)")
        conn.execute("CREATE TABLE position_rounds (round_id TEXT, symbol TEXT, opened_at TIMESTAMP, closed_at TIMESTAMP, closed_reason TEXT)")
        for idx, trade_date in enumerate(dates):
            for code, slope in (("1301", 1.2), ("1302", 0.7), ("1303", -1.0), ("1304", -0.6)):
                base = 100.0 if code != "1303" else 140.0
                close_price = base + (idx * slope)
                conn.execute(
                    "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [code, trade_date, close_price - 0.5, close_price + 1.5, close_price - 1.5, close_price, 1000 + idx, "pan"],
                )
                ma20 = close_price - 2.0 if slope > 0 else close_price + 2.0
                conn.execute(
                    "INSERT INTO daily_ma VALUES (?, ?, ?, ?, ?)",
                    [code, trade_date, ma20 + 1.0, ma20, ma20 - 3.0],
                )
                conn.execute(
                    "INSERT INTO feature_snapshot_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [trade_date, code, close_price, ma20 + 1.0, ma20, ma20 - 3.0, 3.0, (close_price / ma20) - 1.0, 1.0, 18 if slope > 0 else 4, 6 if slope > 0 else 1, 20, "nightly"],
                )
        conn.execute("INSERT INTO monthly_bars VALUES ('1301', 202603, 90, 120, 88, 110, 10000)")
    finally:
        conn.close()
    return dates


def _run_phase1_inputs(monkeypatch, source_db: str, export_db: str, label_db: str, result_db: str, ops_db: str) -> list[int]:
    dates = _seed_source_db(source_db)
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    commands = [
        ["external_analysis", "init-result-db", "--db-path", str(result_db)],
        ["external_analysis", "init-export-db", "--db-path", str(export_db)],
        ["external_analysis", "init-label-db", "--db-path", str(label_db)],
        ["external_analysis", "init-ops-db", "--db-path", str(ops_db)],
        ["external_analysis", "export-sync", "--source-db-path", str(source_db), "--export-db-path", str(export_db)],
        ["external_analysis", "label-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
    ]
    for argv in commands:
        monkeypatch.setattr(sys, "argv", argv)
        assert external_analysis_main() == 0
    return dates


def test_nightly_candidate_metrics_is_idempotent_for_same_publish_id(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    payload_1 = run_candidate_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=dates[45],
        publish_id="pub_2026-03-12_20260312T235000Z_01",
    )
    payload_2 = run_candidate_baseline(
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        as_of_date=dates[45],
        publish_id="pub_2026-03-12_20260312T235000Z_01",
    )

    conn = duckdb.connect(str(result_db), read_only=True)
    try:
        metric_count = conn.execute(
            "SELECT COUNT(*) FROM nightly_candidate_metrics WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235000Z_01"],
        ).fetchone()
    finally:
        conn.close()
    assert payload_1["metrics_saved"] is True
    assert payload_2["metrics_saved"] is True
    assert int(metric_count[0]) == 1


def test_phase2_slice_f_smoke_runs_nightly_pipeline_end_to_end(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    argv = [
        "external_analysis",
        "nightly-candidate-run",
        "--source-db-path",
        str(source_db),
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
        str(dates[45]),
        "--publish-id",
        "pub_2026-03-12_20260312T235500Z_01",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0

    snapshot = get_analysis_bridge_snapshot()
    result_conn = duckdb.connect(str(result_db), read_only=True)
    ops_conn = duckdb.connect(str(ops_db), read_only=True)
    try:
        metric_row = result_conn.execute(
            "SELECT publish_id, candidate_count_long, candidate_count_short FROM nightly_candidate_metrics WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235500Z_01"],
        ).fetchone()
        job_row = ops_conn.execute(
            "SELECT status, publish_id FROM external_job_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        result_conn.close()
        ops_conn.close()
    assert snapshot["degraded"] is False
    assert snapshot["publish"]["publish_id"] == "pub_2026-03-12_20260312T235500Z_01"
    assert metric_row is not None
    assert int(metric_row[1]) > 0
    assert int(metric_row[2]) > 0
    assert job_row == ("success", "pub_2026-03-12_20260312T235500Z_01")


def test_nightly_pipeline_quarantines_metrics_failure_without_breaking_publish(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))
    ensure_ops_db(str(ops_db))

    def _raise_metrics_failure(*, result_db_path, metrics_row):
        raise RuntimeError("forced_metrics_failure")

    monkeypatch.setattr(candidate_baseline_module, "_persist_nightly_metrics", _raise_metrics_failure)

    payload = run_nightly_candidate_pipeline(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235900Z_01",
    )

    snapshot = get_analysis_bridge_snapshot()
    result_conn = duckdb.connect(str(result_db), read_only=True)
    ops_conn = duckdb.connect(str(ops_db), read_only=True)
    try:
        candidate_count = result_conn.execute(
            "SELECT COUNT(*) FROM candidate_daily WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235900Z_01"],
        ).fetchone()
        metric_count = result_conn.execute(
            "SELECT COUNT(*) FROM nightly_candidate_metrics WHERE publish_id = ?",
            ["pub_2026-03-12_20260312T235900Z_01"],
        ).fetchone()
        run_row = ops_conn.execute(
            "SELECT status, error_class FROM external_job_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        quarantine_row = ops_conn.execute(
            "SELECT reason, publish_id FROM external_job_quarantine ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    finally:
        result_conn.close()
        ops_conn.close()

    assert payload["status"] == "published_with_metrics_failure"
    assert snapshot["degraded"] is False
    assert snapshot["publish"]["publish_id"] == "pub_2026-03-12_20260312T235900Z_01"
    assert int(candidate_count[0]) > 0
    assert int(metric_count[0]) == 0
    assert run_row == ("published_with_metrics_failure", "RuntimeError")
    assert quarantine_row == ("nightly_metrics_persist_failed", "pub_2026-03-12_20260312T235900Z_01")


def test_nightly_pipeline_throttled_mode_reduces_candidate_limit(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _run_phase1_inputs(monkeypatch, str(source_db), str(export_db), str(label_db), str(result_db), str(ops_db))

    payload = run_nightly_candidate_pipeline(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        label_db_path=str(label_db),
        result_db_path=str(result_db),
        similarity_db_path=str(similarity_db),
        ops_db_path=str(ops_db),
        as_of_date=str(dates[45]),
        publish_id="pub_2026-03-12_20260312T235910Z_01",
        load_control={"mode": "throttled", "reason": "meemee_foreground_active"},
    )

    assert payload["ok"] is True
    assert payload["runtime_budget"]["candidate_limit_per_side"] == 8
    assert payload["baseline"]["candidate_limit_per_side"] == 8
    assert payload["baseline"]["candidate_count_long"] <= 8
    assert payload["baseline"]["candidate_count_short"] <= 8
