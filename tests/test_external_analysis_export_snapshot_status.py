from __future__ import annotations

import json

import duckdb

from external_analysis.exporter.export_schema import ensure_export_db
from external_analysis.exporter.snapshot_status import (
    EXPORT_SNAPSHOT_REASON_COMPLETE_MATCH,
    EXPORT_SNAPSHOT_REASON_MAX_TRADE_DATE_MISMATCH,
    EXPORT_SNAPSHOT_REASON_META_MISSING,
    EXPORT_SNAPSHOT_REASON_REQUIRED_COUNT_MISMATCH,
    EXPORT_SNAPSHOT_REASON_SOURCE_SIGNATURE_MISMATCH,
    EXPORT_SNAPSHOT_STATUS_COMPLETE,
    EXPORT_SNAPSHOT_STATUS_INCOMPLETE,
    EXPORT_SNAPSHOT_STATUS_MISMATCHED,
    EXPORT_SNAPSHOT_STATUS_STALE,
    build_export_snapshot,
    probe_export_snapshot_readiness,
    resolve_snapshot_progress_path,
    resolve_snapshot_status_path,
)


def _seed_source_db(source_db: str) -> None:
    conn = duckdb.connect(source_db)
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source TEXT)")
        conn.execute("CREATE TABLE daily_ma (code TEXT, date INTEGER, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE)")
        conn.execute(
            "CREATE TABLE feature_snapshot_daily (dt INTEGER, code TEXT, close DOUBLE, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE, atr14 DOUBLE, diff20_pct DOUBLE, diff20_atr DOUBLE, cnt_20_above INTEGER, cnt_7_above INTEGER, day_count INTEGER, candle_flags TEXT)"
        )
        conn.execute("CREATE TABLE monthly_bars (code TEXT, month INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT)")
        conn.execute(
            "CREATE TABLE positions_live (symbol TEXT, spot_qty DOUBLE, margin_long_qty DOUBLE, margin_short_qty DOUBLE, buy_qty DOUBLE, sell_qty DOUBLE, opened_at TIMESTAMP, updated_at TIMESTAMP, has_issue BOOLEAN, issue_note TEXT)"
        )
        conn.execute(
            "CREATE TABLE trade_events (broker TEXT, exec_dt TIMESTAMP, symbol TEXT, action TEXT, qty DOUBLE, price DOUBLE, source_row_hash TEXT)"
        )
        conn.execute(
            "CREATE TABLE position_rounds (round_id TEXT, symbol TEXT, opened_at TIMESTAMP, closed_at TIMESTAMP, closed_reason TEXT)"
        )
        conn.execute(
            """
            INSERT INTO daily_bars VALUES
            ('1301', 20260309, 100, 101, 99, 100, 1000, 'pan'),
            ('1301', 20260310, 101, 103, 100, 102, 1200, 'pan')
            """
        )
        conn.execute(
            """
            INSERT INTO daily_ma VALUES
            ('1301', 20260309, 99, 98, 97),
            ('1301', 20260310, 100, 99, 98)
            """
        )
        conn.execute(
            """
            INSERT INTO feature_snapshot_daily VALUES
            (20260309, '1301', 100, 99, 98, 97, 2.5, 0.02, 1.0, 3, 5, 20, 'flag-a'),
            (20260310, '1301', 102, 100, 99, 98, 2.6, 0.03, 1.1, 4, 6, 21, 'flag-b')
            """
        )
        conn.execute("INSERT INTO monthly_bars VALUES ('1301', 202603, 90, 105, 88, 102, 10000)")
        conn.execute(
            """
            INSERT INTO positions_live VALUES
            ('1301', 100, 0, 0, 100, 0, TIMESTAMP '2026-03-10 09:00:00', TIMESTAMP '2026-03-10 15:00:00', FALSE, NULL)
            """
        )
        conn.execute(
            """
            INSERT INTO trade_events VALUES
            ('rakuten', TIMESTAMP '2026-03-09 09:00:00', '1301', 'SPOT_BUY', 100, 100, 'hash-1'),
            ('rakuten', TIMESTAMP '2026-03-10 15:00:00', '1301', 'SPOT_SELL', 100, 102, 'hash-2')
            """
        )
        conn.execute(
            """
            INSERT INTO position_rounds VALUES
            ('r1', '1301', TIMESTAMP '2026-03-09 09:00:00', TIMESTAMP '2026-03-10 15:00:00', 'tp')
            """
        )
    finally:
        conn.close()


def test_probe_export_snapshot_readiness_reports_meta_missing(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    _seed_source_db(str(source_db))
    build_export_snapshot(str(source_db), str(export_db))

    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        conn.execute("DELETE FROM meta_export_runs")
    finally:
        conn.close()

    payload = probe_export_snapshot_readiness(str(source_db), str(export_db))
    assert payload["status"] == EXPORT_SNAPSHOT_STATUS_INCOMPLETE
    assert payload["reason_code"] == EXPORT_SNAPSHOT_REASON_META_MISSING
    assert payload["reusable"] is False
    assert payload["progress_status"] == "complete"
    assert payload["progress_path"]


def test_probe_export_snapshot_readiness_reports_partial_counts(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    _seed_source_db(str(source_db))
    build_export_snapshot(str(source_db), str(export_db))

    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        conn.execute("DELETE FROM pattern_state_export WHERE code = '1301' AND trade_date = 20260310")
    finally:
        conn.close()

    payload = probe_export_snapshot_readiness(str(source_db), str(export_db))
    assert payload["status"] == EXPORT_SNAPSHOT_STATUS_INCOMPLETE
    assert payload["reason_code"] == EXPORT_SNAPSHOT_REASON_REQUIRED_COUNT_MISMATCH
    assert payload["reusable"] is False


def test_probe_export_snapshot_readiness_reports_stale_max_trade_date(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    _seed_source_db(str(source_db))
    build_export_snapshot(str(source_db), str(export_db))

    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute("INSERT INTO daily_bars VALUES ('1301', 20260311, 102, 104, 101, 103, 1300, 'pan')")
        conn.execute("INSERT INTO daily_ma VALUES ('1301', 20260311, 101, 100, 99)")
        conn.execute(
            "INSERT INTO feature_snapshot_daily VALUES (20260311, '1301', 103, 101, 100, 99, 2.7, 0.04, 1.2, 4, 7, 22, 'flag-c')"
        )
    finally:
        conn.close()

    payload = probe_export_snapshot_readiness(str(source_db), str(export_db))
    assert payload["status"] == EXPORT_SNAPSHOT_STATUS_STALE
    assert payload["reason_code"] == EXPORT_SNAPSHOT_REASON_MAX_TRADE_DATE_MISMATCH
    assert payload["reusable"] is False


def test_probe_export_snapshot_readiness_reports_source_signature_mismatch(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    _seed_source_db(str(source_db))
    build_export_snapshot(str(source_db), str(export_db))

    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute(
            """
            INSERT INTO positions_live VALUES
            ('1301', 0, 10, 0, 0, 10, TIMESTAMP '2026-03-11 09:00:00', TIMESTAMP '2026-03-11 15:00:00', FALSE, NULL)
            """
        )
    finally:
        conn.close()

    payload = probe_export_snapshot_readiness(str(source_db), str(export_db))
    assert payload["status"] == EXPORT_SNAPSHOT_STATUS_MISMATCHED
    assert payload["reason_code"] == EXPORT_SNAPSHOT_REASON_SOURCE_SIGNATURE_MISMATCH
    assert payload["reusable"] is False


def test_probe_export_snapshot_readiness_reports_complete_match(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    _seed_source_db(str(source_db))

    build_payload = build_export_snapshot(str(source_db), str(export_db))
    status_path = resolve_snapshot_status_path(str(export_db))
    progress_path = resolve_snapshot_progress_path(str(export_db))

    assert status_path.exists()
    assert progress_path.exists()
    assert build_payload["status"] == EXPORT_SNAPSHOT_STATUS_COMPLETE
    assert build_payload["reason_code"] == EXPORT_SNAPSHOT_REASON_COMPLETE_MATCH
    assert build_payload["reusable"] is True

    probe_payload = probe_export_snapshot_readiness(str(source_db), str(export_db))
    assert probe_payload["status"] == EXPORT_SNAPSHOT_STATUS_COMPLETE
    assert probe_payload["reason_code"] == EXPORT_SNAPSHOT_REASON_COMPLETE_MATCH
    assert probe_payload["reusable"] is True
    assert probe_payload["required_fields"] == ["bars_count", "indicator_count", "pattern_count", "max_trade_date"]
    assert probe_payload["progress_status"] == "complete"
    assert probe_payload["last_completed_step"] == "meta_export_runs"
    assert probe_payload["incomplete_steps"] == []
    progress_payload = json.loads(progress_path.read_text(encoding="utf-8"))
    step_names = [step["step_name"] for step in progress_payload["steps"]]
    assert "indicator_daily_export" in step_names
    assert "pattern_state_export" in step_names
    indicator_step = next(step for step in progress_payload["steps"] if step["step_name"] == "indicator_daily_export")
    pattern_step = next(step for step in progress_payload["steps"] if step["step_name"] == "pattern_state_export")
    assert indicator_step["status"] == "complete"
    assert pattern_step["status"] == "complete"


def test_build_export_snapshot_resumes_completed_steps_for_same_source_signature(tmp_path) -> None:
    source_db = tmp_path / "source_resume.duckdb"
    export_db = tmp_path / "export_resume.duckdb"
    _seed_source_db(str(source_db))

    first_payload = build_export_snapshot(str(source_db), str(export_db))
    first_run_id = str(first_payload["latest_export_run"]["run_id"])

    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        conn.execute("DELETE FROM bars_monthly_export")
        conn.execute("DELETE FROM indicator_daily_export")
        conn.execute("DELETE FROM pattern_state_export")
        conn.execute("DELETE FROM trade_event_export")
        conn.execute("DELETE FROM position_snapshot_export")
        conn.execute("DELETE FROM meta_export_runs")
        bars_run_id_before = str(conn.execute("SELECT export_run_id FROM bars_daily_export LIMIT 1").fetchone()[0])
    finally:
        conn.close()

    second_payload = build_export_snapshot(str(source_db), str(export_db))
    assert second_payload["status"] == EXPORT_SNAPSHOT_STATUS_COMPLETE
    assert second_payload["reason_code"] == EXPORT_SNAPSHOT_REASON_COMPLETE_MATCH
    assert str(second_payload["latest_export_run"]["run_id"]) != first_run_id

    conn = duckdb.connect(str(export_db), read_only=True)
    try:
        bars_run_id_after = str(conn.execute("SELECT export_run_id FROM bars_daily_export LIMIT 1").fetchone()[0])
        indicator_run_id_after = str(conn.execute("SELECT export_run_id FROM indicator_daily_export LIMIT 1").fetchone()[0])
        progress_rows = conn.execute(
            """
            SELECT step_name, status
            FROM meta_export_table_progress
            WHERE run_id = ?
            ORDER BY step_name
            """,
            [second_payload["latest_export_run"]["run_id"]],
        ).fetchall()
    finally:
        conn.close()

    assert bars_run_id_after == bars_run_id_before
    assert indicator_run_id_after == second_payload["latest_export_run"]["run_id"]
    progress_by_step = {str(step_name): str(status) for step_name, status in progress_rows}
    assert progress_by_step["bars_daily_export"] == "resumed"
    assert progress_by_step["indicator_daily_export"] == "complete"
    assert progress_by_step["pattern_state_export"] == "complete"
