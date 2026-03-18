from __future__ import annotations

import json

import duckdb

from external_analysis.exporter.diff_export import run_diff_export
from external_analysis.exporter.export_schema import ensure_export_db


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


def test_run_diff_export_tracks_changes(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    _seed_source_db(str(source_db))
    ensure_export_db(str(export_db))

    first = run_diff_export(str(source_db), str(export_db))

    assert first["ok"] is True
    assert "bars_daily_export" in first["changed_table_names"]
    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        count = conn.execute("SELECT COUNT(*) FROM bars_daily_export").fetchone()[0]
        assert count == 2
        conn.execute("UPDATE bars_daily_export SET c = c")
    finally:
        conn.close()

    source_conn = duckdb.connect(str(source_db), read_only=False)
    try:
        source_conn.execute("UPDATE daily_bars SET c = 104 WHERE code = '1301' AND date = 20260310")
    finally:
        source_conn.close()

    second = run_diff_export(str(source_db), str(export_db))

    assert second["diff_reason"]["bars_daily_export"]["updated"] == 1
    conn = duckdb.connect(str(export_db), read_only=True)
    try:
        meta = conn.execute(
            "SELECT changed_table_names, diff_reason FROM meta_export_runs WHERE run_id = ?",
            [second["run_id"]],
        ).fetchone()
        trade_row = conn.execute(
            "SELECT event_type, broker_label FROM trade_event_export ORDER BY event_ts ASC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert "bars_daily_export" in json.loads(meta[0])
    assert json.loads(meta[1])["bars_daily_export"]["updated"] == 1
    assert trade_row == ("SPOT_BUY", "rakuten")
