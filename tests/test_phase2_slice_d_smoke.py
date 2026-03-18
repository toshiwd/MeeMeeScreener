from __future__ import annotations

import sys
from datetime import date, timedelta

import duckdb

from app.backend.services.analysis_bridge.reader import get_analysis_bridge_snapshot
from external_analysis.__main__ import main as external_analysis_main


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
                    [trade_date, code, close_price, ma20 + 1.0, ma20, ma20 - 3.0, 3.0, (close_price / ma20) - 1.0, 1.0, 18 if slope > 0 else 4, 6 if slope > 0 else 1, 20, "smoke"],
                )
        conn.execute("INSERT INTO monthly_bars VALUES ('1301', 202603, 90, 120, 88, 110, 10000)")
    finally:
        conn.close()
    return dates


def test_phase2_slice_d_smoke_runs_export_labels_candidate_and_bridge(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    similarity_db = tmp_path / "similarity.duckdb"
    dates = _seed_source_db(str(source_db))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))

    commands = [
        ["external_analysis", "init-result-db", "--db-path", str(result_db)],
        ["external_analysis", "init-export-db", "--db-path", str(export_db)],
        ["external_analysis", "init-label-db", "--db-path", str(label_db)],
        ["external_analysis", "export-sync", "--source-db-path", str(source_db), "--export-db-path", str(export_db)],
        ["external_analysis", "label-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
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
            str(dates[45]),
            "--publish-id",
            "pub_2026-03-12_20260312T220000Z_01",
        ],
    ]
    for argv in commands:
        monkeypatch.setattr(sys, "argv", argv)
        assert external_analysis_main() == 0

    conn = duckdb.connect(str(result_db), read_only=True)
    try:
        pointer = conn.execute(
            "SELECT publish_id FROM publish_pointer WHERE pointer_name='latest_successful'"
        ).fetchone()
        candidate_count = conn.execute(
            "SELECT COUNT(*) FROM candidate_daily WHERE publish_id='pub_2026-03-12_20260312T220000Z_01'"
        ).fetchone()
        regime_count = conn.execute(
            "SELECT COUNT(*) FROM regime_daily WHERE publish_id='pub_2026-03-12_20260312T220000Z_01'"
        ).fetchone()
    finally:
        conn.close()

    snapshot = get_analysis_bridge_snapshot()
    assert pointer == ("pub_2026-03-12_20260312T220000Z_01",)
    assert int(candidate_count[0]) > 0
    assert int(regime_count[0]) == 1
    assert snapshot["degraded"] is False
    assert snapshot["publish"]["publish_id"] == "pub_2026-03-12_20260312T220000Z_01"
    assert snapshot["public_table_counts"]["candidate_daily"] == int(candidate_count[0])
    assert snapshot["public_table_counts"]["regime_daily"] == 1
