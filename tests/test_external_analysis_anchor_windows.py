from __future__ import annotations

from datetime import date, timedelta

import duckdb

from external_analysis.exporter.export_schema import ensure_export_db
from external_analysis.labels.anchor_windows import build_anchor_windows
from external_analysis.labels.store import ensure_label_db
from tests.test_external_analysis_rolling_labels import _insert_export_meta


def _weekday_ints(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def test_build_anchor_windows_creates_complete_jpx_windows_and_overlap_groups(tmp_path) -> None:
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    ensure_export_db(str(export_db))
    ensure_label_db(str(label_db))
    dates = _weekday_ints(date(2026, 1, 5), 70)
    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        for idx, trade_date in enumerate(dates):
            close_value = 100.0
            high_value = 101.0
            low_value = 99.0
            volume = 1000
            if idx == 25:
                close_value = 110.0
                high_value = 111.0
            if idx == 30:
                volume = 3000
            if idx == 31:
                close_value = 112.0
                high_value = 113.0
            conn.execute(
                """
                INSERT INTO bars_daily_export
                (code, trade_date, o, h, l, c, v, source, row_hash, export_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ["1301", trade_date, 100.0, high_value, low_value, close_value, volume, "pan", f"bar-{idx}", "run-1"],
            )
            conn.execute(
                """
                INSERT INTO indicator_daily_export
                (code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr, cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ["1301", trade_date, None, 100.0, None, None, None, None, None, None, None, None, None, None, f"ind-{idx}", "run-1"],
            )
    finally:
        conn.close()

    payload = build_anchor_windows(str(export_db), str(label_db))

    assert payload["ok"] is True
    conn = duckdb.connect(str(label_db), read_only=True)
    try:
        master = conn.execute(
            """
            SELECT anchor_type, overlap_group_id
            FROM anchor_window_master
            WHERE code = '1301'
            ORDER BY anchor_date, anchor_type
            """
        ).fetchall()
        window_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM anchor_window_bars
            WHERE anchor_id = (
                SELECT anchor_id
                FROM anchor_window_master
                WHERE code = '1301'
                ORDER BY anchor_date, anchor_type
                LIMIT 1
            )
            """
        ).fetchone()[0]
    finally:
        conn.close()
    assert master
    assert int(window_count) == 41
    overlap_groups = {str(row[1]) for row in master}
    assert len(overlap_groups) <= len(master)


def test_build_anchor_windows_rebuilds_only_dirty_codes(tmp_path) -> None:
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    ensure_export_db(str(export_db))
    ensure_label_db(str(label_db))
    dates = _weekday_ints(date(2026, 1, 5), 70)
    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        for idx, trade_date in enumerate(dates):
            for code, breakout_idx in (("1301", 25), ("1302", 31)):
                close_value = 100.0
                high_value = 101.0
                volume = 1000
                if idx == breakout_idx:
                    close_value = 112.0
                    high_value = 113.0
                    volume = 3000
                conn.execute(
                    """
                    INSERT INTO bars_daily_export
                    (code, trade_date, o, h, l, c, v, source, row_hash, export_run_id)
                    VALUES (?, ?, 100.0, ?, 99.0, ?, ?, 'pan', ?, 'run-1')
                    """,
                    [code, trade_date, high_value, close_value, volume, f"{code}-bar-{idx}"],
                )
                conn.execute(
                    """
                    INSERT INTO indicator_daily_export
                    (code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr, cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id)
                    VALUES (?, ?, NULL, 100.0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, 'run-1')
                    """,
                    [code, trade_date, f"{code}-ind-{idx}"],
                )
    finally:
        conn.close()
    _insert_export_meta(str(export_db), run_id="run-1", source_signature="sig-1", changed_table_names=["bars_daily_export", "indicator_daily_export"], diff_reason={"bars_daily_export": {"inserted": 140, "updated": 0, "deleted": 0}, "indicator_daily_export": {"inserted": 140, "updated": 0, "deleted": 0}})
    first_payload = build_anchor_windows(str(export_db), str(label_db))
    assert first_payload["skipped"] is False

    conn = duckdb.connect(str(label_db), read_only=True)
    try:
        untouched_before = conn.execute("SELECT MIN(generation_run_id) FROM anchor_window_master WHERE code = '1302'").fetchone()[0]
        changed_before = conn.execute("SELECT MIN(generation_run_id) FROM anchor_window_master WHERE code = '1301'").fetchone()[0]
    finally:
        conn.close()

    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        conn.execute("UPDATE bars_daily_export SET c = 118.0, row_hash = '1301-r2', export_run_id = 'run-2' WHERE code = '1301' AND trade_date = ?", [dates[25]])
        conn.execute("UPDATE indicator_daily_export SET ma20 = 98.0, row_hash = '1301-i-r2', export_run_id = 'run-2' WHERE code = '1301' AND trade_date = ?", [dates[25]])
    finally:
        conn.close()
    _insert_export_meta(str(export_db), run_id="run-2", source_signature="sig-2", changed_table_names=["bars_daily_export", "indicator_daily_export"], diff_reason={"bars_daily_export": {"inserted": 0, "updated": 1, "deleted": 0}, "indicator_daily_export": {"inserted": 0, "updated": 1, "deleted": 0}})
    second_payload = build_anchor_windows(str(export_db), str(label_db))

    conn = duckdb.connect(str(label_db), read_only=True)
    try:
        untouched_after = conn.execute("SELECT MIN(generation_run_id) FROM anchor_window_master WHERE code = '1302'").fetchone()[0]
        changed_after = conn.execute("SELECT MIN(generation_run_id) FROM anchor_window_master WHERE code = '1301'").fetchone()[0]
    finally:
        conn.close()
    assert second_payload["cache_state"] == "partial_stale"
    assert untouched_after == untouched_before
    assert changed_after != changed_before
