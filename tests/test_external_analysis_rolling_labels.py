from __future__ import annotations

from datetime import date, timedelta

import duckdb

from external_analysis.exporter.export_schema import ensure_export_db
from external_analysis.labels.rolling_labels import build_rolling_labels
from external_analysis.labels.store import ensure_label_db


def _weekday_ints(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def _insert_export_meta(export_db: str, *, run_id: str, source_signature: str, changed_table_names: list[str], diff_reason: dict[str, dict[str, int]]) -> None:
    conn = duckdb.connect(export_db, read_only=False)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO meta_export_runs (
                run_id, started_at, finished_at, status, source_db_path, source_signature,
                source_max_trade_date, source_row_counts, changed_table_names, diff_reason
            ) VALUES (?, NOW(), NOW(), 'success', ?, ?, ?, '{}', ?, ?)
            """,
            [run_id, export_db, source_signature, 20260331, __import__("json").dumps(changed_table_names), __import__("json").dumps(diff_reason)],
        )
    finally:
        conn.close()


def test_build_rolling_labels_creates_jpx_horizon_labels(tmp_path) -> None:
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    ensure_export_db(str(export_db))
    ensure_label_db(str(label_db))
    dates = _weekday_ints(date(2026, 1, 5), 70)
    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        for idx, trade_date in enumerate(dates):
            conn.execute(
                """
                INSERT INTO bars_daily_export
                (code, trade_date, o, h, l, c, v, source, row_hash, export_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ["1301", trade_date, 100 + idx, 101 + idx, 99 + idx, 100 + idx, 1000 + idx, "pan", f"a-{idx}", "run-1"],
            )
            conn.execute(
                """
                INSERT INTO bars_daily_export
                (code, trade_date, o, h, l, c, v, source, row_hash, export_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ["1302", trade_date, 100 - (idx * 0.2), 100.5 - (idx * 0.2), 99 - (idx * 0.2), 99 - (idx * 0.2), 900 + idx, "pan", f"b-{idx}", "run-1"],
            )
            conn.execute(
                """
                INSERT INTO indicator_daily_export
                (code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr, cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ["1301", trade_date, 0, 95 + idx, 90 + idx, None, None, None, None, None, None, None, None, None, f"ia-{idx}", "run-1"],
            )
            conn.execute(
                """
                INSERT INTO indicator_daily_export
                (code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr, cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ["1302", trade_date, 0, 95 + idx, 90 + idx, None, None, None, None, None, None, None, None, None, f"ib-{idx}", "run-1"],
            )
    finally:
        conn.close()

    payload = build_rolling_labels(str(export_db), str(label_db))

    assert payload["ok"] is True
    conn = duckdb.connect(str(label_db), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT ret_h, rank_ret_h, top_5pct_h, purge_end_date, embargo_until_date
            FROM label_daily_h20
            WHERE code = '1301'
            ORDER BY as_of_date
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert float(row[0]) > 0
    assert int(row[1]) == 1
    assert bool(row[2]) is True
    assert int(row[4]) > int(row[3])


def test_build_rolling_labels_skips_when_export_signature_is_unchanged(tmp_path) -> None:
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    ensure_export_db(str(export_db))
    ensure_label_db(str(label_db))
    dates = _weekday_ints(date(2026, 1, 5), 70)
    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        for idx, trade_date in enumerate(dates):
            conn.execute(
                """
                INSERT INTO bars_daily_export
                (code, trade_date, o, h, l, c, v, source, row_hash, export_run_id)
                VALUES ('1301', ?, ?, ?, ?, ?, ?, 'pan', ?, 'run-1')
                """,
                [trade_date, 100 + idx, 101 + idx, 99 + idx, 100 + idx, 1000 + idx, f"a-{idx}"],
            )
            conn.execute(
                """
                INSERT INTO indicator_daily_export
                (code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr, cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id)
                VALUES ('1301', ?, 0, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, 'run-1')
                """,
                [trade_date, 95 + idx, 90 + idx, f"ia-{idx}"],
            )
    finally:
        conn.close()
    _insert_export_meta(str(export_db), run_id="run-1", source_signature="sig-1", changed_table_names=["bars_daily_export", "indicator_daily_export"], diff_reason={"bars_daily_export": {"inserted": 70, "updated": 0, "deleted": 0}, "indicator_daily_export": {"inserted": 70, "updated": 0, "deleted": 0}})

    first_payload = build_rolling_labels(str(export_db), str(label_db))
    second_payload = build_rolling_labels(str(export_db), str(label_db))

    assert first_payload["skipped"] is False
    assert second_payload["skipped"] is True
    assert second_payload["reason"] == "source_signature_unchanged"


def test_build_rolling_labels_rebuilds_only_affected_dates_on_partial_update(tmp_path) -> None:
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    ensure_export_db(str(export_db))
    ensure_label_db(str(label_db))
    dates = _weekday_ints(date(2026, 1, 5), 70)
    conn = duckdb.connect(str(export_db), read_only=False)
    target_date = dates[30]
    try:
        for idx, trade_date in enumerate(dates):
            for code, base in (("1301", 100.0), ("1302", 200.0)):
                conn.execute(
                    """
                    INSERT INTO bars_daily_export
                    (code, trade_date, o, h, l, c, v, source, row_hash, export_run_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'pan', ?, 'run-1')
                    """,
                    [code, trade_date, base + idx, base + idx + 1, base + idx - 1, base + idx, 1000 + idx, f"{code}-bar-{idx}"],
                )
                conn.execute(
                    """
                    INSERT INTO indicator_daily_export
                    (code, trade_date, ma7, ma20, ma60, ma100, ma200, atr14, diff20_pct, diff20_atr, cnt_20_above, cnt_7_above, day_count, candle_flags, row_hash, export_run_id)
                    VALUES (?, ?, 0, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, 'run-1')
                    """,
                    [code, trade_date, base + idx - 5, base + idx - 10, f"{code}-ind-{idx}"],
                )
    finally:
        conn.close()
    _insert_export_meta(str(export_db), run_id="run-1", source_signature="sig-1", changed_table_names=["bars_daily_export", "indicator_daily_export"], diff_reason={"bars_daily_export": {"inserted": 140, "updated": 0, "deleted": 0}, "indicator_daily_export": {"inserted": 140, "updated": 0, "deleted": 0}})
    first_payload = build_rolling_labels(str(export_db), str(label_db))
    assert first_payload["skipped"] is False

    conn = duckdb.connect(str(label_db), read_only=True)
    try:
        untouched_before = conn.execute("SELECT generation_run_id FROM label_daily_h20 WHERE code = '1302' AND as_of_date = ?", [dates[45]]).fetchone()[0]
        affected_before = conn.execute("SELECT generation_run_id FROM label_daily_h20 WHERE code = '1301' AND as_of_date = ?", [target_date]).fetchone()[0]
    finally:
        conn.close()

    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        conn.execute(
            """
            UPDATE bars_daily_export
            SET c = c + 10, row_hash = '1301-updated', export_run_id = 'run-2'
            WHERE code = '1301' AND trade_date = ?
            """,
            [target_date],
        )
        conn.execute(
            """
            UPDATE indicator_daily_export
            SET ma20 = ma20 + 5, row_hash = '1301-ind-updated', export_run_id = 'run-2'
            WHERE code = '1301' AND trade_date = ?
            """,
            [target_date],
        )
    finally:
        conn.close()
    _insert_export_meta(str(export_db), run_id="run-2", source_signature="sig-2", changed_table_names=["bars_daily_export", "indicator_daily_export"], diff_reason={"bars_daily_export": {"inserted": 0, "updated": 1, "deleted": 0}, "indicator_daily_export": {"inserted": 0, "updated": 1, "deleted": 0}})
    second_payload = build_rolling_labels(str(export_db), str(label_db))

    conn = duckdb.connect(str(label_db), read_only=True)
    try:
        untouched_after = conn.execute("SELECT generation_run_id FROM label_daily_h20 WHERE code = '1302' AND as_of_date = ?", [dates[45]]).fetchone()[0]
        affected_after = conn.execute("SELECT generation_run_id FROM label_daily_h20 WHERE code = '1301' AND as_of_date = ?", [target_date]).fetchone()[0]
    finally:
        conn.close()
    assert second_payload["cache_state"] == "partial_stale"
    assert untouched_after == untouched_before
    assert affected_after != affected_before
