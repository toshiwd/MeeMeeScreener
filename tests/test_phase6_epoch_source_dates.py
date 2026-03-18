from __future__ import annotations

from datetime import datetime, timedelta, timezone

import duckdb

from external_analysis.exporter.diff_export import run_diff_export
from external_analysis.exporter.source_reader import normalize_market_date
from external_analysis.runtime.historical_replay import _select_replay_dates


def _epoch_ymd(year: int, month: int, day: int) -> tuple[int, int]:
    dt = datetime(year, month, day, tzinfo=timezone.utc)
    return int(dt.timestamp()), int(dt.strftime("%Y%m%d"))


def test_epoch_source_dates_are_normalized_for_export_and_replay(tmp_path) -> None:
    source_db = tmp_path / "source_epoch.duckdb"
    export_db = tmp_path / "export_epoch.duckdb"
    conn = duckdb.connect(str(source_db), read_only=False)
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date BIGINT, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source TEXT)")
        conn.execute("CREATE TABLE daily_ma (code TEXT, date BIGINT, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE)")
        conn.execute(
            """
            CREATE TABLE feature_snapshot_daily (
                code TEXT, dt BIGINT, atr14 DOUBLE, diff20_pct DOUBLE, diff20_atr DOUBLE,
                cnt_20_above DOUBLE, cnt_7_above DOUBLE, day_count DOUBLE, candle_flags TEXT
            )
            """
        )
        conn.execute("CREATE TABLE monthly_bars (code TEXT, month BIGINT, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE)")
        base = datetime(2026, 1, 5, tzinfo=timezone.utc)
        for idx in range(10):
            dt = base + timedelta(days=idx)
            epoch = int(dt.timestamp())
            close = 100.0 + idx
            conn.execute(
                "INSERT INTO daily_bars VALUES ('1301', ?, ?, ?, ?, ?, ?, 'pan')",
                [epoch, close - 1, close + 1, close - 2, close, 1000 + idx],
            )
            conn.execute("INSERT INTO daily_ma VALUES ('1301', ?, ?, ?, ?)", [epoch, close - 3, close - 2, close - 1])
            conn.execute(
                "INSERT INTO feature_snapshot_daily VALUES ('1301', ?, 1.0, 0.1, 0.2, 3, 2, ?, 'flag')",
                [epoch, idx + 1],
            )
        month_epoch = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
        conn.execute("INSERT INTO monthly_bars VALUES ('1301', ?, 90, 110, 80, 105, 5000)", [month_epoch])
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    export_payload = run_diff_export(source_db_path=str(source_db), export_db_path=str(export_db))
    replay_dates = _select_replay_dates(
        source_db_path=str(source_db),
        start_as_of_date="20260105",
        end_as_of_date="20260114",
    )

    export_conn = duckdb.connect(str(export_db), read_only=True)
    try:
        trade_dates = [row[0] for row in export_conn.execute("SELECT trade_date FROM bars_daily_export ORDER BY trade_date").fetchall()]
        monthly_keys = [row[0] for row in export_conn.execute("SELECT month_key FROM bars_monthly_export").fetchall()]
    finally:
        export_conn.close()

    assert export_payload["ok"] is True
    assert trade_dates[0] == 20260105
    assert trade_dates[-1] == 20260114
    assert monthly_keys == [20260101]
    assert replay_dates == trade_dates


def test_normalize_market_date_supports_legacy_9_digit_epoch_seconds() -> None:
    epoch_1994, ymd_1994 = _epoch_ymd(1994, 6, 14)
    assert epoch_1994 == 771552000
    assert normalize_market_date(epoch_1994) == ymd_1994
