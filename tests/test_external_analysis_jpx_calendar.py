from __future__ import annotations

import duckdb

from external_analysis.exporter.jpx_calendar import load_jpx_calendar, offset_trading_date, window_trading_dates


def test_load_jpx_calendar_and_offsets(tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    conn = duckdb.connect(str(source_db))
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source TEXT)")
        conn.execute(
            """
            INSERT INTO daily_bars VALUES
            ('1301', 20260306, 100, 101, 99, 100, 1000, 'pan'),
            ('1301', 20260309, 101, 102, 100, 101, 1100, 'pan'),
            ('1301', 20260310, 102, 103, 101, 102, 1200, 'pan')
            """
        )
    finally:
        conn.close()

    trading_dates = load_jpx_calendar(str(source_db))

    assert trading_dates == [20260306, 20260309, 20260310]
    assert offset_trading_date(trading_dates, 20260306, 1) == 20260309
    assert offset_trading_date(trading_dates, 20260309, -1) == 20260306
    assert window_trading_dates(trading_dates, 20260309, 1, 1) == [20260306, 20260309, 20260310]

