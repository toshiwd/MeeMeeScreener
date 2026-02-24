from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
import sys

import duckdb
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services.ml_service import FEATURE_VERSION, refresh_ml_feature_table


def _month_start_ts(year: int, month: int) -> int:
    return int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp())


def _setup_minimum_source_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE feature_snapshot_daily (
            dt INTEGER,
            code TEXT,
            close DOUBLE,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            atr14 DOUBLE,
            diff20_pct DOUBLE,
            cnt_20_above INTEGER,
            cnt_7_above INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_bars (
            code TEXT,
            date INTEGER,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE daily_ma (
            code TEXT,
            date INTEGER,
            ma20 DOUBLE,
            ma60 DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE monthly_bars (
            code TEXT,
            month INTEGER,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT
        )
        """
    )
    conn.execute("CREATE TABLE tickers (code TEXT)")
    conn.execute("CREATE TABLE industry_master (code TEXT, sector33_code TEXT)")


def _seed_source_data(conn: duckdb.DuckDBPyConnection) -> None:
    codes = ["1001", "1111"]
    conn.executemany("INSERT INTO tickers (code) VALUES (?)", [(code,) for code in codes])
    conn.executemany(
        "INSERT INTO industry_master (code, sector33_code) VALUES (?, ?)",
        [("1001", "MKT"), ("1111", "TECH")],
    )

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    snapshot_rows: list[tuple] = []
    bar_rows: list[tuple] = []
    ma_rows: list[tuple] = []
    for i in range(90):
        dt = int((base + timedelta(days=i)).timestamp())
        market_close = 1000.0 + i
        stock_close = 100.0 + (2.0 * i)
        market_v = 1_500_000 + i * 500
        stock_v = 350_000 + i * 300
        for code, close, vol in (
            ("1001", market_close, market_v),
            ("1111", stock_close, stock_v),
        ):
            o = close - 0.4
            h = close + 1.2
            l = close - 1.1
            ma20 = close - 1.0
            ma60 = close - 2.5
            snapshot_rows.append(
                (
                    dt,
                    code,
                    close,
                    close - 0.5,
                    ma20,
                    ma60,
                    None,
                    0.01,
                    13,
                    5,
                )
            )
            bar_rows.append((code, dt, o, h, l, close, vol))
            ma_rows.append((code, dt, ma20, ma60))

    monthly_rows: list[tuple] = []
    for offset in range(18):
        year = 2023 + ((offset + 1) // 12)
        month = ((offset + 1) % 12) + 1
        month_ts = _month_start_ts(year, month)
        market_close = 900.0 + offset * 8.0
        stock_close = 80.0 + offset * 5.0
        for code, close in (("1001", market_close), ("1111", stock_close)):
            monthly_rows.append(
                (
                    code,
                    month_ts,
                    close - 2.0,
                    close + 3.0,
                    close - 3.0,
                    close,
                    1_000_000 + offset * 1_000,
                )
            )

    conn.executemany(
        """
        INSERT INTO feature_snapshot_daily (
            dt, code, close, ma7, ma20, ma60, atr14, diff20_pct, cnt_20_above, cnt_7_above
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        snapshot_rows,
    )
    conn.executemany(
        "INSERT INTO daily_bars (code, date, o, h, l, c, v) VALUES (?, ?, ?, ?, ?, ?, ?)",
        bar_rows,
    )
    conn.executemany(
        "INSERT INTO daily_ma (code, date, ma20, ma60) VALUES (?, ?, ?, ?)",
        ma_rows,
    )
    conn.executemany(
        "INSERT INTO monthly_bars (code, month, o, h, l, c, v) VALUES (?, ?, ?, ?, ?, ?, ?)",
        monthly_rows,
    )


def test_refresh_ml_feature_table_expands_columns_without_future_leakage() -> None:
    with duckdb.connect(":memory:") as conn:
        _setup_minimum_source_tables(conn)
        _seed_source_data(conn)

        rows = refresh_ml_feature_table(conn, feature_version=FEATURE_VERSION)
        assert rows == 180

        target_idx = 40
        target_dt = int((datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=target_idx)).timestamp())
        actual = conn.execute(
            """
            SELECT close_ret2, market_ret5, rel_ret5, atr14_pct, vol_ratio5_20
            FROM ml_feature_daily
            WHERE code = '1111' AND dt = ?
            """,
            [target_dt],
        ).fetchone()
        assert actual is not None
        close_ret2, market_ret5, rel_ret5, atr14_pct, vol_ratio5_20 = actual

        stock_close_now = 100.0 + (2.0 * target_idx)
        stock_close_prev2 = 100.0 + (2.0 * (target_idx - 2))
        stock_close_prev5 = 100.0 + (2.0 * (target_idx - 5))
        market_close_now = 1000.0 + target_idx
        market_close_prev5 = 1000.0 + (target_idx - 5)
        expected_close_ret2 = (stock_close_now - stock_close_prev2) / stock_close_prev2
        expected_market_ret5 = (market_close_now - market_close_prev5) / market_close_prev5
        expected_stock_ret5 = (stock_close_now - stock_close_prev5) / stock_close_prev5
        expected_rel_ret5 = expected_stock_ret5 - expected_market_ret5

        assert close_ret2 == pytest.approx(expected_close_ret2, rel=1e-9, abs=1e-9)
        assert market_ret5 == pytest.approx(expected_market_ret5, rel=1e-9, abs=1e-9)
        assert rel_ret5 == pytest.approx(expected_rel_ret5, rel=1e-9, abs=1e-9)
        assert atr14_pct is not None
        assert vol_ratio5_20 is not None

        null_count, total_count = conn.execute(
            """
            SELECT
                SUM(CASE WHEN atr14_pct IS NULL THEN 1 ELSE 0 END) AS null_count,
                COUNT(*) AS total_count
            FROM ml_feature_daily
            WHERE code = '1111'
            """
        ).fetchone()
        assert int(total_count) > 0
        assert int(null_count) < int(total_count)

        cols = {
            str(row[1]).lower()
            for row in conn.execute("PRAGMA table_info('ml_feature_daily')").fetchall()
        }
        for expected in (
            "atr14_pct",
            "close_ret2",
            "market_ret20",
            "rel_sector_ret20",
            "sector_breadth_ma20",
        ):
            assert expected in cols
