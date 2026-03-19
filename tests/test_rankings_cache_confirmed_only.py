from __future__ import annotations

import duckdb

from app.backend.services.ml import rankings_cache


def test_rankings_cache_daily_rows_exclude_yahoo_source(tmp_path) -> None:
    db_path = tmp_path / "screener.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date BIGINT,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v DOUBLE,
                source TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO daily_bars VALUES
                ('7203', 20260318, 100.0, 105.0, 99.0, 104.0, 1000.0, 'pan'),
                ('7203', 20260319, 101.0, 106.0, 100.0, 105.0, 1200.0, 'yahoo')
            """
        )

        rows = rankings_cache._fetch_daily_rows(conn)  # type: ignore[attr-defined]
        rows_asof = rankings_cache._fetch_daily_rows_asof(conn, 20260319)  # type: ignore[attr-defined]
    finally:
        conn.close()

    assert rows == [("7203", 20260318, 100.0, 105.0, 99.0, 104.0, 1000.0)]
    assert rows_asof == rows


def test_rankings_cache_analysis_provisional_gate_is_closed_by_default() -> None:
    assert rankings_cache._analysis_provisional_enabled() is False  # type: ignore[attr-defined]
    daily_map = {"7203": [(20260318, 100.0, 105.0, 99.0, 104.0, 1000.0)]}
    merged = rankings_cache._merge_analysis_provisional_rows(daily_map, ["7203"])  # type: ignore[attr-defined]
    assert merged is daily_map

