from __future__ import annotations

import duckdb
import os

from app.backend.services.ml import ml_service


def test_feature_input_repair_dates_detects_sparse_latest_day() -> None:
    previous = os.environ.get("MEEMEE_DISABLE_LEGACY_ANALYSIS")
    os.environ["MEEMEE_DISABLE_LEGACY_ANALYSIS"] = "0"
    conn = duckdb.connect(":memory:")
    try:
        ml_service._ensure_ml_schema(conn)
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT,
                source TEXT
            )
            """
        )
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
                diff20_atr DOUBLE,
                cnt_20_above INTEGER,
                cnt_7_above INTEGER,
                day_count INTEGER,
                candle_flags BIGINT
            )
            """
        )
        rows = []
        for idx in range(40):
            dt = 20260301 + idx
            rows.append(("1111", dt, 100.0, 101.0, 99.0, 100.0 + idx, 1000, "pan"))
            rows.append(("2222", dt, 200.0, 201.0, 199.0, 200.0 + idx, 1000, "pan"))
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        conn.execute(
            """
            INSERT INTO feature_snapshot_daily (
                dt, code, close, ma7, ma20, ma60, atr14, diff20_pct, diff20_atr,
                cnt_20_above, cnt_7_above, day_count, candle_flags
            )
            VALUES (20260340, '1111', 140.0, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            """
        )

        # Expected count is below the production threshold of 30, so no repair should trigger yet.
        assert ml_service._feature_input_repair_dates(conn, target_date_keys=[20260340]) == []

        more_rows = []
        for code_no in range(3, 35):
            code = f"{code_no:04d}"
            for idx in range(40):
                dt = 20260301 + idx
                more_rows.append((code, dt, 100.0, 101.0, 99.0, 100.0 + idx, 1000, "pan"))
        conn.executemany("INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)", more_rows)

        assert ml_service._feature_input_repair_dates(conn, target_date_keys=[20260340]) == [20260340]
    finally:
        conn.close()
        if previous is None:
            os.environ.pop("MEEMEE_DISABLE_LEGACY_ANALYSIS", None)
        else:
            os.environ["MEEMEE_DISABLE_LEGACY_ANALYSIS"] = previous
