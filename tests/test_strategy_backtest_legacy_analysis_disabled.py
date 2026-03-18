from __future__ import annotations

import duckdb
import pandas as pd

from app.backend.services.analysis import strategy_backtest_service


def test_load_market_frame_ignores_ml_pred_table_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT)")
        conn.execute("CREATE TABLE daily_ma (code TEXT, date INTEGER, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE)")
        conn.execute("CREATE TABLE ml_pred_20d (code TEXT, dt INTEGER, p_up DOUBLE)")
        conn.execute("INSERT INTO daily_bars VALUES ('1301', 20260313, 100, 101, 99, 100, 1000)")
        conn.execute("INSERT INTO daily_ma VALUES ('1301', 20260313, 98, 97, 96)")
        conn.execute("INSERT INTO ml_pred_20d VALUES ('1301', 20260313, 0.9)")

        frame = strategy_backtest_service._load_market_frame(  # type: ignore[attr-defined]
            conn,
            start_dt=None,
            end_dt=None,
            max_codes=None,
        )

        assert len(frame) == 1
        assert frame.iloc[0]["code"] == "1301"
        assert pd.isna(frame.iloc[0]["ml_p_up"])
    finally:
        conn.close()
