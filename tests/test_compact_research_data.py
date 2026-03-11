from __future__ import annotations

from pathlib import Path

import duckdb

from app.backend.services import strategy_backtest_service
from app.backend.tools.compact_research_data import compact_research_database


def test_load_market_frame_without_daily_ma() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE daily_bars (
            date INTEGER,
            code TEXT,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO daily_bars VALUES
            (20260301, '2413', 100, 110, 90, 105, 1000),
            (20260302, '2413', 106, 112, 101, 111, 1200)
        """
    )

    frame = strategy_backtest_service._load_market_frame(  # type: ignore[attr-defined]
        conn,
        start_dt=None,
        end_dt=None,
        max_codes=None,
    )

    assert list(frame["code"]) == ["2413", "2413"]
    assert frame["ma7"].isna().all()
    assert frame["ma20"].isna().all()
    assert frame["ma60"].isna().all()


def test_compact_research_database_copies_only_required_tables(tmp_path: Path) -> None:
    source_path = tmp_path / "source.duckdb"
    output_path = tmp_path / "compact.duckdb"

    with duckdb.connect(str(source_path)) as conn:
        conn.execute("CREATE TABLE daily_bars (date INTEGER, code TEXT, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE)")
        conn.execute("INSERT INTO daily_bars VALUES (20260301, '2413', 100, 110, 90, 105, 1000)")
        conn.execute("CREATE TABLE ml_pred_20d (dt INTEGER, code TEXT, p_up DOUBLE)")
        conn.execute("INSERT INTO ml_pred_20d VALUES (20260301, '2413', 0.6)")
        conn.execute("CREATE TABLE industry_master (code TEXT, sector33_code TEXT)")
        conn.execute("INSERT INTO industry_master VALUES ('2413', '3250')")
        conn.execute("CREATE TABLE earnings_planned (code TEXT, planned_date TEXT)")
        conn.execute("INSERT INTO earnings_planned VALUES ('2413', '2026-03-31')")
        conn.execute("CREATE TABLE ex_rights (code TEXT, ex_date TEXT)")
        conn.execute("INSERT INTO ex_rights VALUES ('2413', '2026-03-15')")
        conn.execute("CREATE TABLE strategy_walkforward_runs (run_id TEXT, report_json TEXT)")
        conn.execute("INSERT INTO strategy_walkforward_runs VALUES ('swf_x', '{}')")
        conn.execute("CREATE TABLE feature_snapshot_daily (dt INTEGER, code TEXT)")
        conn.execute("INSERT INTO feature_snapshot_daily VALUES (20260301, '2413')")
        conn.execute("CREATE TABLE daily_ma (date INTEGER, code TEXT, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE)")
        conn.execute("INSERT INTO daily_ma VALUES (20260301, '2413', 101, 102, 103)")

    result = compact_research_database(source_path, output_path, overwrite=True)

    assert result["output_size_bytes"] > 0
    with duckdb.connect(str(output_path), read_only=True) as conn:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}

    assert "daily_bars" in tables
    assert "ml_pred_20d" in tables
    assert "industry_master" in tables
    assert "earnings_planned" in tables
    assert "ex_rights" in tables
    assert "strategy_walkforward_runs" in tables
    assert "feature_snapshot_daily" not in tables
    assert "daily_ma" not in tables
