from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import duckdb

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.infra.duckdb.stock_repo import StockRepository


def _ymd(value: datetime) -> int:
    return int(value.strftime("%Y%m%d"))


def _seed_readonly_analysis_db(db_path: str) -> None:
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    with duckdb.connect(db_path, read_only=False) as conn:
        conn.execute(
            """
            CREATE TABLE phase_pred_daily (
                code VARCHAR,
                dt INTEGER,
                early_score DOUBLE,
                late_score DOUBLE,
                body_score DOUBLE,
                n INTEGER,
                reasons_top3 VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE ml_pred_20d (
                code VARCHAR,
                dt INTEGER,
                p_up DOUBLE,
                p_down DOUBLE,
                p_up_5 DOUBLE,
                p_up_10 DOUBLE,
                p_turn_up DOUBLE,
                p_turn_down DOUBLE,
                p_turn_down_5 DOUBLE,
                p_turn_down_10 DOUBLE,
                p_turn_down_20 DOUBLE,
                ret_pred20 DOUBLE,
                ev20 DOUBLE,
                ev20_net DOUBLE,
                ev5_net DOUBLE,
                ev10_net DOUBLE,
                model_version VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code VARCHAR,
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
            CREATE TABLE sell_analysis_daily (
                code VARCHAR,
                dt INTEGER,
                close DOUBLE,
                day_change_pct DOUBLE,
                p_down DOUBLE,
                p_turn_down DOUBLE,
                ev20_net DOUBLE,
                rank_down_20 DOUBLE,
                pred_dt INTEGER,
                p_up_5 DOUBLE,
                p_up_10 DOUBLE,
                p_up_20 DOUBLE,
                short_score DOUBLE,
                a_score DOUBLE,
                b_score DOUBLE,
                ma20 DOUBLE,
                ma60 DOUBLE,
                ma20_slope DOUBLE,
                ma60_slope DOUBLE,
                dist_ma20_signed DOUBLE,
                dist_ma60_signed DOUBLE,
                trend_down BOOLEAN,
                trend_down_strict BOOLEAN,
                fwd_close_5 DOUBLE,
                fwd_close_10 DOUBLE,
                fwd_close_20 DOUBLE,
                short_ret_5 DOUBLE,
                short_ret_10 DOUBLE,
                short_ret_20 DOUBLE,
                short_win_5 BOOLEAN,
                short_win_10 BOOLEAN,
                short_win_20 BOOLEAN
            )
            """
        )

        conn.execute(
            """
            INSERT INTO phase_pred_daily VALUES
            ('1301', 20260302, 0.11, 0.22, 0.33, 7, '["phase-a"]'),
            ('1301', 20260303, 0.12, 0.23, 0.34, 8, '["phase-b"]')
            """
        )
        conn.execute(
            """
            INSERT INTO ml_pred_20d VALUES
            ('1301', 20260301, 0.72, 0.28, 0.70, 0.69, 0.61, 0.39, 0.38, 0.37, 0.36, 0.021, 0.019, 0.017, 0.013, 0.011, 'v1'),
            ('1301', 20260302, 0.76, 0.24, 0.74, 0.73, 0.64, 0.36, 0.35, 0.34, 0.33, 0.024, 0.021, 0.019, 0.015, 0.012, 'v2')
            """
        )
        daily_rows = []
        sell_rows = []
        for offset in range(21):
            day = start + timedelta(days=offset)
            ymd = _ymd(day)
            close = 100.0 + float(offset)
            daily_rows.append(("1301", ymd, close - 0.6, close + 0.8, close - 1.2, close, 100_000 + offset))
            sell_rows.append(
                (
                    "1301",
                    ymd,
                    close,
                    0.01 * offset,
                    0.42,
                    0.38,
                    0.018 + (0.001 * offset),
                    12.0 + offset,
                    ymd,
                    0.66,
                    0.63,
                    0.60,
                    0.28,
                    0.31,
                    0.29,
                    98.0,
                    95.0,
                    0.12,
                    0.08,
                    0.03,
                    0.04,
                    offset % 2 == 0,
                    offset % 3 == 0,
                    close + 5.0,
                    close + 10.0,
                    close + 15.0,
                    0.02 + (0.001 * offset),
                    0.03 + (0.001 * offset),
                    0.04 + (0.001 * offset),
                    offset % 2 == 0,
                    offset % 3 == 0,
                    offset % 4 == 0,
                )
            )
        conn.executemany(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?)",
            daily_rows,
        )
        conn.executemany(
            """
            INSERT INTO sell_analysis_daily VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            sell_rows,
        )


def test_stock_repo_legacy_analysis_reads_work_when_disabled(monkeypatch, tmp_path):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    db_path = tmp_path / "stock.duckdb"
    _seed_readonly_analysis_db(str(db_path))

    repo = StockRepository(str(db_path))

    phase_row = repo.get_phase_pred("1301", None)
    ml_row = repo.get_ml_analysis_pred("1301", None)
    timeline_rows = repo.get_analysis_timeline("1301", None)
    buy_stage_precision = repo.get_buy_stage_precision("1301", None)
    sell_snapshot = repo.get_sell_analysis_snapshot("1301", None)
    latest_ml_map = repo.get_latest_ml_pred_map(["1301"])

    assert phase_row is not None
    assert phase_row[0] == 20260303
    assert ml_row is not None
    assert ml_row[0] == 20260302
    assert len(timeline_rows) >= 3
    assert [item["dt"] for item in timeline_rows[:3]] == [20260301, 20260302, 20260303]
    assert buy_stage_precision is not None
    assert buy_stage_precision["strategy"]["samples"] > 0
    assert buy_stage_precision["strategy"]["precision"] is not None
    assert sell_snapshot is not None
    assert sell_snapshot[0] == 20260321
    assert "1301" in latest_ml_map
    assert latest_ml_map["1301"]["model_version"] == "v2"
