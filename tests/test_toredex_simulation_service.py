from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.backend.services import toredex_simulation_service


def _seed_toredex_tables(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE toredex_seasons (
                season_id VARCHAR,
                mode VARCHAR,
                start_date DATE,
                end_date DATE,
                initial_cash BIGINT,
                policy_version VARCHAR,
                config_json VARCHAR,
                config_hash VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE toredex_daily_metrics (
                season_id VARCHAR,
                "asOf" DATE,
                net_cum_return_pct DOUBLE,
                max_drawdown_pct DOUBLE,
                risk_gate_pass BOOLEAN
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE toredex_trades (
                season_id VARCHAR,
                trade_id VARCHAR
            )
            """
        )

        seasons = [
            "validate_alpha",
            "validate_beta",
            "validate_gamma",
            "validate_lowdays",
            "validate_fail",
            "season_other",
        ]
        for season_id in seasons:
            conn.execute(
                """
                INSERT INTO toredex_seasons (
                    season_id, mode, start_date, end_date, initial_cash, policy_version, config_json, config_hash, created_at
                ) VALUES (?, 'BACKTEST', DATE '2024-01-01', NULL, 10000000, 'toredex.v8', '{}', 'x', CURRENT_TIMESTAMP)
                """,
                [season_id],
            )

        def insert_metrics(
            *,
            season_id: str,
            days: int,
            latest_return_pct: float,
            latest_dd_pct: float,
            risk_gate_pass: bool,
            base_return_pct: float = 0.0,
        ) -> None:
            if days <= 0:
                return
            if days > 1:
                conn.execute(
                    """
                    INSERT INTO toredex_daily_metrics (season_id, "asOf", net_cum_return_pct, max_drawdown_pct, risk_gate_pass)
                    SELECT ?, DATE '2024-01-01' + CAST(i AS INTEGER), ?, -1.0, ?
                    FROM range(?) AS t(i)
                    """,
                    [season_id, base_return_pct, risk_gate_pass, days - 1],
                )
            conn.execute(
                """
                INSERT INTO toredex_daily_metrics (season_id, "asOf", net_cum_return_pct, max_drawdown_pct, risk_gate_pass)
                VALUES (?, DATE '2024-01-01' + ?, ?, ?, ?)
                """,
                [season_id, days - 1, latest_return_pct, latest_dd_pct, risk_gate_pass],
            )

        insert_metrics(
            season_id="validate_alpha",
            days=201,
            latest_return_pct=15.0,
            latest_dd_pct=-3.0,
            risk_gate_pass=True,
            base_return_pct=40.0,
        )
        insert_metrics(
            season_id="validate_beta",
            days=220,
            latest_return_pct=-5.0,
            latest_dd_pct=-8.0,
            risk_gate_pass=True,
        )
        insert_metrics(
            season_id="validate_gamma",
            days=205,
            latest_return_pct=30.0,
            latest_dd_pct=-2.0,
            risk_gate_pass=True,
        )
        insert_metrics(
            season_id="validate_lowdays",
            days=150,
            latest_return_pct=99.0,
            latest_dd_pct=-1.0,
            risk_gate_pass=True,
        )
        insert_metrics(
            season_id="validate_fail",
            days=210,
            latest_return_pct=50.0,
            latest_dd_pct=-4.0,
            risk_gate_pass=False,
        )
        insert_metrics(
            season_id="season_other",
            days=230,
            latest_return_pct=88.0,
            latest_dd_pct=-5.0,
            risk_gate_pass=True,
        )

        conn.execute(
            """
            INSERT INTO toredex_trades (season_id, trade_id) VALUES
            ('validate_alpha', 'a1'),
            ('validate_alpha', 'a2'),
            ('validate_alpha', 'a3'),
            ('validate_alpha', 'a4'),
            ('validate_beta', 'b1'),
            ('validate_beta', 'b2')
            """
        )
    finally:
        conn.close()


def test_get_validate_simulation_filters_and_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "toredex_sim.duckdb"
    _seed_toredex_tables(db_path)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    result = toredex_simulation_service.get_validate_simulation(principal_jpy=10_000_000, limit=200)

    items = result["items"]
    assert [item["season_id"] for item in items] == ["validate_gamma", "validate_alpha", "validate_beta"]
    assert result["summary"]["count"] == 3

    assert items[0]["net_cum_return_pct"] == 30.0
    assert items[0]["final_jpy"] == 13_000_000
    assert items[0]["gain_jpy"] == 3_000_000
    assert items[0]["trades"] == 0

    assert items[1]["net_cum_return_pct"] == 15.0
    assert items[1]["final_jpy"] == 11_500_000
    assert items[1]["gain_jpy"] == 1_500_000
    assert items[1]["trades"] == 4

    assert items[2]["net_cum_return_pct"] == -5.0
    assert items[2]["final_jpy"] == 9_500_000
    assert items[2]["gain_jpy"] == -500_000
    assert items[2]["trades"] == 2

    summary = result["summary"]
    assert summary["best"]["season_id"] == "validate_gamma"
    assert summary["worst"]["season_id"] == "validate_beta"
    assert summary["median"]["net_cum_return_pct"] == 15.0
    assert summary["avg"]["net_cum_return_pct"] == pytest.approx(13.3333333333)
    assert summary["avg"]["final_jpy"] == 11_333_333


def test_get_validate_simulation_applies_limit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "toredex_sim_limit.duckdb"
    _seed_toredex_tables(db_path)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))

    result = toredex_simulation_service.get_validate_simulation(principal_jpy=10_000_000, limit=2)
    assert len(result["items"]) == 2
    assert [item["season_id"] for item in result["items"]] == ["validate_gamma", "validate_alpha"]
    assert result["summary"]["count"] == 3
