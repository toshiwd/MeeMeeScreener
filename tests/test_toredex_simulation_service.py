from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.backend.services import toredex_simulation_service
from app.backend.services.toredex.toredex_repository import ToredexRepository


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


def test_ensure_season_handles_table_without_primary_key(tmp_path: Path) -> None:
    db_path = tmp_path / "toredex_repo_compat.duckdb"
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
        repo = ToredexRepository(conn=conn)
        repo.ensure_season(
            season_id="season_compat",
            mode="BACKTEST",
            start_date=date(2024, 1, 1),
            initial_cash=10_000_000,
            policy_version="toredex.v8",
            config_json="{}",
            config_hash="hash123",
        )
        row = conn.execute(
            "SELECT season_id, mode, initial_cash, policy_version, config_hash FROM toredex_seasons WHERE season_id = ?",
            ["season_compat"],
        ).fetchone()
        assert row == ("season_compat", "BACKTEST", 10000000, "toredex.v8", "hash123")
    finally:
        conn.close()


def test_save_trades_handles_table_without_primary_key(tmp_path: Path) -> None:
    db_path = tmp_path / "toredex_trades_compat.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE toredex_trades (
                season_id VARCHAR,
                "asOf" DATE,
                trade_id VARCHAR,
                ticker VARCHAR,
                side VARCHAR,
                delta_units INTEGER,
                price DOUBLE,
                reason_id VARCHAR,
                fees_bps DOUBLE,
                slippage_bps DOUBLE,
                borrow_bps_annual DOUBLE,
                notional DOUBLE,
                fees_cost DOUBLE,
                slippage_cost DOUBLE,
                borrow_cost DOUBLE,
                created_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO toredex_trades (
                season_id, "asOf", trade_id, ticker, side, delta_units, price, reason_id,
                fees_bps, slippage_bps, borrow_bps_annual, notional, fees_cost, slippage_cost, borrow_cost, created_at
            ) VALUES ('season_compat', DATE '2024-01-01', 'dup_trade', '1301', 'long', 2, 100.0, 'R1',
                      0.0, 0.0, 0.0, 200.0, 0.0, 0.0, 0.0, CURRENT_TIMESTAMP)
            """
        )
        repo = ToredexRepository(conn=conn)
        repo.save_trades(
            [
                {
                    "season_id": "season_compat",
                    "asOf": date(2024, 1, 1),
                    "trade_id": "dup_trade",
                    "ticker": "1301",
                    "side": "long",
                    "delta_units": 2,
                    "price": 100.0,
                    "reason_id": "R1",
                    "fees_bps": 0.0,
                    "slippage_bps": 0.0,
                    "borrow_bps_annual": 0.0,
                    "notional": 200.0,
                    "fees_cost": 0.0,
                    "slippage_cost": 0.0,
                    "borrow_cost": 0.0,
                },
                {
                    "season_id": "season_compat",
                    "asOf": date(2024, 1, 2),
                    "trade_id": "new_trade",
                    "ticker": "1301",
                    "side": "long",
                    "delta_units": 3,
                    "price": 101.0,
                    "reason_id": "R2",
                    "fees_bps": 0.0,
                    "slippage_bps": 0.0,
                    "borrow_bps_annual": 0.0,
                    "notional": 303.0,
                    "fees_cost": 0.0,
                    "slippage_cost": 0.0,
                    "borrow_cost": 0.0,
                },
            ]
        )
        rows = conn.execute(
            "SELECT trade_id, COUNT(*) FROM toredex_trades WHERE season_id = ? GROUP BY trade_id ORDER BY trade_id",
            ["season_compat"],
        ).fetchall()
        assert rows == [("dup_trade", 1), ("new_trade", 1)]
    finally:
        conn.close()
