from __future__ import annotations

from datetime import date, timedelta

import duckdb
import pytest

from app.backend.core.legacy_analysis_control import LEGACY_ANALYSIS_DISABLE_ENV
from app.backend.services import tradex_research_environment_readiness as readiness_service


def _create_db(path, statements: list[str], rows: list[tuple] | None = None, insert_sql: str | None = None) -> None:
    with duckdb.connect(str(path)) as conn:
        for statement in statements:
            conn.execute(statement)
        if rows and insert_sql:
            conn.executemany(insert_sql, rows)


def _ready_rows() -> list[tuple[int, str, float, float, float, float, float, float, float, float, str, object]]:
    rows: list[tuple[int, str, float, float, float, float, float, float, float, float, str, object]] = []
    current = date(2025, 1, 1)
    for regime_id in ("risk_on_trend", "risk_off_trend", "neutral_range"):
        for _ in range(60):
            rows.append(
                (
                    int(current.strftime("%Y%m%d")),
                    regime_id,
                    0.6,
                    0.6,
                    0.6,
                    0.6,
                    0.6,
                    0.6,
                    0.6,
                    0.1,
                    "v1",
                    "2025-01-01T00:00:00+00:00",
                )
            )
            current += timedelta(days=1)
    return rows


def _base_env(monkeypatch: pytest.MonkeyPatch, db_path) -> None:
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setenv(LEGACY_ANALYSIS_DISABLE_ENV, "0")


def test_missing_required_table_is_classified(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    db_path = tmp_path / "missing_table.duckdb"
    _create_db(db_path, ["CREATE TABLE other_table(id INTEGER)"])
    _base_env(monkeypatch, db_path)

    report = readiness_service.evaluate_environment_readiness()

    assert report["ready"] is False
    assert report["cause_class"] == "required_table_missing"
    assert report["cause_source"] == "table_presence"
    assert report["readiness_summary"]["table_exists"] is False
    assert report["readiness_checks"][0]["check_id"] == "legacy_analysis_enabled"
    assert report["readiness_checks"][-1]["check_id"] == "required_table_exists"


def test_required_table_empty_is_classified(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    db_path = tmp_path / "empty_table.duckdb"
    _create_db(
        db_path,
        [
            """
            CREATE TABLE market_regime_daily (
                dt INTEGER,
                regime_id TEXT,
                breadth_above_ma20 DOUBLE,
                breadth_above_ma60 DOUBLE,
                advancers_ratio DOUBLE,
                index_close_vs_ma20 DOUBLE,
                index_close_vs_ma60 DOUBLE,
                market_atr_pct DOUBLE,
                sector_dispersion DOUBLE,
                regime_score DOUBLE,
                label_version TEXT,
                created_at TIMESTAMP
            )
            """,
        ],
    )
    _base_env(monkeypatch, db_path)

    report = readiness_service.evaluate_environment_readiness()

    assert report["ready"] is False
    assert report["cause_class"] == "required_table_empty"
    assert report["cause_source"] == "table_rows"
    assert report["readiness_summary"]["table_row_count"] == 0


def test_schema_mismatch_is_classified(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    db_path = tmp_path / "schema_mismatch.duckdb"
    _create_db(db_path, ["CREATE TABLE market_regime_daily (dt INTEGER, regime_id TEXT)"])
    _base_env(monkeypatch, db_path)

    report = readiness_service.evaluate_environment_readiness()

    assert report["ready"] is False
    assert report["cause_class"] == "schema_mismatch"
    assert report["cause_source"] == "table_schema"
    assert "breadth_above_ma20" in report["readiness_summary"]["missing_columns"]


def test_genuine_insufficient_windows_is_classified(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    db_path = tmp_path / "insufficient_windows.duckdb"
    _create_db(
        db_path,
        [
            """
            CREATE TABLE market_regime_daily (
                dt INTEGER,
                regime_id TEXT,
                breadth_above_ma20 DOUBLE,
                breadth_above_ma60 DOUBLE,
                advancers_ratio DOUBLE,
                index_close_vs_ma20 DOUBLE,
                index_close_vs_ma60 DOUBLE,
                market_atr_pct DOUBLE,
                sector_dispersion DOUBLE,
                regime_score DOUBLE,
                label_version TEXT,
                created_at TIMESTAMP
            )
            """,
        ],
        _ready_rows()[:60],
        """
        INSERT INTO market_regime_daily (
            dt,
            regime_id,
            breadth_above_ma20,
            breadth_above_ma60,
            advancers_ratio,
            index_close_vs_ma20,
            index_close_vs_ma60,
            market_atr_pct,
            sector_dispersion,
            regime_score,
            label_version,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
    )
    _base_env(monkeypatch, db_path)

    report = readiness_service.evaluate_environment_readiness()

    assert report["ready"] is False
    assert report["cause_class"] == "genuine_data_unavailable"
    assert report["cause_source"] == "evaluation_window_probe"
    assert report["readiness_summary"]["selected_window_count"] < report["readiness_summary"]["minimum_window_count"]


def test_ready_environment_passes(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    db_path = tmp_path / "ready.duckdb"
    _create_db(
        db_path,
        [
            """
            CREATE TABLE market_regime_daily (
                dt INTEGER,
                regime_id TEXT,
                breadth_above_ma20 DOUBLE,
                breadth_above_ma60 DOUBLE,
                advancers_ratio DOUBLE,
                index_close_vs_ma20 DOUBLE,
                index_close_vs_ma60 DOUBLE,
                market_atr_pct DOUBLE,
                sector_dispersion DOUBLE,
                regime_score DOUBLE,
                label_version TEXT,
                created_at TIMESTAMP
            )
            """,
        ],
        _ready_rows(),
        """
        INSERT INTO market_regime_daily (
            dt,
            regime_id,
            breadth_above_ma20,
            breadth_above_ma60,
            advancers_ratio,
            index_close_vs_ma20,
            index_close_vs_ma60,
            market_atr_pct,
            sector_dispersion,
            regime_score,
            label_version,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
    )
    _base_env(monkeypatch, db_path)

    report = readiness_service.evaluate_environment_readiness()

    assert report["ready"] is True
    assert report["cause_class"] == "ready"
    assert report["cause_source"] == "environment_ready"
    assert report["readiness_summary"]["selected_window_count"] >= report["readiness_summary"]["minimum_window_count"]
