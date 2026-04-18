from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from shared.runtime_stock_db_contract import (
    inspect_runtime_stock_db,
    resolve_runtime_stock_db_path,
    resolve_runtime_stock_db_selection,
)


def _build_contract_db(path: Path, *, latest_ymd: int = 20260403) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code VARCHAR,
                date BIGINT,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v DOUBLE,
                source VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE market_regime_daily (
                dt INTEGER,
                regime_id VARCHAR,
                breadth_above_ma20 DOUBLE,
                breadth_above_ma60 DOUBLE,
                advancers_ratio DOUBLE,
                index_close_vs_ma20 DOUBLE,
                index_close_vs_ma60 DOUBLE,
                market_atr_pct DOUBLE,
                sector_dispersion DOUBLE,
                regime_score DOUBLE,
                label_version VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        conn.executemany(
            "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("2531", 20260319, 1.0, 1.1, 0.9, 1.05, 1000.0, "fixture"),
                ("2531", latest_ymd, 1.1, 1.2, 1.0, 1.15, 1100.0, "fixture"),
            ],
        )
        conn.executemany(
            "INSERT INTO market_regime_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (20260319, "up", 0.7, 0.6, 0.55, 1.02, 1.03, 0.8, 0.2, 0.65, "v1", datetime.now(timezone.utc)),
                (20260320, "down", 0.3, 0.4, 0.45, 0.98, 0.97, 1.1, 0.4, 0.35, "v1", datetime.now(timezone.utc)),
                (20260321, "flat", 0.5, 0.5, 0.50, 1.00, 1.00, 0.9, 0.3, 0.50, "v1", datetime.now(timezone.utc)),
            ],
        )
    finally:
        conn.close()
    return path


def test_runtime_stock_db_selection_prefers_meemee_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base_dir = tmp_path / "meemee-data"
    db_path = _build_contract_db(base_dir / "stocks.duckdb")
    monkeypatch.delenv("STOCKS_DB_PATH", raising=False)
    monkeypatch.delenv("TRADEX_LIVE_STOCKS_DB_PATH", raising=False)
    monkeypatch.setenv("MEEMEE_DATA_DIR", str(base_dir))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "localappdata"))
    resolve_runtime_stock_db_selection.cache_clear()

    resolved = resolve_runtime_stock_db_path()
    selection = resolve_runtime_stock_db_selection()

    assert resolved == db_path
    assert selection["runtime_db_path"] == str(db_path)
    assert selection["resolution_source"] == "MEEMEE_DATA_DIR"
    assert selection["validated"] is True


def test_runtime_stock_db_freshness_marks_lagged_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    base_dir = tmp_path / "meemee-data"
    db_path = _build_contract_db(base_dir / "stocks.duckdb", latest_ymd=20260403)
    monkeypatch.delenv("STOCKS_DB_PATH", raising=False)
    monkeypatch.delenv("TRADEX_LIVE_STOCKS_DB_PATH", raising=False)
    monkeypatch.setenv("MEEMEE_DATA_DIR", str(base_dir))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "localappdata"))
    resolve_runtime_stock_db_selection.cache_clear()

    freshness = inspect_runtime_stock_db(
        runtime_db_path=db_path,
        requested_symbol="2531",
        requested_chart_date=20260416,
    )

    assert freshness["runtime_db_path"] == str(db_path)
    assert freshness["latest_available_global_date"] == 20260403
    assert freshness["requested_symbol_latest_date"] == 20260403
    assert freshness["date_gap_days"] == 13
    assert freshness["date_match_status"] == "lagged_provisional"
    assert freshness["source_freshness_status"] == "lagged"
    assert freshness["freshness_blocked"] is True

