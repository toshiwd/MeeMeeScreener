from __future__ import annotations

from pathlib import Path

import duckdb

from app.backend.services.dev_db_sync import (
    development_stock_db_path,
    production_stock_db_path,
    sync_confirmed_production_db_to_dev,
    sync_updated_stock_db_to_local_peer,
)


def _create_stock_db(path: Path, *, confirmed_date: int = 20260601, yahoo_date: int = 20260604) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code VARCHAR,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT,
                source VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO daily_bars VALUES
              ('2730', ?, 2300, 2310, 2290, 2305, 1000, 'pan'),
              ('2730', ?, 2400, 2410, 2390, 2405, 900, 'yahoo')
            """,
            [confirmed_date, yahoo_date],
        )
        conn.execute("CREATE TABLE market_regime_daily (dt INTEGER, regime VARCHAR)")
        conn.execute("INSERT INTO market_regime_daily VALUES (20260601, 'risk_on')")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def test_sync_confirmed_production_db_to_dev_copies_production_and_removes_yahoo(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    prod = production_stock_db_path()
    dev = development_stock_db_path()
    _create_stock_db(prod)

    result = sync_confirmed_production_db_to_dev(source_db_path=prod)

    assert result["attempted"] is True
    assert result["synced"] is True
    assert result["confirmed_latest_date"] == 20260601
    assert result["removed_yahoo_rows"] == 1
    assert dev.exists()

    conn = duckdb.connect(str(dev), read_only=True)
    try:
        rows = conn.execute("SELECT code, date, source FROM daily_bars ORDER BY date").fetchall()
    finally:
        conn.close()

    assert rows == [("2730", 20260601, "pan")]


def test_sync_confirmed_production_db_to_dev_skips_non_production_source(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    other = tmp_path / "other" / "stocks.duckdb"
    _create_stock_db(other)

    result = sync_confirmed_production_db_to_dev(source_db_path=other)

    assert result["attempted"] is False
    assert result["synced"] is False
    assert result["skipped_reason"] == "source_is_not_production_db"
    assert not development_stock_db_path().exists()


def test_sync_updated_stock_db_to_local_peer_never_copies_dev_to_production(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    prod = production_stock_db_path()
    dev = development_stock_db_path()
    _create_stock_db(prod, confirmed_date=20260601, yahoo_date=20260604)
    _create_stock_db(dev, confirmed_date=20260707, yahoo_date=20260708)

    result = sync_updated_stock_db_to_local_peer(source_db_path=dev)

    assert result["attempted"] is False
    assert result["synced"] is False
    assert result["skipped_reason"] == "source_is_not_local_runtime_db"

    conn = duckdb.connect(str(prod), read_only=True)
    try:
        rows = conn.execute("SELECT code, date, source FROM daily_bars ORDER BY date").fetchall()
    finally:
        conn.close()

    assert rows == [
        ("2730", 20260601, "pan"),
        ("2730", 20260604, "yahoo"),
    ]


def test_sync_updated_stock_db_to_local_peer_copies_production_to_dev(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    prod = production_stock_db_path()
    dev = development_stock_db_path()
    _create_stock_db(prod, confirmed_date=20260709, yahoo_date=20260710)
    _create_stock_db(dev, confirmed_date=20260708, yahoo_date=20260709)

    result = sync_updated_stock_db_to_local_peer(source_db_path=prod)

    assert result["attempted"] is True
    assert result["synced"] is True
    assert result["confirmed_latest_date"] == 20260709
    assert result["target_confirmed_latest_date_before"] == 20260708


def test_sync_confirmed_production_db_to_dev_skips_current_dev(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    prod = production_stock_db_path()
    dev = development_stock_db_path()
    _create_stock_db(prod, confirmed_date=20260709, yahoo_date=20260710)
    _create_stock_db(dev, confirmed_date=20260709, yahoo_date=20260710)

    result = sync_confirmed_production_db_to_dev(source_db_path=prod)

    assert result["attempted"] is False
    assert result["synced"] is False
    assert result["skipped_reason"] == "dev_not_older"


def test_sync_updated_stock_db_to_local_peer_rejects_dev_source_even_when_production_is_newer(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    prod = production_stock_db_path()
    dev = development_stock_db_path()
    _create_stock_db(prod, confirmed_date=20260708, yahoo_date=20260709)
    _create_stock_db(dev, confirmed_date=20260707, yahoo_date=20260708)

    result = sync_updated_stock_db_to_local_peer(source_db_path=dev)

    assert result["attempted"] is False
    assert result["synced"] is False
    assert result["skipped_reason"] == "source_is_not_local_runtime_db"
    assert result["confirmed_latest_date"] is None
    assert result["target_confirmed_latest_date_before"] is None


def test_sync_updated_stock_db_to_local_peer_sanitizes_current_target(tmp_path, monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    prod = production_stock_db_path()
    dev = development_stock_db_path()
    _create_stock_db(prod, confirmed_date=20260709, yahoo_date=20260710)
    _create_stock_db(dev, confirmed_date=20260709, yahoo_date=20260710)

    result = sync_updated_stock_db_to_local_peer(source_db_path=prod)

    assert result["attempted"] is True
    assert result["synced"] is True
    assert result["skipped_reason"] == "target_not_older"
    assert result["removed_yahoo_rows"] == 1

    conn = duckdb.connect(str(dev), read_only=True)
    try:
        rows = conn.execute("SELECT code, date, source FROM daily_bars ORDER BY date").fetchall()
    finally:
        conn.close()

    assert rows == [("2730", 20260709, "pan")]
