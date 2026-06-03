from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime

import duckdb
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.backend import import_positions
from app.backend.api.routers import trades
from app.backend.positions import TradeEvent


def _create_trade_events_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE trade_events (
            broker TEXT,
            exec_dt TIMESTAMP,
            symbol TEXT,
            action TEXT,
            qty DOUBLE,
            price DOUBLE,
            source_row_hash TEXT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT now(),
            transaction_type TEXT,
            side_type TEXT,
            margin_type TEXT
        )
        """
    )


def _seed_event(conn, broker: str, source_row_hash: str) -> None:
    conn.execute(
        """
        INSERT INTO trade_events (
            broker, exec_dt, symbol, action, qty, price, source_row_hash,
            transaction_type, side_type, margin_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [broker, datetime(2026, 5, 1), "7203", "SPOT_BUY", 100, 3000, source_row_hash, None, None, None],
    )


def _patch_import_db(monkeypatch, conn):
    @contextmanager
    def _try_get_conn(timeout_sec=2.5):
        yield conn

    monkeypatch.setattr(import_positions, "try_get_conn", _try_get_conn)
    monkeypatch.setattr(import_positions, "operator_mutation_scope", None)
    monkeypatch.setattr(import_positions, "rebuild_positions", lambda _conn: {"ok": True})


def test_process_import_does_not_delete_existing_events_when_parse_returns_empty(monkeypatch):
    conn = duckdb.connect(":memory:")
    _create_trade_events_table(conn)
    _seed_event(conn, "rakuten", "old-rakuten")
    _seed_event(conn, "sbi", "old-sbi")
    _patch_import_db(monkeypatch, conn)

    result = import_positions._process_import("rakuten", [], ["rakuten_header_missing"], True)

    assert result["ok"] is False
    assert result["received"] == 0
    assert result["inserted"] == 0
    rows = conn.execute("SELECT broker, source_row_hash FROM trade_events ORDER BY source_row_hash").fetchall()
    assert rows == [("rakuten", "old-rakuten"), ("sbi", "old-sbi")]


def test_process_import_replace_existing_is_scoped_to_broker(monkeypatch):
    conn = duckdb.connect(":memory:")
    _create_trade_events_table(conn)
    _seed_event(conn, "rakuten", "old-rakuten")
    _seed_event(conn, "sbi", "old-sbi")
    _patch_import_db(monkeypatch, conn)

    new_event = TradeEvent(
        broker="sbi",
        exec_dt=datetime(2026, 5, 2),
        symbol="7203",
        action="SPOT_BUY",
        qty=100,
        price=3100,
        source_row_hash="new-sbi",
    )

    result = import_positions._process_import("sbi", [new_event], [], True)

    assert result["ok"] is True
    assert result["received"] == 1
    assert result["inserted"] == 1
    rows = conn.execute("SELECT broker, source_row_hash FROM trade_events ORDER BY source_row_hash").fetchall()
    assert rows == [("sbi", "new-sbi"), ("rakuten", "old-rakuten")]


def test_process_import_db_busy_does_not_delete_existing_events(monkeypatch):
    conn = duckdb.connect(":memory:")
    _create_trade_events_table(conn)
    _seed_event(conn, "rakuten", "old-rakuten")

    @contextmanager
    def _busy_conn(timeout_sec=2.5):
        yield None

    monkeypatch.setattr(import_positions, "try_get_conn", _busy_conn)
    monkeypatch.setattr(import_positions, "operator_mutation_scope", None)

    new_event = TradeEvent(
        broker="rakuten",
        exec_dt=datetime(2026, 5, 2),
        symbol="7203",
        action="SPOT_BUY",
        qty=100,
        price=3100,
        source_row_hash="new-rakuten",
    )

    try:
        import_positions._process_import("rakuten", [new_event], [], True)
    except import_positions.TradeImportBusyError as exc:
        assert "database" in str(exc)
    else:
        raise AssertionError("TradeImportBusyError was not raised")

    rows = conn.execute("SELECT broker, source_row_hash FROM trade_events ORDER BY source_row_hash").fetchall()
    assert rows == [("rakuten", "old-rakuten")]


def test_process_import_operator_busy_does_not_delete_existing_events(monkeypatch):
    conn = duckdb.connect(":memory:")
    _create_trade_events_table(conn)
    _seed_event(conn, "rakuten", "old-rakuten")

    class _BusyScope:
        def __enter__(self):
            raise import_positions.OperatorMutationBusyError("trade_history_import")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(import_positions, "operator_mutation_scope", lambda action, timeout_sec=0.0: _BusyScope())
    monkeypatch.setattr(import_positions, "try_get_conn", lambda timeout_sec=2.5: None)

    new_event = TradeEvent(
        broker="rakuten",
        exec_dt=datetime(2026, 5, 2),
        symbol="7203",
        action="SPOT_BUY",
        qty=100,
        price=3100,
        source_row_hash="new-rakuten",
    )

    try:
        import_positions._process_import("rakuten", [new_event], [], True)
    except import_positions.TradeImportBusyError as exc:
        assert "already running" in str(exc)
    else:
        raise AssertionError("TradeImportBusyError was not raised")

    rows = conn.execute("SELECT broker, source_row_hash FROM trade_events ORDER BY source_row_hash").fetchall()
    assert rows == [("rakuten", "old-rakuten")]


def test_import_trade_history_returns_retryable_503_when_import_is_busy(monkeypatch):
    app = FastAPI()
    app.include_router(trades.router)

    monkeypatch.setattr(trades.TradeRepository, "detect_broker_from_bytes", lambda _data, _name: ("rakuten", "test"))
    monkeypatch.setattr(trades.TradeRepository, "get_canonical_path", lambda _broker: "ignored.csv")
    monkeypatch.setattr(trades.TradeRepository, "save_raw_content", lambda _path, _content: None)

    def _busy_import(_content, replace_existing=True):
        raise import_positions.TradeImportBusyError("another import or update is already running")

    monkeypatch.setattr(trades.trade_ingest, "process_import_rakuten", _busy_import)

    client = TestClient(app)
    response = client.post(
        "/api/imports/trade-history",
        files={"file": ("rakuten.csv", b"dummy", "text/csv")},
        data={"broker": "auto"},
    )

    assert response.status_code == 503
    payload = response.json()
    assert payload["error"] == "trade_import_busy"
    assert payload["retryable"] is True
    assert response.headers["retry-after"] == "1"
