from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime

import duckdb

from app.backend import import_positions
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
    def _get_conn():
        yield conn

    monkeypatch.setattr(import_positions, "get_conn", _get_conn)
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
