from __future__ import annotations

from contextlib import contextmanager

import duckdb

from app.backend.core import csv_sync


def test_sync_trade_csvs_uses_temp_backup_table(monkeypatch, tmp_path):
    csv_path = tmp_path / "rakuten_trade_history.csv"
    csv_path.write_bytes(b"dummy")

    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE trade_events (
            id BIGINT,
            broker TEXT,
            exec_dt TIMESTAMP,
            symbol TEXT,
            action TEXT,
            qty DOUBLE,
            price DOUBLE,
            source_row_hash TEXT,
            created_at TIMESTAMP,
            transaction_type TEXT,
            side_type TEXT,
            margin_type TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO trade_events VALUES
        (1, 'rakuten', NOW(), '1301', 'BUY', 100, 1000, 'seed-hash', NOW(), 'spot', 'buy', 'cash')
        """
    )

    @contextmanager
    def _get_conn():
        yield conn

    def _process_import_rakuten(content: bytes, replace_existing: bool = False):
        conn.execute(
            """
            INSERT INTO trade_events VALUES
            (2, 'rakuten', NOW(), '1301', 'SELL', 100, 1010, 'import-hash', NOW(), 'spot', 'sell', 'cash')
            """
        )
        return {"inserted": 1, "received": 1}

    monkeypatch.setattr(csv_sync, "get_conn", _get_conn)
    monkeypatch.setattr(csv_sync, "resolve_trade_csv_paths", lambda: [str(csv_path)])
    monkeypatch.setattr(csv_sync, "process_import_rakuten", _process_import_rakuten)
    monkeypatch.setattr(csv_sync, "process_import_sbi", lambda content, replace_existing=False: {"inserted": 0, "received": 0})
    monkeypatch.setattr(
        csv_sync.TradeRepository,
        "detect_broker_from_bytes",
        staticmethod(lambda content, basename: ("rakuten", "filename")),
    )

    result = csv_sync.sync_trade_csvs()

    table_names = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    row_count = conn.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0]

    assert result["imported"] == 1
    assert row_count == 1
    assert "trade_events_backup_sync" not in table_names

    conn.close()
