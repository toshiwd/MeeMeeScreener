import os
import tempfile

import duckdb

from app.db.schema import ensure_schema


def test_duckdb_schema_includes_trade_tables():
    tmp_dir = tempfile.mkdtemp(prefix="meemee_schema_")
    db_path = os.path.join(tmp_dir, "stocks.duckdb")

    os.environ["MEEMEE_DATA_DIR"] = tmp_dir
    os.environ["STOCKS_DB_PATH"] = db_path

    from app.db.session import get_conn

    with get_conn() as conn:
        tables = {row[0] for row in conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()}

    assert "trade_events" in tables
    assert "positions_live" in tables
    assert "position_rounds" in tables
    assert "initial_positions_seed" in tables


def test_trade_events_schema_migrates_source_row_hash_unique():
    tmp_dir = tempfile.mkdtemp(prefix="meemee_trade_schema_")
    db_path = os.path.join(tmp_dir, "stocks.duckdb")

    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE trade_events (
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
        ('rakuten', TIMESTAMP '2026-03-22 09:00:00', '1301', 'SPOT_BUY', 100, 1000, 'hash-1', TIMESTAMP '2026-03-22 09:00:00', 'OPEN_LONG', 'buy', 'cash'),
        ('rakuten', TIMESTAMP '2026-03-22 09:01:00', '1301', 'SPOT_BUY', 100, 1000, 'hash-1', TIMESTAMP '2026-03-22 09:01:00', 'OPEN_LONG', 'buy', 'cash')
        """
    )

    ensure_schema(conn)

    constraint_rows = conn.execute(
        """
        SELECT constraint_type, constraint_column_names
        FROM duckdb_constraints()
        WHERE table_name = 'trade_events'
        """
    ).fetchall()
    row_count = conn.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0]

    conn.execute(
        """
        INSERT INTO trade_events (
            broker,
            exec_dt,
            symbol,
            action,
            qty,
            price,
            source_row_hash,
            created_at,
            transaction_type,
            side_type,
            margin_type
        ) VALUES (
            'rakuten',
            TIMESTAMP '2026-03-22 09:00:00',
            '1301',
            'SPOT_BUY',
            100,
            1000,
            'hash-1',
            TIMESTAMP '2026-03-22 09:00:00',
            'OPEN_LONG',
            'buy',
            'cash'
        )
        ON CONFLICT(source_row_hash) DO NOTHING
        """
    )
    row_count_after = conn.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0]

    conn.close()

    assert any(
        row[0] in {"UNIQUE", "PRIMARY KEY"} and list(row[1] or []) == ["source_row_hash"]
        for row in constraint_rows
    )
    assert row_count == 1
    assert row_count_after == 1
