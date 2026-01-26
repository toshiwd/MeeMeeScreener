import os
import tempfile


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

