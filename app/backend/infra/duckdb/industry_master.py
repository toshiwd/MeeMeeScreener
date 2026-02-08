from __future__ import annotations

import logging

import duckdb

logger = logging.getLogger(__name__)


def ensure_industry_master(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Ensure industry_master table exists.
    If it doesn't exist or is empty, populate it from tickers
    with a default sector.
    """
    tables = {row[0] for row in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    table_exists = "industry_master" in tables

    if table_exists:
        try:
            count = conn.execute("SELECT COUNT(*) FROM industry_master").fetchone()
            if count and count[0] > 0:
                return 0
        except Exception as exc:
            # Table might be corrupted or schema mismatch; treat as empty and rebuild.
            logger.warning("industry_master check failed; rebuilding table: %s", exc)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS industry_master (
            code VARCHAR PRIMARY KEY,
            name VARCHAR,
            sector33_code VARCHAR,
            sector33_name VARCHAR,
            market_code VARCHAR
        )
        """
    )

    if "tickers" not in tables:
        return 0

    tickers_count = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()
    if not tickers_count or tickers_count[0] == 0:
        return 0

    conn.execute("DELETE FROM industry_master")
    conn.execute(
        """
        INSERT INTO industry_master (code, name, sector33_code, sector33_name, market_code)
        SELECT
            code,
            name,
            '00' as sector33_code,
            'UNCLASSIFIED' as sector33_name,
            '' as market_code
        FROM tickers
        """
    )

    count = conn.execute("SELECT COUNT(*) FROM industry_master").fetchone()
    return count[0] if count else 0
