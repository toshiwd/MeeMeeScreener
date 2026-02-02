from __future__ import annotations

import duckdb

def ensure_industry_master(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Ensure industry_master table exists.
    If it doesn't exist or is empty, populate it using tickers table with default 'UNCLASSIFIED' sector.
    """
    # Check if table exists
    tables = {row[0] for row in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    
    table_exists = "industry_master" in tables
    if table_exists:
        # If exists, check if it has data
        try:
            count = conn.execute("SELECT COUNT(*) FROM industry_master").fetchone()
            if count and count[0] > 0:
                return 0
        except Exception:
            # Table might be corrupted or schema mismatch; treat as empty
            pass

    # Create table if not exists
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
    
    # If we are here, either table is new or empty.
    # Try to populate from tickers.
    
    # Check tickers existence
    if "tickers" not in tables:
        return 0
        
    tickers_count = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()
    if not tickers_count or tickers_count[0] == 0:
        return 0

    # Populate with defaults
    # Use "00" for sector33_code and "UNCLASSIFIED" (or "その他") for name
    # We clear it first just in case
    conn.execute("DELETE FROM industry_master")
    
    conn.execute(
        """
        INSERT INTO industry_master (code, name, sector33_code, sector33_name, market_code)
        SELECT 
            code, 
            name, 
            '00' as sector33_code, 
            'その他' as sector33_name, 
            '' as market_code 
        FROM tickers
        """
    )
    
    count = conn.execute("SELECT COUNT(*) FROM industry_master").fetchone()
    return count[0] if count else 0
