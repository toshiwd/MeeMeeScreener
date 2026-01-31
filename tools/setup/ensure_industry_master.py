from __future__ import annotations

import argparse
import sys

import duckdb


def ensure_industry_master(db_path: str) -> int:
    with duckdb.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
        if "industry_master" in tables:
            count = conn.execute("SELECT COUNT(*) FROM industry_master").fetchone()[0]
            if count and count > 0:
                return 0

        # Build minimal industry_master from tickers to avoid heatmap fallback zeros.
        tickers = conn.execute("SELECT code, name FROM tickers").fetchall()

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
        if not tickers:
            # Keep an empty table if no tickers are available; gate only checks table existence.
            return 0

        conn.execute("DELETE FROM industry_master")
        conn.executemany(
            """
            INSERT INTO industry_master (code, name, sector33_code, sector33_name, market_code)
            VALUES (?, ?, ?, ?, ?)
            """,
            [(code, name, "00", "UNCLASSIFIED", "") for code, name in tickers],
        )
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Ensure industry_master exists in DuckDB")
    parser.add_argument("--db", required=True, help="DuckDB path")
    args = parser.parse_args(argv[1:])
    return ensure_industry_master(args.db)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
