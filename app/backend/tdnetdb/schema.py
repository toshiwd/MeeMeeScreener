from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb


def ensure_tdnetdb_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tdnet_disclosures (
            disclosure_id TEXT PRIMARY KEY,
            sec_code TEXT,
            company_name TEXT,
            title TEXT,
            category TEXT,
            published_at TIMESTAMP,
            tdnet_url TEXT,
            pdf_url TEXT,
            xbrl_url TEXT,
            summary_text TEXT,
            raw_json TEXT,
            fetched_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tdnet_disclosure_features (
            disclosure_id TEXT PRIMARY KEY,
            sec_code TEXT,
            published_at TIMESTAMP,
            event_type TEXT,
            sentiment TEXT,
            importance_score DOUBLE,
            forecast_revision BOOLEAN,
            dividend_revision BOOLEAN,
            share_buyback BOOLEAN,
            share_split BOOLEAN,
            earnings BOOLEAN,
            governance BOOLEAN,
            distress BOOLEAN,
            title_normalized TEXT,
            tags_json TEXT,
            raw_text TEXT,
            fetched_at TIMESTAMP
        );
        """
    )


def ensure_tdnetdb_schema_at_path(db_path: str | Path) -> None:
    conn = duckdb.connect(str(Path(db_path).expanduser().resolve()))
    try:
        ensure_tdnetdb_schema(conn)
    finally:
        conn.close()


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
