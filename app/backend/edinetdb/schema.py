from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb


def ensure_edinetdb_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_company_map (
            sec_code TEXT PRIMARY KEY,
            edinet_code TEXT,
            name TEXT,
            industry TEXT,
            updated_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_company_latest (
            edinet_code TEXT PRIMARY KEY,
            latest_fiscal_year TEXT,
            latest_hash TEXT,
            fetched_at TIMESTAMP,
            last_checked_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_financials (
            edinet_code TEXT,
            fiscal_year TEXT,
            accounting_standard TEXT,
            payload_json TEXT,
            fetched_at TIMESTAMP,
            PRIMARY KEY(edinet_code, fiscal_year, accounting_standard)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_ratios (
            edinet_code TEXT,
            fiscal_year TEXT,
            accounting_standard TEXT,
            payload_json TEXT,
            fetched_at TIMESTAMP,
            PRIMARY KEY(edinet_code, fiscal_year, accounting_standard)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_text_blocks (
            edinet_code TEXT,
            fiscal_year TEXT,
            block_name TEXT,
            text TEXT,
            fetched_at TIMESTAMP,
            PRIMARY KEY(edinet_code, fiscal_year, block_name)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_analysis (
            edinet_code TEXT,
            asof_date TEXT,
            payload_json TEXT,
            fetched_at TIMESTAMP,
            PRIMARY KEY(edinet_code, asof_date)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_task_queue (
            task_key TEXT PRIMARY KEY,
            job_name TEXT,
            phase TEXT,
            edinet_code TEXT,
            endpoint TEXT,
            params_json TEXT,
            priority INTEGER,
            status TEXT,
            tries INTEGER DEFAULT 0,
            http_status INTEGER,
            last_error TEXT,
            retry_at TIMESTAMP,
            fetched_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_api_call_log (
            id TEXT PRIMARY KEY,
            called_at TIMESTAMP,
            jst_date DATE,
            job_name TEXT,
            endpoint TEXT,
            edinet_code TEXT,
            http_status INTEGER,
            error_type TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_unmapped_codes (
            sec_code TEXT PRIMARY KEY,
            reason TEXT,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_meta (
            key TEXT PRIMARY KEY,
            value_json TEXT,
            updated_at TIMESTAMP
        );
        """
    )


def ensure_edinetdb_schema_at_path(db_path: str | Path) -> None:
    conn = duckdb.connect(str(Path(db_path).expanduser().resolve()))
    try:
        ensure_edinetdb_schema(conn)
    finally:
        conn.close()


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
