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
        CREATE TABLE IF NOT EXISTS edinetdb_official_documents (
            doc_id TEXT PRIMARY KEY,
            sec_code TEXT,
            edinet_code TEXT,
            filer_name TEXT,
            form_code TEXT,
            doc_type_code TEXT,
            period_start TEXT,
            period_end TEXT,
            submit_datetime TEXT,
            doc_description TEXT,
            csv_flag INTEGER,
            pdf_flag INTEGER,
            xbrl_flag INTEGER,
            legal_status TEXT,
            payload_json TEXT,
            fetched_at TIMESTAMP
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
    for index_name, table_name, columns in (
        ("ux_edinetdb_company_map_sec_code", "edinetdb_company_map", "sec_code"),
        ("ux_edinetdb_company_latest_edinet_code", "edinetdb_company_latest", "edinet_code"),
        ("ux_edinetdb_financials_key", "edinetdb_financials", "edinet_code, fiscal_year, accounting_standard"),
        ("ux_edinetdb_ratios_key", "edinetdb_ratios", "edinet_code, fiscal_year, accounting_standard"),
        ("ux_edinetdb_text_blocks_key", "edinetdb_text_blocks", "edinet_code, fiscal_year, block_name"),
        ("ux_edinetdb_analysis_key", "edinetdb_analysis", "edinet_code, asof_date"),
        ("ux_edinetdb_official_documents_doc_id", "edinetdb_official_documents", "doc_id"),
        ("ux_edinetdb_task_queue_task_key", "edinetdb_task_queue", "task_key"),
        ("ux_edinetdb_unmapped_codes_sec_code", "edinetdb_unmapped_codes", "sec_code"),
        ("ux_edinetdb_meta_key", "edinetdb_meta", "key"),
    ):
        conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name}({columns})")


def ensure_edinetdb_schema_at_path(db_path: str | Path) -> None:
    conn = duckdb.connect(str(Path(db_path).expanduser().resolve()))
    try:
        ensure_edinetdb_schema(conn)
    finally:
        conn.close()


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
