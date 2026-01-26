from __future__ import annotations

import sqlite3
import threading

import duckdb

from app.core.config import FAVORITES_DB_PATH, PRACTICE_DB_PATH, config

_SCHEMA_INIT_LOCK = threading.Lock()


def _init_duckdb_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tickers (
            code TEXT PRIMARY KEY,
            name TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_bars (
            code TEXT,
            date INTEGER,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            PRIMARY KEY(code, date)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_ma (
            code TEXT,
            date INTEGER,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            PRIMARY KEY(code, date)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_bars (
            code TEXT,
            month INTEGER,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            PRIMARY KEY(code, month)
        );
        """
    )
    try:
        conn.execute("ALTER TABLE monthly_bars ADD COLUMN v BIGINT")
    except Exception:
        pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_ma (
            code TEXT,
            month INTEGER,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            PRIMARY KEY(code, month)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sys_jobs (
            id TEXT PRIMARY KEY,
            type TEXT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            progress INTEGER,
            message TEXT,
            error TEXT
        );
        """
    )

    # Trade history / positions (used by /api/trades and Positions UI).
    # Keep schemas compatible with legacy inserts from `app.backend.import_positions`.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_events (
            -- Legacy code sometimes selects `id` but current ingest does not populate it.
            id BIGINT,
            broker TEXT,
            exec_dt TIMESTAMP,
            symbol TEXT,
            action TEXT,
            qty DOUBLE,
            price DOUBLE,
            source_row_hash TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            transaction_type TEXT,
            side_type TEXT,
            margin_type TEXT,
            UNIQUE(source_row_hash)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS initial_positions_seed (
            symbol TEXT PRIMARY KEY,
            buy_qty DOUBLE,
            sell_qty DOUBLE,
            asof_dt TIMESTAMP,
            memo TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS positions_live (
            symbol TEXT PRIMARY KEY,
            spot_qty DOUBLE,
            margin_long_qty DOUBLE,
            margin_short_qty DOUBLE,
            buy_qty DOUBLE,
            sell_qty DOUBLE,
            opened_at TIMESTAMP,
            updated_at TIMESTAMP,
            has_issue BOOLEAN,
            issue_note TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS position_rounds (
            round_id TEXT PRIMARY KEY,
            symbol TEXT,
            opened_at TIMESTAMP,
            closed_at TIMESTAMP,
            closed_reason TEXT,
            last_state_sell_buy TEXT,
            has_issue BOOLEAN,
            issue_note TEXT
        );
        """
    )


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    # CREATE TABLE IF NOT EXISTS is cheap and safe; avoid caching because the app
    # can switch DB paths via env vars (launcher/tests) and we must always ensure
    # the target DB has all required tables.
    with _SCHEMA_INIT_LOCK:
        _init_duckdb_schema(conn)


def init_schema() -> None:
    import duckdb

    conn = duckdb.connect(str(config.DB_PATH))
    try:
        ensure_schema(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _get_favorites_conn():
    conn = sqlite3.connect(FAVORITES_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _get_practice_conn():
    conn = sqlite3.connect(PRACTICE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_practice_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    existing = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if any(row[1] == column for row in existing):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def _init_favorites_schema() -> None:
    with _get_favorites_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS favorites (
                code TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )


def _init_practice_schema() -> None:
    with _get_practice_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS practice_sessions (
                session_id TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                cursor_time INTEGER,
                max_unlocked_time INTEGER,
                lot_size INTEGER,
                range_months INTEGER,
                trades TEXT,
                notes TEXT,
                ui_state TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        _ensure_practice_column(conn, "practice_sessions", "end_date", "TEXT")
        _ensure_practice_column(conn, "practice_sessions", "cursor_time", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "max_unlocked_time", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "lot_size", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "range_months", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "ui_state", "TEXT")


def init_extra_schemas() -> None:
    _init_favorites_schema()
    _init_practice_schema()
