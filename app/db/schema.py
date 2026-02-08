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
        CREATE TABLE IF NOT EXISTS feature_snapshot_daily (
            dt INTEGER,
            code TEXT,
            close DOUBLE,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            atr14 DOUBLE,
            diff20_pct DOUBLE,
            diff20_atr DOUBLE,
            cnt_20_above INTEGER,
            cnt_7_above INTEGER,
            day_count INTEGER,
            candle_flags TEXT,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS label_20d (
            dt INTEGER,
            code TEXT,
            cont_label INTEGER,
            ex_label INTEGER,
            n_forward INTEGER,
            label_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS phase_pred_daily (
            dt INTEGER,
            code TEXT,
            early_score DOUBLE,
            late_score DOUBLE,
            body_score DOUBLE,
            n INTEGER,
            reasons_top3 TEXT,
            pred_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_feature_daily (
            dt INTEGER,
            code TEXT,
            close DOUBLE,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            atr14 DOUBLE,
            diff20_pct DOUBLE,
            cnt_20_above INTEGER,
            cnt_7_above INTEGER,
            close_prev1 DOUBLE,
            close_prev5 DOUBLE,
            close_prev10 DOUBLE,
            ma7_prev1 DOUBLE,
            ma20_prev1 DOUBLE,
            ma60_prev1 DOUBLE,
            diff20_prev1 DOUBLE,
            cnt_20_prev1 INTEGER,
            cnt_7_prev1 INTEGER,
            feature_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_label_20d (
            dt INTEGER,
            code TEXT,
            ret20 DOUBLE,
            up20_label INTEGER,
            train_mask_cls INTEGER,
            turn_up_label INTEGER,
            turn_down_label INTEGER,
            train_mask_turn INTEGER,
            n_forward INTEGER,
            label_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_pred_20d (
            dt INTEGER,
            code TEXT,
            p_up DOUBLE,
            p_turn_up DOUBLE,
            p_turn_down DOUBLE,
            ret_pred20 DOUBLE,
            ev20 DOUBLE,
            ev20_net DOUBLE,
            model_version TEXT,
            n_train INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_model_registry (
            model_version TEXT PRIMARY KEY,
            model_key TEXT,
            objective TEXT,
            feature_version INTEGER,
            label_version INTEGER,
            train_start_dt INTEGER,
            train_end_dt INTEGER,
            metrics_json TEXT,
            artifact_path TEXT,
            n_train INTEGER,
            created_at TIMESTAMP,
            is_active BOOLEAN
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_backtest_runs (
            run_id TEXT PRIMARY KEY,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT,
            start_dt INTEGER,
            end_dt INTEGER,
            max_codes INTEGER,
            config_json TEXT,
            metrics_json TEXT,
            note TEXT
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_memos (
            symbol TEXT,
            date TEXT,
            timeframe TEXT,
            memo TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, date, timeframe)
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
