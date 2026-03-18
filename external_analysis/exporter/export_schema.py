from __future__ import annotations

from typing import Any

import duckdb

from external_analysis.contracts.paths import resolve_export_db_path

EXPORT_TABLES: tuple[str, ...] = (
    "bars_daily_export",
    "bars_monthly_export",
    "indicator_daily_export",
    "pattern_state_export",
    "ranking_snapshot_export",
    "trade_event_export",
    "position_snapshot_export",
    "meta_export_runs",
)


def connect_export_db(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    resolved = resolve_export_db_path(db_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(resolved), read_only=False)


def ensure_export_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bars_daily_export (
            code TEXT NOT NULL,
            trade_date INTEGER NOT NULL,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            source TEXT,
            row_hash TEXT NOT NULL,
            export_run_id TEXT NOT NULL,
            PRIMARY KEY (code, trade_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bars_monthly_export (
            code TEXT NOT NULL,
            month_key INTEGER NOT NULL,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            row_hash TEXT NOT NULL,
            export_run_id TEXT NOT NULL,
            PRIMARY KEY (code, month_key)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS indicator_daily_export (
            code TEXT NOT NULL,
            trade_date INTEGER NOT NULL,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            ma100 DOUBLE,
            ma200 DOUBLE,
            atr14 DOUBLE,
            diff20_pct DOUBLE,
            diff20_atr DOUBLE,
            cnt_20_above INTEGER,
            cnt_7_above INTEGER,
            day_count INTEGER,
            candle_flags TEXT,
            row_hash TEXT NOT NULL,
            export_run_id TEXT NOT NULL,
            PRIMARY KEY (code, trade_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pattern_state_export (
            code TEXT NOT NULL,
            trade_date INTEGER NOT NULL,
            ppp_state TEXT,
            abc_state TEXT,
            box_state TEXT,
            box_upper DOUBLE,
            box_lower DOUBLE,
            ranking_state TEXT,
            event_flags TEXT,
            row_hash TEXT NOT NULL,
            export_run_id TEXT NOT NULL,
            PRIMARY KEY (code, trade_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ranking_snapshot_export (
            trade_date INTEGER NOT NULL,
            code TEXT NOT NULL,
            ranking_family TEXT NOT NULL,
            ranking_value DOUBLE,
            row_hash TEXT NOT NULL,
            export_run_id TEXT NOT NULL,
            PRIMARY KEY (trade_date, code, ranking_family)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_event_export (
            code TEXT NOT NULL,
            event_ts TIMESTAMP NOT NULL,
            event_seq INTEGER NOT NULL,
            event_type TEXT,
            broker_label TEXT,
            qty DOUBLE,
            price DOUBLE,
            row_hash TEXT NOT NULL,
            export_run_id TEXT NOT NULL,
            PRIMARY KEY (code, event_ts, event_seq)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS position_snapshot_export (
            code TEXT NOT NULL,
            snapshot_at TIMESTAMP NOT NULL,
            spot_qty DOUBLE,
            margin_long_qty DOUBLE,
            margin_short_qty DOUBLE,
            buy_qty DOUBLE,
            sell_qty DOUBLE,
            has_issue BOOLEAN,
            issue_note TEXT,
            row_hash TEXT NOT NULL,
            export_run_id TEXT NOT NULL,
            PRIMARY KEY (code, snapshot_at)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_export_runs (
            run_id TEXT PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            status TEXT NOT NULL,
            source_db_path TEXT NOT NULL,
            source_signature TEXT NOT NULL,
            source_max_trade_date INTEGER,
            source_row_counts JSON NOT NULL,
            changed_table_names JSON NOT NULL,
            diff_reason JSON NOT NULL
        )
        """
    )


def ensure_export_db(db_path: str | None = None) -> dict[str, Any]:
    conn = connect_export_db(db_path)
    try:
        ensure_export_schema(conn)
        conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "db_path": str(resolve_export_db_path(db_path)),
            "tables": list(EXPORT_TABLES),
        }
    finally:
        conn.close()
