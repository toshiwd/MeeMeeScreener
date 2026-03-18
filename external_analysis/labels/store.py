from __future__ import annotations

from typing import Any

import duckdb

from external_analysis.contracts.paths import resolve_label_db_path

LABEL_TABLES: tuple[str, ...] = (
    "label_daily_h5",
    "label_daily_h10",
    "label_daily_h20",
    "label_daily_h40",
    "label_daily_h60",
    "label_aux_monthly",
    "anchor_window_master",
    "anchor_window_bars",
    "label_generation_runs",
    "label_generation_manifest",
)


def connect_label_db(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    resolved = resolve_label_db_path(db_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(resolved), read_only=False)


def ensure_label_schema(conn: duckdb.DuckDBPyConnection) -> None:
    for horizon in (5, 10, 20, 40, 60):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS label_daily_h{horizon} (
                code TEXT NOT NULL,
                as_of_date INTEGER NOT NULL,
                horizon_days INTEGER NOT NULL,
                ret_h DOUBLE,
                mfe_h DOUBLE,
                mae_h DOUBLE,
                days_to_mfe_h INTEGER,
                days_to_stop_h INTEGER,
                cross_section_count INTEGER,
                rank_ret_h INTEGER,
                top_1pct_h BOOLEAN,
                top_3pct_h BOOLEAN,
                top_5pct_h BOOLEAN,
                future_window_start_date INTEGER,
                future_window_end_date INTEGER,
                purge_end_date INTEGER,
                embargo_until_date INTEGER,
                leakage_group_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                generation_run_id TEXT NOT NULL,
                PRIMARY KEY (code, as_of_date)
            )
            """
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS label_aux_monthly (
            code TEXT NOT NULL,
            as_of_date INTEGER NOT NULL,
            month_key INTEGER NOT NULL,
            monthly_rank INTEGER,
            monthly_top5 BOOLEAN,
            monthly_top10 BOOLEAN,
            generation_run_id TEXT NOT NULL,
            PRIMARY KEY (code, as_of_date, month_key)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS anchor_window_master (
            anchor_id TEXT PRIMARY KEY,
            code TEXT NOT NULL,
            anchor_type TEXT NOT NULL,
            anchor_date INTEGER NOT NULL,
            window_start_date INTEGER NOT NULL,
            window_end_date INTEGER NOT NULL,
            future_window_end_date INTEGER NOT NULL,
            collision_group_id TEXT NOT NULL,
            overlap_group_id TEXT NOT NULL,
            purge_end_date INTEGER NOT NULL,
            embargo_until_date INTEGER NOT NULL,
            outcome_ret_20 DOUBLE,
            outcome_mfe_20 DOUBLE,
            outcome_mae_20 DOUBLE,
            generation_run_id TEXT NOT NULL,
            policy_version TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS anchor_window_bars (
            anchor_id TEXT NOT NULL,
            code TEXT NOT NULL,
            anchor_type TEXT NOT NULL,
            anchor_date INTEGER NOT NULL,
            rel_day INTEGER NOT NULL,
            trade_date INTEGER NOT NULL,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            ma20 DOUBLE,
            volume_ratio_20 DOUBLE,
            close_to_ma20_pct DOUBLE,
            generation_run_id TEXT NOT NULL,
            PRIMARY KEY (anchor_id, rel_day)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS label_generation_runs (
            run_id TEXT PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP,
            status TEXT NOT NULL,
            kind TEXT NOT NULL,
            export_db_path TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            horizon_set TEXT,
            collision_guard_enabled BOOLEAN NOT NULL,
            overlap_guard_enabled BOOLEAN NOT NULL,
            embargo_days INTEGER NOT NULL,
            summary_json JSON NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS label_generation_manifest (
            generation_key TEXT PRIMARY KEY,
            source_signature TEXT NOT NULL,
            dependency_version TEXT NOT NULL,
            cache_state TEXT NOT NULL,
            dirty_ranges_json JSON NOT NULL,
            row_count INTEGER NOT NULL,
            generation_run_id TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )


def ensure_label_db(db_path: str | None = None) -> dict[str, Any]:
    conn = connect_label_db(db_path)
    try:
        ensure_label_schema(conn)
        conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "db_path": str(resolve_label_db_path(db_path)),
            "tables": list(LABEL_TABLES),
        }
    finally:
        conn.close()
