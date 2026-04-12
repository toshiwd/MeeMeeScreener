from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import duckdb

FEATURE_FRAME_VERSION = "feature_frame_v1"
_NUMERIC_TYPES = {
    "BIGINT",
    "DOUBLE",
    "FLOAT",
    "HUGEINT",
    "INTEGER",
    "REAL",
    "SMALLINT",
    "TINYINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return bool(row)


def _column_map(conn: duckdb.DuckDBPyConnection, table_name: str) -> dict[str, str]:
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return {str(row[1]): str(row[2]).upper() for row in rows}


def _normalized_date_sql(expr: str) -> str:
    return f"""
        CASE
            WHEN {expr} IS NULL THEN NULL
            WHEN CAST({expr} AS BIGINT) >= 100000000 THEN CAST(strftime(to_timestamp(CAST({expr} AS BIGINT)), '%Y%m%d') AS INTEGER)
            ELSE CAST({expr} AS INTEGER)
        END
    """


def _presence_flag(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    if not _table_exists(conn, table_name):
        return 0
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return 1 if row and int(row[0] or 0) > 0 else 0


def _source_presence_flags(conn: duckdb.DuckDBPyConnection, *, base_table: str) -> dict[str, int]:
    flags = {
        "source_presence_flag_feature_frame_daily": 1,
        "source_presence_flag_feature_snapshot_daily": _presence_flag(conn, "feature_snapshot_daily"),
        "source_presence_flag_ml_feature_daily": _presence_flag(conn, "ml_feature_daily"),
        "source_presence_flag_signal_decision_daily": _presence_flag(conn, "signal_decision_daily"),
        "source_presence_flag_trade_events": _presence_flag(conn, "trade_events"),
        "source_presence_flag_taisyaku_balance_daily": _presence_flag(conn, "taisyaku_balance_daily"),
        "source_presence_flag_earnings_planned": _presence_flag(conn, "earnings_planned"),
        "source_presence_flag_ex_rights": _presence_flag(conn, "ex_rights"),
        "source_presence_flag_edinetdb_company_map": _presence_flag(conn, "edinetdb_company_map"),
        "source_presence_flag_edinetdb_ratios": _presence_flag(conn, "edinetdb_ratios"),
        "source_presence_flag_edinetdb_financials": _presence_flag(conn, "edinetdb_financials"),
        "source_presence_flag_edinetdb_official_documents": _presence_flag(conn, "edinetdb_official_documents"),
        "source_presence_flag_edinetdb_text_blocks": _presence_flag(conn, "edinetdb_text_blocks"),
        "source_presence_flag_market_regime_daily": _presence_flag(conn, "market_regime_daily"),
        "source_presence_flag_tdnet_disclosures": _presence_flag(conn, "tdnet_disclosures"),
    }
    return flags


def materialize_feature_frame_daily(
    conn: duckdb.DuckDBPyConnection,
    *,
    feature_frame_version: str = FEATURE_FRAME_VERSION,
) -> dict[str, Any]:
    base_table = None
    if _table_exists(conn, "ml_feature_daily"):
        base_table = "ml_feature_daily"
    elif _table_exists(conn, "feature_snapshot_daily"):
        base_table = "feature_snapshot_daily"
    if base_table is None:
        conn.execute("DROP TABLE IF EXISTS feature_frame_daily")
        return {"ok": False, "reason": "feature_source_missing", "base_table": None}

    columns = _column_map(conn, base_table)
    if "dt" in columns:
        date_column = "dt"
    elif "as_of_date" in columns:
        date_column = "as_of_date"
    else:
        raise ValueError(f"feature_frame_source_missing_date_column:{base_table}")

    if "available_at" in columns:
        future_row = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {base_table}
            WHERE {_normalized_date_sql("available_at")} > {_normalized_date_sql(date_column)}
            """,
        ).fetchone()
        if future_row and int(future_row[0] or 0) > 0:
            raise ValueError(f"feature_frame_future_data:{base_table}")

    selected_columns: list[str] = []
    for column_name, column_type in columns.items():
        if column_name in {"dt", "as_of_date", "feature_version", "feature_frame_version", "computed_at", "available_at", "candle_flags"}:
            continue
        if column_name.startswith("source_presence_flag_"):
            continue
        if column_name == "code":
            continue
        if column_type in _NUMERIC_TYPES:
            selected_columns.append(column_name)

    presence_flags = _source_presence_flags(conn, base_table=base_table)
    feature_version_expr = "feature_version AS feature_version" if "feature_version" in columns else "NULL AS feature_version"
    available_at_source = "available_at" if "available_at" in columns else date_column
    available_at_expr = f"CAST({_normalized_date_sql(available_at_source)} AS INTEGER) AS available_at"
    select_exprs = [
        f"CAST({_normalized_date_sql(date_column)} AS INTEGER) AS dt",
        "code",
        feature_version_expr,
        f"'{feature_frame_version}' AS feature_frame_version",
        f"{_utcnow().isoformat(timespec='seconds')!r}::TIMESTAMP AS computed_at",
        available_at_expr,
    ]
    select_exprs.extend(selected_columns)
    select_exprs.extend([f"{int(value)} AS {name}" for name, value in sorted(presence_flags.items())])

    conn.execute("DROP TABLE IF EXISTS feature_frame_daily")
    conn.execute(
        f"""
        CREATE TABLE feature_frame_daily AS
        SELECT
            {', '.join(select_exprs)}
        FROM {base_table}
        """
    )

    if not _table_exists(conn, "ml_feature_daily"):
        conn.execute("CREATE OR REPLACE VIEW ml_feature_daily AS SELECT * FROM feature_frame_daily")

    row_count = int(conn.execute("SELECT COUNT(*) FROM feature_frame_daily").fetchone()[0] or 0)
    return {
        "ok": True,
        "base_table": base_table,
        "row_count": row_count,
        "feature_frame_version": feature_frame_version,
        "source_presence_flags": presence_flags,
    }
