from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

import duckdb

from external_analysis.contracts.paths import resolve_source_db_path


def connect_source_db(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    resolved = resolve_source_db_path(db_path)
    return duckdb.connect(str(resolved), read_only=True)


def normalize_market_date(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            value = int(text)
        else:
            text = text.replace("-", "")
            if len(text) == 8 and text.isdigit():
                return int(text)
            raise ValueError(f"unsupported market date value: {value}")
    ivalue = int(value)
    if ivalue >= 100_000_000:
        return int(datetime.fromtimestamp(ivalue, tz=timezone.utc).strftime("%Y%m%d"))
    if 10_000_000 <= ivalue <= 99_999_999:
        return ivalue
    if 100_000 <= ivalue <= 999_999:
        return int(f"{ivalue:06d}01")
    return ivalue


def source_table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def source_column_exists(conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ? AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def fetch_rows(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: Iterable[str],
    *,
    order_by: str | None = None,
) -> list[dict[str, Any]]:
    selected = ", ".join(columns)
    query = f"SELECT {selected} FROM {table_name}"
    if order_by:
        query += f" ORDER BY {order_by}"
    rows = conn.execute(query).fetchall()
    names = [str(col) for col in columns]
    return [dict(zip(names, row, strict=True)) for row in rows]
