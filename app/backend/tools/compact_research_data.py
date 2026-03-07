from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

from app.backend.services import strategy_backtest_service


DEFAULT_SOURCE_TABLES: tuple[str, ...] = (
    "daily_bars",
    "ml_pred_20d",
    "industry_master",
    "earnings_planned",
    "ex_rights",
)
DEFAULT_RESULT_TABLES: tuple[str, ...] = (
    "strategy_backtest_runs",
    "strategy_walkforward_runs",
    "strategy_walkforward_gate_reports",
    "strategy_walkforward_research_daily",
)
RESULT_TABLE_BUILDERS: dict[str, Any] = {
    "strategy_backtest_runs": strategy_backtest_service._ensure_backtest_schema,  # type: ignore[attr-defined]
    "strategy_walkforward_runs": strategy_backtest_service._ensure_walkforward_schema,  # type: ignore[attr-defined]
    "strategy_walkforward_gate_reports": strategy_backtest_service._ensure_walkforward_gate_schema,  # type: ignore[attr-defined]
    "strategy_walkforward_research_daily": strategy_backtest_service._ensure_walkforward_research_schema,  # type: ignore[attr-defined]
}


def _quote_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _table_exists(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    *,
    database: str = "memory",
    schema: str = "main",
) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_tables()
        WHERE database_name = ? AND schema_name = ? AND table_name = ?
        """,
        [database, schema, table_name],
    ).fetchone()
    return bool(row and row[0])


def _remove_db_file(path: Path) -> None:
    for candidate in (path, path.with_name(path.name + ".wal")):
        try:
            if candidate.exists():
                candidate.unlink()
        except FileNotFoundError:
            pass


def _copy_table(
    conn: duckdb.DuckDBPyConnection,
    *,
    src_schema: str,
    table_name: str,
) -> int:
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {src_schema}.{table_name}")
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _copy_result_table_with_schema(
    conn: duckdb.DuckDBPyConnection,
    *,
    src_schema: str,
    table_name: str,
) -> int:
    builder = RESULT_TABLE_BUILDERS.get(table_name)
    if builder is None:
        return _copy_table(conn, src_schema=src_schema, table_name=table_name)
    builder(conn)
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM {src_schema}.{table_name}")
    row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def compact_research_database(
    source_db_path: str | Path,
    output_db_path: str | Path,
    *,
    keep_daily_ma: bool = False,
    overwrite: bool = False,
) -> dict[str, Any]:
    source_path = Path(source_db_path).expanduser().resolve(strict=True)
    output_path = Path(output_db_path).expanduser().resolve(strict=False)
    if source_path == output_path:
        raise ValueError("source_db_path and output_db_path must differ")

    if output_path.exists():
        if not overwrite:
            raise FileExistsError(f"Output DB already exists: {output_path}")
        _remove_db_file(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    source_tables = list(DEFAULT_SOURCE_TABLES)
    if keep_daily_ma:
        source_tables.append("daily_ma")
    preserve_tables = list(DEFAULT_RESULT_TABLES)

    copied_tables: list[dict[str, Any]] = []
    with duckdb.connect(str(output_path)) as conn:
        conn.execute(f"ATTACH '{_quote_path(source_path)}' AS src (READ_ONLY)")
        for table_name in [*source_tables, *preserve_tables]:
            if not _table_exists(conn, table_name, database="src", schema="main"):
                continue
            copied_rows = (
                _copy_result_table_with_schema(conn, src_schema="src", table_name=table_name)
                if table_name in RESULT_TABLE_BUILDERS
                else _copy_table(conn, src_schema="src", table_name=table_name)
            )
            copied_tables.append({"table": table_name, "rows": copied_rows})
        conn.execute("CHECKPOINT")

    output_size = output_path.stat().st_size if output_path.exists() else 0
    source_size = source_path.stat().st_size if source_path.exists() else 0
    return {
        "source_db_path": str(source_path),
        "output_db_path": str(output_path),
        "source_size_bytes": int(source_size),
        "output_size_bytes": int(output_size),
        "saved_bytes": int(max(0, source_size - output_size)),
        "saved_ratio": (float(source_size - output_size) / float(source_size)) if source_size > 0 else None,
        "keep_daily_ma": bool(keep_daily_ma),
        "copied_tables": copied_tables,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a lightweight research DuckDB copy.")
    parser.add_argument("--source-db", required=True, help="Source DuckDB path")
    parser.add_argument("--output-db", required=True, help="Output DuckDB path")
    parser.add_argument(
        "--keep-daily-ma",
        action="store_true",
        help="Keep daily_ma in the output DB. Default omits it because walkforward can recompute MA values.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output DB when it already exists.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    result = compact_research_database(
        args.source_db,
        args.output_db,
        keep_daily_ma=bool(args.keep_daily_ma),
        overwrite=bool(args.overwrite),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
