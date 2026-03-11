from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

from app.backend.edinetdb.schema import ensure_edinetdb_schema, ensure_edinetdb_schema_at_path

EDINET_TABLES = (
    "edinetdb_company_map",
    "edinetdb_company_latest",
    "edinetdb_financials",
    "edinetdb_ratios",
    "edinetdb_text_blocks",
    "edinetdb_analysis",
    "edinetdb_task_queue",
    "edinetdb_api_call_log",
    "edinetdb_unmapped_codes",
    "edinetdb_meta",
)


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, bool]]:
    rows = conn.execute(f"PRAGMA table_info({_qident(table)})").fetchall()
    # cid, name, type, notnull, dflt_value, pk
    return [(str(r[1]), bool(r[5])) for r in rows]


def merge_edinetdb_tables(*, dst_db_path: str | Path, src_db_path: str | Path) -> dict[str, int]:
    dst = str(Path(dst_db_path).expanduser().resolve())
    src = str(Path(src_db_path).expanduser().resolve())
    ensure_edinetdb_schema_at_path(dst)
    ensure_edinetdb_schema_at_path(src)

    merged: dict[str, int] = {}
    conn = duckdb.connect(dst)
    try:
        ensure_edinetdb_schema(conn)
        escaped_src = src.replace("'", "''")
        conn.execute(f"ATTACH '{escaped_src}' AS edsrc")
        for table in EDINET_TABLES:
            cols = _table_columns(conn, table)
            if not cols:
                continue
            col_names = [name for name, _ in cols]
            pk_names = [name for name, is_pk in cols if is_pk]
            if not pk_names:
                continue
            updatable = [c for c in col_names if c not in pk_names]
            cols_sql = ", ".join(_qident(c) for c in col_names)
            pk_sql = ", ".join(_qident(c) for c in pk_names)
            set_sql = ", ".join(f"{_qident(c)}=excluded.{_qident(c)}" for c in updatable)
            sql = (
                f"INSERT INTO {_qident(table)} ({cols_sql}) "
                f"SELECT {cols_sql} FROM edsrc.{_qident(table)} "
                f"ON CONFLICT ({pk_sql}) DO UPDATE SET {set_sql}"
            )
            conn.execute(sql)
            merged[table] = int(conn.execute("SELECT changes()").fetchone()[0] or 0)
    finally:
        conn.close()
    return merged


def merge_raw_dirs(*, dst_raw_dir: str | Path, src_raw_dir: str | Path) -> dict[str, int]:
    dst = Path(dst_raw_dir).expanduser().resolve()
    src = Path(src_raw_dir).expanduser().resolve()
    copied = 0
    skipped = 0
    if not src.exists():
        return {"copied": 0, "skipped": 0}
    for file in src.rglob("*.json.gz"):
        rel = file.relative_to(src)
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            skipped += 1
            continue
        shutil.copy2(file, target)
        copied += 1
    return {"copied": copied, "skipped": skipped}
