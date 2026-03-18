from __future__ import annotations

from datetime import datetime, timezone
import shutil
from pathlib import Path
from typing import Any

import duckdb

from research.storage import ResearchPaths, default_source_db_path, ensure_clean_dir, read_json, write_json


def _quote_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _quote_ident(name: str) -> str:
    text = str(name).replace('"', '""')
    return f'"{text}"'


def resolve_source_db_path(source_db: str | None = None) -> Path:
    if source_db and str(source_db).strip():
        return Path(str(source_db)).expanduser().resolve()
    return default_source_db_path()


def source_signature(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
    }


def _list_source_tables(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT schema_name, table_name
        FROM duckdb_tables()
        WHERE database_name = 'src' AND internal = false
        ORDER BY schema_name, table_name
        """
    ).fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def _copy_source_tables(target_db: Path, source_db: Path) -> list[dict[str, Any]]:
    copied: list[dict[str, Any]] = []
    conn = duckdb.connect(str(target_db))
    try:
        conn.execute(f"ATTACH '{_quote_path(source_db)}' AS src (READ_ONLY)")
        for schema_name, table_name in _list_source_tables(conn):
            if schema_name and schema_name != "main":
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema_name)}")
                target_ref = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
                source_ref = f"src.{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
            else:
                target_ref = _quote_ident(table_name)
                source_ref = f"src.main.{_quote_ident(table_name)}"
            conn.execute(f"CREATE TABLE {target_ref} AS SELECT * FROM {source_ref}")
            count = int(conn.execute(f"SELECT COUNT(*) FROM {target_ref}").fetchone()[0])
            copied.append({"schema": schema_name or "main", "table": table_name, "rows": count})
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return copied


def sync_source_mirror(
    paths: ResearchPaths,
    *,
    source_db: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    source_path = resolve_source_db_path(source_db)
    if not source_path.exists():
        raise FileNotFoundError(f"source DB not found: {source_path}")

    signature = source_signature(source_path)
    current_db = paths.current_mirror_db
    current_manifest_path = paths.current_mirror_manifest
    if (not force) and current_db.exists() and current_manifest_path.exists():
        try:
            current_manifest = read_json(current_manifest_path)
        except Exception:
            current_manifest = {}
        if current_manifest.get("source_signature") == signature:
            return {
                "ok": True,
                "changed": False,
                "source_db": str(source_path),
                "source_signature": signature,
                "mirror_db": str(current_db),
                "mirror_manifest": str(current_manifest_path),
                "table_count": int(len(current_manifest.get("tables") or [])),
            }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    stage_dir = paths.mirror_root / f".stage_{stamp}"
    ensure_clean_dir(stage_dir)
    stage_db = stage_dir / "source.duckdb"
    try:
        copied_tables = _copy_source_tables(stage_db, source_path)
        manifest = {
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_signature": signature,
            "source_db": str(source_path),
            "table_count": int(len(copied_tables)),
            "tables": copied_tables,
        }
        write_json(stage_dir / "mirror_manifest.json", manifest)
        paths.replace_dir_atomically(stage_dir, paths.current_mirror_dir)
        return {
            "ok": True,
            "changed": True,
            "source_db": str(source_path),
            "source_signature": signature,
            "mirror_db": str(paths.current_mirror_db),
            "mirror_manifest": str(paths.current_mirror_manifest),
            "table_count": int(len(copied_tables)),
        }
    except Exception:
        shutil.rmtree(stage_dir, ignore_errors=True)
        raise
