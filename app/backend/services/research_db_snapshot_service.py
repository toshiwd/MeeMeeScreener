from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.config import config
from app.db.session import try_get_conn
from shared.tradex_storage import tradex_db_path


SNAPSHOT_SCHEMA_VERSION = "meemee_research_db_snapshot_v1"
DEFAULT_SNAPSHOT_LOCK_TIMEOUT_SEC = 0.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_component(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "manual"
    out = []
    for char in raw:
        if char.isalnum() or char in {"-", "_"}:
            out.append(char)
        else:
            out.append("_")
    return "".join(out).strip("_")[:48] or "manual"


def _snapshot_root() -> Path:
    raw = str(os.getenv("MEEMEE_RESEARCH_DB_SNAPSHOT_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return tradex_db_path("meemee_snapshots").resolve()


def _database_name(conn: Any) -> str:
    row = conn.execute("SELECT current_database()").fetchone()
    name = str(row[0] or "").strip() if row else ""
    if not name:
        raise RuntimeError("current DuckDB database name could not be resolved")
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _quote_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _latest_dates(conn: Any) -> dict[str, Any]:
    def _date_expr(column_name: str) -> str:
        return f"""
            CASE
                WHEN "{column_name}" BETWEEN 19000101 AND 20991231 THEN CAST("{column_name}" AS INTEGER)
                WHEN "{column_name}" >= 1000000000000 THEN CAST(strftime(to_timestamp("{column_name}" / 1000), '%Y%m%d') AS INTEGER)
                WHEN "{column_name}" >= 1000000000 THEN CAST(strftime(to_timestamp("{column_name}"), '%Y%m%d') AS INTEGER)
                ELSE NULL
            END
        """

    def _has_table(table_name: str) -> bool:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main' AND lower(table_name) = lower(?)
            """,
            [table_name],
        ).fetchone()
        return bool(row and int(row[0] or 0) > 0)

    def _latest_int(table_name: str, column_name: str) -> int | None:
        if not _has_table(table_name):
            return None
        row = conn.execute(f'SELECT MAX({_date_expr(column_name)}) FROM "{table_name}"').fetchone()
        if not row or row[0] is None:
            return None
        try:
            return int(row[0])
        except Exception:
            return None

    return {
        "daily_bars": _latest_int("daily_bars", "date"),
        "feature_snapshot_daily": _latest_int("feature_snapshot_daily", "dt"),
        "ml_pred_20d": _latest_int("ml_pred_20d", "dt"),
    }


def _write_manifest(manifest_path: Path, payload: dict[str, Any]) -> None:
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _make_snapshot_dir(root: Path, *, stamp: str, suffix: str) -> Path:
    for index in range(100):
        name = f"{stamp}_{suffix}" if index == 0 else f"{stamp}_{suffix}_{index:02d}"
        candidate = root / name
        try:
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        except FileExistsError:
            continue
    raise RuntimeError("Could not allocate a unique research DB snapshot directory")


def create_research_db_snapshot(
    *,
    reason: str | None = None,
    actor: str | None = None,
    lock_timeout_sec: float = DEFAULT_SNAPSHOT_LOCK_TIMEOUT_SEC,
) -> dict[str, Any]:
    """
    Create a point-in-time DuckDB copy for Codex/TRADEX validation.

    This is intentionally explicit and fail-fast. It does not run from ranking,
    chart, or list-view paths, and it refuses to wait behind active MeeMee DB use
    unless the caller opts into a non-zero lock timeout.
    """

    source_path = Path(config.DB_PATH).expanduser().resolve(strict=False)
    root = _snapshot_root()
    root.mkdir(parents=True, exist_ok=True)

    started = _utc_now()
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    suffix = _safe_component(reason or actor or "manual")
    snapshot_dir = _make_snapshot_dir(root, stamp=stamp, suffix=suffix)
    snapshot_path = snapshot_dir / "stocks.duckdb"
    manifest_path = snapshot_dir / "manifest.json"

    start_perf = time.perf_counter()
    with try_get_conn(timeout_sec=max(0.0, float(lock_timeout_sec))) as conn:
        if conn is None:
            payload = {
                "ok": False,
                "reason": "live_db_busy",
                "schema_version": SNAPSHOT_SCHEMA_VERSION,
                "source_db_path": str(source_path),
                "snapshot_dir": str(snapshot_dir),
                "snapshot_db_path": str(snapshot_path),
                "manifest_path": str(manifest_path),
                "created_at": started.isoformat(),
                "lock_timeout_sec": max(0.0, float(lock_timeout_sec)),
                "message": "Live DB is busy; retry when MeeMee is idle or use a larger explicit timeout.",
            }
            _write_manifest(manifest_path, payload)
            return payload

        latest_dates = _latest_dates(conn)
        try:
            conn.execute("CHECKPOINT")
        except Exception:
            pass
        target_alias = "research_snapshot"
        conn.execute(f"ATTACH '{_quote_path(snapshot_path)}' AS {target_alias}")
        try:
            conn.execute(f"COPY FROM DATABASE {_database_name(conn)} TO {target_alias}")
        finally:
            try:
                conn.execute(f"DETACH {target_alias}")
            except Exception:
                pass

    elapsed_ms = int((time.perf_counter() - start_perf) * 1000)
    size_bytes = snapshot_path.stat().st_size if snapshot_path.exists() else 0
    payload = {
        "ok": True,
        "reason": "created",
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "source_db_path": str(source_path),
        "snapshot_dir": str(snapshot_dir),
        "snapshot_db_path": str(snapshot_path),
        "manifest_path": str(manifest_path),
        "created_at": started.isoformat(),
        "completed_at": _utc_now().isoformat(),
        "elapsed_ms": elapsed_ms,
        "size_bytes": int(size_bytes),
        "latest_dates": latest_dates,
        "usage": {
            "intended_reader": "Codex/TRADEX validation",
            "set_env": f"STOCKS_DB_PATH={snapshot_path}",
            "live_meemee_db_unchanged": True,
            "automatic_ranking_path": False,
        },
    }
    _write_manifest(manifest_path, payload)
    return payload
