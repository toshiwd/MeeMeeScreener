from __future__ import annotations

import json
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from external_analysis.contracts.paths import resolve_source_db_path
from external_analysis.models.feature_frame_store import materialize_feature_frame_daily
from shared.tradex_storage import tradex_scratch_path

SNAPSHOT_KEEP_LATEST = 2
SNAPSHOT_COPY_RETRIES = 10
SNAPSHOT_COPY_WAIT_SEC = 0.25
_UNIVERSE_SOURCE_PRIORITY: tuple[tuple[str, str], ...] = (
    ("feature_frame_daily", "dt"),
    ("feature_snapshot_daily", "dt"),
    ("daily_bars", "date"),
    ("ml_feature_daily", "dt"),
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _default_snapshot_root() -> Path:
    return tradex_scratch_path("source_snapshots").resolve()


def _normalize_as_of_date(value: str | int) -> int:
    text = str(value).strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"unsupported_as_of_date:{value}")
    return int(text)


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
            WHEN CAST({expr} AS BIGINT) BETWEEN 19000101 AND 20991231 THEN CAST({expr} AS INTEGER)
            WHEN CAST({expr} AS BIGINT) >= 1000000000000 THEN CAST(strftime(to_timestamp(CAST({expr} AS BIGINT) / 1000), '%Y%m%d') AS INTEGER)
            WHEN CAST({expr} AS BIGINT) >= 100000000 THEN CAST(strftime(to_timestamp(CAST({expr} AS BIGINT)), '%Y%m%d') AS INTEGER)
            ELSE CAST({expr} AS INTEGER)
        END
    """


def _resolve_universe_source(conn: duckdb.DuckDBPyConnection) -> tuple[str | None, str | None]:
    for table_name, date_column in _UNIVERSE_SOURCE_PRIORITY:
        if not _table_exists(conn, table_name):
            continue
        columns = _column_map(conn, table_name)
        if date_column in columns and "code" in columns:
            return table_name, date_column
    return None, None


def _load_universe_source_stats(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    date_column: str,
    as_of_date: int,
) -> dict[str, Any]:
    date_expr = _normalized_date_sql(date_column)
    observed_code_count_row = conn.execute(
        f"""
        SELECT COUNT(DISTINCT code)
        FROM {table_name}
        WHERE {date_expr} = ?
        """,
        [as_of_date],
    ).fetchone()
    latest_trade_date_row = conn.execute(
        f"""
        SELECT MAX({date_expr})
        FROM {table_name}
        """
    ).fetchone()
    return {
        "source_table": table_name,
        "source_date_column": date_column,
        "observed_code_count": int((observed_code_count_row or [0])[0] or 0),
        "latest_trade_date": None
        if latest_trade_date_row is None or latest_trade_date_row[0] is None
        else int(latest_trade_date_row[0]),
    }


def _wal_path(db_path: Path) -> Path:
    return Path(f"{db_path}.wal")


def _snapshot_name(*, label: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(label or "analysis"))
    timestamp = _utcnow().strftime("%Y%m%dT%H%M%S%fZ")
    return f"{safe}_{timestamp}"


def _copy_file_with_retry(*, source: Path, target: Path) -> None:
    last_error: Exception | None = None
    for _ in range(SNAPSHOT_COPY_RETRIES):
        try:
            shutil.copy2(str(source), str(target))
            return
        except Exception as exc:  # pragma: no cover - exercised via final retry path
            last_error = exc
            time.sleep(SNAPSHOT_COPY_WAIT_SEC)
    if last_error is not None:
        raise last_error


def _cleanup_old_snapshots(*, snapshot_root: Path, keep_latest: int, retention_days: int = 14) -> None:
    keep = max(1, int(keep_latest))
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(retention_days)))
    metadata_files = sorted(snapshot_root.glob("*.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    stale_metadata = metadata_files[keep:]
    for metadata_path in stale_metadata:
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        snapshot_db_raw = str(payload.get("snapshot_db_path") or "").strip()
        snapshot_wal_raw = str(payload.get("snapshot_wal_path") or "").strip()
        snapshot_db = Path(snapshot_db_raw) if snapshot_db_raw else None
        snapshot_wal = Path(snapshot_wal_raw) if snapshot_wal_raw else None
        for candidate in (snapshot_wal, snapshot_db, metadata_path):
            if candidate is not None and candidate.exists() and candidate != snapshot_root:
                try:
                    candidate.unlink(missing_ok=True)
                except PermissionError:
                    # Windows can keep DuckDB WAL handles alive; cleanup is best-effort.
                    continue
    for metadata_path in list(snapshot_root.glob("*.json")):
        try:
            modified = datetime.fromtimestamp(metadata_path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if modified >= cutoff:
            continue
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        snapshot_db_raw = str(payload.get("snapshot_db_path") or "").strip()
        snapshot_wal_raw = str(payload.get("snapshot_wal_path") or "").strip()
        snapshot_db = Path(snapshot_db_raw) if snapshot_db_raw else None
        snapshot_wal = Path(snapshot_wal_raw) if snapshot_wal_raw else None
        for candidate in (snapshot_wal, snapshot_db, metadata_path):
            if candidate is not None and candidate.exists() and candidate != snapshot_root:
                try:
                    candidate.unlink(missing_ok=True)
                except PermissionError:
                    continue


def probe_source_universe_readiness(
    *,
    source_db_path: str | None = None,
    as_of_date: str | int,
    min_universe_code_count: int = 650,
) -> dict[str, Any]:
    resolved_source = resolve_source_db_path(source_db_path)
    if not resolved_source.exists():
        raise FileNotFoundError(f"source_db_not_found:{resolved_source}")
    as_of_date_int = _normalize_as_of_date(as_of_date)
    required_universe = max(1, int(min_universe_code_count))
    conn = duckdb.connect(str(resolved_source), read_only=True)
    try:
        source_table, date_column = _resolve_universe_source(conn)
        if not source_table or not date_column:
            return {
                "ok": False,
                "ready": False,
                "reason": "universe_source_missing",
                "as_of_date": as_of_date_int,
                "min_universe_code_count": required_universe,
                "observed_code_count": 0,
                "latest_trade_date": None,
                "source_table": None,
                "source_date_column": None,
            }
        fallback_stats: dict[str, Any] | None = None
        selected_stats: dict[str, Any] | None = None
        for candidate_table, candidate_date_column in _UNIVERSE_SOURCE_PRIORITY:
            if not _table_exists(conn, candidate_table):
                continue
            columns = _column_map(conn, candidate_table)
            if candidate_date_column not in columns or "code" not in columns:
                continue
            stats = _load_universe_source_stats(
                conn,
                table_name=candidate_table,
                date_column=candidate_date_column,
                as_of_date=as_of_date_int,
            )
            if fallback_stats is None:
                fallback_stats = stats
            if int(stats["observed_code_count"]) > 0:
                selected_stats = stats
                break
    finally:
        conn.close()
    source_stats = selected_stats or fallback_stats or {
        "source_table": source_table,
        "source_date_column": date_column,
        "observed_code_count": 0,
        "latest_trade_date": None,
    }
    observed_code_count = int(source_stats["observed_code_count"])
    latest_trade_date = source_stats["latest_trade_date"]
    reason = "ready"
    ready = True
    if latest_trade_date is None:
        reason = "universe_source_empty"
        ready = False
    elif observed_code_count <= 0:
        reason = "source_universe_missing_date"
        ready = False
    elif observed_code_count < required_universe:
        reason = "source_universe_too_small"
        ready = False
    return {
        "ok": ready,
        "ready": ready,
        "reason": reason,
        "as_of_date": as_of_date_int,
        "min_universe_code_count": required_universe,
        "observed_code_count": observed_code_count,
        "latest_trade_date": latest_trade_date,
        "source_table": source_stats["source_table"],
        "source_date_column": source_stats["source_date_column"],
    }


def create_source_snapshot(
    *,
    source_db_path: str | None = None,
    snapshot_root: str | None = None,
    label: str = "analysis",
    keep_latest: int = SNAPSHOT_KEEP_LATEST,
) -> dict[str, Any]:
    resolved_source = resolve_source_db_path(source_db_path)
    if not resolved_source.exists():
        raise FileNotFoundError(f"source_db_not_found:{resolved_source}")
    root = Path(str(snapshot_root)).expanduser().resolve() if snapshot_root else _default_snapshot_root()
    root.mkdir(parents=True, exist_ok=True)
    snapshot_id = _snapshot_name(label=label)
    snapshot_db_path = root / f"{snapshot_id}.duckdb"
    source_wal = _wal_path(resolved_source)
    clone_conn = duckdb.connect()
    try:
        clone_conn.execute(f"ATTACH '{resolved_source.as_posix()}' AS source_db (READ_ONLY)")
        clone_conn.execute(f"ATTACH '{snapshot_db_path.as_posix()}' AS snapshot_db")
        clone_conn.execute("COPY FROM DATABASE source_db TO snapshot_db")
        clone_conn.execute("USE snapshot_db")
        feature_frame_payload = materialize_feature_frame_daily(clone_conn)
    finally:
        clone_conn.close()
    snapshot_wal_path = _wal_path(snapshot_db_path)
    wal_copied = snapshot_wal_path.exists()

    metadata = {
        "snapshot_id": snapshot_id,
        "label": label,
        "created_at": _utcnow().isoformat(),
        "source_db_path": str(resolved_source),
        "snapshot_db_path": str(snapshot_db_path),
        "source_wal_path": str(source_wal) if source_wal.exists() else None,
        "snapshot_wal_path": str(snapshot_wal_path) if wal_copied else None,
        "source_size": resolved_source.stat().st_size,
        "snapshot_size": snapshot_db_path.stat().st_size,
        "wal_copied": wal_copied,
        "feature_frame": feature_frame_payload,
    }
    metadata_path = root / f"{snapshot_id}.json"
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    _cleanup_old_snapshots(snapshot_root=root, keep_latest=keep_latest)
    return metadata
