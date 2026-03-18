from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from app.backend.api.dependencies import get_screener_repo, get_stock_repo
from app.core.config import config
from app.db.session import get_conn_for_path

logger = logging.getLogger(__name__)
_CACHE_LOCK = threading.Lock()
_SNAPSHOT_CACHE: dict[str, dict[str, Any]] = {}


def _slot_for_limit(limit: int) -> str:
    return f"daily:{max(1, int(limit or 260))}"


def _resolve_db_path(
    *,
    db_path: str | None = None,
    screener_repo: Any | None = None,
    stock_repo: Any | None = None,
) -> str:
    candidate = (
        db_path
        or getattr(screener_repo, "db_path", None)
        or getattr(stock_repo, "_db_path", None)
        or str(config.DB_PATH)
    )
    return str(Path(candidate).expanduser().resolve(strict=False))


def invalidate_screener_snapshot_cache(limit: int | None = None) -> None:
    with _CACHE_LOCK:
        if limit is None:
            _SNAPSHOT_CACHE.clear()
            return
        _SNAPSHOT_CACHE.pop(_slot_for_limit(limit), None)


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS screener_snapshot_state (
            slot TEXT PRIMARY KEY,
            generation TEXT,
            as_of TEXT,
            updated_at TIMESTAMP,
            last_attempt_at TIMESTAMP,
            payload_json TEXT,
            row_count INTEGER,
            source TEXT,
            build_ms INTEGER,
            last_status TEXT,
            last_error TEXT
        )
        """
    )
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS generation TEXT")
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS as_of TEXT")
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP")
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMP")
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS payload_json TEXT")
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS row_count INTEGER")
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS source TEXT")
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS build_ms INTEGER")
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS last_status TEXT")
    conn.execute("ALTER TABLE screener_snapshot_state ADD COLUMN IF NOT EXISTS last_error TEXT")


def _load_row(conn: duckdb.DuckDBPyConnection, slot: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT slot, generation, as_of, updated_at, last_attempt_at, payload_json, row_count,
               source, build_ms, last_status, last_error
        FROM screener_snapshot_state
        WHERE slot = ?
        """,
        [slot],
    ).fetchone()
    if not row:
        return None
    return {
        "slot": row[0],
        "generation": row[1],
        "as_of": row[2],
        "updated_at": row[3],
        "last_attempt_at": row[4],
        "payload_json": row[5],
        "row_count": int(row[6] or 0),
        "source": row[7],
        "build_ms": int(row[8] or 0),
        "last_status": row[9],
        "last_error": row[10],
    }


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _decode_snapshot_row(row: dict[str, Any]) -> dict[str, Any]:
    payload_raw = row.get("payload_json")
    items: list[dict[str, Any]] = []
    if isinstance(payload_raw, str) and payload_raw.strip():
        try:
            decoded = json.loads(payload_raw)
            if isinstance(decoded, list):
                items = [dict(item) for item in decoded if isinstance(item, dict)]
        except json.JSONDecodeError:
            logger.warning("Failed to decode screener snapshot payload for slot=%s", row.get("slot"))
    stale = bool(items) and str(row.get("last_status") or "ready") != "ready"
    return {
        "items": items,
        "stale": stale,
        "asOf": row.get("as_of"),
        "updatedAt": _iso_or_none(row.get("updated_at")),
        "generation": row.get("generation"),
        "lastError": row.get("last_error"),
        "rowCount": int(row.get("row_count") or len(items)),
        "source": row.get("source"),
        "lastAttemptAt": _iso_or_none(row.get("last_attempt_at")),
        "buildMs": int(row.get("build_ms") or 0),
        "buildFailed": False,
    }


def _cache_put(slot: str, payload: dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _SNAPSHOT_CACHE[slot] = dict(payload)


def _cache_get(slot: str) -> dict[str, Any] | None:
    with _CACHE_LOCK:
        cached = _SNAPSHOT_CACHE.get(slot)
        return dict(cached) if cached else None


def _derive_as_of(items: list[dict[str, Any]]) -> str | None:
    latest: str | None = None
    for item in items:
        raw = item.get("asOf")
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        if latest is None or text > latest:
            latest = text
    return latest


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _compute_snapshot_items(limit: int, screener_repo: Any, stock_repo: Any) -> list[dict[str, Any]]:
    from app.backend.api.routers.grid import _compute_live_screener_rows

    return _compute_live_screener_rows(limit=limit, screener_repo=screener_repo, stock_repo=stock_repo)


def inspect_screener_snapshot(limit: int = 260, *, db_path: str | None = None) -> dict[str, Any]:
    resolved_db_path = _resolve_db_path(db_path=db_path)
    slot = _slot_for_limit(limit)
    with get_conn_for_path(resolved_db_path, timeout_sec=2.5, read_only=False) as conn:
        _ensure_table(conn)
        row = _load_row(conn, slot)
    if not row:
        return {"exists": False, "slot": slot}
    payload = _decode_snapshot_row(row)
    payload["exists"] = True
    payload["slot"] = slot
    return payload


def refresh_screener_snapshot(
    *,
    limit: int = 260,
    source: str = "manual",
    db_path: str | None = None,
    screener_repo: Any | None = None,
    stock_repo: Any | None = None,
) -> dict[str, Any]:
    resolved_limit = max(1, int(limit or 260))
    resolved_screener_repo = screener_repo or get_screener_repo()
    resolved_stock_repo = stock_repo or get_stock_repo()
    resolved_db_path = _resolve_db_path(
        db_path=db_path,
        screener_repo=resolved_screener_repo,
        stock_repo=resolved_stock_repo,
    )
    slot = _slot_for_limit(resolved_limit)
    started_at = datetime.now(timezone.utc)
    build_started = time.perf_counter()
    try:
        items = _compute_snapshot_items(resolved_limit, resolved_screener_repo, resolved_stock_repo)
        generation = started_at.strftime("%Y%m%dT%H%M%S%fZ")
        as_of = _derive_as_of(items)
        payload_json = json.dumps(items, ensure_ascii=False, separators=(",", ":"), default=_json_default)
        build_ms = int((time.perf_counter() - build_started) * 1000)
        completed_at = datetime.now(timezone.utc)
        with get_conn_for_path(resolved_db_path, timeout_sec=2.5, read_only=False) as conn:
            _ensure_table(conn)
            conn.execute(
                """
                INSERT INTO screener_snapshot_state (
                    slot, generation, as_of, updated_at, last_attempt_at, payload_json,
                    row_count, source, build_ms, last_status, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', NULL)
                ON CONFLICT(slot) DO UPDATE SET
                    generation = excluded.generation,
                    as_of = excluded.as_of,
                    updated_at = excluded.updated_at,
                    last_attempt_at = excluded.last_attempt_at,
                    payload_json = excluded.payload_json,
                    row_count = excluded.row_count,
                    source = excluded.source,
                    build_ms = excluded.build_ms,
                    last_status = 'ready',
                    last_error = NULL
                """,
                [slot, generation, as_of, completed_at, completed_at, payload_json, len(items), source, build_ms],
            )
            row = _load_row(conn, slot)
        assert row is not None
        payload = _decode_snapshot_row(row)
        payload["buildFailed"] = False
        _cache_put(slot, payload)
        return payload
    except Exception as exc:
        build_ms = int((time.perf_counter() - build_started) * 1000)
        failed_at = datetime.now(timezone.utc)
        logger.warning("screener snapshot refresh failed source=%s limit=%s: %s", source, resolved_limit, exc)
        with get_conn_for_path(resolved_db_path, timeout_sec=2.5, read_only=False) as conn:
            _ensure_table(conn)
            conn.execute(
                """
                INSERT INTO screener_snapshot_state (
                    slot, generation, as_of, updated_at, last_attempt_at, payload_json,
                    row_count, source, build_ms, last_status, last_error
                )
                VALUES (?, NULL, NULL, NULL, ?, NULL, 0, ?, ?, 'error', ?)
                ON CONFLICT(slot) DO UPDATE SET
                    last_attempt_at = excluded.last_attempt_at,
                    source = excluded.source,
                    build_ms = excluded.build_ms,
                    last_status = CASE
                        WHEN screener_snapshot_state.payload_json IS NULL OR screener_snapshot_state.payload_json = '' THEN 'error'
                        ELSE 'stale'
                    END,
                    last_error = excluded.last_error
                """,
                [slot, failed_at, source, build_ms, str(exc)],
            )
            row = _load_row(conn, slot)
        if row:
            payload = _decode_snapshot_row(row)
            payload["buildFailed"] = True
            payload["lastError"] = str(exc)
            if payload.get("items"):
                payload["stale"] = True
                _cache_put(slot, payload)
                return payload
        raise


def get_screener_snapshot_response(
    *,
    limit: int = 260,
    force_refresh: bool = False,
    db_path: str | None = None,
    screener_repo: Any | None = None,
    stock_repo: Any | None = None,
) -> dict[str, Any]:
    resolved_limit = max(1, int(limit or 260))
    slot = _slot_for_limit(resolved_limit)
    if not force_refresh:
        cached = _cache_get(slot)
        if cached:
            return cached

    resolved_db_path = _resolve_db_path(db_path=db_path, screener_repo=screener_repo, stock_repo=stock_repo)
    with get_conn_for_path(resolved_db_path, timeout_sec=2.5, read_only=False) as conn:
        _ensure_table(conn)
        row = _load_row(conn, slot)
    if row and not force_refresh:
        payload = _decode_snapshot_row(row)
        if payload.get("items"):
            _cache_put(slot, payload)
            return payload
    payload = refresh_screener_snapshot(
        limit=resolved_limit,
        source="api_force_refresh" if force_refresh else "api_bootstrap",
        db_path=resolved_db_path,
        screener_repo=screener_repo,
        stock_repo=stock_repo,
    )
    return payload
