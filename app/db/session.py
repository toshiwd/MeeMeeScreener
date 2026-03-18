from __future__ import annotations

import os
import random
import subprocess
import sys
import time
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI

from app.core.config import config
from app.db.schema import ensure_schema, init_extra_schemas, init_schema

_OPEN_LOCK: Any = None
_SCHEMA_STATE_LOCK: Any = None
_SCHEMA_READY_FOR_DB: str | None = None
_DB_OPEN_MODE_BY_PATH: dict[str, str] = {}
_DB_ACCESS_LOCKS: dict[str, Any] = {}
_DB_ACCESS_LOCKS_GUARD: Any = None
_CONNECT_STATS_LOCK: Any = None
_CONNECT_STATS: dict[str, Any] = {
    "open_calls": 0,
    "open_attempts": 0,
    "open_success": 0,
    "open_failures": 0,
    "transient_retries": 0,
    "ro_to_rw_fallbacks": 0,
    "last_error": None,
    "last_error_at_epoch_ms": None,
}


def _ensure_locks() -> None:
    import threading

    global _OPEN_LOCK, _SCHEMA_STATE_LOCK, _DB_ACCESS_LOCKS_GUARD, _CONNECT_STATS_LOCK
    if _OPEN_LOCK is None:
        _OPEN_LOCK = threading.Lock()
    if _SCHEMA_STATE_LOCK is None:
        _SCHEMA_STATE_LOCK = threading.Lock()
    if _DB_ACCESS_LOCKS_GUARD is None:
        _DB_ACCESS_LOCKS_GUARD = threading.Lock()
    if _CONNECT_STATS_LOCK is None:
        _CONNECT_STATS_LOCK = threading.Lock()


def _inc_connect_stat(key: str, delta: int = 1) -> None:
    _ensure_locks()
    with _CONNECT_STATS_LOCK:
        _CONNECT_STATS[key] = int(_CONNECT_STATS.get(key) or 0) + int(delta)


def _set_last_connect_error(exc: Exception) -> None:
    _ensure_locks()
    with _CONNECT_STATS_LOCK:
        _CONNECT_STATS["last_error"] = str(exc)
        _CONNECT_STATS["last_error_at_epoch_ms"] = int(time.time() * 1000)


def get_connect_stats() -> dict[str, Any]:
    _ensure_locks()
    with _CONNECT_STATS_LOCK:
        return dict(_CONNECT_STATS)


def _is_transient_duckdb_open_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    if not msg:
        return False
    keywords = (
        "already open",
        "used by",
        "cannot open file",
        "database is locked",
        "different configuration",
        "unique file handle conflict",
        "cannot attach",
    )
    return any(key in msg for key in keywords)


def is_transient_duckdb_error(exc: Exception) -> bool:
    return _is_transient_duckdb_open_error(exc)


def _connect_retry_wait_sec(default_sec: float = 1.0) -> float:
    raw = os.getenv("MEEMEE_DB_CONNECT_RETRY_SEC")
    try:
        value = float(raw) if raw is not None else float(default_sec)
    except (TypeError, ValueError):
        value = float(default_sec)
    return max(0.1, min(10.0, value))


def _read_only_connections_enabled() -> bool:
    raw = os.getenv("MEEMEE_ENABLE_DUCKDB_READ_ONLY")
    if raw is not None:
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}
    # DuckDB on Windows is prone to "different configuration" errors when
    # the same file is opened with mixed read-only/read-write modes.
    return os.name != "nt"


def _normalize_db_path(db_path: str) -> str:
    raw = str(db_path or "").strip() or str(config.DB_PATH)
    try:
        resolved = Path(raw).expanduser().resolve(strict=False)
        normalized = str(resolved)
    except Exception:
        normalized = os.path.abspath(raw)
    if os.name == "nt":
        normalized = os.path.normcase(os.path.normpath(normalized))
    return normalized


def _remember_db_open_mode(db_path: str, mode: str) -> None:
    current = _DB_OPEN_MODE_BY_PATH.get(db_path)
    if current == "rw":
        return
    _DB_OPEN_MODE_BY_PATH[db_path] = "ro" if mode == "ro" else "rw"


def _serialize_db_access_enabled() -> bool:
    raw = os.getenv("MEEMEE_SERIALIZE_DUCKDB_ACCESS", "1")
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _get_db_access_lock(db_path: str) -> Any | None:
    if not _serialize_db_access_enabled():
        return None
    _ensure_locks()
    normalized = _normalize_db_path(db_path)
    with _DB_ACCESS_LOCKS_GUARD:
        lock = _DB_ACCESS_LOCKS.get(normalized)
        if lock is None:
            import threading

            lock = threading.RLock()
            _DB_ACCESS_LOCKS[normalized] = lock
        return lock


def _try_acquire_access_lock(lock: Any | None, timeout_sec: float) -> bool:
    if lock is None:
        return True
    timeout_sec = max(0.0, float(timeout_sec))
    try:
        if timeout_sec <= 0.0:
            return bool(lock.acquire(blocking=False))
        return bool(lock.acquire(timeout=timeout_sec))
    except TypeError:
        if timeout_sec <= 0.0:
            return bool(lock.acquire(False))
        return bool(lock.acquire(True))


def _connect_with_retry_path(
    *,
    db_path: str,
    max_wait_sec: float = 1.0,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    _ensure_locks()
    _inc_connect_stat("open_calls")
    db_path = _normalize_db_path(db_path)
    deadline = time.time() + _connect_retry_wait_sec(max_wait_sec)
    attempt = 0
    while True:
        attempt += 1
        _inc_connect_stat("open_attempts")
        try:
            with _OPEN_LOCK:
                known_mode = _DB_OPEN_MODE_BY_PATH.get(db_path)
                open_read_only = bool(
                    read_only
                    and _read_only_connections_enabled()
                    and known_mode != "rw"
                )
                if open_read_only:
                    try:
                        conn = duckdb.connect(db_path, read_only=True)
                        _remember_db_open_mode(db_path, "ro")
                        _inc_connect_stat("open_success")
                        return conn
                    except Exception as exc:
                        # Same-process mixed open mode. Keep reads available.
                        if "different configuration" in str(exc).lower():
                            conn = duckdb.connect(db_path)
                            _remember_db_open_mode(db_path, "rw")
                            _inc_connect_stat("ro_to_rw_fallbacks")
                            _inc_connect_stat("open_success")
                            return conn
                        raise
                conn = duckdb.connect(db_path)
                _remember_db_open_mode(db_path, "rw")
                _inc_connect_stat("open_success")
                return conn
        except Exception as exc:
            if _is_transient_duckdb_open_error(exc) and time.time() < deadline:
                _inc_connect_stat("transient_retries")
                # Jitter helps reduce synchronized retries under burst access.
                sleep_sec = min(0.25, 0.03 * (2 ** min(attempt, 4))) + random.uniform(0.0, 0.015)
                time.sleep(sleep_sec)
                continue
            _inc_connect_stat("open_failures")
            _set_last_connect_error(exc)
            raise


def _connect_with_retry(max_wait_sec: float = 1.0) -> duckdb.DuckDBPyConnection:
    global _SCHEMA_READY_FOR_DB
    _ensure_locks()

    deadline = time.time() + _connect_retry_wait_sec(max_wait_sec)
    db_path = _normalize_db_path(str(config.DB_PATH))
    while True:
        try:
            conn = _connect_with_retry_path(db_path=db_path, max_wait_sec=max_wait_sec, read_only=False)
            needs_schema = False
            with _SCHEMA_STATE_LOCK:
                needs_schema = _SCHEMA_READY_FOR_DB != db_path
            if needs_schema:
                try:
                    ensure_schema(conn)
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    raise
                with _SCHEMA_STATE_LOCK:
                    _SCHEMA_READY_FOR_DB = db_path
            return conn
        except Exception as exc:
            if _is_transient_duckdb_open_error(exc) and time.time() < deadline:
                time.sleep(0.05)
                continue
            raise


class _ConnContext:
    def __init__(self):
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._access_lock: Any = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        db_path = _normalize_db_path(str(config.DB_PATH))
        self._access_lock = _get_db_access_lock(db_path)
        if self._access_lock is not None:
            self._access_lock.acquire()
        try:
            self._conn = _connect_with_retry(max_wait_sec=2.5)
        except Exception:
            if self._access_lock is not None:
                self._access_lock.release()
                self._access_lock = None
            raise
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        finally:
            if self._access_lock is not None:
                self._access_lock.release()
                self._access_lock = None
        return False


def get_conn() -> _ConnContext:
    return _ConnContext()


class _PathConnContext:
    def __init__(self, *, db_path: str, timeout_sec: float = 1.0, read_only: bool = False):
        self._db_path = _normalize_db_path(db_path)
        self._timeout_sec = max(0.0, float(timeout_sec))
        self._read_only = bool(read_only)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._access_lock: Any = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        self._access_lock = _get_db_access_lock(self._db_path)
        if self._access_lock is not None:
            self._access_lock.acquire()
        try:
            self._conn = _connect_with_retry_path(
                db_path=self._db_path,
                max_wait_sec=self._timeout_sec,
                read_only=self._read_only,
            )
        except Exception:
            if self._access_lock is not None:
                self._access_lock.release()
                self._access_lock = None
            raise
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        finally:
            if self._access_lock is not None:
                self._access_lock.release()
                self._access_lock = None
        return False


def get_conn_for_path(db_path: str, *, timeout_sec: float = 1.0, read_only: bool = False) -> _PathConnContext:
    return _PathConnContext(db_path=db_path, timeout_sec=timeout_sec, read_only=read_only)


class _TryConnContext:
    def __init__(self, timeout_sec: float = 0.0):
        self._timeout_sec = max(0.0, float(timeout_sec))
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._access_lock: Any = None

    def __enter__(self) -> duckdb.DuckDBPyConnection | None:
        db_path = _normalize_db_path(str(config.DB_PATH))
        self._access_lock = _get_db_access_lock(db_path)
        if not _try_acquire_access_lock(self._access_lock, self._timeout_sec):
            self._access_lock = None
            return None
        try:
            self._conn = _connect_with_retry(max_wait_sec=self._timeout_sec)
        except Exception:
            self._conn = None
            if self._access_lock is not None:
                self._access_lock.release()
                self._access_lock = None
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        finally:
            if self._access_lock is not None:
                self._access_lock.release()
                self._access_lock = None
        return False


def try_get_conn(timeout_sec: float = 0.0) -> _TryConnContext:
    return _TryConnContext(timeout_sec=timeout_sec)


class _TryPathConnContext:
    def __init__(self, *, db_path: str, timeout_sec: float = 0.0, read_only: bool = False):
        self._db_path = _normalize_db_path(db_path)
        self._timeout_sec = max(0.0, float(timeout_sec))
        self._read_only = bool(read_only)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._access_lock: Any = None

    def __enter__(self) -> duckdb.DuckDBPyConnection | None:
        self._access_lock = _get_db_access_lock(self._db_path)
        if not _try_acquire_access_lock(self._access_lock, self._timeout_sec):
            self._access_lock = None
            return None
        try:
            self._conn = _connect_with_retry_path(
                db_path=self._db_path,
                max_wait_sec=self._timeout_sec,
                read_only=self._read_only,
            )
        except Exception:
            self._conn = None
            if self._access_lock is not None:
                self._access_lock.release()
                self._access_lock = None
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        finally:
            if self._access_lock is not None:
                self._access_lock.release()
                self._access_lock = None
        return False


def try_get_conn_for_path(
    db_path: str,
    *,
    timeout_sec: float = 0.0,
    read_only: bool = False,
) -> _TryPathConnContext:
    return _TryPathConnContext(db_path=db_path, timeout_sec=timeout_sec, read_only=read_only)


def _acquire_lock() -> tuple[object | None, Any]:
    lock_path = config.LOCK_FILE_PATH
    lock_handle = None

    try:
        if lock_path.exists():
            try:
                old_pid_str = lock_path.read_text(errors="replace").strip()
                if old_pid_str.isdigit():
                    old_pid = int(old_pid_str)
                    try:
                        cmd = f'tasklist /fi "PID eq {old_pid}" /fo csv /nh'
                        # Use errors='replace' to handle any Windows locale (cp932, cp1252, etc.)
                        output = subprocess.check_output(
                            cmd, shell=True, stderr=subprocess.DEVNULL
                        ).decode(errors="replace")
                        if str(old_pid) in output:
                            raise RuntimeError(f"Another instance (PID {old_pid}) is running.")
                    except RuntimeError:
                        raise  # Propagate the "already running" error
                    except Exception:
                        pass  # tasklist unavailable or failed 驕ｯ・ｶ郢晢ｽｻallow startup
                lock_path.unlink(missing_ok=True)
            except RuntimeError:
                raise
            except Exception:
                pass

        lock_handle = open(lock_path, "w")
        lock_handle.write(str(os.getpid()))
        lock_handle.flush()
        return lock_handle, lock_path
    except OSError as exc:
        print(f"[startup] FATAL: Could not acquire lock: {exc}", file=sys.stderr)
        raise RuntimeError("Could not acquire lock.") from exc
    except Exception:
        if lock_handle:
            try:
                lock_handle.close()
            except Exception:
                pass
        raise


def _release_lock(lock_handle: object | None, lock_path: Any) -> None:
    if lock_handle:
        try:
            lock_handle.close()
        except Exception:
            pass
    try:
        if lock_path and lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    lock_handle = None
    lock_path = None
    try:
        lock_handle, lock_path = _acquire_lock()
        try:
            init_schema()
            init_extra_schemas()
            _auto_sync_trade_csvs_if_needed()
        except Exception as exc:
            print(f"[startup] FATAL: An exception occurred during startup: {exc}", file=sys.stderr)
            traceback.print_exc()
            raise
        yield
    finally:
        _release_lock(lock_handle, lock_path)


def _auto_sync_trade_csvs_if_needed() -> None:
    """
    Best-effort startup sync for trade CSVs -> DuckDB.

    Why:
    - After the main split, parts of the app relied on trade CSVs already sitting in the data dir.
    - If `trade_events` is empty, the Positions UI will show nothing.
    - We only auto-sync when the DB has zero trade rows to avoid overwriting user data.
    """
    if os.getenv("AUTO_SYNC_TRADES_ON_STARTUP", "1") != "1":
        return

    # Avoid importing this at module import time (circular imports).
    try:
        from app.backend.core.csv_sync import resolve_trade_csv_paths, sync_trade_csvs
    except Exception:
        return

    try:
        with get_conn() as conn:
            try:
                count = conn.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0]
            except Exception:
                count = 0
        if int(count or 0) > 0:
            return

        candidates = [path for path in resolve_trade_csv_paths() if path and os.path.isfile(path)]
        if not candidates:
            return

        result = sync_trade_csvs()
        imported = int(result.get("imported") or 0)
        if imported > 0:
            print(f"[startup] Auto-synced trade CSVs: {imported} rows")
    except Exception:
        # Never block startup on trade sync issues.
        return
