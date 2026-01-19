from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
import calendar
import csv
import glob
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid

from fastapi import Body, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

# Ensure current directory is in path for local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db import get_conn, init_schema
from box_detector import detect_boxes
from similarity import SimilarityService, SearchResult
from events import fetch_earnings_snapshot, fetch_rights_snapshot, jst_now
from positions import parse_rakuten_csv, parse_sbi_csv, rebuild_positions
from import_positions import process_import_rakuten, process_import_sbi
from position_engine import get_events

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        print("[startup] Initializing database schema...")
        init_schema()
        print("[startup] Initializing favorites schema...")
        _init_favorites_schema()
        print("[startup] Initializing practice schema...")
        _init_practice_schema()
        try:
            print("[startup] Loading similarity artifacts...")
            _similarity_service.load_artifacts()
        except Exception as e:
            print(f"[startup] Warning: Failed to load similarity artifacts: {e}", file=sys.stderr)
        print("[startup] All schemas initialized successfully.")
    except Exception as exc:
        print(f"[startup] FATAL: An exception occurred during schema initialization: {exc}", file=sys.stderr)
        traceback.print_exc()
        # Re-raise the exception to ensure uvicorn exits with an error
        raise exc
    
    yield
    # Shutdown
    print("[shutdown] Application shutting down.")

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_PAN_VBS_PATH = os.path.join(REPO_ROOT, "tools", "export_pan.vbs")
DEFAULT_PAN_CODE_PATH = os.path.join(REPO_ROOT, "tools", "code.txt")
DEFAULT_PAN_OUT_DIR = os.path.join(REPO_ROOT, "data", "txt")
APP_VERSION = os.getenv("APP_VERSION", "dev")
APP_ENV = os.getenv("APP_ENV") or os.getenv("ENV") or "dev"
DEBUG = os.getenv("DEBUG", "0") == "1"


def resolve_data_dir() -> str:
    env = os.getenv("PAN_OUT_TXT_DIR") or os.getenv("TXT_DATA_DIR")
    if env:
        return os.path.abspath(env)
    return os.path.abspath(DEFAULT_PAN_OUT_DIR)


DATA_DIR = resolve_data_dir()
STATIC_DIR = os.path.abspath(os.getenv("STATIC_DIR") or os.path.join(os.path.dirname(__file__), "static"))
DEFAULT_TRADE_RAKUTEN_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "楽天証券取引履歴.csv")
)
DEFAULT_TRADE_SBI_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "SBI証券取引履歴.csv")
)


def resolve_trade_csv_paths() -> list[str]:
    env = os.getenv("TRADE_CSV_PATH")
    if env:
        parts = [p.strip() for p in re.split(r"[;,\\n]+", env) if p.strip()]
        return [os.path.abspath(part) for part in parts]
    trade_dir = os.getenv("TRADE_CSV_DIR")
    if trade_dir:
        base_dir = os.path.abspath(trade_dir)
        candidates = [
            os.path.join(base_dir, "楽天証券取引履歴.csv"),
            os.path.join(base_dir, "SBI証券取引履歴.csv")
        ]
        return [path for path in candidates if os.path.isfile(path)] or candidates
    
    paths: list[str] = []
    
    # Check DATA_DIR
    if DATA_DIR:
        candidates = [
            os.path.join(DATA_DIR, "楽天証券取引履歴.csv"),
            os.path.join(DATA_DIR, "SBI証券取引履歴.csv")
        ]
        for p in candidates:
            if os.path.isfile(p):
                paths.append(p)
    
    # Check user AppData directly (Fix for dev environment)
    user_data = r"C:\Users\enish\AppData\Local\MeeMeeScreener\data"
    if os.path.isdir(user_data):
        rakuten_path = os.path.join(user_data, "楽天証券取引履歴.csv")
        sbi_path = os.path.join(user_data, "SBI証券取引履歴.csv")
        if os.path.isfile(rakuten_path):
            paths.append(rakuten_path)
        if os.path.isfile(sbi_path):
            paths.append(sbi_path)

    if os.path.isfile(DEFAULT_TRADE_RAKUTEN_PATH):
        paths.append(DEFAULT_TRADE_RAKUTEN_PATH)
    if os.path.isfile(DEFAULT_TRADE_SBI_PATH):
        paths.append(DEFAULT_TRADE_SBI_PATH)
        
    # Dedup
    unique_paths = list(set(paths))
    if not unique_paths and not paths: # if empty, fallback
        unique_paths.append(DEFAULT_TRADE_RAKUTEN_PATH)
        
    return unique_paths


def resolve_trade_csv_dir() -> str:
    env = os.getenv("TRADE_CSV_DIR")
    if env:
        return os.path.abspath(env)
    return os.path.abspath(os.path.join(REPO_ROOT, "data"))


TRADE_CSV_PATHS = resolve_trade_csv_paths()
USE_CODE_TXT = os.getenv("USE_CODE_TXT", "0") == "1"
DEFAULT_DB_PATH = os.getenv("STOCKS_DB_PATH", os.path.join(os.path.dirname(__file__), "stocks.duckdb"))
RANK_CONFIG_PATH = os.getenv("RANK_CONFIG_PATH", os.path.join(os.path.dirname(__file__), "rank_config.json"))
FAVORITES_DB_PATH = os.getenv(
    "FAVORITES_DB_PATH", os.path.join(os.path.dirname(__file__), "favorites.sqlite")
)
PRACTICE_DB_PATH = os.getenv(
    "PRACTICE_DB_PATH", os.path.join(os.path.dirname(__file__), "practice.sqlite")
)
PAN_EXPORT_VBS_PATH = os.path.abspath(
    os.getenv("PAN_EXPORT_VBS_PATH") or os.getenv("UPDATE_VBS_PATH") or DEFAULT_PAN_VBS_PATH
)
PAN_CODE_TXT_PATH = os.path.abspath(
    os.getenv("PAN_CODE_TXT_PATH") or DEFAULT_PAN_CODE_PATH
)
PAN_OUT_TXT_DIR = os.path.abspath(
    os.getenv("PAN_OUT_TXT_DIR") or DEFAULT_PAN_OUT_DIR
)
UPDATE_VBS_PATH = PAN_EXPORT_VBS_PATH
INGEST_SCRIPT_PATH = os.getenv(
    "INGEST_SCRIPT_PATH",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "ingest_txt.py"))
)
UPDATE_STATE_PATH = os.getenv(
    "UPDATE_STATE_PATH",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "update_state.json"))
)
SPLIT_SUSPECTS_PATH = os.path.abspath(os.path.join(DATA_DIR, "_split_suspects.csv"))
WATCHLIST_TRASH_DIR = os.path.abspath(os.path.join(DATA_DIR, "trash"))
WATCHLIST_TRASH_PATTERNS = [
    pattern
    for pattern in re.split(
        r"[;\n]+",
        os.getenv(
            "WATCHLIST_TRASH_PATTERNS",
            os.path.join(REPO_ROOT, "data", "csv", "{code}*.csv")
            + ";"
            + os.path.join(REPO_ROOT, "data", "txt", "{code}*.txt")
        )
    )
    if pattern.strip()
]
WATCHLIST_CODE_RE = re.compile(r"^\d{4}[A-Z]?$")
_watchlist_lock = threading.Lock()


def _build_name_map_from_txt() -> dict[str, str]:
    if not os.path.isdir(PAN_OUT_TXT_DIR):
        return {}
    name_map: dict[str, str] = {}
    for filename in os.listdir(PAN_OUT_TXT_DIR):
        if not filename.endswith(".txt") or filename.lower() == "code.txt":
            continue
        base = os.path.splitext(filename)[0]
        if "_" not in base:
            continue
        code, name = base.split("_", 1)
        code = code.strip()
        name = name.strip()
        if code and name and code not in name_map:
            name_map[code] = name
    return name_map


_trade_cache = {"key": None, "rows": [], "warnings": []}
_screener_cache = {"mtime": None, "rows": []}
_rank_cache = {"mtime": None, "config_mtime": None, "weekly": {}, "monthly": {}}
_rank_config_cache = {"mtime": None, "config": None}
_update_txt_lock = threading.Lock()
_update_txt_status = {
    "running": False,
    "phase": "idle",
    "started_at": None,
    "finished_at": None,
    "processed": 0,
    "total": 0,
    "summary": {},
    "error": None,
    "stdout_tail": [],
    "last_updated_at": None
}
_similarity_service = SimilarityService()
_similarity_refresh_lock = threading.Lock()
_similarity_refresh_status = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "mode": None
}
_events_refresh_lock = threading.Lock()
_events_refresh_timeout = timedelta(minutes=30)


def _run_similarity_refresh(mode: str) -> None:
    error = None
    try:
        _similarity_service.refresh_data(incremental=(mode == "incremental"))
    except Exception as exc:
        error = str(exc)
        print(f"Similarity Refresh Error: {exc}")
        traceback.print_exc()
    finally:
        with _similarity_refresh_lock:
            _similarity_refresh_status["running"] = False
            _similarity_refresh_status["finished_at"] = datetime.now().isoformat()
            _similarity_refresh_status["error"] = error


def _start_similarity_refresh(mode: str) -> bool:
    with _similarity_refresh_lock:
        if _similarity_refresh_status["running"]:
            return False
        _similarity_refresh_status["running"] = True
        _similarity_refresh_status["started_at"] = datetime.now().isoformat()
        _similarity_refresh_status["finished_at"] = None
        _similarity_refresh_status["error"] = None
        _similarity_refresh_status["mode"] = mode
    thread = threading.Thread(target=_run_similarity_refresh, args=(mode,), daemon=True)
    thread.start()
    return True


def _get_favorites_conn():
    conn = sqlite3.connect(FAVORITES_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_favorites_schema() -> None:
    with _get_favorites_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS favorites (
                code TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )


def _get_practice_conn():
    conn = sqlite3.connect(PRACTICE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_practice_schema() -> None:
    with _get_practice_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS practice_sessions (
                session_id TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                cursor_time INTEGER,
                max_unlocked_time INTEGER,
                lot_size INTEGER,
                range_months INTEGER,
                trades TEXT,
                notes TEXT,
                ui_state TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        _ensure_practice_column(conn, "practice_sessions", "end_date", "TEXT")
        _ensure_practice_column(conn, "practice_sessions", "cursor_time", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "max_unlocked_time", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "lot_size", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "range_months", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "ui_state", "TEXT")


def _ensure_practice_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    existing = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if any(row[1] == column for row in existing):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def _ensure_events_meta_row(conn) -> None:
    row = conn.execute("SELECT id FROM events_meta LIMIT 1").fetchone()
    if row:
        return
    conn.execute(
        """
        INSERT INTO events_meta (
            id,
            is_refreshing
        ) VALUES (
            1,
            FALSE
        );
        """
    )


def _normalize_meta_datetime(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _format_event_date(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return None
    return None


def _format_event_timestamp(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return None
    return None


def _load_events_meta(conn) -> dict:
    _ensure_events_meta_row(conn)
    row = conn.execute(
        """
        SELECT
            earnings_last_success_at,
            rights_last_success_at,
            last_error,
            last_attempt_at,
            is_refreshing,
            refresh_lock_job_id,
            refresh_lock_started_at
        FROM events_meta
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return {}
    return {
        "earnings_last_success_at": row[0],
        "rights_last_success_at": row[1],
        "last_error": row[2],
        "last_attempt_at": row[3],
        "is_refreshing": bool(row[4]) if row[4] is not None else False,
        "refresh_lock_job_id": row[5],
        "refresh_lock_started_at": row[6]
    }


def _is_events_lock_stale(started_at: object | None) -> bool:
    locked_at = _normalize_meta_datetime(started_at)
    if locked_at is None:
        return False
    return jst_now().replace(tzinfo=None) - locked_at >= _events_refresh_timeout


def _update_events_job(job_id: str, status: str, finished_at: datetime, error: str | None) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE events_refresh_jobs
            SET status = ?, finished_at = ?, error = ?
            WHERE job_id = ?
            """,
            [status, finished_at, error, job_id]
        )


def _run_events_refresh(job_id: str, reason: str | None) -> None:
    earnings_rows: list[dict] = []
    rights_rows: list[dict] = []
    errors: list[str] = []
    finished_at = None
    error_text = None
    try:
        earnings_rows = fetch_earnings_snapshot()
        if not earnings_rows:
            errors.append("earnings:no_rows")
    except Exception as exc:
        errors.append(f"earnings:{exc}")
    try:
        rights_rows = fetch_rights_snapshot()
        if not rights_rows:
            errors.append("rights:no_rows")
    except Exception as exc:
        errors.append(f"rights:{exc}")

    try:
        finished_at = jst_now().replace(tzinfo=None)
        error_text = "; ".join(errors) if errors else None

        with get_conn() as conn:
            _ensure_events_meta_row(conn)
            if earnings_rows:
                conn.execute("DELETE FROM earnings_planned WHERE source = 'JPX'")
                conn.executemany(
                    """
                    INSERT INTO earnings_planned (
                        code,
                        planned_date,
                        kind,
                        company_name,
                        source,
                        fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row.get("code"),
                            row.get("planned_date"),
                            row.get("kind"),
                            row.get("company_name"),
                            row.get("source"),
                            row.get("fetched_at")
                        )
                        for row in earnings_rows
                    ]
                )
                conn.execute(
                    "UPDATE events_meta SET earnings_last_success_at = ? WHERE id = 1",
                    [finished_at]
                )
            if rights_rows:
                conn.execute("DELETE FROM ex_rights WHERE source = 'JPX'")
                conn.executemany(
                    """
                    INSERT INTO ex_rights (
                        code,
                        ex_date,
                        record_date,
                        category,
                        last_rights_date,
                        source,
                        fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row.get("code"),
                            row.get("ex_date"),
                            row.get("record_date"),
                            row.get("category"),
                            row.get("last_rights_date"),
                            row.get("source"),
                            row.get("fetched_at")
                        )
                        for row in rights_rows
                    ]
                )
                conn.execute(
                    "UPDATE events_meta SET rights_last_success_at = ? WHERE id = 1",
                    [finished_at]
                )
            conn.execute(
                """
                UPDATE events_meta
                SET
                    last_error = ?,
                    last_attempt_at = ?,
                    is_refreshing = FALSE,
                    refresh_lock_job_id = NULL,
                    refresh_lock_started_at = NULL
                WHERE id = 1
                """,
                [error_text, finished_at]
            )
        _invalidate_screener_cache()
    except Exception as exc:
        finished_at = jst_now().replace(tzinfo=None)
        error_text = f"refresh_failed:{exc}"
        with get_conn() as conn:
            _ensure_events_meta_row(conn)
            conn.execute(
                """
                UPDATE events_meta
                SET
                    last_error = ?,
                    last_attempt_at = ?,
                    is_refreshing = FALSE,
                    refresh_lock_job_id = NULL,
                    refresh_lock_started_at = NULL
                WHERE id = 1
                """,
                [error_text, finished_at]
            )
        _invalidate_screener_cache()

    status = "success" if not error_text else "failed"
    _update_events_job(job_id, status, finished_at, error_text)


def _start_events_refresh(reason: str | None) -> str:
    with _events_refresh_lock:
        with get_conn() as conn:
            meta = _load_events_meta(conn)
            refreshing = bool(meta.get("is_refreshing"))
            lock_started_at = meta.get("refresh_lock_started_at")
            lock_job_id = meta.get("refresh_lock_job_id")
            if refreshing and lock_job_id and not _is_events_lock_stale(lock_started_at):
                return lock_job_id
            if refreshing:
                conn.execute(
                    """
                    UPDATE events_meta
                    SET
                        is_refreshing = FALSE,
                        refresh_lock_job_id = NULL,
                        refresh_lock_started_at = NULL
                    WHERE id = 1
                    """
                )
            job_id = uuid.uuid4().hex
            started_at = jst_now().replace(tzinfo=None)
            conn.execute(
                """
                UPDATE events_meta
                SET
                    is_refreshing = TRUE,
                    refresh_lock_job_id = ?,
                    refresh_lock_started_at = ?,
                    last_attempt_at = ?
                WHERE id = 1
                """,
                [job_id, started_at, started_at]
            )
            conn.execute(
                """
                INSERT INTO events_refresh_jobs (
                    job_id,
                    status,
                    reason,
                    started_at
                ) VALUES (?, ?, ?, ?)
                """,
                [job_id, "running", reason, started_at]
            )
    thread = threading.Thread(target=_run_events_refresh, args=(job_id, reason), daemon=True)
    thread.start()
    return job_id


def _normalize_code(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    match = re.search(r"\d{4}", text)
    if match:
        return match.group(0)
    return text.upper()


def _classify_exception(exc: Exception) -> tuple[int, str, str]:
    detail = str(exc)
    lower = detail.lower()
    db_missing = not os.path.isfile(DEFAULT_DB_PATH)
    if "io error" in lower or "failed to open" in lower or "cannot open" in lower:
        return 503, "DB_OPEN_FAILED", "Database open failed"
    if db_missing:
        return 503, "DATA_NOT_INITIALIZED", "Data not initialized"
    if (
        "no such table" in lower
        or "does not exist" in lower
        or "catalog error" in lower
        or "table with name" in lower
    ):
        return 503, "DATA_NOT_INITIALIZED", "Data not initialized"
    if isinstance(exc, sqlite3.Error):
        return 500, "SQLITE_ERROR", "Database error"
    return 500, "UNHANDLED_EXCEPTION", "Internal server error"


def _build_error_payload(exc: Exception, trace_id: str) -> dict:
    status_code, error_code, message = _classify_exception(exc)
    payload = {
        "trace_id": trace_id,
        "error_code": error_code,
        "message": message,
        "detail": str(exc)
    }
    if DEBUG:
        payload["stack"] = traceback.format_exc()
    return payload


def _load_favorite_codes() -> list[str]:
    with _get_favorites_conn() as conn:
        rows = conn.execute("SELECT code FROM favorites ORDER BY code").fetchall()
    return [row["code"] for row in rows]


def _load_favorite_items() -> list[dict]:
    codes = _load_favorite_codes()
    if not codes:
        return []
    names_by_code: dict[str, str] = {}
    placeholders = ",".join(["?"] * len(codes))
    with get_conn() as conn:
        rows = conn.execute(
            f"SELECT code, name FROM tickers WHERE code IN ({placeholders})",
            codes
        ).fetchall()
    for row in rows:
        code = str(row[0])
        name = row[1] or code
        names_by_code[code] = name
    return [{"code": code, "name": names_by_code.get(code, code)} for code in codes]


def find_code_txt_path(data_dir: str) -> str | None:
    if os.path.exists(PAN_CODE_TXT_PATH):
        return PAN_CODE_TXT_PATH
    return None


def _normalize_watch_code(raw: str | None) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    fullwidth = str.maketrans(
        "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    )
    text = text.translate(fullwidth)
    text = re.sub(r"\s+", "", text)
    text = text.upper()
    if not WATCHLIST_CODE_RE.match(text):
        return None
    return text


def _extract_code_from_line(line: str) -> str | None:
    stripped = line.strip()
    if not stripped:
        return None
    if stripped.startswith("#") or stripped.startswith("'"):
        return None
    token = re.split(r"[,\t ]+", stripped, maxsplit=1)[0]
    return _normalize_watch_code(token)


def _read_watchlist_lines(path: str) -> list[str]:
    for encoding in ("utf-8", "cp932"):
        try:
            with open(path, "r", encoding=encoding) as handle:
                return handle.read().splitlines()
        except OSError:
            continue
    return []


def _write_watchlist_lines(path: str, lines: list[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
        handle.write("\n")
    os.replace(tmp_path, path)


def _load_watchlist_codes(path: str) -> list[str]:
    lines = _read_watchlist_lines(path)
    seen: set[str] = set()
    codes: list[str] = []
    for line in lines:
        code = _extract_code_from_line(line)
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def _update_watchlist_file(path: str, code: str, remove: bool) -> bool:
    lines = _read_watchlist_lines(path)
    seen: set[str] = set()
    updated: list[str] = []
    removed = False
    for line in lines:
        parsed = _extract_code_from_line(line)
        if not parsed:
            updated.append(line)
            continue
        if parsed == code and remove:
            removed = True
            continue
        if parsed in seen:
            continue
        seen.add(parsed)
        updated.append(parsed)

    if not remove and code not in seen:
        updated.append(code)
    _write_watchlist_lines(path, updated)
    return removed


def _trash_watchlist_artifacts(code: str) -> tuple[str | None, list[str]]:
    if not WATCHLIST_TRASH_PATTERNS:
        return None, []
    trashed: list[str] = []
    token = datetime.now().strftime("%Y%m%d_%H%M%S")
    trash_dir = os.path.join(WATCHLIST_TRASH_DIR, token)
    os.makedirs(trash_dir, exist_ok=True)
    manifest: list[dict] = []
    for pattern in WATCHLIST_TRASH_PATTERNS:
        expanded = pattern.format(code=code)
        for path in glob.glob(expanded):
            if not os.path.isfile(path):
                continue
            dest = os.path.join(trash_dir, os.path.basename(path))
            shutil.move(path, dest)
            trashed.append(path)
            manifest.append({"from": path, "to": dest})
    if manifest:
        manifest_path = os.path.join(trash_dir, "_manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2)
        return token, trashed
    return None, []


def _restore_watchlist_artifacts(token: str) -> list[str]:
    if not token:
        return []
    trash_dir = os.path.join(WATCHLIST_TRASH_DIR, token)
    manifest_path = os.path.join(trash_dir, "_manifest.json")
    if not os.path.isfile(manifest_path):
        return []
    try:
        with open(manifest_path, "r", encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return []
    restored: list[str] = []
    for entry in manifest:
        src = entry.get("to")
        dest = entry.get("from")
        if not src or not dest:
            continue
        if not os.path.isfile(src):
            continue
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.move(src, dest)
        restored.append(dest)
    return restored


def _invalidate_screener_cache() -> None:
    _screener_cache["mtime"] = None
    _screener_cache["rows"] = []
    _rank_cache["weekly"] = {}
    _rank_cache["monthly"] = {}
    _rank_cache["mtime"] = None
    _rank_cache["config_mtime"] = _rank_config_cache.get("mtime")


def _delete_ticker_db_rows(code: str) -> dict:
    tables = ["daily_bars", "daily_ma", "monthly_bars", "monthly_ma", "stock_meta", "tickers"]
    counts: dict[str, int] = {}
    with get_conn() as conn:
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE code = ?", [code]).fetchone()[0]
            counts[table] = int(count or 0)
        for table in tables:
            conn.execute(f"DELETE FROM {table} WHERE code = ?", [code])
    return counts


def _delete_favorites_code(code: str) -> int:
    with _get_favorites_conn() as conn:
        cursor = conn.execute("DELETE FROM favorites WHERE code = ?", [code])
        return cursor.rowcount or 0


def _delete_practice_sessions(code: str) -> int:
    with _get_practice_conn() as conn:
        cursor = conn.execute("DELETE FROM practice_sessions WHERE code = ?", [code])
        return cursor.rowcount or 0

app = FastAPI(lifespan=lifespan)

if APP_ENV == "dev":
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"]
    )


@app.get("/health")
def simple_health():
    return JSONResponse(content={"ok": True})


@app.get("/api/events/meta")
def events_meta():
    with get_conn() as conn:
        meta = _load_events_meta(conn)
        if meta.get("is_refreshing"):
            lock_started_at = meta.get("refresh_lock_started_at")
            if not lock_started_at or _is_events_lock_stale(lock_started_at):
                finished_at = jst_now().replace(tzinfo=None)
                conn.execute(
                    """
                    UPDATE events_meta
                    SET
                        last_error = ?,
                        last_attempt_at = ?,
                        is_refreshing = FALSE,
                        refresh_lock_job_id = NULL,
                        refresh_lock_started_at = NULL
                    WHERE id = 1
                    """,
                    ["refresh_timeout", finished_at]
                )
                job_id = meta.get("refresh_lock_job_id")
                if job_id:
                    conn.execute(
                        """
                        UPDATE events_refresh_jobs
                        SET status = ?, finished_at = ?, error = ?
                        WHERE job_id = ? AND status = 'running'
                        """,
                        ["failed", finished_at, "refresh_timeout", job_id]
                    )
                meta = _load_events_meta(conn)
        rights_max = conn.execute(
            """
            SELECT MAX(COALESCE(last_rights_date, ex_date)) AS rights_max_date
            FROM ex_rights
            """
        ).fetchone()[0]
    payload = {
        "earnings_last_success_at": _format_event_timestamp(meta.get("earnings_last_success_at")),
        "rights_last_success_at": _format_event_timestamp(meta.get("rights_last_success_at")),
        "is_refreshing": bool(meta.get("is_refreshing")),
        "refresh_job_id": meta.get("refresh_lock_job_id"),
        "last_error": meta.get("last_error"),
        "last_attempt_at": _format_event_timestamp(meta.get("last_attempt_at")),
        "data_coverage": {
            "rights_max_date": _format_event_date(rights_max)
        }
    }
    return JSONResponse(content=payload)


@app.post("/api/events/refresh")
def events_refresh(reason: str | None = None):
    job_id = _start_events_refresh(reason)
    if not job_id:
        return JSONResponse(content={"error": "refresh_lock_failed"}, status_code=409)
    return JSONResponse(content={"refresh_job_id": job_id})


@app.get("/api/events/refresh/{job_id}")
def events_refresh_status(job_id: str):
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT status, started_at, finished_at, error
            FROM events_refresh_jobs
            WHERE job_id = ?
            """,
            [job_id]
        ).fetchone()
    if not row:
        return JSONResponse(content={"error": "job_not_found"}, status_code=404)
    return JSONResponse(
        content={
            "status": row[0],
            "started_at": _format_event_timestamp(row[1]),
            "finished_at": _format_event_timestamp(row[2]),
            "error": row[3]
        }
    )


@app.post("/api/positions/seed")
def upsert_position_seed(payload: dict = Body(...)):
    symbol = _normalize_code(payload.get("symbol"))
    if not symbol:
        return JSONResponse(content={"error": "symbol_required"}, status_code=400)
    buy_qty = float(payload.get("buy_qty") or 0)
    sell_qty = float(payload.get("sell_qty") or 0)
    asof_dt = payload.get("asof_dt") or jst_now().replace(tzinfo=None).isoformat()
    memo = payload.get("memo")
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO initial_positions_seed (symbol, buy_qty, sell_qty, asof_dt, memo)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                buy_qty = excluded.buy_qty,
                sell_qty = excluded.sell_qty,
                asof_dt = excluded.asof_dt,
                memo = excluded.memo
            """,
            [symbol, buy_qty, sell_qty, asof_dt, memo]
        )
        rebuild_summary = rebuild_positions(conn)
    return JSONResponse(content={"symbol": symbol, "rebuild": rebuild_summary})


@app.get("/api/positions/held")
def positions_held():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT p.symbol, p.buy_qty, p.sell_qty, p.opened_at, p.has_issue, p.issue_note, t.name
            FROM positions_live p
            LEFT JOIN tickers t ON p.symbol = t.code
            WHERE p.buy_qty > 0 OR p.sell_qty > 0
            ORDER BY p.symbol
            """
        ).fetchall()
    items = []
    for symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note, name in rows:
        buy = float(buy_qty or 0)
        sell = float(sell_qty or 0)
        items.append(
            {
                "symbol": symbol,
                "name": name or symbol,
                "buy_qty": buy,
                "sell_qty": sell,
                "sell_buy_text": f"{sell:g}-{buy:g}",
                "opened_at": _format_event_timestamp(opened_at),
                "has_issue": bool(has_issue),
                "issue_note": issue_note
            }
        )
    return JSONResponse(content={"items": items})


@app.get("/api/positions/current")
def positions_current():
    with get_conn() as conn:
        live_rows = conn.execute(
            """
            SELECT symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note
            FROM positions_live
            """
        ).fetchall()
        traded_rows = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM trade_events
            """
        ).fetchall()

    def to_lots(value: float | None) -> float:
        if value is None:
            return 0.0
        try:
            return float(value) / 100.0
        except (TypeError, ValueError):
            return 0.0

    current_positions_by_code: dict[str, dict] = {}
    holding_codes: list[str] = []

    for symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note in live_rows:
        buy_lots = to_lots(buy_qty)
        sell_lots = to_lots(sell_qty)
        current_positions_by_code[str(symbol)] = {
            "buyShares": buy_lots,
            "sellShares": sell_lots,
            "opened_at": _format_event_timestamp(opened_at),
            "has_issue": bool(has_issue),
            "issue_note": issue_note
        }
        if buy_lots > 0 or sell_lots > 0:
            holding_codes.append(str(symbol))

    all_traded_codes = sorted({str(row[0]) for row in traded_rows if row and row[0]})

    return JSONResponse(
        content={
            "holding_codes": holding_codes,
            "current_positions_by_code": current_positions_by_code,
            "all_traded_codes": all_traded_codes
        }
    )


@app.post("/api/positions/rebuild")
def positions_rebuild():
    """Force rebuild all positions from trade events"""
    try:
        with get_conn() as conn:
            summary = rebuild_positions(conn)
            
            # Get updated holdings count
            holdings_count = conn.execute("""
                SELECT COUNT(*) FROM positions_live 
                WHERE buy_qty > 0 OR sell_qty > 0
            """).fetchone()[0]
            
        return JSONResponse(
            content={
                "success": True,
                "summary": summary,
                "holdings_count": holdings_count,
                "message": f"Positions rebuilt successfully. {holdings_count} symbols with holdings."
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "message": "Failed to rebuild positions"
            }
        )

@app.get("/api/positions/history")
def positions_history(symbol: str | None = None):
    params: list = []
    where_clause = ""
    if symbol:
        where_clause = "WHERE symbol = ?"
        params.append(_normalize_code(symbol))
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT r.round_id, r.symbol, r.opened_at, r.closed_at, r.closed_reason, r.has_issue,
                   r.issue_note, t.name
            FROM position_rounds r
            LEFT JOIN tickers t ON r.symbol = t.code
            {where_clause}
            ORDER BY r.closed_at DESC
            """,
            params
        ).fetchall()
    items = []
    counts: dict[str, int] = {}
    for round_id, sym, opened_at, closed_at, closed_reason, has_issue, issue_note, name in rows:
        counts[sym] = counts.get(sym, 0) + 1
        items.append(
            {
                "round_id": round_id,
                "symbol": sym,
                "name": name or sym,
                "opened_at": _format_event_timestamp(opened_at),
                "closed_at": _format_event_timestamp(closed_at),
                "closed_reason": closed_reason,
                "has_issue": bool(has_issue),
                "issue_note": issue_note,
                "round_no": counts[sym]
            }
        )
    return JSONResponse(content={"items": items})


@app.get("/api/positions/history/events")
def position_round_events(round_id: str):
    with get_conn() as conn:
        round_row = conn.execute(
            "SELECT symbol, opened_at, closed_at FROM position_rounds WHERE round_id = ?",
            [round_id]
        ).fetchone()
        if not round_row:
            return JSONResponse(content={"error": "round_not_found"}, status_code=404)
        symbol, opened_at, closed_at = round_row
        rows = conn.execute(
            """
            SELECT broker, exec_dt, action, qty, price
            FROM trade_events
            WHERE symbol = ? AND exec_dt BETWEEN ? AND ?
            ORDER BY exec_dt
            """,
            [symbol, opened_at, closed_at]
        ).fetchall()
    events = [
        {
            "broker": row[0],
            "exec_dt": _format_event_timestamp(row[1]),
            "action": row[2],
            "qty": float(row[3] or 0),
            "price": float(row[4]) if row[4] is not None else None
        }
        for row in rows
    ]
    return JSONResponse(
        content={
            "round_id": round_id,
            "symbol": symbol,
            "opened_at": _format_event_timestamp(opened_at),
            "closed_at": _format_event_timestamp(closed_at),
            "events": events
        }
    )


@app.get("/api/search/similar", response_model=list[SearchResult])
def search_similar(ticker: str, asof: str = None, k: int = 30, alpha: float = 0.7):
    try:
        return _similarity_service.search(ticker, asof, k, alpha)
    except ValueError as e:
        # Expected error: Ticker not indexed
        err_msg = str(e)
        if "not indexed" in err_msg:
             raise HTTPException(status_code=404, detail="データ期間不足のため検索対象外です (120ヶ月未満)")
        raise HTTPException(status_code=400, detail=err_msg)
    except (FileNotFoundError, RuntimeError) as e:
        # Similarity artifacts are missing or not loaded
        message = str(e)
        if "Parquet engine" in message or "pyarrow" in message or "fastparquet" in message:
            raise HTTPException(
                status_code=503,
                detail="類似検索データの読み込みに必要なpyarrowが見つかりません。pyarrowをインストールしてください。"
            )
        raise HTTPException(status_code=503, detail="類似検索データが未作成です。更新処理を実行してください。")
    except Exception as e:
        # Unexpected
        print(f"Similarity Search Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Search processing failed")


@app.post("/api/search/similar/refresh")
def refresh_similarity(mode: str = "incremental"):
    if mode not in ("incremental", "full"):
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_mode"})
    started = _start_similarity_refresh(mode)
    if not started:
        return JSONResponse(status_code=409, content={"ok": False, "error": "already_running"})
    return JSONResponse(content={"ok": True, "status": "started", "mode": mode})


@app.get("/api/search/similar/status")
def similarity_refresh_status():
    with _similarity_refresh_lock:
        return JSONResponse(content={"ok": True, "status": dict(_similarity_refresh_status)})


# Daily Memo API endpoints
@app.get("/api/memo")
def get_daily_memo(symbol: str, date: str, timeframe: str = "D"):
    """Get memo for a specific symbol and date"""
    normalized_symbol = _normalize_code(symbol)
    if not normalized_symbol:
        return JSONResponse(status_code=400, content={"error": "invalid_symbol"})
    
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT memo, updated_at
            FROM daily_memo
            WHERE symbol = ? AND date = ? AND timeframe = ?
            """,
            [normalized_symbol, date, timeframe]
        ).fetchone()
        
        if not row:
            return JSONResponse(content={"memo": "", "updated_at": None})
        
        memo, updated_at = row
        return JSONResponse(content={
            "memo": memo or "",
            "updated_at": updated_at.isoformat() if updated_at else None
        })


@app.get("/api/memo/list")
def list_daily_memo(symbol: str, timeframe: str = "D"):
    """List memos for a symbol/timeframe"""
    normalized_symbol = _normalize_code(symbol)
    if not normalized_symbol:
        return JSONResponse(status_code=400, content={"error": "invalid_symbol"})

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT date, memo
            FROM daily_memo
            WHERE symbol = ? AND timeframe = ?
            ORDER BY date
            """,
            [normalized_symbol, timeframe]
        ).fetchall()

    items = []
    for row in rows:
        date_value = row[0]
        date_str = date_value.isoformat() if hasattr(date_value, "isoformat") else str(date_value)
        items.append({"date": date_str, "memo": row[1] or ""})
    return JSONResponse(content={"items": items})


@app.put("/api/memo")
def save_daily_memo(payload: dict = Body(...)):
    """Save or update memo for a specific symbol and date"""
    symbol = _normalize_code(payload.get("symbol"))
    date = payload.get("date")
    timeframe = payload.get("timeframe", "D")
    memo = payload.get("memo", "").strip()
    
    if not symbol or not date:
        return JSONResponse(status_code=400, content={"error": "symbol_and_date_required"})
    
    # Validate memo length (max 100 characters)
    if len(memo) > 100:
        return JSONResponse(status_code=400, content={"error": "memo_too_long", "max_length": 100})
    
    now = jst_now().replace(tzinfo=None)
    
    with get_conn() as conn:
        if not memo:
            # Delete if memo is empty
            conn.execute(
                """
                DELETE FROM daily_memo
                WHERE symbol = ? AND date = ? AND timeframe = ?
                """,
                [symbol, date, timeframe]
            )
            return JSONResponse(content={"ok": True, "deleted": True, "updated_at": None})
        else:
            # Upsert memo
            conn.execute(
                """
                INSERT INTO daily_memo (symbol, date, timeframe, memo, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol, date, timeframe) DO UPDATE SET
                    memo = excluded.memo,
                    updated_at = excluded.updated_at
                """,
                [symbol, date, timeframe, memo, now, now]
            )
            return JSONResponse(content={
                "ok": True,
                "updated_at": now.isoformat()
            })


@app.delete("/api/memo")
def delete_daily_memo(symbol: str, date: str, timeframe: str = "D"):
    """Delete memo for a specific symbol and date"""
    normalized_symbol = _normalize_code(symbol)
    if not normalized_symbol:
        return JSONResponse(status_code=400, content={"error": "invalid_symbol"})
    
    with get_conn() as conn:
        cursor = conn.execute(
            """
            DELETE FROM daily_memo
            WHERE symbol = ? AND date = ? AND timeframe = ?
            """,
            [normalized_symbol, date, timeframe]
        )
        deleted = cursor.rowcount > 0
        
    return JSONResponse(content={"ok": True, "deleted": deleted})


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    trace_id = str(uuid.uuid4())
    payload = {
        "trace_id": trace_id,
        "error_code": "HTTP_ERROR",
        "message": "Request failed",
        "detail": str(exc.detail)
    }
    if DEBUG:
        payload["stack"] = traceback.format_exc()

    return JSONResponse(status_code=exc.status_code, content=payload, headers={"X-Request-Id": trace_id})


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    trace_id = str(uuid.uuid4())
    status_code, _, _ = _classify_exception(exc)
    payload = _build_error_payload(exc, trace_id)
    return JSONResponse(status_code=status_code, content=payload, headers={"X-Request-Id": trace_id})


@app.post("/api/trade_csv/upload")
async def trade_csv_upload(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        return JSONResponse(status_code=400, content={"ok": False, "error": "csv_required"})
    dest_dir = resolve_trade_csv_dir()
    os.makedirs(dest_dir, exist_ok=True)
    filename = os.path.basename(file.filename)
    dest_path = os.path.join(dest_dir, filename)
    
    # Save file
    file.file.seek(0)
    content = file.file.read()
    with open(dest_path, "wb") as handle:
        handle.write(content)
        
    # Ingest
    try:
        broker = "rakuten"
        try:
            head_sample = content[:8192].decode("cp932", errors="ignore")
            if "受渡金額/決済損益" in head_sample or "信用新規買" in head_sample:
                broker = "sbi"
            elif "口座" in head_sample and "手数料" in head_sample:
                broker = "rakuten"
        except Exception:
            pass

        if broker == "sbi":
            result = process_import_sbi(content, replace_existing=True)
        else:
            result = process_import_rakuten(content, replace_existing=True)
        return JSONResponse(content={"ok": True, "filename": filename, "path": dest_path, "ingest": result})
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": f"ingest_failed:{e}"})





def _parse_trade_csv() -> dict:
    warnings: list[dict] = []
    paths = resolve_trade_csv_paths()
    existing_paths = [path for path in paths if os.path.isfile(path)]
    if not existing_paths:
        missing = ", ".join(paths)
        warnings.append({"type": "trade_csv_missing", "message": f"trade_csv_missing:{missing}"})
        return {"rows": [], "warnings": warnings}

    key = tuple((path, os.path.getmtime(path)) for path in existing_paths)
    if _trade_cache["key"] == key:
        return {"rows": _trade_cache["rows"], "warnings": _trade_cache["warnings"]}

    rows: list[dict] = []

    def normalize_text(value: str | None) -> str:
        if value is None:
            return ""
        text = str(value).replace("\ufeff", "")
        if text.strip().lower() in ("nan", "none", "--"):
            return ""
        text = text.replace("\u3000", " ")
        return text.strip()

    def normalize_label(value: str | None) -> str:
        text = normalize_text(value)
        if not text:
            return ""
        return re.sub(r"\s+", "", text)

    def read_csv_rows(path: str, encoding: str) -> list[list[str]]:
        with open(path, "r", encoding=encoding, newline="") as handle:
            reader = csv.reader(handle)
            return list(reader)

    def make_dedup_key(
        code: str,
        date_value: str | None,
        trade_label: str,
        qty_raw: str,
        price_raw: str,
        amount_raw: str,
        fee_raw: str = "",
        tax_raw: str = "",
        account: str = ""
    ) -> str:
        parts = [
            normalize_text(code),
            normalize_text(date_value or ""),
            normalize_label(trade_label),
            normalize_text(qty_raw),
            normalize_text(price_raw),
            normalize_text(amount_raw),
            normalize_text(fee_raw),
            normalize_text(tax_raw),
            normalize_text(account)
        ]
        return "|".join(parts)

    def log_dedup_summary(duplicate_counts: dict[str, int]) -> None:
        if not duplicate_counts:
            return
        print(
            "trade_dedup_key=code|date|trade|qty|price|amount|fee|tax|account "
            f"duplicates={duplicate_counts}"
        )

    def to_float(value: str) -> float:
        try:
            return float(value.replace(",", ""))
        except ValueError:
            return 0.0

    def to_optional_float(value: str) -> float | None:
        text = normalize_text(value)
        if not text:
            return None
        try:
            return float(text.replace(",", ""))
        except ValueError:
            return None

    def parse_date(value: str) -> str | None:
        raw = normalize_text(value)
        if not raw:
            return None
        for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def find_sbi_header_index(rows_all: list[list[str]]) -> int | None:
        start = min(6, max(0, len(rows_all)))
        for idx in range(start, min(len(rows_all), start + 6)):
            row = rows_all[idx]
            if not row or not any(cell.strip() for cell in row):
                continue
            if "約定日" in row or "銘柄コード" in row:
                return idx
        return None

    def looks_like_sbi(rows_all: list[list[str]]) -> bool:
        for row in rows_all[:10]:
            if any("CSV作成日" in cell for cell in row):
                return True
        header_index = find_sbi_header_index(rows_all)
        if header_index is None:
            return False
        header_row = [cell.strip() for cell in rows_all[header_index]]
        if any(
            name in header_row
            for name in ("受渡金額/決済損益", "決済損益", "受渡金額", "手数料/諸経費等")
        ):
            return True
        if "取引" in header_row:
            trade_idx = header_row.index("取引")
            for row in rows_all[header_index + 1 : header_index + 50]:
                if trade_idx < len(row) and any(
                    key in row[trade_idx] for key in ("信用新規買", "信用返済売", "信用新規売", "信用返済買")
                ):
                    return True
        return False

    def parse_sbi_rows(rows_all: list[list[str]], encoding_used: str) -> dict:
        header_index = find_sbi_header_index(rows_all)
        if header_index is None:
            warnings.append({"type": "sbi_header_missing", "message": "sbi_header_missing"})
            return {"rows": [], "warnings": warnings}

        header = [cell.strip() for cell in rows_all[header_index]]

        def find_col(*names: str) -> int | None:
            for name in names:
                if name in header:
                    return header.index(name)
            return None

        col_trade_date = find_col("約定日")
        col_settle_date = find_col("受渡日")
        col_code = find_col("銘柄コード")
        col_name = find_col("銘柄")
        col_market = find_col("市場")
        col_trade = find_col("取引")
        col_account = find_col("預り")
        col_qty = find_col("約定数量", "数量")
        col_price = find_col("約定単価", "単価")
        col_fee = find_col("手数料/諸経費等", "手数料等")
        col_tax = find_col("税額")
        col_amount = find_col("受渡金額/決済損益", "決済損益", "受渡金額")

        dedup_keys: set[str] = set()
        duplicate_counts: dict[str, int] = {}
        for row_index, line in enumerate(rows_all[header_index + 1 :], start=1):
            if not line or col_trade_date is None or col_code is None:
                continue
            if not any(cell.strip() for cell in line):
                continue
            date_value = parse_date(line[col_trade_date]) if col_trade_date < len(line) else None
            code_raw = normalize_text(line[col_code]) if col_code < len(line) else ""
            if not date_value or not code_raw:
                continue
            code = _normalize_code(code_raw)
            if not code:
                continue

            name = normalize_text(line[col_name]) if col_name is not None and col_name < len(line) else ""
            market = normalize_text(line[col_market]) if col_market is not None and col_market < len(line) else ""
            account = normalize_text(line[col_account]) if col_account is not None and col_account < len(line) else ""
            trade_raw = normalize_text(line[col_trade]) if col_trade is not None and col_trade < len(line) else ""
            qty_raw = normalize_text(line[col_qty]) if col_qty is not None and col_qty < len(line) else ""
            price_raw = normalize_text(line[col_price]) if col_price is not None and col_price < len(line) else ""
            fee_raw = normalize_text(line[col_fee]) if col_fee is not None and col_fee < len(line) else ""
            tax_raw = normalize_text(line[col_tax]) if col_tax is not None and col_tax < len(line) else ""
            amount_raw = normalize_text(line[col_amount]) if col_amount is not None and col_amount < len(line) else ""
            settle_date = (
                parse_date(line[col_settle_date]) if col_settle_date is not None and col_settle_date < len(line) else None
            )

            qty_shares = to_float(qty_raw)
            if qty_shares <= 0:
                continue
            if qty_shares % 100 != 0:
                warnings.append(
                    {
                        "type": "non_100_shares",
                        "message": f"non_100_shares:{code}:{date_value}:{qty_shares}",
                        "code": code
                    }
                )
            price = to_optional_float(price_raw)
            fee = to_optional_float(fee_raw)
            tax = to_optional_float(tax_raw)
            amount = to_optional_float(amount_raw)
            realized_net = None
            if amount is not None:
                realized_net = amount
                if fee is not None:
                    realized_net -= fee
                if tax is not None:
                    realized_net -= tax

            trade_label = normalize_label(trade_raw)
            txn_type = ""
            event_kind = None
            if "信用新規買" in trade_label:
                txn_type = "OPEN_LONG"
                event_kind = "BUY_OPEN"
            elif "信用返済売" in trade_label:
                txn_type = "CLOSE_LONG"
                event_kind = "SELL_CLOSE"
            elif "信用新規売" in trade_label:
                txn_type = "OPEN_SHORT"
                event_kind = "SELL_OPEN"
            elif "信用返済買" in trade_label:
                txn_type = "CLOSE_SHORT"
                event_kind = "BUY_CLOSE"
            elif "現物買" in trade_label or "買付" in trade_label:
                txn_type = "OPEN_LONG"
                event_kind = "BUY_OPEN"
            elif "現物売" in trade_label or "売付" in trade_label:
                txn_type = "CLOSE_LONG"
                event_kind = "SELL_CLOSE"
            elif "入庫" in trade_label:
                txn_type = "CORPORATE_ACTION"
                event_kind = "INBOUND"
            elif "出庫" in trade_label:
                txn_type = "CORPORATE_ACTION"
                event_kind = "OUTBOUND"

            if event_kind is None:
                sample = f"取引={trade_raw or '(blank)'}"
                unknown_labels_by_code.setdefault(code, set()).add(sample)
                continue

            dedup_key = make_dedup_key(
                code,
                date_value,
                trade_label,
                qty_raw,
                price_raw,
                amount_raw,
                fee_raw,
                tax_raw,
                account
            )
            if dedup_key in dedup_keys:
                duplicate_counts[code] = duplicate_counts.get(code, 0) + 1
                continue
            dedup_keys.add(dedup_key)

            if event_kind == "BUY_OPEN":
                side = "buy"
                action = "open"
            elif event_kind == "BUY_CLOSE":
                side = "buy"
                action = "close"
            elif event_kind == "SELL_OPEN":
                side = "sell"
                action = "open"
            elif event_kind == "SELL_CLOSE":
                side = "sell"
                action = "close"
            else:
                side = "buy"
                action = "open"

            if event_kind in ("BUY_OPEN", "SELL_OPEN"):
                event_order = 0
            elif event_kind in ("SELL_CLOSE", "BUY_CLOSE"):
                event_order = 1
            else:
                event_order = 2

            rows.append(
                {
                    "broker": "SBI",
                    "tradeDate": date_value,
                    "trade_date": date_value,
                    "settleDate": settle_date,
                    "settle_date": settle_date,
                    "code": code,
                    "name": name,
                    "market": market,
                    "account": account,
                    "txnType": txn_type,
                    "txn_type": txn_type,
                    "qty": qty_shares,
                    "qtyShares": qty_shares,
                    "units": int(qty_shares // 100),
                    "price": price if price is not None and price > 0 else None,
                    "fee": fee,
                    "tax": tax,
                    "realizedPnlGross": amount,
                    "realizedPnlNet": realized_net,
                    "memo": trade_raw,
                    "date": date_value,
                    "side": side,
                    "action": action,
                    "kind": event_kind,
                    "_row_index": row_index,
                    "_event_order": event_order,
                    "raw": {
                        "date": line[col_trade_date] if col_trade_date is not None and col_trade_date < len(line) else "",
                        "code": code_raw,
                        "name": name,
                        "trade": trade_raw,
                        "market": market,
                        "account": account,
                        "qty": qty_raw,
                        "price": price_raw,
                        "fee": fee_raw,
                        "tax": tax_raw,
                        "amount": amount_raw,
                        "encoding": encoding_used
                    }
                }
            )

        for code, count in duplicate_counts.items():
            warnings.append(
                {"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code}
            )

        log_dedup_summary(duplicate_counts)

        for code, samples_set in unknown_labels_by_code.items():
            samples = sorted(list(samples_set))[:5]
            warnings.append(
                {
                    "type": "unrecognized_labels",
                    "count": len(samples_set),
                    "samples": samples,
                    "code": code
                }
            )

        rows.sort(
            key=lambda item: (item.get("date", ""), item.get("_event_order", 2), item.get("_row_index", 0))
        )
        return {"rows": rows, "warnings": warnings}

    def parse_single(path: str) -> tuple[list[dict], list[dict]]:
        file_rows: list[dict] = []
        file_warnings: list[dict] = []
        unknown_labels_by_code: dict[str, set[str]] = {}

        try:
            rows_all = read_csv_rows(path, "cp932")
            encoding_used = "cp932"
        except UnicodeDecodeError:
            rows_all = read_csv_rows(path, "utf-8-sig")
            encoding_used = "utf-8-sig"

        if rows_all:
            header = [normalize_text(cell) for cell in rows_all[0]] if rows_all else []
            if not looks_like_sbi(rows_all) and ("約定日" not in header and "約定日付" not in header):
                try:
                    rows_all = read_csv_rows(path, "utf-8-sig")
                    encoding_used = "utf-8-sig"
                except UnicodeDecodeError:
                    pass

        if looks_like_sbi(rows_all):
            header_index = find_sbi_header_index(rows_all)
            if header_index is None:
                file_warnings.append(
                    {"type": "sbi_header_missing", "message": f"sbi_header_missing:{path}"}
                )
                return file_rows, file_warnings
            raw_header = [normalize_text(cell) for cell in rows_all[header_index]]
            data_rows = rows_all[header_index + 1 :]
            header = raw_header
            col_map = {name: index for index, name in enumerate(header) if name}
            get_cell = lambda row, key: normalize_text(row[col_map.get(key, -1)]) if key in col_map else ""

            dedup_keys: set[str] = set()
            duplicate_counts: dict[str, int] = {}

            for row_index, row in enumerate(data_rows, start=1):
                if not row or not any(cell.strip() for cell in row):
                    continue
                trade_date = parse_date(get_cell(row, "約定日"))
                if not trade_date:
                    continue
                code = _normalize_code(get_cell(row, "銘柄コード"))
                name = get_cell(row, "銘柄")
                market = get_cell(row, "市場")
                account = get_cell(row, "預り")
                trade_kind = get_cell(row, "取引") or get_cell(row, "取引区分")
                qty_raw = get_cell(row, "約定数量") or get_cell(row, "数量")
                qty_shares = to_float(qty_raw)
                price_raw = get_cell(row, "約定単価") or get_cell(row, "単価")
                price = to_optional_float(price_raw)
                fee_raw = get_cell(row, "手数料/諸経費等")
                tax_raw = get_cell(row, "税金") or get_cell(row, "税額")
                pnl_raw = get_cell(row, "受渡金額/決済損益") or get_cell(row, "決済損益")
                realized_pnl = to_optional_float(pnl_raw)
                if qty_shares <= 0:
                    continue

                event_kind = None
                if "信用新規買" in trade_kind:
                    event_kind = "BUY_OPEN"
                elif "信用返済売" in trade_kind:
                    event_kind = "SELL_CLOSE"
                elif "信用新規売" in trade_kind:
                    event_kind = "SELL_OPEN"
                elif "信用返済買" in trade_kind:
                    event_kind = "BUY_CLOSE"
                elif "現物買" in trade_kind or "買付" in trade_kind:
                    event_kind = "BUY_OPEN"
                elif "現物売" in trade_kind or "売付" in trade_kind:
                    event_kind = "SELL_CLOSE"

                if event_kind is None:
                    sample = f"取引区分={trade_kind or '(blank)'}, 売買区分=(blank)"
                    unknown_labels_by_code.setdefault(code, set()).add(sample)
                    continue

                dedup_key = make_dedup_key(
                    code,
                    trade_date,
                    trade_kind,
                    qty_raw,
                    price_raw,
                    pnl_raw,
                    fee_raw,
                    tax_raw,
                    account
                )
                if dedup_key in dedup_keys:
                    duplicate_counts[code] = duplicate_counts.get(code, 0) + 1
                    continue
                dedup_keys.add(dedup_key)

                if event_kind == "BUY_OPEN":
                    side = "buy"
                    action = "open"
                elif event_kind == "BUY_CLOSE":
                    side = "buy"
                    action = "close"
                elif event_kind == "SELL_OPEN":
                    side = "sell"
                    action = "open"
                else:
                    side = "sell"
                    action = "close"

                if event_kind in ("BUY_OPEN", "SELL_OPEN"):
                    event_order = 0
                elif event_kind in ("SELL_CLOSE", "BUY_CLOSE"):
                    event_order = 1
                else:
                    event_order = 2

                txn_type = "CORPORATE_ACTION"
                if event_kind == "BUY_OPEN":
                    txn_type = "OPEN_LONG"
                elif event_kind == "SELL_CLOSE":
                    txn_type = "CLOSE_LONG"
                elif event_kind == "SELL_OPEN":
                    txn_type = "OPEN_SHORT"
                elif event_kind == "BUY_CLOSE":
                    txn_type = "CLOSE_SHORT"

                file_rows.append(
                    {
                        "broker": "SBI",
                        "tradeDate": trade_date,
                        "trade_date": trade_date,
                        "settleDate": parse_date(get_cell(row, "受渡日")),
                        "settle_date": parse_date(get_cell(row, "受渡日")),
                        "date": trade_date,
                        "code": code,
                        "name": name,
                        "market": market,
                        "account": account,
                        "txnType": txn_type,
                        "txn_type": txn_type,
                        "qty": qty_shares,
                        "side": side,
                        "action": action,
                        "kind": event_kind,
                        "qtyShares": qty_shares,
                        "units": int(qty_shares // 100),
                        "price": price if price is not None and price > 0 else None,
                        "fee": to_optional_float(fee_raw),
                        "tax": to_optional_float(tax_raw),
                        "realizedPnlGross": realized_pnl,
                        "realizedPnlNet": realized_pnl,
                        "memo": trade_kind,
                        "_row_index": row_index,
                        "_event_order": event_order,
                        "raw": {
                            "date": trade_date,
                            "code": code,
                            "name": name,
                            "trade": trade_kind,
                            "qty": qty_raw,
                            "price": price_raw,
                            "amount": pnl_raw,
                            "encoding": encoding_used
                        }
                    }
                )

            for code, count in duplicate_counts.items():
                file_warnings.append(
                    {"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code}
                )

            log_dedup_summary(duplicate_counts)

        else:
            rows_all = rows_all
            header = [normalize_text(cell) for cell in rows_all[0]] if rows_all else []
            data_rows = rows_all[1:] if rows_all else []
            col_map = {name: index for index, name in enumerate(header) if name}
            get_cell = lambda row, key: normalize_text(row[col_map.get(key, -1)]) if key in col_map else ""

            dedup_keys: set[str] = set()
            duplicate_counts: dict[str, int] = {}

            for row_index, row in enumerate(data_rows, start=1):
                if not row or not any(cell.strip() for cell in row):
                    continue
                date_raw = get_cell(row, "約定日") or get_cell(row, "日付")
                date_value = parse_date(date_raw)
                if not date_value:
                    continue
                settle_date = parse_date(get_cell(row, "受渡日"))
                code_raw = get_cell(row, "銘柄コード") or get_cell(row, "銘柄ｺｰﾄﾞ") or get_cell(row, "銘柄")
                code = _normalize_code(code_raw)
                name = get_cell(row, "銘柄名") or get_cell(row, "銘柄")
                market = get_cell(row, "市場")
                account = get_cell(row, "口座区分") or get_cell(row, "預り区分")
                type_raw = get_cell(row, "取引区分")
                kind_raw = get_cell(row, "売買区分")
                trade_type = normalize_label(type_raw)
                trade_kind = normalize_label(kind_raw)
                qty_raw = (
                    get_cell(row, "数量［株］")
                    or get_cell(row, "数量[株]")
                    or get_cell(row, "数量")
                    or get_cell(row, "数量(株)")
                )
                qty_shares = to_float(qty_raw)
                price_raw = (
                    get_cell(row, "単価［円］")
                    or get_cell(row, "単価[円]")
                    or get_cell(row, "単価")
                    or get_cell(row, "約定単価")
                )
                price = to_optional_float(price_raw)
                amount_raw = (
                    get_cell(row, "受渡金額［円］")
                    or get_cell(row, "受渡金額[円]")
                    or get_cell(row, "受渡金額")
                )
                fee_raw = (
                    get_cell(row, "手数料［円］")
                    or get_cell(row, "手数料[円]")
                    or get_cell(row, "手数料")
                )
                tax_raw = (
                    get_cell(row, "税金等［円］")
                    or get_cell(row, "税金等[円]")
                    or get_cell(row, "税金")
                )
                if qty_shares <= 0:
                    continue

                event_kind = None
                if trade_kind == "現渡" or trade_type == "現渡":
                    event_kind = "DELIVERY"
                elif trade_kind == "現引" or trade_type == "現引":
                    event_kind = "TAKE_DELIVERY"
                elif trade_type == "入庫" or trade_kind == "入庫":
                    event_kind = "INBOUND"
                elif trade_type == "出庫" or trade_kind == "出庫":
                    event_kind = "OUTBOUND"

                if event_kind is None:
                    if "買建" in trade_kind:
                        event_kind = "BUY_OPEN"
                    elif "売建" in trade_kind:
                        event_kind = "SELL_OPEN"
                    elif "買埋" in trade_kind:
                        event_kind = "BUY_CLOSE"
                    elif "売埋" in trade_kind:
                        event_kind = "SELL_CLOSE"
                    elif "現物買" in trade_kind or "買付" in trade_kind:
                        event_kind = "BUY_OPEN"
                    elif "現物売" in trade_kind or "売付" in trade_kind:
                        event_kind = "SELL_CLOSE"
                    elif trade_type == "入庫" or trade_kind == "入庫":
                        event_kind = "INBOUND"
                    elif trade_type == "出庫" or trade_kind == "出庫":
                        event_kind = "OUTBOUND"
                    elif trade_type == "現渡" or trade_kind == "現渡":
                        event_kind = "DELIVERY"
                    elif trade_type == "現引" or trade_kind == "現引":
                        event_kind = "TAKE_DELIVERY"

                if event_kind is None:
                    sample = f"取引区分={trade_type or '(blank)'}, 売買区分={trade_kind or '(blank)'}"
                    unknown_labels_by_code.setdefault(code, set()).add(sample)
                    continue

                dedup_key = make_dedup_key(
                    code,
                    date_value,
                    trade_type + "|" + trade_kind,
                    qty_raw,
                    price_raw,
                    amount_raw,
                    fee_raw,
                    tax_raw,
                    account
                )
                if dedup_key in dedup_keys:
                    duplicate_counts[code] = duplicate_counts.get(code, 0) + 1
                    continue
                dedup_keys.add(dedup_key)

                if event_kind == "BUY_OPEN":
                    side = "buy"
                    action = "open"
                elif event_kind == "BUY_CLOSE":
                    side = "buy"
                    action = "close"
                elif event_kind == "SELL_OPEN":
                    side = "sell"
                    action = "open"
                elif event_kind == "SELL_CLOSE":
                    side = "sell"
                    action = "close"
                else:
                    side = "buy"
                    action = "open"

                if event_kind in ("BUY_OPEN", "SELL_OPEN"):
                    event_order = 0
                elif event_kind in ("SELL_CLOSE", "BUY_CLOSE"):
                    event_order = 1
                else:
                    event_order = 2

                txn_type = "CORPORATE_ACTION"
                if event_kind == "BUY_OPEN":
                    txn_type = "OPEN_LONG"
                elif event_kind == "SELL_CLOSE":
                    txn_type = "CLOSE_LONG"
                elif event_kind == "SELL_OPEN":
                    txn_type = "OPEN_SHORT"
                elif event_kind == "BUY_CLOSE":
                    txn_type = "CLOSE_SHORT"

                file_rows.append(
                    {
                        "broker": "RAKUTEN",
                        "tradeDate": date_value,
                        "trade_date": date_value,
                        "settleDate": settle_date,
                        "settle_date": settle_date,
                        "date": date_value,
                        "code": code,
                        "name": name,
                        "market": market,
                        "account": account,
                        "txnType": txn_type,
                        "txn_type": txn_type,
                        "qty": qty_shares,
                        "side": side,
                        "action": action,
                        "kind": event_kind,
                        "qtyShares": qty_shares,
                        "units": int(qty_shares // 100),
                        "price": price if price is not None and price > 0 else None,
                        "fee": to_optional_float(fee_raw),
                        "tax": to_optional_float(tax_raw),
                        "realizedPnlGross": None,
                        "realizedPnlNet": None,
                        "memo": kind_raw or type_raw,
                        "_row_index": row_index,
                        "_event_order": event_order,
                        "raw": {
                            "date": date_raw,
                            "code": code_raw,
                            "name": name,
                            "trade": kind_raw,
                            "type": type_raw,
                            "qty": qty_raw,
                            "price": price_raw,
                            "amount": amount_raw,
                            "encoding": encoding_used
                        }
                    }
                )

            for code, count in duplicate_counts.items():
                file_warnings.append(
                    {"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code}
                )

            log_dedup_summary(duplicate_counts)

        for code, samples_set in unknown_labels_by_code.items():
            samples = sorted(list(samples_set))[:5]
            file_warnings.append(
                {
                    "type": "unrecognized_labels",
                    "count": len(samples_set),
                    "samples": samples,
                    "code": code
                }
            )

        file_rows.sort(
            key=lambda item: (
                item.get("date", ""),
                item.get("_event_order", 2),
                item.get("_row_index", 0)
            )
        )
        return file_rows, file_warnings

    for path in existing_paths:
        file_rows, file_warnings = parse_single(path)
        if not file_rows and not file_warnings:
            continue
        rows.extend(file_rows)
        warnings.extend(file_warnings)

    global_dedup_keys: set[str] = set()
    global_duplicate_counts: dict[str, int] = {}
    deduped_rows: list[dict] = []
    for row in rows:
        raw = row.get("raw") or {}
        code = row.get("code") or ""
        date_value = row.get("date") or row.get("tradeDate") or ""
        trade_label = raw.get("trade") or raw.get("type") or row.get("memo") or row.get("kind") or ""
        qty_raw = raw.get("qty") or row.get("qtyShares") or ""
        price_raw = raw.get("price") or row.get("price") or ""
        amount_raw = raw.get("amount") or row.get("realizedPnlGross") or row.get("realizedPnlNet") or ""
        fee_raw = raw.get("fee") or row.get("fee") or ""
        tax_raw = raw.get("tax") or row.get("tax") or ""
        account = raw.get("account") or row.get("account") or ""
        dedup_key = make_dedup_key(
            str(code),
            str(date_value),
            str(trade_label),
            str(qty_raw),
            str(price_raw),
            str(amount_raw),
            str(fee_raw),
            str(tax_raw),
            str(account)
        )
        if dedup_key in global_dedup_keys:
            code_key = str(code) if code is not None else "unknown"
            global_duplicate_counts[code_key] = global_duplicate_counts.get(code_key, 0) + 1
            continue
        global_dedup_keys.add(dedup_key)
        deduped_rows.append(row)

    if global_duplicate_counts:
        for code, count in global_duplicate_counts.items():
            warnings.append(
                {"type": "duplicate_rows", "message": f"duplicate_skipped:{code}:{count}", "code": code}
            )
        log_dedup_summary(global_duplicate_counts)

    rows = deduped_rows
    rows.sort(
        key=lambda item: (item.get("date", ""), item.get("_event_order", 2), item.get("_row_index", 0))
    )

    _trade_cache["key"] = key
    _trade_cache["rows"] = rows
    _trade_cache["warnings"] = warnings
    return {"rows": rows, "warnings": warnings}


def _build_daily_positions(trades: list[dict]) -> list[dict]:
    # Legacy wrapper or unused
    return []

def _get_daily_positions_db(target_codes: list[str] | None = None) -> dict[str, list[dict]]:
    # Reconstruct daily positions from DB events
    with get_conn() as conn:
        events = get_events(conn, target_codes)
    
    events_by_code = {}
    for ev in events:
        # 3:symbol
        code = ev[3]
        events_by_code.setdefault(code, []).append(ev)
        
    result = {}
    
    for code, ev_list in events_by_code.items():
        daily_list = []
        
        # Sorting is guaranteed by get_events (exec_dt ASC)
        # We need to walk through days.
        # But for chart, we usually just want the change points or one per day?
        # Usually one per day matching the daily bars is ideal, but here we just produce 
        # a series of "end of day" positions for days where events happened OR all days?
        # Frontend filters by `time === selectedBarData.time`. So we need entries for every relevant date.
        # Ideally, we return a dense list or the frontend needs to fill gaps.
        # Current _build_daily_positions returns sparse list (only days with trades?).
        # No, let's look at `_build_daily_positions` again. It iterates keys of grouped trades.
        # So it only returns days with trades. 
        # BUT DetailView filters: `dailyPositions.filter(p => p.time === selectedBarData.time)`.
        # This implies it EXPECTS an entry for that exact day.
        # If the user held a position for 30 days but only traded on day 1 and 30, 
        # the middle days would show 0 if sparse.
        # This seems like a BUG in existing frontend logic if it relies on exact match from sparse list?
        # Or maybe `dailyPositions` was dense?
        # Existing logic: `for date in sorted(grouped.keys())`. Sparse.
        # So the Chart Overlay probably only shows dots on trade days?
        # The Position Ledger PnL panel?
        # Wait, if `posList` is empty, `buy` is 0.
        # So the existing app only shows position ON THE DAY OF TRADE? That seems wrong for "Held Position".
        # But maybe that's the current behavior.
        # User wants "Fix the calculation".
        # If I output a dense list (carried forward), it would fix the chart to show "Holding" line.
        # I will implement DENSE (carry forward) positions for every day since first trade?
        # Or at least for every day in the event range.
        # Actually, let's stick to Sparse (Event Days) + "carry forward" logic is handled where? 
        # If I change to Dense, frontend logic `posList.reduce` will work (1 item).
        # Let's try to be smart. Return a list of all dates where position CHANGED.
        # Frontend logic `dailyPositions.filter(p => p.time === ...)` is strict equality.
        # If I return dense list for all known dates, it acts as full history.
        # But I don't know all "market dates" here easily without querying daily_bars.
        # I'll stick to returning "Event Days" but with "Accumulated Balance".
        # Wait, if I only return Event Days, then on non-event days the chart says 0.
        # The user complained about "Trade History Sync".
        # I will produce a DENSE list by querying daily_bars dates for the code?
        # That might be too heavy.
        # Let's perform a simpler approach: 
        # Just return the Event Days with the *Post-Trade Balance*.
        # And if the frontend needs more, we might need to change frontend.
        # Checking DetailView: `const posList = dailyPositions.filter(p => p.time === selectedBarData.time);`
        # It is strictly checking. So if I don't provide data for that day, it thinks 0.
        # This confirms existing app only shows dots on trade days. 
        # I will keep this behavior (Sparse) but ensure the values are "Total Held After Trade" (Cumulative), 
        # not just "Trade Quantity".
        # Existing `_build_daily_positions` accumulates `long_shares` and `short_shares` in the loop.
        # So it WAS cumulative.
        # My new logic must also be cumulative.
        
        spot = 0.0
        margin_long = 0.0
        margin_short = 0.0
        
        # 0:id, 1:broker, 2:exec_dt, 3:symbol, 4:action, 5:qty, 6:price, 7:hash, 8:created, 9:txn, 10:side
        for ev in ev_list:
             dt_obj = ev[2] # datetime
             date_str = dt_obj.strftime("%Y-%m-%d")
             ts = int(dt_obj.replace(tzinfo=None).timestamp()) 
             # Note: timezone handling might be needed. Data is likely JST/UTC naive.
             # In `main.py`, `daily_bars` dates are unix timestamps.
             # I should align with that. 
             # `daily_bars` uses `date` column (integer YYYYMMDD? No, checking schema).
             # Schema: `date INTEGER`. Usually YYYYMMDD or Unix?
             # `ingest_txt.py`: `daily["date"] = (daily["date"].astype("int64") // 1_000_000_000)` -> Unix Secs.
             # So I need Unix Secs for `time`.
             
             qty = float(ev[5] or 0)
             action = ev[4]
             
             if action in ("SPOT_BUY", "SPOT_IN"): spot += qty
             elif action == "SPOT_SELL": spot -= qty
             elif action == "SPOT_OUT": spot -= qty
             elif action == "MARGIN_OPEN_LONG": margin_long += qty
             elif action == "MARGIN_CLOSE_LONG": margin_long -= qty
             elif action == "MARGIN_OPEN_SHORT": margin_short += qty
             elif action == "MARGIN_CLOSE_SHORT": margin_short -= qty
             elif action == "DELIVERY_SHORT": spot -= qty; margin_short -= qty
             elif action == "MARGIN_SWAP_TO_SPOT": margin_long -= qty; spot += qty
             
             # Append current state
             # Check if we already have an entry for this day? 
             # If multiple trades on same day, we update the last one or append?
             # Frontend uses `filter`, so it handles multiple. But Chart usually wants one.
             # We'll just append.
             
             daily_list.append({
                 "time": ts,
                 "date": date_str,
                 "longLots": spot + margin_long,
                 "shortLots": margin_short,
                 "spotLots": spot,
                 "marginLongLots": margin_long,
                 "marginShortLots": margin_short
             })
             
        result[code] = daily_list
        
    return result
    grouped: dict[str, list[dict]] = {}
    for trade in trades:
        date = trade.get("date")
        if not date:
            continue
        grouped.setdefault(date, []).append(trade)

    long_shares = 0.0
    short_shares = 0.0
    positions: list[dict] = []

    def sort_key(item: dict) -> tuple[int, int]:
        return (item.get("_event_order", 2), item.get("_row_index", 0))

    for date in sorted(grouped.keys()):
        for trade in sorted(grouped[date], key=sort_key):
            qty_shares = float(trade.get("qtyShares") or 0)
            kind = trade.get("kind")
            if kind == "BUY_OPEN":
                long_shares += qty_shares
            elif kind == "SELL_CLOSE":
                long_shares = max(0.0, long_shares - qty_shares)
            elif kind == "SELL_OPEN":
                short_shares += qty_shares
            elif kind == "BUY_CLOSE":
                short_shares = max(0.0, short_shares - qty_shares)
            elif kind == "DELIVERY":
                long_shares = max(0.0, long_shares - qty_shares)
                short_shares = max(0.0, short_shares - qty_shares)
            elif kind == "TAKE_DELIVERY":
                continue
            elif kind == "INBOUND":
                if long_shares > 0 and qty_shares > 0:
                    long_shares += qty_shares
                continue
            elif kind == "OUTBOUND":
                if long_shares > 0 and qty_shares > 0:
                    long_shares = max(0.0, long_shares - qty_shares)
                continue

        positions.append(
            {
                "date": date,
                "buyShares": long_shares,
                "sellShares": short_shares,
                "buyUnits": long_shares / 100,
                "sellUnits": short_shares / 100,
                "text": f"{short_shares/100:g}-{long_shares/100:g}"
            }
        )

    return positions


def _strip_internal(row: dict) -> dict:
    return {key: value for key, value in row.items() if not key.startswith("_")}


def _build_warning_payload(warnings: list[dict], code: str | None = None) -> dict:
    items: list[str] = []
    info: list[str] = []
    unrecognized_count = 0
    unrecognized_samples: list[str] = []

    for warning in warnings:
        warning_code = warning.get("code")
        if code is not None and warning_code not in (None, code):
            continue
        if warning.get("type") == "unrecognized_labels":
            count = int(warning.get("count") or 0)
            samples = warning.get("samples") or []
            unrecognized_count += count
            for sample in samples:
                if sample in unrecognized_samples:
                    continue
                unrecognized_samples.append(sample)
                if len(unrecognized_samples) >= 5:
                    break
        elif warning.get("type") == "duplicate_rows":
            message = warning.get("message") or warning.get("type") or ""
            if message:
                info.append(message)
        else:
            message = warning.get("message") or warning.get("type") or ""
            if message:
                items.append(message)

    payload = {"items": items}
    if info:
        payload["info"] = info
    if unrecognized_count:
        payload["unrecognized_labels"] = {
            "count": unrecognized_count,
            "samples": unrecognized_samples
        }
    return payload


def _parse_daily_date(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        raw = str(int(value)).zfill(8)
        year = int(raw[:4])
        month = int(raw[4:6])
        day = int(raw[6:8])
        return datetime(year, month, day)
    except (ValueError, TypeError):
        return None


def _parse_practice_date(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            numeric = int(value)
        except (TypeError, ValueError):
            return None
        if numeric >= 10_000_000_000_000:
            return datetime.utcfromtimestamp(numeric / 1000)
        if numeric >= 10_000_000_000:
            return datetime.utcfromtimestamp(numeric)
        if numeric >= 10_000_000:
            return _parse_daily_date(numeric)
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            return _parse_practice_date(int(text))
        match = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$", text)
        if match:
            try:
                year = int(match.group(1))
                month = int(match.group(2))
                day = int(match.group(3))
                return datetime(year, month, day)
            except ValueError:
                return None
    return None


def _format_practice_date(value: int | str | None) -> str | None:
    parsed = _parse_practice_date(value)
    if not parsed:
        return None
    return f"{parsed.year:04d}-{parsed.month:02d}-{parsed.day:02d}"


def _resolve_practice_start_date(session_id: str | None, start_date: int | str | None) -> datetime | None:
    if session_id:
        with _get_practice_conn() as conn:
            row = conn.execute(
                "SELECT start_date FROM practice_sessions WHERE session_id = ?",
                [session_id]
            ).fetchone()
        if row and row["start_date"]:
            parsed = _parse_practice_date(row["start_date"])
            if parsed:
                return parsed
    return _parse_practice_date(start_date)


def _parse_month_value(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        raw = str(int(value)).zfill(6)
        year = int(raw[:4])
        month = int(raw[4:6])
        return datetime(year, month, 1)
    except (ValueError, TypeError):
        return None


def _format_month_label(value: int | str | None) -> str | None:
    month = _parse_month_value(value)
    if not month:
        return None
    return f"{month.year:04d}-{month.month:02d}"


def _month_label_to_int(label: str | None) -> int | None:
    if not label:
        return None
    try:
        parts = label.split("-")
        if len(parts) != 2:
            return None
        year = int(parts[0])
        month = int(parts[1])
        if month < 1 or month > 12:
            return None
        return year * 100 + month
    except (TypeError, ValueError):
        return None


def _pct_change(latest: float | None, prev: float | None) -> float | None:
    if latest is None or prev is None:
        return None
    if prev == 0:
        return None
    return (latest - prev) / prev * 100


def _build_weekly_bars(daily_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_key = None
    for row in daily_rows:
        if len(row) < 5:
            continue
        date_value, open_, high, low, close = row[:5]
        if open_ is None or high is None or low is None or close is None:
            continue
        dt = _parse_daily_date(date_value)
        if not dt:
            continue
        week_start = (dt - timedelta(days=dt.weekday())).date()
        if current_key != week_start:
            items.append(
                {
                    "week_start": week_start,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close),
                    "last_date": dt.date()
                }
            )
            current_key = week_start
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
            current["last_date"] = dt.date()
    return items


def _drop_incomplete_weekly(weekly: list[dict], last_daily: datetime | None) -> list[dict]:
    if not weekly or not last_daily:
        return weekly
    last_week_start = (last_daily - timedelta(days=last_daily.weekday())).date()
    if weekly[-1]["week_start"] == last_week_start and last_daily.weekday() < 4:
        return weekly[:-1]
    return weekly


def _drop_incomplete_monthly(monthly_rows: list[tuple], last_daily: datetime | None) -> list[tuple]:
    if not monthly_rows or not last_daily:
        return monthly_rows
    last_month = _parse_month_value(monthly_rows[-1][0] if monthly_rows else None)
    if last_month and last_month.year == last_daily.year and last_month.month == last_daily.month:
        return monthly_rows[:-1]
    return monthly_rows


def _build_quarterly_bars(monthly_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_key: tuple[int, int] | None = None
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        dt = _parse_month_value(month_value)
        if not dt:
            continue
        quarter = (dt.month - 1) // 3 + 1
        key = (dt.year, quarter)
        if current_key != key:
            items.append(
                {
                    "year": dt.year,
                    "quarter": quarter,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close)
                }
            )
            current_key = key
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
    return items


def _build_yearly_bars(monthly_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_year = None
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        dt = _parse_month_value(month_value)
        if not dt:
            continue
        if current_year != dt.year:
            items.append(
                {
                    "year": dt.year,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close)
                }
            )
            current_year = dt.year
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
    return items


def _build_ma_series(values: list[float], period: int) -> list[float | None]:
    if period <= 0:
        return [None for _ in values]
    result: list[float | None] = []
    total = 0.0
    for index, value in enumerate(values):
        total += value
        if index >= period:
            total -= values[index - period]
        if index >= period - 1:
            result.append(total / period)
        else:
            result.append(None)
    return result


def _count_streak(
    values: list[float],
    averages: list[float | None],
    direction: str
) -> int | None:
    count = 0
    opposite = 0
    has_values = False
    for value, avg in zip(values, averages):
        if avg is None:
            continue
        has_values = True
        if direction == "up":
            if value > avg:
                count += 1
                opposite = 0
            elif value < avg:
                opposite += 1
                if opposite >= 2:
                    count = 0
            else:
                opposite = 0
        else:
            if value < avg:
                count += 1
                opposite = 0
            elif value > avg:
                opposite += 1
                if opposite >= 2:
                    count = 0
            else:
                opposite = 0
    return None if not has_values else count


def _build_box_metrics(
    monthly_rows: list[tuple],
    last_close: float | None
) -> tuple[dict | None, str, str | None, str | None, str]:
    if not monthly_rows:
        return None, "NONE", None, None, "NONE"
    boxes = detect_boxes(monthly_rows)
    if not boxes:
        return None, "NONE", None, None, "NONE"

    bars = []
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        if open_ is None or close is None:
            continue
        bars.append(
            {
                "month": month_value,
                "open": float(open_),
                "close": float(close)
            }
        )

    if not bars:
        return None, "NONE", None, None, "NONE"

    latest_box = max(boxes, key=lambda item: item["endIndex"])
    months = latest_box["endIndex"] - latest_box["startIndex"] + 1
    if months < 3:
        return None, "NONE", None, None, "NONE"

    active_box = {**latest_box, "months": months}
    latest_index = len(bars) - 1
    start_index = active_box["startIndex"]
    end_index = active_box["endIndex"]
    body_low = None
    body_high = None
    for bar in bars[start_index : end_index + 1]:
        low = min(bar["open"], bar["close"])
        high = max(bar["open"], bar["close"])
        body_low = low if body_low is None else min(body_low, low)
        body_high = high if body_high is None else max(body_high, high)

    if body_low is None or body_high is None:
        return None, "NONE", None, None, "NONE"

    base = max(abs(body_low), 1e-9)
    range_pct = (body_high - body_low) / base
    start_label = _format_month_label(active_box["startTime"])
    end_label = _format_month_label(active_box["endTime"])

    box_state = "NONE"
    if end_index == latest_index:
        box_state = "IN_BOX"
    elif end_index == latest_index - 1:
        box_state = "JUST_BREAKOUT"

    breakout_month = None
    if box_state == "JUST_BREAKOUT" and latest_index >= 0:
        breakout_month = _format_month_label(bars[latest_index]["month"])

    direction_state = "NONE"
    if box_state != "NONE" and last_close is not None:
        if last_close > body_high:
            direction_state = "BREAKOUT_UP"
        elif last_close < body_low:
            direction_state = "BREAKOUT_DOWN"
        else:
            direction_state = "IN_BOX"

    payload = {
        "startDate": start_label,
        "endDate": end_label,
        "bodyLow": body_low,
        "bodyHigh": body_high,
        "months": active_box["months"],
        "rangePct": range_pct,
        "isActive": box_state == "IN_BOX",
        "boxState": box_state,
        "boxEndMonth": end_label,
        "breakoutMonth": breakout_month
    }
    return payload, box_state, end_label, breakout_month, direction_state


def _load_rank_config() -> dict:
    path = RANK_CONFIG_PATH
    mtime = os.path.getmtime(path) if os.path.isfile(path) else None
    cached = _rank_config_cache.get("config")
    if _rank_config_cache.get("mtime") == mtime and cached is not None:
        return cached
    config: dict = {}
    if mtime is not None:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                config = json.load(handle) or {}
        except (OSError, json.JSONDecodeError):
            config = {}
    _rank_config_cache["mtime"] = mtime
    _rank_config_cache["config"] = config
    return config


def _get_config_value(config: dict, keys: list[str], default):
    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _parse_as_of_date(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.match(r"^\d{8}$", text):
        try:
            year = int(text[:4])
            month = int(text[4:6])
            day = int(text[6:8])
            return datetime(year, month, day)
        except ValueError:
            return None
    if re.match(r"^\d{6}$", text):
        try:
            year = int(text[:4])
            month = int(text[4:6])
            return datetime(year, month, 1)
        except ValueError:
            return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _as_of_int(value: str | None) -> int | None:
    dt = _parse_as_of_date(value)
    if not dt:
        return None
    return dt.year * 10000 + dt.month * 100 + dt.day


def _as_of_month_int(value: str | None) -> int | None:
    dt = _parse_as_of_date(value)
    if not dt:
        return None
    return dt.year * 100 + dt.month


def _format_daily_label(value: int | None) -> str | None:
    if value is None:
        return None
    raw = str(int(value)).zfill(8)
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def _parse_codes_from_text(text: str) -> list[str]:
    codes = re.findall(r"\d{4}", text)
    return sorted(set(codes))


def _load_universe_codes(universe: str | None) -> tuple[list[str], str | None, float | None]:
    if not universe:
        return [], None, None
    key = universe.strip().lower()
    if not key or key in ("all", "*"):
        return [], None, None

    path = None
    if key in ("watchlist", "code", "code.txt"):
        path = find_code_txt_path(DATA_DIR)
    else:
        candidates = [
            os.path.join(DATA_DIR, f"{universe}.txt"),
            os.path.join(os.path.dirname(DATA_DIR), f"{universe}.txt"),
            os.path.join(os.path.dirname(os.path.dirname(DATA_DIR)), f"{universe}.txt")
        ]
        for candidate in candidates:
            if os.path.isfile(candidate):
                path = candidate
                break

    if not path or not os.path.isfile(path):
        return [], None, None

    try:
        with open(path, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        return [], path, None

    codes = _parse_codes_from_text(text)
    mtime = os.path.getmtime(path) if os.path.isfile(path) else None
    return codes, path, mtime


def _resolve_universe_codes(conn, universe: str | None) -> tuple[list[str], dict]:
    all_codes = [row[0] for row in conn.execute(
        "SELECT DISTINCT code FROM daily_bars ORDER BY code"
    ).fetchall()]
    if not universe or universe.strip().lower() in ("", "all", "*"):
        return all_codes, {"source": "all", "requested": universe}

    universe_codes, path, mtime = _load_universe_codes(universe)
    if not universe_codes:
        return all_codes, {"source": "all", "requested": universe, "warning": "universe_not_found"}

    allowed = set(all_codes)
    filtered = [code for code in universe_codes if code in allowed]
    return filtered, {
        "source": "file",
        "requested": universe,
        "path": path,
        "mtime": mtime,
        "missing": len(universe_codes) - len(filtered)
    }


def _group_rows_by_code(rows: list[tuple]) -> dict[str, list[tuple]]:
    grouped: dict[str, list[tuple]] = {}
    for row in rows:
        if not row:
            continue
        code = row[0]
        grouped.setdefault(code, []).append(row[1:])
    return grouped


def _fetch_daily_rows(conn, codes: list[str], as_of: int | None, limit: int) -> dict[str, list[tuple]]:
    if not codes:
        return {}
    placeholders = ",".join(["?"] * len(codes))
    where_clauses = [f"code IN ({placeholders})"]
    params: list = list(codes)
    if as_of is not None:
        where_clauses.append("date <= ?")
        params.append(as_of)
    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT code, date, o, h, l, c, v
        FROM (
            SELECT
                code,
                date,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM daily_bars
            WHERE {where_sql}
        )
        WHERE rn <= ?
        ORDER BY code, date
    """
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return _group_rows_by_code(rows)


def _fetch_monthly_rows(conn, codes: list[str], as_of_month: int | None, limit: int) -> dict[str, list[tuple]]:
    if not codes:
        return {}
    placeholders = ",".join(["?"] * len(codes))
    where_clauses = [f"code IN ({placeholders})"]
    params: list = list(codes)
    if as_of_month is not None:
        where_clauses.append("month <= ?")
        params.append(as_of_month)
    where_sql = " AND ".join(where_clauses)
    query = f"""
        SELECT code, month, o, h, l, c
        FROM (
            SELECT
                code,
                month,
                o,
                h,
                l,
                c,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
            FROM monthly_bars
            WHERE {where_sql}
        )
        WHERE rn <= ?
        ORDER BY code, month
    """
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return _group_rows_by_code(rows)


def _normalize_daily_rows(rows: list[tuple], as_of: int | None) -> list[tuple]:
    by_date: dict[int, tuple] = {}
    for row in rows:
        if len(row) < 6:
            continue
        date_value = row[0]
        if date_value is None:
            continue
        date_int = int(date_value)
        if as_of is not None and date_int > as_of:
            continue
        by_date[date_int] = row
    return [by_date[key] for key in sorted(by_date.keys())]


def _normalize_monthly_rows(rows: list[tuple], as_of_month: int | None) -> list[tuple]:
    by_month: dict[int, tuple] = {}
    for row in rows:
        if len(row) < 5:
            continue
        month_value = row[0]
        if month_value is None:
            continue
        month_int = int(month_value)
        if as_of_month is not None and month_int > as_of_month:
            continue
        by_month[month_int] = row
    return [by_month[key] for key in sorted(by_month.keys())]


def _compute_atr(highs: list[float], lows: list[float], closes: list[float], period: int) -> float | None:
    if len(closes) < 2 or len(closes) != len(highs) or len(closes) != len(lows):
        return None
    trs: list[float] = []
    prev_close = closes[0]
    for high, low, close in zip(highs, lows, closes):
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = close
    if len(trs) < period:
        return None
    window = trs[-period:]
    return sum(window) / period


def _compute_volume_ratio(volumes: list[float], period: int, include_latest: bool) -> float | None:
    if period <= 0:
        return None
    if include_latest:
        if len(volumes) < period:
            return None
        window = volumes[-period:]
    else:
        if len(volumes) < period + 1:
            return None
        window = volumes[-period - 1:-1]
    avg = sum(window) / period if period else 0
    if avg <= 0:
        return None
    latest = volumes[-1]
    return latest / avg


def _calc_slope(values: list[float | None], lookback: int) -> float | None:
    if lookback <= 0 or len(values) <= lookback:
        return None
    current = values[-1]
    past = values[-1 - lookback]
    if current is None or past is None:
        return None
    return float(current) - float(past)


# ============================================================================
# Short-selling screener helper functions
# ============================================================================

def _calc_regression_slope(values: list[float | None], window: int = 5) -> float | None:
    """Calculate regression slope over the last `window` values (simple difference average)."""
    if len(values) < window:
        return None
    recent = values[-window:]
    valid = [v for v in recent if v is not None]
    if len(valid) < 2:
        return None
    # Simple: average of consecutive differences
    diffs = [valid[i + 1] - valid[i] for i in range(len(valid) - 1)]
    return sum(diffs) / len(diffs) if diffs else None


def _calc_gap_down(opens: list[float], prev_closes: list[float]) -> float | None:
    """Calculate gap down = prev_close - current_open (positive means gap down)."""
    if len(opens) < 1 or len(prev_closes) < 1:
        return None
    return prev_closes[-1] - opens[-1]


def _calc_lower_shadow(open_: float, low: float, close: float) -> float:
    """Calculate lower shadow = min(O, C) - L."""
    return min(open_, close) - low


def _calc_body(open_: float, close: float) -> float:
    """Calculate body = |C - O|."""
    return abs(close - open_)


def _calc_range_bounds_with_mid(
    highs: list[float], lows: list[float], lookback: int
) -> tuple[float | None, float | None, float | None]:
    """Calculate (high, low, midpoint) for the range over `lookback` periods."""
    if not highs or not lows:
        return None, None, None
    window_highs = highs[-lookback:] if len(highs) >= lookback else highs
    window_lows = lows[-lookback:] if len(lows) >= lookback else lows
    range_high = max(window_highs)
    range_low = min(window_lows)
    mid = (range_high + range_low) / 2
    return range_high, range_low, mid


def _check_short_prohibition_zones(
    close: float,
    ma20: float | None,
    ma60: float | None,
    slope20: float | None,
    slope60: float | None,
    atr14: float | None,
    range_mid: float | None,
    range_high: float | None,
    range_low: float | None
) -> tuple[str | None, int]:
    """
    Check prohibition zones for short selling.
    Returns (zone_name, penalty_score):
    - Z1: 上昇優位 -> ShortScore = 0 (force)
    - Z2: 末期下げ -> ShortScore = 0 (force)
    - Z3: レンジ中央 -> -30 penalty
    - None: No prohibition
    """
    if ma20 is None or ma60 is None:
        return None, 0

    # Z1: 上昇優位（ネットショート事故ゾーン）
    # 終値 > MA20 かつ MA20傾き > 0（上向き）
    # かつ（終値 > MA60 または MA60傾き > 0）
    if close > ma20 and (slope20 is not None and slope20 > 0):
        if close > ma60 or (slope60 is not None and slope60 > 0):
            return "Z1", -9999  # Force to 0

    # Z2: 末期下げ（利確・触らないゾーン）
    # 終値 < MA20 − 1.2×ATR(14)
    if atr14 is not None and close < ma20 - 1.2 * atr14:
        return "Z2", -9999  # Force to 0

    # Z3: レンジ中央（期待値薄）
    # 直近60日の高安の中点±15%に終値が位置
    if range_mid is not None and range_high is not None and range_low is not None:
        range_band = (range_high - range_low) * 0.15
        if range_mid - range_band <= close <= range_mid + range_band:
            return "Z3", -30  # Penalty

    return None, 0


def _calc_short_a_score(
    closes: list[float],
    opens: list[float],
    lows: list[float],
    ma5_series: list[float | None],
    ma20_series: list[float | None],
    atr14: float | None,
    volumes: list[float],
    avg_volume: float | None,
    down7: int | None,
    highs: list[float]
) -> tuple[int, list[str], list[str]]:
    """
    A型: 反転確定ショート（20割れ2本 + 決定打B/G/M 2/3成立）
    Returns (score, reasons, badges)
    """
    if len(closes) < 3 or len(ma20_series) < 3 or ma20_series[-1] is None:
        return 0, [], []

    close = closes[-1]
    ma20 = ma20_series[-1]
    prev_close = closes[-2]
    prev_ma20 = ma20_series[-2] if len(ma20_series) >= 2 and ma20_series[-2] is not None else None

    # A型の必須条件
    # 1. 終値 < MA20（実体割れ扱い）
    if close >= ma20:
        return 0, [], []

    # 2. 直近2本のうち 2本連続で終値 < MA20（=「20割れ2本」）
    if prev_ma20 is None or prev_close >= prev_ma20:
        return 0, [], []

    # 3. 下げの決定打（B/G/M の 2/3成立）
    decisive_count = 0
    reasons: list[str] = []
    badges: list[str] = ["20割れ2本"]

    # B（大陰線）：|C−O| ≥ 0.8×ATR(14) かつ 下ヒゲ ≤ 0.25×実体
    b_condition = False
    if atr14 is not None and len(opens) >= 1:
        body = _calc_body(opens[-1], close)
        lower_shadow = _calc_lower_shadow(opens[-1], lows[-1], close)
        if body >= 0.8 * atr14 and close < opens[-1]:  # Bearish candle
            if body > 0 and lower_shadow <= 0.25 * body:
                b_condition = True
                decisive_count += 1
                reasons.append("大陰線")

    # G（ギャップダウン）：GD幅 ≥ 0.5×ATR(14)
    g_condition = False
    if atr14 is not None and len(closes) >= 2:
        gap_down = closes[-2] - opens[-1]  # Previous close - current open
        if gap_down >= 0.5 * atr14:
            g_condition = True
            decisive_count += 1
            reasons.append("ギャップダウン")

    # M：終値 < MA5
    m_condition = False
    if ma5_series and len(ma5_series) >= 1 and ma5_series[-1] is not None:
        if close < ma5_series[-1]:
            m_condition = True
            decisive_count += 1
            reasons.append("MA5下")

    # B/G/Mの2/3成立が必須
    if decisive_count < 2:
        return 0, [], []

    badges.append("B/G/M")

    # ベーススコア: 70点
    score = 70

    # 加点
    if b_condition:
        score += 25  # B成立 +25

    if g_condition:
        score += 20  # G成立 +20
        if b_condition:
            score += 10  # B+Gならさらに +10

    if m_condition:
        score += 10  # M成立 +10

    # 出来高≥20日平均 +10
    if avg_volume is not None and len(volumes) >= 1 and avg_volume > 0:
        if volumes[-1] >= avg_volume:
            score += 10
            reasons.append("出来高増")

    # 直近10日安値を終値で更新 +10
    if len(lows) >= 10:
        recent_low = min(lows[-10:-1]) if len(lows) > 1 else lows[-1]
        if close < recent_low:
            score += 10
            reasons.append("安値更新")

    # 7下本数が1〜3本目 +5（下げ初動を優先）
    if down7 is not None and 1 <= down7 <= 3:
        score += 5
        reasons.append(f"下げ初動（{down7}本目）")

    # 減点
    # 終値がMA20から乖離（終値 < MA20 − 1.0×ATR） -15
    if atr14 is not None and close < ma20 - 1.0 * atr14:
        score -= 15
        reasons.append("MA20乖離大")

    badges.insert(0, "反転確定")
    return max(0, score), reasons, badges


def _calc_short_b_score(
    closes: list[float],
    opens: list[float],
    lows: list[float],
    ma5_series: list[float | None],
    ma20_series: list[float | None],
    ma60_series: list[float | None],
    slope20: float | None,
    slope60: float | None,
    atr14: float | None,
    volumes: list[float],
    avg_volume: float | None,
    down20: int | None,
    ma7_series: list[float | None]
) -> tuple[int, list[str], list[str]]:
    """
    B型: 下落トレンドの戻り売り（MA60下向き + 戻り失速）
    Returns (score, reasons, badges)
    """
    if len(closes) < 5 or len(ma60_series) < 5 or ma60_series[-1] is None:
        return 0, [], []

    close = closes[-1]
    ma20 = ma20_series[-1] if ma20_series and ma20_series[-1] is not None else None
    ma60 = ma60_series[-1]

    # B型の必須条件
    # 1. MA60傾き < 0（下向き）
    if slope60 is None or slope60 >= 0:
        return 0, [], []

    # 2. 終値 < MA60
    if close >= ma60:
        return 0, [], []

    # 3. 終値 < MA20
    if ma20 is not None and close >= ma20:
        return 0, [], []

    # 4.「戻り失速」判定
    pullback_stall = False
    reasons: list[str] = []

    # 直近5本以内に終値がMA7〜MA20帯に接近→その後2本以内で終値<MA5
    ma7 = ma7_series[-1] if ma7_series and len(ma7_series) >= 1 and ma7_series[-1] is not None else None
    ma5 = ma5_series[-1] if ma5_series and len(ma5_series) >= 1 and ma5_series[-1] is not None else None

    if ma7 is not None and ma20 is not None and ma5 is not None:
        # Check if price approached MA7-MA20 band in last 5 bars
        for i in range(-5, 0):
            if abs(i) > len(closes) or abs(i) > len(ma7_series) or abs(i) > len(ma20_series):
                continue
            past_close = closes[i]
            past_ma7 = ma7_series[i] if ma7_series[i] is not None else None
            past_ma20 = ma20_series[i] if ma20_series[i] is not None else None
            if past_ma7 is not None and past_ma20 is not None:
                band_low = min(past_ma7, past_ma20)
                band_high = max(past_ma7, past_ma20)
                if band_low <= past_close <= band_high:
                    # Check if current close < MA5
                    if close < ma5:
                        pullback_stall = True
                        reasons.append("戻り失速")
                        break

    # Alternative: 陰線実体 + 翌日安値更新
    if not pullback_stall and len(closes) >= 2 and len(opens) >= 2:
        prev_bearish = closes[-2] < opens[-2]  # Previous bar was bearish
        low_break = lows[-1] < lows[-2] if len(lows) >= 2 else False
        if prev_bearish and low_break:
            pullback_stall = True
            reasons.append("陰線後安値更新")

    if not pullback_stall:
        return 0, [], []

    badges: list[str] = ["戻り売り"]

    # ベーススコア: 60点
    score = 60

    # 加点
    # MA20傾き < 0 +15
    if slope20 is not None and slope20 < 0:
        score += 15
        reasons.append("MA20下向き")

    # 20下本数が10本以上 +10
    if down20 is not None and down20 >= 10:
        score += 10
        reasons.append(f"下落明確（{down20}本）")

    # 前安値ラインを実体で割る（終値で前安値割れ） +20
    if len(lows) >= 11:
        prev_low = min(lows[-11:-1]) if len(lows) > 1 else lows[-1]
        if close < prev_low:
            score += 20
            reasons.append("前安値割れ")

    # 出来高≥20日平均 +10
    if avg_volume is not None and len(volumes) >= 1 and avg_volume > 0:
        if volumes[-1] >= avg_volume:
            score += 10
            reasons.append("出来高増")

    # 7MA上に戻しても1〜2本で失速（戻り弱） +10
    if ma7 is not None and len(closes) >= 3:
        was_above_ma7 = False
        for i in range(-3, -1):
            if abs(i) <= len(closes) and abs(i) <= len(ma7_series):
                past_close = closes[i]
                past_ma7 = ma7_series[i] if ma7_series[i] is not None else None
                if past_ma7 is not None and past_close > past_ma7:
                    was_above_ma7 = True
                    break
        if was_above_ma7 and close < ma7:
            score += 10
            reasons.append("戻り弱")

    # 減点
    # 末期（終値 < MA20 − 1.2×ATR） -30 (Z2は既にチェック済みだが、ここでもペナルティ)
    if ma20 is not None and atr14 is not None and close < ma20 - 1.2 * atr14:
        score -= 30
        reasons.append("末期警戒")

    return max(0, score), reasons, badges


def _calc_recent_bounds(highs: list[float], lows: list[float], lookback: int) -> tuple[float | None, float | None]:
    if not highs or not lows:
        return None, None
    if lookback <= 0:
        return max(highs), min(lows)
    window_highs = highs[-lookback:] if len(highs) >= lookback else highs
    window_lows = lows[-lookback:] if len(lows) >= lookback else lows
    return max(window_highs), min(window_lows)


def _detect_body_box(monthly_rows: list[tuple], config: dict) -> dict | None:
    thresholds = _get_config_value(config, ["monthly", "thresholds"], {})
    min_months = int(thresholds.get("min_months", 3))
    max_months = int(thresholds.get("max_months", 14))
    max_range_pct = float(thresholds.get("max_range_pct", 0.2))
    wild_wick_pct = float(thresholds.get("wild_wick_pct", 0.1))

    bars: list[dict] = []
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        if month_value is None or open_ is None or high is None or low is None or close is None:
            continue
        body_high = max(float(open_), float(close))
        body_low = min(float(open_), float(close))
        bars.append(
            {
                "time": int(month_value),
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "body_high": body_high,
                "body_low": body_low
            }
        )

    if len(bars) < min_months:
        return None

    bars.sort(key=lambda item: item["time"])
    max_months = min(max_months, len(bars))

    for length in range(max_months, min_months - 1, -1):
        window = bars[-length:]
        upper = max(item["body_high"] for item in window)
        lower = min(item["body_low"] for item in window)
        base = max(abs(lower), 1e-9)
        range_pct = (upper - lower) / base
        if range_pct > max_range_pct:
            continue
        wild = False
        for item in window:
            if item["high"] > upper * (1 + wild_wick_pct) or item["low"] < lower * (1 - wild_wick_pct):
                wild = True
                break
        return {
            "start": window[0]["time"],
            "end": window[-1]["time"],
            "upper": upper,
            "lower": lower,
            "months": length,
            "range_pct": range_pct,
            "wild": wild,
            "last_close": window[-1]["close"]
        }

    return None


def _score_weekly_candidate(code: str, name: str, rows: list[tuple], config: dict, as_of: int | None) -> tuple[dict | None, dict | None, str | None]:
    rows = _normalize_daily_rows(rows, as_of)
    common = _get_config_value(config, ["common"], {})
    min_bars = int(common.get("min_daily_bars", 80))
    if len(rows) < min_bars:
        return None, None, "insufficient_daily_bars"

    dates = [int(row[0]) for row in rows]
    opens = [float(row[1]) for row in rows]
    highs = [float(row[2]) for row in rows]
    lows = [float(row[3]) for row in rows]
    closes = [float(row[4]) for row in rows]
    volumes = [float(row[5]) if row[5] is not None else 0.0 for row in rows]

    close = closes[-1] if closes else None
    if close is None:
        return None, None, "missing_close"

    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma100_series = _build_ma_series(closes, 100)
    ma200_series = _build_ma_series(closes, 200)

    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None
    ma100 = ma100_series[-1] if ma100_series else None
    ma200 = ma200_series[-1] if ma200_series else None
    if ma20 is None or ma60 is None:
        return None, None, "missing_ma"
    if ma100 is None or ma200 is None:
        return None, None, "missing_ma_long_term"

    slope_lookback = int(common.get("slope_lookback", 3))
    slope20 = _calc_slope(ma20_series, slope_lookback)
    slope100 = _calc_slope(ma100_series, slope_lookback)
    slope200 = _calc_slope(ma200_series, slope_lookback)

    atr_period = int(common.get("atr_period", 14))
    atr14 = _compute_atr(highs, lows, closes, atr_period)

    volume_period = int(common.get("volume_period", 20))
    include_latest = common.get("volume_ratio_mode", "exclude_latest") == "include_latest"
    volume_ratio = _compute_volume_ratio(volumes, volume_period, include_latest)

    up7 = _count_streak(closes, ma7_series, "up")
    down7 = _count_streak(closes, ma7_series, "down")

    trigger_lookback = int(common.get("trigger_lookback", 20))
    recent_high, recent_low = _calc_recent_bounds(highs, lows, trigger_lookback)
    break_up_pct = None
    break_down_pct = None
    if recent_high is not None and close:
        break_up_pct = max(0.0, (recent_high - close) / close * 100)
    if recent_low is not None and close:
        break_down_pct = max(0.0, (close - recent_low) / close * 100)

    weekly = _get_config_value(config, ["weekly"], {})
    weights = weekly.get("weights", {})
    thresholds = weekly.get("thresholds", {})
    down_weights = weekly.get("down_weights", {})
    down_thresholds = weekly.get("down_thresholds", {})
    max_reasons = int(common.get("max_reasons", 6))

    up_reasons: list[tuple[float, str]] = []
    down_reasons: list[tuple[float, str]] = []
    up_badges: list[str] = []
    down_badges: list[str] = []
    up_score = 0.0
    down_score = 0.0

    def push_reason(target: list[tuple[float, str]], weight: float, label: str):
        if weight:
            target.append((weight, label))

    def push_badge(target: list[str], label: str):
        if label and label not in target:
            target.append(label)

    if close > ma20 and ma20 > ma60:
        weight = float(weights.get("ma_alignment", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA20 > MA60")
        push_badge(up_badges, "MA整列")

    if ma60 > ma100:
        weight = float(weights.get("ma_alignment_100", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA60 > MA100")

    if ma100 > ma200:
        weight = float(weights.get("ma_alignment_200", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA100 > MA200")

    if close > ma100:
        weight = float(weights.get("obs_above_ma100", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA100より上")
    
    if close > ma200:
        weight = float(weights.get("obs_above_ma200", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA200より上")

    pull_min = int(thresholds.get("pullback_down7_min", 1))
    pull_max = int(thresholds.get("pullback_down7_max", 2))
    slope_min = float(thresholds.get("slope_min", 0))
    if close > ma20 and down7 is not None and pull_min <= down7 <= pull_max:
        if slope20 is None or slope20 >= slope_min:
            weight = float(weights.get("pullback_above_ma20", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA20上で押し目（下{down7}本）")
            push_badge(up_badges, "押し目")

    vol_thresh = float(thresholds.get("volume_ratio", 1.5))
    if volume_ratio is not None and volume_ratio >= vol_thresh:
        weight = float(weights.get("volume_spike", 0))
        up_score += weight
        push_reason(up_reasons, weight, f"出来高増（20日比{volume_ratio:.2f}倍）")
        push_badge(up_badges, "出来高増")

    near_pct = float(thresholds.get("near_break_pct", 2.0))
    if break_up_pct is not None and break_up_pct <= near_pct:
        weight = float(weights.get("near_high_break", 0))
        up_score += weight
        push_reason(up_reasons, weight, f"高値ブレイク接近（{break_up_pct:.1f}%）")
        push_badge(up_badges, "高値接近")

    if slope20 is not None and slope20 >= slope_min:
        weight = float(weights.get("slope_up", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA20上向き")
        push_badge(up_badges, "MA上向き")

    if slope100 is not None and slope100 >= slope_min:
        weight = float(weights.get("slope_up_100", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA100上向き")

    if slope200 is not None and slope200 >= slope_min:
        weight = float(weights.get("slope_up_200", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA200上向き")

    big_candle = float(thresholds.get("big_candle_atr", 1.2))
    if atr14 is not None and abs(close - opens[-1]) >= atr14 * big_candle and close > opens[-1]:
        weight = float(weights.get("big_bull_candle", 0))
        up_score += weight
        push_reason(up_reasons, weight, "強い陽線")
        push_badge(up_badges, "陽線強")

    ma20_dist = float(thresholds.get("ma20_distance_pct", 2.0))
    if ma20:
        dist_pct = abs(close - ma20) / ma20 * 100
        if close >= ma20 and dist_pct <= ma20_dist:
            weight = float(weights.get("ma20_support", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA20近接（{dist_pct:.1f}%）")
            push_badge(up_badges, "MA20近接")

    ma100_thresh = float(thresholds.get("ma100_distance_pct", 3.0))
    if close >= ma100:
        dist100 = abs(close - ma100) / ma100 * 100
        if dist100 <= ma100_thresh:
            weight = float(weights.get("ma100_support", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA100近接（{dist100:.1f}%）")

    ma200_thresh = float(thresholds.get("ma200_distance_pct", 3.0))
    if close >= ma200:
        dist200 = abs(close - ma200) / ma200 * 100
        if dist200 <= ma200_thresh:
            weight = float(weights.get("ma200_support", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA200近接（{dist200:.1f}%）")

    if close < ma20 and ma20 < ma60:
        weight = float(down_weights.get("ma_alignment", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA20 < MA60")
        push_badge(down_badges, "MA逆転")

    if ma60 < ma100:
        weight = float(down_weights.get("ma_alignment_100", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA60 < MA100")

    if ma100 < ma200:
        weight = float(down_weights.get("ma_alignment_200", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA100 < MA200")

    if close < ma100:
        weight = float(down_weights.get("obs_below_ma100", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA100より下")
    
    if close < ma200:
        weight = float(down_weights.get("obs_below_ma200", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA200より下")

    pull_min = int(down_thresholds.get("pullback_up7_min", 1))
    pull_max = int(down_thresholds.get("pullback_up7_max", 2))
    slope_max = float(down_thresholds.get("slope_max", 0))
    if close < ma20 and up7 is not None and pull_min <= up7 <= pull_max:
        if slope20 is None or slope20 <= slope_max:
            weight = float(down_weights.get("pullback_below_ma20", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA20下で戻り（上{up7}本）")
            push_badge(down_badges, "戻り")

    vol_thresh = float(down_thresholds.get("volume_ratio", vol_thresh))
    if volume_ratio is not None and volume_ratio >= vol_thresh:
        weight = float(down_weights.get("volume_spike", 0))
        down_score += weight
        push_reason(down_reasons, weight, f"出来高増（20日比{volume_ratio:.2f}倍）")
        push_badge(down_badges, "出来高増")

    near_pct = float(down_thresholds.get("near_break_pct", near_pct))
    if break_down_pct is not None and break_down_pct <= near_pct:
        weight = float(down_weights.get("near_low_break", 0))
        down_score += weight
        push_reason(down_reasons, weight, f"安値ブレイク接近（{break_down_pct:.1f}%）")
        push_badge(down_badges, "安値接近")

    if slope20 is not None and slope20 <= slope_max:
        weight = float(down_weights.get("slope_down", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA20下向き")
        push_badge(down_badges, "MA下向き")

    if slope100 is not None and slope100 <= slope_max:
        weight = float(down_weights.get("slope_down_100", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA100下向き")

    if slope200 is not None and slope200 <= slope_max:
        weight = float(down_weights.get("slope_down_200", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA200下向き")

    big_candle = float(down_thresholds.get("big_candle_atr", big_candle))
    if atr14 is not None and abs(close - opens[-1]) >= atr14 * big_candle and close < opens[-1]:
        weight = float(down_weights.get("big_bear_candle", 0))
        down_score += weight
        push_reason(down_reasons, weight, "強い陰線")
        push_badge(down_badges, "陰線強")

    ma20_dist = float(down_thresholds.get("ma20_distance_pct", ma20_dist))
    if ma20:
        dist_pct = abs(close - ma20) / ma20 * 100
        if close <= ma20 and dist_pct <= ma20_dist:
            weight = float(down_weights.get("ma20_resistance", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA20近接（{dist_pct:.1f}%）")
            push_badge(down_badges, "MA20近接")

    ma100_thresh = float(down_thresholds.get("ma100_distance_pct", 3.0))
    if close <= ma100:
        dist100 = abs(close - ma100) / ma100 * 100
        if dist100 <= ma100_thresh:
            weight = float(down_weights.get("ma100_resistance", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA100近接（{dist100:.1f}%）")

    ma200_thresh = float(down_thresholds.get("ma200_distance_pct", 3.0))
    if close <= ma200:
        dist200 = abs(close - ma200) / ma200 * 100
        if dist200 <= ma200_thresh:
            weight = float(down_weights.get("ma200_resistance", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA200近接（{dist200:.1f}%）")

    up_reasons.sort(key=lambda item: item[0], reverse=True)
    down_reasons.sort(key=lambda item: item[0], reverse=True)

    levels = {
        "close": close,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "atr14": atr14,
        "volume_ratio": volume_ratio
    }

    chart_hint = {
        "lines": {
            "ma20": ma20,
            "ma60": ma60,
            "ma100": ma100,
            "ma200": ma200,
            "recent_high": recent_high,
            "recent_low": recent_low
        }
    }

    as_of_label = _format_daily_label(dates[-1])
    series_bars = int(common.get("rank_series_bars", 60))
    series_rows = rows[-series_bars:] if series_bars > 0 else rows
    series = [
        [int(item[0]), float(item[1]), float(item[2]), float(item[3]), float(item[4])]
        for item in series_rows
    ]

    base = {
        "code": code,
        "name": name or code,
        "as_of": as_of_label,
        "levels": levels,
        "series": series,
        "distance_to_trigger": {
            "break_up_pct": break_up_pct,
            "break_down_pct": break_down_pct
        },
        "chart_hint": chart_hint
    }

    up_item = {
        **base,
        "total_score": round(up_score, 3),
        "reasons": [label for _, label in up_reasons[:max_reasons]],
        "badges": up_badges[:max_reasons]
    }
    down_item = {
        **base,
        "total_score": round(down_score, 3),
        "reasons": [label for _, label in down_reasons[:max_reasons]],
        "badges": down_badges[:max_reasons]
    }

    return up_item, down_item, None


def _score_monthly_candidate(code: str, name: str, rows: list[tuple], config: dict, as_of_month: int | None) -> tuple[dict | None, str | None]:
    rows = _normalize_monthly_rows(rows, as_of_month)
    thresholds = _get_config_value(config, ["monthly", "thresholds"], {})
    min_months = int(thresholds.get("min_months", 3))
    if len(rows) < min_months:
        return None, "insufficient_monthly_bars"

    box = _detect_body_box(rows, config)
    if not box:
        return None, "no_box"

    weights = _get_config_value(config, ["monthly", "weights"], {})
    max_reasons = int(_get_config_value(config, ["common", "max_reasons"], 6))
    near_edge_pct = float(thresholds.get("near_edge_pct", 4.0))
    wild_penalty = float(weights.get("wild_box_penalty", 0))

    close = float(box["last_close"])
    upper = float(box["upper"])
    lower = float(box["lower"])
    break_up_pct = max(0.0, (upper - close) / close * 100) if close else None
    break_down_pct = max(0.0, (close - lower) / close * 100) if close else None
    edge_pct = None
    if break_up_pct is not None and break_down_pct is not None:
        edge_pct = min(break_up_pct, break_down_pct)

    reasons: list[tuple[float, str]] = []
    score = 0.0

    months = int(box["months"])
    weight_month = float(weights.get("box_months", 0))
    if weight_month:
        score += weight_month * months
        reasons.append((weight_month, f"箱の期間{months}か月"))

    if edge_pct is not None and edge_pct <= near_edge_pct:
        weight = float(weights.get("near_edge", 0))
        ratio = 1 - edge_pct / near_edge_pct if near_edge_pct else 1
        score += weight * ratio
        if break_up_pct is not None and break_down_pct is not None:
            if break_up_pct <= break_down_pct:
                reasons.append((weight, f"上抜けまで{break_up_pct:.1f}%"))
            else:
                reasons.append((weight, f"下抜けまで{break_down_pct:.1f}%"))

    if box["wild"] and wild_penalty:
        score += wild_penalty
        reasons.append((wild_penalty, "荒れ箱"))

    closes = [float(row[4]) for row in rows if len(row) >= 5 and row[4] is not None]
    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None

    # New Logic: MA Alignment for Monthly
    if ma7 and ma20 and ma60:
        if ma7 > ma20 and ma20 > ma60:
            w_order = float(weights.get("ma_order_7_20_60", 0))
            score += w_order
            reasons.append((w_order, "月足MA配列(7>20>60)"))

        # Simple slope using last 2 points
        s7 = ma7_series[-1] - ma7_series[-2] if len(ma7_series) > 1 else 0
        s20 = ma20_series[-1] - ma20_series[-2] if len(ma20_series) > 1 else 0
        if s7 > 0 and s20 > 0:
            w_slopes = float(weights.get("ma_slopes_up", 0))
            score += w_slopes
            reasons.append((w_slopes, "月足MA上昇"))

    reasons.sort(key=lambda item: item[0], reverse=True)

    levels = {
        "close": close,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "atr14": None
    }

    chart_hint = {
        "lines": {
            "box_upper": upper,
            "box_lower": lower,
            "ma20": ma20
        }
    }

    return {
        "code": code,
        "name": name or code,
        "as_of": _format_month_label(box["end"]),
        "total_score": round(score, 3),
        "reasons": [label for _, label in reasons[:max_reasons]],
        "levels": levels,
        "distance_to_trigger": {
            "break_up_pct": break_up_pct,
            "break_down_pct": break_down_pct
        },
        "box_info": {
            "box_start": _format_month_label(box["start"]),
            "box_end": _format_month_label(box["end"]),
            "box_upper_body": upper,
            "box_lower_body": lower,
            "box_months": months,
            "wild_box_flag": box["wild"],
            "range_pct": box["range_pct"]
        },
        "box_start": _format_month_label(box["start"]),
        "box_end": _format_month_label(box["end"]),
        "box_upper_body": upper,
        "box_lower_body": lower,
        "box_months": months,
        "wild_box_flag": box["wild"],
        "chart_hint": chart_hint
    }, None


def _rank_cache_key(as_of: str | None, limit: int, universe_meta: dict) -> str:
    uni_key = universe_meta.get("path") or universe_meta.get("requested") or "all"
    mtime = universe_meta.get("mtime")
    return f"{as_of or 'latest'}|{limit}|{uni_key}|{mtime or 'none'}"


def _ensure_rank_cache_state() -> tuple[float | None, float | None]:
    db_mtime = os.path.getmtime(DEFAULT_DB_PATH) if os.path.isfile(DEFAULT_DB_PATH) else None
    config_mtime = _rank_config_cache.get("mtime")
    if _rank_cache.get("mtime") != db_mtime or _rank_cache.get("config_mtime") != config_mtime:
        _rank_cache["weekly"] = {}
        _rank_cache["monthly"] = {}
        _rank_cache["mtime"] = db_mtime
        _rank_cache["config_mtime"] = config_mtime
    return db_mtime, config_mtime


def _build_weekly_ranking(as_of: str | None, limit: int, universe: str | None) -> dict:
    start = time.perf_counter()
    config = _load_rank_config()
    _ensure_rank_cache_state()
    as_of_int = _as_of_int(as_of)
    common = _get_config_value(config, ["common"], {})
    max_bars = int(common.get("max_daily_bars", 260))

    with get_conn() as conn:
        codes, universe_meta = _resolve_universe_codes(conn, universe)
        if not codes:
            return {"up": [], "down": [], "meta": {"as_of": as_of, "count": 0, "errors": []}}
        cache_key = _rank_cache_key(as_of, limit, universe_meta)
        cached = _rank_cache["weekly"].get(cache_key)
        if cached:
            return cached
        meta_rows = conn.execute(
            f"SELECT code, name FROM stock_meta WHERE code IN ({','.join(['?'] * len(codes))})",
            codes
        ).fetchall()
        name_map = {row[0]: row[1] for row in meta_rows}
        daily_map = _fetch_daily_rows(conn, codes, as_of_int, max_bars)

    up_items: list[dict] = []
    down_items: list[dict] = []
    skipped: list[dict] = []

    for code in codes:
        rows = daily_map.get(code, [])
        up_item, down_item, skip_reason = _score_weekly_candidate(code, name_map.get(code, code), rows, config, as_of_int)
        if skip_reason:
            skipped.append({"code": code, "reason": skip_reason})
            continue
        if up_item:
            up_items.append(up_item)
        if down_item:
            down_items.append(down_item)

    up_items.sort(key=lambda item: item.get("total_score", 0), reverse=True)
    down_items.sort(key=lambda item: item.get("total_score", 0), reverse=True)

    elapsed = (time.perf_counter() - start) * 1000
    print(f"[rank_weekly] codes={len(codes)} skipped={len(skipped)} ms={elapsed:.1f}")

    result = {
        "up": up_items[:limit],
        "down": down_items[:limit],
        "meta": {
            "as_of": as_of,
            "count": len(codes),
            "skipped": skipped,
            "elapsed_ms": round(elapsed, 2),
            "universe": universe_meta,
            "errors": []
        }
    }
    _rank_cache["weekly"][cache_key] = result
    return result


def _build_monthly_ranking(as_of: str | None, limit: int, universe: str | None) -> dict:
    start = time.perf_counter()
    config = _load_rank_config()
    _ensure_rank_cache_state()
    as_of_month = _as_of_month_int(as_of)
    common = _get_config_value(config, ["common"], {})
    max_bars = int(common.get("max_monthly_bars", 120))

    with get_conn() as conn:
        codes, universe_meta = _resolve_universe_codes(conn, universe)
        if not codes:
            return {"box": [], "meta": {"as_of": as_of, "count": 0, "errors": []}}
        cache_key = _rank_cache_key(as_of, limit, universe_meta)
        cached = _rank_cache["monthly"].get(cache_key)
        if cached:
            return cached
        meta_rows = conn.execute(
            f"SELECT code, name FROM stock_meta WHERE code IN ({','.join(['?'] * len(codes))})",
            codes
        ).fetchall()
        name_map = {row[0]: row[1] for row in meta_rows}
        monthly_map = _fetch_monthly_rows(conn, codes, as_of_month, max_bars)

    items: list[dict] = []
    skipped: list[dict] = []

    for code in codes:
        rows = monthly_map.get(code, [])
        item, skip_reason = _score_monthly_candidate(code, name_map.get(code, code), rows, config, as_of_month)
        if skip_reason:
            skipped.append({"code": code, "reason": skip_reason})
            continue
        if item:
            items.append(item)

    items.sort(key=lambda item: item.get("total_score", 0), reverse=True)
    elapsed = (time.perf_counter() - start) * 1000
    print(f"[rank_monthly] codes={len(codes)} skipped={len(skipped)} ms={elapsed:.1f}")

    result = {
        "box": items[:limit],
        "meta": {
            "as_of": as_of,
            "count": len(codes),
            "skipped": skipped,
            "elapsed_ms": round(elapsed, 2),
            "universe": universe_meta,
            "errors": []
        }
    }
    _rank_cache["monthly"][cache_key] = result
    return result


def _compute_screener_metrics(
    daily_rows: list[tuple],
    monthly_rows: list[tuple]
) -> dict:
    reasons: list[str] = []
    daily_rows = sorted(daily_rows, key=lambda item: item[0])
    monthly_rows = sorted(monthly_rows, key=lambda item: item[0])

    last_daily = _parse_daily_date(daily_rows[-1][0]) if daily_rows else None
    closes = [float(row[4]) for row in daily_rows if len(row) >= 5 and row[4] is not None]
    opens = [float(row[1]) for row in daily_rows if len(row) >= 5 and row[1] is not None]
    highs = [float(row[2]) for row in daily_rows if len(row) >= 5 and row[2] is not None]
    lows = [float(row[3]) for row in daily_rows if len(row) >= 5 and row[3] is not None]
    volumes = [float(row[5]) if len(row) >= 6 and row[5] is not None else 0.0 for row in daily_rows]
    last_close = closes[-1] if closes else None
    if last_close is None:
        reasons.append("missing_last_close")

    chg1d = _pct_change(closes[-1], closes[-2]) if len(closes) >= 2 else None

    weekly = _build_weekly_bars(daily_rows)
    weekly = _drop_incomplete_weekly(weekly, last_daily)
    weekly_closes = [item["c"] for item in weekly]
    chg1w = _pct_change(weekly_closes[-1], weekly_closes[-2]) if len(weekly_closes) >= 2 else None
    prev_week_chg = _pct_change(weekly_closes[-2], weekly_closes[-3]) if len(weekly_closes) >= 3 else None

    confirmed_monthly = _drop_incomplete_monthly(monthly_rows, last_daily)
    monthly_closes = [float(row[4]) for row in confirmed_monthly if len(row) >= 5 and row[4] is not None]
    chg1m = _pct_change(monthly_closes[-1], monthly_closes[-2]) if len(monthly_closes) >= 2 else None
    prev_month_chg = _pct_change(monthly_closes[-2], monthly_closes[-3]) if len(monthly_closes) >= 3 else None

    quarterly = _build_quarterly_bars(confirmed_monthly)
    quarterly_closes = [item["c"] for item in quarterly]
    chg1q = _pct_change(quarterly_closes[-1], quarterly_closes[-2]) if len(quarterly_closes) >= 2 else None
    prev_quarter_chg = _pct_change(quarterly_closes[-2], quarterly_closes[-3]) if len(quarterly_closes) >= 3 else None

    yearly = _build_yearly_bars(confirmed_monthly)
    yearly_closes = [item["c"] for item in yearly]
    chg1y = _pct_change(yearly_closes[-1], yearly_closes[-2]) if len(yearly_closes) >= 2 else None
    prev_year_chg = _pct_change(yearly_closes[-2], yearly_closes[-3]) if len(yearly_closes) >= 3 else None

    ma5_series = _build_ma_series(closes, 5)
    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma100_series = _build_ma_series(closes, 100)

    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None
    ma100 = ma100_series[-1] if ma100_series else None

    prev_ma20 = ma20_series[-2] if len(ma20_series) >= 2 else None
    slope20 = ma20 - prev_ma20 if ma20 is not None and prev_ma20 is not None else None

    # Calculate regression slopes for short-selling (5-bar average of differences)
    slope20_reg = _calc_regression_slope(ma20_series, 5)
    slope60_reg = _calc_regression_slope(ma60_series, 5)

    # Calculate ATR(14) for short-selling
    atr14 = _compute_atr(highs, lows, closes, 14)

    # Calculate 20-day volume average
    volume_avg_20 = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else None

    up7 = _count_streak(closes, ma7_series, "up")
    down7 = _count_streak(closes, ma7_series, "down")
    up20 = _count_streak(closes, ma20_series, "up")
    down20 = _count_streak(closes, ma20_series, "down")
    up60 = _count_streak(closes, ma60_series, "up")
    down60 = _count_streak(closes, ma60_series, "down")
    up100 = _count_streak(closes, ma100_series, "up")
    down100 = _count_streak(closes, ma100_series, "down")

    if ma20 is None:
        reasons.append("missing_ma20")
    if ma60 is None:
        reasons.append("missing_ma60")
    if ma100 is None:
        reasons.append("missing_ma100")
    if chg1m is None:
        reasons.append("missing_chg1m")
    if chg1q is None:
        reasons.append("missing_chg1q")
    if chg1y is None:
        reasons.append("missing_chg1y")

    box_monthly, box_state, box_end_month, breakout_month, box_direction = _build_box_metrics(
        monthly_rows, last_close
    )

    latest_month_label = _format_month_label(confirmed_monthly[-1][0]) if confirmed_monthly else None
    prev_month_label = _format_month_label(confirmed_monthly[-2][0]) if len(confirmed_monthly) >= 2 else None
    latest_month_value = _month_label_to_int(latest_month_label)
    prev_month_value = _month_label_to_int(prev_month_label)
    box_active = False
    if box_monthly:
        box_start_value = _month_label_to_int(box_monthly.get("startDate"))
        box_end_value = _month_label_to_int(box_monthly.get("endDate"))
        if box_start_value is not None and box_end_value is not None:
            if latest_month_value is not None and box_start_value <= latest_month_value <= box_end_value:
                box_active = True
            elif prev_month_value is not None and box_start_value <= prev_month_value <= box_end_value:
                box_active = True

    monthly_ma7_series = _build_ma_series(monthly_closes, 7)
    monthly_ma20_series = _build_ma_series(monthly_closes, 20)
    monthly_down20 = _count_streak(monthly_closes, monthly_ma20_series, "down")
    bottom_zone = bool(monthly_down20 is not None and monthly_down20 >= 6)

    weekly_closes = [item["c"] for item in weekly]
    weekly_highs = [item["h"] for item in weekly]
    weekly_lows = [item["l"] for item in weekly]
    weekly_ma7_series = _build_ma_series(weekly_closes, 7)
    weekly_ma20_series = _build_ma_series(weekly_closes, 20)
    weekly_ma7 = weekly_ma7_series[-1] if weekly_ma7_series else None
    weekly_ma20 = weekly_ma20_series[-1] if weekly_ma20_series else None
    weekly_above_ma7 = (
        weekly_closes[-1] > weekly_ma7 if weekly_ma7 is not None and weekly_closes else False
    )
    weekly_above_ma20 = (
        weekly_closes[-1] > weekly_ma20 if weekly_ma20 is not None and weekly_closes else False
    )

    weekly_low_stop = False
    if len(weekly_lows) >= 6:
        recent_lows = weekly_lows[-6:]
        previous_lows = weekly_lows[:-6]
        if previous_lows:
            weekly_low_stop = min(recent_lows) >= min(previous_lows)

    weekly_range_contraction = False
    if len(weekly_highs) >= 12:
        recent_range = max(weekly_highs[-6:]) - min(weekly_lows[-6:])
        prev_range = max(weekly_highs[-12:-6]) - min(weekly_lows[-12:-6])
        if prev_range > 0 and recent_range <= prev_range * 0.8:
            weekly_range_contraction = True

    daily_cross_ma7 = False
    daily_cross_ma20 = False
    if len(closes) >= 2 and len(ma7_series) >= 2:
        daily_cross_ma7 = closes[-1] > ma7_series[-1] and closes[-2] <= ma7_series[-2]
    if len(closes) >= 2 and len(ma20_series) >= 2:
        daily_cross_ma20 = closes[-1] > ma20_series[-1] and closes[-2] <= ma20_series[-2]

    daily_pre_signal = False
    if daily_rows:
        last_row = daily_rows[-1]
        if len(last_row) >= 5:
            open_ = float(last_row[1]) if last_row[1] is not None else None
            high = float(last_row[2]) if last_row[2] is not None else None
            low = float(last_row[3]) if last_row[3] is not None else None
            close = float(last_row[4]) if last_row[4] is not None else None
            if open_ is not None and high is not None and low is not None and close is not None:
                rng = max(high - low, 1e-9)
                body = abs(close - open_)
                lower_shadow = min(open_, close) - low
                if body / rng <= 0.35 or lower_shadow / rng >= 0.45:
                    daily_pre_signal = True

    daily_low_break = False
    if len(daily_rows) >= 11:
        lows = [
            float(row[3])
            for row in daily_rows[-11:-1]
            if len(row) >= 4 and row[3] is not None
        ]
        if lows and daily_rows[-1][3] is not None:
            daily_low_break = float(daily_rows[-1][3]) < min(lows)

    weekly_low_break = False
    if len(weekly_lows) >= 7:
        weekly_low_break = weekly_lows[-1] < min(weekly_lows[-7:-1])

    falling_knife = daily_low_break or weekly_low_break
    monthly_ok = box_active or bottom_zone

    score_monthly = 0
    if box_active:
        score_monthly += 18
    if bottom_zone:
        score_monthly += 12

    score_weekly = 0
    if weekly_low_stop:
        score_weekly += 15
    if weekly_range_contraction:
        score_weekly += 10
    if weekly_above_ma7:
        score_weekly += 7
    if weekly_above_ma20:
        score_weekly += 8

    score_daily = 0
    if daily_cross_ma7:
        score_daily += 10
    if daily_cross_ma20:
        score_daily += 12
    if daily_pre_signal:
        score_daily += 8

    daily_ma20_down = False
    if len(ma20_series) >= 2:
        daily_ma20_down = ma20_series[-1] < ma20_series[-2]

    buy_state = "その他"
    buy_state_rank = 0
    buy_state_score = 0
    buy_state_reason_parts: list[str] = []

    if monthly_ok and weekly_low_stop and not falling_knife:
        if daily_cross_ma7 or daily_cross_ma20 or daily_pre_signal:
            buy_state = "初動"
            buy_state_rank = 2
            buy_state_score = score_monthly + score_weekly + score_daily
            if daily_ma20_down and ma20 is not None and last_close is not None and last_close < ma20:
                buy_state_score -= 15
        elif weekly_range_contraction:
            buy_state = "底がため"
            buy_state_rank = 1
            buy_state_score = score_monthly + score_weekly + min(score_daily, 10)

    if buy_state_score < 0:
        buy_state_score = 0
    if buy_state == "初動":
        buy_state_score = min(100, buy_state_score)
    elif buy_state == "底がため":
        buy_state_score = min(80, buy_state_score)

    if monthly_ok:
        month_parts = []
        if box_active:
            month_parts.append("箱有")
        if bottom_zone:
            month_parts.append("大底警戒")
        buy_state_reason_parts.append(f"月:{'/'.join(month_parts)}")
    if weekly_low_stop or weekly_range_contraction:
        week_parts = []
        if weekly_low_stop:
            week_parts.append("安値更新停止")
        if weekly_range_contraction:
            week_parts.append("収縮")
        if weekly_above_ma7:
            week_parts.append("7MA上")
        if weekly_above_ma20:
            week_parts.append("20MA上")
        buy_state_reason_parts.append(f"週:{'/'.join(week_parts)}")
    if daily_cross_ma7 or daily_cross_ma20 or daily_pre_signal:
        day_parts = []
        if daily_cross_ma7:
            day_parts.append("7MA上抜け")
        if daily_cross_ma20:
            day_parts.append("20MA上抜け")
        if daily_pre_signal:
            day_parts.append("事前決定打")
        buy_state_reason_parts.append(f"日:{'/'.join(day_parts)}")
    if falling_knife:
        buy_state_reason_parts.append("落ちるナイフ")

    buy_state_reason = " / ".join(buy_state_reason_parts) if buy_state_reason_parts else "N/A"

    buy_risk_distance = None
    if last_close is not None and box_monthly and box_monthly.get("bodyLow") is not None:
        body_low = float(box_monthly["bodyLow"])
        if last_close > 0:
            buy_risk_distance = max(0.0, (last_close - body_low) / last_close * 100)

    status_label = "UNKNOWN"
    essential_missing = last_close is None or ma20 is None or ma60 is None
    if not essential_missing:
        if last_close > ma20 and ma20 > ma60:
            status_label = "UP"
        elif last_close < ma20 and ma20 < ma60:
            status_label = "DOWN"
        else:
            status_label = "RANGE"

    up_score = None
    down_score = None
    overheat_up = None
    overheat_down = None

    if status_label != "UNKNOWN" and last_close is not None and ma20 is not None and ma60 is not None:
        up_score = 0
        down_score = 0

        if last_close > ma20:
            up_score += 10
        if ma20 > ma60:
            up_score += 10
        if slope20 is not None and slope20 > 0:
            up_score += 10

        if up7 is not None:
            if up7 >= 14:
                up_score += 20
            elif up7 >= 7:
                up_score += 10

        if box_state != "NONE":
            if box_direction == "BREAKOUT_UP":
                up_score += 30
            elif box_state == "IN_BOX" and box_monthly and box_monthly.get("months", 0) >= 3:
                up_score += 10

        if chg1m is not None and chg1m > 0:
            up_score += 10
        if chg1q is not None and chg1q > 0:
            up_score += 10

        if last_close < ma20:
            down_score += 10
        if ma20 < ma60:
            down_score += 10
        if slope20 is not None and slope20 < 0:
            down_score += 10

        if down7 is not None:
            if down7 >= 14:
                down_score += 20
            elif down7 >= 7:
                down_score += 10

        if box_state != "NONE" and box_direction == "BREAKOUT_DOWN":
            down_score += 30

        if chg1m is not None and chg1m < 0:
            down_score += 10
        if chg1q is not None and chg1q < 0:
            down_score += 10

        up_score = min(100, max(0, up_score))
        down_score = min(100, max(0, down_score))

        if up20 is not None:
            overheat_up = min(1.0, max(0.0, (up20 - 16) / 4))
        if down20 is not None:
            overheat_down = min(1.0, max(0.0, (down20 - 16) / 4))

    # ========================================================================
    # Short-selling score calculation
    # ========================================================================
    short_score = None
    a_score = None
    b_score = None
    short_type = None
    short_badges: list[str] = []
    short_reasons: list[str] = []
    short_prohibition = None

    if last_close is not None and ma20 is not None and ma60 is not None:
        # Calculate 60-day range bounds for Z3 check
        range_high_60, range_low_60, range_mid_60 = _calc_range_bounds_with_mid(highs, lows, 60)

        # Check prohibition zones
        short_prohibition, zone_penalty = _check_short_prohibition_zones(
            last_close, ma20, ma60, slope20_reg, slope60_reg, atr14,
            range_mid_60, range_high_60, range_low_60
        )

        # Calculate A-type score (反転確定ショート)
        a_score_raw, a_reasons, a_badges = _calc_short_a_score(
            closes, opens, lows, ma5_series, ma20_series, atr14,
            volumes, volume_avg_20, down7, highs
        )

        # Calculate B-type score (戻り売り)
        b_score_raw, b_reasons, b_badges = _calc_short_b_score(
            closes, opens, lows, ma5_series, ma20_series, ma60_series,
            slope20_reg, slope60_reg, atr14, volumes, volume_avg_20, down20, ma7_series
        )

        # Apply Z3 penalty (not forced to 0, just penalty)
        if short_prohibition == "Z3":
            a_score_raw = max(0, a_score_raw + zone_penalty)
            b_score_raw = max(0, b_score_raw + zone_penalty)

        # Determine final score and type
        if short_prohibition in ("Z1", "Z2"):
            # Forced to 0 for prohibition zones
            short_score = 0
            a_score = 0
            b_score = 0
            short_type = None
            short_badges = []
            short_reasons = [f"禁止ゾーン: {short_prohibition}"]
        else:
            a_score = a_score_raw
            b_score = b_score_raw
            short_score = max(a_score, b_score)

            if a_score >= b_score and a_score > 0:
                short_type = "A"
                short_badges = a_badges
                short_reasons = a_reasons
            elif b_score > 0:
                short_type = "B"
                short_badges = b_badges
                short_reasons = b_reasons
            else:
                short_type = None
                short_badges = []
                short_reasons = []


    return {
        "lastClose": last_close,
        "chg1D": chg1d,
        "chg1W": chg1w,
        "chg1M": chg1m,
        "chg1Q": chg1q,
        "chg1Y": chg1y,
        "prevWeekChg": prev_week_chg,
        "prevMonthChg": prev_month_chg,
        "prevQuarterChg": prev_quarter_chg,
        "prevYearChg": prev_year_chg,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "ma100": ma100,
        "slope20": slope20,
        "counts": {
            "up7": up7,
            "down7": down7,
            "up20": up20,
            "down20": down20,
            "up60": up60,
            "down60": down60,
            "up100": up100,
            "down100": down100
        },
        "boxMonthly": box_monthly,
        "boxState": box_state,
        "boxEndMonth": box_end_month,
        "breakoutMonth": breakout_month,
        "boxActive": box_active,
        "hasBox": box_active,
        "box_state": box_state,
        "box_end_month": box_end_month,
        "breakout_month": breakout_month,
        "box_active": box_active,
        "buyState": buy_state,
        "buyStateRank": buy_state_rank,
        "buyStateScore": buy_state_score,
        "buyStateReason": buy_state_reason,
        "buyRiskDistance": buy_risk_distance,
        "buy_state": buy_state,
        "buy_state_rank": buy_state_rank,
        "buy_state_score": buy_state_score,
        "buy_state_reason": buy_state_reason,
        "buy_risk_distance": buy_risk_distance,
        "buyStateDetails": {
            "monthly": score_monthly,
            "weekly": score_weekly,
            "daily": score_daily
        },
        "scores": {
            "upScore": up_score,
            "downScore": down_score,
            "overheatUp": overheat_up,
            "overheatDown": overheat_down
        },
        "statusLabel": status_label,
        "reasons": reasons,
        # Short-selling score fields
        "shortScore": short_score,
        "aScore": a_score,
        "bScore": b_score,
        "shortType": short_type,
        "shortBadges": short_badges,
        "shortReasons": short_reasons,
        "shortProhibition": short_prohibition
    }


def _format_event_date(value: object | None) -> str | None:
    """Format event date for frontend display."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, date):
        return value.isoformat()
    return None


def _build_screener_rows() -> list[dict]:
    today = jst_now().date()
    window_end = today + timedelta(days=30)
    with get_conn() as conn:
        codes = [row[0] for row in conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
        meta_rows = conn.execute(
            "SELECT code, name, stage, score, reason, score_status, missing_reasons_json, score_breakdown_json FROM stock_meta"
        ).fetchall()
        daily_rows = conn.execute(
            """
            SELECT code, date, o, h, l, c, v
            FROM (
                SELECT
                    code,
                    date,
                    o,
                    h,
                    l,
                    c,
                    v,
                    ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
                FROM daily_bars
            )
            WHERE rn <= 260
            ORDER BY code, date
            """
        ).fetchall()
        monthly_rows = conn.execute(
            """
            SELECT code, month, o, h, l, c
            FROM monthly_bars
            ORDER BY code, month
            """
        ).fetchall()
        earnings_rows = conn.execute(
            """
            SELECT code, MIN(planned_date) AS planned_date
            FROM earnings_planned
            WHERE planned_date BETWEEN ? AND ?
            GROUP BY code
            """,
            [today, window_end]
        ).fetchall()
        rights_rows = conn.execute(
            """
            SELECT code, MIN(COALESCE(last_rights_date, ex_date)) AS rights_date
            FROM ex_rights
            WHERE COALESCE(last_rights_date, ex_date) >= ?
            GROUP BY code
            """,
            [today]
        ).fetchall()

    meta_map = {row[0]: row for row in meta_rows}
    fallback_names = _build_name_map_from_txt()
    daily_map: dict[str, list[tuple]] = {}
    monthly_map: dict[str, list[tuple]] = {}
    earnings_map = {row[0]: row[1] for row in earnings_rows}
    rights_map = {row[0]: row[1] for row in rights_rows}

    for row in daily_rows:
        code = row[0]
        daily_map.setdefault(code, []).append(row[1:])

    for row in monthly_rows:
        code = row[0]
        monthly_map.setdefault(code, []).append(row[1:])

    items: list[dict] = []
    for code in codes:
        meta = meta_map.get(code)
        name = meta[1] if meta else None
        stage = meta[2] if meta else None
        score = meta[3] if meta and meta[3] is not None else None
        reason = meta[4] if meta and meta[4] is not None else ""
        score_status = meta[5] if meta else None
        missing_reasons = []
        if meta and meta[6]:
            try:
                missing_reasons = json.loads(meta[6]) or []
            except (TypeError, json.JSONDecodeError):
                missing_reasons = []
        score_breakdown = None
        if meta and meta[7]:
            try:
                score_breakdown = json.loads(meta[7]) or None
            except (TypeError, json.JSONDecodeError):
                score_breakdown = None
        metrics = _compute_screener_metrics(daily_map.get(code, []), monthly_map.get(code, []))
        fallback_name = fallback_names.get(code)
        if not name or name == code:
            name = fallback_name
        if not name:
            name = code
        if not stage or stage.upper() == "UNKNOWN":
            stage = metrics.get("statusLabel") or stage or "UNKNOWN"
        if isinstance(score, (int, float)) and float(score) == 0.0:
            if (
                not score_status
                or score_status == "INSUFFICIENT_DATA"
                or not reason
                or reason == "TODO"
                or not stage
                or (isinstance(stage, str) and stage.upper() == "UNKNOWN")
            ):
                score = None
                score_status = "INSUFFICIENT_DATA"
        if score is None:
            fallback_score = None
            buy_score = metrics.get("buyStateScore")
            if isinstance(buy_score, (int, float)) and buy_score > 0:
                fallback_score = float(buy_score)
            else:
                scores = metrics.get("scores") or {}
                if isinstance(scores, dict):
                    values = [
                        scores.get("upScore"),
                        scores.get("downScore")
                    ]
                    values = [float(v) for v in values if isinstance(v, (int, float)) and v > 0]
                    if values:
                        fallback_score = max(values)
            if fallback_score is not None:
                score = fallback_score
                if not reason:
                    reason = "DERIVED"
                if not score_status:
                    score_status = "OK"
        if not score_status:
            score_status = "OK" if score is not None else "INSUFFICIENT_DATA"
        if not missing_reasons:
            missing_reasons = metrics.get("reasons") or []
        event_earnings_date = _format_event_date(earnings_map.get(code))
        event_rights_date = _format_event_date(rights_map.get(code))
        items.append(
            {
                "code": code,
                "name": name,
                "stage": stage,
                "score": score,
                "reason": reason,
                "scoreStatus": score_status,
                "score_status": score_status,
                "missingReasons": missing_reasons,
                "missing_reasons": missing_reasons,
                "scoreBreakdown": score_breakdown,
                "score_breakdown": score_breakdown,
                "eventEarningsDate": event_earnings_date,
                "eventRightsDate": event_rights_date,
                "event_earnings_date": event_earnings_date,
                "event_rights_date": event_rights_date,
                **metrics
            }
        )
    return items



# --- Events API ---

def _refresh_events_job():
    print("[events] Starting background refresh...")
    meta_id = 1
    started_at = jst_now()
    
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO events_meta (id, is_refreshing) VALUES (?, ?)
        """, (meta_id, False))
        conn.execute("""
            UPDATE events_meta
            SET is_refreshing = 1,
                last_attempt_at = ?
            WHERE id = ?
        """, (started_at, meta_id))
    
    try:
        # 1. Fetch Earnings
        earnings_rows = fetch_earnings_snapshot()
        print(f"[events] Fetched {len(earnings_rows)} earnings records")
        
        with get_conn() as conn:
            conn.execute("DELETE FROM earnings_planned")
            conn.executemany("""
                INSERT INTO earnings_planned (code, planned_date, kind, company_name, source, fetched_at)
                VALUES (:code, :planned_date, :kind, :company_name, :source, :fetched_at)
            """, earnings_rows)
            conn.execute("""
                UPDATE events_meta
                SET earnings_last_success_at = ?
                WHERE id = ?
            """, (jst_now(), meta_id))

        # 2. Fetch Rights
        rights_rows = fetch_rights_snapshot()
        print(f"[events] Fetched {len(rights_rows)} rights records")
        
        with get_conn() as conn:
            conn.execute("DELETE FROM ex_rights")
            conn.executemany("""
                INSERT INTO ex_rights (code, ex_date, record_date, category, last_rights_date, source, fetched_at)
                VALUES (:code, :ex_date, :record_date, :category, :last_rights_date, :source, :fetched_at)
            """, rights_rows)
            # Update meta success status
            conn.execute("""
                UPDATE events_meta
                SET rights_last_success_at = ?,
                    is_refreshing = 0,
                    last_error = NULL
                WHERE id = ?
            """, (jst_now(), meta_id))
        _invalidate_screener_cache()
            
        print("[events] Refresh completed successfully")
        
    except Exception as e:
        print(f"[events] Refresh failed: {e}")
        traceback.print_exc()
        with get_conn() as conn:
            conn.execute("""
                UPDATE events_meta
                SET is_refreshing = 0,
                    last_error = ?
                WHERE id = ?
            """, (str(e), meta_id))
        _invalidate_screener_cache()


@app.get("/api/events/meta")
def get_events_meta():
    return events_meta()


@app.post("/api/events/refresh")
def refresh_events(reason: str | None = None):
    return events_refresh(reason)


def _get_screener_rows() -> list[dict]:
    mtime = None
    if os.path.isfile(DEFAULT_DB_PATH):
        mtime = os.path.getmtime(DEFAULT_DB_PATH)
    if _screener_cache["mtime"] == mtime and _screener_cache["rows"]:
        return _screener_cache["rows"]

    rows = _build_screener_rows()
    _screener_cache["mtime"] = mtime
    _screener_cache["rows"] = rows
    return rows


def get_txt_status() -> dict:
    if not os.path.isdir(PAN_OUT_TXT_DIR):
        return {
            "txt_count": 0,
            "code_txt_missing": False,
            "last_updated": None
        }

    txt_files = [
        os.path.join(PAN_OUT_TXT_DIR, name)
        for name in os.listdir(PAN_OUT_TXT_DIR)
        if name.endswith(".txt") and name.lower() != "code.txt"
    ]
    code_txt_missing = False
    if USE_CODE_TXT:
        code_txt_missing = find_code_txt_path(PAN_OUT_TXT_DIR) is None
    last_updated = None
    if txt_files:
        last_updated = max(os.path.getmtime(path) for path in txt_files)
        last_updated = datetime.utcfromtimestamp(last_updated).isoformat() + "Z"

    return {
        "txt_count": len(txt_files),
        "code_txt_missing": code_txt_missing,
        "last_updated": last_updated
    }


def _run_command(cmd: list[str], timeout: int) -> tuple[int, str]:
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        creationflags=creationflags,
        env=os.environ  # Pass environment variables to subprocess
    )
    output = "\n".join([result.stdout or "", result.stderr or ""]).strip()
    if len(output) > 8000:
        output = output[-8000:]
    return result.returncode, output


def _read_text_lines(path: str) -> list[str]:
    for encoding in ("utf-8", "cp932"):
        try:
            with open(path, "r", encoding=encoding, errors="ignore") as handle:
                return handle.read().splitlines()
        except OSError:
            break
    return []


def _count_codes(path: str) -> int:
    count = 0
    for line in _read_text_lines(path):
        text = line.strip()
        if not text:
            continue
        if text.startswith("#") or text.startswith("'"):
            continue
        count += 1
    return count


def _append_stdout_tail(line: str) -> None:
    with _update_txt_lock:
        tail = list(_update_txt_status.get("stdout_tail") or [])
        tail.append(line)
        if len(tail) > 20:
            tail = tail[-20:]
        _update_txt_status["stdout_tail"] = tail


def _set_update_status(**kwargs) -> None:
    with _update_txt_lock:
        _update_txt_status.update(kwargs)


def _get_update_status_snapshot() -> dict:
    with _update_txt_lock:
        return dict(_update_txt_status)


def _run_streaming_command(cmd: list[str], timeout: int, on_line) -> tuple[int, str, bool]:
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="cp932" if os.name == "nt" else "utf-8",
        errors="replace",
        creationflags=creationflags
    )
    output_lines: list[str] = []
    start = time.time()
    timed_out = False
    while True:
        if process.stdout is None:
            break
        line = process.stdout.readline()
        if line:
            text = line.rstrip()
            output_lines.append(text)
            on_line(text)
        if process.poll() is not None:
            break
        if time.time() - start > timeout:
            process.kill()
            timed_out = True
            break
    if process.stdout is not None:
        remaining = process.stdout.read()
        if remaining:
            for extra in remaining.splitlines():
                output_lines.append(extra)
                on_line(extra)
    return process.wait(), "\n".join(output_lines).strip(), timed_out


def _load_update_state() -> dict:
    if not os.path.isfile(UPDATE_STATE_PATH):
        return {}
    try:
        with open(UPDATE_STATE_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}


def _save_update_state(state: dict) -> None:
    try:
        with open(UPDATE_STATE_PATH, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _parse_vbs_summary(output: str) -> dict:
    summary: dict[str, int] = {}
    for line in output.splitlines():
        if line.startswith("SUMMARY:"):
            for key, value in re.findall(r"(\\w+)=(\\d+)", line):
                summary[key] = int(value)
    return summary


def _run_txt_update_job(code_path: str, out_dir: str) -> None:
    processed = 0

    def on_line(line: str) -> None:
        nonlocal processed
        _append_stdout_tail(line)
        if line.startswith(("OK   :", "ERROR:", "SPLIT :")):
            processed += 1
            _set_update_status(processed=processed)

    try:
        sys_root = os.environ.get("SystemRoot") or "C:\\Windows"
        cscript = os.path.join(sys_root, "SysWOW64", "cscript.exe")
        if not os.path.isfile(cscript):
            cscript = os.path.join(sys_root, "System32", "cscript.exe")
        vbs_cmd = [cscript, "//nologo", UPDATE_VBS_PATH, code_path, out_dir]
        timeout_sec = 1800
        vbs_code, vbs_output, timed_out = _run_streaming_command(
            vbs_cmd, timeout=timeout_sec, on_line=on_line
        )
        summary = _parse_vbs_summary(vbs_output)
        _set_update_status(summary=summary)
        if timed_out:
            _set_update_status(
                running=False,
                phase="error",
                error="timeout",
                finished_at=datetime.now().isoformat(),
                timeout_sec=timeout_sec
            )
            return
        if vbs_code != 0:
            _set_update_status(
                running=False,
                phase="error",
                error=f"vbs_failed:{vbs_code}",
                finished_at=datetime.now().isoformat()
            )
            return

        _set_update_status(phase="ingesting")
        ingest_code, ingest_output = _run_ingest_command()
        for line in ingest_output.splitlines():
            _append_stdout_tail(line)
        if ingest_code != 0:
            _set_update_status(
                running=False,
                phase="error",
                error=f"ingest_failed:{ingest_code}",
                finished_at=datetime.now().isoformat(),
                summary=summary
            )
            return

        state = _load_update_state()
        state["last_txt_update_date"] = datetime.now().date().isoformat()
        state["last_txt_update_at"] = datetime.now().isoformat()
        _save_update_state(state)
        _set_update_status(
            running=False,
            phase="done",
            error=None,
            finished_at=datetime.now().isoformat(),
            summary=summary,
            last_updated_at=state.get("last_txt_update_at"),
            processed=processed
        )
    except Exception as exc:
        _append_stdout_tail(str(exc))
        _set_update_status(
            running=False,
            phase="error",
            error=f"update_txt_failed:{exc}",
            finished_at=datetime.now().isoformat()
        )


def _start_txt_update(code_path: str, out_dir: str, total: int, cscript: str) -> dict:
    started_at = datetime.now().isoformat()
    with _update_txt_lock:
        if _update_txt_status.get("running"):
            return {}
        _update_txt_status.update(
            {
                "running": True,
                "phase": "running",
                "started_at": started_at,
                "finished_at": None,
                "processed": 0,
                "total": total,
                "summary": {},
                "error": None,
                "stdout_tail": [],
                "code_path": code_path,
                "out_dir": out_dir,
                "script_path": UPDATE_VBS_PATH,
                "cscript_path": cscript
            }
        )
    thread = threading.Thread(target=_run_txt_update_job, args=(code_path, out_dir), daemon=True)
    thread.start()
    return {"ok": True, "started": True, "started_at": started_at, "total": total}


def _run_ingest_command() -> tuple[int, str]:
    # Always use in-process import to avoid subprocess issues
    # (subprocess may try to initialize pywebview and show "already running" dialog)
    import importlib
    import io
    from contextlib import redirect_stdout, redirect_stderr

    output = io.StringIO()
    try:
        with redirect_stdout(output), redirect_stderr(output):
            module = None
            for name in ("ingest_txt", "app.backend.ingest_txt"):
                try:
                    module = importlib.import_module(name)
                    break
                except Exception:
                    continue
            if module is None:
                raise ModuleNotFoundError("ingest_txt")
            module.ingest()
        return 0, output.getvalue()
    except Exception as exc:
        output.write(f"ingest_module_failed:{exc}\n")
        output.write(traceback.format_exc())
        return 1, output.getvalue()


@app.post("/api/txt_update/run")
def txt_update_run():
    state = _load_update_state()
    today = datetime.now().date().isoformat()
    # Daily limit check removed at user request
    # if state.get("last_txt_update_date") == today:
    #     return JSONResponse(
    #         status_code=200,
    #         content={
    #             "ok": False,
    #             "error": "already_updated_today",
    #             "last_updated_at": state.get("last_txt_update_at")
    #         }
    #     )

    if not os.path.isfile(UPDATE_VBS_PATH):
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": f"vbs_not_found:{UPDATE_VBS_PATH}"}
        )

    code_path = PAN_CODE_TXT_PATH if os.path.isfile(PAN_CODE_TXT_PATH) else None
    if not code_path:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": "code_txt_missing",
                "searched": [PAN_CODE_TXT_PATH]
            }
        )

    os.makedirs(PAN_OUT_TXT_DIR, exist_ok=True)
    total = _count_codes(code_path)
    sys_root = os.environ.get("SystemRoot") or "C:\\Windows"
    cscript = os.path.join(sys_root, "SysWOW64", "cscript.exe")
    if not os.path.isfile(cscript):
        cscript = os.path.join(sys_root, "System32", "cscript.exe")
    started = _start_txt_update(code_path, PAN_OUT_TXT_DIR, total, cscript)
    if not started:
        return JSONResponse(status_code=409, content={"ok": False, "error": "update_in_progress"})
    return started


@app.get("/api/txt_update/status")
def txt_update_status():
    snapshot = _get_update_status_snapshot()
    if not snapshot.get("last_updated_at"):
        state = _load_update_state()
        snapshot["last_updated_at"] = state.get("last_txt_update_at")
    summary = snapshot.get("summary") or {}
    if summary.get("ok", 0) > 0 and summary.get("err", 0) > 0:
        snapshot["warning"] = True
    else:
        snapshot["warning"] = False
    elapsed_ms = None
    if snapshot.get("started_at"):
        try:
            started = datetime.fromisoformat(snapshot["started_at"])
            elapsed_ms = int((datetime.now() - started).total_seconds() * 1000)
        except ValueError:
            elapsed_ms = None
    snapshot["elapsed_ms"] = elapsed_ms
    return snapshot


@app.get("/api/txt_update/split_suspects")
def txt_update_split_suspects():
    if not os.path.isfile(SPLIT_SUSPECTS_PATH):
        return {"items": []}
    items = []
    try:
        for line in _read_text_lines(SPLIT_SUSPECTS_PATH):
            if not line or line.lower().startswith("code,"):
                continue
            parts = [part.strip() for part in line.split(",")]
            if len(parts) < 7:
                continue
            items.append(
                {
                    "code": parts[0],
                    "file_date": parts[1],
                    "file_close": parts[2],
                    "pan_date": parts[3],
                    "pan_close": parts[4],
                    "diff_ratio": parts[5],
                    "reason": parts[6],
                    "detected_at": parts[7] if len(parts) > 7 else ""
                }
            )
        return {"items": items}
    except Exception as exc:
        return JSONResponse(status_code=200, content={"items": [], "error": str(exc)})


@app.post("/api/update_txt")
def update_txt():
    return txt_update_run()


@app.get("/api/watchlist")
def get_watchlist():
    path = PAN_CODE_TXT_PATH
    if not os.path.isfile(path):
        return {"codes": [], "path": path, "missing": True}
    with _watchlist_lock:
        codes = _load_watchlist_codes(path)
    return {"codes": codes, "path": path, "missing": False}


@app.post("/api/watchlist/add")
def watchlist_add(payload: dict = Body(default=None)):
    payload = payload or {}
    code = _normalize_watch_code(payload.get("code"))
    if not code:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    path = PAN_CODE_TXT_PATH
    with _watchlist_lock:
        codes = _load_watchlist_codes(path) if os.path.isfile(path) else []
        already = code in codes
        _update_watchlist_file(path, code, remove=False)
    return {"ok": True, "code": code, "alreadyExisted": already}


@app.post("/api/watchlist/remove")
def watchlist_remove(payload: dict = Body(default=None)):
    payload = payload or {}
    code = _normalize_watch_code(payload.get("code"))
    if not code:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    delete_artifacts = payload.get("deleteArtifacts", True)
    delete_db = payload.get("deleteDb", False)
    delete_related = payload.get("deleteRelated", False)
    path = PAN_CODE_TXT_PATH
    if not os.path.isfile(path):
        return JSONResponse(status_code=400, content={"ok": False, "error": "code_txt_missing"})
    with _watchlist_lock:
        removed = _update_watchlist_file(path, code, remove=True)
        trash_token = None
        trashed: list[str] = []
        if delete_artifacts:
            trash_token, trashed = _trash_watchlist_artifacts(code)
    db_counts: dict[str, int] = {}
    favorites_deleted = 0
    practice_deleted = 0
    if delete_db:
        try:
            db_counts = _delete_ticker_db_rows(code)
            _invalidate_screener_cache()
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"db_delete_failed:{exc}", "code": code}
            )
    if delete_related:
        try:
            favorites_deleted = _delete_favorites_code(code)
            practice_deleted = _delete_practice_sessions(code)
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"related_delete_failed:{exc}", "code": code}
            )
    return {
        "ok": True,
        "code": code,
        "removed": removed,
        "deleteArtifacts": bool(delete_artifacts),
        "deleteDb": bool(delete_db),
        "deleteRelated": bool(delete_related),
        "dbDeletedCounts": db_counts,
        "dbDeletedTotal": sum(db_counts.values()),
        "favoritesDeleted": favorites_deleted,
        "practiceDeleted": practice_deleted,
        "trashed": trashed,
        "trashToken": trash_token
    }


@app.post("/api/watchlist/undo_remove")
def watchlist_undo_remove(payload: dict = Body(default=None)):
    payload = payload or {}
    code = _normalize_watch_code(payload.get("code"))
    token = payload.get("trashToken") or ""
    if not code:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    with _watchlist_lock:
        restored = _restore_watchlist_artifacts(token)
        _update_watchlist_file(PAN_CODE_TXT_PATH, code, remove=False)
    return {"ok": True, "code": code, "restored": restored}


@app.post("/api/watchlist/open")
def watchlist_open():
    path = PAN_CODE_TXT_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.isfile(path):
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")
    try:
        if os.name == "nt":
            os.startfile(path)
        else:
            opener = "open" if sys.platform == "darwin" else "xdg-open"
            subprocess.Popen([opener, path])
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"open_failed:{exc}", "path": path}
        )
    return {"ok": True, "path": path}


def _list_tables(conn) -> set[str]:
    rows = conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()
    return {row[0] for row in rows}


def _collect_db_stats() -> dict:
    stats = {
        "tickers": None,
        "daily_rows": None,
        "monthly_rows": None,
        "missing_tables": [],
        "errors": []
    }
    required_tables = ["tickers", "daily_bars", "monthly_bars", "daily_ma", "monthly_ma"]
    try:
        with get_conn() as conn:
            tables = _list_tables(conn)
            stats["missing_tables"] = [name for name in required_tables if name not in tables]
            if "tickers" in tables:
                stats["tickers"] = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
            if "daily_bars" in tables:
                stats["daily_rows"] = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]
            if "monthly_bars" in tables:
                stats["monthly_rows"] = conn.execute("SELECT COUNT(*) FROM monthly_bars").fetchone()[0]
    except Exception as exc:
        stats["errors"].append(str(exc))
    return stats


@app.get("/api/health")
def health():
    now = datetime.utcnow().isoformat()
    status = get_txt_status()
    stats = _collect_db_stats()
    is_data_ready = (
        not stats["missing_tables"]
        and stats["errors"] == []
        and (stats["daily_rows"] or 0) > 0
        and (stats["monthly_rows"] or 0) > 0
    )
    if not is_data_ready:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "status": "starting",
                "ready": False,
                "phase": "starting",
                "message": "起動中",
                "error_code": "DATA_NOT_INITIALIZED",
                "version": APP_VERSION,
                "env": APP_ENV,
                "time": now,
                "retryAfterMs": 1000,
                "stats": stats,
                "txt_count": status.get("txt_count"),
                "last_updated": status.get("last_updated"),
                "code_txt_missing": status.get("code_txt_missing"),
                "errors": stats["errors"] + [f"missing_tables:{','.join(stats['missing_tables'])}"]
                if stats["missing_tables"]
                else stats["errors"]
            }
        )
    return {
        "ok": True,
        "status": "ok",
        "ready": True,
        "phase": "ready",
        "message": "準備完了",
        "version": APP_VERSION,
        "env": APP_ENV,
        "time": now,
        "stats": {
            "tickers": stats["tickers"],
            "daily_rows": stats["daily_rows"],
            "monthly_rows": stats["monthly_rows"]
        },
        "txt_count": status.get("txt_count"),
        "code_count": stats["tickers"],
        "last_updated": status.get("last_updated"),
        "code_txt_missing": status.get("code_txt_missing"),
        "errors": []
    }


@app.get("/api/diagnostics")
def diagnostics():
    now = datetime.utcnow().isoformat()
    db_path = os.path.abspath(DEFAULT_DB_PATH)
    stats = _collect_db_stats()
    return {
        "ok": True,
        "version": APP_VERSION,
        "env": APP_ENV,
        "time": now,
        "data_dir": DATA_DIR,
        "pan_out_txt_dir": PAN_OUT_TXT_DIR,
        "db_path": db_path,
        "db_exists": os.path.isfile(db_path),
        "stats": stats
    }


@app.get("/api/trades/{code}")
def trades_by_code(code: str):
    try:
        daily_positions = _get_daily_positions_db([code])
        daily_for_code = daily_positions.get(code, [])
        
        # Events from DB
        with get_conn() as conn:
            # 0:id, 1:broker, 2:exec_dt, 3:symbol, 4:action, 5:qty, 6:price, 7:hash, 8:created, 9:txn, 10:side
            db_events = get_events(conn, [code])
            
            # Fetch current position from positions_live
            row = conn.execute(
                "SELECT spot_qty, margin_long_qty, margin_short_qty, buy_qty, sell_qty, has_issue, issue_note FROM positions_live WHERE symbol = ?", 
                [code]
            ).fetchone()
            
            current_position = None
            if row:
                current_position = {
                    "spotLots": row[0],
                    "marginLongLots": row[1],
                    "marginShortLots": row[2],
                    "longLots": row[3], # aggregated supported by engine
                    "shortLots": row[4],
                    "hasIssue": row[5],
                    "issueNote": row[6]
                }
        
        events_payload = []
        for ev in db_events:
             events_payload.append(_map_trade_event(ev))
        current_positions = _calc_current_positions_by_broker(events_payload)
        current_metrics = _calc_position_metrics(events_payload)

        return JSONResponse(
            content={
                "events": events_payload,
                "dailyPositions": daily_for_code,
                "currentPosition": {
                    "longLots": current_metrics["longLots"],
                    "shortLots": current_metrics["shortLots"]
                },
                "currentPositions": current_positions,
                "warnings": {"items": []},
                "errors": []
            }
        )
    except Exception as exc:
        return JSONResponse(
            content={
                "events": [],
                "dailyPositions": [],
                "currentPosition": None,
                "warnings": {"items": []},
                "errors": [f"trades_by_code_failed:{exc}"]
            }
        )


@app.get("/api/trades")
def trades(code: str | None = None):
    try:
        # Fetch for all codes if code is None
        code_list = [code] if code else None
        daily_positions_map = _get_daily_positions_db(code_list)
        
        # Events from DB
        with get_conn() as conn:
            db_events = get_events(conn, code_list)
            
        events_payload = []
        for ev in db_events:
             events_payload.append(_map_trade_event(ev))

        # Flatten daily positions
        all_daily = []
        for d_list in daily_positions_map.values():
            all_daily.extend(d_list)

        return JSONResponse(
             content={
                "events": events_payload,
                "dailyPositions": all_daily,
                "currentPosition": None, # Not efficient to fetch all live positions here unless needed
                "warnings": {"items": []},
                "errors": []
            }
        )
    except Exception as exc:
        return JSONResponse(
            content={
                "events": [],
                "dailyPositions": [],
                "currentPosition": None,
                "warnings": {"items": []},
                "errors": [f"trades_failed:{exc}"]
            }
        )


@app.get("/api/list")
def list_tickers():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT d.code,
                   COALESCE(m.name, d.code) AS name,
                   COALESCE(m.stage, 'UNKNOWN') AS stage,
                   m.score AS score,
                   COALESCE(m.reason, 'TXT_ONLY') AS reason,
                   p.spot_qty,
                   p.margin_long_qty,
                   p.margin_short_qty,
                   p.has_issue,
                   p.issue_note
            FROM (SELECT DISTINCT code FROM daily_bars) d
            LEFT JOIN stock_meta m ON d.code = m.code
            LEFT JOIN positions_live p ON d.code = p.symbol
            ORDER BY d.code
            """
        ).fetchall()
    return JSONResponse(content=rows)


@app.get("/rank/weekly")
def rank_weekly(as_of: str | None = None, limit: int = 50, universe: str | None = None):
    try:
        limit_value = max(1, min(200, int(limit)))
    except (TypeError, ValueError):
        limit_value = 50
    try:
        result = _build_weekly_ranking(as_of, limit_value, universe)
        return JSONResponse(content=result)
    except Exception as exc:
        return JSONResponse(
            content={
                "up": [],
                "down": [],
                "meta": {
                    "as_of": as_of,
                    "count": 0,
                    "errors": [f"rank_weekly_failed:{exc}"]
                }
            }
        )


@app.get("/rank/monthly")
def rank_monthly(as_of: str | None = None, limit: int = 50, universe: str | None = None):
    try:
        limit_value = max(1, min(200, int(limit)))
    except (TypeError, ValueError):
        limit_value = 50
    try:
        result = _build_monthly_ranking(as_of, limit_value, universe)
        return JSONResponse(content=result)
    except Exception as exc:
        return JSONResponse(
            content={
                "box": [],
                "meta": {
                    "as_of": as_of,
                    "count": 0,
                    "errors": [f"rank_monthly_failed:{exc}"]
                }
            }
        )


@app.get("/rank")
@app.get("/api/rank")
def rank_dir(dir: str = "up", as_of: str | None = None, limit: int = 50, universe: str | None = None):
    try:
        limit_value = max(1, min(200, int(limit)))
    except (TypeError, ValueError):
        limit_value = 50
    direction = (dir or "up").lower()
    if direction not in ("up", "down"):
        direction = "up"
    try:
        result = _build_weekly_ranking(as_of, limit_value, universe)
        favorites = set(_load_favorite_codes())
        items = []
        for item in result.get(direction, []):
            code = item.get("code")
            items.append(
                {
                    **item,
                    "is_favorite": bool(code and code in favorites)
                }
            )
        return JSONResponse(
            content={
                "items": items,
                "meta": {
                    "as_of": result.get("meta", {}).get("as_of"),
                    "count": len(items),
                    "dir": direction,
                    "universe": result.get("meta", {}).get("universe")
                },
                "errors": []
            }
        )
    except Exception as exc:
        return JSONResponse(
            content={
                "items": [],
                "meta": {"as_of": as_of, "count": 0, "dir": direction, "universe": universe},
                "errors": [f"rank_failed:{exc}"]
            }
        )


@app.get("/favorites")
@app.get("/api/favorites")
def favorites_list():
    try:
        items = _load_favorite_items()
        return JSONResponse(content={"items": items, "errors": []})
    except Exception as exc:
        return JSONResponse(content={"items": [], "errors": [f"favorites_failed:{exc}"]})


@app.post("/favorites/{code}")
@app.post("/api/favorites/{code}")
def favorites_add(code: str):
    normalized = _normalize_code(code)
    if not normalized:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    try:
        with _get_favorites_conn() as conn:
            conn.execute("INSERT OR IGNORE INTO favorites (code) VALUES (?)", (normalized,))
        return JSONResponse(content={"ok": True, "code": normalized})
    except Exception as exc:
        return JSONResponse(status_code=200, content={"ok": False, "error": f"favorite_add_failed:{exc}"})


@app.delete("/favorites/{code}")
@app.delete("/api/favorites/{code}")
def favorites_remove(code: str):
    normalized = _normalize_code(code)
    if not normalized:
        return JSONResponse(status_code=400, content={"ok": False, "error": "invalid_code"})
    try:
        with _get_favorites_conn() as conn:
            conn.execute("DELETE FROM favorites WHERE code = ?", (normalized,))
        return JSONResponse(content={"ok": True, "code": normalized})
    except Exception as exc:
        return JSONResponse(status_code=200, content={"ok": False, "error": f"favorite_remove_failed:{exc}"})


@app.get("/api/screener")
def screener():
    try:
        rows = _get_screener_rows()
        return JSONResponse(content={"items": rows, "errors": []})
    except Exception as exc:
        return JSONResponse(content={"items": [], "errors": [f"screener_failed:{exc}"]})


@app.post("/api/batch_bars")
def batch_bars(payload: dict = Body(default={})):  # { timeframe, codes, limit }
    timeframe = payload.get("timeframe", "monthly")
    codes = payload.get("codes", [])
    limit = min(int(payload.get("limit", 60)), 2000)

    if not codes:
        return JSONResponse(content={"timeframe": timeframe, "limit": limit, "items": {}})

    if timeframe == "daily":
        bars_table = "daily_bars"
        ma_table = "daily_ma"
        time_col = "date"
    else:
        bars_table = "monthly_bars"
        ma_table = "monthly_ma"
        time_col = "month"

    placeholders = ",".join(["?"] * len(codes))
    query = f"""
        WITH base AS (
            SELECT b.code,
                   b.{time_col} AS t,
                   b.o,
                   b.h,
                   b.l,
                   b.c,
                   b.v,
                   m.ma7,
                   m.ma20,
                   m.ma60,
                   ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY b.{time_col} DESC) AS rn
            FROM {bars_table} b
            LEFT JOIN {ma_table} m
              ON b.code = m.code AND b.{time_col} = m.{time_col}
            WHERE b.code IN ({placeholders})
        )
        SELECT code, t, o, h, l, c, v, ma7, ma20, ma60
        FROM base
        WHERE rn <= ?
        ORDER BY code, t
    """

    with get_conn() as conn:
        rows = conn.execute(query, codes + [limit]).fetchall()
        monthly_rows = conn.execute(
            f"""
            SELECT code, month, o, h, l, c, v
            FROM monthly_bars
            WHERE code IN ({placeholders})
            ORDER BY code, month
            """,
            codes
        ).fetchall()

    monthly_by_code: dict[str, list[tuple]] = {}
    for code, month, o, h, l, c, v in monthly_rows:
        monthly_by_code.setdefault(code, []).append((month, o, h, l, c, v))

    boxes_by_code = {code: detect_boxes(monthly_by_code.get(code, [])) for code in codes}

    items: dict[str, dict[str, list]] = {
        code: {"bars": [], "ma": {"ma7": [], "ma20": [], "ma60": []}, "boxes": boxes_by_code.get(code, [])}
        for code in codes
    }
    for code, t, o, h, l, c, v, ma7, ma20, ma60 in rows:
        payload = items.setdefault(code, {"bars": [], "ma": {"ma7": [], "ma20": [], "ma60": []}, "boxes": boxes_by_code.get(code, [])})
        payload["bars"].append([t, o, h, l, c, v])
        payload["ma"]["ma7"].append([t, ma7])
        payload["ma"]["ma20"].append([t, ma20])
        payload["ma"]["ma60"].append([t, ma60])

    return JSONResponse(content={"timeframe": timeframe, "limit": limit, "items": items})


@app.get("/api/ticker/daily")
def daily(code: str, limit: int = 400):
    query_with_ma = """
        WITH base AS (
            SELECT
                b.date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                m.ma7,
                m.ma20,
                m.ma60
            FROM daily_bars b
            LEFT JOIN daily_ma m
              ON b.code = m.code AND b.date = m.date
            WHERE b.code = ?
            ORDER BY b.date
        ),
        tail AS (
            SELECT *
            FROM base
            ORDER BY date DESC
            LIMIT ?
        )
        SELECT date, o, h, l, c, v, ma7, ma20, ma60
        FROM tail
        ORDER BY date
    """
    query_basic = """
        WITH base AS (
            SELECT
                b.date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v
            FROM daily_bars b
            WHERE b.code = ?
            ORDER BY b.date
        ),
        tail AS (
            SELECT *
            FROM base
            ORDER BY date DESC
            LIMIT ?
        )
        SELECT date, o, h, l, c, v
        FROM tail
        ORDER BY date
    """
    errors: list[str] = []
    try:
        with get_conn() as conn:
            rows = conn.execute(query_with_ma, [code, limit]).fetchall()
        return JSONResponse(content={"data": rows, "errors": []})
    except Exception as exc:
        errors.append(f"daily_query_failed:{exc}")
        try:
            with get_conn() as conn:
                rows = conn.execute(query_basic, [code, limit]).fetchall()
            return JSONResponse(content={"data": rows, "errors": []})
        except Exception as fallback_exc:
            errors.append(f"daily_query_fallback_failed:{fallback_exc}")
            return JSONResponse(content={"data": [], "errors": errors})


@app.get("/api/practice/session")
def practice_session(session_id: str | None = None):
    if not session_id:
        return JSONResponse(content={"error": "session_id_required"}, status_code=400)
    with _get_practice_conn() as conn:
        row = conn.execute(
            """
            SELECT
                session_id,
                code,
                start_date,
                end_date,
                cursor_time,
                max_unlocked_time,
                lot_size,
                range_months,
                trades,
                notes,
                ui_state
            FROM practice_sessions
            WHERE session_id = ?
            """,
            [session_id]
        ).fetchone()
    if not row:
        return JSONResponse(content={"session": None})
    trades_raw = row["trades"] or "[]"
    try:
        trades = json.loads(trades_raw)
        if not isinstance(trades, list):
            trades = []
    except (TypeError, ValueError):
        trades = []
    ui_state_raw = row["ui_state"] or "{}"
    try:
        ui_state = json.loads(ui_state_raw)
        if not isinstance(ui_state, dict):
            ui_state = {}
    except (TypeError, ValueError):
        ui_state = {}
    return JSONResponse(
        content={
            "session": {
                "session_id": row["session_id"],
                "code": row["code"],
                "start_date": row["start_date"],
                "end_date": row["end_date"],
                "cursor_time": row["cursor_time"],
                "max_unlocked_time": row["max_unlocked_time"],
                "lot_size": row["lot_size"],
                "range_months": row["range_months"],
                "trades": trades,
                "notes": row["notes"] or "",
                "ui_state": ui_state
            }
        }
    )


@app.post("/api/practice/session")
def practice_session_upsert(payload: dict = Body(...)):
    session_id = payload.get("session_id")
    code = payload.get("code")
    if not session_id or not code:
        return JSONResponse(content={"error": "session_id_code_required"}, status_code=400)
    start_date = _format_practice_date(payload.get("start_date"))
    end_date = _format_practice_date(payload.get("end_date"))
    cursor_time = payload.get("cursor_time")
    max_unlocked_time = payload.get("max_unlocked_time")
    lot_size = payload.get("lot_size")
    range_months = payload.get("range_months")
    trades = payload.get("trades")
    if not isinstance(trades, list):
        trades = []
    notes = payload.get("notes")
    if notes is not None:
        notes = str(notes)
    ui_state = payload.get("ui_state")
    if ui_state is None:
        ui_state = {}
    if not isinstance(ui_state, dict):
        ui_state = {}
    trades_json = json.dumps(trades, ensure_ascii=True)
    ui_state_json = json.dumps(ui_state, ensure_ascii=True)
    with _get_practice_conn() as conn:
        conn.execute(
            """
            INSERT INTO practice_sessions (
                session_id,
                code,
                start_date,
                end_date,
                cursor_time,
                max_unlocked_time,
                lot_size,
                range_months,
                trades,
                notes,
                ui_state,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(session_id) DO UPDATE SET
                code = excluded.code,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                cursor_time = excluded.cursor_time,
                max_unlocked_time = excluded.max_unlocked_time,
                lot_size = excluded.lot_size,
                range_months = excluded.range_months,
                trades = excluded.trades,
                notes = excluded.notes,
                ui_state = excluded.ui_state,
                updated_at = CURRENT_TIMESTAMP
            """,
            [
                session_id,
                code,
                start_date,
                end_date,
                cursor_time,
                max_unlocked_time,
                lot_size,
                range_months,
                trades_json,
                notes,
                ui_state_json
            ]
        )
    return JSONResponse(
        content={
            "session_id": session_id,
            "code": code,
            "start_date": start_date
        }
    )


@app.post("/api/imports/trade-history")
async def import_trade_history(
    file: UploadFile = File(...),
    broker: str = Form("rakuten")
):
    try:
        raw_data = await file.read()
        
        # Auto-detect broker logic
        try:
            head_sample = raw_data[:8192].decode("cp932", errors="ignore")
            if "受渡金額/決済損益" in head_sample or "信用新規買" in head_sample:
                broker = "sbi"
            elif "口座" in head_sample and "手数料" in head_sample:
                broker = "rakuten"
        except:
            pass
            
        if broker == "rakuten":
            result = process_import_rakuten(raw_data, replace_existing=True)
        elif broker == "sbi":
            result = process_import_sbi(raw_data, replace_existing=True)
        else:
            return JSONResponse(status_code=400, content={"error": f"Unknown broker: {broker}"})
        if result.get("received", 0) == 0:
            return JSONResponse(status_code=400, content={"error": "No events parsed", "warnings": result.get("warnings")})
        return JSONResponse(content={"result": "success", **result})
            
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})


        return JSONResponse(status_code=500, content={"error": str(e)})


_trade_file_mtime_cache: dict[str, float] = {}
_trade_sync_status = {
    "last_run": None,
    "paths": [],
    "processed": [],
    "errors": []
}

def _auto_sync_csv_files(conn, force: bool = False):
    global _trade_sync_status
    try:
        paths = resolve_trade_csv_paths()
        _trade_sync_status["last_run"] = datetime.now().isoformat()
        _trade_sync_status["paths"] = paths
        
        if force:
            conn.execute("DELETE FROM trade_events")
        
        updated = False
        files_to_process = []
        
        for path in paths:
            if not os.path.isfile(path):
                continue
            mtime = os.path.getmtime(path)
            if force or _trade_file_mtime_cache.get(path) != mtime:
                files_to_process.append((path, mtime))
                
        if not files_to_process:
            return

        for path, mtime in files_to_process:
            try:
                filename = os.path.basename(path)
                broker = "rakuten"
                if "SBI" in filename or "sbi" in filename.lower():
                    broker = "sbi"
                
                with open(path, "rb") as f:
                    raw_data = f.read()
                
                # Auto-detect broker logic for auto-sync as well
                try:
                    head_sample = raw_data[:8192].decode("cp932", errors="ignore")
                    if "受渡金額/決済損益" in head_sample or "信用新規買" in head_sample:
                        broker = "sbi"
                    elif "口座" in head_sample and "手数料" in head_sample:
                        broker = "rakuten"
                except:
                    pass

                events = []
                warns = []
                if broker == "rakuten":
                    events, warns = parse_rakuten_csv(raw_data)
                elif broker == "sbi":
                    events, warns = parse_sbi_csv(raw_data)
                
                if warns:
                    _trade_sync_status["errors"].extend([f"{path}: {w}" for w in warns])

                if events:
                    conn.execute("DELETE FROM trade_events WHERE broker = ?", [broker])

                inserted_any = False
                for ev in events:
                    if not conn.execute("SELECT 1 FROM trade_events WHERE source_row_hash = ?", [ev.source_row_hash]).fetchone():
                        conn.execute("""
                            INSERT INTO trade_events (
                                broker,
                                exec_dt,
                                symbol,
                                action,
                                qty,
                                price,
                                source_row_hash,
                                transaction_type,
                                side_type,
                                margin_type
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            ev.broker,
                            ev.exec_dt,
                            ev.symbol,
                            ev.action,
                            ev.qty,
                            ev.price,
                            ev.source_row_hash,
                            getattr(ev, "transaction_type", None),
                            getattr(ev, "side_type", None),
                            getattr(ev, "margin_type", None)
                        ])
                        inserted_any = True
                        updated = True
                
                if inserted_any:
                    _trade_sync_status["processed"].append(path)

                _trade_file_mtime_cache[path] = mtime
                
            except Exception as exc:
                msg = f"Failed to auto sync {path}: {exc}"
                print(msg)
                _trade_sync_status["errors"].append(msg)

        if updated:
            rebuild_positions(conn)
            
    except Exception as e:
        msg = f"Auto sync error: {e}"
        print(msg)
        _trade_sync_status["errors"].append(msg)


@app.get("/api/debug/trade-sync")
def get_trade_sync_status():
    # Trigger sync to ensure we have latest status
    try:
        with get_conn() as conn:
            _auto_sync_csv_files(conn, force=True)
    except Exception as e:
        _trade_sync_status["errors"].append(f"Debug sync triggers error: {e}")
        
    return _trade_sync_status


@app.get("/api/positions/held")
def get_held_positions():
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT symbol, buy_qty, sell_qty, opened_at, has_issue, issue_note
            FROM positions_live
            WHERE buy_qty > 0 OR sell_qty > 0
            ORDER BY opened_at DESC
        """).fetchall()
        
        result = []
        for r in rows:
            sym = r[0]
            name_row = conn.execute("SELECT name FROM tickers WHERE code = ?", [sym]).fetchone()
            name = name_row[0] if name_row else ""
            
            b_qty = r[1]
            s_qty = r[2]
            b_str = f"{int(b_qty)}" if b_qty.is_integer() else f"{b_qty}"
            s_str = f"{int(s_qty)}" if s_qty.is_integer() else f"{s_qty}"
            
            result.append({
                "symbol": sym,
                "name": name,
                "buy_qty": b_qty,
                "sell_qty": s_qty,
                "sell_buy_text": f"{s_str}-{b_str}",
                "opened_at": r[3],
                "has_issue": r[4],
                "issue_note": r[5]
            })
            
        return {"items": result}

@app.get("/api/positions/history")
def get_position_history(symbol: str | None = None):
    with get_conn() as conn:
        query = """
            SELECT round_id, symbol, opened_at, closed_at, closed_reason, last_state_sell_buy, has_issue, issue_note
            FROM position_rounds
        """
        params = []
        if symbol:
            query += " WHERE symbol = ?"
            params.append(symbol)
            
        query += " ORDER BY closed_at DESC"
        
        rows = conn.execute(query, params).fetchall()
        result = []
        for r in rows:
            sym = r[1]
            name_row = conn.execute("SELECT name FROM tickers WHERE code = ?", [sym]).fetchone()
            name = name_row[0] if name_row else ""
            
            result.append({
                "round_id": r[0],
                "symbol": sym,
                "name": name,
                "opened_at": r[2],
                "closed_at": r[3],
                "closed_reason": r[4],
                "last_state_sell_buy": r[5],
                "has_issue": r[6],
                "issue_note": r[7]
            })
        return {"items": result}


@app.get("/api/positions/history/events")
def get_round_events(round_id: str):
    with get_conn() as conn:
        round_info = conn.execute(
            "SELECT symbol, opened_at, closed_at FROM position_rounds WHERE round_id = ?",
            [round_id]
        ).fetchone()
        
        if not round_info:
            return {"events": []}
            
        symbol = round_info[0]
        start_at = round_info[1]
        end_at = round_info[2]
        
        query = "SELECT broker, exec_dt, action, qty, price FROM trade_events WHERE symbol = ?"
        params = [symbol]
        
        if start_at:
            query += " AND exec_dt >= ?"
            params.append(start_at)
        if end_at:
            query += " AND exec_dt <= ?"
            params.append(end_at)
            
        query += " ORDER BY exec_dt ASC"
        
        rows = conn.execute(query, params).fetchall()
        events = []
        for r in rows:
            events.append({
                "broker": r[0],
                "exec_dt": r[1],
                "action": r[2],
                "qty": r[3],
                "price": r[4]
            })
            
        return {"events": events}


@app.delete("/api/practice/session")
def practice_session_delete(session_id: str | None = None):
    if not session_id:
        return JSONResponse(content={"error": "session_id_required"}, status_code=400)
    with _get_practice_conn() as conn:
        conn.execute(
            "DELETE FROM practice_sessions WHERE session_id = ?",
            [session_id]
        )
    return JSONResponse(content={"deleted": True})


@app.get("/api/practice/sessions")
def practice_sessions(code: str | None = None):
    query = """
        SELECT
            session_id,
            code,
            start_date,
            end_date,
            cursor_time,
            max_unlocked_time,
            lot_size,
            range_months,
            trades,
            notes,
            ui_state,
            created_at,
            updated_at
        FROM practice_sessions
        {where_clause}
        ORDER BY datetime(updated_at) DESC
    """
    params: list = []
    where_clause = ""
    if code:
        where_clause = "WHERE code = ?"
        params.append(code)
    with _get_practice_conn() as conn:
        rows = conn.execute(query.format(where_clause=where_clause), params).fetchall()
    sessions: list[dict] = []
    for row in rows:
        trades_raw = row["trades"] or "[]"
        try:
            trades = json.loads(trades_raw)
            if not isinstance(trades, list):
                trades = []
        except (TypeError, ValueError):
            trades = []
        ui_state_raw = row["ui_state"] or "{}"
        try:
            ui_state = json.loads(ui_state_raw)
            if not isinstance(ui_state, dict):
                ui_state = {}
        except (TypeError, ValueError):
            ui_state = {}
        sessions.append(
            {
                "session_id": row["session_id"],
                "code": row["code"],
                "start_date": row["start_date"],
                "end_date": row["end_date"],
                "cursor_time": row["cursor_time"],
                "max_unlocked_time": row["max_unlocked_time"],
                "lot_size": row["lot_size"],
                "range_months": row["range_months"],
                "trades": trades,
                "notes": row["notes"] or "",
                "ui_state": ui_state,
                "created_at": row["created_at"],
                "updated_at": row["updated_at"]
            }
        )
    return JSONResponse(content={"sessions": sessions})


@app.get("/api/practice/daily")
def practice_daily(code: str, limit: int = 400, session_id: str | None = None, start_date: str | None = None):
    query_with_ma = """
        WITH base AS (
            SELECT
                b.date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                m.ma7,
                m.ma20,
                m.ma60
            FROM daily_bars b
            LEFT JOIN daily_ma m
              ON b.code = m.code AND b.date = m.date
            WHERE b.code = ?
            {date_filter}
            ORDER BY b.date
        ),
        tail AS (
            SELECT *
            FROM base
            ORDER BY date DESC
            LIMIT ?
        )
        SELECT date, o, h, l, c, v, ma7, ma20, ma60
        FROM tail
        ORDER BY date
    """
    query_basic = """
        WITH base AS (
            SELECT
                b.date,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v
            FROM daily_bars b
            WHERE b.code = ?
            {date_filter}
            ORDER BY b.date
        ),
        tail AS (
            SELECT *
            FROM base
            ORDER BY date DESC
            LIMIT ?
        )
        SELECT date, o, h, l, c, v
        FROM tail
        ORDER BY date
    """
    errors: list[str] = []
    parsed_start = _parse_practice_date(start_date) if start_date is not None else None
    resolved = _resolve_practice_start_date(session_id, start_date)
    if start_date is not None and parsed_start is None and resolved is None:
        errors.append("practice_start_date_invalid")
    date_filter = ""
    params: list = [code]
    date_value = None
    try:
        with get_conn() as conn:
            if resolved:
                max_date = conn.execute(
                    "SELECT MAX(date) FROM daily_bars WHERE code = ?",
                    [code]
                ).fetchone()[0]
                use_epoch = max_date is not None and max_date >= 1_000_000_000
                if use_epoch:
                    date_value = int(calendar.timegm(resolved.timetuple()))
                else:
                    date_value = resolved.year * 10000 + resolved.month * 100 + resolved.day
                date_filter = "AND b.date <= ?"
                params.append(date_value)
            params.append(limit)
            rows = conn.execute(query_with_ma.format(date_filter=date_filter), params).fetchall()
        return JSONResponse(content={"data": rows, "errors": errors})
    except Exception as exc:
        errors.append(f"daily_query_failed:{exc}")
        try:
            with get_conn() as conn:
                fallback_params: list = [code]
                if resolved:
                    if date_value is None:
                        max_date = conn.execute(
                            "SELECT MAX(date) FROM daily_bars WHERE code = ?",
                            [code]
                        ).fetchone()[0]
                        use_epoch = max_date is not None and max_date >= 1_000_000_000
                        if use_epoch:
                            date_value = int(calendar.timegm(resolved.timetuple()))
                        else:
                            date_value = resolved.year * 10000 + resolved.month * 100 + resolved.day
                        date_filter = "AND b.date <= ?"
                    fallback_params.append(date_value)
                fallback_params.append(limit)
                rows = conn.execute(query_basic.format(date_filter=date_filter), fallback_params).fetchall()
            return JSONResponse(content={"data": rows, "errors": errors})
        except Exception as fallback_exc:
            errors.append(f"daily_query_fallback_failed:{fallback_exc}")
            return JSONResponse(content={"data": [], "errors": errors})


@app.get("/api/practice/monthly")
def practice_monthly(
    code: str,
    limit: int = 240,
    session_id: str | None = None,
    start_date: str | None = None
):
    errors: list[str] = []
    parsed_start = _parse_practice_date(start_date) if start_date is not None else None
    resolved = _resolve_practice_start_date(session_id, start_date)
    if start_date is not None and parsed_start is None and resolved is None:
        errors.append("practice_start_date_invalid")
    month_filter = ""
    params: list = [code]
    month_value = None
    try:
        with get_conn() as conn:
            if resolved:
                max_month = conn.execute(
                    "SELECT MAX(month) FROM monthly_bars WHERE code = ?",
                    [code]
                ).fetchone()[0]
                use_epoch = max_month is not None and max_month >= 1_000_000_000
                if use_epoch:
                    month_value = int(calendar.timegm(resolved.replace(day=1).timetuple()))
                else:
                    month_value = resolved.year * 100 + resolved.month
                month_filter = "AND month <= ?"
                params.append(month_value)
            params.append(limit)
            rows = conn.execute(
                f"""
                WITH base AS (
                    SELECT
                        month,
                        o,
                        h,
                        l,
                        c
                    FROM monthly_bars
                    WHERE code = ?
                    {month_filter}
                    ORDER BY month DESC
                    LIMIT ?
                )
                SELECT month, o, h, l, c
                FROM base
                ORDER BY month
                """,
                params
            ).fetchall()
        return JSONResponse(content={"data": rows, "errors": errors})
    except Exception as exc:
        errors.append(f"monthly_query_failed:{exc}")
        return JSONResponse(content={"data": [], "errors": errors})


@app.get("/api/ticker/monthly")
def monthly(code: str, limit: int = 240):
    try:
        with get_conn() as conn:
            rows = conn.execute(
                """
                WITH base AS (
                    SELECT
                        month,
                        o,
                        h,
                        l,
                        c
                    FROM monthly_bars
                    WHERE code = ?
                    ORDER BY month DESC
                    LIMIT ?
                )
                SELECT month, o, h, l, c
                FROM base
                ORDER BY month
                """,
                [code, limit]
            ).fetchall()

        return JSONResponse(content={"data": rows, "errors": []})
    except Exception as exc:
        return JSONResponse(content={"data": [], "errors": [f"monthly_query_failed:{exc}"]})


@app.get("/api/ticker/boxes")
def ticker_boxes(code: str):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT month, o, h, l, c
            FROM monthly_bars
            WHERE code = ?
            ORDER BY month
            """,
            [code]
        ).fetchall()

    return JSONResponse(content=detect_boxes(rows))


def _resolve_static_file(request_path: str) -> str | None:
    if not STATIC_DIR or not os.path.isdir(STATIC_DIR):
        return None
    safe_path = os.path.abspath(os.path.join(STATIC_DIR, request_path))
    if os.path.commonpath([STATIC_DIR, safe_path]) != STATIC_DIR:
        return None
    if os.path.isdir(safe_path):
        safe_path = os.path.join(safe_path, "index.html")
    if os.path.isfile(safe_path):
        return safe_path
    return None


def _normalize_units(qty: float) -> float:
    if not qty:
        return 0.0
    if qty >= 100:
        return qty / 100
    return qty


def _map_trade_event(ev: tuple) -> dict:
    exec_dt = ev[2]
    date_str = exec_dt.strftime("%Y-%m-%d") if exec_dt else ""
    action = ev[4] or ""
    qty = float(ev[5] or 0)
    units = _normalize_units(qty)
    side = "buy"
    trade_action = "open"
    kind = ""
    if action in ("SPOT_BUY", "MARGIN_OPEN_LONG"):
        side = "buy"
        trade_action = "open"
        kind = "BUY_OPEN"
    elif action in ("SPOT_SELL", "MARGIN_CLOSE_LONG"):
        side = "sell"
        trade_action = "close"
        kind = "SELL_CLOSE"
    elif action == "MARGIN_OPEN_SHORT":
        side = "sell"
        trade_action = "open"
        kind = "SELL_OPEN"
    elif action == "MARGIN_CLOSE_SHORT":
        side = "buy"
        trade_action = "close"
        kind = "BUY_CLOSE"
    elif action == "DELIVERY_SHORT":
        side = "buy"
        trade_action = "close"
        kind = "DELIVERY"
    elif action == "SPOT_IN":
        side = "buy"
        trade_action = "open"
        kind = "INBOUND"
    elif action == "SPOT_OUT":
        side = "sell"
        trade_action = "close"
        kind = "OUTBOUND"
    elif action == "MARGIN_SWAP_TO_SPOT":
        side = "buy"
        trade_action = "open"
        kind = "TAKE_DELIVERY"
    else:
        kind = action or "UNKNOWN"

    return {
        "broker": ev[1],
        "exec_dt": exec_dt.isoformat() if exec_dt else "",
        "tradeDate": date_str,
        "date": date_str,
        "code": ev[3],
        "symbol": ev[3],
        "action": trade_action,
        "side": side,
        "kind": kind,
        "qty": qty,
        "qtyShares": qty,
        "units": units,
        "price": ev[6],
        "memo": f"{ev[9]} {ev[10]}" if len(ev) > 9 else action,
        "raw": {
            "action": action,
            "transaction_type": ev[9] if len(ev) > 9 else "",
            "side_type": ev[10] if len(ev) > 10 else ""
        }
    }


def _calc_current_lots(trades: list[dict]) -> tuple[int, int]:
    ordered = [
        (trade, index)
        for index, trade in enumerate(trades)
    ]
    def sort_key(item: tuple[dict, int]) -> tuple[str, int, int]:
        trade, index = item
        date = trade.get("date") or ""
        action = trade.get("action") or ""
        action_rank = 0 if action == "open" else 1 if action == "close" else 2
        return (date, action_rank, index)
    ordered.sort(key=sort_key)

    long_lots = 0.0
    short_lots = 0.0
    for trade, _ in ordered:
        lots = trade.get("units") or 0
        try:
            lots = float(lots)
        except (TypeError, ValueError):
            lots = 0.0
        lots = max(0.0, lots)
        kind = trade.get("kind") or ""
        action = trade.get("action") or "open"
        side = trade.get("side") or ""

        if kind == "DELIVERY":
            long_lots = max(0, long_lots - lots)
            short_lots = max(0, short_lots - lots)
            continue
        if kind == "TAKE_DELIVERY":
            continue
        if kind == "INBOUND":
            if lots > 0:
                long_lots += lots
            continue
        if kind == "OUTBOUND":
            long_lots = max(0, long_lots - lots)
            continue

        if side == "buy" and action == "open":
            long_lots += lots
        elif side == "sell" and action == "close":
            long_lots = max(0.0, long_lots - lots)
        elif side == "sell" and action == "open":
            short_lots += lots
        elif side == "buy" and action == "close":
            short_lots = max(0.0, short_lots - lots)

    return long_lots, short_lots


def _calc_position_metrics(trades: list[dict]) -> dict:
    ordered = [
        (trade, index)
        for index, trade in enumerate(trades)
    ]
    def sort_key(item: tuple[dict, int]) -> tuple[str, int, int]:
        trade, index = item
        date = trade.get("date") or ""
        action = trade.get("action") or ""
        action_rank = 0 if action == "open" else 1 if action == "close" else 2
        return (date, action_rank, index)
    ordered.sort(key=sort_key)

    long_lots = 0.0
    short_lots = 0.0
    avg_long_price = 0.0
    avg_short_price = 0.0
    realized_pnl = 0.0

    for trade, _ in ordered:
        lots = trade.get("units") or 0
        try:
            lots = float(lots)
        except (TypeError, ValueError):
            lots = 0.0
        lots = max(0.0, lots)
        price = float(trade.get("price") or 0)
        action = trade.get("action") or "open"
        kind = trade.get("kind") or ""
        side = trade.get("side") or ""

        if kind == "DELIVERY":
            long_lots = max(0.0, long_lots - lots)
            short_lots = max(0.0, short_lots - lots)
            continue
        if kind == "TAKE_DELIVERY":
            continue
        if kind == "INBOUND":
            if long_lots > 0 and lots > 0:
                total_cost = avg_long_price * long_lots
                long_lots += lots
                avg_long_price = total_cost / long_lots
            elif lots > 0:
                long_lots += lots
                avg_long_price = price or avg_long_price
            continue
        if kind == "OUTBOUND":
            long_lots = max(0.0, long_lots - lots)
            if long_lots == 0:
                avg_long_price = 0.0
            continue

        if side == "buy" and action == "open":
            next_lots = long_lots + lots
            avg_long_price = (
                (avg_long_price * long_lots + price * lots) / next_lots
                if next_lots > 0
                else 0.0
            )
            long_lots = next_lots
        elif side == "sell" and action == "close":
            close_lots = min(long_lots, lots)
            realized_pnl += (price - avg_long_price) * close_lots * 100
            long_lots = max(0.0, long_lots - lots)
            if long_lots == 0:
                avg_long_price = 0.0
        elif side == "sell" and action == "open":
            next_lots = short_lots + lots
            avg_short_price = (
                (avg_short_price * short_lots + price * lots) / next_lots
                if next_lots > 0
                else 0.0
            )
            short_lots = next_lots
        elif side == "buy" and action == "close":
            close_lots = min(short_lots, lots)
            realized_pnl += (avg_short_price - price) * close_lots * 100
            short_lots = max(0.0, short_lots - lots)
            if short_lots == 0:
                avg_short_price = 0.0

    return {
        "longLots": long_lots,
        "shortLots": short_lots,
        "avgLongPrice": avg_long_price,
        "avgShortPrice": avg_short_price,
        "realizedPnL": realized_pnl
    }


def _resolve_broker_meta(raw: str | None) -> tuple[str, str]:
    text = (raw or "").strip()
    lower = text.lower()
    if "sbi" in lower:
        return "sbi", "SBI"
    if "rakuten" in lower:
        return "rakuten", "RAKUTEN"
    if text:
        return lower, text.upper()
    return "unknown", "N/A"


def _calc_current_positions_by_broker(trades: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for trade in trades:
        key, label = _resolve_broker_meta(trade.get("broker"))
        bucket = grouped.get(key)
        if not bucket:
            bucket = {"key": key, "label": label, "trades": []}
            grouped[key] = bucket
        bucket["trades"].append(trade)

    results: list[dict] = []
    for bucket in grouped.values():
        metrics = _calc_position_metrics(bucket["trades"])
        results.append(
            {
                "brokerKey": bucket["key"],
                "brokerLabel": bucket["label"],
                "longLots": metrics["longLots"],
                "shortLots": metrics["shortLots"],
                "avgLongPrice": metrics["avgLongPrice"],
                "avgShortPrice": metrics["avgShortPrice"],
                "realizedPnL": metrics["realizedPnL"]
            }
        )
    return results


@app.get("/")
def serve_root():
    index_path = _resolve_static_file("index.html")
    if not index_path:
        return JSONResponse(status_code=404, content={"error": "static_not_found"})
    return FileResponse(index_path)


@app.get("/{full_path:path}")
def serve_spa(full_path: str):
    if full_path.startswith("api") or full_path.startswith("health"):
        raise HTTPException(status_code=404)
    resolved = _resolve_static_file(full_path)
    if resolved:
        return FileResponse(resolved)
    index_path = _resolve_static_file("index.html")
    if not index_path:
        return JSONResponse(status_code=404, content={"error": "static_not_found"})
    return FileResponse(index_path)
