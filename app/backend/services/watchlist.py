from __future__ import annotations

import glob
import json
import os
import re
import shutil
import sqlite3
import threading
from datetime import datetime

from app.core.config import (
    DATA_DIR,
    FAVORITES_DB_PATH,
    PRACTICE_DB_PATH,
    find_code_txt_path,
    resolve_pan_code_txt_path,
)
from app.db.session import get_conn
from app.services.screener_engine import _invalidate_screener_cache

WATCHLIST_TRASH_DIR = os.path.join(DATA_DIR, "trash")
WATCHLIST_TRASH_PATTERNS = [
    os.path.join(DATA_DIR, "csv", "{code}*.csv"),
    os.path.join(DATA_DIR, "txt", "{code}*.txt"),
]
WATCHLIST_CODE_RE = re.compile(r"^\d{4}[A-Z]?$")
watchlist_lock = threading.Lock()


def resolve_watchlist_path() -> str:
    primary = resolve_pan_code_txt_path()
    fallback = find_code_txt_path(DATA_DIR)
    return fallback or primary


def normalize_watch_code(raw: str | None) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    fullwidth = str.maketrans(
        "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ",
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    )
    text = text.translate(fullwidth)
    text = re.sub(r"\s+", "", text)
    text = text.upper()
    if not WATCHLIST_CODE_RE.match(text):
        return None
    return text


def _extract_code_from_line(line: str) -> str | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or stripped.startswith("'"):
        return None
    token = re.split(r"[,\t ]+", stripped, maxsplit=1)[0]
    return normalize_watch_code(token)


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


def load_watchlist_codes(path: str) -> list[str]:
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


def update_watchlist_file(path: str, code: str, remove: bool) -> bool:
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


def trash_watchlist_artifacts(code: str) -> tuple[str | None, list[str]]:
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


def restore_watchlist_artifacts(token: str) -> list[str]:
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
        if not src or not dest or not os.path.isfile(src):
            continue
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.move(src, dest)
        restored.append(dest)
    return restored


def invalidate_screener_cache() -> None:
    _invalidate_screener_cache()


def _get_favorites_conn():
    conn = sqlite3.connect(FAVORITES_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def delete_favorites_code(code: str) -> int:
    with _get_favorites_conn() as conn:
        cursor = conn.execute("DELETE FROM favorites WHERE code = ?", [code])
        return cursor.rowcount or 0


def _get_practice_conn():
    conn = sqlite3.connect(PRACTICE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def delete_practice_sessions(code: str) -> int:
    with _get_practice_conn() as conn:
        cursor = conn.execute("DELETE FROM practice_sessions WHERE code = ?", [code])
        return cursor.rowcount or 0


def delete_ticker_db_rows(code: str) -> dict[str, int]:
    tables = ["daily_bars", "daily_ma", "monthly_bars", "monthly_ma", "stock_meta", "tickers"]
    counts: dict[str, int] = {}
    with get_conn() as conn:
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE code = ?", [code]).fetchone()[0]
            counts[table] = int(count or 0)
        for table in tables:
            conn.execute(f"DELETE FROM {table} WHERE code = ?", [code])
    return counts
