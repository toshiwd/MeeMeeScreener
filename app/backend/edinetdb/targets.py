from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import duckdb

from app.core.config import DATA_DIR, FAVORITES_DB_PATH, find_code_txt_path, resolve_pan_code_txt_path


_SEC_CODE_RE = re.compile(r"(\d{4})")


def _dedup_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def normalize_sec_code(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = _SEC_CODE_RE.search(text)
    if not match:
        return None
    return match.group(1)


def _read_lines(path: Path) -> list[str]:
    for enc in ("utf-8", "cp932"):
        try:
            return path.read_text(encoding=enc).splitlines()
        except OSError:
            continue
    return []


def load_code_txt_codes() -> list[str]:
    primary = Path(resolve_pan_code_txt_path())
    fallback = find_code_txt_path(DATA_DIR)
    path = primary if primary.exists() else (Path(fallback) if fallback else primary)
    if not path.exists():
        return []
    seen: set[str] = set()
    codes: list[str] = []
    for line in _read_lines(path):
        code = normalize_sec_code(line)
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def load_holdings_codes(db_path: str | Path) -> list[str]:
    query = """
        SELECT symbol
        FROM positions_live
        WHERE COALESCE(buy_qty, 0) > 0 OR COALESCE(sell_qty, 0) > 0
    """
    codes: list[str] = []
    conn = duckdb.connect(str(Path(db_path).expanduser().resolve()))
    try:
        rows = conn.execute(query).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()
    for row in rows:
        code = normalize_sec_code(row[0] if row else None)
        if code:
            codes.append(code)
    return sorted(set(codes))


def load_favorites_codes(favorites_db_path: str | Path = FAVORITES_DB_PATH) -> list[str]:
    db_path = str(Path(favorites_db_path).expanduser().resolve())
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT code FROM favorites ORDER BY created_at DESC").fetchall()
    except sqlite3.Error:
        rows = []
    finally:
        conn.close()
    codes: list[str] = []
    for row in rows:
        code = normalize_sec_code(row[0] if row else None)
        if code:
            codes.append(code)
    return sorted(set(codes))


def load_ranking_codes_from_stock_scores(db_path: str | Path, limit: int) -> list[str]:
    conn = duckdb.connect(str(Path(db_path).expanduser().resolve()))
    try:
        rows = conn.execute(
            """
            SELECT code
            FROM stock_scores
            ORDER BY score_a DESC NULLS LAST, score_b DESC NULLS LAST, updated_at DESC NULLS LAST
            LIMIT ?
            """,
            [int(limit)],
        ).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()
    codes: list[str] = []
    for row in rows:
        code = normalize_sec_code(row[0] if row else None)
        if code:
            codes.append(code)
    return codes


def load_ranking_codes_from_rankings_cache(limit: int) -> list[str]:
    try:
        from app.backend.services import rankings_cache

        response = rankings_cache.get_rankings(
            "D",
            "latest",
            "up",
            int(limit),
            mode="hybrid",
            risk_mode="balanced",
        )
    except Exception:
        return []
    codes: list[str] = []
    for item in (response or {}).get("items", []):
        code = normalize_sec_code(item.get("code") if isinstance(item, dict) else None)
        if code:
            codes.append(code)
    return codes


def load_ranking_codes(db_path: str | Path, limit: int) -> list[str]:
    codes = load_ranking_codes_from_stock_scores(db_path, limit)
    if codes:
        return _dedup_preserve_order(codes)
    return _dedup_preserve_order(load_ranking_codes_from_rankings_cache(limit))
