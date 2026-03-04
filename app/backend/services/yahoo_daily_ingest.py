from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from app.backend.services.yahoo_provisional import (
    get_provisional_daily_rows_from_spark,
    normalize_date_key,
)
from app.db.session import get_conn

logger = logging.getLogger(__name__)


def _today_jst_key() -> int:
    return int((datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y%m%d"))


def _date_key_to_utc_epoch(date_key: int) -> int:
    text = str(int(date_key))
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"invalid date key: {date_key}")
    parsed = datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _load_target_codes(conn, *, max_codes: int | None = None) -> tuple[list[str], str]:
    queries = (
        (
            "tickers",
            """
            SELECT DISTINCT TRIM(code) AS code
            FROM tickers
            WHERE code IS NOT NULL AND TRIM(code) <> ''
            ORDER BY code
            """,
        ),
        (
            "daily_bars",
            """
            SELECT DISTINCT TRIM(code) AS code
            FROM daily_bars
            WHERE code IS NOT NULL AND TRIM(code) <> ''
            ORDER BY code
            """,
        ),
    )
    for source, query in queries:
        try:
            rows = conn.execute(query).fetchall()
        except Exception:
            continue
        codes = [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]
        if codes:
            if max_codes is not None and max_codes > 0:
                codes = codes[: max_codes]
            return codes, source
    return [], "none"


def _load_latest_date_key_map(conn, codes: Sequence[str]) -> dict[str, int | None]:
    if not codes:
        return {}
    latest: dict[str, int | None] = {str(code): None for code in codes}
    chunk_size = 500
    for start in range(0, len(codes), chunk_size):
        chunk = [str(code) for code in codes[start : start + chunk_size] if str(code)]
        if not chunk:
            continue
        placeholders = ",".join(["?"] * len(chunk))
        query = f"""
            SELECT code, MAX(date) AS max_date
            FROM daily_bars
            WHERE code IN ({placeholders})
            GROUP BY code
        """
        try:
            rows = conn.execute(query, chunk).fetchall()
        except Exception:
            logger.exception("Failed to load latest daily_bars for chunk size=%s", len(chunk))
            continue
        for row in rows:
            code = str(row[0]).strip()
            key = normalize_date_key(row[1])
            latest[code] = key
    return latest


def _insert_rows(conn, rows: Sequence[tuple[str, int, float, float, float, float, float]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DROP TABLE IF EXISTS _tmp_yf_daily_ingest")
        conn.execute(
            """
            CREATE TEMP TABLE _tmp_yf_daily_ingest (
                code TEXT,
                date BIGINT,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v DOUBLE
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO _tmp_yf_daily_ingest (code, date, o, h, l, c, v)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [list(row) for row in rows],
        )

        inserted = int(
            conn.execute(
                """
                WITH dedup AS (
                    SELECT
                        code,
                        date,
                        o,
                        h,
                        l,
                        c,
                        v,
                        ROW_NUMBER() OVER (PARTITION BY code, date ORDER BY code) AS rn
                    FROM _tmp_yf_daily_ingest
                ),
                pending AS (
                    SELECT code, date, o, h, l, c, v
                    FROM dedup
                    WHERE rn = 1
                )
                SELECT COUNT(*)
                FROM pending p
                LEFT JOIN daily_bars d
                    ON d.code = p.code AND d.date = p.date
                WHERE d.code IS NULL
                """
            ).fetchone()[0]
        )

        conn.execute(
            """
            INSERT INTO daily_bars (code, date, o, h, l, c, v)
            WITH dedup AS (
                SELECT
                    code,
                    date,
                    o,
                    h,
                    l,
                    c,
                    v,
                    ROW_NUMBER() OVER (PARTITION BY code, date ORDER BY code) AS rn
                FROM _tmp_yf_daily_ingest
            )
            SELECT p.code, p.date, p.o, p.h, p.l, p.c, p.v
            FROM dedup p
            LEFT JOIN daily_bars d
                ON d.code = p.code AND d.date = p.date
            WHERE p.rn = 1 AND d.code IS NULL
            """
        )
        conn.execute("DROP TABLE IF EXISTS _tmp_yf_daily_ingest")
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise

    conflicts = max(0, len(rows) - inserted)
    return inserted, conflicts


def get_daily_ingest_coverage(*, target_date_key: int | None = None, max_codes: int | None = None) -> dict[str, Any]:
    target_key = normalize_date_key(target_date_key) if target_date_key is not None else _today_jst_key()
    if target_key is None:
        target_key = _today_jst_key()
    with get_conn() as conn:
        codes, universe_source = _load_target_codes(conn, max_codes=max_codes)
        latest_map = _load_latest_date_key_map(conn, codes)

    total = len(codes)
    covered_codes = 0
    missing_codes: list[str] = []
    for code in codes:
        latest_key = latest_map.get(code)
        if latest_key is not None and latest_key >= target_key:
            covered_codes += 1
        else:
            missing_codes.append(code)

    coverage_ratio = (float(covered_codes) / float(total)) if total > 0 else 1.0
    return {
        "target_date": target_key,
        "total_codes": total,
        "covered_codes": covered_codes,
        "missing_codes": total - covered_codes,
        "coverage_ratio": coverage_ratio,
        "universe_source": universe_source,
        "missing_codes_sample": missing_codes[:50],
    }


def ingest_latest_provisional_daily_rows(
    *,
    max_codes: int | None = None,
    asof_dt: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    asof_key = normalize_date_key(asof_dt) if asof_dt is not None else None

    with get_conn() as conn:
        codes, universe_source = _load_target_codes(conn, max_codes=max_codes)
        latest_map = _load_latest_date_key_map(conn, codes)

    if not codes:
        coverage = get_daily_ingest_coverage(target_date_key=asof_key)
        return {
            "ok": True,
            "dry_run": bool(dry_run),
            "asof_date": asof_key,
            "target_codes": 0,
            "universe_source": universe_source,
            "fetched_codes": 0,
            "insert_candidates": 0,
            "inserted": 0,
            "conflicts": 0,
            "skipped_not_newer": 0,
            "skipped_asof": 0,
            "missing_from_yahoo": 0,
            "latest_yahoo_date": None,
            "coverage": coverage,
        }

    provisional_map = get_provisional_daily_rows_from_spark(codes)

    rows_to_insert: list[tuple[str, int, float, float, float, float, float]] = []
    fetched_codes = 0
    skipped_not_newer = 0
    skipped_asof = 0
    missing_from_yahoo = 0
    latest_yahoo_key: int | None = None

    for code in codes:
        row = provisional_map.get(code)
        if not row:
            missing_from_yahoo += 1
            continue
        fetched_codes += 1
        row_key = normalize_date_key(row[0])
        if row_key is None:
            missing_from_yahoo += 1
            continue
        if latest_yahoo_key is None or row_key > latest_yahoo_key:
            latest_yahoo_key = row_key
        if asof_key is not None and row_key > asof_key:
            skipped_asof += 1
            continue
        last_key = latest_map.get(code)
        if last_key is not None and row_key <= last_key:
            skipped_not_newer += 1
            continue
        rows_to_insert.append(
            (
                code,
                _date_key_to_utc_epoch(int(row_key)),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[5]),
            )
        )

    inserted = 0
    conflicts = 0
    if rows_to_insert and not dry_run:
        with get_conn() as conn:
            inserted, conflicts = _insert_rows(conn, rows_to_insert)

    coverage_target = latest_yahoo_key or asof_key or _today_jst_key()
    coverage = get_daily_ingest_coverage(target_date_key=coverage_target, max_codes=max_codes)
    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "asof_date": asof_key,
        "target_codes": len(codes),
        "universe_source": universe_source,
        "fetched_codes": fetched_codes,
        "insert_candidates": len(rows_to_insert),
        "inserted": inserted,
        "conflicts": conflicts,
        "skipped_not_newer": skipped_not_newer,
        "skipped_asof": skipped_asof,
        "missing_from_yahoo": missing_from_yahoo,
        "latest_yahoo_date": latest_yahoo_key,
        "coverage": coverage,
    }
