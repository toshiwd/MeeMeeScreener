from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.sell_analysis_accumulator import (  # noqa: E402
    _ensure_table,
    _refresh_future_outcomes,
    _upsert_snapshot_for_date,
)


def _resolve_db_path(cli_value: str | None) -> Path:
    if cli_value:
        path = Path(cli_value).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"DB not found: {path}")
        return path
    env = os.getenv("STOCKS_DB_PATH")
    if env:
        path = Path(env).expanduser().resolve()
        if path.exists():
            return path
    default = Path(os.getenv("LOCALAPPDATA", str(Path.home()))) / "MeeMeeScreener" / "data" / "stocks.duckdb"
    if default.exists():
        return default
    raise FileNotFoundError("Could not resolve DB path. Pass --db-path or set STOCKS_DB_PATH.")


def _normalize_ymd_expr(column: str) -> str:
    num = f"TRY_CAST({column} AS BIGINT)"
    return (
        "CASE "
        f"WHEN {num} BETWEEN 19000101 AND 20991231 THEN CAST({num} AS INTEGER) "
        f"WHEN {num} >= 1000000000000 THEN CAST(strftime(to_timestamp({num}/1000), '%Y%m%d') AS INTEGER) "
        f"WHEN {num} BETWEEN 600000000 AND 5000000000 THEN CAST(strftime(to_timestamp({num}), '%Y%m%d') AS INTEGER) "
        "ELSE NULL END"
    )


def _fetch_range_dates(
    conn: duckdb.DuckDBPyConnection,
    *,
    start_ymd: int,
    end_ymd: int,
) -> list[int]:
    ymd_expr = _normalize_ymd_expr("date")
    rows = conn.execute(
        f"""
        SELECT DISTINCT CAST(date AS BIGINT) AS dt_raw
        FROM daily_bars
        WHERE {ymd_expr} BETWEEN ? AND ?
        ORDER BY dt_raw
        """,
        [int(start_ymd), int(end_ymd)],
    ).fetchall()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def _load_tmp_dates(conn: duckdb.DuckDBPyConnection, dates: list[int]) -> None:
    conn.execute("DROP TABLE IF EXISTS tmp_backfill_dates")
    conn.execute("CREATE TEMP TABLE tmp_backfill_dates (dt BIGINT PRIMARY KEY)")
    if dates:
        conn.executemany(
            "INSERT INTO tmp_backfill_dates(dt) VALUES (?)",
            [(int(v),) for v in dates],
        )


def _coverage_summary(conn: duckdb.DuckDBPyConnection) -> dict[str, int | None]:
    row = conn.execute(
        """
        WITH first_seen AS (
          SELECT code, MIN(CAST(date AS BIGINT)) AS first_dt
          FROM daily_bars
          GROUP BY code
        ),
        expected AS (
          SELECT d.dt, COUNT(*) AS expected_codes
          FROM tmp_backfill_dates d
          JOIN first_seen f ON f.first_dt <= d.dt
          GROUP BY d.dt
        ),
        actual AS (
          SELECT CAST(s.dt AS BIGINT) AS dt, COUNT(*) AS actual_rows
          FROM sell_analysis_daily s
          JOIN tmp_backfill_dates d ON CAST(s.dt AS BIGINT) = d.dt
          GROUP BY CAST(s.dt AS BIGINT)
        ),
        joined AS (
          SELECT
            e.dt,
            e.expected_codes,
            COALESCE(a.actual_rows, 0) AS actual_rows
          FROM expected e
          LEFT JOIN actual a ON a.dt = e.dt
        )
        SELECT
          COUNT(*) AS n_dates,
          SUM(CASE WHEN actual_rows = 0 THEN 1 ELSE 0 END) AS zero_dates,
          SUM(CASE WHEN actual_rows < expected_codes THEN 1 ELSE 0 END) AS incomplete_dates,
          MIN(actual_rows) AS min_rows,
          MAX(actual_rows) AS max_rows,
          MIN(expected_codes) AS min_expected,
          MAX(expected_codes) AS max_expected
        FROM joined
        """
    ).fetchone()
    if not row:
        return {
            "n_dates": 0,
            "zero_dates": 0,
            "incomplete_dates": 0,
            "min_rows": None,
            "max_rows": None,
            "min_expected": None,
            "max_expected": None,
        }
    return {
        "n_dates": int(row[0]) if row[0] is not None else 0,
        "zero_dates": int(row[1]) if row[1] is not None else 0,
        "incomplete_dates": int(row[2]) if row[2] is not None else 0,
        "min_rows": int(row[3]) if row[3] is not None else None,
        "max_rows": int(row[4]) if row[4] is not None else None,
        "min_expected": int(row[5]) if row[5] is not None else None,
        "max_expected": int(row[6]) if row[6] is not None else None,
    }


def _count_rows_for_tmp_dates(conn: duckdb.DuckDBPyConnection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM sell_analysis_daily s
        JOIN tmp_backfill_dates d ON CAST(s.dt AS BIGINT) = d.dt
        """
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _count_sell_codes_for_tmp_dates(conn: duckdb.DuckDBPyConnection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT s.code)
        FROM sell_analysis_daily s
        JOIN tmp_backfill_dates d ON CAST(s.dt AS BIGINT) = d.dt
        """
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _select_target_dates(
    conn: duckdb.DuckDBPyConnection,
    *,
    all_dates: list[int],
    missing_only: bool,
    min_coverage_ratio: float,
) -> list[int]:
    if not missing_only:
        return all_dates
    rows = conn.execute(
        """
        WITH first_seen AS (
          SELECT code, MIN(CAST(date AS BIGINT)) AS first_dt
          FROM daily_bars
          GROUP BY code
        ),
        expected AS (
          SELECT d.dt, COUNT(*) AS expected_codes
          FROM tmp_backfill_dates d
          JOIN first_seen f ON f.first_dt <= d.dt
          GROUP BY d.dt
        ),
        actual AS (
          SELECT CAST(s.dt AS BIGINT) AS dt, COUNT(*) AS actual_rows
          FROM sell_analysis_daily s
          JOIN tmp_backfill_dates d ON CAST(s.dt AS BIGINT) = d.dt
          GROUP BY CAST(s.dt AS BIGINT)
        )
        SELECT e.dt
        FROM expected e
        LEFT JOIN actual a ON a.dt = e.dt
        WHERE COALESCE(a.actual_rows, 0) < CAST(CEIL(e.expected_codes * ?) AS BIGINT)
        ORDER BY e.dt
        """,
        [float(min_coverage_ratio)],
    ).fetchall()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill sell_analysis_daily for a historical date range.")
    parser.add_argument("--db-path", default="", help="Path to stocks.duckdb")
    parser.add_argument("--start-ymd", type=int, default=20160226, help="Start date (YYYYMMDD)")
    parser.add_argument("--end-ymd", type=int, default=20260226, help="End date (YYYYMMDD)")
    parser.add_argument("--chunk-size", type=int, default=30, help="Dates per checkpoint chunk")
    parser.add_argument("--missing-only", action="store_true", help="Only process dates missing in sell_analysis_daily")
    parser.add_argument(
        "--min-coverage-ratio",
        type=float,
        default=0.995,
        help="When --missing-only, reprocess dates with row coverage below this ratio",
    )
    parser.add_argument(
        "--output",
        default="tmp/backfill_sell_analysis_summary.json",
        help="Output summary JSON path",
    )
    args = parser.parse_args()

    if int(args.start_ymd) > int(args.end_ymd):
        raise ValueError("--start-ymd must be <= --end-ymd")
    chunk_size = max(1, int(args.chunk_size))
    min_coverage_ratio = max(0.0, min(float(args.min_coverage_ratio), 1.0))

    db_path = _resolve_db_path(args.db_path or None)
    started_at = datetime.now(timezone.utc)
    print(
        f"[start] db={db_path} range={args.start_ymd}..{args.end_ymd} "
        f"missing_only={bool(args.missing_only)} min_coverage_ratio={min_coverage_ratio:.4f}"
    )

    with duckdb.connect(str(db_path)) as conn:
        _ensure_table(conn)
        all_dates = _fetch_range_dates(conn, start_ymd=int(args.start_ymd), end_ymd=int(args.end_ymd))
        _load_tmp_dates(conn, all_dates)
        before_rows = _count_rows_for_tmp_dates(conn)
        coverage_before = _coverage_summary(conn)
        target_dates = _select_target_dates(
            conn,
            all_dates=all_dates,
            missing_only=bool(args.missing_only),
            min_coverage_ratio=min_coverage_ratio,
        )

        total = len(target_dates)
        if total == 0:
            print("[done] no target dates")
            after_rows = _count_rows_for_tmp_dates(conn)
            coverage_after = _coverage_summary(conn)
            payload = {
                "ok": True,
                "db_path": str(db_path),
                "start_ymd": int(args.start_ymd),
                "end_ymd": int(args.end_ymd),
                "all_dates_in_range": int(len(all_dates)),
                "target_dates": 0,
                "processed_dates": 0,
                "before_rows_in_range": int(before_rows),
                "after_rows_in_range": int(after_rows),
                "coverage_before": coverage_before,
                "coverage_after": coverage_after,
                "min_coverage_ratio": min_coverage_ratio,
                "started_at_utc": started_at.isoformat(),
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                "duration_seconds": (datetime.now(timezone.utc) - started_at).total_seconds(),
            }
            out_path = Path(args.output).expanduser().resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[summary] {out_path}")
            return

        processed = 0
        last_rows = 0
        for idx, dt in enumerate(target_dates, start=1):
            last_rows = _upsert_snapshot_for_date(conn, int(dt))
            processed = idx
            if idx % chunk_size == 0 or idx == total:
                conn.execute("CHECKPOINT")
                print(f"[progress] {idx}/{total} dt={dt} rows_for_dt={last_rows}")

        print("[progress] refreshing future outcomes...")
        _refresh_future_outcomes(conn)
        conn.execute("CHECKPOINT")

        after_rows = _count_rows_for_tmp_dates(conn)
        coverage_after = _coverage_summary(conn)
        sell_codes = _count_sell_codes_for_tmp_dates(conn)

    finished_at = datetime.now(timezone.utc)
    payload = {
        "ok": True,
        "db_path": str(db_path),
        "start_ymd": int(args.start_ymd),
        "end_ymd": int(args.end_ymd),
        "all_dates_in_range": int(len(all_dates)),
        "target_dates": int(total),
        "processed_dates": int(processed),
        "before_rows_in_range": int(before_rows),
        "after_rows_in_range": int(after_rows),
        "coverage_before": coverage_before,
        "coverage_after": coverage_after,
        "sell_codes_in_range": int(sell_codes),
        "last_rows_for_dt": int(last_rows),
        "missing_only": bool(args.missing_only),
        "min_coverage_ratio": min_coverage_ratio,
        "chunk_size": int(chunk_size),
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "duration_seconds": (finished_at - started_at).total_seconds(),
    }
    out_path = Path(args.output).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] {processed}/{total} dates updated")
    print(f"[summary] {out_path}")


if __name__ == "__main__":
    main()
