from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


AXIS_ID = "quarantine_stale_history_codes_v1"
DEFAULT_DB_PATH = Path(os.environ.get("STOCKS_DB_PATH") or r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\quarantine_stale_history_codes_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _ymd_expr(column: str = "date") -> str:
    return f"CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)"


def _load_stale_codes(conn: duckdb.DuckDBPyConnection, *, min_latest_ymd: int, explicit_codes: list[str] | None) -> list[dict[str, Any]]:
    code_filter = ""
    params: list[Any] = []
    if explicit_codes:
        placeholders = ",".join("?" for _ in explicit_codes)
        code_filter = f" AND d.code IN ({placeholders})"
        params.extend(explicit_codes)
    rows = conn.execute(
        f"""
        SELECT
          d.code,
          COALESCE(sm.name, d.code) AS name,
          MAX({_ymd_expr('d.date')}) AS max_ymd,
          COUNT(*) AS daily_rows,
          string_agg(DISTINCT COALESCE(d.source, 'unknown'), ',') AS sources,
          sm.stage,
          sm.score,
          sm.score_status,
          sm.missing_reasons_json,
          sm.reason,
          sm.latest_close
        FROM daily_bars d
        LEFT JOIN stock_meta sm ON sm.code = d.code
        WHERE 1=1
        {code_filter}
        GROUP BY d.code, sm.name, sm.stage, sm.score, sm.score_status, sm.missing_reasons_json, sm.reason, sm.latest_close
        HAVING max_ymd < ?
        ORDER BY max_ymd DESC, d.code
        """,
        [*params, int(min_latest_ymd)],
    ).fetchall()
    return [
        {
            "code": str(code),
            "name": str(name),
            "max_ymd": int(max_ymd),
            "daily_rows": int(daily_rows),
            "sources": str(sources or ""),
            "before_stock_meta": {
                "stage": stage,
                "score": float(score) if score is not None else None,
                "score_status": score_status,
                "missing_reasons_json": missing_reasons_json,
                "reason": reason,
                "latest_close": float(latest_close) if latest_close is not None else None,
            },
        }
        for code, name, max_ymd, daily_rows, sources, stage, score, score_status, missing_reasons_json, reason, latest_close in rows
    ]


def _quarantine(conn: duckdb.DuckDBPyConnection, stale: list[dict[str, Any]], *, min_latest_ymd: int) -> int:
    if not stale:
        return 0
    codes = [row["code"] for row in stale]
    frame = pd.DataFrame(
        [
            {
                "code": row["code"],
                "reason": f"STALE_HISTORY: latest_daily_bar={row['max_ymd']} < required={min_latest_ymd}",
                "missing_reasons_json": json.dumps(
                    [
                        "stale_history",
                        f"latest_daily_bar:{row['max_ymd']}",
                        f"required_latest_daily_bar:{min_latest_ymd}",
                    ],
                    ensure_ascii=False,
                ),
                "score_breakdown_json": json.dumps(
                    {
                        "quarantine": True,
                        "axis_id": AXIS_ID,
                        "latest_daily_bar": row["max_ymd"],
                        "required_latest_daily_bar": min_latest_ymd,
                        "sources": row["sources"],
                    },
                    ensure_ascii=False,
                ),
            }
            for row in stale
        ]
    )
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.register("stale_quarantine_df", frame)
        conn.execute(
            """
            UPDATE stock_meta AS sm
            SET
              stage = 'STALE',
              score = NULL,
              reason = q.reason,
              score_status = 'STALE_HISTORY',
              missing_reasons_json = q.missing_reasons_json,
              score_breakdown_json = q.score_breakdown_json,
              signal_flags = NULL,
              updated_at = NOW()
            FROM stale_quarantine_df q
            WHERE sm.code = q.code
            """
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return len(codes)


def run(
    *,
    db_path: Path,
    output_root: Path,
    min_latest_ymd: int,
    codes: list[str] | None,
    dry_run: bool,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    with duckdb.connect(str(db_path)) as conn:
        stale = _load_stale_codes(conn, min_latest_ymd=min_latest_ymd, explicit_codes=codes)
        quarantined = 0 if dry_run else _quarantine(conn, stale, min_latest_ymd=min_latest_ymd)
        after = _load_stale_codes(conn, min_latest_ymd=min_latest_ymd, explicit_codes=[row["code"] for row in stale])
        after_meta = conn.execute(
            "SELECT code, stage, score, score_status, missing_reasons_json, reason FROM stock_meta WHERE code IN (SELECT code FROM (SELECT UNNEST(?) AS code)) ORDER BY code",
            [[row["code"] for row in stale]],
        ).fetchall() if stale else []

    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "MeeMee",
        "db_path": str(db_path),
        "min_latest_ymd": int(min_latest_ymd),
        "dry_run": bool(dry_run),
        "runtime_db_write": not dry_run and bool(stale),
        "production_ranking_changed": False,
        "research_logic_changed": False,
        "stale_count": len(stale),
        "quarantined_count": quarantined,
        "stale_codes": stale,
        "stale_after_count": len(after),
        "post_quarantine_stock_meta": [
            {
                "code": str(code),
                "stage": stage,
                "score": float(score) if score is not None else None,
                "score_status": score_status,
                "missing_reasons_json": missing_reasons_json,
                "reason": reason,
            }
            for code, stage, score, score_status, missing_reasons_json, reason in after_meta
        ],
        "decision": {
            "candidate_local_decision": "quarantine_applied" if quarantined else "no_quarantine_applied",
            "reason": "stale histories that could not be refreshed are explicitly excluded from scored candidate status",
        },
    }
    _write_json(output_dir / "quarantine_stale_history_codes_report.json", report)
    _write_json(output_root / "latest_quarantine_stale_history_codes_report.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-latest-ymd", type=int, default=20260703)
    parser.add_argument("--codes", default="", help="Comma-separated codes. Empty means all stale codes.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    codes = [item.strip() for item in args.codes.split(",") if item.strip()] or None
    out = run(
        db_path=args.db_path,
        output_root=args.output_root,
        min_latest_ymd=args.min_latest_ymd,
        codes=codes,
        dry_run=args.dry_run,
    )
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
