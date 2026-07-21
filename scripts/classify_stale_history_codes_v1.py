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


AXIS_ID = "classify_stale_history_codes_v1"
DEFAULT_DB_PATH = Path(os.environ.get("STOCKS_DB_PATH") or r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_DEV_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_CODE_LIST_PATH = REPO_ROOT / "tools" / "code.txt"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\classify_stale_history_codes_v1")

DELISTED_CODES: dict[str, dict[str, str]] = {
    "6670": {"delisted_on": "2026-06-16", "reason": "stock_consolidation", "source": "JPX"},
    "9338": {"delisted_on": "2026-06-16", "reason": "stock_consolidation", "source": "JPX"},
    "4974": {"delisted_on": "2026-06-12", "reason": "stock_consolidation", "source": "JPX"},
    "6201": {"delisted_on": "2026-06-01", "reason": "going_private", "source": "company_ir"},
    "4384": {"delisted_on": "2026-05-29", "reason": "mbo_stock_consolidation", "source": "JPX"},
    "5727": {"delisted_on": "2026-05-28", "reason": "share_exchange", "source": "company_ir"},
    "4530": {"delisted_on": "2026-05-11", "reason": "mbo_stock_consolidation", "source": "JPX"},
    "7250": {"delisted_on": "2026-04-13", "reason": "stock_consolidation", "source": "JPX"},
    "7205": {"delisted_on": "2026-03-30", "reason": "management_integration_successor_543A", "source": "company_ir"},
    "8515": {"delisted_on": "2026-03-30", "reason": "share_transfer_successor_547A", "source": "broker_notice"},
    "2389": {"delisted_on": "2026-03-19", "reason": "stock_consolidation", "source": "JPX"},
    "3655": {"delisted_on": "2026-03-17", "reason": "stock_consolidation", "source": "JPX"},
    "9719": {"delisted_on": "2026-03-12", "reason": "stock_consolidation", "source": "JPX"},
    "5017": {"delisted_on": "2026-01-20", "reason": "stock_consolidation", "source": "JPX"},
    "6957": {"delisted_on": "2026-01-13", "reason": "stock_consolidation", "source": "JPX"},
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _ymd_expr(column: str = "date") -> str:
    return f"CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)"


def _load_current(conn: duckdb.DuckDBPyConnection, codes: list[str]) -> list[dict[str, Any]]:
    if not codes:
        return []
    rows = conn.execute(
        f"""
        SELECT
          d.code,
          COALESCE(sm.name, d.code) AS name,
          MAX({_ymd_expr('d.date')}) AS latest_ymd,
          COUNT(*) AS daily_rows,
          sm.stage,
          sm.score,
          sm.score_status,
          sm.reason,
          sm.latest_close
        FROM daily_bars d
        LEFT JOIN stock_meta sm ON sm.code = d.code
        WHERE d.code IN (SELECT code FROM (SELECT UNNEST(?) AS code))
        GROUP BY d.code, sm.name, sm.stage, sm.score, sm.score_status, sm.reason, sm.latest_close
        ORDER BY d.code
        """,
        [codes],
    ).fetchall()
    return [
        {
            "code": str(code),
            "name": str(name),
            "latest_ymd": int(latest_ymd),
            "daily_rows": int(daily_rows),
            "stage": stage,
            "score": float(score) if score is not None else None,
            "score_status": score_status,
            "reason": reason,
            "latest_close": float(latest_close) if latest_close is not None else None,
        }
        for code, name, latest_ymd, daily_rows, stage, score, score_status, reason, latest_close in rows
    ]


def _apply_db_classification(conn: duckdb.DuckDBPyConnection, *, delisted: dict[str, dict[str, str]], tracking: list[str]) -> None:
    delisted_rows = [
        {
            "code": code,
            "reason": f"DELISTED: delisted_on={meta['delisted_on']} source={meta['source']}",
            "missing_reasons_json": json.dumps(["delisted", f"delisted_on:{meta['delisted_on']}", meta["reason"]], ensure_ascii=False),
            "score_breakdown_json": json.dumps({"quarantine": True, "axis_id": AXIS_ID, **meta}, ensure_ascii=False),
        }
        for code, meta in delisted.items()
    ]
    tracking_rows = [
        {
            "code": code,
            "reason": "STALE_TRACKING: not confirmed delisted; keep in PAN update target and recheck next update",
            "missing_reasons_json": json.dumps(["stale_tracking", "not_confirmed_delisted"], ensure_ascii=False),
            "score_breakdown_json": json.dumps({"tracking": True, "axis_id": AXIS_ID}, ensure_ascii=False),
        }
        for code in tracking
    ]

    conn.execute("BEGIN TRANSACTION")
    try:
        if delisted_rows:
            conn.register("delisted_classification_df", pd.DataFrame(delisted_rows))
            conn.execute(
                """
                UPDATE stock_meta AS sm
                SET
                  stage = 'DELISTED',
                  score = NULL,
                  reason = c.reason,
                  score_status = 'DELISTED',
                  missing_reasons_json = c.missing_reasons_json,
                  score_breakdown_json = c.score_breakdown_json,
                  signal_flags = NULL,
                  updated_at = NOW()
                FROM delisted_classification_df c
                WHERE sm.code = c.code
                """
            )
        if tracking_rows:
            conn.register("tracking_classification_df", pd.DataFrame(tracking_rows))
            conn.execute(
                """
                UPDATE stock_meta AS sm
                SET
                  stage = 'STALE',
                  score = NULL,
                  reason = c.reason,
                  score_status = 'STALE_TRACKING',
                  missing_reasons_json = c.missing_reasons_json,
                  score_breakdown_json = c.score_breakdown_json,
                  signal_flags = NULL,
                  updated_at = NOW()
                FROM tracking_classification_df c
                WHERE sm.code = c.code
                """
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _update_code_list(path: Path, *, remove_codes: set[str], keep_codes: set[str], dry_run: bool) -> dict[str, Any]:
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    before = list(lines)
    after = [code for code in lines if code not in remove_codes]
    missing_keep = sorted(keep_codes - set(after))
    after.extend(missing_keep)

    def sort_key(value: str) -> tuple[int, int | str]:
        return (0, int(value)) if value.isdigit() else (1, value)

    after = sorted(dict.fromkeys(after), key=sort_key)
    if not dry_run and after != before:
        path.write_text("\n".join(after) + "\n", encoding="utf-8")
    return {
        "path": str(path),
        "before_count": len(before),
        "after_count": len(after),
        "removed_codes": sorted(set(before) - set(after), key=sort_key),
        "added_tracking_codes": missing_keep,
        "changed": before != after,
    }


def run(
    *,
    db_paths: list[Path],
    code_list_path: Path,
    output_root: Path,
    tracking_codes: list[str],
    dry_run: bool,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    delisted = dict(DELISTED_CODES)
    tracking = [code for code in tracking_codes if code not in delisted]
    db_results: list[dict[str, Any]] = []
    for db_path in db_paths:
        with duckdb.connect(str(db_path)) as conn:
            before = _load_current(conn, sorted([*delisted.keys(), *tracking]))
            if not dry_run:
                _apply_db_classification(conn, delisted=delisted, tracking=tracking)
            after = _load_current(conn, sorted([*delisted.keys(), *tracking]))
            db_results.append({"db_path": str(db_path), "before": before, "after": after})
    code_list_result = _update_code_list(
        code_list_path,
        remove_codes=set(delisted.keys()),
        keep_codes=set(tracking),
        dry_run=dry_run,
    )
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "MeeMee",
        "dry_run": dry_run,
        "runtime_db_write": not dry_run,
        "production_ranking_formula_changed": False,
        "delisted_count": len(delisted),
        "tracking_count": len(tracking),
        "delisted_codes": delisted,
        "tracking_codes": tracking,
        "db_results": db_results,
        "code_list_result": code_list_result,
        "decision": {
            "candidate_local_decision": "classified",
            "reason": "confirmed delisted codes are quarantined as DELISTED; non-delisted stale codes remain tracked",
        },
    }
    _write_json(output_dir / "classify_stale_history_codes_report.json", report)
    _write_json(output_root / "latest_classify_stale_history_codes_report.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--dev-db-path", type=Path, default=DEFAULT_DEV_DB_PATH)
    parser.add_argument("--code-list-path", type=Path, default=DEFAULT_CODE_LIST_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--tracking-codes", default="", help="Comma-separated stale codes that are not confirmed delisted.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    tracking_codes = [item.strip() for item in args.tracking_codes.split(",") if item.strip()]
    out = run(
        db_paths=[args.db_path, args.dev_db_path],
        code_list_path=args.code_list_path,
        output_root=args.output_root,
        tracking_codes=tracking_codes,
        dry_run=args.dry_run,
    )
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
