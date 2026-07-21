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

from app.backend import ingest_txt
from app.backend.core.yahoo_history_rows import get_historical_daily_rows_from_chart


AXIS_ID = "repair_yahoo_history_gap_codes_v1"
DEFAULT_DB_PATH = Path(os.environ.get("STOCKS_DB_PATH") or r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\repair_yahoo_history_gap_codes_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _ymd_expr(column: str = "date") -> str:
    return f"CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)"


def _before_state(conn: duckdb.DuckDBPyConnection, code: str) -> dict[str, Any]:
    rows = conn.execute(
        f"""
        SELECT
          COUNT(*) AS rows,
          MIN({_ymd_expr()}) AS min_ymd,
          MAX({_ymd_expr()}) AS max_ymd,
          MAX_BY(c, date) AS latest_close
        FROM daily_bars
        WHERE code = ?
        """,
        [code],
    ).fetchone()
    sources = conn.execute(
        "SELECT COALESCE(source, 'unknown') AS source, COUNT(*) FROM daily_bars WHERE code = ? GROUP BY 1 ORDER BY 1",
        [code],
    ).fetchall()
    meta = conn.execute("SELECT name, latest_close, updated_at FROM stock_meta WHERE code = ?", [code]).fetchone()
    return {
        "code": code,
        "daily_rows": int(rows[0] or 0),
        "min_ymd": int(rows[1]) if rows and rows[1] is not None else None,
        "max_ymd": int(rows[2]) if rows and rows[2] is not None else None,
        "latest_close": float(rows[3]) if rows and rows[3] is not None else None,
        "sources": {str(source): int(count) for source, count in sources},
        "stock_meta": {
            "name": str(meta[0]) if meta else code,
            "latest_close": float(meta[1]) if meta and meta[1] is not None else None,
            "updated_at": str(meta[2]) if meta and meta[2] is not None else None,
        },
    }


def _history_to_daily(code: str, *, range_token: str) -> pd.DataFrame:
    rows = get_historical_daily_rows_from_chart(code, range_token=range_token)
    frame = pd.DataFrame(rows, columns=["date", "o", "h", "l", "c", "v"])
    if frame.empty:
        return pd.DataFrame(columns=["code", "date", "o", "h", "l", "c", "v"])
    frame.insert(0, "code", str(code))
    frame["date"] = frame["date"].astype("int64")
    for col in ["o", "h", "l", "c"]:
        frame[col] = frame[col].astype("float64")
    frame["v"] = frame["v"].fillna(0).round().astype("int64")
    return frame.sort_values(["code", "date"]).drop_duplicates(["code", "date"], keep="last")


def _replace_code(
    conn: duckdb.DuckDBPyConnection,
    *,
    code: str,
    daily: pd.DataFrame,
    name: str,
) -> dict[str, Any]:
    daily_ma = ingest_txt.build_daily_ma(daily)
    monthly = ingest_txt.build_monthly(daily)
    monthly_ma = ingest_txt.build_monthly_ma(monthly)
    feature_snapshot = ingest_txt.build_feature_snapshot_daily(daily, daily_ma)
    meta, meta_summary = ingest_txt.build_stock_meta(daily, monthly, {code: name})

    conn.execute("BEGIN TRANSACTION")
    try:
        for table in ["daily_bars", "daily_ma", "feature_snapshot_daily", "monthly_bars", "monthly_ma", "stock_meta", "tickers"]:
            conn.execute(f"DELETE FROM {table} WHERE code = ?", [code])

        conn.register("repair_daily_df", daily)
        conn.execute("INSERT INTO daily_bars SELECT code, date, o, h, l, c, v, 'yahoo' FROM repair_daily_df")
        conn.register("repair_daily_ma_df", daily_ma)
        conn.execute("INSERT INTO daily_ma SELECT code, date, ma7, ma20, ma60 FROM repair_daily_ma_df")
        conn.register("repair_feature_snapshot_df", feature_snapshot)
        conn.execute(
            """
            INSERT INTO feature_snapshot_daily (
              dt, code, close, ma7, ma20, ma60, atr14, diff20_pct, diff20_atr,
              cnt_20_above, cnt_7_above, day_count, candle_flags
            )
            SELECT
              dt, code, close, ma7, ma20, ma60, atr14, diff20_pct, diff20_atr,
              cnt_20_above, cnt_7_above, day_count, candle_flags
            FROM repair_feature_snapshot_df
            """
        )
        conn.register("repair_monthly_df", monthly)
        conn.execute("INSERT INTO monthly_bars SELECT code, month, o, h, l, c, v FROM repair_monthly_df")
        conn.register("repair_monthly_ma_df", monthly_ma)
        conn.execute("INSERT INTO monthly_ma SELECT code, month, ma7, ma20, ma60 FROM repair_monthly_ma_df")
        conn.register("repair_meta_df", meta)
        conn.execute(
            """
            INSERT INTO stock_meta (
              code, name, stage, score, reason, score_status, missing_reasons_json,
              score_breakdown_json, latest_close, monthly_box_status, box_duration,
              box_upper, box_lower, ma20_monthly_trend, days_since_peak,
              days_since_bottom, signal_flags, updated_at
            )
            SELECT
              code, name, stage, score, reason, score_status, missing_reasons_json,
              score_breakdown_json, latest_close, monthly_box_status, box_duration,
              box_upper, box_lower, ma20_monthly_trend, days_since_peak,
              days_since_bottom, signal_flags, updated_at
            FROM repair_meta_df
            """
        )
        tickers = pd.DataFrame([{"code": code, "name": name}])
        conn.register("repair_tickers_df", tickers)
        conn.execute("INSERT INTO tickers SELECT code, name FROM repair_tickers_df")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "daily_rows": int(len(daily)),
        "daily_ma_rows": int(len(daily_ma)),
        "monthly_rows": int(len(monthly)),
        "monthly_ma_rows": int(len(monthly_ma)),
        "feature_snapshot_rows": int(len(feature_snapshot)),
        "stock_meta_rows": int(len(meta)),
        "meta_summary": meta_summary,
    }


def run(
    *,
    db_path: Path,
    output_root: Path,
    codes: list[str],
    range_token: str,
    min_latest_ymd: int,
    dry_run: bool,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    results: list[dict[str, Any]] = []

    with duckdb.connect(str(db_path)) as conn:
        for code in codes:
            before = _before_state(conn, code)
            name = before.get("stock_meta", {}).get("name") or code
            daily = _history_to_daily(code, range_token=range_token)
            fetched_min = None
            fetched_max = None
            if not daily.empty:
                ymd = pd.to_datetime(daily["date"], unit="s", utc=True).dt.strftime("%Y%m%d").astype(int)
                fetched_min = int(ymd.min())
                fetched_max = int(ymd.max())
            eligible = not daily.empty and fetched_max is not None and fetched_max >= int(min_latest_ymd)
            update_stats: dict[str, Any] = {"updated": False}
            if eligible and not dry_run:
                update_stats = {"updated": True, **_replace_code(conn, code=code, daily=daily, name=str(name))}
            after = _before_state(conn, code)
            results.append(
                {
                    "code": code,
                    "name": name,
                    "range_token": range_token,
                    "before": before,
                    "fetched": {
                        "rows": int(len(daily)),
                        "min_ymd": fetched_min,
                        "max_ymd": fetched_max,
                        "latest_close": float(daily["c"].iloc[-1]) if not daily.empty else None,
                    },
                    "eligible": bool(eligible),
                    "dry_run": bool(dry_run),
                    "update_stats": update_stats,
                    "after": after,
                    "source_policy": "replace_code_history_with_yahoo_adjusted_history",
                }
            )

    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "MeeMee",
        "db_path": str(db_path),
        "range_token": range_token,
        "min_latest_ymd": int(min_latest_ymd),
        "dry_run": bool(dry_run),
        "runtime_db_write": not dry_run,
        "production_ranking_changed": False,
        "research_logic_changed": False,
        "results": results,
        "decision": {
            "candidate_local_decision": "repair_applied"
            if any(row.get("update_stats", {}).get("updated") for row in results)
            else "no_repair_applied",
            "reason": "stale or split-inconsistent code history replaced with Yahoo adjusted history for MeeMee display/input freshness",
        },
    }
    _write_json(output_dir / "repair_yahoo_history_gap_codes_report.json", report)
    _write_json(output_root / "latest_repair_yahoo_history_gap_codes_report.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--codes", required=True, help="Comma-separated codes.")
    parser.add_argument("--range-token", default="10y")
    parser.add_argument("--min-latest-ymd", type=int, default=20260703)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    codes = [item.strip() for item in args.codes.split(",") if item.strip()]
    if not codes:
        raise SystemExit("--codes is required")
    out = run(
        db_path=args.db_path,
        output_root=args.output_root,
        codes=codes,
        range_token=args.range_token,
        min_latest_ymd=args.min_latest_ymd,
        dry_run=args.dry_run,
    )
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
