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


AXIS_ID = "repair_stale_pan_txt_codes_v1"
DEFAULT_DB_PATH = Path(os.environ.get("STOCKS_DB_PATH") or r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_TXT_DIR = Path(os.environ.get("PAN_OUT_TXT_DIR") or r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\txt")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\repair_stale_pan_txt_codes_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _date_expr(column: str = "date") -> str:
    return f"CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)"


def _resolve_txt_file(txt_dir: Path, code: str) -> Path | None:
    matches = sorted(txt_dir.glob(f"{code}_*.txt"))
    if matches:
        return matches[0]
    fallback = txt_dir / f"{code}.txt"
    return fallback if fallback.exists() else None


def _stale_codes(db_path: Path, *, min_ymd: int, explicit_codes: list[str] | None) -> list[dict[str, Any]]:
    code_filter = ""
    params: list[Any] = []
    if explicit_codes:
        placeholders = ",".join("?" for _ in explicit_codes)
        code_filter = f" AND code IN ({placeholders})"
        params.extend(explicit_codes)
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            f"""
SELECT
  code,
  MAX({_date_expr()}) AS max_ymd,
  COUNT(*) AS rows
FROM daily_bars
WHERE COALESCE(source, 'pan') = 'pan'
{code_filter}
GROUP BY code
HAVING max_ymd < ?
ORDER BY max_ymd DESC, code
""",
            [*params, int(min_ymd)],
        ).fetchall()
    return [{"code": str(code), "db_max_ymd": int(max_ymd), "db_rows": int(rows_count)} for code, max_ymd, rows_count in rows]


def _load_daily_for_codes(txt_dir: Path, stale: list[dict[str, Any]]) -> tuple[pd.DataFrame, dict[str, str], list[dict[str, Any]]]:
    counts = ingest_txt._empty_counts_delta()
    files: list[tuple[str, int]] = []
    inputs: list[dict[str, Any]] = []
    for row in stale:
        code = row["code"]
        path = _resolve_txt_file(txt_dir, code)
        inputs.append({**row, "txt_path": str(path) if path else None, "txt_exists": bool(path)})
        if path:
            files.append((str(path), 0))
    daily, name_map = ingest_txt.read_daily_files(files, None, counts)
    daily_codes = set(daily["code"].astype(str).unique().tolist()) if not daily.empty else set()
    for row in inputs:
        row["parsed"] = row["code"] in daily_codes
        if row["parsed"]:
            group = daily[daily["code"].astype(str) == row["code"]]
            row["txt_max_ymd"] = int(pd.to_datetime(group["date"], unit="s", utc=True).dt.strftime("%Y%m%d").max())
            row["txt_rows"] = int(len(group))
    return daily, name_map, inputs


def _replace_codes(db_path: Path, daily: pd.DataFrame, name_map: dict[str, str]) -> dict[str, Any]:
    if daily.empty:
        return {"updated_codes": 0, "daily_rows": 0}
    codes = [str(code) for code in daily["code"].dropna().astype(str).unique().tolist()]
    daily_ma = ingest_txt.build_daily_ma(daily)
    monthly = ingest_txt.build_monthly(daily)
    monthly_ma = ingest_txt.build_monthly_ma(monthly)
    feature_snapshot = ingest_txt.build_feature_snapshot_daily(daily, daily_ma)
    meta, meta_summary = ingest_txt.build_stock_meta(daily, monthly, name_map)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            codes_df = pd.DataFrame({"code": codes})
            conn.register("repair_codes_df", codes_df)
            conn.execute("CREATE TEMP TABLE _tmp_repair_codes AS SELECT DISTINCT code FROM repair_codes_df")
            for table in ["daily_bars", "daily_ma", "feature_snapshot_daily", "monthly_bars", "monthly_ma", "stock_meta", "tickers"]:
                conn.execute(f"DELETE FROM {table} WHERE code IN (SELECT code FROM _tmp_repair_codes)")

            conn.register("daily_df", daily)
            conn.execute("INSERT INTO daily_bars SELECT code, date, o, h, l, c, v, 'pan' FROM daily_df")
            conn.register("daily_ma_df", daily_ma)
            conn.execute("INSERT INTO daily_ma SELECT code, date, ma7, ma20, ma60 FROM daily_ma_df")
            conn.register("feature_snapshot_df", feature_snapshot)
            conn.execute(
                """
INSERT INTO feature_snapshot_daily (
  dt, code, close, ma7, ma20, ma60, atr14, diff20_pct, diff20_atr,
  cnt_20_above, cnt_7_above, day_count, candle_flags
)
SELECT
  dt, code, close, ma7, ma20, ma60, atr14, diff20_pct, diff20_atr,
  cnt_20_above, cnt_7_above, day_count, candle_flags
FROM feature_snapshot_df
"""
            )
            conn.register("monthly_df", monthly)
            conn.execute("INSERT INTO monthly_bars SELECT code, month, o, h, l, c, v FROM monthly_df")
            conn.register("monthly_ma_df", monthly_ma)
            conn.execute("INSERT INTO monthly_ma SELECT code, month, ma7, ma20, ma60 FROM monthly_ma_df")
            conn.register("meta_df", meta)
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
FROM meta_df
"""
            )
            tickers_df = pd.DataFrame([{"code": code, "name": name_map.get(code, code)} for code in codes])
            conn.register("tickers_df", tickers_df)
            conn.execute("INSERT INTO tickers SELECT code, name FROM tickers_df")
            conn.execute("DROP TABLE IF EXISTS _tmp_repair_codes")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return {
        "updated_codes": len(codes),
        "daily_rows": int(len(daily)),
        "daily_ma_rows": int(len(daily_ma)),
        "monthly_rows": int(len(monthly)),
        "monthly_ma_rows": int(len(monthly_ma)),
        "feature_snapshot_rows": int(len(feature_snapshot)),
        "stock_meta_rows": int(len(meta)),
        "meta_summary": meta_summary,
    }


def run(*, db_path: Path, txt_dir: Path, output_root: Path, min_ymd: int, codes: list[str] | None, dry_run: bool) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    stale = _stale_codes(db_path, min_ymd=min_ymd, explicit_codes=codes)
    daily, name_map, inputs = _load_daily_for_codes(txt_dir, stale)
    eligible = [row for row in inputs if row.get("parsed") and int(row.get("txt_max_ymd") or 0) >= min_ymd]
    eligible_codes = {row["code"] for row in eligible}
    eligible_daily = daily[daily["code"].astype(str).isin(eligible_codes)].copy() if not daily.empty else daily
    update_stats = {"updated_codes": 0, "daily_rows": 0}
    if not dry_run and not eligible_daily.empty:
        update_stats = _replace_codes(db_path, eligible_daily, name_map)

    after = _stale_codes(db_path, min_ymd=min_ymd, explicit_codes=sorted(eligible_codes)) if not dry_run else []
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "MeeMee",
        "db_path": str(db_path),
        "txt_dir": str(txt_dir),
        "min_ymd": int(min_ymd),
        "dry_run": bool(dry_run),
        "stale_before_count": len(stale),
        "eligible_repair_count": len(eligible),
        "inputs": inputs,
        "update_stats": update_stats,
        "stale_after_eligible_count": len(after),
        "stale_after_eligible": after,
        "runtime_db_write": not dry_run and not eligible_daily.empty,
        "production_ranking_changed": False,
        "decision": {
            "candidate_local_decision": "repair_applied" if update_stats.get("updated_codes") else "no_repair_applied",
            "reason": "stale pan TXT code histories repaired from local TXT files",
        },
    }
    _write_json(output_dir / "repair_stale_pan_txt_codes_report.json", report)
    _write_json(output_root / "latest_repair_stale_pan_txt_codes_report.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--txt-dir", type=Path, default=DEFAULT_TXT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--min-ymd", type=int, default=20260701)
    parser.add_argument("--codes", default="", help="Comma-separated codes. Empty means all stale codes.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    codes = [item.strip() for item in args.codes.split(",") if item.strip()] or None
    out = run(db_path=args.db_path, txt_dir=args.txt_dir, output_root=args.output_root, min_ymd=args.min_ymd, codes=codes, dry_run=args.dry_run)
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
