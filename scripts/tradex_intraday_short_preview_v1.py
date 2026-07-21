from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


AXIS_ID = "tradex_intraday_short_preview_v1"
DEFAULT_OUTPUT = Path(r"G:\Tradex\intraday_short_preview_v1\latest_intraday_short_preview.json")


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        return _clean(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def build_intraday_short_preview(db_path: str | Path | None = None, *, limit: int = 30) -> dict[str, Any]:
    resolved = Path(db_path) if db_path else resolve_runtime_stock_db_path()
    sql = r"""
    WITH normalized AS (
      SELECT code, source,
        CASE WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
             ELSE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) END AS ymd,
        o,h,l,c,v
      FROM daily_bars WHERE o>0 AND h>0 AND l>0 AND c>0
    ), dates AS (
      SELECT max(ymd) FILTER (WHERE source='pan') confirmed_ymd,
             max(ymd) FILTER (WHERE source='yahoo') provisional_ymd
      FROM normalized
    ), confirmed AS (
      SELECT n.*,
        avg(c) OVER w19 AS prior_ma19,
        avg(v) OVER w20 AS prior_vol20,
        min(l) OVER w20 AS prior_low20
      FROM normalized n, dates d
      WHERE n.source='pan' AND n.ymd <= d.confirmed_ymd
      WINDOW w19 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 18 PRECEDING AND CURRENT ROW),
             w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    ), latest_confirmed AS (
      SELECT * EXCLUDE(rn) FROM (
        SELECT *, row_number() OVER(PARTITION BY code ORDER BY ymd DESC) rn FROM confirmed
      ) WHERE rn=1
    ), provisional AS (
      SELECT * EXCLUDE(rn) FROM (
        SELECT n.*, row_number() OVER(PARTITION BY code ORDER BY ymd DESC) rn
        FROM normalized n, dates d WHERE n.source='yahoo' AND n.ymd=d.provisional_ymd
      ) WHERE rn=1
    ), rows AS (
      SELECT p.code,p.ymd,p.o,p.h,p.l,p.c,p.v,
        lc.ymd confirmed_ymd, lc.prior_low20,lc.prior_vol20,
        (lc.prior_ma19*19+p.c)/20 AS provisional_ma20,
        CASE WHEN p.h>p.l THEN (p.c-p.l)/(p.h-p.l) END AS close_pos,
        CASE WHEN lc.prior_vol20>0 THEN p.v/lc.prior_vol20 END AS volume_vs20,
        CASE WHEN lc.prior_low20>0 THEN p.c/lc.prior_low20-1 END AS break_vs_low20,
        CASE WHEN (lc.prior_ma19*19+p.c)>0 THEN p.c/((lc.prior_ma19*19+p.c)/20)-1 END AS dist_ma20,
        im.name
      FROM provisional p JOIN latest_confirmed lc USING(code)
      LEFT JOIN industry_master im USING(code)
    ), breadth AS (
      SELECT avg(CASE WHEN c<provisional_ma20 THEN 1.0 ELSE 0.0 END) breadth_below_ma20,
             count(*) coverage FROM rows
    )
    SELECT rows.*, breadth.* FROM rows CROSS JOIN breadth
    ORDER BY code
    """
    # MeeMee keeps the runtime DB open in normal mode. DuckDB rejects opening
    # the same file with a different read_only configuration in one process.
    # Every statement below remains SELECT-only.
    with duckdb.connect(str(resolved)) as conn:
        date_row = conn.execute(
            """
            SELECT
              max(CASE WHEN source='pan' THEN CASE WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER) ELSE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) END END),
              max(CASE WHEN source='yahoo' THEN CASE WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER) ELSE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) END END)
            FROM daily_bars
            """
        ).fetchone()
        raw = [{key: _clean(value) for key, value in row.items()} for row in conn.execute(sql).fetchdf().to_dict("records")]
    confirmed_ymd = int(date_row[0]) if date_row and date_row[0] is not None else None
    provisional_ymd = int(date_row[1]) if date_row and date_row[1] is not None else None
    intraday_available = bool(provisional_ymd and confirmed_ymd and provisional_ymd > confirmed_ymd)
    breadth = float(raw[0]["breadth_below_ma20"]) if raw and raw[0].get("breadth_below_ma20") is not None else None
    market_gate = bool(intraday_available and breadth is not None and breadth >= 0.40)
    candidates: list[dict[str, Any]] = []
    near_matches: list[dict[str, Any]] = []
    for row in raw:
        gates = {
            "support_break": bool((row.get("break_vs_low20") or 0) < 0),
            "volume_3x": bool((row.get("volume_vs20") or 0) >= 3.0),
            "bottom_10pct_close": bool(row.get("close_pos") is not None and float(row["close_pos"]) <= 0.10),
            "ma20_minus_10pct": bool((row.get("dist_ma20") or 0) <= -0.10),
            "market_breadth_40pct": market_gate,
        }
        payload = {
            "code": str(row["code"]), "name": row.get("name"), "provisional_ymd": row.get("ymd"),
            "price": row.get("c"), "signal_low": row.get("l"), "volume_vs20": row.get("volume_vs20"),
            "close_pos": row.get("close_pos"), "dist_ma20": row.get("dist_ma20"),
            "break_vs_low20": row.get("break_vs_low20"), "passed_gate_count": sum(gates.values()), "gates": gates,
            "state": "引け前候補" if all(gates.values()) else "条件接近",
            "next_action": "引け確定を待つ。確定後も条件維持なら翌日シグナル安値割れで売り" if all(gates.values()) else "見送り",
        }
        if all(gates.values()): candidates.append(payload)
        elif sum(gates.values()) >= 3: near_matches.append(payload)
    candidates.sort(
        key=lambda row: (
            -(row["volume_vs20"] or 0),
            row["break_vs_low20"] if row["break_vs_low20"] is not None else 0,
            row["close_pos"] if row["close_pos"] is not None else 1,
            row["code"],
        )
    )
    for rank, row in enumerate(candidates, start=1):
        row["intraday_rank"] = rank
    near_matches.sort(key=lambda row: (row["passed_gate_count"], row["volume_vs20"] or 0), reverse=True)
    return {
        "schema_version": f"{AXIS_ID}_v1", "generated_at": datetime.now(timezone.utc).isoformat(),
        "boundary_owner": "TRADEX", "status": "provisional_intraday_preview" if intraday_available else "no_newer_provisional_bar",
        "db_path": str(resolved), "confirmed_ymd": confirmed_ymd, "provisional_ymd": provisional_ymd,
        "intraday_available": intraday_available, "coverage": int(raw[0]["coverage"]) if raw else 0,
        "market_breadth_below_ma20": breadth, "market_gate_pass": market_gate,
        "candidate_count": len(candidates), "candidates": candidates[:limit], "near_matches": near_matches[:limit],
        "ranking_contract": {
            "scope": "provisional_candidates_only",
            "order": ["volume_vs20_desc", "break_vs_low20_asc", "close_pos_asc", "code_asc"],
            "validated_expected_return_rank": False,
            "production_ranking_unchanged": True,
        },
        "contract": {"selection_time":"intraday/pre-close","not_entry_signal":True,"confirmation":"official close required","entry":"next-session signal-low break only","tp":0.10,"sl":0.05,"max_hold_days":10},
        "runtime_db_write": False, "production_ranking_changed": False, "silent_fallback_used": False,
    }


def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--db",type=Path); parser.add_argument("--output",type=Path,default=DEFAULT_OUTPUT); parser.add_argument("--limit",type=int,default=30); args=parser.parse_args()
    payload=build_intraday_short_preview(args.db,limit=args.limit); args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(args.output); return 0


if __name__=="__main__": raise SystemExit(main())
