from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


AXIS_ID = "tradex_adaptive_current_family_scan_v1"
OUT = Path(r"G:\Tradex\adaptive_current_family_scan_v1")


def run() -> Path:
    sys.path[:0] = [str(Path.cwd()), "app"]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    db_path = Path(runtime["selected_runtime_db_path"])
    sql = """
    WITH b AS (
      SELECT code,date,o,h,l,c,v,row_number() OVER w rn,lag(c) OVER w prev_c,
        lag(h) OVER w prior_h1,
        max(h) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) hi200,
        min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) lo200,
        max(h) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) hi60,
        min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) lo60,
        max(h) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) hi20,
        max(h) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) prior_hi10,
        max(h) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) hi10,
        min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) lo10,
        avg(v) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) av20,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) ma7,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) ma60
      FROM daily_bars WHERE source='pan'
      WINDOW w AS(PARTITION BY code ORDER BY date)
    ), f AS (
      SELECT *,lag(ma60,20) OVER(PARTITION BY code ORDER BY date) ma60_20,
        lag(ma20,5) OVER(PARTITION BY code ORDER BY date) ma20_5
      FROM b
    ), latest AS (SELECT max(date) date FROM f), current AS (SELECT * FROM f WHERE date=(SELECT date FROM latest)),
    base_candidates AS (
      SELECT 'base_breakout' rule_name,code,date,c confirmed_close,hi60/lo60-1 shape_value,
        row_number() OVER(ORDER BY hi60/lo60-1,code) family_rank
      FROM current WHERE hi200>lo200 AND hi60>lo60 AND (c-lo200)/(hi200-lo200)<=.5
        AND ma60/ma60_20-1>=-.01 AND c>=hi20 AND v/av20>=2 AND c>o
        AND (c-l)/nullif(h-l,0)>=.75 AND hi60/lo60-1<=.20
    ), contraction_candidates AS (
      SELECT 'volatility_contraction_breakout' rule_name,code,date,c confirmed_close,hi10/lo10-1 shape_value,
        row_number() OVER(ORDER BY hi10/lo10-1,code) family_rank
      FROM current WHERE c>=prior_hi10 AND ma20>ma60 AND v/av20>=1.5
        AND (c-l)/nullif(h-l,0)>=.8 AND hi10/lo10-1<=.06
    ), clean_breakout_candidates AS (
      SELECT 'clean_breakout' rule_name,code,date,c confirmed_close,v/av20 shape_value,
        row_number() OVER(ORDER BY v/av20 DESC,code) family_rank
      FROM current WHERE c>=prior_hi10 AND ma20>ma60 AND v/av20>=1.5
        AND (c-l)/nullif(h-l,0)>=.8
    ), gu_pairs AS (
      SELECT 'gu_first_pullback' rule_name,s.code,s.date,s.c confirmed_close,g.o/g.prev_c-1 shape_value,
        row_number() OVER(PARTITION BY s.code,s.date ORDER BY g.rn DESC) anchor_rank
      FROM current s JOIN f g ON s.code=g.code AND s.rn-g.rn BETWEEN 2 AND 7
      WHERE g.prev_c>0 AND g.av20>0 AND g.o/g.prev_c-1>=.03 AND g.v/g.av20>=2
        AND s.l>g.prev_c AND s.l<=s.ma7*1.02 AND s.c>s.o
        AND (least(s.o,s.c)-s.l)/nullif(s.h-s.l,0)>=.2 AND (s.c-s.l)/nullif(s.h-s.l,0)>=.65
    ), gu_candidates AS (
      SELECT rule_name,code,date,confirmed_close,shape_value,
        row_number() OVER(ORDER BY shape_value DESC,code) family_rank FROM gu_pairs WHERE anchor_rank=1
    ), ma20_reclaim_candidates AS (
      SELECT 'ma20_support_reclaim' rule_name,code,date,c confirmed_close,
        (c-l)/nullif(h-l,0) shape_value,
        row_number() OVER(ORDER BY (c-l)/nullif(h-l,0) DESC,v/av20 DESC,code) family_rank
      FROM current
      WHERE ma20>ma60 AND ma20>ma20_5 AND l<=ma20 AND l>=ma20*.97
        AND c>ma20 AND c>prior_h1 AND c>o
        AND (c-l)/nullif(h-l,0)>=.70
    )
    SELECT * FROM base_candidates WHERE family_rank<=3
    UNION ALL SELECT * FROM clean_breakout_candidates WHERE family_rank<=3
    UNION ALL SELECT * FROM contraction_candidates WHERE family_rank<=3
    UNION ALL SELECT * FROM gu_candidates WHERE family_rank<=3
    UNION ALL SELECT * FROM ma20_reclaim_candidates WHERE family_rank<=3
    ORDER BY rule_name,family_rank,code
    """
    with duckdb.connect(str(db_path), read_only=True) as db:
        rows = db.execute(sql).fetchdf().to_dict("records")
    candidates = [{
        "side": "buy", "code": str(row["code"]), "rule": row["rule_name"],
        "signal_date": runtime["latest_confirmed_daily_bars_date_iso"], "confirmed_close": float(row["confirmed_close"]),
        "shape_value": float(row["shape_value"]), "family_rank": int(row["family_rank"]),
        "entry_condition": "review_next_session_open", "automatic_trade": False,
    } for row in rows]
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    payload = {
        "schema_version": f"{AXIS_ID}.board.v1", "artifact_role": "authoritative",
        "confirmed_signal_date": runtime["latest_confirmed_daily_bars_date_iso"], "candidate_count": len(candidates),
        "counts_by_rule": {rule: sum(row["rule"] == rule for row in candidates) for rule in sorted({row["rule"] for row in candidates})},
        "candidates": candidates, "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading": False,
    }
    path = output / "current_family_scan.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
