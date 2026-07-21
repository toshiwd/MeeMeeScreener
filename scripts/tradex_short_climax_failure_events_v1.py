from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


AXIS_ID = "tradex_short_climax_failure_events_v1"
OUT = Path(r"G:\Tradex\short_climax_failure_events_v1")
ROOT = Path(__file__).resolve().parents[1]


def run() -> Path:
    sys.path[:0] = [str(ROOT), str(ROOT / "app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime = get_runtime_stock_db_status()
    db_path = Path(runtime["selected_runtime_db_path"])
    highs = ",".join(f"lead(h,{i}) over w h{i}" for i in range(1, 6))
    lows = ",".join(f"lead(l,{i}) over w l{i}" for i in range(1, 6))
    sql = f"""
    WITH bars AS (
      SELECT code,date,o,h,l,c,v,lag(c,20) over w c20,
        avg(v) over(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) av20,
        lead(c,5) over w c5,{highs},{lows}
      FROM daily_bars WHERE source='pan'
      WINDOW w AS(PARTITION BY code ORDER BY date)
    ), signals AS (
      SELECT *,c/c20-1 ret20,v/av20 volume_ratio,(c-l)/nullif(h-l,0) close_pos,
        row_number() OVER(PARTITION BY date ORDER BY v/av20 DESC,c/c20 DESC,code) family_rank
      FROM bars WHERE c20>0 AND av20>0 AND c5 IS NOT NULL
        AND c/c20-1>=.20 AND v/av20>=3 AND c<o AND (c-l)/nullif(h-l,0)<=.35
    ), selected AS (
      SELECT * FROM signals WHERE family_rank<=3
        AND CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) BETWEEN 20190101 AND 20260710
    ), hit AS (
      SELECT *,least({','.join(f'CASE WHEN h{i}>=c*1.08 THEN {i} ELSE 99 END' for i in range(1,6))}) stop_day,
        least({','.join(f'CASE WHEN l{i}<=c*.90 THEN {i} ELSE 99 END' for i in range(1,6))}) target_day
      FROM selected
    )
    SELECT CAST(code AS VARCHAR) code,
      CAST(strftime(to_timestamp(date),'%Y-%m-%d') AS DATE) signal_date,
      CAST(strftime(to_timestamp(date),'%Y-%m-%d') AS DATE) entry_date,
      CASE WHEN stop_day<=5 AND stop_day<=target_day THEN -.08
           WHEN target_day<=5 THEN .10 ELSE c/c5-1 END ret,
      'climax_failure' AS rule,
      CAST(strftime(to_timestamp(date)+INTERVAL 12 DAY,'%Y-%m-%d') AS DATE) outcome_known_date,
      family_rank,ret20,volume_ratio,close_pos
    FROM hit ORDER BY signal_date,code
    """
    current_sql = """
    WITH bars AS (
      SELECT code,date,o,h,l,c,v,lag(c,20) over w c20,
        avg(v) over(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) av20
      FROM daily_bars WHERE source='pan'
      WINDOW w AS(PARTITION BY code ORDER BY date)
    ), latest AS (SELECT max(date) date FROM bars), candidates AS (
      SELECT code,date,c,c/c20-1 ret20,v/av20 volume_ratio,(c-l)/nullif(h-l,0) close_pos,
        row_number() OVER(ORDER BY v/av20 DESC,c/c20 DESC,code) family_rank
      FROM bars WHERE date=(SELECT date FROM latest) AND c20>0 AND av20>0
        AND c/c20-1>=.20 AND v/av20>=3 AND c<o AND (c-l)/nullif(h-l,0)<=.35
    ) SELECT * FROM candidates WHERE family_rank<=3 ORDER BY family_rank
    """
    with duckdb.connect(str(db_path), read_only=True) as db:
        events = db.execute(sql).fetchdf()
        current = [{"code": str(row["code"]), "signal_date": runtime["latest_confirmed_daily_bars_date_iso"], "confirmed_close": float(row["c"]), "ret20": float(row["ret20"]), "volume_ratio": float(row["volume_ratio"]), "close_pos": float(row["close_pos"]), "family_rank": int(row["family_rank"])} for row in db.execute(current_sql).fetchdf().to_dict("records")]
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    events.to_csv(output / "events.csv", index=False)
    yearly = []
    for year, part in events.groupby(events.signal_date.dt.year):
        gain = float(part.ret[part.ret > 0].sum()); loss = float(-part.ret[part.ret < 0].sum())
        yearly.append({"year": int(year), "n": len(part), "expectancy": float(part.ret.mean()), "profit_factor": gain/loss if loss else None, "win_rate": float((part.ret>0).mean())})
    payload = {
        "schema_version": f"{AXIS_ID}.inventory.v1", "artifact_role": "authoritative",
        "shape": "ret20>=20%; volume>=3x prior20; bearish candle; close in bottom35%",
        "selection": "top3/day by volume ratio then ret20", "execution": {"entry":"signal close","tp":.10,"sl":.08,"horizon":5,"same_bar":"stop_first"},
        "event_count": len(events), "yearly": yearly, "current_candidates": current, "runtime_db": str(db_path),
        "runtime_db_write": False, "production_ranking_changed": False,
    }
    path = output / "inventory.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path); return path


if __name__ == "__main__": run()
