from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


AXIS_ID = "tradex_ma20_reclaim_family_events_v1"
OUT = Path(r"G:\Tradex\ma20_reclaim_family_events_v1")
ROOT = Path(__file__).resolve().parents[1]


def run() -> Path:
    sys.path[:0] = [str(ROOT), str(ROOT / "app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    db_path = Path(runtime["selected_runtime_db_path"])
    future = ",".join(
        f"lead(h,{day}) over w h{day},lead(l,{day}) over w l{day}"
        for day in range(1, 11)
    )
    sql = f"""
    WITH bars AS (
      SELECT code,date,o,h,l,c,v,
        lag(h) over w prior_h1,
        avg(c) over(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20,
        avg(c) over(PARTITION BY code ORDER BY date ROWS BETWEEN 24 PRECEDING AND 5 PRECEDING) ma20_5,
        avg(c) over(PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) ma60,
        avg(v) over(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) av20,
        lead(date,1) over w entry_date,lead(o,1) over w next_open,
        lead(date,10) over w d10,lead(c,10) over w close10,
        {future}
      FROM daily_bars WHERE source='pan'
      WINDOW w AS(PARTITION BY code ORDER BY date)
    ), signals AS (
      SELECT *, (c-l)/nullif(h-l,0) close_position,
        row_number() OVER(PARTITION BY date ORDER BY (c-l)/nullif(h-l,0) DESC,v/av20 DESC,code) family_rank
      FROM bars
      WHERE ma20>ma60 AND ma20>ma20_5 AND l BETWEEN ma20*.97 AND ma20
        AND c>ma20 AND c>prior_h1 AND c>o
        AND (c-l)/nullif(h-l,0)>=.70
        AND next_open IS NOT NULL AND close10 IS NOT NULL
    ), selected AS (
      SELECT * FROM signals WHERE family_rank<=3
        AND CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) BETWEEN 20190101 AND 20260710
    ), evaluated AS (
      SELECT *, least({','.join(f'CASE WHEN l{d}<=next_open*.95 THEN {d} ELSE 99 END' for d in range(1,11))}) stop_day,
        least({','.join(f'CASE WHEN h{d}>=next_open*1.08 THEN {d} ELSE 99 END' for d in range(1,11))}) target_day
      FROM selected
    )
    SELECT 'buy' side,CAST(code AS VARCHAR) code,
      CAST(strftime(to_timestamp(date),'%Y-%m-%d') AS DATE) signal_date,
      CAST(strftime(to_timestamp(entry_date),'%Y-%m-%d') AS DATE) entry_date,
      CASE WHEN stop_day<=10 AND stop_day<=target_day THEN -.05
           WHEN target_day<=10 THEN .08 ELSE close10/next_open-1 END ret,
      'ma20_support_reclaim' AS "rule",
      CASE WHEN stop_day<=10 AND stop_day<=target_day THEN stop_day
           WHEN target_day<=10 THEN target_day ELSE 10 END exit_offset,
      family_rank,close_position,next_open
    FROM evaluated ORDER BY entry_date,code
    """
    with duckdb.connect(str(db_path), read_only=True) as db:
        events = db.execute(sql).fetchdf()

    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    events.to_csv(output / "events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.inventory.v1",
        "artifact_role": "authoritative",
        "runtime_db": str(db_path),
        "event_count": len(events),
        "signal_day_count": int(events.signal_date.nunique()),
        "latest_signal_date": str(events.signal_date.max()),
        "shape": "rising MA20 above MA60; <=3% MA20 undercut; bullish prior-high reclaim; close position >=70%",
        "selection": "top3 each signal date by close position then volume ratio",
        "execution": {"entry": "next open", "take_profit": .08, "stop_loss": .05, "maximum_sessions": 10, "same_bar": "stop_first"},
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "automatic_trading": False,
    }
    path = output / "inventory.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
