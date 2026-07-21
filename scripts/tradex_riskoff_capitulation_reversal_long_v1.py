from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_long_short_weekly_coverage_v1 import metrics, weekly_coverage

AXIS_ID = "tradex_riskoff_capitulation_reversal_long_v1"
OUT = Path(r"G:\Tradex\riskoff_capitulation_reversal_long_v1")


def run() -> Path:
    sys.path.insert(0, "app")
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    db_path = Path(runtime["selected_runtime_db_path"])
    leads = ",".join(
        f"lead(h,{i}) over w h{i},lead(l,{i}) over w l{i},lead(c,{i}) over w c{i},lead(date,{i}) over w d{i}"
        for i in range(1, 11)
    )
    sql = f"""
    WITH b AS (
      SELECT code,date,o,h,l,c,v,
        lag(c) OVER w prev_c,lag(c,5) OVER w c5,
        avg(v) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) vol20,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20,
        lead(o) OVER w next_open,lead(date) OVER w next_entry_date,{leads}
      FROM daily_bars WHERE source='pan'
      WINDOW w AS(PARTITION BY code ORDER BY date)
    ), breadth AS (
      SELECT date,avg(CASE WHEN c>ma20 THEN 1.0 ELSE 0.0 END) breadth_above_ma20
      FROM b WHERE ma20 IS NOT NULL GROUP BY date
    ), signals AS (
      SELECT b.*,breadth_above_ma20,
        row_number() OVER(PARTITION BY b.date ORDER BY v/vol20 DESC,(c-l)/nullif(h-l,0) DESC,code) day_rank
      FROM b JOIN breadth USING(date)
      WHERE d10 IS NOT NULL AND c5 IS NOT NULL AND c5>0 AND vol20>0
        AND c/c5-1<=-.08 AND v/vol20>=2.0
        AND c>o AND (c-l)/nullif(h-l,0)>=.60 AND c>=prev_c*.99
        AND breadth_above_ma20<=.45 AND next_open<=c*1.03
    )
    SELECT * FROM signals WHERE day_rank<=3 ORDER BY date,day_rank,code
    """
    with duckdb.connect(str(db_path), read_only=True) as db:
        raw = db.execute(sql).fetchdf()
    rows = []
    for item in raw.to_dict("records"):
        entry = float(item["next_open"])
        ret, exit_offset = None, 10
        for offset in range(1, 11):
            if float(item[f"l{offset}"]) <= entry * .95:
                ret, exit_offset = -.05, offset
                break
            if float(item[f"h{offset}"]) >= entry * 1.10:
                ret, exit_offset = .10, offset
                break
        if ret is None:
            ret = float(item["c10"]) / entry - 1
        rows.append({
            "side": "buy", "code": str(item["code"]),
            "signal_date": pd.to_datetime(int(item["date"]), unit="s"),
            "entry_date": pd.to_datetime(int(item["next_entry_date"]), unit="s"),
            "ret": ret, "rule": "riskoff_capitulation_reversal_long", "regime": "risk_off",
            "exit_offset": exit_offset, "breadth_above_ma20": float(item["breadth_above_ma20"]),
            "volume_ratio20": float(item["v"] / item["vol20"]),
        })
    events = pd.DataFrame(rows)
    reports = {}
    for name, start, end in [("development_2019_2025", "2019-01-01", "2025-12-31"), ("validation_2026", "2026-01-01", "2026-07-10")]:
        part = events[(events.entry_date >= start) & (events.entry_date <= end)]
        yearly = [{"year": int(y), **metrics(part[part.entry_date.dt.year == y])} for y in sorted(part.entry_date.dt.year.unique())]
        reports[name] = {"metrics": metrics(part), "weekly_coverage": weekly_coverage(part, start, end), "yearly": yearly}
    dev, val = reports["development_2019_2025"]["metrics"], reports["validation_2026"]["metrics"]
    keep = bool((dev.get("event_count") or 0)>=100 and (dev.get("daily_profit_factor") or 0)>=1.2 and (dev.get("daily_expectancy") or 0)>0 and (val.get("event_count") or 0)>=5 and (val.get("daily_profit_factor") or 0)>=1.2 and (val.get("daily_expectancy") or 0)>0)
    now = datetime.now(timezone.utc); output = OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True)
    events.to_csv(output/"events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "shape": "five-session decline at least 8%; volume ratio20 at least 2; bullish candle; close position at least 60%; close recovers to within 1% of prior close",
            "regime": "breadth above MA20 <=45%", "entry": "next open when gap up is no more than 3% versus signal close",
            "exit": "long TP10 SL5 H10 stop-first", "ranking": "volume ratio then close position top3/day", "costs": "ignored"
        },
        "reports": reports,
        "decision": {"candidate_local_decision": "keep" if keep else "drop", "authoritative_rollup_decision": "research_only", "reason_type": "fixed_capitulation_reversal_gate_pass" if keep else "fixed_gate_failed"},
        "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading": False,
    }
    path=output/"compare.json"; path.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(path); return path


if __name__ == "__main__":
    run()
