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

AXIS_ID = "tradex_riskoff_ma20_failed_bounce_short_v1"
OUT = Path(r"G:\Tradex\riskoff_ma20_failed_bounce_short_v1")


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
        lag(c,5) OVER w c5,lag(c,10) OVER w c10lag,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) ma60,
        lead(o) OVER w next_open,lead(date) OVER w next_entry_date,{leads}
      FROM daily_bars WHERE source='pan'
      WINDOW w AS(PARTITION BY code ORDER BY date)
    ), breadth AS (
      SELECT date,avg(CASE WHEN c>ma20 THEN 1.0 ELSE 0.0 END) breadth_above_ma20
      FROM b WHERE ma20 IS NOT NULL GROUP BY date
    ), signals AS (
      SELECT b.*,breadth_above_ma20,
        row_number() OVER(PARTITION BY b.date ORDER BY (h-c)/nullif(h-l,0) DESC,code) day_rank
      FROM b JOIN breadth USING(date)
      WHERE d10 IS NOT NULL AND ma20<ma60 AND c5 IS NOT NULL AND c5>0
        AND c10lag IS NOT NULL AND c5/c10lag - 1 <= -.08
        AND h>=ma20*.99 AND h<=ma20*1.03 AND c<ma20 AND c<o
        AND (h-c)/nullif(h-l,0)>=.50
        AND breadth_above_ma20<=.45 AND next_open>=c
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
            if float(item[f"h{offset}"]) >= entry * 1.05:
                ret, exit_offset = -.05, offset
                break
            if float(item[f"l{offset}"]) <= entry * .90:
                ret, exit_offset = .10, offset
                break
        if ret is None:
            ret = 1 - float(item["c10"]) / entry
        rows.append({
            "side": "sell", "code": str(item["code"]),
            "signal_date": pd.to_datetime(int(item["date"]), unit="s"),
            "entry_date": pd.to_datetime(int(item["next_entry_date"]), unit="s"),
            "ret": ret, "rule": "riskoff_ma20_failed_bounce_short", "regime": "risk_off",
            "exit_offset": exit_offset, "breadth_above_ma20": float(item["breadth_above_ma20"]),
        })
    events = pd.DataFrame(rows)
    reports = {}
    for name, start, end in [("development_2019_2025", "2019-01-01", "2025-12-31"), ("validation_2026", "2026-01-01", "2026-07-10")]:
        part = events[(events.entry_date >= start) & (events.entry_date <= end)]
        yearly = [{"year": int(y), **metrics(part[part.entry_date.dt.year == y])} for y in sorted(part.entry_date.dt.year.unique())]
        reports[name] = {"metrics": metrics(part), "weekly_coverage": weekly_coverage(part, start, end), "yearly": yearly}
    dev, val = reports["development_2019_2025"]["metrics"], reports["validation_2026"]["metrics"]
    keep = bool((dev.get("event_count") or 0)>=100 and (dev.get("daily_profit_factor") or 0)>=1.2 and (dev.get("daily_expectancy") or 0)>0 and (val.get("event_count") or 0)>=5 and (val.get("daily_profit_factor") or 0)>=1.2 and (val.get("daily_expectancy") or 0)>0)
    now = datetime.now(timezone.utc); output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True)
    events.to_csv(output / "events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "shape": "MA20<MA60; prior five-session leg down at least 8%; high retests MA20 -1%/+3%; bearish close below MA20; upper rejection at least 50%",
            "regime": "breadth above MA20 <=45%", "entry": "next open only when open>=signal close",
            "exit": "short TP10 SL5 H10 stop-first", "ranking": "largest upper rejection top3/day", "costs": "ignored"
        },
        "reports": reports,
        "decision": {"candidate_local_decision": "keep" if keep else "drop", "authoritative_rollup_decision": "research_only", "reason_type": "fixed_ma20_failed_bounce_gate_pass" if keep else "fixed_gate_failed"},
        "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading": False,
    }
    path = output / "compare.json"; path.write_text(json.dumps(payload, ensure_ascii=False, indent=2)+"\n", encoding="utf-8"); print(path); return path


if __name__ == "__main__":
    run()
