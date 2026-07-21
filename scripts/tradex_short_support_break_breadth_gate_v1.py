from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_short_support_break_exit_grid_v1 import DB_PATH, clean, metrics, period, simulate


AXIS_ID = "tradex_short_support_break_breadth_gate_v1"
OUT = Path(r"G:\Tradex\short_support_break_breadth_gate_v1")
SPLITS = {"train": (2019, 2021), "validation": (2022, 2023), "test": (2024, 2025)}
GATES = (("all_market", None), ("breadth_below_ma20_ge_40pct", .40), ("breadth_below_ma20_ge_50pct", .50), ("breadth_below_ma20_ge_60pct", .60))

SQL = r"""
WITH normalized AS (
 SELECT code, CASE WHEN date>30000000 THEN CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) ELSE CAST(date AS INTEGER) END ymd,o,h,l,c,v
 FROM daily_bars WHERE o>0 AND h>0 AND l>0 AND c>0
), base AS (
 SELECT *, avg(c) OVER w20 ma20, avg(v) OVER w20 vol20,
   min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) prior_low20,
   lead(ymd,1) OVER w e_ymd, lead(h,1) OVER w e_h,lead(l,1) OVER w e_l,lead(c,1) OVER w e_c,
   lead(h,2) OVER w f1_h,lead(l,2) OVER w f1_l,lead(c,2) OVER w f1_c,lead(h,3) OVER w f2_h,lead(l,3) OVER w f2_l,lead(c,3) OVER w f2_c,
   lead(h,4) OVER w f3_h,lead(l,4) OVER w f3_l,lead(c,4) OVER w f3_c,lead(h,5) OVER w f4_h,lead(l,5) OVER w f4_l,lead(c,5) OVER w f4_c,
   lead(h,6) OVER w f5_h,lead(l,6) OVER w f5_l,lead(c,6) OVER w f5_c,lead(h,7) OVER w f6_h,lead(l,7) OVER w f6_l,lead(c,7) OVER w f6_c,
   lead(h,8) OVER w f7_h,lead(l,8) OVER w f7_l,lead(c,8) OVER w f7_c,lead(h,9) OVER w f8_h,lead(l,9) OVER w f8_l,lead(c,9) OVER w f8_c,
   lead(h,10) OVER w f9_h,lead(l,10) OVER w f9_l,lead(c,10) OVER w f9_c,lead(h,11) OVER w f10_h,lead(l,11) OVER w f10_l,lead(c,11) OVER w f10_c
 FROM normalized WINDOW w AS (PARTITION BY code ORDER BY ymd), w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
), breadth AS (
 SELECT ymd, avg(CASE WHEN c < ma20 THEN 1.0 ELSE 0.0 END) AS breadth_below_ma20
 FROM base WHERE ma20 IS NOT NULL GROUP BY ymd
)
SELECT base.*, breadth.breadth_below_ma20 FROM base JOIN breadth USING(ymd)
WHERE ma20 IS NOT NULL AND vol20>0 AND prior_low20 IS NOT NULL AND e_ymd IS NOT NULL AND f10_c IS NOT NULL
  AND c<prior_low20 AND v/vol20>=3.0 AND (c-l)/NULLIF(h-l,0)<=.10 AND c/ma20-1<=-.10 AND e_l<=l
ORDER BY ymd,code
"""


def main() -> int:
    now=datetime.now(timezone.utc); run=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; run.mkdir(parents=True)
    with duckdb.connect(str(DB_PATH),read_only=True) as db: raw=[{k:clean(v) for k,v in r.items()} for r in db.execute(SQL).fetchdf().to_dict("records")]
    reports=[]
    for name, floor in GATES:
        selected=[r for r in raw if floor is None or float(r["breadth_below_ma20"])>=floor]
        rows=[{**r,**simulate(r,.10,.08,10)} for r in selected]
        reports.append({"gate":name,"breadth_floor":floor,"splits":{k:metrics(period(rows,*v)) for k,v in SPLITS.items()},"yearly":[{"year":y,**metrics(period(rows,y,y))} for y in range(2019,2026)]})
    def gate(r):
        p=r["splits"]
        return all((p[k]["n"] or 0)>=30 and (p[k]["profit_factor"] or 0)>=1.2 and (p[k]["p05_ret"] or -1)>=-.08 for k in ("train","validation"))
    eligible=[r for r in reports if gate(r)]
    best=max(eligible,key=lambda r:(r["splits"]["train"]["profit_factor"],r["splits"]["validation"]["profit_factor"])) if eligible else None
    annual=best["yearly"] if best else []
    annual_pass=bool(annual) and all((r["n"] or 0)>=30 and (r["profit_factor"] or 0)>=1.0 for r in annual)
    payload={"schema_version":f"{AXIS_ID}_v1","generated_at":now.isoformat(),"axis_id":AXIS_ID,"boundary_owner":"TRADEX","research_phase":"branching_generation",
      "fixed_evaluation_conditions":{"base_shape":"support break capitulation","entry":"next-day signal-low stop entry","exit":"short TP10/SL8/max10, stop-before-target", "information_axis":"same-day all-stock breadth below own MA20", "splits":SPLITS,"costs":"not modeled"},
      "reports":reports,"selection":{"pretest_gate":"train and validation: n>=30 PF>=1.2 p05>=-8%","selected_gate":best["gate"] if best else None},"post_selection_evaluation":{"annual_gate":"each 2019-2025 year n>=30 and PF>=1.0","annual_pass":annual_pass,"annual_failures":[r for r in annual if (r["n"] or 0)<30 or (r["profit_factor"] or 0)<1.0]},
      "decision":{"candidate_local_decision":"keep" if best and annual_pass else ("hold" if best else "drop"),"authoritative_rollup_decision":"research_only","reason":"breadth gate passes selection and annual gate" if best and annual_pass else ("selected breadth gate still fails annual gate" if best else "no breadth gate passes pre-test selection")},"runtime_db_write":False,"meemee_unchanged":True,"production_ranking_changed":False,"silent_fallback_used":False}
    (run/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (OUT/"latest_compare.json").write_text(json.dumps({"run_root":str(run),**payload},ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(run/"compare.json")


if __name__ == "__main__": main()
