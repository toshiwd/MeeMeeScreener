from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))
from scripts.tradex_short_support_break_breadth_gate_v1 import SQL
from scripts.tradex_short_support_break_exit_grid_v1 import DB_PATH, clean, simulate


AXIS_ID="tradex_short_breadth_daily_basket_replay_v1"
OUT=Path(r"G:\Tradex\short_breadth_daily_basket_replay_v1")
SPLITS={"train":(2019,2021),"validation":(2022,2023),"test":(2024,2025)}


def pf(values: pd.Series) -> float|None:
    positive=float(values[values>0].sum()); negative=float(-values[values<0].sum())
    return positive/negative if negative else None


def metric(df: pd.DataFrame) -> dict:
    daily=df.groupby("ymd",as_index=False)["ret"].mean()
    return {"sample_count":int(len(df)),"signal_days":int(df.ymd.nunique()),"expectancy":float(df.ret.mean()) if len(df) else None,"profit_factor":pf(df.ret),"daily_profit_factor":pf(daily.ret),"daily_expectancy":float(daily.ret.mean()) if len(daily) else None}


def main() -> None:
    now=datetime.now(timezone.utc); run=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; run.mkdir(parents=True)
    with duckdb.connect(str(DB_PATH),read_only=True) as db: raw=[{k:clean(v) for k,v in r.items()} for r in db.execute(SQL).fetchdf().to_dict("records")]
    rows=[{**r,**simulate(r,.10,.08,10)} for r in raw if float(r["breadth_below_ma20"])>=.40]
    df=pd.DataFrame(rows); df["year"]=df.ymd.astype(str).str[:4].astype(int)
    splits={k:metric(df[df.year.between(*span)]) for k,span in SPLITS.items()}
    yearly=[{"year":year,**metric(df[df.year==year])} for year in range(2019,2026)]
    gates={k: bool(v["sample_count"]>=300 and (v["expectancy"] or 0)>0 and (v["profit_factor"] or 0)>=1.2 and (v["daily_profit_factor"] or 0)>=1.15) for k,v in splits.items()}
    annual_pass=all((row["daily_profit_factor"] or 0)>=1.0 for row in yearly)
    payload={"schema_version":f"{AXIS_ID}_v1","generated_at":now.isoformat(),"axis_id":AXIS_ID,"boundary_owner":"TRADEX","research_phase":"effectiveness_judgment",
      "fixed_evaluation_conditions":{"shape":"support break capitulation","information_gate":"same-day all-stock breadth below MA20 >=40%","entry":"next-day signal-low stop entry","exit":"TP10/SL8/max10 stop-first","aggregation":"equal-weight mean return per signal day, identical to leaf20 daily basket metric","splits":SPLITS,"costs":"not modeled"},
      "metrics_by_split":splits,"yearly_daily_basket_metrics":yearly,"leaf20_compatibility_gate":{"per_split":"sample_count>=300, expectancy>0, trade PF>=1.2, daily PF>=1.15","pass":gates,"annual_daily_pf_pass":annual_pass},
      "decision":{"candidate_local_decision":"hold_insufficient_breadth" if not all(gates.values()) else ("keep" if annual_pass else "hold"),"authoritative_rollup_decision":"research_only","reason":"daily basket aggregation is now aligned with leaf20; sample-count gate remains binding"},"runtime_db_write":False,"meemee_unchanged":True,"production_ranking_changed":False,"silent_fallback_used":False}
    (run/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (OUT/"latest_compare.json").write_text(json.dumps({"run_root":str(run),**payload},ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(run/"compare.json")


if __name__=="__main__": main()
