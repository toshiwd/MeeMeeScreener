from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
from scripts.tradex_short_support_break_breadth_gate_v1 import SQL
from scripts.tradex_short_support_break_exit_grid_v1 import DB_PATH, clean, simulate
from scripts.tradex_short_breadth_daily_basket_replay_v1 import metric

AXIS_ID="tradex_short_breadth_loss_cap_v1"; OUT=Path(r"G:\Tradex\short_breadth_loss_cap_v1")
SPLITS={"train":(2019,2021),"validation":(2022,2023),"test":(2024,2025)}
SCENARIOS=(("tp5_sl3_h10",.05,.03), ("tp8_sl5_h10",.08,.05), ("tp10_sl5_h10",.10,.05))


def percentile(values: pd.Series,q:float): return float(values.quantile(q)) if len(values) else None
def summary(df:pd.DataFrame)->dict:
    result=metric(df); result["p05_ret"]=percentile(df.ret,.05); result["loss_mean"]=float(df.loc[df.ret<0,"ret"].mean()) if (df.ret<0).any() else None; return result


def main()->None:
    now=datetime.now(timezone.utc); run=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; run.mkdir(parents=True)
    with duckdb.connect(str(DB_PATH),read_only=True) as db: raw=[{k:clean(v) for k,v in row.items()} for row in db.execute(SQL).fetchdf().to_dict("records")]
    raw=[r for r in raw if float(r["breadth_below_ma20"])>=.40]
    reports=[]
    for name,tp,sl in SCENARIOS:
        df=pd.DataFrame([{**r,**simulate(r,tp,sl,10)} for r in raw]); df["year"]=df.ymd.astype(str).str[:4].astype(int)
        splits={key:summary(df[df.year.between(*span)]) for key,span in SPLITS.items()}
        yearly=[{"year":year,**summary(df[df.year==year])} for year in range(2019,2026)]
        reports.append({"scenario":name,"take_profit":tp,"stop_loss":sl,"splits":splits,"yearly":yearly})
    def split_gate(item):
        return (item["sample_count"] or 0)>=60 and (item["profit_factor"] or 0)>=1.2 and (item["daily_profit_factor"] or 0)>=1.15 and (item["p05_ret"] or -1)>=-.05
    eligible=[r for r in reports if split_gate(r["splits"]["train"])]
    best=max(eligible,key=lambda r:r["splits"]["train"]["daily_profit_factor"]) if eligible else None
    annual=best["yearly"] if best else []
    annual_pass=bool(annual) and all((r["sample_count"] or 0)>=20 and (r["daily_profit_factor"] or 0)>=1.0 and (r["p05_ret"] or -1)>=-.05 for r in annual)
    out_of_sample_pass=bool(best) and all(split_gate(best["splits"][key]) for key in ("validation","test"))
    payload={"schema_version":f"{AXIS_ID}_v1","generated_at":now.isoformat(),"axis_id":AXIS_ID,"boundary_owner":"TRADEX","research_phase":"effectiveness_judgment",
      "fixed_evaluation_conditions":{"shape":"support break capitulation","information_gate":"same-day breadth below MA20 >=40%","entry":"next-day signal-low stop entry","holding":"10 days","changed_axis":"loss cap and predeclared paired target","aggregation":"equal-weight daily basket","splits":SPLITS,"costs":"not modeled"},
      "reports":reports,"selection":{"train_only_gate":"n>=60, trade PF>=1.2, daily PF>=1.15, p05>=-5%","selection_protocol":"highest train daily PF among predeclared exits; validation and test are not used for selection","selected_scenario":best["scenario"] if best else None},"post_selection_evaluation":{"out_of_sample_gate":"validation and test each meet the same split gate","out_of_sample_pass":out_of_sample_pass,"annual_gate":"2019-2025 each n>=20, daily PF>=1.0, p05>=-5%","annual_pass":annual_pass,"failures":[r for r in annual if (r["sample_count"] or 0)<20 or (r["daily_profit_factor"] or 0)<1.0 or (r["p05_ret"] or -1)<-.05]},
      "decision":{"candidate_local_decision":"keep" if best and out_of_sample_pass and annual_pass else ("hold" if best else "drop"),"authoritative_rollup_decision":"research_only","reason":"train-only selected loss-capped rule passes validation, test, and annual gates" if best and out_of_sample_pass and annual_pass else ("train-only selected rule fails an out-of-sample or annual gate" if best else "no predeclared loss-capped exit passes the train gate")},"runtime_db_write":False,"meemee_unchanged":True,"production_ranking_changed":False,"silent_fallback_used":False}
    (run/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); (OUT/"latest_compare.json").write_text(json.dumps({"run_root":str(run),**payload},ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(run/"compare.json")

if __name__=="__main__": main()
