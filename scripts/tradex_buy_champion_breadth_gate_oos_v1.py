from __future__ import annotations
import argparse,json,glob
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import duckdb,pandas as pd

AXIS_ID="tradex_buy_champion_breadth_gate_oos_v1"; DB=Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb"); OUT=Path(r"G:\Tradex\buy_champion_breadth_gate_oos_v1")
THRESHOLDS=(.3,.4,.5,.6,.7,.8)
def pf(s:pd.Series)->float|None:
 l=float(-s[s<0].sum());return float(s[s>0].sum())/l if l else None
def metrics(d:pd.DataFrame)->dict[str,Any]: return {"n":int(len(d)),"expectancy":float(d.close_return.mean()) if len(d) else None,"profit_factor":pf(d.close_return) if len(d) else None,"win_rate":float((d.close_return>0).mean()) if len(d) else None,"stop_rate":float(d.close_return_stop.mean()) if len(d) else None,"p05":float(d.close_return.quantile(.05)) if len(d) else None}
def run(events:Path,db:Path,out:Path)->Path:
 d=pd.read_csv(events)
 with duckdb.connect(str(db),read_only=True) as c:
  b=c.execute("""WITH n AS (SELECT code,CASE WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER) ELSE CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) END ymd,c FROM daily_bars WHERE source='pan'), x AS (SELECT *,avg(c) OVER(PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20 FROM n) SELECT ymd,avg(CASE WHEN c>ma20 THEN 1.0 ELSE 0.0 END) breadth_above_ma20 FROM x WHERE ymd BETWEEN 20240101 AND 20251231 GROUP BY ymd""").fetchdf()
 d=d.merge(b,on='ymd',how='left'); reports=[]
 for threshold in THRESHOLDS:
  row={"breadth_floor":threshold,"train":metrics(d[(d.year==2024)&(d.breadth_above_ma20>=threshold)]),"test":metrics(d[(d.year==2025)&(d.breadth_above_ma20>=threshold)])}; reports.append(row)
 eligible=[r for r in reports if r['train']['n']>=20 and (r['train']['expectancy'] or 0)>0 and (r['train']['profit_factor'] or 0)>=1.2 and (r['train']['stop_rate'] or 1)<=.75]
 selected=max(eligible,key=lambda r:r['train']['profit_factor']) if eligible else None
 test_pass=bool(selected and selected['test']['n']>=50 and (selected['test']['expectancy'] or 0)>0 and (selected['test']['profit_factor'] or 0)>=1.2 and (selected['test']['stop_rate'] or 1)<=.75)
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True)
 payload={"schema_version":f"{AXIS_ID}_v1","generated_at":datetime.now(timezone.utc).isoformat(),"axis_id":AXIS_ID,"boundary_owner":"TRADEX","research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"source_events":str(events),"candidate_shape_and_priority":"existing MA60-slope/lower-wick champion unchanged","entry":"signal close","stop":.03,"hold":20,"changed_axis":"same-day all-stock breadth above MA20 floor only","predeclared_thresholds":THRESHOLDS,"train":2024,"untouched_test":2025,"costs":"not modeled"},"reports":reports,"selection":{"protocol":"highest train PF among n>=20 expectancy>0 PF>=1.2 stop_rate<=.75","selected_breadth_floor":selected['breadth_floor'] if selected else None},"test_gate":{"pass":test_pass,"requirements":"n>=50 expectancy>0 PF>=1.2 stop_rate<=.75"},"decision":{"candidate_local_decision":"keep_for_long_history_validation" if test_pass else "drop","authoritative_rollup_decision":"research_only","reason":"market breadth gate selected on 2024 and tested untouched on 2025"},"limitations":["ranking_appearance history starts in 2024","train sample at the selected strict breadth gate is small"],"runtime_db_write":False,"production_ranking_changed":False,"silent_fallback_used":False}
 (root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding='utf-8');print(root/'compare.json');return root
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--events',type=Path,default=None);p.add_argument('--db',type=Path,default=DB);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();event=a.events or Path(sorted(glob.glob(r'G:\Tradex\buy_champion_next_open_oos_v1\*\events.csv'))[-1]);run(event,a.db,a.out)
