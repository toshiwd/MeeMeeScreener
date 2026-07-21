from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric,gates
AXIS_ID='leaf_hold_signal_quality_v1';OUT=Path(r'G:\Tradex\leaf_hold_signal_quality_v1');HOLDS=(3,5,7,10);TP=.08;SL=.05
def outcomes(x,h):
 z=x.copy();v=[]
 for r in z.itertuples(index=False):
  th=next((i for i in range(1,h+1) if getattr(r,f'h{i}')>=r.entry_price*1.08),99);sh=next((i for i in range(1,h+1) if getattr(r,f'l{i}')<=r.entry_price*.95),99)
  if sh<=th and sh<=h:v.append(-SL)
  elif th<=h:v.append(TP)
  else:v.append((r.close_horizon if h==10 else getattr(r,f'c{h}'))/r.entry_price-1)
 z['ret']=v;return z
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);rows=[]
 for h in HOLDS:
  z=outcomes(x,h);m={'train':metric(z[z.year<=2021]),'validation':metric(z[z.year.between(2022,2023)]),'test':metric(z[z.year>=2024])};rows.append({'hold_sessions':h,'metrics_by_split':m,'train_gate_pass':gates(m['train'],200)})
 e=[r for r in rows if r['train_gate_pass']];ch=max(e,key=lambda r:r['metrics_by_split']['train']['geometric_mean']) if e else None;oos=bool(ch and gates(ch['metrics_by_split']['validation'],100) and gates(ch['metrics_by_split']['test'],100));root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'changed_axis':'maximum holding sessions only','holds':HOLDS,'take_profit':TP,'stop_loss':SL,'capital_allocation':'not used','horizon_exit':'exact close of selected session'},'variants':rows,'selection':{'selected':ch},'oos_gate_pass':oos,'decision':{'candidate_local_decision':'keep' if oos else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
