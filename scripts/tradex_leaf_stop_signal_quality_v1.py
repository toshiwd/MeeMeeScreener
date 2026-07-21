from __future__ import annotations
import glob,json,math
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric,gates
AXIS_ID='leaf_stop_signal_quality_v1';OUT=Path(r'G:\Tradex\leaf_stop_signal_quality_v1');STOPS=(.03,.04,.05);TP=.08;H=10
def outcomes(x,sl):
 z=x.copy();v=[]
 for r in z.itertuples(index=False):
  th=next((i for i in range(1,H+1) if getattr(r,f'h{i}')>=r.entry_price*(1+TP)),99);sh=next((i for i in range(1,H+1) if getattr(r,f'l{i}')<=r.entry_price*(1-sl)),99);v.append(-sl if sh<=th and sh<=H else TP if th<=H else r.close_horizon/r.entry_price-1)
 z['ret']=v;return z
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);rows=[];sets={}
 for sl in STOPS:
  z=outcomes(x,sl);sets[sl]=z;m={'train':metric(z[z.year<=2021]),'validation':metric(z[z.year.between(2022,2023)]),'test':metric(z[z.year>=2024])};rows.append({'stop_loss':sl,'metrics_by_split':m,'train_gate_pass':gates(m['train'],200)})
 e=[r for r in rows if r['train_gate_pass']];ch=max(e,key=lambda r:(r['metrics_by_split']['train']['geometric_mean'],r['metrics_by_split']['train']['expectancy'])) if e else None;sl=ch['stop_loss'] if ch else None;oos=bool(ch and gates(ch['metrics_by_split']['validation'],100) and gates(ch['metrics_by_split']['test'],100));root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);(sets[sl] if sl else x.iloc[:0]).to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'leaf_shape_entry_unchanged':True,'changed_axis':'stop loss only','stop_levels':STOPS,'take_profit':TP,'maximum_holding_sessions':H,'same_day_dual_hit':'stop first','selection':'2019-2021 mean log return among hard-gate pass','capital_allocation':'not used'},'variants':rows,'selection':{'selected_stop':sl,'selected':ch},'oos_gate_pass':oos,'decision':{'candidate_local_decision':'keep_for_hold_axis' if oos else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
