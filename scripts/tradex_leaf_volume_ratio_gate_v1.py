from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='leaf_volume_ratio_gate_v1';OUT=Path(r'G:\Tradex\leaf_volume_ratio_gate_v1');CEILINGS=(.8,1.0,1.2,1.5,99.0)
def cap5(x):
 a=[];active=[]
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d]
  for i,r in g.sort_values(['tie_gap_ma60','code'],ascending=[False,True]).head(max(0,min(3,5-len(active)))).iterrows():a.append(i);active.append(r.exit_date)
 return x.loc[a]
def metric(x):
 p=x.assign(pnl=x.next_open_return*2e6).groupby('exit_date').pnl.sum().sort_index();eq=1e7+p.cumsum();dd=eq-eq.cummax();s,y=_metrics(x,'next_open_return');return {'n':len(x),'pnl_2024_2025_yen':float((x[x.year>=2024].next_open_return*2e6).sum()),'max_dd_yen':float(dd.min()),'metrics_by_split':s,'yearly':y,'red_year_count':sum((r['daily_expectancy'] or 0)<=0 for r in y)}
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);rows=[];sets={}
 for c in CEILINGS:z=cap5(x[x.tie_volume_ratio<=c]);sets[c]=z;rows.append({'volume_ma20_ratio_ceiling':c,'metrics':metric(z)})
 e=[z for z in rows if z['metrics']['metrics_by_split']['train']['profit_factor']>=1.2 and z['metrics']['metrics_by_split']['train']['daily_profit_factor']>=1.15];ch=max(e,key=lambda z:z['metrics']['metrics_by_split']['train']['daily_profit_factor']) if e else None;c=ch['volume_ma20_ratio_ceiling'] if ch else None;m=ch['metrics'] if ch else {};ok=bool(ch and m['metrics_by_split']['validation']['profit_factor']>=1.2 and m['metrics_by_split']['test']['profit_factor']>=1.5 and m['pnl_2024_2025_yen']>4964781 and m['max_dd_yen']>=-1500000 and m['red_year_count']==0)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);(sets[c] if c is not None else x.iloc[:0]).to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'leaf_shape_exit_rank_cap_unchanged':True,'changed_axis':'signal-day volume/MA20 ceiling only','ceilings':CEILINGS,'selection':'2019-2021 only','validation':'2022-2023','test':'2024-2025','capital':'10m/2m/max5'},'variants':rows,'selection':{'selected_ceiling':c,'protocol':'highest train daily PF among train gates'},'target_gate':{'pass':ok},'decision':{'candidate_local_decision':'keep' if ok else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
