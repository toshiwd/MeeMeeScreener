from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='leaf_variable_intensity_axis_v1';OUT=Path(r'G:\Tradex\leaf_variable_intensity_axis_v1');POLICIES=((.33,.67),(.5,.75),(.6,.8))
def replay(x,q1,q2):
 train=x[x.year<=2021].groupby('next_entry_date').tie_gap_ma60.max();t1,t2=float(train.quantile(q1)),float(train.quantile(q2));a=[];active=[]
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d];score=g.tie_gap_ma60.max();daily_cap=1 if score<=t1 else (3 if score<=t2 else 5);avail=min(daily_cap,5-len(active))
  for i,r in g.sort_values(['tie_gap_ma60','code'],ascending=[False,True]).head(max(0,avail)).iterrows():a.append(i);active.append(r.exit_date)
 return x.loc[a].copy(),t1,t2
def metric(x):
 pnl=x.assign(pnl=x.next_open_return*2e6).groupby('exit_date').pnl.sum().sort_index();eq=1e7+pnl.cumsum();dd=eq-eq.cummax();s,y=_metrics(x,'next_open_return');recent=x[x.year.between(2024,2025)];return {'trade_count':len(x),'pnl_2024_2025_yen':float((recent.next_open_return*2e6).sum()),'max_realized_drawdown_yen':float(dd.min()),'metrics_by_split':s,'yearly':y,'red_year_count':sum((r['daily_expectancy'] or 0)<=0 for r in y)}
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);rows=[];sets={}
 for q1,q2 in POLICIES:
  c,t1,t2=replay(x,q1,q2);name=f'q{int(q1*100)}_q{int(q2*100)}';sets[name]=c;rows.append({'policy':name,'train_quantiles':[q1,q2],'frozen_gap_thresholds':[t1,t2],'allocation':'1/3/5 new-entry cap; total concurrent cap5','metrics':metric(c)})
 eligible=[r for r in rows if r['metrics']['metrics_by_split']['train']['profit_factor']>=1.2 and r['metrics']['metrics_by_split']['train']['daily_profit_factor']>=1.15];chosen=max(eligible,key=lambda r:r['metrics']['metrics_by_split']['train']['daily_profit_factor']) if eligible else None;name=chosen['policy'] if chosen else None;final=sets[name] if name else x.iloc[:0];m=chosen['metrics'] if chosen else {};passes=bool(chosen and m['metrics_by_split']['validation']['profit_factor']>=1.2 and m['metrics_by_split']['test']['profit_factor']>=1.5 and m['pnl_2024_2025_yen']>4964781 and m['max_realized_drawdown_yen']>=-1500000 and m['red_year_count']==0)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);final.to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'shape_exit_ranking_unchanged':True,'changed_axis':'daily new-entry intensity only','policies':POLICIES,'total_concurrent_cap':5,'slot_yen':2000000,'threshold_fit':'2019-2021 daily maximum gap_ma60 distribution only','validation':'2022-2023','test':'2024-2025'},'variants':rows,'selection':{'protocol':'highest train daily PF among train gate pass','selected_policy':name},'selected':chosen,'target_gate':{'pass':passes},'decision':{'candidate_local_decision':'keep' if passes else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
