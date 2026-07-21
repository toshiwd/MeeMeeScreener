from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='leaf_position_cap_axis_v1';OUT=Path(r'G:\Tradex\leaf_position_cap_axis_v1');CAPS=(1,2,3,4,5)
def replay(x:pd.DataFrame,cap:int)->pd.DataFrame:
 accepted=[];active=[]
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d];avail=cap-len(active)
  for i,r in g.sort_values(['tie_gap_ma60','code'],ascending=[False,True]).head(max(0,min(3,avail))).iterrows():accepted.append(i);active.append(r.exit_date)
 return x.loc[accepted].copy()
def metric(x:pd.DataFrame)->dict:
 pnl=x.assign(pnl=x.next_open_return*2_000_000).groupby('exit_date').pnl.sum().sort_index();eq=10_000_000+pnl.cumsum();dd=eq-eq.cummax();s,y=_metrics(x,'next_open_return');recent=x[x.year.between(2024,2025)];return {'trade_count':len(x),'pnl_2024_2025_yen':float((recent.next_open_return*2e6).sum()),'net_pnl_yen':float((x.next_open_return*2e6).sum()),'max_realized_drawdown_yen':float(dd.min()),'metrics_by_split':s,'yearly':y,'red_year_count':sum((r['daily_expectancy'] or 0)<=0 for r in y)}
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);rows=[];sets={}
 for cap in CAPS:c=replay(x,cap);sets[cap]=c;rows.append({'maximum_positions':cap,'metrics':metric(c)})
 eligible=[r for r in rows if r['metrics']['metrics_by_split']['train']['profit_factor']>=1.2 and r['metrics']['metrics_by_split']['train']['daily_profit_factor']>=1.15];chosen=max(eligible,key=lambda r:r['metrics']['metrics_by_split']['train']['daily_profit_factor']) if eligible else None;cap=chosen['maximum_positions'] if chosen else None;final=sets[cap] if cap else x.iloc[:0];passes=bool(chosen and chosen['metrics']['metrics_by_split']['validation']['profit_factor']>=1.2 and chosen['metrics']['metrics_by_split']['test']['profit_factor']>=1.5 and chosen['metrics']['pnl_2024_2025_yen']>4964781 and chosen['metrics']['max_realized_drawdown_yen']>=-1500000 and chosen['metrics']['red_year_count']==0)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);final.to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'shape_exit_ranking_unchanged':True,'ranking':'gap_ma60 descending','changed_axis':'maximum concurrent positions only','caps':CAPS,'slot_yen':2000000,'capital_yen':10000000,'selection_period':'2019-2021 only','validation':'2022-2023','test':'2024-2025'},'variants':rows,'selection':{'protocol':'highest train daily PF among train gate pass','selected_maximum_positions':cap},'selected':chosen,'target_gate':{'pnl_2024_2025_above_yen':4964781,'test_pf_min':1.5,'max_dd_floor_yen':-1500000,'red_year_count':0,'pass':passes},'decision':{'candidate_local_decision':'keep_for_next_axis' if passes else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
