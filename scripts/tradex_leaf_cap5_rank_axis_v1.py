from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='leaf_cap5_rank_axis_v1';OUT=Path(r'G:\Tradex\leaf_cap5_rank_axis_v1')
VARIANTS=(('gap_ma60_high','tie_gap_ma60',False),('gap_ma60_low','tie_gap_ma60',True),('ma20_slope_high','tie_ma20_slope',False),('volume_low','tie_volume_ratio',True),('range10_low','tie_range10',True),('prior_high_gap_high','tie_prior_high1_gap',False),('ret10_high','tie_ret10',False))
def cap5(x:pd.DataFrame,feature:str,ascending:bool)->pd.DataFrame:
 accepted=[];active=[]
 for d,g in x.sort_values('next_entry_date').groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d];avail=5-len(active)
  for i,r in g.sort_values([feature,'code'],ascending=[ascending,True]).head(max(0,min(3,avail))).iterrows():accepted.append(i);active.append(r.exit_date)
 return x.loc[accepted].copy()
def budget(x:pd.DataFrame)->dict:
 pnl=x.assign(pnl=x.next_open_return*2_000_000).groupby('exit_date').pnl.sum().sort_index();eq=10_000_000+pnl.cumsum();dd=eq-eq.cummax();splits,yearly=_metrics(x,'next_open_return');recent=x[x.year.between(2024,2025)];return {'trade_count':len(x),'net_pnl_yen':float((x.next_open_return*2_000_000).sum()),'pnl_2024_2025_yen':float((recent.next_open_return*2_000_000).sum()),'max_realized_drawdown_yen':float(dd.min()),'metrics_by_split':splits,'yearly':yearly,'red_year_count':sum((r['daily_expectancy'] or 0)<=0 for r in yearly)}
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);rows=[];sets={}
 for name,feature,asc in VARIANTS:
  c=cap5(x,feature,asc);sets[name]=c;m=budget(c);rows.append({'variant':name,'feature':feature,'ascending':asc,'metrics':m})
 eligible=[r for r in rows if r['metrics']['metrics_by_split']['train']['profit_factor']>=1.2 and r['metrics']['metrics_by_split']['train']['daily_profit_factor']>=1.15];chosen=max(eligible,key=lambda r:r['metrics']['metrics_by_split']['train']['daily_profit_factor']) if eligible else None;final=sets[chosen['variant']] if chosen else x.iloc[:0];oos=chosen and all(chosen['metrics']['metrics_by_split'][s]['profit_factor']>=1.2 for s in ('validation','test')) and chosen['metrics']['red_year_count']==0
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);final.to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source_events':str(source),'shape_exit_unchanged':True,'changed_axis':'same-day candidate ordering feature only','capital_yen':10_000_000,'slot_yen':2_000_000,'maximum_positions':5,'selection_period':'2019-2021 train only','validation':'2022-2023','test':'2024-2025','costs':'not modeled'},'variants':rows,'selection':{'protocol':'highest train daily PF among train PF>=1.2 and daily PF>=1.15','selected_variant':chosen['variant'] if chosen else None},'selected':chosen,'decision':{'candidate_local_decision':'keep_for_next_axis' if oos else 'drop','authoritative_rollup_decision':'research_only','reason_type':'train_selected_then_validation_test_and_all_years_checked'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
