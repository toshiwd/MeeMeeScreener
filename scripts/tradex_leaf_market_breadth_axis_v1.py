from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='leaf_market_breadth_axis_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf_market_breadth_axis_v1');FLOORS=(0,.3,.4,.5,.6,.7,.8)
def cap5(x):
 a=[];active=[]
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d]
  for i,r in g.sort_values(['tie_gap_ma60','code'],ascending=[False,True]).head(max(0,min(3,5-len(active)))).iterrows():a.append(i);active.append(r.exit_date)
 return x.loc[a].copy()
def metric(x):
 pnl=x.assign(pnl=x.next_open_return*2e6).groupby('exit_date').pnl.sum().sort_index();eq=1e7+pnl.cumsum();dd=eq-eq.cummax();s,y=_metrics(x,'next_open_return');recent=x[x.year.between(2024,2025)];return {'trade_count':len(x),'pnl_2024_2025_yen':float((recent.next_open_return*2e6).sum()),'max_realized_drawdown_yen':float(dd.min()) if len(dd) else 0,'metrics_by_split':s,'yearly':y,'red_year_count':sum((r['daily_expectancy'] or 0)<=0 for r in y)}
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source)
 q="""WITH b AS(SELECT date,code,c,avg(c) over(partition by code order by date rows between 19 preceding and current row) ma20 FROM daily_bars WHERE source='pan') SELECT date,avg(case when c>ma20 then 1.0 else 0.0 end) breadth FROM b GROUP BY date"""
 with duckdb.connect(str(DB),read_only=True) as c:b=c.execute(q).fetchdf()
 x=x.merge(b,on='date',how='left');rows=[];sets={}
 for floor in FLOORS:c=cap5(x[x.breadth>=floor]);sets[floor]=c;rows.append({'breadth_floor':floor,'metrics':metric(c)})
 eligible=[r for r in rows if r['metrics']['metrics_by_split']['train']['profit_factor']>=1.2 and r['metrics']['metrics_by_split']['train']['daily_profit_factor']>=1.15];chosen=max(eligible,key=lambda r:r['metrics']['metrics_by_split']['train']['daily_profit_factor']) if eligible else None;floor=chosen['breadth_floor'] if chosen else None;final=sets[floor] if floor is not None else x.iloc[:0];m=chosen['metrics'] if chosen else {};passes=bool(chosen and m['metrics_by_split']['validation']['profit_factor']>=1.2 and m['metrics_by_split']['test']['profit_factor']>=1.5 and m['pnl_2024_2025_yen']>4964781 and m['max_realized_drawdown_yen']>=-1500000 and m['red_year_count']==0)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);final.to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'shape_exit_ranking_cap_unchanged':True,'changed_axis':'same-day PAN breadth above MA20 floor only','floors':FLOORS,'maximum_positions':5,'slot_yen':2000000,'selection_period':'2019-2021 only','validation':'2022-2023','test':'2024-2025'},'variants':rows,'selection':{'protocol':'highest train daily PF among train gate pass','selected_breadth_floor':floor},'selected':chosen,'target_gate':{'pass':passes},'decision':{'candidate_local_decision':'keep_for_next_axis' if passes else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
