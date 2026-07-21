from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='leaf_relative_strength_gate_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf_relative_strength_gate_v1');FLOORS=(0,.5,.7,.8,.9)
def cap5(x):
 a=[];active=[]
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d]
  for i,r in g.sort_values(['tie_gap_ma60','code'],ascending=[False,True]).head(max(0,min(3,5-len(active)))).iterrows():a.append(i);active.append(r.exit_date)
 return x.loc[a]
def metric(x):
 p=x.assign(pnl=x.next_open_return*2e6).groupby('exit_date').pnl.sum().sort_index();eq=1e7+p.cumsum();dd=eq-eq.cummax();s,y=_metrics(x,'next_open_return');return {'n':len(x),'pnl_2024_2025_yen':float((x[x.year>=2024].next_open_return*2e6).sum()),'max_dd_yen':float(dd.min()),'metrics_by_split':s,'yearly':y,'red_year_count':sum((r['daily_expectancy'] or 0)<=0 for r in y)}
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);x['code']=x.code.astype(str)
 q="""WITH b AS(SELECT code,date,c,lag(c,20) over(partition by code order by date) c20 FROM daily_bars WHERE source='pan'),r AS(SELECT code,date,c/c20-1 ret20 FROM b WHERE c20>0) SELECT code,date,ret20,percent_rank() over(partition by date order by ret20) rs20_pct FROM r"""
 with duckdb.connect(str(DB),read_only=True) as c:r=c.execute(q).fetchdf();r['code']=r.code.astype(str);x=x.merge(r,on=['code','date'],how='left');rows=[];sets={}
 for f in FLOORS:c=cap5(x[x.rs20_pct>=f]);sets[f]=c;rows.append({'rs20_percentile_floor':f,'metrics':metric(c)})
 e=[z for z in rows if z['metrics']['metrics_by_split']['train']['profit_factor']>=1.2 and z['metrics']['metrics_by_split']['train']['daily_profit_factor']>=1.15];ch=max(e,key=lambda z:z['metrics']['metrics_by_split']['train']['daily_profit_factor']) if e else None;f=ch['rs20_percentile_floor'] if ch else None;m=ch['metrics'] if ch else {};ok=bool(ch and m['metrics_by_split']['validation']['profit_factor']>=1.2 and m['metrics_by_split']['test']['profit_factor']>=1.5 and m['pnl_2024_2025_yen']>4964781 and m['max_dd_yen']>=-1500000 and m['red_year_count']==0)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);(sets[f] if f is not None else x.iloc[:0]).to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'leaf_shape_exit_rank_cap_unchanged':True,'changed_axis':'cross-sectional 20-row return percentile floor only','floors':FLOORS,'selection':'2019-2021 only','validation':'2022-2023','test':'2024-2025','capital':'10m/2m/max5'},'variants':rows,'selection':{'selected_floor':f,'protocol':'highest train daily PF among train gates'},'target_gate':{'pass':ok},'decision':{'candidate_local_decision':'keep' if ok else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
