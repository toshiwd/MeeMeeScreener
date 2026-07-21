from __future__ import annotations
import json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='low_zone_volume_breakout_axis_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\low_zone_volume_breakout_axis_v1');ZONES=(.2,.3,.4);TP=.10;SL=.05;H=15
def raw():
 hs=','.join(f'lead(h,{i}) over w h{i}' for i in range(1,H+1));ls=','.join(f'lead(l,{i}) over w l{i}' for i in range(1,H+1));ds=','.join(f'lead(date,{i}) over w d{i}' for i in range(1,H+1))
 q=f"""WITH b AS(SELECT code,date,CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) ymd,o,h,l,c,v,max(h) over(partition by code order by date rows between 199 preceding and current row) hi200,min(l) over(partition by code order by date rows between 199 preceding and current row) lo200,max(h) over(partition by code order by date rows between 20 preceding and 1 preceding) hi20,avg(v) over(partition by code order by date rows between 19 preceding and current row) av20,avg(c) over(partition by code order by date rows between 59 preceding and current row) ma60,lead(o) over w next_open,lead(date) over w next_entry_date,lead(c,{H}) over w exit_c,{hs},{ls},{ds} FROM daily_bars WHERE source='pan' WINDOW w AS(partition by code order by date)),f AS(SELECT *,lag(ma60,20) over(partition by code order by date) ma60_20 FROM b) SELECT * FROM f WHERE ymd BETWEEN 20190101 AND 20251231 AND exit_c IS NOT NULL AND hi200>lo200 AND c>=hi20 AND ma60>ma60_20 AND v/av20>=2 AND c>o AND (c-o)/nullif(h-l,0)>=.5 AND (h-c)/nullif(h-l,0)<=.25"""
 with duckdb.connect(str(DB),read_only=True) as c:return c.execute(q).fetchdf()
def events(x,zone):
 z=x[(x.c-x.lo200)/(x.hi200-x.lo200)<=zone].copy();z['zone_pos']=(z.c-z.lo200)/(z.hi200-z.lo200);z=z.sort_values(['ymd','zone_pos','code']);z['rank']=z.groupby('ymd').cumcount()+1;z=z[z['rank']<=3]
 def out(r):
  tp=next((i for i in range(1,H+1) if r[f'h{i}']>=r.next_open*(1+TP)),99);sl=next((i for i in range(1,H+1) if r[f'l{i}']<=r.next_open*(1-SL)),99)
  if sl<=tp and sl<=H:return -SL,r[f'd{sl}']
  if tp<=H:return TP,r[f'd{tp}']
  return r.exit_c/r.next_open-1,r[f'd{H}']
 v=z.apply(out,axis=1);z['next_open_return']=[a[0] for a in v];z['exit_date']=[a[1] for a in v];z['year']=z.ymd//10000;z['split']=z.year.map(lambda y:'train' if y<=2021 else ('validation' if y<=2023 else 'test'));return z
def cap5(x):
 a=[];active=[]
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d]
  for i,r in g.head(max(0,min(3,5-len(active)))).iterrows():a.append(i);active.append(r.exit_date)
 return x.loc[a]
def metric(x):
 p=x.assign(pnl=x.next_open_return*2e6).groupby('exit_date').pnl.sum().sort_index();eq=1e7+p.cumsum();dd=eq-eq.cummax();s,y=_metrics(x,'next_open_return');return {'n':len(x),'pnl_2024_2025_yen':float((x[x.year>=2024].next_open_return*2e6).sum()),'max_dd_yen':float(dd.min()) if len(dd) else 0,'metrics_by_split':s,'yearly':y,'red_year_count':sum((r['daily_expectancy'] or 0)<=0 for r in y)}
def run():
 x=raw();rows=[];sets={}
 for z in ZONES:c=cap5(events(x,z));sets[z]=c;rows.append({'maximum_200d_range_position':z,'metrics':metric(c)})
 e=[r for r in rows if r['metrics']['metrics_by_split'].get('train',{}).get('sample_count',0)>=100 and r['metrics']['metrics_by_split']['train']['profit_factor']>=1.2];ch=max(e,key=lambda r:r['metrics']['metrics_by_split']['train']['daily_profit_factor']) if e else None;z=ch['maximum_200d_range_position'] if ch else None;m=ch['metrics'] if ch else {};ok=bool(ch and m['metrics_by_split']['validation']['profit_factor']>=1.2 and m['metrics_by_split']['test']['profit_factor']>=1.5 and m['red_year_count']==0 and m['max_dd_yen']>=-1500000)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);(sets[z] if z else x.iloc[:0]).to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'branching_generation','fixed_evaluation_conditions':{'universe':'all PAN 2019-2025','base_shape':'20d high breakout; MA60 rising20; volume>=2x; bull body>=50%; upper wick<=25%','changed_axis':'maximum 200d range position only','zones':ZONES,'entry':'next open','tp':TP,'sl':SL,'hold':H,'capital':'10m/2m/max5','selection':'2019-2021 only'},'variants':rows,'selection':{'selected_zone':z},'stability_gate':{'pass':ok},'decision':{'candidate_local_decision':'keep_for_leaf_additive_test' if ok else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
