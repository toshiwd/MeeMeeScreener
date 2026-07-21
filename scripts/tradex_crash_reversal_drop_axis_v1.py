from __future__ import annotations
import json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='crash_reversal_drop_axis_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\crash_reversal_drop_axis_v1');DROPS=(-.08,-.12,-.16);TP=.08;SL=.04;H=10
def raw():
 hs=','.join(f'lead(h,{i}) over w h{i}' for i in range(1,11));ls=','.join(f'lead(l,{i}) over w l{i}' for i in range(1,11));ds=','.join(f'lead(date,{i}) over w d{i}' for i in range(1,11))
 q=f"""WITH b AS(SELECT code,date,CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) ymd,o,h,l,c,v,lag(l) over w prev_l,lag(c,10) over w c10,avg(v) over(partition by code order by date rows between 19 preceding and current row) av20,lead(o) over w next_open,lead(date) over w next_entry_date,lead(c,10) over w exit_c,{hs},{ls},{ds} FROM daily_bars WHERE source='pan' WINDOW w AS(partition by code order by date)) SELECT * FROM b WHERE ymd BETWEEN 20190101 AND 20251231 AND c10>0 AND av20>0 AND exit_c IS NOT NULL AND l<prev_l AND c>o AND (c-l)/nullif(h-l,0)>=.7 AND v/av20>=1.5"""
 with duckdb.connect(str(DB),read_only=True) as c:return c.execute(q).fetchdf()
def events(x,drop):
 z=x[x.c/x.c10-1<=drop].copy();z['drop10']=z.c/z.c10-1;z=z.sort_values(['ymd','drop10','code']);z['day_rank']=z.groupby('ymd').cumcount()+1;z=z[z.day_rank<=3].copy()
 def out(r):
  tp=next((i for i in range(1,11) if r[f'h{i}']>=r.next_open*(1+TP)),99);sl=next((i for i in range(1,11) if r[f'l{i}']<=r.next_open*(1-SL)),99)
  if sl<=tp and sl<=10:return -SL,True,r[f'd{sl}']
  if tp<=10:return TP,False,r[f'd{tp}']
  return r.exit_c/r.next_open-1,False,r.d10
 vals=z.apply(out,axis=1);z['next_open_return']=[v[0] for v in vals];z['stopped']=[v[1] for v in vals];z['exit_date']=[v[2] for v in vals];z['year']=(z.ymd//10000).astype(int);z['split']=z.year.map(lambda y:'train' if y<=2021 else ('validation' if y<=2023 else 'test'));return z
def cap5(x):
 a=[];active=[]
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d]
  for i,r in g.head(max(0,min(3,5-len(active)))).iterrows():a.append(i);active.append(r.exit_date)
 return x.loc[a].copy()
def metric(x):
 p=x.assign(pnl=x.next_open_return*2e6).groupby('exit_date').pnl.sum().sort_index();eq=1e7+p.cumsum();dd=eq-eq.cummax();s,y=_metrics(x,'next_open_return');recent=x[x.year.between(2024,2025)];return {'trade_count':len(x),'pnl_2024_2025_yen':float((recent.next_open_return*2e6).sum()),'net_pnl_yen':float((x.next_open_return*2e6).sum()),'max_realized_drawdown_yen':float(dd.min()) if len(dd) else 0,'metrics_by_split':s,'yearly':y,'red_year_count':sum((r['daily_expectancy'] or 0)<=0 for r in y)}
def run():
 x=raw();rows=[];sets={}
 for d in DROPS:c=cap5(events(x,d));sets[d]=c;rows.append({'drop10_ceiling':d,'metrics':metric(c)})
 eligible=[r for r in rows if r['metrics']['metrics_by_split'].get('train',{}).get('sample_count',0)>=100 and r['metrics']['metrics_by_split']['train']['profit_factor']>=1.2 and r['metrics']['metrics_by_split']['train']['daily_profit_factor']>=1.15];chosen=max(eligible,key=lambda r:r['metrics']['metrics_by_split']['train']['daily_profit_factor']) if eligible else None;d=chosen['drop10_ceiling'] if chosen else None;final=sets[d] if d else x.iloc[:0];m=chosen['metrics'] if chosen else {};passes=bool(chosen and m['metrics_by_split']['validation']['profit_factor']>=1.2 and m['metrics_by_split']['test']['profit_factor']>=1.5 and m['red_year_count']==0 and m['max_realized_drawdown_yen']>=-1500000)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);final.to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'branching_generation','fixed_evaluation_conditions':{'universe':'all PAN codes 2019-2025','base_shape':'low<prior low; bullish candle; close position>=0.7; volume/MA20>=1.5','changed_axis':'10-row decline ceiling only','thresholds':DROPS,'entry':'next session open','tp':TP,'sl':SL,'hold':H,'same_bar':'stop first','ranking':'deepest 10-row decline then code; top3/day','capital':'10m, 2m slot, max5','selection':'2019-2021 only','validation':'2022-2023','test':'2024-2025'},'variants':rows,'selection':{'protocol':'highest train daily PF among n>=100 PF>=1.2 dailyPF>=1.15','selected_drop10_ceiling':d},'selected':chosen,'stability_gate':{'pass':passes},'decision':{'candidate_local_decision':'keep_for_leaf_additive_test' if passes else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
