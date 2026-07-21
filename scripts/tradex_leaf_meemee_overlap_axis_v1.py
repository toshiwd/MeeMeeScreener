from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='leaf_meemee_overlap_axis_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf_meemee_overlap_axis_v1')
def cap5(x):
 a=[];active=[]
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d]
  for i,r in g.sort_values(['tie_gap_ma60','code'],ascending=[False,True]).head(max(0,min(3,5-len(active)))).iterrows():a.append(i);active.append(r.exit_date)
 return x.loc[a].copy()
def metric(x):
 s,y=_metrics(x,'next_open_return') if len(x) else ({},[]);return {'trade_count':len(x),'pnl_yen':float((x.next_open_return*2e6).sum()),'metrics_by_split':s,'yearly':y}
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);x['ymd']=pd.to_datetime(x.date,unit='s').dt.strftime('%Y%m%d').astype(int)
 x['code']=x.code.astype(str)
 with duckdb.connect(str(DB),read_only=True) as c:r=c.execute("SELECT DISTINCT code,dt FROM ranking_appearance_daily WHERE dir='up' AND rank<=5 AND ranking_logic_version='ranking:trade:top50:v1'").fetchdf()
 r['code']=r.code.astype(str)
 r['overlap']=True;x=x.merge(r,left_on=['code','ymd'],right_on=['code','dt'],how='left');x['overlap']=x.overlap.fillna(False);x['split']=x.year.map(lambda y:'proxy_train_2024' if y==2024 else ('proxy_test_2025' if y==2025 else 'unavailable_pre2024'))
 variants=[]
 for name,mask in [('all',pd.Series(True,index=x.index)),('overlap_only',x.overlap),('exclude_overlap',~x.overlap)]:variants.append({'variant':name,'metrics':metric(cap5(x[mask & x.year.between(2024,2025)]))})
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'changed_axis':'same-day MeeMee up top5 overlap only','required_selection_period':'2019-2021','available_overlap_history':'2024-2025 only','proxy_split':'2024 descriptive train, 2025 descriptive test','shape_exit_ranking_cap_unchanged':True},'variants':variants,'selection':{'selected_variant':None,'reason':'required 2019-2021 overlap history unavailable; proxy period is not substituted'},'decision':{'candidate_local_decision':'drop_insufficient_train_history','authoritative_rollup_decision':'research_only','reason_type':'boundary_not_instrumented_before_2024'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
