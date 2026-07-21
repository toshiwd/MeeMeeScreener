from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric
AXIS_ID='leaf20_vol3_2026_performance_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf20_vol3_2026_performance_v1')
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\prehistory_reference_events.csv'))[-1]);x=pd.read_csv(source);x=x[(x.shape_leaf==20)&(x.year==2026)].copy();x['code']=x.code.astype(str);x['ret']=x.next_open_return
 q="""WITH b AS(SELECT code,date,c/lag(c) over(partition by code order by date)-1 r FROM daily_bars WHERE source='pan') SELECT code,date,stddev_samp(r) over(partition by code order by date rows between 19 preceding and current row) vol20 FROM b"""
 with duckdb.connect(str(DB),read_only=True) as c:v=c.execute(q).fetchdf();v['code']=v.code.astype(str);x=x.merge(v,on=['code','date'],how='left');z=x[x.vol20<.03].copy();m=metric(z);z['signal_month']=pd.to_datetime(z.date,unit='s').dt.strftime('%Y-%m');monthly={month:metric(g) for month,g in z.groupby('signal_month')};root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);z.to_csv(root/'completed_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'confirmed_data_through':'2026-07-10','completion_contract':'only signals with next-open entry and complete 10-session OHLC path are included','rule':{'leaf':20,'vol20_ceiling':.03,'next_open_gap_ceiling':0,'tp':.08,'sl':.05,'hold':10},'completed_2026_metrics':m,'monthly_metrics':monthly,'completed_events':[{'code':str(r.code),'signal_date':pd.to_datetime(r.date,unit='s').strftime('%Y-%m-%d'),'return':float(r.ret),'vol20':float(r.vol20)} for r in z.itertuples()],'decision':{'candidate_local_decision':'positive_so_far' if m['n'] and m['expectancy']>0 and (m['profit_factor'] or 0)>1 else 'negative_or_no_sample','authoritative_rollup_decision':'research_only'},'capital_allocation':'not used','runtime_db_write':False,'production_ranking_changed':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
