from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric
AXIS_ID='leaf20_vol3_prehistory_stability_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf20_vol3_prehistory_stability_v1')
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\prehistory_reference_events.csv'))[-1]);x=pd.read_csv(source);x=x[x.shape_leaf==20].copy();x['code']=x.code.astype(str);x['ret']=x.next_open_return
 q="""WITH b AS(SELECT code,date,c/lag(c) over(partition by code order by date)-1 r FROM daily_bars WHERE source='pan') SELECT code,date,stddev_samp(r) over(partition by code order by date rows between 19 preceding and current row) vol20 FROM b"""
 with duckdb.connect(str(DB),read_only=True) as c:v=c.execute(q).fetchdf();v['code']=v.code.astype(str);x=x.merge(v,on=['code','date'],how='left');z=x[x.vol20<.03];parts={'prehistory_2015_2018':metric(z[z.year.between(2015,2018)]),'train_2019_2021':metric(z[z.year.between(2019,2021)]),'validation_2022_2023':metric(z[z.year.between(2022,2023)]),'test_2024_2025':metric(z[z.year.between(2024,2025)])};pre=parts['prehistory_2015_2018'];stable=pre['n']>=100 and pre['expectancy']>0 and pre['profit_factor']>=1.2 and pre['win_rate']>=.45
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);z.to_csv(root/'events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_candidate':{'leaf':20,'vol20_ceiling':.03,'entry':'next open gap<=0','tp':.08,'sl':.05,'hold':10},'period_metrics':parts,'prehistory_gate':{'requirements':'n>=100 expectancy>0 PF>=1.2 win>=.45','pass':stable},'decision':{'candidate_local_decision':'keep_for_forward_accumulation' if stable else 'drop','authoritative_rollup_decision':'research_only'},'selection_changed':False,'capital_allocation':'not used','runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
