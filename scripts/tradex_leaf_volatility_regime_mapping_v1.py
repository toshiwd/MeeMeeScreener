from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric,gates
AXIS_ID='leaf_volatility_regime_mapping_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf_volatility_regime_mapping_v1');LEAVES=(9,14,20);REGIMES=('low','medium','high')
def bucket(v):return 'low' if v<.02 else 'medium' if v<.04 else 'high'
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);x['code']=x.code.astype(str);x['ret']=x.next_open_return
 q="""WITH b AS(SELECT code,date,c/lag(c) over(partition by code order by date)-1 r FROM daily_bars WHERE source='pan'),v AS(SELECT code,date,stddev_samp(r) over(partition by code order by date rows between 19 preceding and current row) vol20 FROM b) SELECT code,date,vol20 FROM v"""
 with duckdb.connect(str(DB),read_only=True) as c:v=c.execute(q).fetchdf();v['code']=v.code.astype(str);x=x.merge(v,on=['code','date'],how='left');x['vol_regime']=x.vol20.map(bucket);train=x[x.year<=2021];mapping={};diag=[]
 for rg in REGIMES:
  cand=[]
  for leaf in LEAVES:
   m=metric(train[(train.vol_regime==rg)&(train.shape_leaf==leaf)]);ok=m['n']>=50 and m['expectancy']>0 and m['profit_factor']>=1.2 and m['win_rate']>=.45;cand.append({'regime':rg,'leaf':leaf,'train_metrics':m,'local_gate_pass':ok})
  diag+=cand;valid=[r for r in cand if r['local_gate_pass']];mapping[rg]=max(valid,key=lambda r:r['train_metrics']['geometric_mean'])['leaf'] if valid else None
 z=x[x.apply(lambda r:mapping.get(r.vol_regime)==r.shape_leaf,axis=1)];parts={'train':metric(z[z.year<=2021]),'validation':metric(z[z.year.between(2022,2023)]),'test':metric(z[z.year>=2024])};oos=gates(parts['train'],200) and gates(parts['validation'],100) and gates(parts['test'],100);root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);z.to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'changed_axis':'leaf selected conditionally by fixed stock realized-volatility regime only','regimes':{'low':'vol20<2%','medium':'2%<=vol20<4%','high':'vol20>=4%'},'local_train_gate':'n>=50 expectancy>0 PF>=1.2 win>=.45','entry_exit':'next open gap<=0, TP8%, SL5%, H10','capital_allocation':'not used','selection':'2019-2021 only'},'train_regime_leaf_diagnostics':diag,'selected_mapping':mapping,'selected_metrics_by_split':parts,'oos_gate_pass':oos,'decision':{'candidate_local_decision':'keep_for_reproducibility' if oos else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
