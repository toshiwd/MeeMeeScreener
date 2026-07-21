from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric,gates
AXIS_ID='leaf_breadth_regime_mapping_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf_breadth_regime_mapping_v1');LEAVES=(9,14,20);REGIMES=('weak','neutral','strong')
def regime(v):return 'weak' if v<.4 else 'neutral' if v<.6 else 'strong'
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);x['ret']=x.next_open_return
 q="""WITH b AS(SELECT code,date,c,avg(c) over(partition by code order by date rows between 19 preceding and current row) ma20 FROM daily_bars WHERE source='pan') SELECT date,avg(case when c>ma20 then 1.0 else 0.0 end) breadth FROM b GROUP BY date"""
 with duckdb.connect(str(DB),read_only=True) as c:b=c.execute(q).fetchdf()
 x=x.merge(b,on='date',how='left');x['breadth_regime']=x.breadth.map(regime);train=x[x.year<=2021];mapping={};diagnostics=[]
 for rg in REGIMES:
  candidates=[]
  for leaf in LEAVES:
   z=train[(train.breadth_regime==rg)&(train.shape_leaf==leaf)];m=metric(z);eligible=m['n']>=50 and m['expectancy']>0 and m['profit_factor']>=1.2 and m['win_rate']>=.45;candidates.append({'regime':rg,'leaf':leaf,'train_metrics':m,'local_gate_pass':eligible})
  diagnostics.extend(candidates);valid=[r for r in candidates if r['local_gate_pass']];mapping[rg]=max(valid,key=lambda r:r['train_metrics']['geometric_mean'])['leaf'] if valid else None
 selected=x[x.apply(lambda r:mapping.get(r.breadth_regime)==r.shape_leaf,axis=1)].copy();parts={'train':metric(selected[selected.year<=2021]),'validation':metric(selected[selected.year.between(2022,2023)]),'test':metric(selected[selected.year>=2024])};oos=gates(parts['train'],200) and gates(parts['validation'],100) and gates(parts['test'],100)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);selected.to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'changed_axis':'leaf selected conditionally by fixed PAN breadth regime only','regimes':{'weak':'breadth<0.4','neutral':'0.4<=breadth<0.6','strong':'breadth>=0.6'},'local_train_gate':'n>=50 expectancy>0 PF>=1.2 win>=.45','entry_exit':'next open gap<=0, TP8%, SL5%, H10','capital_allocation':'not used','selection':'2019-2021 only'},'train_regime_leaf_diagnostics':diagnostics,'selected_mapping':mapping,'selected_metrics_by_split':parts,'oos_gate_pass':oos,'decision':{'candidate_local_decision':'keep_for_reproducibility' if oos else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
