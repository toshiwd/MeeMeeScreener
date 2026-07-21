from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric
AXIS_ID='leaf20_volatility_walkforward_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf20_volatility_walkforward_v1');CEILINGS=(.02,.025,.03,.04);YEARS=tuple(range(2019,2026))
def local_gate(m):return m['n']>=100 and m['expectancy']>0 and m['profit_factor']>=1.2 and m['win_rate']>=.45 and m['payoff_ratio']>=1.1 and m['maximum_loss']>=-.05
def final_gate(m):return m['n']>=300 and m['expectancy']>0 and m['profit_factor']>=1.3 and m['win_rate']>=.45 and m['payoff_ratio']>=1.2 and m['p05']>=-.05 and m['maximum_loss']>=-.05
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\prehistory_reference_events.csv'))[-1]);x=pd.read_csv(source);x=x[x.shape_leaf==20].copy();x['code']=x.code.astype(str);x['ret']=x.next_open_return
 q="""WITH b AS(SELECT code,date,c/lag(c) over(partition by code order by date)-1 r FROM daily_bars WHERE source='pan') SELECT code,date,stddev_samp(r) over(partition by code order by date rows between 19 preceding and current row) vol20 FROM b"""
 with duckdb.connect(str(DB),read_only=True) as c:v=c.execute(q).fetchdf();v['code']=v.code.astype(str);x=x.merge(v,on=['code','date'],how='left');decisions=[];parts=[]
 for year in YEARS:
  train=x[x.year.between(year-3,year-1)];variants=[]
  for ceiling in CEILINGS:
   m=metric(train[train.vol20<ceiling]);variants.append({'ceiling':ceiling,'metrics':m,'gate_pass':local_gate(m)})
  valid=[r for r in variants if r['gate_pass']];chosen=max(valid,key=lambda r:(r['metrics']['geometric_mean'],r['metrics']['expectancy'])) if valid else None;ceiling=chosen['ceiling'] if chosen else None;applied=x[(x.year==year)&(x.vol20<ceiling)].copy() if ceiling else x.iloc[:0].copy();parts.append(applied);decisions.append({'target_year':year,'training_years':[year-3,year-1],'variants':variants,'selected_ceiling':ceiling,'target_year_metrics':metric(applied)})
 selected=pd.concat(parts,ignore_index=True);overall=metric(selected);yearly={str(y):metric(selected[selected.year==y]) for y in YEARS};gate=final_gate(overall) and all(m['n']>=20 and m['expectancy']>0 for m in yearly.values());root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);selected.to_csv(root/'walkforward_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'leaf':20,'changed_axis':'annual vol20 ceiling re-estimation policy only','ceilings':CEILINGS,'training_window':'three complete prior calendar years','application':'selected ceiling frozen for next calendar year','entry_exit':'next open gap<=0 TP8 SL5 H10','capital_allocation':'not used','local_gate':'n>=100 expectancy>0 PF>=1.2 win>=.45 payoff>=1.1 maxloss>=-5%'},'annual_selection_decisions':decisions,'walkforward_metrics_2019_2025':overall,'yearly_metrics':yearly,'final_gate_pass':gate,'decision':{'candidate_local_decision':'keep_for_forward_monitoring' if gate else 'drop','authoritative_rollup_decision':'research_only','reason_type':'strict trailing-three-year walkforward with no future-year selection'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
