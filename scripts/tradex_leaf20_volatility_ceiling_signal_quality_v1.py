from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric,gates
AXIS_ID='leaf20_volatility_ceiling_signal_quality_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf20_volatility_ceiling_signal_quality_v1');CEILINGS=(.015,.02,.025,.03,.04)
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);x=x[x.shape_leaf==20].copy();x['code']=x.code.astype(str);x['ret']=x.next_open_return
 q="""WITH b AS(SELECT code,date,c/lag(c) over(partition by code order by date)-1 r FROM daily_bars WHERE source='pan') SELECT code,date,stddev_samp(r) over(partition by code order by date rows between 19 preceding and current row) vol20 FROM b"""
 with duckdb.connect(str(DB),read_only=True) as c:v=c.execute(q).fetchdf();v['code']=v.code.astype(str);x=x.merge(v,on=['code','date'],how='left');rows=[];sets={}
 for c in CEILINGS:
  z=x[x.vol20<c];sets[c]=z;m={'train':metric(z[z.year<=2021]),'validation':metric(z[z.year.between(2022,2023)]),'test':metric(z[z.year>=2024])};rows.append({'vol20_ceiling':c,'metrics_by_split':m,'train_gate_pass':gates(m['train'],200)})
 e=[r for r in rows if r['train_gate_pass']];ch=max(e,key=lambda r:r['metrics_by_split']['train']['geometric_mean']) if e else None;c=ch['vol20_ceiling'] if ch else None;oos=bool(ch and gates(ch['metrics_by_split']['validation'],100) and gates(ch['metrics_by_split']['test'],100));root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);(sets[c] if c else x.iloc[:0]).to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'leaf_subset':[20],'changed_axis':'stock vol20 ceiling only','ceilings':CEILINGS,'entry_exit':'next open gap<=0 TP8 SL5 H10','capital_allocation':'not used','selection':'2019-2021 mean log return among hard gates'},'variants':rows,'selection':{'selected':ch},'oos_gate_pass':oos,'decision':{'candidate_local_decision':'keep' if oos else 'hold_insufficient_sample' if any(r['metrics_by_split']['test']['profit_factor'] and r['metrics_by_split']['test']['profit_factor']>=1.3 for r in rows) else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
