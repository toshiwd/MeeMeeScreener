from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric,gates
AXIS_ID='leaf_entry_gap_signal_quality_v1';OUT=Path(r'G:\Tradex\leaf_entry_gap_signal_quality_v1');CEILINGS=(-.02,-.01,0.0)
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);x['ret']=x.next_open_return;rows=[];sets={}
 for c in CEILINGS:
  z=x[x.next_open_gap<=c].copy();sets[c]=z;m={'train':metric(z[z.year<=2021]),'validation':metric(z[z.year.between(2022,2023)]),'test':metric(z[z.year>=2024])};rows.append({'maximum_next_open_gap':c,'metrics_by_split':m,'train_gate_pass':gates(m['train'],200)})
 e=[r for r in rows if r['train_gate_pass']];ch=max(e,key=lambda r:r['metrics_by_split']['train']['geometric_mean']) if e else None;c=ch['maximum_next_open_gap'] if ch else None;oos=bool(ch and gates(ch['metrics_by_split']['validation'],100) and gates(ch['metrics_by_split']['test'],100));root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);(sets[c] if c is not None else x.iloc[:0]).to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'changed_axis':'maximum next-open gap only','ceilings':CEILINGS,'leaf_subset':[9,14,20],'take_profit':.08,'stop_loss':.05,'hold':10,'capital_allocation':'not used','selection':'2019-2021 mean log return among hard gates'},'variants':rows,'selection':{'selected':ch},'oos_gate_pass':oos,'decision':{'candidate_local_decision':'keep_for_reproducibility' if oos else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
