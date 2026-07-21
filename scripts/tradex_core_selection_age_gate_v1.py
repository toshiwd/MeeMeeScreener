"""Single-axis gate: CORE requires a fresh monthly selection episode."""
import argparse,hashlib,json
from pathlib import Path
import pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--actions',required=True);ap.add_argument('--monthly-state',required=True);ap.add_argument('--max-age',type=int,default=5);ap.add_argument('--output',type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 x=pd.read_parquet(a.actions);x.code=x.code.astype(str).str.zfill(4);m=pd.read_parquet(a.monthly_state,columns=['code','ymd','selection_age_sessions']);m.code=m.code.astype(str).str.zfill(4)
 z=x.merge(m,on=['code','ymd'],how='left',validate='many_to_one');z['gate_pass']=~(z.action.eq('CORE')&(z.selection_age_sessions.isna()|z.selection_age_sessions.gt(a.max_age)))
 kept=z[z.gate_pass].drop(columns=['gate_pass','selection_age_sessions']);removed=z[~z.gate_pass]
 anchors={f'{c}:{ymd}':bool(((kept.code==c)&(kept.ymd==ymd)&kept.action.eq('CORE')).any()) for c,ymd in [('9107',20241122),('7733',20260213)]}
 data={'schema_version':'tradex_core_selection_age_gate_v1.compare.v1','artifact_role':'authoritative_challenger','review_only':True,'axis':'CORE monthly selection age only','fixed_conditions':{'gate':f'selection_age_sessions <= {a.max_age}','other_actions':'unchanged','weekly_inputs':[],'future_inputs':[]},'observed_branching':{'baseline_core':int(x.action.eq('CORE').sum()),'selected_core':int(kept.action.eq('CORE').sum()),'removed_core':len(removed),'removed_by_year':(removed.ymd//10000).value_counts().sort_index().to_dict(),'removed_2026':removed[removed.ymd//10000==2026][['code','ymd','selection_age_sessions']].to_dict(orient='records')},'teacher_checks':anchors,'judgment':{'decision':'hold_pending_same_condition_outcome','teachers_preserved':all(anchors.values())},'not_changed':['monthly selection','daily candle rules','PROBE','profit take','reentry','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');kept.to_parquet(a.output/'gated_action_ledger.parquet',index=False);removed.to_parquet(a.output/'removed_core_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'duplicates':int(kept.duplicated(['code','ymd','action']).sum()),'weekly_columns_used':[],'future_columns_used':[],'actions_sha256':sha(a.actions),'monthly_state_sha256':sha(a.monthly_state)},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps(data,ensure_ascii=False))
if __name__=='__main__':main()
