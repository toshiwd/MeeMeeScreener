"""Generate one continuation ADD signal after an accepted daily CORE shape."""
import argparse,hashlib,json
from pathlib import Path
import pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--actions',required=True);ap.add_argument('--daily',required=True);ap.add_argument('--output',type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 acts=pd.read_parquet(a.actions);acts.code=acts.code.astype(str).str.zfill(4);cores=acts[acts.action.eq('CORE')]
 cols=['code','ymd','o','c','ma60','body_ratio','close_pos','ret3','pos20'];d=pd.read_parquet(a.daily,columns=cols);d.code=d.code.astype(str).str.zfill(4);d=d.sort_values(['code','ymd']);hist={c:g.reset_index(drop=True) for c,g in d.groupby('code')}
 rows=[]
 for r in cores.sort_values(['code','ymd']).itertuples():
  g=hist[r.code];idx=g.index[g.ymd.eq(r.ymd)]
  if len(idx)!=1:continue
  for j in range(int(idx[0])+1,min(int(idx[0])+6,len(g))):
   z=g.iloc[j]
   if z.c<z.o and z.body_ratio>=.55 and z.close_pos<=.25 and z.c<z.ma60 and z.ret3<=-.03 and z.pos20<=.15:
    rows.append({'code':r.code,'ymd':int(z.ymd),'action':'CORE','reason':'ADD_CONTINUATION','monthly_state':r.monthly_state,'parent_core_ymd':int(r.ymd)});break
 q=pd.DataFrame(rows)
 if len(q):q=q.sort_values(['code','ymd','parent_core_ymd']).drop_duplicates(['code','ymd'],keep='first').reset_index(drop=True)
 out=q[['code','ymd','action','reason','monthly_state']] if len(q) else pd.DataFrame(columns=['code','ymd','action','reason','monthly_state'])
 teacher=bool(((q.code=='9107')&(q.ymd==20241126)).any()) if len(q) else False
 data={'schema_version':'tradex_core_add_continuation_v1.compare.v1','artifact_role':'authoritative_challenger','review_only':True,'axis':'ADD continuation generation only','fixed_conditions':{'window':'first qualifying session within five sessions after CORE','daily':'bearish body>=55%, close_pos<=0.25, close<MA60, ret3<=-3%, pos20<=0.15','execution_evaluation':'next session open in separate artifact','weekly_inputs':[],'future_inputs':[]},'teacher_checks':{'9107_add_signal_20241126':teacher,'9107_execution_expected_20241127':teacher},'observed_branching':{'parent_cores':len(cores),'add_signals':len(q),'codes':int(q.code.nunique()) if len(q) else 0,'by_year':(q.ymd//10000).value_counts().sort_index().to_dict() if len(q) else {}},'judgment':{'decision':'hold_pending_same_condition_outcome'},'not_changed':['PROBE','CORE generation','monthly selection','profit take','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');out.to_parquet(a.output/'add_action_ledger.parquet',index=False);q.to_parquet(a.output/'add_parent_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'duplicates':int(q.duplicated(['code','ymd']).sum()) if len(q) else 0,'weekly_columns_used':[],'future_columns_used':[],'actions_sha256':sha(a.actions),'daily_sha256':sha(a.daily)},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps(data,ensure_ascii=False))
if __name__=='__main__':main()
