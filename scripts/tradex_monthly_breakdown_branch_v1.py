"""Independent BREAKDOWN CORE branch under a monthly-only selected episode."""
import argparse,hashlib,json
from pathlib import Path
import pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--daily',required=True);ap.add_argument('--monthly-state',required=True);ap.add_argument('--output',type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 cols=['code','ymd','o','c','ma20','body_ratio','close_pos','support_break','support_break_depth_atr','oversold_risk','pos20']
 d=pd.read_parquet(a.daily,columns=cols);d.code=d.code.astype(str).str.zfill(4)
 mc=['code','ymd','monthly_selection_state','selection_age_sessions','new_entry_blocked_reason','management_state_valid'];m=pd.read_parquet(a.monthly_state,columns=mc);m.code=m.code.astype(str).str.zfill(4)
 z=d.merge(m,on=['code','ymd'],how='inner',validate='one_to_one')
 z['candidate']=z.management_state_valid.fillna(False)&z.selection_age_sessions.le(5)&z.new_entry_blocked_reason.isna()&z.support_break.eq(1)&z.c.lt(z.o)&z.body_ratio.ge(.55)&z.close_pos.le(.20)&z.c.lt(z.ma20)&~z.oversold_risk.fillna(0).astype(bool)
 q=z[z.candidate].copy();actions=q[['code','ymd','monthly_selection_state']].rename(columns={'monthly_selection_state':'monthly_state'});actions['action']='CORE';actions['reason']='breakdown_support_close'
 actions=actions[['code','ymd','action','reason','monthly_state']]
 data={'schema_version':'tradex_monthly_breakdown_branch_v1.compare.v1','artifact_role':'authoritative_challenger','review_only':True,'axis':'BREAKDOWN branch generation only','fixed_conditions':{'monthly':'active monthly-only episode age<=5 and no lower-zone entry block','daily':'support_break, bearish body>=55%, close_pos<=0.20, close<MA20, oversold_risk=false','execution_evaluation':'separate next-open artifact','weekly_inputs':[],'future_inputs':[]},'observed_branching':{'candidate_rows':len(actions),'codes':int(actions.code.nunique()),'by_year':(actions.ymd//10000).value_counts().sort_index().to_dict(),'by_monthly_state':actions.monthly_state.value_counts().to_dict()},'teacher_checks':{'avoids_4208_20260514':not bool(((actions.code=='4208')&(actions.ymd==20260514)).any()),'avoids_7004_20260317':not bool(((actions.code=='7004')&(actions.ymd==20260317)).any()),'avoids_9531_20260603':not bool(((actions.code=='9531')&(actions.ymd==20260603)).any())},'judgment':{'decision':'hold_pending_same_condition_outcome'},'not_changed':['TOP_FAILURE','RETURN_SELL','monthly selection','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');actions.to_parquet(a.output/'breakdown_action_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'duplicates':int(actions.duplicated(['code','ymd','action']).sum()),'weekly_columns_used':[],'future_columns_used':[],'daily_sha256':sha(a.daily),'monthly_state_sha256':sha(a.monthly_state)},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps(data,ensure_ascii=False))
if __name__=='__main__':main()
