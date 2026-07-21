"""Monthly-only selection state carried forward for daily entry evaluation."""
import argparse,hashlib,json
from pathlib import Path
import numpy as np,pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--confirmed',required=True);p.add_argument('--overlay',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 c=pd.read_parquet(a.confirmed);c.code=c.code.astype(str).str.zfill(4);c.effective_month=c.effective_month.astype(str)
 o=pd.read_parquet(a.overlay);o.code=o.code.astype(str).str.zfill(4);o['effective_month']=o.decision_month.astype(str);x=o.merge(c,on=['code','effective_month'],how='left',suffixes=('','_confirmed'),validate='many_to_one')
 high_fail=(x.monthly_ret12.ge(.15)&x.monthly_pos24.ge(.60)&x.c.lt(x.ma7m)&x.above_ma20m_run.ge(12)&x.monthly_close_pos.le(.50))
 post_fail=(x.base_regime.eq('POST_BOX_BREAKOUT_CONSOLIDATION')&x.current_local_box_position.between(.55,.85)&x.current_month_close_position.le(.40)&x.current_month_body_pct.lt(0))
 box_upper=(x.base_regime.eq('BOX')&x.local_box_mature.fillna(False)&x.current_local_box_position.ge(.55))
 x['new_entry_blocked_reason']=np.select(
  [x.current_below_local_box.fillna(False),x.current_local_box_position.lt(.35)],
  ['BELOW_LOCAL_BOX','LOCAL_BOX_LOWER_OR_BOTTOM'],default=None)
 new_entry_allowed=x.new_entry_blocked_reason.isna()
 x['selection_event']=np.select(
  [new_entry_allowed&high_fail,new_entry_allowed&post_fail,new_entry_allowed&box_upper],
  ['HIGH_ZONE_FAILURE','POST_BOX_RETURN_SELL','MATURE_BOX_UPPER'],default=None)
 x['new_entry_eligible']=x.selection_event.notna()
 active=[];age=[]
 for _,g in x.groupby('code',sort=False):
  cur=None;n=999
  for r in g.sort_values('ymd').itertuples():
   if r.selection_event is not None:cur=r.selection_event;n=0
   elif n<20:n+=1
   else:cur=None;n=999
   active.append((r.Index,cur,None if n==999 else n))
 aidx=pd.DataFrame(active,columns=['idx','monthly_selection_state','selection_age_sessions']).set_index('idx');x=x.join(aidx)
 x['management_state_valid']=x.monthly_selection_state.notna()
 out=x[['code','ymd','source_month','effective_month','base_regime','confirmed_environment','selection_event','new_entry_eligible','new_entry_blocked_reason','monthly_selection_state','selection_age_sessions','management_state_valid','current_local_box_position','current_month_body_pct','current_month_close_position','monthly_ret12','monthly_pos24','ma7m','ma20m','ma60m','above_ma20m_run','above_ma60m_run']].copy()
 teachers={}
 for code,ymd in [('9107',20241121),('7733',20260119),('7733',20260210),('7733',20260213),('3405',20260618),('3405',20260630),('4208',20260514),('7004',20260317),('7004',20260319),('9531',20260603)]:teachers[f'{code}:{ymd}']=json.loads(out[(out.code==code)&(out.ymd==ymd)].to_json(orient='records'))
 data={'schema_version':'tradex_monthly_selection_state_daily_v1.compare.v2','artifact_role':'authoritative_challenger','review_only':True,'fixed_conditions':{'selection_inputs':'confirmed monthly features plus month-to-date OHLC truncated at decision date','states':['HIGH_ZONE_FAILURE','POST_BOX_RETURN_SELL','MATURE_BOX_UPPER'],'carry':'20 trading sessions after latest eligible monthly selection event','new_entry_policy':'lower/bottom or below-box blocks new selection; an already latched episode remains valid for management','weekly_inputs':[],'daily_entry_features':[]},'teacher_checks':teachers,'summary':{'rows':len(out),'event_counts':out.selection_event.value_counts().to_dict(),'active_counts':out.monthly_selection_state.value_counts().to_dict(),'new_entry_block_counts':out.new_entry_blocked_reason.value_counts().to_dict()},'judgment':{'decision':'hold_pending_daily_entry_join'},'not_changed':['daily entry rules','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8');out.to_parquet(a.output/'monthly_selection_state_daily.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'rows':len(out),'duplicates':int(out.duplicated(['code','ymd']).sum()),'weekly_columns_used':[],'future_columns_used':[],'confirmed_sha256':sha(a.confirmed),'overlay_sha256':sha(a.overlay)},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps({'output':str(a.output),'summary':data['summary'],'teachers':teachers},ensure_ascii=False))
if __name__=='__main__':main()
