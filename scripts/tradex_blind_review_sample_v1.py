"""Freeze a future-blind, stratified review board for the fixed sell model."""
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

EXCLUDED={"9107","7733","3405","4208","7004","9531","4188","5631"}
SEED="monthly-daily-sell-blind-v1"
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def rank(code,ymd,bucket):return hashlib.sha256(f"{SEED}|{bucket}|{code}|{ymd}".encode()).hexdigest()
def choose(x,bucket,n,used,year=None):
 x=x[~x.code.isin(used)].copy()
 if year is not None:x=x[x.ymd//10000==year]
 x['event_hash']=[rank(c,y,f'{bucket}:EVENT') for c,y in zip(x.code,x.ymd)]
 x=x.sort_values(['event_hash','code','ymd']).drop_duplicates('code')
 x['selection_hash']=[hashlib.sha256(f'{SEED}|{bucket}:CODE|{c}'.encode()).hexdigest() for c in x.code]
 x=x.sort_values(['selection_hash','code']).head(n);used.update(x.code);return x
def choose_quota(x,bucket,quotas,year_plan,used):
 parts=[];remaining=dict(quotas)
 for year,action in year_plan:
  hit=choose(x[x.model_action.eq(action)],f'{bucket}:{action}:Y{year}',1,used,year)
  if len(hit)!=1:raise RuntimeError(f'no candidate for {bucket} {year} {action}')
  parts.append(hit);remaining[action]-=1
 for action,n in remaining.items():
  if n>0:parts.append(choose(x[x.model_action.eq(action)],f'{bucket}:{action}:REST',n,used))
 out=pd.concat(parts,ignore_index=True)
 if len(out)!=sum(quotas.values()):raise RuntimeError(f'quota shortfall {bucket}')
 return out
def choose_year_stratified(x,bucket,n,used):
 parts=[]
 for year in range(2020,2026):
  hit=choose(x,f'{bucket}:Y{year}',1,used,year)
  if len(hit)!=1:raise RuntimeError(f'no candidate for {bucket} {year}')
  parts.append(hit)
 parts.append(choose(x,f'{bucket}:REST',n-6,used));return pd.concat(parts,ignore_index=True)
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--actions',required=True);ap.add_argument('--breakdown',required=True);ap.add_argument('--monthly-state',required=True);ap.add_argument('--daily',required=True);ap.add_argument('--per-bucket',type=int,default=10);ap.add_argument('--output',type=Path,required=True);ap.add_argument('--sealed-output',type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False);a.sealed_output.mkdir(parents=True,exist_ok=False)
 actions=pd.read_parquet(a.actions);actions.code=actions.code.astype(str).str.zfill(4);actions=actions[(actions.ymd//10000).between(2020,2025)&~actions.code.isin(EXCLUDED)]
 top=actions[(actions.monthly_state.isin(['HIGH_ZONE_FAILURE','MATURE_BOX_UPPER']))&actions.action.isin(['PROBE','CORE','ADD'])].copy();top['bucket']='TOP_FAILURE';top['model_action']=top.action
 ret=actions[(actions.monthly_state.eq('POST_BOX_RETURN_SELL')|actions.reason.isin(['return_sell_cross_ma7_ma20','rebound_to_ma20_after_exhaustion','eight_below_ma7_with_bottoming_positive_candle']))].copy();ret['bucket']='RETURN_SELL';ret['model_action']=ret.action
 br=pd.read_parquet(a.breakdown);br.code=br.code.astype(str).str.zfill(4);br=br[(br.ymd//10000).between(2020,2025)&~br.code.isin(EXCLUDED)].copy();br['bucket']='BREAKDOWN_REJECTED';br['model_action']='AVOID';br['reason']='breakdown_branch_dropped';br['monthly_state']=br.monthly_state
 mcols=['code','ymd','source_month','effective_month','base_regime','confirmed_environment','monthly_selection_state','selection_age_sessions','new_entry_blocked_reason','current_local_box_position','current_month_body_pct','current_month_close_position']
 m=pd.read_parquet(a.monthly_state,columns=mcols);m.code=m.code.astype(str).str.zfill(4)
 dcols=['code','ymd','o','h','l','c','ma7','ma20','ma60','ma100','ma200','body_ratio','upper_wick_ratio','lower_wick_ratio','close_pos','ret5','ret10','pos20','support_break','oversold_risk','dist_ma7_atr','dist_ma20_atr','dist_ma60_atr']
 d=pd.read_parquet(a.daily,columns=dcols);d.code=d.code.astype(str).str.zfill(4)
 avoid=d.merge(m,on=['code','ymd'],how='inner',validate='one_to_one');avoid=avoid[(avoid.ymd//10000).between(2020,2025)&~avoid.code.isin(EXCLUDED)&avoid.new_entry_blocked_reason.notna()&avoid.c.lt(avoid.o)&avoid.body_ratio.ge(.45)&avoid.close_pos.le(.35)].copy();avoid['bucket']='TEMPTING_AVOID';avoid['model_action']='AVOID';avoid['action']='AVOID';avoid['reason']=avoid.new_entry_blocked_reason;avoid['monthly_state']=avoid.monthly_selection_state
 used=set();parts=[
  choose_quota(top,'TOP_FAILURE',{'PROBE':5,'CORE':3,'ADD':2},[(2020,'PROBE'),(2021,'ADD'),(2022,'ADD'),(2023,'PROBE'),(2024,'CORE'),(2025,'PROBE')],used),
  choose_quota(ret,'RETURN_SELL',{'PROBE':4,'CORE':2,'REENTRY_PROBE':2,'TAKE_PROFIT_FULL_HEDGE':2},[(2020,'CORE'),(2021,'REENTRY_PROBE'),(2022,'TAKE_PROFIT_FULL_HEDGE'),(2023,'PROBE'),(2024,'CORE'),(2025,'PROBE')],used),
  choose_year_stratified(br,'BREAKDOWN_REJECTED',a.per_bucket,used),
  choose_year_stratified(avoid,'TEMPTING_AVOID',a.per_bucket,used),
 ]
 keys=pd.concat(parts,ignore_index=True);keys['display_hash']=[hashlib.sha256(f'{SEED}|DISPLAY|{c}|{y}'.encode()).hexdigest() for c,y in zip(keys.code,keys.ymd)];keys=keys.sort_values('display_hash').reset_index(drop=True);keys.insert(0,'case_id',[f'B{i:02d}' for i in range(1,len(keys)+1)])
 sealed=keys[['case_id','code','ymd','bucket','model_action','reason','monthly_state','event_hash','selection_hash','display_hash']].copy();sealed['source_family']=sealed.bucket;sealed['contract_version']='v2';sealed['outcome_joined']=False
 ctx=d.merge(m,on=['code','ymd'],how='inner',validate='one_to_one');board=keys[['case_id','code','ymd','display_hash']].merge(ctx,on=['code','ymd'],how='left',validate='one_to_one');board=board.drop(columns=['monthly_selection_state','new_entry_blocked_reason']);board['chart_cutoff_ymd']=board.ymd;board['max_daily_ymd_used']=board.ymd;board['outcome_joined']=False;board['new_entry_decision']=None;board['existing_short_management']=None;board['entry_stage']=None;board['confidence']=None;board['reason_codes']=None;board['reviewer_note']=None;board['reviewed_at']=None
 forbidden=[c for c in board.columns if c.startswith(('ret_close_','down_exc_','up_exc_','weekly_'))]
 if forbidden:raise RuntimeError(f'forbidden columns: {forbidden}')
 board.to_parquet(a.output/'blind_review_board.parquet',index=False);board.to_csv(a.output/'blind_review_board.csv',index=False,encoding='utf-8-sig');sealed.to_parquet(a.sealed_output/'machine_annotation_sealed.parquet',index=False)
 bucket_counts=sealed.bucket.value_counts().to_dict();action_counts=sealed.model_action.value_counts().to_dict();years_by_bucket={b:sorted((g.ymd//10000).unique().tolist()) for b,g in sealed.groupby('bucket')};gates={'bucket_10_each':all(bucket_counts.get(b)==a.per_bucket for b in ['TOP_FAILURE','RETURN_SELL','BREAKDOWN_REJECTED','TEMPTING_AVOID']),'rows_40':len(board)==4*a.per_bucket,'unique_codes_40':board.code.nunique()==len(board),'excluded_hits_0':int(board.code.isin(EXCLUDED).sum())==0,'duplicate_code_ymd_0':int(board.duplicated(['code','ymd']).sum())==0,'all_years_each_bucket':all(v==list(range(2020,2026)) for v in years_by_bucket.values()),'future_columns_0':len(forbidden)==0,'outcome_joined_false':not bool(board.outcome_joined.any())}
 if not all(gates.values()):raise RuntimeError(f'blind sample gates failed: {gates}')
 display_order_payload='|'.join(f'{r.case_id}|{r.code}|{int(r.ymd)}|{r.display_hash}' for r in board.itertuples())
 manifest={'schema_version':'tradex_blind_review_sample_v1.compare.v3','artifact_role':'authoritative_blind_sample','review_only':True,'status':'frozen_before_human_annotation_and_outcome_reveal','evaluation_name':'outcome-blind human chart review of frozen historical candidates','not_clean_oos_reason':'2020-2025 participated in prior branch research','fixed_conditions':{'years':[2020,2021,2022,2023,2024,2025],'excluded_codes':sorted(EXCLUDED),'seed':SEED,'per_bucket':a.per_bucket,'one_case_per_code':True,'code_before_event_weighting':True,'one_case_per_year_per_bucket_minimum':True,'selection_uses_future_outcomes':False,'machine_annotation_sealed':True,'sealed_annotation_not_in_reviewer_bundle':True,'breakdown_role':'diagnostic_negative_control_excluded_from_accuracy_denominator','weekly_inputs':[]},'bucket_counts':bucket_counts,'action_counts':action_counts,'years_by_bucket':years_by_bucket,'display_order_sha256':hashlib.sha256(display_order_payload.encode()).hexdigest(),'case_ids':board[['case_id','code','ymd']].to_dict(orient='records'),'gates':gates,'judgment':{'decision':'keep_frozen_sample_pending_human_review'},'not_changed':['fixed model rules','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');audit={'rows':len(board),'unique_codes':int(board.code.nunique()),'duplicate_code_ymd':int(board.duplicated(['code','ymd']).sum()),'excluded_hits':int(board.code.isin(EXCLUDED).sum()),'forbidden_columns':forbidden,'outcome_columns_present':False,'gates':gates,'board_sha256':sha(a.output/'blind_review_board.parquet'),'sealed_annotation_sha256':sha(a.sealed_output/'machine_annotation_sealed.parquet'),'source_paths':{'actions':str(Path(a.actions).resolve()),'breakdown':str(Path(a.breakdown).resolve()),'monthly_state':str(Path(a.monthly_state).resolve()),'daily':str(Path(a.daily).resolve())},'actions_sha256':sha(a.actions),'breakdown_sha256':sha(a.breakdown),'monthly_state_sha256':sha(a.monthly_state),'daily_sha256':sha(a.daily)};(a.output/'audit.json').write_text(json.dumps(audit,indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');(a.sealed_output/'seal_audit.json').write_text(json.dumps({'reviewer_bundle':str(a.output.resolve()),'sealed_annotation_sha256':audit['sealed_annotation_sha256'],'outcome_joined':False},indent=2)+'\n');print(json.dumps({'output':str(a.output),'sealed_output':str(a.sealed_output),'bucket_counts':bucket_counts,'action_counts':action_counts,'rows':len(board),'unique_codes':int(board.code.nunique()),'gates':gates},ensure_ascii=False))
if __name__=='__main__':main()
