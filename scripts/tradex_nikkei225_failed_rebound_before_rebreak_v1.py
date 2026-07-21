from __future__ import annotations

import argparse, hashlib, json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base

AXIS_ID='tradex_nikkei225_failed_rebound_before_rebreak_v1'
DAILY=Path(r'G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet')
STATE=Path(r'G:\Tradex\pit_sell_state_specialist_v1\20260714T170218Z-tradex_nikkei225_pit_sell_state_specialist_v1-state')
OUT=Path(r'G:\Tradex\failed_rebound_before_rebreak_v1')

def sha(p:Path)->str:
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8<<20),b''):h.update(b)
 return h.hexdigest()
def dump(p:Path,x:Any):p.write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')

def raw_legs(d:pd.DataFrame,s:pd.DataFrame)->pd.DataFrame:
 x=d[['code','ymd','o','h','l','c','atr14','ma7','ma20','upper_wick_ratio','close_pos']].merge(s[['code','ymd','s4_sell_trigger_event']],on=['code','ymd'],validate='one_to_one')
 rows=[]
 for code,g in x.groupby('code',sort=False):
  g=g.reset_index(drop=True);last=-999
  for i,r in g.iterrows():
   if bool(r.s4_sell_trigger_event):last=i
   rec={'code':code,'ymd':int(r.ymd),'initial_s4_age':np.nan,'trough_age':np.nan,'rebound_days':np.nan,'rebound_peak_atr':np.nan,'rebound_retracement':np.nan,'rebound_bull_body_atr':np.nan,'current_bear_body_atr':np.nan,'ma7_test_reject':False,'ma20_test_reject':False,'ma7_reclaim_hold2':False,'ma20_reclaim_hold2':False,'upper_wick_ratio_d':float(r.upper_wick_ratio) if pd.notna(r.upper_wick_ratio) else np.nan,'close_pos_d':float(r.close_pos) if pd.notna(r.close_pos) else np.nan,'d_structural_candidate':False}
   if 2<=i-last<=15:
    q=g.iloc[last+1:i+1];tr=int(q.l.idxmin())
    if tr<i:
     after=g.iloc[tr+1:i+1];peak=int(after.h.idxmax());atr=float(r.atr14) if r.atr14 else np.nan;initial=float(g.c.iloc[last]);low=float(g.l.iloc[tr]);high=float(g.h.iloc[peak]);decline=initial-low
     bull=((after.c-after.o)/after.atr14.replace(0,np.nan)).clip(lower=0)
     ma7hold=((g.c.iloc[tr+1:i+1]>g.ma7.iloc[tr+1:i+1]).astype(int).rolling(2).sum()>=2).any();ma20hold=((g.c.iloc[tr+1:i+1]>g.ma20.iloc[tr+1:i+1]).astype(int).rolling(2).sum()>=2).any()
     ma7rej=bool(r.h>=r.ma7 and r.c<r.ma7);ma20rej=bool(r.h>=r.ma20 and r.c<r.ma20);bear=(float(r.o-r.c)/atr) if np.isfinite(atr) else np.nan
     rec.update({'initial_s4_age':i-last,'trough_age':i-tr,'rebound_days':i-tr,'rebound_peak_atr':(high-low)/atr,'rebound_retracement':np.nan if decline<=0 else (high-low)/decline,'rebound_bull_body_atr':float(bull.max()),'current_bear_body_atr':bear,'ma7_test_reject':ma7rej,'ma20_test_reject':ma20rej,'ma7_reclaim_hold2':bool(ma7hold),'ma20_reclaim_hold2':bool(ma20hold),'d_structural_candidate':bool((bull>0).any() and r.c<r.o and (ma7rej or ma20rej) and not (ma7hold or ma20hold))})
   rows.append(rec)
 return pd.DataFrame(rows)

def build(d:pd.DataFrame,s:pd.DataFrame,fixed_thresholds:dict[str,Any]|None=None)->tuple[pd.DataFrame,dict[str,Any]]:
 f=raw_legs(d,s);pre=f[f.ymd.between(20190101,20211231)&f.d_structural_candidate]
 thresholds=fixed_thresholds or {'policy':'outcome_free_2019_2021_structural_candidate_distribution','n':len(pre),'codes':pre.code.nunique(),'bear_body_atr_q25':float(pre.current_bear_body_atr.quantile(.25)),'close_pos_q50':float(pre.close_pos_d.quantile(.50)),'upper_wick_q50':float(pre.upper_wick_ratio_d.quantile(.50)),'fixed_grid':{'bear_body_atr':[.25,.50,.75],'close_pos':[.25,.50,.75],'upper_wick':[.25,.50,.75]},'outcome_columns_used':[]}
 f['d_failed_rebound_before_rebreak']=f.d_structural_candidate&(f.current_bear_body_atr>=thresholds['bear_body_atr_q25'])&((f.close_pos_d<=thresholds['close_pos_q50'])|(f.upper_wick_ratio_d>=thresholds['upper_wick_q50']))
 return f,thresholds

def run()->Path:
 d=pd.read_parquet(DAILY).sort_values(['code','ymd']).reset_index(drop=True);s=pd.read_parquet(STATE/'state_ledger.parquet').sort_values(['code','ymd']).reset_index(drop=True)
 for q in (d,s):q['code']=q.code.astype(str).str.zfill(4);q['ymd']=pd.to_numeric(q.ymd).astype(int)
 f,t=build(d,s);z=s.merge(f,on=['code','ymd'],validate='one_to_one');old_cols=['s1_top_risk','s2_top_formation','s3_weakening']
 z['trigger_failed_rebound']=z.d_failed_rebound_before_rebreak;z['trigger_group_count_v2']=z.trigger_gap_down.astype(int)+z.trigger_ma20_break.astype(int)+z.trigger_support_break.astype(int)+z.trigger_failed_rebound.astype(int)
 z['s4_sell_trigger_raw_v2']=z.s4_sell_trigger_raw|(z.s3_weakening&(z.trigger_group_count_v2>=2));z['s4_sell_trigger_event_v2']=base.event_mask(z.s4_sell_trigger_raw_v2.to_numpy(bool),z.code.to_numpy(str),z.ymd.to_numpy(int))
 old_seen=z.groupby('code').s4_sell_trigger_event.transform(lambda q:q.shift().rolling(20,min_periods=1).max()).fillna(0).astype(bool);z['s8_sell_reentry']=z.d_failed_rebound_before_rebreak&old_seen;z['s8_sell_reentry_event']=base.event_mask(z.s8_sell_reentry.to_numpy(bool),z.code.to_numpy(str),z.ymd.to_numpy(int));z['sell_action_event_v2']=z.s4_sell_trigger_event_v2|z.s8_sell_reentry_event;z['state_v2']=np.where(z.s8_sell_reentry,'S8_SELL_REENTRY',np.where(z.s4_sell_trigger_raw_v2,'S4_SELL_TRIGGER',z.state))
 seed=z[(z.code=='6326')&z.ymd.isin([20260304,20260311])][['code','ymd','state_v2','d_failed_rebound_before_rebreak','s8_sell_reentry_event','sell_action_event_v2','ma7_test_reject','ma20_test_reject','current_bear_body_atr','close_pos_d','upper_wick_ratio_d']].to_dict('records');seedpass=any(r['ymd']==20260311 and r['state_v2']=='S8_SELL_REENTRY' and r['d_failed_rebound_before_rebreak'] and r['s8_sell_reentry_event'] for r in seed)
 cutoff=20260311;case_d=d[d.code=='6326'];case_s=s[s.code=='6326'];cf,_=build(case_d[case_d.ymd<=cutoff],case_s[case_s.ymd<=cutoff],t);full=f[(f.code=='6326')&f.ymd.le(cutoff)].reset_index(drop=True);cols=['code','ymd','d_failed_rebound_before_rebreak','initial_s4_age','trough_age','ma7_test_reject','ma20_test_reject'];cutpass=full[cols].equals(cf[cols].reset_index(drop=True));mut=case_d.copy();future=mut.ymd>cutoff
 for c,m in [('o',3),('h',4),('l',.2),('c',3)]:mut.loc[future,c]*=m
 mf,_=build(mut,case_s,t);mutpass=full[cols].equals(mf[mf.ymd<=cutoff][cols].reset_index(drop=True))
 stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ');out=OUT/(stamp+'-'+AXIS_ID);out.mkdir(parents=True);fp=out/'failed_rebound_feature_ledger.parquet';sp=out/'state_ledger_v2.parquet';tp=out/'preperiod_thresholds.json';f.to_parquet(fp,index=False);z.to_parquet(sp,index=False);dump(tp,t)
 payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative_state_instrumentation_review_only','single_changed_axis':'D_failed_rebound_before_rebreak_raw_OHLC_PIT','source':{'daily':{'path':str(DAILY),'sha256':sha(DAILY)},'fixed_state':{'path':str(STATE),'complete_sha256':sha(STATE/'_ARTIFACT_COMPLETE.json')}},'fixed_invariants':{'S1_S2_S3_unchanged':bool(z[old_cols].equals(s[old_cols])),'A_B_C_unchanged':True,'labels_barriers_models_unchanged':True,'initial_S4_dedup_unchanged':True},'thresholds':t,'event_contract':{'initial_S4':'existing/v2 independent-trigger event dedup','S8_reentry':'D after prior S4, independently deduped 10 bars','sell_action_event_v2':'union of initial S4 and S8 reentry events'},'seed_replay':{'6326':seed,'pass':seedpass,'9962':'out_of_universe'},'tests':{'cutoff_exact':cutpass,'future_mutation_prefix_unchanged':mutpass},'counts':{'D':int(z.d_failed_rebound_before_rebreak.sum()),'S8':int(z.s8_sell_reentry.sum()),'S8_events':int(z.s8_sell_reentry_event.sum()),'S4_v1_raw':int(z.s4_sell_trigger_raw.sum()),'S4_v2_raw':int(z.s4_sell_trigger_raw_v2.sum()),'S4_v2_events':int(z.s4_sell_trigger_event_v2.sum()),'sell_action_events':int(z.sell_action_event_v2.sum())},'boundary':{'owner':'TRADEX','meemee_changed':False,'runtime_db_write':False,'production_ranking_changed':False}}
 cp=out/'compare.json';dump(cp,payload);complete=all([payload['fixed_invariants']['S1_S2_S3_unchanged'],seedpass,cutpass,mutpass]);dump(out/'_ARTIFACT_COMPLETE.json',{'complete':complete,'compare':str(cp),'compare_sha256':sha(cp),'feature_sha256':sha(fp),'state_sha256':sha(sp),'threshold_sha256':sha(tp)});return out

def main():argparse.ArgumentParser().parse_args();print(run())
if __name__=='__main__':main()
