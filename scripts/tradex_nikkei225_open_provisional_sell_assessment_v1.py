from __future__ import annotations

import argparse, gc, hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base

AXIS_ID='tradex_nikkei225_open_provisional_sell_assessment_v1'
DAILY=Path(r'G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet')
STATE=Path(r'G:\Tradex\pit_sell_state_specialist_v1\20260714T170218Z-tradex_nikkei225_pit_sell_state_specialist_v1-state')
OUT=Path(r'G:\Tradex\open_provisional_sell_assessment_v1')
CLASS={0:'down_first',1:'rebound_first',2:'neutral'}

LABEL_CONTRACT={'decision_time':'official historical open at t; Yahoo live open must be labelled provisional','allowed_t_information':['open_t'],'confirmed_information':'all bars through t-1 only','barrier_fraction':'clip(multiplier*ATR14[t-1]/close[t-1],floor,cap), up=0.8*down','barrier_origin':'open_t','scan':'t same-day high/low then t+1 through t+h-1; earliest hit wins','same_day_both':'neutral_path_ambiguous','no_hit':'neutral_no_hit','target_change':'different origin and window from prior-close first-passage; no same-label paired improvement claim'}

def sha(p:Path)->str:
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8<<20),b''):h.update(b)
 return h.hexdigest()
def dump(p:Path,x:Any):p.write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')

def open_labels(frame:pd.DataFrame,horizon:int)->pd.DataFrame:
 m,lo,hi=base.HORIZONS[horizon];g=frame.groupby('code',sort=False);n=len(frame);o=frame.o.to_numpy(float);pc=g.c.shift().to_numpy(float);pa=g.atr14.shift().to_numpy(float);frac=np.clip(m*pa/pc,lo,hi);down=o*(1-frac);up=o*(1+.8*frac);lab=np.full(n,2,np.int8);kind=np.full(n,'neutral_no_hit',object);hit=np.zeros(n,np.int8);resolved=np.zeros(n,bool);valid=np.isfinite(o)&np.isfinite(pc)&np.isfinite(pa);available_all=valid.copy()
 for day in range(horizon):
  op=g.o.shift(-day).to_numpy(float);hh=g.h.shift(-day).to_numpy(float);ll=g.l.shift(-day).to_numpy(float);cc=g.c.shift(-day).to_numpy(float);avail=np.isfinite(op)&np.isfinite(hh)&np.isfinite(ll)&np.isfinite(cc);available_all&=avail;active=(~resolved)&avail;lowhit=active&(ll<=down);highhit=active&(hh>=up);both=lowhit&highhit;onlydown=lowhit&~highhit;onlyup=highhit&~lowhit
  for mask,value,name in ((both,2,'neutral_path_ambiguous'),(onlydown,0,'down_intraday'),(onlyup,1,'rebound_intraday')):
   lab[mask]=value;kind[mask]=name;hit[mask]=day+1;resolved[mask]=True
 endclose=g.c.shift(-(horizon-1)).to_numpy(float);ret=endclose/o-1;valid&=available_all;ret[~valid]=np.nan
 return pd.DataFrame({'code':frame.code.astype(str).to_numpy(),'ymd':frame.ymd.astype(int).to_numpy(),'horizon':horizon,'label_id':lab,'outcome_kind':kind,'hit_day':hit,'target_valid':valid,'ret_open_h':ret})

def self_tests()->dict[str,Any]:
 rows=[]
 cases=[(101,96,0),(103,98,1),(103,96,2),(101,99,2)]
 for k,(high,low,expected) in enumerate(cases):
  rows.extend([{'code':str(k),'ymd':1,'o':100.,'h':100.,'l':100.,'c':100.,'atr14':4.},{'code':str(k),'ymd':2,'o':100.,'h':high,'l':low,'c':100.,'atr14':4.}])
 f=pd.DataFrame(rows);q=open_labels(f,1).iloc[1::2].reset_index(drop=True);checks=[{'case':i,'expected':e,'got':int(q.label_id.iloc[i]),'pass':int(q.label_id.iloc[i])==e} for i,(*_,e) in enumerate(cases)];return {'status':'pass' if all(x['pass'] for x in checks) else 'fail','checks':checks}

def prepare()->tuple[pd.DataFrame,pd.DataFrame,dict[int,pd.Series],pd.DataFrame,list[str],dict[str,Any]]:
 d=pd.read_parquet(DAILY).sort_values(['code','ymd']).reset_index(drop=True);s=pd.read_parquet(STATE/'state_ledger.parquet').sort_values(['code','ymd']).reset_index(drop=True)
 for q in (d,s):q['code']=q.code.astype(str).str.zfill(4);q['ymd']=pd.to_numeric(q.ymd).astype(int)
 if not d[['code','ymd']].equals(s[['code','ymd']]):raise ValueError('state/daily key mismatch')
 labels={h:open_labels(d,h) for h in base.HORIZONS};keys=pd.MultiIndex.from_frame(d[['code','ymd']]);maps={h:pd.Series(q.label_id.to_numpy(),index=pd.MultiIndex.from_frame(q[['code','ymd']])) for h,q in labels.items()}
 g,x0=base.features(d);prior=x0.groupby(d.code,sort=False).shift(1);prior.columns=['prior_'+c for c in prior]
 grp=d.groupby('code',sort=False);pc=grp.c.shift();pa=grp.atr14.shift();openx=pd.DataFrame(index=d.index);openx['open_gap_pct']=d.o/pc-1;openx['open_gap_atr']=(d.o-pc)/pa
 for ma in ('ma7','ma20','ma60','ma100'):
  openx['open_dist_prior_'+ma+'_atr']=(d.o-grp[ma].shift())/pa
 openx['open_dist_prior_support_atr']=(d.o-grp.support20.shift())/pa
 for c in ('s1_top_risk','s2_top_formation','s3_weakening','s4_sell_trigger_event'):
  openx['prior_'+c]=s.groupby('code',sort=False)[c].shift().astype(float)
 features=pd.concat([prior,openx],axis=1).astype('float32');cohort=openx[['prior_s1_top_risk','prior_s2_top_formation','prior_s3_weakening']].fillna(0).any(axis=1)
 inp=d.loc[cohort].copy().reset_index(drop=True);fx=features.loc[cohort].reset_index(drop=True)
 for h,q in labels.items():
  qm=q.set_index(['code','ymd']).reindex(pd.MultiIndex.from_frame(inp[['code','ymd']])).reset_index(drop=True);bad=~qm.target_valid.to_numpy(bool);inp.loc[bad,f'ret_close_{h}']=np.nan;inp.loc[~bad,f'ret_close_{h}']=qm.loc[~bad,'ret_open_h'].to_numpy();inp.loc[bad,[f'down_exc_{h}',f'up_exc_{h}']]=np.nan;inp.loc[~bad,[f'down_exc_{h}',f'up_exc_{h}']]=0.
 audit={'rows_full':len(d),'rows_cohort':len(inp),'codes':inp.code.nunique(),'feature_count':features.shape[1],'prior_sequence_features':prior.shape[1],'open_features':list(openx),'forbidden_t_columns':['h','l','c','v'],'state_shifted_from_t_minus_1':True}
 return d,inp,maps,fx,list(features),audit

def run(out_root:Path,resume:Path|None)->Path:
 tests=self_tests()
 if tests['status']!='pass':raise ValueError(tests)
 full,inp,maps,fx,names,audit=prepare();root=resume or out_root/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+AXIS_ID);root.mkdir(parents=True,exist_ok=True);ip=root/'cohort_input.parquet';fpfeat=root/'feature_matrix.parquet'
 if not ip.exists():inp.to_parquet(ip,index=False);fx.to_parquet(fpfeat,index=False)
 key_to_row=pd.Series(np.arange(len(inp)),index=pd.MultiIndex.from_frame(inp[['code','ymd']]))
 oldf,oldl,olda=base.features,base.labels,base.AXIS_ID
 def features(frame):
  k=pd.MultiIndex.from_frame(frame[['code','ymd']]);idx=key_to_row.reindex(k)
  if idx.isna().any():raise ValueError('feature key lookup failed')
  return frame.copy(),fx.iloc[idx.astype(int).to_numpy()].reset_index(drop=True)
 def labels(frame,h):
  k=pd.MultiIndex.from_frame(frame[['code','ymd']]);v=maps[h].reindex(k)
  if v.isna().any():raise ValueError('label key lookup failed')
  return v.to_numpy(np.int8)
 try:
  base.features,base.labels,base.AXIS_ID=features,labels,'open_provisional_runner_v1';prior=sorted((root/'candidate').glob('*/compare.json')) if (root/'candidate').exists() else [];cand=prior[-1].parent if prior else base.run(ip,root/'candidate')
 finally:base.features,base.labels,base.AXIS_ID=oldf,oldl,olda
 candidate=json.loads((cand/'compare.json').read_text(encoding='utf-8'));led=[]
 for h,q in {h:open_labels(full,h) for h in base.HORIZONS}.items():led.append(q)
 ledger=pd.concat(led,ignore_index=True);lp=root/'open_origin_label_ledger.parquet';ledger.to_parquet(lp,index=False)
 seed=inp[(inp.code=='6326')&inp.ymd.eq(20260304)][['code','ymd','o']].copy();row=key_to_row.reindex(pd.MultiIndex.from_frame(seed[['code','ymd']]))
 seed_payload={'available':not seed.empty,'policy':'replay_only_not_training_or_selection'}
 if not seed.empty:
  vals=fx.iloc[int(row.iloc[0])];seed_payload.update({'open':float(seed.o.iloc[0]),'gap_pct':float(vals.open_gap_pct),'gap_atr':float(vals.open_gap_atr),'open_dist_prior_ma20_atr':float(vals.open_dist_prior_ma20_atr),'prior_S3':bool(vals.prior_s3_weakening)})
 payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','decision_time_contract':LABEL_CONTRACT,'source':{'daily':{'path':str(DAILY),'sha256':sha(DAILY)},'prior_state':{'path':str(STATE),'complete_sha256':sha(STATE/'_ARTIFACT_COMPLETE.json')}},'fixed_contract':{'splits':candidate['fixed_contract']['splits'],'variants':base.VARIANTS,'general_SELL_rebound_gates_bootstrap_Holm':'unchanged_base_runner','cohort':'prior shifted S1/S2/S3','historical_open':'official proxy','live_Yahoo_open':'provisional limitation'},'feature_audit':audit,'self_tests':tests,'seed_replay_6326_20260304':seed_payload,'candidate':str(cand/'compare.json'),'candidate_results':candidate['results'],'comparison_policy':{'cohort_constant':'candidate own frozen constant','prior_close_S4':'diagnostic only; target differs','same_label_paired_improvement_claim':False},'artifacts':{'label_ledger':{'path':str(lp),'sha256':sha(lp)}},'decision':{'candidate_local_decision':'review_candidate_results','authoritative_rollup_decision':'review_only'},'boundary':{'owner':'TRADEX','meemee_changed':False,'runtime_db_write':False,'production_ranking_changed':False}};cp=root/'compare.json';dump(cp,payload);dump(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(cp),'compare_sha256':sha(cp),'label_ledger_sha256':sha(lp)});return root

def main():
 p=argparse.ArgumentParser();p.add_argument('--output-root',type=Path,default=OUT);p.add_argument('--resume-root',type=Path);a=p.parse_args();print(run(a.output_root,a.resume_root))
if __name__=='__main__':main()
