from __future__ import annotations

import argparse, hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_open_provisional_sell_assessment_v1 as op

AXIS_ID='tradex_nikkei225_open_trigger_topology_audit_v1';OUT=Path(r'G:\Tradex\open_trigger_topology_audit_v1');FINAL_N={1:100,3:80,5:70,10:60}
def sha(p:Path)->str:
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8<<20),b''):h.update(b)
 return h.hexdigest()
def dump(p:Path,x:Any):p.write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')

def prepare()->tuple[pd.DataFrame,dict[int,np.ndarray],dict[str,Any]]:
 full,inp,maps,fx,names,audit=op.prepare();x=inp[['code','ymd','o','ret_close_1','ret_close_3','ret_close_5','ret_close_10','down_exc_1','down_exc_3','down_exc_5','down_exc_10','up_exc_1','up_exc_3','up_exc_5','up_exc_10','atr14','c']].copy();x=pd.concat([x.reset_index(drop=True),fx[[c for c in fx if c.startswith('open_') or c.startswith('prior_s')]].reset_index(drop=True)],axis=1)
 pre=x[x.ymd.between(20190101,20211231)];cuts={'gap_q10':float(pre.open_gap_pct.quantile(.10)),'gap_q25':float(pre.open_gap_pct.quantile(.25))}
 x['gap_bin']=np.select([x.open_gap_pct<=cuts['gap_q10'],x.open_gap_pct<=cuts['gap_q25'],x.open_gap_pct<0],['GD_SEVERE','GD_MODERATE','GD_MILD'],'NON_GD')
 x['prior_state']=np.select([x.prior_s3_weakening>=.5,x.prior_s2_top_formation>=.5,x.prior_s1_top_risk>=.5],['S3','S2','S1'],'NONE')
 for ma in ('ma7','ma20','ma60','ma100'):x['below_'+ma]=x['open_dist_prior_'+ma+'_atr']<0
 x['below_support']=x.open_dist_prior_support_atr<0
 x['branch_gap_ma20']=x.prior_state+'|'+x.gap_bin+'|M20_'+np.where(x.below_ma20,'BELOW','ABOVE')
 x['branch_gap_short_stack']=x.prior_state+'|'+x.gap_bin+'|M7'+np.where(x.below_ma7,'B','A')+'M20'+np.where(x.below_ma20,'B','A')
 bits=np.where(x.below_ma7,'7B','7A')+np.where(x.below_ma20,'20B','20A')+np.where(x.below_ma60,'60B','60A')+np.where(x.below_ma100,'100B','100A')+np.where(x.below_support,'SB','SA')
 x['branch_gap_full_stack']=x.prior_state+'|'+x.gap_bin+'|'+bits
 labels={h:maps[h].reindex(pd.MultiIndex.from_frame(x[['code','ymd']])).to_numpy(np.int8) for h in base.HORIZONS};contract={'cutpoints':cuts,'policy':'outcome_free_2019_2021_open_gap_distribution','families':['gap_ma20','gap_short_stack','gap_full_stack'],'no_score':True,'target':op.LABEL_CONTRACT};return x,labels,contract

def metric(x:pd.DataFrame,y:np.ndarray,e:np.ndarray,c:np.ndarray)->dict[str,Any]:
 def one(z):
  q=x.loc[z];n=int(z.sum());mo=q.ymd.astype(str).str[:6];return {'n':n,'codes':int(q.code.nunique()) if n else 0,'months':int(mo.nunique()) if n else 0,'down':float((y[z]==0).mean()) if n else None,'rebound':float((y[z]==1).mean()) if n else None,'neutral':float((y[z]==2).mean()) if n else None,'max_code':float(q.groupby('code').size().max()/n) if n else None,'max_month':float(mo.value_counts().max()/n) if n else None}
 a,b=one(e),one(c);return {'event':a,'control':b,'down_uplift':None if not a['n'] or not b['n'] else a['down']-b['down'],'rebound_delta':None if not a['n'] or not b['n'] else a['rebound']-b['rebound']}
def boot(x,y,e,c,h,key):
 use=e|c;f=x.loc[use].reset_index(drop=True);yy=y[use];ee=e[use];cc=c[use];vals={'e':ee.astype(float),'c':cc.astype(float),'ed':ee*(yy==0),'cd':cc*(yy==0),'er':ee*(yy==1),'cr':cc*(yy==1)}
 def st(d,k):return d['ed']/d['e']-d['cd']/d['c'] if k=='down' else d['er']/d['e']-d['cr']/d['c']
 seed=int(hashlib.sha256(key.encode()).hexdigest()[:6],16);out={};mo=f.ymd.astype(str).str[:6].to_numpy()
 for n,g,o in (('code',f.code.to_numpy(),0),('month',mo,1000)):out[n]={k:base.cluster_boot(g,vals,lambda d,k=k:st(d,k),base.SEED+h+o+seed+(0 if k=='down' else 10)) for k in ('down','rebound')}
 return out

def fast_event_mask(raw:np.ndarray,codes:np.ndarray,positions:np.ndarray)->np.ndarray:
 out=np.zeros(len(raw),dtype=bool);last={}
 for i in np.flatnonzero(raw):
  code=codes[i];pos=int(positions[i])
  if code not in last or pos-last[code]>10:out[i]=True;last[code]=pos
 return out

def run()->Path:
 x,labels,contract=prepare();families=['branch_gap_ma20','branch_gap_short_stack','branch_gap_full_stack'];enum=[]
 for h in base.HORIZONS:
  historical=x.ymd.between(20220101,20251231);valid=x[[f'ret_close_{h}',f'down_exc_{h}',f'up_exc_{h}']].notna().all(axis=1)
  if not valid[historical].all():raise ValueError(f'historical target validity differs for h{h}; cached masks unsafe')
 for fam in families:
  for branch,g in x[x.ymd.between(20190101,20211231)].groupby(fam):
   mo=g.ymd.astype(str).str[:6];r={'family':fam.removeprefix('branch_'),'branch':branch,'prior_state':g.prior_state.iloc[0],'n':len(g),'codes':g.code.nunique(),'months':mo.nunique()};r['breadth_eligible']=bool(r['prior_state']!='NONE' and r['n']>=30 and r['codes']>=20 and r['months']>=18);enum.append(r)
 cand=[r for r in enum if r['breadth_eligible']];results={};primary={};led=[];codes=x.code.to_numpy(str);dates=x.ymd.to_numpy(int);positions=x.groupby('code',sort=False).cumcount().to_numpy();periods={'2022':x.ymd.between(20220101,20221231).to_numpy(),'frozen':x.ymd.between(20230101,20251231).to_numpy()};mask_cache={}
 for spec in cand:
  col='branch_'+spec['family'];raw=x[col].eq(spec['branch']).to_numpy();same=x.prior_state.eq(spec['prior_state']).to_numpy();key=spec['family']+'|'+spec['branch']
  for pname,period in periods.items():mask_cache[(pname,key,'event')]=fast_event_mask(raw&period,codes,positions);mask_cache[(pname,key,'control')]=fast_event_mask(same&~raw&period,codes,positions)
 fixture=[];sample_codes=set(x.code.drop_duplicates().head(5));sample=np.array([c in sample_codes for c in codes])
 for spec in cand[:min(8,len(cand))]:
  col='branch_'+spec['family'];raw=x[col].eq(spec['branch']).to_numpy();same=x.prior_state.eq(spec['prior_state']).to_numpy();key=spec['family']+'|'+spec['branch']
  for kind,mask in (('event',raw),('control',same&~raw)):
   test_raw=mask&periods['2022']&sample;old=base.event_mask(test_raw,codes,dates);new=fast_event_mask(test_raw,codes,positions);fixture.append({'branch':key,'kind':kind,'equal':bool(np.array_equal(old,new)),'old_n':int(old.sum()),'new_n':int(new.sum())})
 if not all(t['equal'] for t in fixture):raise ValueError({'mask_semantic_mismatch':fixture})
 for h,y in labels.items():
  valid=x[[f'ret_close_{h}',f'down_exc_{h}',f'up_exc_{h}']].notna().all(axis=1).to_numpy();search=[];selected=[]
  for spec in cand:
   key=spec['family']+'|'+spec['branch'];e=mask_cache[('2022',key,'event')]&valid;c=mask_cache[('2022',key,'control')]&valid;m=metric(x,y,e,c);ok=bool(m['event']['n']>=30 and m['event']['codes']>=20 and m['event']['months']>=9 and m['down_uplift'] is not None and m['down_uplift']>=.05 and m['rebound_delta']<=-.03);search.append({'spec':spec,'metrics':m,'eligible':ok});
   if ok:selected.append(spec)
  frozen=[]
  for spec in selected:
   key=spec['family']+'|'+spec['branch'];e=mask_cache[('frozen',key,'event')]&valid;c=mask_cache[('frozen',key,'control')]&valid;m=metric(x,y,e,c);years={};yearok=True;absolute=0
   for yr in (2023,2024,2025):
    z=x.ymd.between(yr*10000+101,yr*10000+1231).to_numpy();ym=metric(x,y,e&z,c&z);years[str(yr)]=ym;yearok&=bool(ym['event']['n'] and ym['down_uplift']>0 and ym['rebound_delta']<=.02);absolute+=int(bool(ym['event']['n'] and ym['down_uplift']>=.05 and ym['rebound_delta']<=-.03))
   breadth=m['event']['n']>=FINAL_N[h] and m['event']['codes']>=50 and m['event']['months']>=24 and m['event']['max_code']<=.10 and m['event']['max_month']<=.15;direction=m['down_uplift']>=.05 and m['rebound_delta']<=-.03 and absolute>=2
   if breadth and direction and yearok:
    b=boot(x,y,e,c,h,key);bootok=all(v['down']['ci'][0]>0 and v['rebound']['ci'][1]<0 for v in b.values());p=max([v['down']['p_le0'] for v in b.values()]+[v['rebound']['p_ge0'] for v in b.values()]);primary[f'{h}:{key}']=p
   else:b={'status':'not_run_prebootstrap_gate_failed'};bootok=False;p=None
   frozen.append({'spec':spec,'metrics':m,'yearly':years,'bootstrap':b,'primary_p':p,'gate':{'breadth':breadth,'direction':direction,'yearly':yearok,'bootstrap':bootok},'decision':'provisional_keep' if breadth and direction and yearok and bootok else 'drop'});q=x.loc[e,['code','ymd','prior_state','gap_bin','open_gap_pct','open_gap_atr','below_ma7','below_ma20','below_ma60','below_ma100','below_support']].copy();q['horizon']=h;q['label_id']=y[e];q['branch']=key;led.append(q)
  results[str(h)]={'selection_2022':search,'selected_branches':[s['family']+'|'+s['branch'] for s in selected],'frozen':frozen,'decision':'provisional_keep' if any(r['decision']=='provisional_keep' for r in frozen) else ('drop_no_2022_open_trigger' if not selected else 'drop')}
 hm=base.holm({i:p for i,p in enumerate(primary.values())}) if primary else {};keys=list(primary);adj={keys[i]:v for i,v in hm.items()} if hm else {}
 for h,v in results.items():
  for r in v['frozen']:
   key=f'{h}:{r["spec"]["family"]}|{r["spec"]["branch"]}';r['holm']=adj.get(key);r['decision']='keep' if r['holm'] and r['holm']['pass'] and r['decision']=='provisional_keep' else 'drop'
  v['decision']='keep' if any(r['decision']=='keep' for r in v['frozen']) else ('drop_no_2022_open_trigger' if not v['selected_branches'] else 'drop')
 seed=x[(x.code=='6326')&x.ymd.eq(20260304)][['code','ymd','prior_state','gap_bin','open_gap_pct','open_gap_atr','below_ma7','below_ma20','below_ma60','below_ma100','below_support',*families]].to_dict('records');decision='keep_review_only' if any(v['decision']=='keep' for v in results.values()) else 'drop_official_open_sell_rule'
 out=OUT/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+AXIS_ID);out.mkdir(parents=True);lp=out/'frozen_open_event_ledger.parquet';pd.concat(led,ignore_index=True).to_parquet(lp,index=False) if led else pd.DataFrame({'code':pd.Series(dtype=str),'ymd':pd.Series(dtype=int),'horizon':pd.Series(dtype=int),'branch':pd.Series(dtype=str)}).to_parquet(lp,index=False);payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','single_changed_axis':'exact_open_trigger_topology_on_fixed_open_origin_labels','fixed_contract':{'features_labels_models':'unchanged','topology':contract,'enumeration':'2019_2021_outcome_free','selection':'2022_only','frozen':'2023_2025','event_dedup_bars':10},'mask_cache_audit':{'fixture':fixture,'all_equal':all(t['equal'] for t in fixture),'cached_masks':len(mask_cache),'historical_target_validity_required_and_verified':True},'preperiod_enumeration':enum,'results':results,'holm_family':adj,'seed_replay_6326_20260304':seed,'decision':{'candidate_local_decision':decision,'authoritative_rollup_decision':'review_only'},'artifacts':{'event_ledger':{'path':str(lp),'sha256':sha(lp)}},'boundary':{'owner':'TRADEX','meemee_changed':False,'runtime_db_write':False,'production_ranking_changed':False}};p=out/'compare.json';dump(p,payload);dump(out/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p),'compare_sha256':sha(p),'ledger_sha256':sha(lp)});return out

def main():argparse.ArgumentParser().parse_args();print(run())
if __name__=='__main__':main()
