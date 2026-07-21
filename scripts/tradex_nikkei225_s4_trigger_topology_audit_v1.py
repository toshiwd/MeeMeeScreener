from __future__ import annotations

import argparse, hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_first_passage_order_v1 as fp

AXIS_ID = "tradex_nikkei225_s4_trigger_topology_audit_v1"
DAILY = Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet")
STATE = Path(r"G:\Tradex\pit_sell_state_specialist_v1\20260714T170218Z-tradex_nikkei225_pit_sell_state_specialist_v1-state")
OUT = Path(r"G:\Tradex\s4_trigger_topology_audit_v1")
FINAL_N = {1:100, 3:80, 5:70, 10:60}

def sha(p:Path)->str:
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8<<20),b''):h.update(b)
 return h.hexdigest()
def dump(p:Path,x:Any)->None:p.write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')

def load()->pd.DataFrame:
 z=json.loads((STATE/'_ARTIFACT_COMPLETE.json').read_text(encoding='utf-8'));a=json.loads((STATE/'audit.json').read_text(encoding='utf-8'))
 if z.get('complete') is not True or not all(a['checks'].values()):raise ValueError('state artifact invalid')
 d=pd.read_parquet(DAILY).sort_values(['code','ymd']).reset_index(drop=True);s=pd.read_parquet(STATE/'state_ledger.parquet')
 for q in (d,s):q['code']=q.code.astype(str).str.zfill(4);q['ymd']=pd.to_numeric(q.ymd).astype(int)
 x=d.merge(s,on=['code','ymd'],validate='one_to_one').sort_values(['code','ymd']).reset_index(drop=True)
 a=x.trigger_gap_down.astype(bool);b=x.trigger_ma20_break.astype(bool);c=x.trigger_support_break.astype(bool)
 x['trigger_topology']=np.select([a&b&c,a&b&~c,a&~b&c,~a&b&c],['ABC','AB','AC','BC'],'OTHER')
 g=x.groupby('code',sort=False)
 er=g.bull_erasure_retry_candidate.transform(lambda q:q.shift().rolling(20,min_periods=1).max()).fillna(0).astype(bool)
 retry_source=(x.s2_candidate_today.astype(bool)&~x.bull_erasure_retry_candidate.astype(bool))
 rt=retry_source.groupby(x.code,sort=False).transform(lambda q:q.shift().rolling(20,min_periods=1).max()).fillna(0).astype(bool)
 x['prior_path']=np.select([er&rt,er,rt],['MIXED','ERASURE','RETRY'],'UNRESOLVED')
 x['topology_path']=x.trigger_topology+'__'+x.prior_path
 eligible=(x.s1_top_risk|x.s2_top_formation|x.s3_weakening)&~x.s4_sell_trigger_raw
 x['nontrigger_dedup']=base.event_mask(eligible.to_numpy(bool),x.code.to_numpy(str),x.ymd.to_numpy(int))
 return x

def summary(f:pd.DataFrame,y:np.ndarray,mask:np.ndarray)->dict[str,Any]:
 q=f.loc[mask];n=int(mask.sum());months=q.ymd.astype(str).str[:6]
 return {'n':n,'codes':int(q.code.nunique()) if n else 0,'months':int(months.nunique()) if n else 0,'down':float((y[mask]==0).mean()) if n else None,'rebound':float((y[mask]==1).mean()) if n else None,'neutral':float((y[mask]==2).mean()) if n else None,'max_code':float(q.groupby('code').size().max()/n) if n else None,'max_month':float(months.value_counts().max()/n) if n else None}

def compare(f:pd.DataFrame,y:np.ndarray,event:np.ndarray,control:np.ndarray)->dict[str,Any]:
 e=summary(f,y,event);c=summary(f,y,control)
 return {'event':e,'control':c,'down_uplift':None if not e['n'] or not c['n'] else e['down']-c['down'],'rebound_delta':None if not e['n'] or not c['n'] else e['rebound']-c['rebound']}

def boot(f:pd.DataFrame,y:np.ndarray,event:np.ndarray,control:np.ndarray,h:int,tag:str)->dict[str,Any]:
 use=event|control; ff=f.loc[use].reset_index(drop=True); yy=y[use];ee=event[use];cc=control[use]
 vals={'e':ee.astype(float),'c':cc.astype(float),'ed':ee*(yy==0),'cd':cc*(yy==0),'er':ee*(yy==1),'cr':cc*(yy==1)}
 def stat(d,k):return d['ed']/d['e']-d['cd']/d['c'] if k=='down' else d['er']/d['e']-d['cr']/d['c']
 seedtag=int(hashlib.sha256(tag.encode()).hexdigest()[:6],16)
 out={};month=ff.ymd.astype(str).str[:6].to_numpy()
 for name,groups,off in (('code',ff.code.to_numpy(),0),('month',month,1000)):
  out[name]={k:base.cluster_boot(groups,vals,lambda d,k=k:stat(d,k),base.SEED+h+off+seedtag+(0 if k=='down' else 10)) for k in ('down','rebound')}
 return out

def run()->Path:
 x=load();labels={h:fp.labels(x,h) for h in base.HORIZONS}
 pre=x.s4_sell_trigger_event&x.ymd.between(20190101,20211231)
 enum=[]
 for path,g in x.loc[pre].groupby('topology_path'):
  months=g.ymd.astype(str).str[:6];row={'topology_path':path,'trigger_topology':g.trigger_topology.iloc[0],'prior_path':g.prior_path.iloc[0],'n':len(g),'codes':g.code.nunique(),'months':months.nunique()};row['breadth_eligible']=bool(row['n']>=30 and row['codes']>=20 and row['months']>=18);enum.append(row)
 candidates=[r['topology_path'] for r in enum if r['breadth_eligible'] and not r['topology_path'].startswith('OTHER')]
 results={};primary={}
 for h,y in labels.items():
  valid=x[[f'ret_close_{h}',f'down_exc_{h}',f'up_exc_{h}','atr14','c']].notna().all(axis=1).to_numpy();search=[];selected=[]
  for path in candidates:
   lineage=path.split('__',1)[1];e=valid&x.s4_sell_trigger_event.to_numpy(bool)&x.ymd.between(20220101,20221231).to_numpy()&x.topology_path.eq(path).to_numpy();c=valid&x.nontrigger_dedup.to_numpy(bool)&x.ymd.between(20220101,20221231).to_numpy()&x.prior_path.eq(lineage).to_numpy();m=compare(x,y,e,c);ok=bool(m['event']['n']>=30 and m['event']['codes']>=20 and m['event']['months']>=9 and m['down_uplift'] is not None and m['down_uplift']>=.05 and m['rebound_delta']<=-.03);search.append({'topology_path':path,'metrics':m,'eligible':ok});
   if ok:selected.append(path)
  frozen=[]
  for path in selected:
   lineage=path.split('__',1)[1];e=valid&x.s4_sell_trigger_event.to_numpy(bool)&x.ymd.between(20230101,20251231).to_numpy()&x.topology_path.eq(path).to_numpy();c=valid&x.nontrigger_dedup.to_numpy(bool)&x.ymd.between(20230101,20251231).to_numpy()&x.prior_path.eq(lineage).to_numpy();m=compare(x,y,e,c);boots=boot(x,y,e,c,h,path);years={};yearok=True;absolute=0
   for yr in (2023,2024,2025):
    z=x.ymd.between(yr*10000+101,yr*10000+1231).to_numpy();ym=compare(x,y,e&z,c&z);years[str(yr)]=ym;yearok&=bool(ym['event']['n'] and ym['down_uplift']>0 and ym['rebound_delta']<=.02);absolute+=int(bool(ym['event']['n'] and ym['down_uplift']>=.05 and ym['rebound_delta']<=-.03))
   breadth=m['event']['n']>=FINAL_N[h] and m['event']['codes']>=50 and m['event']['months']>=24 and m['event']['max_code']<=.10 and m['event']['max_month']<=.15;direction=m['down_uplift']>=.05 and m['rebound_delta']<=-.03 and absolute>=2;bootok=all(v['down']['ci'][0]>0 and v['rebound']['ci'][1]<0 for v in boots.values());p=max([v['down']['p_le0'] for v in boots.values()]+[v['rebound']['p_ge0'] for v in boots.values()]);key=f'{h}:{path}';primary[key]=p;frozen.append({'topology_path':path,'metrics':m,'yearly':years,'bootstrap':boots,'primary_p':p,'gate':{'breadth':breadth,'direction':direction,'yearly':yearok,'bootstrap':bootok},'decision':'provisional_keep' if breadth and direction and yearok and bootok else 'drop'})
  results[str(h)]={'selection_2022':search,'selected_paths':[r for r in selected],'frozen':frozen,'decision':'provisional_keep' if any(r['decision']=='provisional_keep' for r in frozen) else ('drop_no_2022_topology' if not selected else 'drop')}
 holm=base.holm({i:p for i,p in enumerate(primary.values())}) if primary else {};keys=list(primary)
 adjusted={keys[i]:v for i,v in holm.items()} if holm else {}
 for h,v in results.items():
  for r in v['frozen']:
   a=adjusted.get(f'{h}:{r["topology_path"]}');r['holm']=a
   if not a or not a['pass'] or r['decision']!='provisional_keep':r['decision']='drop'
   else:r['decision']='keep'
  v['decision']='keep' if any(r['decision']=='keep' for r in v['frozen']) else ('drop_no_2022_topology' if not v['selected_paths'] else 'drop')
 decision='keep_review_only' if any(v['decision']=='keep' for v in results.values()) else 'drop_family'
 out=OUT/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+AXIS_ID);out.mkdir(parents=True)
 payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','single_changed_axis':'exact_fixed_S4_trigger_topology_and_prior_state_path','source':{'daily':{'path':str(DAILY),'sha256':sha(DAILY)},'state_root':str(STATE),'state_complete_sha256':sha(STATE/'_ARTIFACT_COMPLETE.json')},'fixed_contract':{'state_and_s4_events':'immutable','trigger_axes':{'A':'gap_down','B':'ma20_break_or_rebreak','C':'support_break_instrumented','D':'excluded_boundary_not_instrumented'},'raw_erasure_lineage':'measured_fixed_state_column_20bar_prior_only','enumeration':'2019_2021_breadth_outcome_free','selection':'2022_only','frozen':'2023_2025','threshold_changes':False,'veto_changes':False,'score_changes':False,'model_changes':False},'preperiod_enumeration':enum,'results':results,'holm_family':adjusted,'decision':{'candidate_local_decision':decision,'authoritative_rollup_decision':'review_only'},'boundary':{'owner':'TRADEX','meemee_changed':False,'runtime_db_write':False,'production_ranking_changed':False}}
 p=out/'compare.json';dump(p,payload);dump(out/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p),'compare_sha256':sha(p)});return out

def main():
 p=argparse.ArgumentParser();p.parse_args();print(run())
if __name__=='__main__':main()
