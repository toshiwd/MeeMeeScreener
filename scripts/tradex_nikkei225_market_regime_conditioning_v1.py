from __future__ import annotations

import argparse, hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0,str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_first_passage_order_v1 as fp
import tradex_nikkei225_s4_trigger_topology_audit_v1 as topo
import tradex_nikkei225_s4_topology_with_failed_rebound_v1 as dtopo

AXIS_ID='tradex_nikkei225_market_regime_conditioning_v1'
MRP=Path(r'G:\Tradex\mrp_v1\20260714T120930Z-tradex_nikkei225_market_relative_path_v1\market_relative_path_features.parquet')
STATE=dtopo.STATE_V2
OUT=Path(r'G:\Tradex\market_regime_conditioning_v1')
FINAL_N={1:100,3:80,5:70,10:60}

def sha(p:Path)->str:
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8<<20),b''):h.update(b)
 return h.hexdigest()
def dump(p:Path,x:Any):p.write_text(json.dumps(x,ensure_ascii=False,indent=2,default=str)+'\n',encoding='utf-8')

def market_regimes()->tuple[pd.DataFrame,dict[str,Any]]:
 m=pd.read_parquet(MRP,columns=['ymd','mret1','advancers_ratio','market_n','market_valid']);m['ymd']=pd.to_numeric(m.ymd).astype(int)
 consistency=m.groupby('ymd')[['mret1','advancers_ratio']].nunique(dropna=False).max().max()
 if consistency>1:raise ValueError('market aggregate differs within date')
 m=m.drop_duplicates('ymd').sort_values('ymd').reset_index(drop=True);m['market_log_index']=np.log1p(m.mret1.clip(lower=-.999)).fillna(0).cumsum();m['market_ma20']=m.market_log_index.rolling(20,min_periods=20).mean();m['market_ma60']=m.market_log_index.rolling(60,min_periods=60).mean();m['market_dist_ma20']=m.market_log_index-m.market_ma20;m['market_dist_ma60']=m.market_log_index-m.market_ma60;m['market_ma20_slope5']=m.market_ma20-m.market_ma20.shift(5);m['market_ma60_slope5']=m.market_ma60-m.market_ma60.shift(5);m['market_vol20']=m.mret1.rolling(20,min_periods=20).std()
 pre=m[m.ymd.between(20190101,20211231)];cuts={'breadth_q25':float(pre.advancers_ratio.quantile(.25)),'breadth_q75':float(pre.advancers_ratio.quantile(.75)),'vol_q25':float(pre.market_vol20.quantile(.25)),'vol_q75':float(pre.market_vol20.quantile(.75))}
 m['regime_trend']=np.select([(m.market_dist_ma20>=0)&(m.market_dist_ma60>=0),(m.market_dist_ma20<0)&(m.market_dist_ma60<0)],['ABOVE_MA20_60','BELOW_MA20_60'],'BETWEEN_MA20_60')
 m['regime_slope']=np.select([(m.market_ma20_slope5>=0)&(m.market_ma60_slope5>=0),(m.market_ma20_slope5<0)&(m.market_ma60_slope5<0)],['SLOPES_UP','SLOPES_DOWN'],'SLOPES_MIXED')
 m['regime_breadth']=np.select([m.advancers_ratio<=cuts['breadth_q25'],m.advancers_ratio>=cuts['breadth_q75']],['BREADTH_LOW','BREADTH_HIGH'],'BREADTH_MID')
 m['regime_volatility']=np.select([m.market_vol20<=cuts['vol_q25'],m.market_vol20>=cuts['vol_q75']],['VOL_LOW','VOL_HIGH'],'VOL_MID')
 contract={'source':'PIT cross_sectional market aggregate mret1/advancers_ratio','preperiod':[20190101,20211231],'cutpoints':cuts,'families':{'trend':['ABOVE_MA20_60','BELOW_MA20_60','BETWEEN_MA20_60'],'slope':['SLOPES_UP','SLOPES_DOWN','SLOPES_MIXED'],'breadth':['BREADTH_LOW','BREADTH_MID','BREADTH_HIGH'],'volatility':['VOL_LOW','VOL_MID','VOL_HIGH']},'outcome_columns_used':[],'combined_regime_score':False}
 return m[['ymd','mret1','advancers_ratio','market_dist_ma20','market_dist_ma60','market_ma20_slope5','market_ma60_slope5','market_vol20','regime_trend','regime_slope','regime_breadth','regime_volatility']],contract

def run()->Path:
 x=dtopo.load_v2();market,contract=market_regimes();x=x.merge(market,on='ymd',how='left',validate='many_to_one');labels={h:fp.labels(x,h) for h in base.HORIZONS};families=['regime_trend','regime_slope','regime_breadth','regime_volatility']
 pre=x.s4_sell_trigger_event&x.ymd.between(20190101,20211231);enum=[]
 for (path,fam,reg),g in pd.concat([x.loc[pre,['code','ymd','topology_path',f]].rename(columns={f:'regime'}).assign(family=f) for f in families]).groupby(['topology_path','family','regime']):
  mo=g.ymd.astype(str).str[:6];r={'topology_path':path,'family':fam,'regime':reg,'n':len(g),'codes':g.code.nunique(),'months':mo.nunique()};r['breadth_eligible']=bool(r['n']>=30 and r['codes']>=20 and r['months']>=18);enum.append(r)
 candidates=[r for r in enum if r['breadth_eligible'] and not r['topology_path'].startswith('OTHER')];results={};primary={}
 for h,y in labels.items():
  valid=x[[f'ret_close_{h}',f'down_exc_{h}',f'up_exc_{h}','atr14','c']].notna().all(axis=1).to_numpy();search=[];selected=[]
  for spec in candidates:
   path,fam,reg=spec['topology_path'],spec['family'],spec['regime'];lineage=path.split('__',1)[1];e=valid&x.s4_sell_trigger_event.to_numpy(bool)&x.ymd.between(20220101,20221231).to_numpy()&x.topology_path.eq(path).to_numpy()&x[fam].eq(reg).to_numpy();c=valid&x.nontrigger_dedup.to_numpy(bool)&x.ymd.between(20220101,20221231).to_numpy()&x.prior_path.eq(lineage).to_numpy()&x[fam].eq(reg).to_numpy();m=topo.compare(x,y,e,c);ok=bool(m['event']['n']>=30 and m['event']['codes']>=20 and m['event']['months']>=9 and m['down_uplift'] is not None and m['down_uplift']>=.05 and m['rebound_delta']<=-.03);key=f'{path}|{fam}|{reg}';search.append({'branch':key,'spec':spec,'metrics':m,'eligible':ok});
   if ok:selected.append((key,spec))
  frozen=[]
  for key,spec in selected:
   path,fam,reg=spec['topology_path'],spec['family'],spec['regime'];lineage=path.split('__',1)[1];e=valid&x.s4_sell_trigger_event.to_numpy(bool)&x.ymd.between(20230101,20251231).to_numpy()&x.topology_path.eq(path).to_numpy()&x[fam].eq(reg).to_numpy();c=valid&x.nontrigger_dedup.to_numpy(bool)&x.ymd.between(20230101,20251231).to_numpy()&x.prior_path.eq(lineage).to_numpy()&x[fam].eq(reg).to_numpy();m=topo.compare(x,y,e,c);boots=topo.boot(x,y,e,c,h,key);years={};yearok=True;absolute=0
   for yr in (2023,2024,2025):
    z=x.ymd.between(yr*10000+101,yr*10000+1231).to_numpy();ym=topo.compare(x,y,e&z,c&z);years[str(yr)]=ym;yearok&=bool(ym['event']['n'] and ym['down_uplift']>0 and ym['rebound_delta']<=.02);absolute+=int(bool(ym['event']['n'] and ym['down_uplift']>=.05 and ym['rebound_delta']<=-.03))
   breadth=m['event']['n']>=FINAL_N[h] and m['event']['codes']>=50 and m['event']['months']>=24 and m['event']['max_code']<=.10 and m['event']['max_month']<=.15;direction=m['down_uplift']>=.05 and m['rebound_delta']<=-.03 and absolute>=2;bootok=all(v['down']['ci'][0]>0 and v['rebound']['ci'][1]<0 for v in boots.values());p=max([v['down']['p_le0'] for v in boots.values()]+[v['rebound']['p_ge0'] for v in boots.values()]);primary[f'{h}:{key}']=p;frozen.append({'branch':key,'spec':spec,'metrics':m,'yearly':years,'bootstrap':boots,'primary_p':p,'gate':{'breadth':breadth,'direction':direction,'yearly':yearok,'bootstrap':bootok},'decision':'provisional_keep' if breadth and direction and yearok and bootok else 'drop'})
  results[str(h)]={'selection_2022':search,'selected_branches':[k for k,_ in selected],'frozen':frozen,'decision':'provisional_keep' if any(r['decision']=='provisional_keep' for r in frozen) else ('drop_no_2022_regime' if not selected else 'drop')}
 hm=base.holm({i:p for i,p in enumerate(primary.values())}) if primary else {};keys=list(primary);adj={keys[i]:v for i,v in hm.items()} if hm else {}
 for h,v in results.items():
  for r in v['frozen']:
   a=adj.get(f'{h}:{r["branch"]}');r['holm']=a;r['decision']='keep' if a and a['pass'] and r['decision']=='provisional_keep' else 'drop'
  v['decision']='keep' if any(r['decision']=='keep' for r in v['frozen']) else ('drop_no_2022_regime' if not v['selected_branches'] else 'drop')
 decision='keep_review_only' if any(v['decision']=='keep' for v in results.values()) else 'drop_state_trigger_family'
 out=OUT/(datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+AXIS_ID);out.mkdir(parents=True);payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','single_changed_axis':'independent_market_regime_conditioning_on_fixed_S4_and_S8_events','source':{'market_relative_path':{'path':str(MRP),'sha256':sha(MRP)},'fixed_state_D_root':str(STATE),'state_complete_sha256':sha(STATE/'_ARTIFACT_COMPLETE.json')},'fixed_contract':{'events_thresholds_models_veto':'unchanged','regime':contract,'selection':'2022_only','frozen':'2023_2025','exact_single_regime_branch_only':True},'preperiod_enumeration':enum,'results':results,'holm_family':adj,'decision':{'candidate_local_decision':decision,'authoritative_rollup_decision':'review_only'},'boundary':{'owner':'TRADEX','meemee_changed':False,'runtime_db_write':False,'production_ranking_changed':False}};p=out/'compare.json';dump(p,payload);dump(out/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p),'compare_sha256':sha(p)});return out

def main():argparse.ArgumentParser().parse_args();print(run())
if __name__=='__main__':main()
