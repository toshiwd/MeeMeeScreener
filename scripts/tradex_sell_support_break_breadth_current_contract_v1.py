from __future__ import annotations
import argparse,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import duckdb,numpy as np,pandas as pd
from scripts.tradex_early_signal_first_detect_v1 import _metrics,_branching
from scripts.tradex_shallow_high_zone_universe_repair_v1 import no_reentry_sessions,_sha,_write,extra_metrics
AXIS_ID='tradex_sell_support_break_breadth_current_contract_v1';OUT=Path(r'G:\Tradex\tradex_sell_support_break_breadth_current_contract_v1');WAITS=(1,3,5);PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}
def load(db:Path):
 with duckdb.connect(str(db),read_only=True) as q:
  f=q.execute("select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code from ml_feature_daily where dt between epoch(date '2023-01-01') and epoch(date '2026-12-31')").fetchdf();b=q.execute("select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code,o,h,l,c,v from daily_bars where source='pan' order by code,signal_ymd").fetchdf();r=q.execute("select dt signal_ymd,cast(code as varchar) code,rank baseline_rank from ranking_appearance_daily where ranking_logic_version='ranking:trade:top50:v1' and dir='down' and rank<=10 and dt between 20240101 and 20261231").fetchdf()
 return f,b,r,{'feature_rows':len(f),'feature_codes':f.code.nunique(),'feature_min_date':int(f.signal_ymd.min()),'feature_max_date':int(f.signal_ymd.max()),'pan_rows':len(b),'pan_codes':b.code.nunique(),'ranking_rows_top10':len(r)}
def features(frame:pd.DataFrame,bars:pd.DataFrame)->pd.DataFrame:
 parts=[]
 for _,p in bars.groupby('code',sort=False):
  p=p.sort_values('signal_ymd').copy();p['prior20_low']=p.l.shift(1).rolling(20,20).min();p['ma20']=p.c.rolling(20,20).mean();p['vol20']=p.v.shift(1).rolling(20,20).mean();p['vol_ratio']=p.v/p.vol20;p['close_pos']=(p.c-p.l)/(p.h-p.l);parts.append(p)
 x=pd.concat(parts,ignore_index=True).merge(frame,on=['signal_ymd','code'],how='inner',validate='one_to_one');breadth=x.groupby('signal_ymd').apply(lambda q:float((q.c<q.ma20).mean()),include_groups=False).rename('breadth_below_ma20');x=x.merge(breadth,on='signal_ymd');x['setup_hit']=(x.c<x.prior20_low)&(x.vol_ratio>=3)&(x.close_pos<=.10)&(x.c<=x.ma20*.90);x['breadth_hit']=x.breadth_below_ma20>=.40;return x
def score_stage(x:pd.DataFrame,wait:int)->pd.DataFrame:
 out=[]
 for _,p in x.groupby('code',sort=False):
  p=p.sort_values('signal_ymd').copy();active=None;ready=[];setup_date=[]
  for i,r in enumerate(p.itertuples()):
   if active is not None and i-active[0]>wait:active=None
   if bool(r.setup_hit) and bool(r.breadth_hit):active=(i,float(r.l),int(r.signal_ymd))
   hit=bool(active is not None and i>active[0] and i-active[0]<=wait and float(r.l)<active[1]);ready.append(hit);setup_date.append(active[2] if active else None)
   if hit:active=None
  p['readiness_hit']=ready;p['setup_ymd']=pd.array(setup_date,dtype='Int64');out.append(p)
 z=pd.concat(out,ignore_index=True);stage=np.select([z.readiness_hit,z.setup_hit&z.breadth_hit,z.setup_hit],[3.,2.,1.],default=0.);z['family_hit']=z.readiness_hit;z['score']=stage*100+(-z.c/z.prior20_low+1).fillna(-1)+z.vol_ratio.fillna(0).clip(0,10)/100;z=z.sort_values(['signal_ymd','score','code'],ascending=[True,False,True]);z['rank']=z.groupby('signal_ymd').cumcount()+1;z['percentile']=1-(z['rank']-1)/z.groupby('signal_ymd').code.transform('size');z['top10']=z['rank']<=10;z['side']='SELL';return z
def outcomes(frame:pd.DataFrame,bars:pd.DataFrame)->pd.DataFrame:
 rows=[]
 for code,p in bars.groupby('code',sort=False):
  p=p.sort_values('signal_ymd').reset_index(drop=True);n=len(p);entry=p.o.shift(-1).to_numpy(float);ret=np.full(n,np.nan);off=np.zeros(n,dtype=np.int16);target=np.zeros(n,dtype=np.int8);mover=np.zeros(n,dtype=np.int8)
  for d in range(1,11):
   op=p.o.shift(-d).to_numpy(float);hi=p.h.shift(-d).to_numpy(float);lo=p.l.shift(-d).to_numpy(float);mover|=(lo<=entry*.92).astype(np.int8);u=off==0;gs=op>=entry*1.05;gt=op<=entry*.92;stop=hi>=entry*1.05;tp=lo<=entry*.92;ev=u&(gs|gt|stop|tp);off[ev]=d;rv=np.where(gs,entry/op-1,np.where(gt,entry/op-1,np.where(stop,-.05,.08)));ret[ev]=rv[ev];target[ev]=(gt|(~gs&~stop&tp))[ev]
  valid=np.arange(n)+10<n;terminal=entry/p.c.shift(-10).to_numpy(float)-1;u=(off==0)&valid;ret[u]=terminal[u];off[u]=10;rows.append(pd.DataFrame({'signal_ymd':p.signal_ymd,'code':str(code),'trade_return_h10':np.where(valid,ret-.001,np.nan),'exit_day_h10':np.where(valid,off,np.nan),'target_before_stop20':np.where(valid,target,np.nan),'realized_mover20':np.where(valid,mover,np.nan)}))
 return frame.merge(pd.concat(rows,ignore_index=True),on=['signal_ymd','code'],how='left',validate='one_to_one')
def select(s:pd.DataFrame,eligible_only:bool,cal:list[int],rank_col:str)->pd.DataFrame:
 q=s[s.trade_return_h10.notna()];q=q[q.family_hit] if eligible_only else q;q=q.sort_values(['signal_ymd',rank_col,'code']).groupby('signal_ymd',as_index=False).head(1);return no_reentry_sessions(q.reset_index(drop=True),cal,rank_col)
def generate(db:Path,out:Path)->Path:
 f,b,r,cov=load(db);base=features(f,b);cal=sorted(base[base.signal_ymd>=20240101].signal_ymd.unique());variants=[];sets={};td=sum(d<=20241231 for d in cal)
 for w in WAITS:
  s=outcomes(score_stage(base,w),b);s=s[s.signal_ymd>=20240101];e=select(s,True,cal,'rank');m=_metrics(e[e.signal_ymd<=20241231],td);variants.append({'readiness_wait_sessions':w,'train_metrics':m});sets[w]=(s,e)
 ok=[v for v in variants if (v['train_metrics']['expectancy'] or 0)>0 and v['train_metrics']['profit_factor'] is not None and v['train_metrics']['signals_per_week']>=1]
 pool=ok or [v for v in variants if v['train_metrics']['profit_factor'] is not None]
 if not pool:raise ValueError('NO_2024_VARIANT_WITH_METRICS')
 chosen=max(pool,key=lambda v:(v['train_metrics']['profit_factor'],v['train_metrics']['expectancy']));chosen['train_selection_gate_pass']=bool(ok);s,e=sets[chosen['readiness_wait_sessions']];s['split']=np.select([s.signal_ymd<20250101,s.signal_ymd<20260101],['train','validation'],default='shadow');elig=s[s.family_hit][['signal_ymd']].drop_duplicates();mm=r.merge(elig,on='signal_ymd',how='inner').merge(s[['signal_ymd','code','trade_return_h10','exit_day_h10','target_before_stop20','realized_mover20']],on=['signal_ymd','code'],how='left');mm['family_hit']=True;mm=select(mm.rename(columns={'baseline_rank':'rank'}),False,cal,'rank');latest=int(f.signal_ymd.max());periods={**PERIODS,'shadow':(20260101,latest)};metrics={};gates={};branch={};coverage={}
 for split,(a,z) in periods.items():
  days=sum(a<=d<=z for d in cal);ch=e[e.signal_ymd.between(a,z)];bl=mm[mm.signal_ymd.between(a,z)];allx=s[s.signal_ymd.between(a,z)&s.trade_return_h10.notna()];cm={**_metrics(ch,days),**extra_metrics(ch,allx)};bm={**_metrics(bl,days),**extra_metrics(bl,allx)};metrics[split]={'sell_eligible_top1':cm,'meemee_sell_same_eligible_top1':bm};gates[split]={'pf_ge_1_30':(cm['profit_factor'] or 0)>=1.3,'expectancy_positive':(cm['expectancy'] or 0)>0,'frequency_ge_1_week':cm['signals_per_week']>=1,'pf_improves':cm['profit_factor'] is not None and bm['profit_factor'] is not None and cm['profit_factor']>bm['profit_factor'],'expectancy_improves':cm['expectancy'] is not None and bm['expectancy'] is not None and cm['expectancy']>bm['expectancy'],'cvar_non_degrade':cm['cvar10'] is not None and bm['cvar10'] is not None and cm['cvar10']>=bm['cvar10'],'dd_non_degrade':cm['max_drawdown'] is not None and bm['max_drawdown'] is not None and cm['max_drawdown']>=bm['max_drawdown']};branch[split]=_branching(s[s.top10],r.assign(side='SELL'),'SELL',a,z);cnt=s[s.signal_ymd.between(a,z)].groupby('signal_ymd').size();coverage[split]={'days':len(cnt),'min_ranked':int(cnt.min()),'all_days_ranked':bool(cnt.gt(0).all()),'eligible_days':int(elig.signal_ymd.between(a,z).sum())}
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);s.to_parquet(root/'all_symbol_daily_sell_scores.parquet',index=False);e.to_parquet(root/'sell_entry_ledger.parquet',index=False);mm.to_parquet(root/'meemee_sell_baseline_ledger.parquet',index=False);keep=all(all(gates[x].values()) for x in ('validation','shadow'));payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'shape':'close<prior20 low, volume>=3x prior20, close_pos<=.10, close<=MA20*.90','breadth':'same-day all feature-covered PAN below MA20 >=.40','readiness':'subsequent low breaks setup signal low','variants_train_only':'readiness wait 1/3/5 sessions','ranking':'all symbols daily SELL rank; nonhits included; top1 entry only complete stage','entry_exit':'readiness confirmed then next-open TP8/SL5/H10/10bp; gap-through actual open; reentry prohibited','baseline':'MeeMee SELL top1 exact eligible days','survivorship_filter':False,'fallback':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)}],'source_coverage':cov,'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'branching':branch,'rank_coverage':coverage,'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only'},'shadow_tuning_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
