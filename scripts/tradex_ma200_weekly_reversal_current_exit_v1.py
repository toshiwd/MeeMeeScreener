from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import numpy as np,pandas as pd
try:
 from scripts.tradex_early_signal_first_detect_v1 import _branching,_metrics
 from scripts.tradex_shallow_high_zone_universe_repair_v1 import load_source,attach_gap_outcomes,no_reentry_sessions,_sha,_write,extra_metrics
except ModuleNotFoundError:
 from tradex_early_signal_first_detect_v1 import _branching,_metrics
 from tradex_shallow_high_zone_universe_repair_v1 import load_source,attach_gap_outcomes,no_reentry_sessions,_sha,_write,extra_metrics
AXIS_ID='tradex_ma200_weekly_reversal_current_exit_v1';OUT=Path(r'G:\Tradex\tradex_ma200_weekly_reversal_current_exit_v1');SOURCE=Path(r'G:\Tradex\long_ma_weekly_reversal_axis_v1\20260712T123303Z-long_ma_weekly_reversal_axis_v1\compare.json')
PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)};VARIANTS=('support_distance','ma200_slope20','close_position')
def build_frame(features:pd.DataFrame,bars:pd.DataFrame)->pd.DataFrame:
 parts=[]
 for _,p in bars.groupby('code',sort=False):
  p=p.sort_values('signal_ymd').copy();p['c5']=p.c.shift(5);p['ma200']=p.c.rolling(200,200).mean();p['ma200_20']=p.ma200.shift(20);parts.append(p)
 x=pd.concat(parts,ignore_index=True).merge(features,on=['signal_ymd','code'],how='inner',validate='one_to_one');x['support_distance']=(x.l/x.ma200-1).abs();x['ma200_slope20']=x.ma200/x.ma200_20-1;x['close_position']=(x.c-x.l)/(x.h-x.l)
 x['family_hit']=(x.ma200>x.ma200_20)&(x.l<=x.ma200*1.02)&(x.l>=x.ma200*.97)&(x.c>x.ma200)&(x.c>x.o)&(x.c>x.c5)&(x.close_position>=.70);return x
def score(x:pd.DataFrame,variant:str)->pd.DataFrame:
 z=x.copy();quality={'support_distance':-z.support_distance,'ma200_slope20':z.ma200_slope20,'close_position':z.close_position}[variant].replace([np.inf,-np.inf],np.nan).fillna(-9)
 z['score']=z.family_hit.astype(float)*100+quality;z=z.sort_values(['signal_ymd','score','code'],ascending=[True,False,True]);z['rank']=z.groupby('signal_ymd').cumcount()+1;z['top10']=z['rank']<=10;z['percentile']=1-(z['rank']-1)/z.groupby('signal_ymd').code.transform('size');z['side']='BUY';return z
def generate(db:Path,out:Path)->Path:
 f,b,r,cov=load_source(db);base=attach_gap_outcomes(build_frame(f,b),b);base=base[base.signal_ymd>=20240101];cal=sorted(base.signal_ymd.unique());td=sum(d<=20241231 for d in cal);variants=[];scored={}
 for v in VARIANTS:
  s=score(base,v);e=no_reentry_sessions(s[s.top10&s.trade_return_h10.notna()],cal,'rank');m=_metrics(e[e.signal_ymd<=20241231],td);variants.append({'variant':v,'train_metrics':m});scored[v]=s
 ok=[v for v in variants if v['train_metrics']['expectancy'] is not None and v['train_metrics']['expectancy']>0 and v['train_metrics']['profit_factor'] is not None]
 if not ok:raise ValueError('NO_POSITIVE_2024_VARIANT')
 chosen=max(ok,key=lambda v:(v['train_metrics']['profit_factor'],v['train_metrics']['expectancy']));s=scored[chosen['variant']];s['split']=np.select([s.signal_ymd<20250101,s.signal_ymd<20260101],['train','validation'],default='shadow');top=no_reentry_sessions(s[s.top10&s.trade_return_h10.notna()],cal,'rank')
 bl=r.merge(s[['signal_ymd','code','side','trade_return_h10','exit_day_h10','target_before_stop20','realized_mover20']],on=['signal_ymd','code'],how='left',validate='many_to_one');bl=no_reentry_sessions(bl[bl.trade_return_h10.notna()],cal,'baseline_rank');latest=int(f.signal_ymd.max());periods={**PERIODS,'shadow':(20260101,latest)};metrics={};branch={};coverage={};gates={}
 for split,(a,z) in periods.items():
  days=sum(a<=d<=z for d in cal);ch=top[top.signal_ymd.between(a,z)];mm=bl[bl.signal_ymd.between(a,z)];elig=s[s.signal_ymd.between(a,z)&s.trade_return_h10.notna()];cm={**_metrics(ch,days),**extra_metrics(ch,elig)};bm={**_metrics(mm,days),**extra_metrics(mm,elig)};metrics[split]={'challenger_top10':cm,'meemee_buy_top10':bm};branch[split]=_branching(s[s.top10],r.assign(side='BUY'),'BUY',a,z);cnt=s[s.signal_ymd.between(a,z)].groupby('signal_ymd').size();coverage[split]={'days':len(cnt),'min_ranked':int(cnt.min()),'all_days_ranked':bool(cnt.gt(0).all())}
  gates[split]={'pf_ge_1_30':(cm['profit_factor'] or 0)>=1.3,'expectancy_positive':(cm['expectancy'] or 0)>0,'pf_improves':cm['profit_factor'] is not None and bm['profit_factor'] is not None and cm['profit_factor']>bm['profit_factor'],'expectancy_improves':cm['expectancy'] is not None and bm['expectancy'] is not None and cm['expectancy']>bm['expectancy'],'cvar_non_degrade':cm['cvar10'] is not None and bm['cvar10'] is not None and cm['cvar10']>=bm['cvar10'],'dd_non_degrade':cm['max_drawdown'] is not None and bm['max_drawdown'] is not None and cm['max_drawdown']>=bm['max_drawdown']}
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);s.to_parquet(root/'all_symbol_daily_scores.parquet',index=False);top.to_parquet(root/'challenger_top10_events.parquet',index=False);bl.to_parquet(root/'meemee_buy_top10_events.parquet',index=False)
 keep=all(all(gates[x].values()) for x in ('validation','shadow'));payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'shape':'fixed MA200 rising; low within -3%/+2%; close above MA200; bullish close position>=.7; close above 5-session-ago close','score_variants':'2024 only, maximum 3 outcome-free priorities','entry_exit':'next-open TP8/SL5/H10/10bp; gap-through actual open; intraday stop-first','universe':'all historical each-day feature-covered PAN; no survivor filter','ranking':'all symbols daily including nonmatches; top10','reentry':'same code prohibited while trade open','validation':'2025','untouched_shadow':'2026','fallback':False},'source_hypothesis_artifact':{'path':str(SOURCE),'sha256':_sha(SOURCE),'role':'shape_definition_only'},'source_artifacts':[{'path':str(db),'sha256':_sha(db)}],'source_coverage':cov,'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'branching':branch,'rank_coverage':coverage,'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only'},'shadow_tuning_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
