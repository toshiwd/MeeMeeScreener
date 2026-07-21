from __future__ import annotations
import argparse
from datetime import datetime,timezone
from pathlib import Path
import duckdb,numpy as np,pandas as pd
from scripts.tradex_early_signal_first_detect_v1 import _metrics,_branching
from scripts.tradex_contraction_sequence_challenger_v1 import load_source
from scripts.tradex_ma200_weekly_reversal_current_exit_v1 import build_frame
from scripts.tradex_shallow_high_zone_universe_repair_v1 import attach_gap_outcomes,no_reentry_sessions,_sha,_write,extra_metrics
from scripts.tradex_strict_pit_adaptive_buy_router_v1 import add_exit_ymd,lane_stats
AXIS_ID='tradex_pit_router_vs_meemee_meta_v1';OUT=Path(r'G:\Tradex\tradex_pit_router_vs_meemee_meta_v1');SOURCE=Path(r'G:\Tradex\tradex_family_hit_only_rolling_gate_v1\20260713T092906Z-tradex_family_hit_only_rolling_gate_v1');WINDOWS=(40,60,120);PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}
def choose_lane(router:pd.DataFrame,meemee:pd.DataFrame,day:int,window:int)->str|None:
 rows=[]
 for name,e in [('router',router),('meemee',meemee)]:
  pf,exp,n=lane_stats(e,day,window)
  if n>=10:rows.append((name,pf,exp))
 return max(rows,key=lambda x:(x[1],x[2],x[0]))[0] if rows else None
def meta_events(router_candidates:pd.DataFrame,meemee_candidates:pd.DataFrame,router_paper:pd.DataFrame,meemee_paper:pd.DataFrame,window:int,calendar:list[int])->pd.DataFrame:
 picks=[]
 for day in sorted(set(router_candidates.signal_ymd)|set(meemee_candidates.signal_ymd)):
  lane=choose_lane(router_paper,meemee_paper,int(day),window)
  if lane is None:continue
  q=(router_candidates if lane=='router' else meemee_candidates);q=q[q.signal_ymd==day]
  if len(q):r=q.iloc[0].copy();r['meta_lane']=lane;picks.append(r)
 z=pd.DataFrame(picks).drop_duplicates(['signal_ymd'],keep='first').reset_index(drop=True);return no_reentry_sessions(z,calendar,'rank') if len(z) else z
def generate(db:Path,out:Path)->Path:
 f,b,r,cov=load_source(db)
 with duckdb.connect(str(db),read_only=True) as q:v=q.execute("select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code,v from daily_bars where source='pan'").fetchdf()
 b=b.merge(v,on=['signal_ymd','code'],how='left',validate='one_to_one');common=attach_gap_outcomes(build_frame(f[['signal_ymd','code']],b),b);outs=common[['signal_ymd','code','trade_return_h10','exit_day_h10','target_before_stop20','realized_mover20']];cal=sorted(f[f.signal_ymd>=20240101].signal_ymd.unique());s=pd.read_parquet(SOURCE/'all_symbol_daily_scores_and_eligibility.parquet');e=s[s.eligible]
 rc=e.sort_values(['signal_ymd','rank','code']).groupby('signal_ymd',as_index=False).head(1)[['signal_ymd','code','rank']].merge(outs,on=['signal_ymd','code'],how='left');mc=r.merge(e[['signal_ymd','eligible']].drop_duplicates(),on='signal_ymd',how='inner');mc=mc[mc.eligible].sort_values(['signal_ymd','baseline_rank','code']).groupby('signal_ymd',as_index=False).head(1).rename(columns={'baseline_rank':'rank'}).merge(outs,on=['signal_ymd','code'],how='left');rc=rc[rc.trade_return_h10.notna()];mc=mc[mc.trade_return_h10.notna()];rp=add_exit_ymd(no_reentry_sessions(rc,cal,'rank'),b);mp=add_exit_ymd(no_reentry_sessions(mc,cal,'rank'),b);variants=[];events={};td=sum(d<=20241231 for d in cal)
 for w in WINDOWS:
  z=meta_events(rc,mc,rp,mp,w,cal);m=_metrics(z[z.signal_ymd<=20241231],td);variants.append({'completed_event_window':w,'train_metrics':m});events[w]=z
 ok=[x for x in variants if (x['train_metrics']['expectancy'] or 0)>0 and x['train_metrics']['profit_factor'] is not None]
 if not ok:raise ValueError('NO_POSITIVE_2024_META_WINDOW')
 chosen=max(ok,key=lambda x:(x['train_metrics']['profit_factor'],x['train_metrics']['expectancy']));z=events[chosen['completed_event_window']];latest=int(f.signal_ymd.max());periods={**PERIODS,'shadow':(20260101,latest)};metrics={};gates={};coverage={};branch={}
 for split,(a,d) in periods.items():
  days=sum(a<=x<=d for x in cal);ch=z[z.signal_ymd.between(a,d)];bl=mp[mp.signal_ymd.between(a,d)];elig=common[common.signal_ymd.between(a,d)&common.trade_return_h10.notna()];cm={**_metrics(ch,days),**extra_metrics(ch,elig)};bm={**_metrics(bl,days),**extra_metrics(bl,elig)};metrics[split]={'meta_selected_top1':cm,'meemee_same_eligible_top1':bm};gates[split]={'pf_ge_1_30':(cm['profit_factor'] or 0)>=1.3,'expectancy_positive':(cm['expectancy'] or 0)>0,'pf_improves':cm['profit_factor'] is not None and bm['profit_factor'] is not None and cm['profit_factor']>bm['profit_factor'],'expectancy_improves':cm['expectancy'] is not None and bm['expectancy'] is not None and cm['expectancy']>bm['expectancy'],'cvar_non_degrade':cm['cvar10'] is not None and bm['cvar10'] is not None and cm['cvar10']>=bm['cvar10'],'dd_non_degrade':cm['max_drawdown'] is not None and bm['max_drawdown'] is not None and cm['max_drawdown']>=bm['max_drawdown']};cnt=s[s.signal_ymd.between(a,d)].groupby('signal_ymd').size();coverage[split]={'days':len(cnt),'min_ranked':int(cnt.min()),'all_days_ranked':bool(cnt.gt(0).all()),'eligible_days':int(e[e.signal_ymd.between(a,d)].signal_ymd.nunique())};branch[split]=_branching(s[s.top10],r.assign(side='BUY'),'BUY',a,d)
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);s.to_parquet(root/'all_symbol_daily_scores.parquet',index=False);z.to_parquet(root/'meta_execution_ledger.parquet',index=False);mp.to_parquet(root/'meemee_baseline_ledger.parquet',index=False);keep=all(all(gates[x].values()) for x in ('validation','shadow'));payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source_router':str(SOURCE),'eligibility':'fixed gate C/window120','meta_lanes':['family_hit_only_router_top1','meemee_top1'],'rolling':'only completed eligible trades with exit_ymd<decision; minimum sample10','windows_train_only':[40,60,120],'selection':'2024 maximum PF then expectancy','execution':'next-open TP8/SL5/H10/10bp; gap-through actual open; open reentry prohibited','fallback':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)},{'path':str(SOURCE/'compare.json'),'sha256':_sha(SOURCE/'compare.json')}],'source_coverage':cov,'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'branching':branch,'rank_coverage':coverage,'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only'},'shadow_tuning_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
