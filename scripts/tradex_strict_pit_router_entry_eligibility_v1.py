from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import duckdb,numpy as np,pandas as pd
from scripts.tradex_early_signal_first_detect_v1 import _metrics,_branching
from scripts.tradex_contraction_sequence_challenger_v1 import load_source as load_contraction,build_scores as contraction_scores
from scripts.tradex_ma200_weekly_reversal_current_exit_v1 import build_frame as ma_frame,score as ma_score
from scripts.tradex_shallow_high_zone_universe_repair_v1 import attach_gap_outcomes,no_reentry_sessions,_sha,_write,extra_metrics
from scripts.tradex_strict_pit_adaptive_buy_router_v1 import riskoff_scores,add_exit_ymd,lane_stats
AXIS_ID='tradex_strict_pit_router_entry_eligibility_v1';OUT=Path(r'G:\Tradex\tradex_strict_pit_router_entry_eligibility_v1');ROUTER=Path(r'G:\Tradex\tradex_strict_pit_adaptive_buy_router_v1\20260713T091826Z-tradex_strict_pit_adaptive_buy_router_v1');GATES=('A','B','C');PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}
def gate_pass(kind:str,pf:float,exp:float,top_hit:bool)->bool:
 return pf>=(1.3 if kind=='B' else 1.2) and exp>0 and (kind!='C' or top_hit)
def select_top1(scores:pd.DataFrame,outcomes:pd.DataFrame,elig:pd.DataFrame,rank_col:str,calendar:list[int])->pd.DataFrame:
 x=scores.merge(elig[['signal_ymd','eligible']],on='signal_ymd',how='left');x=x[x.eligible].sort_values(['signal_ymd',rank_col,'code']).groupby('signal_ymd',as_index=False).head(1);x=x.merge(outcomes,on=['signal_ymd','code'],how='left',validate='one_to_one');return no_reentry_sessions(x[x.trade_return_h10.notna()],calendar,rank_col)
def generate(db:Path,out:Path,hit_only:bool=False,axis_id:str=AXIS_ID)->Path:
 f,b,r,cov=load_contraction(db)
 with duckdb.connect(str(db),read_only=True) as q:vol=q.execute("select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code,v from daily_bars where source='pan'").fetchdf()
 b=b.merge(vol,on=['signal_ymd','code'],how='left',validate='one_to_one');common=attach_gap_outcomes(ma_frame(f[['signal_ymd','code']],b),b);outs=common[['signal_ymd','code','trade_return_h10','exit_day_h10','target_before_stop20','realized_mover20']];cal=sorted(f[f.signal_ymd>=20240101].signal_ymd.unique())
 risk=riskoff_scores(f,b).merge(outs,on=['signal_ymd','code'],how='left');con=contraction_scores(f,b,.08).merge(outs,on=['signal_ymd','code'],how='left');ma=ma_score(ma_frame(f[['signal_ymd','code']],b),'support_distance').merge(outs,on=['signal_ymd','code'],how='left');risk=risk[risk.signal_ymd>=20240101];con=con[con.signal_ymd>=20240101];ma=ma[ma.signal_ymd>=20240101]
 s=pd.read_parquet(ROUTER/'all_symbol_daily_scores.parquet');actual=s[['signal_ymd','code','lane']]
 lanes={};
 for name,z in [('riskoff',risk),('contraction',con),('ma200',ma)]:
  pool=z.merge(actual,on=['signal_ymd','code'],how='left',validate='one_to_one');mask=pool.top10&pool.trade_return_h10.notna()
  if hit_only:mask=mask&pool.family_hit&pool.lane.eq(name)
  lanes[name]=add_exit_ymd(no_reentry_sessions(pool[mask],cal,'rank'),b)
 mm=r.merge(outs,on=['signal_ymd','code'],how='left').merge(actual,on=['signal_ymd','code'],how='left');mask=mm.trade_return_h10.notna()
 if hit_only:mask=mask&mm.lane.eq('meemee')
 mm=no_reentry_sessions(mm[mask],cal,'baseline_rank');lanes['meemee']=add_exit_ymd(mm,b)
 top=s.sort_values(['signal_ymd','rank','code']).groupby('signal_ymd',as_index=False).head(1);hitmaps={'riskoff':risk.set_index(['signal_ymd','code']).family_hit,'contraction':con.set_index(['signal_ymd','code']).family_hit,'ma200':ma.set_index(['signal_ymd','code']).family_hit};rows=[]
 for q in top.itertuples():
  ps={'riskoff':q.family_priority_riskoff,'contraction':q.family_priority_contraction,'ma200':q.family_priority_ma200,'meemee':q.family_priority_meemee};fam=max(ps,key=lambda k:(ps[k],k));pf,exp,n=lane_stats(lanes[fam],int(q.signal_ymd),120);hit=True if fam=='meemee' else bool(hitmaps[fam].get((int(q.signal_ymd),str(q.code)),False));rows.append({'signal_ymd':int(q.signal_ymd),'selected_family':fam,'actual_top1_lane':q.lane,'lane_match':fam==q.lane,'rolling_pf':pf,'rolling_expectancy':exp,'completed_n':n,'top_rank_family_hit':hit})
 state=pd.DataFrame(rows);variants=[];selected_events={};td=sum(d<=20241231 for d in cal)
 for g in GATES:
  e=state.copy();e['eligible']=[gate_pass(g,p,x,h) for p,x,h in zip(e.rolling_pf,e.rolling_expectancy,e.top_rank_family_hit)];
  if hit_only:e['eligible']=e.eligible&e.lane_match
  ev=select_top1(s[['signal_ymd','code','rank']],outs,e,'rank',cal);m=_metrics(ev[ev.signal_ymd<=20241231],td);variants.append({'gate':g,'train_metrics':m,'eligible_days_train':int(e[e.signal_ymd<=20241231].eligible.sum())});selected_events[g]=(e,ev)
 ok=[v for v in variants if (v['train_metrics']['expectancy'] or 0)>0 and v['train_metrics']['profit_factor'] is not None and v['train_metrics']['signals_per_week']>=1]
 if not ok:raise ValueError('NO_2024_GATE_PASS')
 chosen=max(ok,key=lambda v:(v['train_metrics']['profit_factor'],v['train_metrics']['expectancy']));elig,ev=selected_events[chosen['gate']];base_scores=r.rename(columns={'baseline_rank':'rank'})[['signal_ymd','code','rank']];base=select_top1(base_scores,outs,elig,'rank',cal);latest=int(f.signal_ymd.max());periods={**PERIODS,'shadow':(20260101,latest)};metrics={};gates={};branch={};coverage={}
 for split,(a,z) in periods.items():
  days=sum(a<=d<=z for d in cal);ch=ev[ev.signal_ymd.between(a,z)];bl=base[base.signal_ymd.between(a,z)];allx=common[common.signal_ymd.between(a,z)&common.trade_return_h10.notna()];cm={**_metrics(ch,days),**extra_metrics(ch,allx)};bm={**_metrics(bl,days),**extra_metrics(bl,allx)};metrics[split]={'router_eligible_top1':cm,'meemee_same_eligible_days_top1':bm};gates[split]={'pf_ge_1_30':(cm['profit_factor'] or 0)>=1.3,'expectancy_positive':(cm['expectancy'] or 0)>0,'pf_improves':cm['profit_factor'] is not None and bm['profit_factor'] is not None and cm['profit_factor']>bm['profit_factor'],'expectancy_improves':cm['expectancy'] is not None and bm['expectancy'] is not None and cm['expectancy']>bm['expectancy'],'cvar_non_degrade':cm['cvar10'] is not None and bm['cvar10'] is not None and cm['cvar10']>=bm['cvar10'],'dd_non_degrade':cm['max_drawdown'] is not None and bm['max_drawdown'] is not None and cm['max_drawdown']>=bm['max_drawdown']};branch[split]=_branching(s[s.top10],r.assign(side='BUY'),'BUY',a,z);cnt=s[s.signal_ymd.between(a,z)].groupby('signal_ymd').size();coverage[split]={'days':len(cnt),'min_ranked':int(cnt.min()),'all_days_ranked':bool(cnt.gt(0).all()),'eligible_days':int(elig[elig.signal_ymd.between(a,z)].eligible.sum())}
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{axis_id}";root.mkdir(parents=True,exist_ok=False);s.merge(elig,on='signal_ymd',how='left').to_parquet(root/'all_symbol_daily_scores_and_eligibility.parquet',index=False);ev.to_parquet(root/'router_entry_ledger.parquet',index=False);base.to_parquet(root/'meemee_fair_baseline_ledger.parquet',index=False);keep=all(all(gates[x].values()) for x in ('validation','shadow'));payload={'schema_version':axis_id+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','axis_id':axis_id,'fixed_evaluation_conditions':{'router':str(ROUTER),'router_window':120,'rolling_population':'family_hit=True and actual lane match only; MeeMee hit=True' if hit_only else 'all completed lane events','lane_mismatch':'entry prohibited' if hit_only else 'not applicable','entry_variants':{'A':'PF>=1.2 and exp>0','B':'PF>=1.3 and exp>0','C':'A plus top-rank family_hit'},'selection':'2024 maximum PF among exp>0 and >=1 signal day/week','entry':'eligible day top1 only; next-open TP8/SL5/H10/10bp; gap-through actual open; reentry prohibited','baseline':'MeeMee top1 on exact same eligible days','noneligible':'all ranks and direction hypotheses persisted; no entry','fallback':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)},{'path':str(ROUTER/'compare.json'),'sha256':_sha(ROUTER/'compare.json')}],'source_coverage':cov,'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'branching':branch,'rank_coverage':coverage,'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only'},'shadow_tuning_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
