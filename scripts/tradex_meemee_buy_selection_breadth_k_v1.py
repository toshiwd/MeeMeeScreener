from __future__ import annotations
import argparse,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_early_signal_first_detect_v1 import _metrics
from scripts.tradex_shallow_high_zone_universe_repair_v1 import attach_gap_outcomes,no_reentry_sessions,_sha,_write,extra_metrics
AXIS_ID='tradex_meemee_buy_selection_breadth_k_v1';OUT=Path(r'G:\Tradex\tradex_meemee_buy_selection_breadth_k_v1');KS=(3,5,10);PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}
def load(db:Path):
 with duckdb.connect(str(db),read_only=True) as q:
  b=q.execute("select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code,o,h,l,c from daily_bars where source='pan' order by code,signal_ymd").fetchdf();r=q.execute("select dt signal_ymd,cast(code as varchar) code,rank from ranking_appearance_daily where ranking_logic_version='ranking:trade:top50:v1' and dir='up' and rank<=50 and dt between 20240101 and 20261231 order by dt,rank,code").fetchdf()
 return b,r,{'pan_rows':len(b),'pan_codes':b.code.nunique(),'pan_min_date':int(b.signal_ymd.min()),'pan_max_date':int(b.signal_ymd.max()),'ranking_rows_top50':len(r),'ranking_days':r.signal_ymd.nunique()}
def complete_metrics(e:pd.DataFrame,days:int,eligible:pd.DataFrame)->dict:
 m={**_metrics(e,days),**extra_metrics(e,eligible)};m['event_expectancy']=float(e.trade_return_h10.mean()) if len(e) else None;return m
def generate(db:Path,out:Path)->Path:
 b,r,cov=load(db);universe=b[['signal_ymd','code']].copy();outs=attach_gap_outcomes(universe,b)[['signal_ymd','code','trade_return_h10','exit_day_h10','target_before_stop20','realized_mover20']];ranked=r.merge(outs,on=['signal_ymd','code'],how='left',validate='one_to_one');cal=sorted(r.signal_ymd.unique());td=sum(d<=20241231 for d in cal);variants=[];ledgers={};eligible_all=ranked[ranked.trade_return_h10.notna()]
 for k in KS:
  raw=ranked[ranked['rank'].le(k)&ranked.trade_return_h10.notna()];e=no_reentry_sessions(raw,cal,'rank');m=complete_metrics(e[e.signal_ymd<=20241231],td,eligible_all[eligible_all.signal_ymd<=20241231]);variants.append({'k':k,'train_metrics':m});ledgers[k]=e
 ok=[v for v in variants if (v['train_metrics']['event_expectancy'] or 0)>0 and v['train_metrics']['event_profit_factor'] is not None];chosen=max(ok,key=lambda v:(v['train_metrics']['event_profit_factor'],v['train_metrics']['event_expectancy'])) if ok else None;e=ledgers[chosen['k']] if chosen else None;bench=ledgers[10];latest=int(r.signal_ymd.max());periods={**PERIODS,'shadow':(20260101,latest)};metrics={};gates={}
 for split,(a,z) in periods.items():
  days=sum(a<=d<=z for d in cal);elig=eligible_all[eligible_all.signal_ymd.between(a,z)];cm=complete_metrics(e[e.signal_ymd.between(a,z)],days,elig) if e is not None else None;bm=complete_metrics(bench[bench.signal_ymd.between(a,z)],days,elig);metrics[split]={'selected_k':cm,'K10_benchmark':bm};gates[split]={'event_pf_ge_1_30':bool(cm and (cm['event_profit_factor'] or 0)>=1.3),'event_expectancy_positive':bool(cm and (cm['event_expectancy'] or 0)>0),'cvar_non_degrade_vs_K10':bool(cm and cm['cvar10'] is not None and bm['cvar10'] is not None and cm['cvar10']>=bm['cvar10']),'dd_non_degrade_vs_K10':bool(cm and cm['max_drawdown'] is not None and bm['max_drawdown'] is not None and cm['max_drawdown']>=bm['max_drawdown'])}
 keep=chosen is not None and all(all(gates[x].values()) for x in ('validation','shadow'));root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);ranked.to_parquet(root/'fixed_meemee_buy_all_ranks_with_outcomes.parquet',index=False)
 for k,v in ledgers.items():v.to_parquet(root/f'K{k}_entry_ledger.parquet',index=False)
 payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'single_axis':'daily selection breadth K only','selection':'formal MeeMee BUY ranking fixed; K=3/5/10 predeclared','ranking':'all formal ranks retained','entry':'each daily rank<=K entered unless same code remains open','execution':'next-open TP8/SL5/H10/10bp; gap-through actual open; stop-first','metrics':'individual trade event PF/expectancy and calendar equal-weight daily PF/expectancy both recorded','train_selection':'2024 maximum event PF among event expectancy>0','validation':'2025','untouched_shadow':'2026','benchmark':'K10','survivorship_filter':False,'fallback':False,'tuning_after_2024':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)}],'source_coverage':cov,'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'rank_coverage':{'ranking_days':int(r.signal_ymd.nunique()),'min_ranked':int(r.groupby('signal_ymd').size().min()),'all_days_ranked':bool(r.groupby('signal_ymd').size().gt(0).all())},'branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'rank membership unchanged; only entry breadth K differs'},'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only'},'shadow_tuning_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
