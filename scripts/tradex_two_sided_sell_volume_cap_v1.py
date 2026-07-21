from __future__ import annotations
import argparse,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,numpy as np,pandas as pd
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_early_signal_first_detect_v1 import _pf
from scripts.tradex_shallow_high_zone_universe_repair_v1 import _sha,_write
from scripts.tradex_two_sided_pit_allocation_v1 import add_exit_ymd
from scripts.tradex_two_sided_allocation_intensity_v1 import allocate,calendar_aligned_metrics,correct_sell_cost_contract

AXIS_ID='tradex_two_sided_sell_volume_cap_v1'
OUT=Path(r'G:\Tradex\tradex_two_sided_sell_volume_cap_v1')
LEDGER_SOURCE=Path(r'G:\Tradex\tradex_two_sided_pit_allocation_v1\20260713T100616Z-tradex_two_sided_pit_allocation_v1')
RANK_SOURCE=Path(r'G:\Tradex\tradex_two_sided_portfolio_union_v1\20260713T100321Z-tradex_two_sided_portfolio_union_v1')
BASE_COMPARE=Path(r'G:\Tradex\tradex_two_sided_allocation_intensity_v1\20260713T122203Z-tradex_two_sided_allocation_intensity_v1\compare.json')
QUANTILES=(.60,.70,.80);PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}

def volume_relation(sell:pd.DataFrame,start:int,end:int,cap:float)->dict:
 q=sell[sell.signal_ymd.between(start,end)].copy()
 if not len(q):return {'n':0}
 q['above_cap']=q.v>cap;tail_n=max(1,int(np.ceil(.1*len(q))));tail=q.nsmallest(tail_n,'trade_return_h10')
 def g(x:pd.DataFrame)->dict:return {'n':len(x),'expectancy':float(x.trade_return_h10.mean()) if len(x) else None,'profit_factor':_pf(x.trade_return_h10) if len(x) else None}
 return {'n':len(q),'volume_return_spearman':float(q[['v','trade_return_h10']].corr(method='spearman').iloc[0,1]) if len(q)>1 else None,'at_or_below_cap':g(q[~q.above_cap]),'above_cap':g(q[q.above_cap]),'worst_tail_n':tail_n,'worst_tail_volume_median':float(tail.v.median()),'all_volume_median':float(q.v.median())}

def evaluate(x:pd.DataFrame,buy:pd.DataFrame,calendar:list[int],start:int,end:int)->dict:
 pr=x.set_index('signal_ymd').portfolio_return;br=buy.groupby('signal_ymd').trade_return_h10.mean()
 return {'portfolio':calendar_aligned_metrics(pr,calendar,start,end),'buy_only':calendar_aligned_metrics(br,calendar,start,end)}

def generate(db:Path,out:Path)->Path:
 buy=pd.read_parquet(LEDGER_SOURCE/'fixed_buy_ledger_with_exit_ymd.parquet')
 sell=correct_sell_cost_contract(pd.read_parquet(LEDGER_SOURCE/'fixed_sell_ledger_with_exit_ymd.parquet'))
 with duckdb.connect(str(db),read_only=True) as q:
  calendar=q.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int)d from daily_bars where source='pan' and date>=epoch(date '2024-01-01') order by d").fetchdf().d.astype(int).tolist()
 latest=max(calendar);periods={**PERIODS,'shadow':(20260101,latest)}
 train=sell[sell.signal_ymd.between(20240101,20241231)];thresholds={str(int(q*100)):float(train.v.quantile(q)) for q in QUANTILES}
 variants=[];timelines={};filtered={}
 for q in QUANTILES:
  cap=thresholds[str(int(q*100))];s=sell[sell.v<=cap].copy();x=allocate(buy,s,.90);m=evaluate(x,buy,calendar,20240101,20241231)['portfolio'];variants.append({'quantile':q,'raw_volume_cap':cap,'train_metrics':m,'eligible_sell_n':len(s),'train_sell_n':len(s[s.signal_ymd.between(20240101,20241231)])});timelines[q]=x;filtered[q]=s
 chosen=max(variants,key=lambda z:((z['train_metrics']['calendar_profit_factor'] if z['train_metrics']['calendar_profit_factor'] is not None else -1),(z['train_metrics']['calendar_expectancy'] if z['train_metrics']['calendar_expectancy'] is not None else -1)))
 q=float(chosen['quantile']);cap=float(chosen['raw_volume_cap']);x=timelines[q];fs=filtered[q];metrics={};gates={}
 for split,(a,z) in periods.items():
  mm=evaluate(x,buy,calendar,a,z);pm=mm['portfolio'];bm=mm['buy_only'];metrics[split]=mm;gates[split]={'pf_ge_1_30':(pm['calendar_profit_factor'] or 0)>=1.3,'expectancy_positive':(pm['calendar_expectancy'] or 0)>0,'cvar_non_degrade_vs_buy':pm['calendar_cvar10'] is not None and bm['calendar_cvar10'] is not None and pm['calendar_cvar10']>=bm['calendar_cvar10'],'dd_non_degrade_vs_buy':pm['calendar_max_drawdown'] is not None and bm['calendar_max_drawdown'] is not None and pm['calendar_max_drawdown']>=bm['calendar_max_drawdown'],'frequency_ge_1_week':pm['signals_per_week']>=1,'same_calendar_n':pm['calendar_sessions']==bm['calendar_sessions'],'same_tail_count':pm['tail_count']==bm['tail_count']}
 keep=all(all(gates[s].values()) for s in ('validation','shadow'))
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False)
 pd.read_parquet(RANK_SOURCE/'all_support_break_sell_scores.parquet').to_parquet(root/'all_support_break_sell_scores.parquet',index=False)
 sell.to_parquet(root/'fixed_sell_ledger_zero_cost.parquet',index=False);fs.to_parquet(root/'selected_sell_ledger.parquet',index=False);x.to_parquet(root/'selected_portfolio_daily.parquet',index=False)
 rank=pd.read_parquet(RANK_SOURCE/'all_support_break_sell_scores.parquet');rank_cov={'all_sell_rank_rows':len(rank),'sell_rank_days':int(rank.signal_ymd.nunique()),'min_ranked_per_day':int(rank.groupby('signal_ymd').size().min()),'all_days_ranked':bool(rank.groupby('signal_ymd').size().gt(0).all()),'rank_rows_removed':0}
 rationale={s:volume_relation(sell,a,z,cap) for s,(a,z) in periods.items()};rationale['selection_use']={'train_2024':True,'validation_2025':False,'shadow_2026':False,'shadow_diagnostic_only':True}
 payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'single_axis':'SELL signal-day raw volume eligibility ceiling only','base_metric_contract':str(BASE_COMPARE),'buy_lane':'fixed MeeMee top10, 10bp','sell_lane':'fixed support-break ledger and exits, 0bp; only entry eligibility can change','allocation':'fixed PIT window120 and stronger side 90/10','caps':'2024 eligible SELL raw-volume p60/p70/p80','selection':'2024 calendar PF maximum, calendar expectancy tie-break','validation':'2025','untouched_shadow':'2026','all_sell_ranks_preserved':True,'survivorship_filter':False,'fallback':False,'tuning_after_2024':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)},{'path':str(BASE_COMPARE),'sha256':_sha(BASE_COMPARE)},{'path':str(RANK_SOURCE/'all_support_break_sell_scores.parquet'),'sha256':_sha(RANK_SOURCE/'all_support_break_sell_scores.parquet')}],'thresholds_2024':thresholds,'train_variants':variants,'selected_variant':chosen,'axis_rationale_volume_tail_relation':rationale,'metrics':metrics,'adoption_gates':gates,'rank_coverage':rank_cov,'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only','reason_type':'validation_shadow_all_gates_pass' if keep else 'validation_or_shadow_gate_failed'},'shadow_threshold_selection_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p

def main():
 p=argparse.ArgumentParser();p.add_argument('--db',required=True,type=Path);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
