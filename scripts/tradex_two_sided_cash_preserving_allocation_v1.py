from __future__ import annotations
import argparse,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,numpy as np,pandas as pd
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_shallow_high_zone_universe_repair_v1 import _sha,_write
from scripts.tradex_two_sided_pit_allocation_v1 import side_stats
from scripts.tradex_two_sided_allocation_intensity_v1 import calendar_aligned_metrics,correct_sell_cost_contract

AXIS_ID='tradex_two_sided_cash_preserving_allocation_v1';OUT=Path(r'G:\Tradex\tradex_two_sided_cash_preserving_allocation_v1')
LEDGER_SOURCE=Path(r'G:\Tradex\tradex_two_sided_pit_allocation_v1\20260713T100616Z-tradex_two_sided_pit_allocation_v1')
BASE=Path(r'G:\Tradex\tradex_two_sided_sell_volume_cap_v1\20260713T122534Z-tradex_two_sided_sell_volume_cap_v1')
WEIGHTS=(.60,.75,.90);WINDOW=120;SELL_VOLUME_CAP=2284.8;PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}
FAILED_VERIFICATION_ARTIFACT=Path(r'G:\Tradex\tradex_two_sided_cash_preserving_allocation_v1\20260713T122812Z-tradex_two_sided_cash_preserving_allocation_v1')

def performance_key(pf:float|None,expectancy:float)->float:
 if pf is not None:return float(pf)
 return float('inf') if expectancy>0 else 0.

def allocate_cash_preserving(buy:pd.DataFrame,sell:pd.DataFrame,strong:float)->pd.DataFrame:
 bd=buy.groupby('signal_ymd').trade_return_h10.mean();sd=sell.groupby('signal_ymd').trade_return_h10.mean();rows=[]
 for d in sorted(set(bd.index)|set(sd.index)):
  hb=d in bd.index;hs=d in sd.index;bp,be,bn=side_stats(buy,int(d),WINDOW);sp,se,sn=side_stats(sell,int(d),WINDOW);enough=bn>=10 and sn>=10
  if not enough:
   bw=.5 if hb else 0.;sw=.5 if hs else 0.;reason='insufficient_n_both_half' if hb and hs else 'insufficient_n_single_half_cash'
  else:
   buy_strong=performance_key(bp,be)>=performance_key(sp,se)
   bw=(strong if buy_strong else 1-strong) if hb else 0.;sw=(1-strong if buy_strong else strong) if hs else 0.
   reason=('both_buy_strong' if buy_strong else 'both_sell_strong') if hb and hs else (('buy_only_strong' if buy_strong else 'buy_only_weak') if hb else ('sell_only_weak' if buy_strong else 'sell_only_strong'))
  exposure=bw+sw
  rows.append({'signal_ymd':int(d),'buy_return':float(bd[d]) if hb else np.nan,'sell_return':float(sd[d]) if hs else np.nan,'buy_weight':bw,'sell_weight':sw,'cash_weight':1-exposure,'gross_exposure':exposure,'buy_completed_n':bn,'sell_completed_n':sn,'buy_rolling_pf':bp,'sell_rolling_pf':sp,'allocation_reason':reason,'portfolio_return':(float(bd[d])*bw if hb else 0)+(float(sd[d])*sw if hs else 0)})
 return pd.DataFrame(rows)

def eval_metrics(x:pd.DataFrame,buy:pd.DataFrame,cal:list[int],a:int,z:int)->dict:
 pm=calendar_aligned_metrics(x.set_index('signal_ymd').portfolio_return,cal,a,z);bm=calendar_aligned_metrics(buy.groupby('signal_ymd').trade_return_h10.mean(),cal,a,z);q=x[x.signal_ymd.between(a,z)]
 pm.update({'average_exposure_active_days':float(q.gross_exposure.mean()) if len(q) else None,'average_cash_active_days':float(q.cash_weight.mean()) if len(q) else None,'average_exposure_calendar':float(q.gross_exposure.sum()/pm['calendar_sessions']) if pm['calendar_sessions'] else None})
 return {'portfolio':pm,'buy_only_100pct':bm}

def generate(db:Path,out:Path)->Path:
 buy=pd.read_parquet(LEDGER_SOURCE/'fixed_buy_ledger_with_exit_ymd.parquet');sell=correct_sell_cost_contract(pd.read_parquet(LEDGER_SOURCE/'fixed_sell_ledger_with_exit_ymd.parquet'));sell=sell[sell.v<=SELL_VOLUME_CAP].copy()
 with duckdb.connect(str(db),read_only=True) as q:cal=q.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int)d from daily_bars where source='pan' and date>=epoch(date '2024-01-01') order by d").fetchdf().d.astype(int).tolist()
 latest=max(cal);periods={**PERIODS,'shadow':(20260101,latest)};variants=[];timelines={}
 for w in WEIGHTS:
  x=allocate_cash_preserving(buy,sell,w);m=eval_metrics(x,buy,cal,20240101,20241231)['portfolio'];variants.append({'stronger_side_weight':w,'weaker_side_weight':1-w,'train_metrics':m});timelines[w]=x
 chosen=max(variants,key=lambda v:((v['train_metrics']['calendar_profit_factor'] if v['train_metrics']['calendar_profit_factor'] is not None else -1),(v['train_metrics']['calendar_expectancy'] if v['train_metrics']['calendar_expectancy'] is not None else -1)));w=float(chosen['stronger_side_weight']);x=timelines[w];metrics={};gates={}
 for split,(a,z) in periods.items():
  mm=eval_metrics(x,buy,cal,a,z);pm=mm['portfolio'];bm=mm['buy_only_100pct'];metrics[split]=mm;gates[split]={'pf_ge_1_30':(pm['calendar_profit_factor'] or 0)>=1.3,'expectancy_positive':(pm['calendar_expectancy'] or 0)>0,'pf_improve_vs_buy':pm['calendar_profit_factor'] is not None and bm['calendar_profit_factor'] is not None and pm['calendar_profit_factor']>bm['calendar_profit_factor'],'expectancy_improve_vs_buy':pm['calendar_expectancy'] is not None and bm['calendar_expectancy'] is not None and pm['calendar_expectancy']>bm['calendar_expectancy'],'cvar_non_degrade_vs_buy':pm['calendar_cvar10'] is not None and bm['calendar_cvar10'] is not None and pm['calendar_cvar10']>=bm['calendar_cvar10'],'dd_non_degrade_vs_buy':pm['calendar_max_drawdown'] is not None and bm['calendar_max_drawdown'] is not None and pm['calendar_max_drawdown']>=bm['calendar_max_drawdown'],'frequency_ge_1_week':pm['signals_per_week']>=1,'same_calendar_n':pm['calendar_sessions']==bm['calendar_sessions'],'same_tail_count':pm['tail_count']==bm['tail_count']}
 keep=all(all(gates[s].values()) for s in ('validation','shadow'));root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False)
 for k,t in timelines.items():t.to_parquet(root/f'cash_preserving_strong{int(k*100)}.parquet',index=False)
 payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','failed_verification_artifact':{'path':str(FAILED_VERIFICATION_ARTIFACT),'status':'invalid_failed_focused_tests','authoritative':False},'allocation_semantics_contract_correction':{'changed_axis':'cash-preserving allocation semantics only','base_strategy':str(BASE),'base_status':'reallocation_contract_mismatch','missing_side_reallocation':False,'cash_return':0,'future_tuning':False},'fixed_evaluation_conditions':{'buy_lane':'fixed MeeMee top10 10bp','sell_lane':f'fixed support-break 0bp raw volume <= {SELL_VOLUME_CAP}','window':120,'signals_exits_costs_ranks_changed':False,'variants_2024_only':['60/40','75/25','90/10'],'both_signals':'strong/weak weights','single_signal':'active side receives its strong/weak weight; remainder cash','insufficient_n':'both signals 50/50; single signal 50% and 50% cash','selection':'2024 calendar PF maximum, calendar expectancy tie-break','validation':'2025','untouched_shadow':'2026','benchmark':'BUY-only 100%','fallback':False,'survivorship_filter':False,'tuning_after_2024':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)},{'path':str(BASE/'compare.json'),'sha256':_sha(BASE/'compare.json')}],'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only','reason_type':'validation_shadow_all_gates_pass' if keep else 'validation_or_shadow_gate_failed'},'silent_fallback_used':False,'shadow_tuning_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p

def main():
 p=argparse.ArgumentParser();p.add_argument('--db',required=True,type=Path);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
