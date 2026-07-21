from __future__ import annotations
import argparse,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,numpy as np,pandas as pd
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_shallow_high_zone_universe_repair_v1 import _sha,_write
from scripts.tradex_two_sided_allocation_intensity_v1 import calendar_aligned_metrics,correct_sell_cost_contract

AXIS_ID='tradex_two_sided_sell_only_exposure_cap_v1';OUT=Path(r'G:\Tradex\tradex_two_sided_sell_only_exposure_cap_v1')
LEDGER_SOURCE=Path(r'G:\Tradex\tradex_two_sided_pit_allocation_v1\20260713T100616Z-tradex_two_sided_pit_allocation_v1')
RANK_SOURCE=Path(r'G:\Tradex\tradex_two_sided_portfolio_union_v1\20260713T100321Z-tradex_two_sided_portfolio_union_v1')
BASE=Path(r'G:\Tradex\tradex_two_sided_core_hedge_v1\20260713T123344Z-tradex_two_sided_core_hedge_v1')
CAPS=(.25,.50,.75);BOTH_BUY=.90;BOTH_SELL=.10;SELL_VOLUME_CAP=2284.8;WINDOW=120;PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}

def allocate(buy:pd.DataFrame,sell:pd.DataFrame,sell_only_cap:float)->pd.DataFrame:
 bd=buy.groupby('signal_ymd').trade_return_h10.mean();sd=sell.groupby('signal_ymd').trade_return_h10.mean();rows=[]
 for d in sorted(set(bd.index)|set(sd.index)):
  hb=d in bd.index;hs=d in sd.index
  if hb and hs:bw,sw,cash,reason=BOTH_BUY,BOTH_SELL,0.,'both_fixed_90_10'
  elif hb:bw,sw,cash,reason=1.,0.,0.,'buy_only_full'
  else:bw,sw,cash,reason=0.,sell_only_cap,1-sell_only_cap,'sell_only_capped'
  rows.append({'signal_ymd':int(d),'buy_return':float(bd[d]) if hb else np.nan,'sell_return':float(sd[d]) if hs else np.nan,'buy_weight':bw,'sell_weight':sw,'cash_weight':cash,'gross_exposure':bw+sw,'allocation_reason':reason,'portfolio_return':(float(bd[d])*bw if hb else 0)+(float(sd[d])*sw if hs else 0)})
 return pd.DataFrame(rows)

def evaluate(x:pd.DataFrame,buy:pd.DataFrame,cal:list[int],a:int,z:int)->dict:
 pm=calendar_aligned_metrics(x.set_index('signal_ymd').portfolio_return,cal,a,z);bm=calendar_aligned_metrics(buy.groupby('signal_ymd').trade_return_h10.mean(),cal,a,z);q=x[x.signal_ymd.between(a,z)]
 pm.update({'average_exposure_active_days':float(q.gross_exposure.mean()) if len(q) else None,'average_cash_active_days':float(q.cash_weight.mean()) if len(q) else None,'average_exposure_calendar':float(q.gross_exposure.sum()/pm['calendar_sessions']) if pm['calendar_sessions'] else None,'both_side_days':int((q.allocation_reason=='both_fixed_90_10').sum()),'buy_only_days':int((q.allocation_reason=='buy_only_full').sum()),'sell_only_days':int((q.allocation_reason=='sell_only_capped').sum())})
 return {'portfolio':pm,'buy_only_100pct':bm}

def generate(db:Path,out:Path)->Path:
 buy=pd.read_parquet(LEDGER_SOURCE/'fixed_buy_ledger_with_exit_ymd.parquet');sell=correct_sell_cost_contract(pd.read_parquet(LEDGER_SOURCE/'fixed_sell_ledger_with_exit_ymd.parquet'));sell=sell[sell.v<=SELL_VOLUME_CAP].copy()
 with duckdb.connect(str(db),read_only=True) as q:cal=q.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int)d from daily_bars where source='pan' and date>=epoch(date '2024-01-01') order by d").fetchdf().d.astype(int).tolist()
 latest=max(cal);periods={**PERIODS,'shadow':(20260101,latest)};variants=[];timelines={}
 for c in CAPS:
  x=allocate(buy,sell,c);m=evaluate(x,buy,cal,20240101,20241231)['portfolio'];variants.append({'sell_only_exposure_cap':c,'sell_only_cash':1-c,'train_metrics':m});timelines[c]=x
 chosen=max(variants,key=lambda v:((v['train_metrics']['calendar_profit_factor'] if v['train_metrics']['calendar_profit_factor'] is not None else -1),(v['train_metrics']['calendar_expectancy'] if v['train_metrics']['calendar_expectancy'] is not None else -1)));c=float(chosen['sell_only_exposure_cap']);x=timelines[c];metrics={};gates={}
 for split,(a,z) in periods.items():
  mm=evaluate(x,buy,cal,a,z);pm=mm['portfolio'];bm=mm['buy_only_100pct'];metrics[split]=mm;pfb=pm['calendar_profit_factor']>bm['calendar_profit_factor'];exb=pm['calendar_expectancy']>bm['calendar_expectancy'];gates[split]={'pf_ge_1_30':(pm['calendar_profit_factor'] or 0)>=1.3,'expectancy_positive':(pm['calendar_expectancy'] or 0)>0,'pf_improve_vs_buy':pfb,'expectancy_improve_vs_buy':exb,'pf_or_expectancy_improve_vs_buy':pfb or exb,'cvar_non_degrade_vs_buy':pm['calendar_cvar10']>=bm['calendar_cvar10'],'dd_non_degrade_vs_buy':pm['calendar_max_drawdown']>=bm['calendar_max_drawdown'],'frequency_ge_1_week':pm['signals_per_week']>=1,'same_calendar_n':pm['calendar_sessions']==bm['calendar_sessions'],'same_tail_count':pm['tail_count']==bm['tail_count']}
 keep=all(all(v for k,v in gates[s].items() if k not in ('pf_improve_vs_buy','expectancy_improve_vs_buy')) for s in ('validation','shadow'));root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False)
 for k,t in timelines.items():t.to_parquet(root/f'sell_only_cap_{int(k*100)}.parquet',index=False)
 ranks=pd.read_parquet(RANK_SOURCE/'all_support_break_sell_scores.parquet');ranks.to_parquet(root/'all_support_break_sell_scores.parquet',index=False)
 payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'single_axis':'SELL-only exposure cap only','buy_only':'100%','both_signals':'BUY90/SELL10 fixed roles','sell_only':'cap exposure, remainder cash','caps_2024_only':['25%','50%','75%'],'selection':'2024 calendar PF maximum, calendar expectancy tie-break','base_sell_volume_cap':SELL_VOLUME_CAP,'fixed_window_side_state':WINDOW,'signals_exits_costs_ranks_changed':False,'validation':'2025','untouched_shadow':'2026','benchmark':'BUY-only 100%','main_gate':'both years PF>=1.30, expectancy>0, PF or expectancy improves vs BUY, CVaR/DD non-degrade','fallback':False,'survivorship_filter':False,'tuning_after_2024':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)},{'path':str(BASE/'compare.json'),'sha256':_sha(BASE/'compare.json')}],'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'rank_coverage':{'rows':len(ranks),'days':int(ranks.signal_ymd.nunique()),'min_per_day':int(ranks.groupby('signal_ymd').size().min()),'rows_removed':0},'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only','reason_type':'validation_shadow_all_gates_pass' if keep else 'validation_or_shadow_gate_failed'},'silent_fallback_used':False,'shadow_tuning_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p

def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
