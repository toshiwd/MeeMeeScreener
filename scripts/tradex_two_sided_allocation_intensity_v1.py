from __future__ import annotations
import argparse,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,numpy as np,pandas as pd
import math
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_early_signal_first_detect_v1 import _metrics
from scripts.tradex_shallow_high_zone_universe_repair_v1 import _sha,_write
from scripts.tradex_two_sided_pit_allocation_v1 import side_stats,daily_metrics
AXIS_ID='tradex_two_sided_allocation_intensity_v1';OUT=Path(r'G:\Tradex\tradex_two_sided_allocation_intensity_v1');SOURCE=Path(r'G:\Tradex\tradex_two_sided_pit_allocation_v1\20260713T100616Z-tradex_two_sided_pit_allocation_v1');WEIGHTS=(.60,.75,.90);WINDOW=120;PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}
SELL_COST_CONTRACT_CORRECTION=.001
SUPERSEDED=Path(r'G:\Tradex\tradex_two_sided_allocation_intensity_v1\20260713T120859Z-tradex_two_sided_allocation_intensity_v1\compare.json')
FIXED_STRATEGY_ARTIFACT=Path(r'G:\Tradex\tradex_two_sided_allocation_intensity_v1\20260713T121150Z-tradex_two_sided_allocation_intensity_v1')
def correct_sell_cost_contract(sell:pd.DataFrame)->pd.DataFrame:
 x=sell.copy();x['trade_return_h10']=x.trade_return_h10+SELL_COST_CONTRACT_CORRECTION;return x
def allocate(buy:pd.DataFrame,sell:pd.DataFrame,strong:float)->pd.DataFrame:
 bd=buy.groupby('signal_ymd').trade_return_h10.mean();sd=sell.groupby('signal_ymd').trade_return_h10.mean();rows=[]
 for d in sorted(set(bd.index)|set(sd.index)):
  hb=d in bd.index;hs=d in sd.index;bp,be,bn=side_stats(buy,int(d),WINDOW);sp,se,sn=side_stats(sell,int(d),WINDOW)
  if hb and hs:
   if bn<10 or sn<10:bw=sw=.5;reason='insufficient_n'
   elif bp>=sp:bw,sw=strong,1-strong;reason='buy_pf_higher'
   else:bw,sw=1-strong,strong;reason='sell_pf_higher'
  elif hb:bw,sw,reason=1.,0.,'buy_only_signal'
  else:bw,sw,reason=0.,1.,'sell_only_signal'
  rows.append({'signal_ymd':int(d),'buy_return':float(bd[d]) if hb else np.nan,'sell_return':float(sd[d]) if hs else np.nan,'buy_weight':bw,'sell_weight':sw,'buy_completed_n':bn,'sell_completed_n':sn,'buy_rolling_pf':bp,'sell_rolling_pf':sp,'allocation_reason':reason,'portfolio_return':(float(bd[d])*bw if hb else 0)+(float(sd[d])*sw if hs else 0)})
 return pd.DataFrame(rows)
def calendar_aligned_metrics(returns:pd.Series,calendar:list[int],start:int,end:int)->dict:
 days=[int(d) for d in calendar if start<=int(d)<=end]
 by_day=returns.groupby(level=0).mean() if len(returns) else pd.Series(dtype=float)
 r=by_day.reindex(days,fill_value=0.).astype(float)
 curve=(1+r).cumprod();dd=curve/curve.cummax()-1
 tail_n=max(1,math.ceil(.10*len(r))) if len(r) else 0
 tail=r.sort_values(kind='stable').iloc[:tail_n]
 positive=float(r[r>0].sum());negative=float(-r[r<0].sum())
 weeks=pd.to_datetime(pd.Series(days,dtype=str),format='%Y%m%d').dt.strftime('%G-W%V').nunique() if days else 0
 return {'calendar_sessions':len(r),'tail_count':tail_n,'calendar_profit_factor':positive/negative if negative else (float('inf') if positive else None),'calendar_expectancy':float(r.mean()) if len(r) else None,'calendar_cvar10':float(tail.mean()) if tail_n else None,'calendar_max_drawdown':float(dd.min()) if len(r) else None,'active_signal_days':int((r!=0).sum()),'signals_per_week':float((r!=0).sum()/weeks) if weeks else 0.}
def generate(db:Path,out:Path)->Path:
 buy=pd.read_parquet(SOURCE/'fixed_buy_ledger_with_exit_ymd.parquet');sell=correct_sell_cost_contract(pd.read_parquet(SOURCE/'fixed_sell_ledger_with_exit_ymd.parquet'))
 with duckdb.connect(str(db),read_only=True) as q:cal=q.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int)d from daily_bars where source='pan' and date>=epoch(date '2024-01-01') order by d").fetchdf().d.astype(int).tolist();latest=max(cal)
 variants=[];timelines={}
 for w in WEIGHTS:
  x=allocate(buy,sell,w);m=daily_metrics(x,20240101,20241231);variants.append({'stronger_side_weight':w,'weaker_side_weight':1-w,'train_metrics':m});timelines[w]=x
 chosen=next(v for v in variants if v['stronger_side_weight']==.90);x=pd.read_parquet(FIXED_STRATEGY_ARTIFACT/'weights_strong90.parquet');periods={**PERIODS,'shadow':(20260101,latest)};metrics={};gates={}
 for split,(a,z) in periods.items():
  pm_active=daily_metrics(x,a,z);bm_active=_metrics(buy[buy.signal_ymd.between(a,z)],sum(a<=d<=z for d in cal))
  pr=x.set_index('signal_ymd').portfolio_return; br=buy.groupby('signal_ymd').trade_return_h10.mean()
  pm=calendar_aligned_metrics(pr,cal,a,z);bm=calendar_aligned_metrics(br,cal,a,z)
  metrics[split]={'calendar_aligned':{'intensity_portfolio':pm,'buy_only':bm},'noncomparable_active_day_metrics':{'status':'noncomparable_metric','intensity_portfolio':pm_active,'buy_only':bm_active}}
  gates[split]={'pf_ge_1_30':(pm['calendar_profit_factor'] or 0)>=1.3,'expectancy_positive':(pm['calendar_expectancy'] or 0)>0,'cvar_non_degrade_vs_buy':pm['calendar_cvar10'] is not None and bm['calendar_cvar10'] is not None and pm['calendar_cvar10']>=bm['calendar_cvar10'],'dd_non_degrade_vs_buy':pm['calendar_max_drawdown'] is not None and bm['calendar_max_drawdown'] is not None and pm['calendar_max_drawdown']>=bm['calendar_max_drawdown'],'same_calendar_n':pm['calendar_sessions']==bm['calendar_sessions'],'same_tail_count':pm['tail_count']==bm['tail_count']}
 keep=all(all(gates[s].values()) for s in ('validation','shadow'));root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False)
 for w,t in timelines.items():t.to_parquet(root/f'weights_strong{int(w*100)}.parquet',index=False)
 payload={'schema_version':AXIS_ID+'.compare.v3','artifact_role':'authoritative','research_phase':'effectiveness_judgment','metric_contract_correction':{'reason':'portfolio and BUY-only risk metrics must use the same PAN trading-session calendar and equal tail count','changed_axis':'metric alignment only','strategy_source_fixed':str(FIXED_STRATEGY_ARTIFACT),'strategy_signals_exits_costs_ranks_changed':False,'non_signal_return':0,'tail_count':'ceil(10% * same split calendar sessions)','prior_active_day_cvar_status':'noncomparable_metric','future_tuning':False},'fixed_evaluation_conditions':{'single_axis':'metric contract correction only','fixed_completed_trade_window':120,'fixed_selected_intensity':'90/10','fixed_side_pf_judgment':'only trades exit_ymd<decision day','fixed_ledgers_exits_signals_ranks':str(SOURCE),'buy_cost_bps':10,'sell_cost_bps':0,'validation':'2025','untouched_shadow':'2026','fallback':False,'tuning_after_2024':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)},{'path':str(FIXED_STRATEGY_ARTIFACT/'compare.json'),'sha256':_sha(FIXED_STRATEGY_ARTIFACT/'compare.json')}],'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only'},'shadow_tuning_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
