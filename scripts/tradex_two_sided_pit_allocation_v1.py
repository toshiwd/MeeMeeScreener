from __future__ import annotations
import argparse,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,numpy as np,pandas as pd
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_early_signal_first_detect_v1 import _pf,_metrics
from scripts.tradex_shallow_high_zone_universe_repair_v1 import _sha,_write
from scripts.tradex_two_sided_portfolio_union_v1 import portfolio_metrics
AXIS_ID='tradex_two_sided_pit_allocation_v1';OUT=Path(r'G:\Tradex\tradex_two_sided_pit_allocation_v1');SOURCE=Path(r'G:\Tradex\tradex_two_sided_portfolio_union_v1\20260713T100321Z-tradex_two_sided_portfolio_union_v1');WINDOWS=(40,60,120);PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}
def add_exit_ymd(e:pd.DataFrame,calendar:list[int])->pd.DataFrame:
 pos={d:i for i,d in enumerate(calendar)};x=e.copy();x['exit_ymd']=[calendar[min(len(calendar)-1,pos[int(d)]+int(o))] for d,o in zip(x.signal_ymd,x.exit_day_h10)];return x
def side_stats(e:pd.DataFrame,day:int,window:int)->tuple[float,float,int]:
 q=e[e.exit_ymd<day].sort_values(['exit_ymd','signal_ymd']).tail(window);return (_pf(q.trade_return_h10),float(q.trade_return_h10.mean()) if len(q) else 0.,len(q))
def allocate(buy:pd.DataFrame,sell:pd.DataFrame,window:int)->tuple[dict,pd.DataFrame]:
 bd=buy.groupby('signal_ymd').trade_return_h10.mean();sd=sell.groupby('signal_ymd').trade_return_h10.mean();rows=[]
 for d in sorted(set(bd.index)|set(sd.index)):
  hb=d in bd.index;hs=d in sd.index;bp,be,bn=side_stats(buy,int(d),window);sp,se,sn=side_stats(sell,int(d),window)
  if hb and hs:
   if bn<10 or sn<10:bw=sw=.5;reason='insufficient_n'
   elif bp>=sp:bw,sw=.75,.25;reason='buy_pf_higher'
   else:bw,sw=.25,.75;reason='sell_pf_higher'
  elif hb:bw,sw,reason=1.,0.,'buy_only_signal'
  else:bw,sw,reason=0.,1.,'sell_only_signal'
  rows.append({'signal_ymd':int(d),'buy_return':float(bd[d]) if hb else np.nan,'sell_return':float(sd[d]) if hs else np.nan,'buy_weight':bw,'sell_weight':sw,'buy_completed_n':bn,'sell_completed_n':sn,'buy_rolling_pf':bp,'sell_rolling_pf':sp,'buy_rolling_exp':be,'sell_rolling_exp':se,'allocation_reason':reason,'portfolio_return':(float(bd[d])*bw if hb else 0)+(float(sd[d])*sw if hs else 0)})
 x=pd.DataFrame(rows);return {},x
def daily_metrics(x:pd.DataFrame,a:int,z:int)->dict:
 q=x[x.signal_ymd.between(a,z)];r=q.portfolio_return;curve=(1+r).cumprod();dd=curve/curve.cummax()-1;cut=r.quantile(.1);weeks=pd.to_datetime(q.signal_ymd.astype(str),format='%Y%m%d').dt.strftime('%G-W%V').nunique();return {'signal_days':len(q),'profit_factor':_pf(r),'expectancy':float(r.mean()) if len(r) else None,'cvar10':float(r[r<=cut].mean()) if len(r) else None,'max_drawdown':float(dd.min()) if len(r) else None,'signals_per_week':float(len(r)/weeks) if weeks else 0.}
def generate(db:Path,out:Path)->Path:
 if not SOURCE.exists():raise FileNotFoundError(SOURCE)
 buy=pd.read_parquet(SOURCE/'buy_top10_ledger.parquet');sell=pd.read_parquet(SOURCE/'sell_wait1_ledger.parquet')
 with duckdb.connect(str(db),read_only=True) as q:cal=q.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) d from daily_bars where source='pan' and date>=epoch(date '2024-01-01') order by d").fetchdf().d.astype(int).tolist();latest=max(cal)
 buy=add_exit_ymd(buy,cal);sell=add_exit_ymd(sell,cal);variants=[];timelines={}
 for w in WINDOWS:
  _,x=allocate(buy,sell,w);m=daily_metrics(x,20240101,20241231);variants.append({'completed_trade_window':w,'train_metrics':m});timelines[w]=x
 chosen=max(variants,key=lambda v:((v['train_metrics']['profit_factor'] if v['train_metrics']['profit_factor'] is not None else -1),v['train_metrics']['expectancy'] if v['train_metrics']['expectancy'] is not None else -1));x=timelines[chosen['completed_trade_window']];periods={**PERIODS,'shadow':(20260101,latest)};metrics={};gates={}
 _,fixed=portfolio_metrics(buy,sell,20240101,latest);fixed=fixed.rename(columns={'portfolio_return':'fixed_return'});buyday=buy.groupby('signal_ymd').trade_return_h10.mean().rename('buy_return').reset_index()
 for split,(a,z) in periods.items():
  pm=daily_metrics(x,a,z);bm=_metrics(buy[buy.signal_ymd.between(a,z)],sum(a<=d<=z for d in cal));fm=daily_metrics(fixed.rename(columns={'fixed_return':'portfolio_return'}),a,z);metrics[split]={'pit_allocation':pm,'buy_only':bm,'fixed_50_50_union':fm};gates[split]={'pf_ge_1_30':bool(pm and (pm['profit_factor'] or 0)>=1.3),'expectancy_positive':bool(pm and (pm['expectancy'] or 0)>0),'cvar_non_degrade_vs_buy':bool(pm and pm['cvar10'] is not None and bm['cvar10'] is not None and pm['cvar10']>=bm['cvar10']),'dd_non_degrade_vs_buy':bool(pm and pm['max_drawdown'] is not None and bm['max_drawdown'] is not None and pm['max_drawdown']>=bm['max_drawdown'])}
 keep=all(all(gates[s].values()) for s in ('validation','shadow'));root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);buy.to_parquet(root/'fixed_buy_ledger_with_exit_ymd.parquet',index=False);sell.to_parquet(root/'fixed_sell_ledger_with_exit_ymd.parquet',index=False)
 for w,t in timelines.items():t.to_parquet(root/f'weights_window{w}.parquet',index=False)
 payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'single_axis':'PIT two-sided allocation window only','fixed_ledgers':str(SOURCE),'rolling':'side completed trades with exit_ymd strictly before decision day; most recent 40/60/120 trades','both_side':'higher historical PF side 75%, lower side 25%; nonpositive expectancy side remains 25%','insufficient':'either side n<10 => 50/50','one_side':'100% active side','selection':'2024 maximum calendar PF; tie expectancy','validation':'2025','untouched_shadow':'2026','benchmarks':['BUY-only','fixed 50/50 union'],'signals_exits_ranks_changed':False,'survivorship_filter':False,'fallback':False,'tuning_after_2024':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)},{'path':str(SOURCE/'compare.json'),'sha256':_sha(SOURCE/'compare.json')}],'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only'},'shadow_tuning_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
