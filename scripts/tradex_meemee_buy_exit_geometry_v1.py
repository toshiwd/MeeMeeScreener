from __future__ import annotations
import argparse,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,numpy as np,pandas as pd
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_early_signal_first_detect_v1 import _metrics
from scripts.tradex_shallow_high_zone_universe_repair_v1 import no_reentry_sessions,_sha,_write
AXIS_ID='tradex_meemee_buy_exit_geometry_v1';OUT=Path(r'G:\Tradex\tradex_meemee_buy_exit_geometry_v1');EXITS={'A':(.05,.03,5),'B':(.06,.04,7),'C':(.08,.05,10)};PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}
def load(db:Path):
 with duckdb.connect(str(db),read_only=True) as q:
  b=q.execute("select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code,o,h,l,c from daily_bars where source='pan' order by code,signal_ymd").fetchdf();r=q.execute("select dt signal_ymd,cast(code as varchar) code,rank from ranking_appearance_daily where ranking_logic_version='ranking:trade:top50:v1' and dir='up' and rank<=50 and dt between 20240101 and 20261231 order by dt,rank,code").fetchdf()
 return b,r,{'pan_rows':len(b),'pan_codes':b.code.nunique(),'pan_min_date':int(b.signal_ymd.min()),'pan_max_date':int(b.signal_ymd.max()),'ranking_rows_top50':len(r),'ranking_days':r.signal_ymd.nunique()}
def outcomes(candidates:pd.DataFrame,bars:pd.DataFrame,tp:float,sl:float,horizon:int)->pd.DataFrame:
 rows=[]
 for code,p in bars.groupby('code',sort=False):
  p=p.sort_values('signal_ymd').reset_index(drop=True);n=len(p);entry=p.o.shift(-1).to_numpy(float);ret=np.full(n,np.nan);off=np.zeros(n,np.int16);target=np.zeros(n,np.int8)
  for d in range(1,horizon+1):
   op=p.o.shift(-d).to_numpy(float);hi=p.h.shift(-d).to_numpy(float);lo=p.l.shift(-d).to_numpy(float);u=off==0;gs=op<=entry*(1-sl);gt=op>=entry*(1+tp);stop=lo<=entry*(1-sl);take=hi>=entry*(1+tp);ev=u&(gs|gt|stop|take);off[ev]=d;rv=np.where(gs,op/entry-1,np.where(gt,op/entry-1,np.where(stop,-sl,tp)));ret[ev]=rv[ev];target[ev]=(gt|(~gs&~stop&take))[ev]
  valid=np.arange(n)+horizon<n;u=(off==0)&valid;ret[u]=p.c.shift(-horizon).to_numpy(float)[u]/entry[u]-1;off[u]=horizon;rows.append(pd.DataFrame({'signal_ymd':p.signal_ymd,'code':str(code),'trade_return_h10':np.where(valid,ret-.001,np.nan),'exit_day_h10':np.where(valid,off,np.nan),'target_before_stop20':np.where(valid,target,np.nan)}))
 return candidates.merge(pd.concat(rows,ignore_index=True),on=['signal_ymd','code'],how='left',validate='one_to_one')
def generate(db:Path,out:Path)->Path:
 b,r,cov=load(db);raw=r[r['rank'].eq(1)].copy();cal=sorted(r.signal_ymd.unique());variants=[];ledgers={};td=sum(d<=20241231 for d in cal)
 for name,(tp,sl,h) in EXITS.items():
  x=outcomes(raw,b,tp,sl,h);x=x[x.trade_return_h10.notna()];e=no_reentry_sessions(x,cal,'rank');m=_metrics(e[e.signal_ymd<=20241231],td);variants.append({'exit_id':name,'tp':tp,'sl':sl,'horizon':h,'train_metrics':m});ledgers[name]=e
 ok=[v for v in variants if (v['train_metrics']['expectancy'] or 0)>0 and v['train_metrics']['profit_factor'] is not None];chosen=max(ok,key=lambda v:(v['train_metrics']['profit_factor'],v['train_metrics']['expectancy'])) if ok else None;e=ledgers[chosen['exit_id']] if chosen else None;cur=ledgers['C'];latest=int(r.signal_ymd.max());periods={**PERIODS,'shadow':(20260101,latest)};metrics={};gates={}
 for split,(a,z) in periods.items():
  days=sum(a<=d<=z for d in cal);cm=_metrics(e[e.signal_ymd.between(a,z)],days) if e is not None else None;bm=_metrics(cur[cur.signal_ymd.between(a,z)],days);metrics[split]={'selected_exit':cm,'current_C':bm};gates[split]={'pf_ge_1_30':bool(cm and (cm['profit_factor'] or 0)>=1.3),'expectancy_positive':bool(cm and (cm['expectancy'] or 0)>0),'cvar_non_degrade_vs_C':bool(cm and cm['cvar10'] is not None and bm['cvar10'] is not None and cm['cvar10']>=bm['cvar10']),'dd_non_degrade_vs_C':bool(cm and cm['max_drawdown'] is not None and bm['max_drawdown'] is not None and cm['max_drawdown']>=bm['max_drawdown'])}
 keep=all(all(gates[x].values()) for x in ('validation','shadow'));root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);raw.to_parquet(root/'fixed_meemee_buy_top1_candidates.parquet',index=False)
 for k,v in ledgers.items():v.to_parquet(root/f'exit_{k}_ledger.parquet',index=False)
 payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'single_axis':'exit geometry only','selection':'formal MeeMee BUY rank1 fixed daily','ranking':'formal all-symbol MeeMee ranking fixed; raw entry candidates unchanged across variants','variants_2024_only':{'A':'TP5 SL3 H5','B':'TP6 SL4 H7','C':'TP8 SL5 H10 current'},'selection_rule':'2024 maximum PF among expectancy>0','execution':'next-open/10bp/gap-through actual open/stop-first/open-trade same-code reentry prohibited','validation':'2025','untouched_shadow':'2026','benchmark':'current C','entry_threshold_regime_changes':False,'survivorship_filter':False,'fallback':False},'source_artifacts':[{'path':str(db),'sha256':_sha(db)}],'source_coverage':cov,'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'rank_coverage':{'ranking_days':int(r.signal_ymd.nunique()),'min_ranked':int(r.groupby('signal_ymd').size().min()),'raw_top1_days':int(raw.signal_ymd.nunique()),'all_days_top1':bool(raw.signal_ymd.nunique()==r.signal_ymd.nunique())},'branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'none; exit-only axis'},'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only'},'shadow_tuning_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
