from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import duckdb,numpy as np,pandas as pd
try:
 from scripts.tradex_early_signal_first_detect_v1 import _branching,_metrics
 from scripts.tradex_contraction_sequence_challenger_v1 import load_source as load_contraction,build_scores as contraction_scores
 from scripts.tradex_ma200_weekly_reversal_current_exit_v1 import build_frame as ma_frame,score as ma_score
 from scripts.tradex_shallow_high_zone_universe_repair_v1 import attach_gap_outcomes,no_reentry_sessions,_sha,_write,extra_metrics
except ModuleNotFoundError:
 from tradex_early_signal_first_detect_v1 import _branching,_metrics
 from tradex_contraction_sequence_challenger_v1 import load_source as load_contraction,build_scores as contraction_scores
 from tradex_ma200_weekly_reversal_current_exit_v1 import build_frame as ma_frame,score as ma_score
 from tradex_shallow_high_zone_universe_repair_v1 import attach_gap_outcomes,no_reentry_sessions,_sha,_write,extra_metrics
AXIS_ID='tradex_strict_pit_adaptive_buy_router_v1';OUT=Path(r'G:\Tradex\tradex_strict_pit_adaptive_buy_router_v1');WINDOWS=(40,60,120);LEAKY=Path('scripts/tradex_adaptive_rule_router_v1.py')
PERIODS={'train':(20240101,20241231),'validation':(20250101,20251231),'shadow':(20260101,20261231)}
def add_exit_ymd(e:pd.DataFrame,bars:pd.DataFrame)->pd.DataFrame:
 maps={str(c):list(p.sort_values('signal_ymd').signal_ymd.astype(int)) for c,p in bars.groupby('code',sort=False)};pos={c:{d:i for i,d in enumerate(ds)} for c,ds in maps.items()};out=[]
 for r in e.itertuples():
  ds=maps[str(r.code)];i=pos[str(r.code)][int(r.signal_ymd)];j=i+int(r.exit_day_h10);out.append(ds[j] if j<len(ds) else None)
 z=e.copy();z['exit_ymd']=pd.array(out,dtype='Int64');return z
def lane_stats(events:pd.DataFrame,day:int,window:int)->tuple[float,float,int]:
 q=events[(events.exit_ymd.notna())&(events.exit_ymd<day)].sort_values(['exit_ymd','signal_ymd','code']).tail(window);r=q.trade_return_h10
 if not len(r):return 0.,-9.,0
 loss=float(-r[r<0].sum());pf=float(r[r>0].sum()/loss) if loss else 99.;return pf,float(r.mean()),len(r)
def priorities(lanes:dict[str,pd.DataFrame],day:int,window:int)->dict[str,int]:
 rows=[]
 for name,e in lanes.items():
  pf,exp,n=lane_stats(e,day,window);rows.append((name,pf,exp,n))
 rows.sort(key=lambda x:(x[2]>0,x[1],x[2],x[0]),reverse=True);return {name:len(rows)-i for i,(name,_,_,_) in enumerate(rows)}
def riskoff_scores(features:pd.DataFrame,bars:pd.DataFrame)->pd.DataFrame:
 parts=[]
 for _,p in bars.groupby('code',sort=False):
  p=p.sort_values('signal_ymd').copy();p['prev_c']=p.c.shift(1);p['c5']=p.c.shift(5);p['vol20']=p.v.shift(1).rolling(20,20).mean();parts.append(p)
 x=pd.concat(parts,ignore_index=True).merge(features[['signal_ymd','code']],on=['signal_ymd','code'],how='inner',validate='one_to_one');x['ret5']=x.c/x.c5-1;x['volume_ratio20']=x.v/x.vol20;x['close_position']=(x.c-x.l)/(x.h-x.l);x['family_hit']=(x.ret5<=-.08)&(x.volume_ratio20>=2)&(x.c>x.o)&(x.close_position>=.60)&(x.c>=x.prev_c*.99)
 x['score']=x.family_hit.astype(float)*100+(-x.ret5.fillna(0)).clip(-1,1)+x.volume_ratio20.fillna(0).clip(0,10)/100+x.close_position.fillna(0)/1000;x=x.sort_values(['signal_ymd','score','code'],ascending=[True,False,True]);x['rank']=x.groupby('signal_ymd').cumcount()+1;x['top10']=x['rank']<=10;x['percentile']=1-(x['rank']-1)/x.groupby('signal_ymd').code.transform('size');x['side']='BUY';return x
def build_router_scores(risk:pd.DataFrame,con:pd.DataFrame,ma:pd.DataFrame,ranks:pd.DataFrame,lanes:dict[str,pd.DataFrame],window:int)->pd.DataFrame:
 rows=[]
 for day in sorted(con.signal_ymd.unique()):
  pr=priorities(lanes,int(day),window);a=con[con.signal_ymd==day][['signal_ymd','code','percentile']].copy();a['lane']='contraction';a['router_score']=pr['contraction']*10+a.percentile
  b=ma[ma.signal_ymd==day][['signal_ymd','code','percentile']].copy();b['lane']='ma200';b['router_score']=pr['ma200']*10+b.percentile
  c=risk[risk.signal_ymd==day][['signal_ymd','code','percentile']].copy();c['lane']='riskoff';c['router_score']=pr['riskoff']*10+c.percentile
  m=ranks[ranks.signal_ymd==day][['signal_ymd','code','baseline_rank']].copy();m['percentile']=1-(m.baseline_rank-1)/10;m['lane']='meemee';m['router_score']=pr['meemee']*10+m.percentile
  q=pd.concat([a,b,c,m],ignore_index=True).sort_values(['signal_ymd','code','router_score','lane'],ascending=[True,True,False,True]).drop_duplicates(['signal_ymd','code']);q['family_priority_riskoff']=pr['riskoff'];q['family_priority_contraction']=pr['contraction'];q['family_priority_ma200']=pr['ma200'];q['family_priority_meemee']=pr['meemee'];rows.append(q)
 z=pd.concat(rows,ignore_index=True).sort_values(['signal_ymd','router_score','code'],ascending=[True,False,True]);z['rank']=z.groupby('signal_ymd').cumcount()+1;z['top10']=z['rank']<=10;z['side']='BUY';return z
def generate(db:Path,out:Path)->Path:
 f,b,r,cov=load_contraction(db)
 with duckdb.connect(str(db),read_only=True) as q:vol=q.execute("select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code,v from daily_bars where source='pan'").fetchdf()
 b=b.merge(vol,on=['signal_ymd','code'],how='left',validate='one_to_one');common=attach_gap_outcomes(ma_frame(f[['signal_ymd','code']],b),b);outs=common[['signal_ymd','code','trade_return_h10','exit_day_h10','target_before_stop20','realized_mover20']]
 risk=riskoff_scores(f,b).merge(outs,on=['signal_ymd','code'],how='left',validate='one_to_one');con=contraction_scores(f,b,.08).merge(outs,on=['signal_ymd','code'],how='left',validate='one_to_one');ma=ma_score(ma_frame(f[['signal_ymd','code']],b),'support_distance').merge(outs,on=['signal_ymd','code'],how='left',validate='one_to_one');risk=risk[risk.signal_ymd>=20240101];con=con[con.signal_ymd>=20240101];ma=ma[ma.signal_ymd>=20240101];cal=sorted(con.signal_ymd.unique())
 risk_e=add_exit_ymd(no_reentry_sessions(risk[risk.top10&risk.trade_return_h10.notna()],cal,'rank'),b);con_e=add_exit_ymd(no_reentry_sessions(con[con.top10&con.trade_return_h10.notna()],cal,'rank'),b);ma_e=add_exit_ymd(no_reentry_sessions(ma[ma.top10&ma.trade_return_h10.notna()],cal,'rank'),b)
 mm=r.merge(outs,on=['signal_ymd','code'],how='left',validate='many_to_one');mm=no_reentry_sessions(mm[mm.trade_return_h10.notna()],cal,'baseline_rank');mm_e=add_exit_ymd(mm,b);lanes={'riskoff':risk_e,'contraction':con_e,'ma200':ma_e,'meemee':mm_e};variants=[];scored={};td=sum(d<=20241231 for d in cal)
 for w in WINDOWS:
  s=build_router_scores(risk,con,ma,r,lanes,w).merge(outs,on=['signal_ymd','code'],how='left',validate='one_to_one');e=add_exit_ymd(no_reentry_sessions(s[s.top10&s.trade_return_h10.notna()],cal,'rank'),b);m=_metrics(e[e.signal_ymd<=20241231],td);variants.append({'completed_event_window':w,'train_metrics':m});scored[w]=(s,e)
 ok=[v for v in variants if (v['train_metrics']['expectancy'] or 0)>0 and v['train_metrics']['profit_factor'] is not None]
 if not ok:raise ValueError('NO_POSITIVE_2024_WINDOW')
 chosen=max(ok,key=lambda v:(v['train_metrics']['profit_factor'],v['train_metrics']['expectancy']));s,events=scored[chosen['completed_event_window']];s['split']=np.select([s.signal_ymd<20250101,s.signal_ymd<20260101],['train','validation'],default='shadow');latest=int(f.signal_ymd.max());periods={**PERIODS,'shadow':(20260101,latest)};metrics={};branch={};coverage={};gates={}
 for split,(a,z) in periods.items():
  days=sum(a<=d<=z for d in cal);ch=events[events.signal_ymd.between(a,z)];bl=mm_e[mm_e.signal_ymd.between(a,z)];elig=common[common.signal_ymd.between(a,z)&common.trade_return_h10.notna()];cm={**_metrics(ch,days),**extra_metrics(ch,elig)};bm={**_metrics(bl,days),**extra_metrics(bl,elig)};metrics[split]={'router_top10':cm,'meemee_buy_top10':bm};branch[split]=_branching(s[s.top10],r.assign(side='BUY'),'BUY',a,z);cnt=s[s.signal_ymd.between(a,z)].groupby('signal_ymd').size();coverage[split]={'days':len(cnt),'min_ranked':int(cnt.min()),'all_days_ranked':bool(cnt.gt(0).all())};gates[split]={'pf_ge_1_30':(cm['profit_factor'] or 0)>=1.3,'expectancy_positive':(cm['expectancy'] or 0)>0,'pf_improves':cm['profit_factor'] is not None and bm['profit_factor'] is not None and cm['profit_factor']>bm['profit_factor'],'expectancy_improves':cm['expectancy'] is not None and bm['expectancy'] is not None and cm['expectancy']>bm['expectancy'],'cvar_non_degrade':cm['cvar10'] is not None and bm['cvar10'] is not None and cm['cvar10']>=bm['cvar10'],'dd_non_degrade':cm['max_drawdown'] is not None and bm['max_drawdown'] is not None and cm['max_drawdown']>=bm['max_drawdown']}
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);s.to_parquet(root/'all_symbol_daily_scores.parquet',index=False);events.to_parquet(root/'router_execution_ledger.parquet',index=False);mm_e.to_parquet(root/'meemee_execution_ledger.parquet',index=False)
 keep=all(all(gates[x].values()) for x in ('validation','shadow'));payload={'schema_version':AXIS_ID+'.compare.v1','artifact_role':'authoritative','research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'lanes':['riskoff_capitulation_shape','contraction_sequence','ma200_reversal','meemee_buy'],'riskoff_exclusions':'original top3, breadth permission, and next-open gap permission are not candidate suppressors; shape score only','static_shapes':'fixed','rolling_data':'only trades with exit_ymd strictly before decision day','windows_train_only':[40,60,120],'ranking':'daily family priority band plus within-family percentile; same-code max; all symbols ranked; top10','execution':'next-open TP8/SL5/H10/10bp; gap-through actual open; open-trade reentry prohibited','splits':periods,'fallback':False},'invalid_source_evidence':{'path':str(LEAKY),'sha256':_sha(LEAKY),'status':'future_permission_leakage_not_reused'},'source_artifacts':[{'path':str(db),'sha256':_sha(db)}],'source_coverage':cov,'train_variants':variants,'selected_variant':chosen,'metrics':metrics,'adoption_gates':gates,'branching':branch,'rank_coverage':coverage,'decision':{'candidate_local_decision':'keep' if keep else 'drop','authoritative_rollup_decision':'review_only'},'shadow_tuning_used':False,'silent_fallback_used':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';_write(p,payload);_write(root/'_ARTIFACT_COMPLETE.json',{'complete':True,'compare':str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,required=True);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=='__main__':main()
