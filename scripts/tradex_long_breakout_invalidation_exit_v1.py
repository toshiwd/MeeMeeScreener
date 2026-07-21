from __future__ import annotations
import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_long_gap_guard_protective_stop_v1 import metrics

EXITS=['20日固定','4.5%非常停止','シグナル安値終値割れ','20MA終値割れ','突破水準終値割れ'];COST=.3
def sim(g,kind):
 g=g.sort_values('session_no');e=float(g.entry_price.iloc[0]);cat=e*.955
 levels={'シグナル安値終値割れ':float(g.signal_low.iloc[0]),'20MA終値割れ':float(g.signal_ma20.iloc[0]),'突破水準終値割れ':float(g.breakout_level.iloc[0])};rows=list(g.itertuples())
 for idx,r in enumerate(rows):
  if kind!='20日固定':
   if float(r.o)<=cat:return 100*(float(r.o)/e-1)-COST
   if float(r.l)<=cat:return -4.5-COST
  if kind in levels and float(r.c)<levels[kind] and idx+1<len(rows):return 100*(float(rows[idx+1].o)/e-1)-COST
 return 100*(float(rows[-1].c)/e-1)-COST
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--source',default=r'G:\Tradex\long_selector_10y_research_v1\20260626T054716Z-long_selector_10y_research_v1\strict_breakout_top2_events.csv');a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/'app')]
 from backend.services.codex_bridge_service import get_runtime_stock_db_status
 runtime=get_runtime_stock_db_status();src=pd.read_csv(a.source,dtype={'code':str});src['event_id']=range(len(src))
 with duckdb.connect(runtime['selected_runtime_db_path'],read_only=True) as c:
  c.register('src',src);events=c.execute("""select * exclude(rn) from (select s.event_id,s.code,s.date,s.l signal_low,s.ma20 signal_ma20,s.high20 breakout_level,b.date entry_date,b.o entry_price,row_number() over(partition by s.event_id order by b.date) rn from src s join daily_bars b on b.code=s.code and b.date>s.date) where rn=1""").fetchdf();c.register('events',events)
  bars=c.execute("""select * from (select e.*,i.market_code,b.date bar_date,b.o,b.h,b.l,b.c,row_number() over(partition by e.event_id order by b.date) session_no from events e join industry_master i using(code) join daily_bars b on b.code=e.code and b.date>=e.entry_date where i.market_code in ('プライム（内国株式）','スタンダード（内国株式）','グロース（内国株式）')) where session_no<=20""").fetchdf()
 valid=bars.groupby('event_id').size();valid=valid[valid.eq(20)].index;base=events[events.event_id.isin(valid)].copy();bars=bars[bars.event_id.isin(valid)];base['signal_dt']=pd.to_datetime(base.date,unit='s');base['year']=base.signal_dt.dt.year
 for kind in EXITS:base[kind]=base.event_id.map({int(e):sim(g,kind) for e,g in bars.groupby('event_id')})
 rows=[]
 for kind in EXITS:
  z=base.copy();z['realized_ret']=z[kind];dev=z[z.year.between(2016,2023)];val=z[z.year.between(2024,2025)];ym={str(y):metrics(dev[dev.year.eq(y)]) for y in range(2016,2024)};rows.append({'exit':kind,'development':metrics(dev),'development_years':ym,'positive_development_years':sum((m['mean_return_pct'] or -99)>0 for m in ym.values() if m['n']>0),'validation_2024_2025':metrics(val),'validation_years':{str(y):metrics(val[val.year.eq(y)]) for y in [2024,2025]}})
 eligible=[x for x in rows if x['exit']!='20日固定' and x['development']['n']>=400 and x['development']['mean_return_pct']>0 and x['development']['win_rate']>=.50 and x['development']['severe_loss5_rate']<=.03 and x['positive_development_years']>=6 and x['validation_2024_2025']['mean_return_pct']>0 and x['validation_2024_2025']['win_rate']>=.50 and x['validation_2024_2025']['severe_loss5_rate']<=.03 and x['validation_2024_2025']['top3_positive_profit_share']<=.35]
 chosen=max(eligible,key=lambda x:(x['validation_2024_2025']['mean_return_pct'],x['development']['mean_return_pct'])) if eligible else None;test=base[base.year.eq(2026)].copy()
 if chosen:test['realized_ret']=test[chosen['exit']]
 else:test=test.iloc[0:0].copy()
 sm=metrics(test);monthly={str(m):metrics(g) for m,g in test.groupby(test.signal_dt.dt.to_period('M'))};pos=sum((x['mean_return_pct'] or -99)>0 for x in monthly.values());checks={'ordinary_event_audit_complete':len(base)>0,'selected_without_2026':chosen is not None,'test_n250_or_full_audit':chosen is not None,'test_mean_positive':(sm['mean_return_pct'] or -99)>0,'test_win_at_least_50pct':(sm['win_rate'] or 0)>=.50,'test_severe5_at_most_3pct':(sm['severe_loss5_rate'] or 1)<=.03,'test_months_majority_positive':bool(monthly) and pos>len(monthly)/2,'profit_not_concentrated':(sm['top3_positive_profit_share'] or 1)<=.35};decision='hold_for_portfolio_gate' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_breakout_invalidation_exit_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'runtime':runtime,'fixed_evaluation_conditions':{'source':a.source,'source_events':len(src),'ordinary_complete_events':len(base),'universe':'ordinary domestic stocks only: Prime, Standard, Growth','selection':'strict_breakout_top2 fixed','entry':'next session open','exit_candidates':EXITS,'catastrophe_stop_pct':4.5,'close_invalidation_execution':'next session open','round_trip_cost_pct':COST,'axis_changed':'invalidation exit only','development':'2016-2023','validation':'2024-2025','full_audit_test':'2026 matured events','production_changed':False},'authoritative_result':{'candidates':rows,'eligible_without_2026':eligible,'chosen_without_2026':chosen,'test_2026':sm,'monthly_2026':monthly,'checks':checks},'observed_branching':{'changed_top5_members_count':None,'changed_top10_members_count':None,'changed_rank_count':sm['n'],'selection_divergence_reason':'event selection fixed; structure invalidation exit changed'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'long_history_breakout_invalidation_gate'},'remaining_risks':['overlap and capital allocation pending only if event gate passes']}
 test.to_parquet(out/'test_2026_ledger.parquet',index=False);base.to_parquet(out/'ordinary_breakout_exit_ledger.parquet',index=False);(out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'ordinary_events':[len(base),len(src)],'eligible_count':len(eligible),'chosen':chosen,'test':sm,'monthly':monthly,'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
