from __future__ import annotations
import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_long_gap_guard_protective_stop_v1 import metrics

COST=.3
def simulate(events,max_positions):
 active=[];accepted=[]
 for day,g in events.sort_values(['entry_date','rank','code']).groupby('entry_date',sort=True):
  active=[x for x in active if x['exit_date']>=day]
  used={x['code'] for x in active};free=max_positions-len(active)
  if free<=0:continue
  for _,r in g.sort_values(['rank','code']).iterrows():
   if free<=0:break
   if r.code in used:continue
   raw=100*(r.exit_price/r.entry_price-1)-COST;row=r.to_dict();row['raw_return_pct']=raw;row['realized_ret']=raw/max_positions;accepted.append(row);active.append({'code':r.code,'exit_date':r.exit_date});used.add(r.code);free-=1
 return pd.DataFrame(accepted)
def portfolio_summary(t):
 if t.empty:return {'trades':0,'raw_trade_metrics':metrics(t),'capital_contribution_metrics':metrics(t),'total_return_pct':0,'positive_month_rate':None,'positive_year_rate':None,'monthly':{},'yearly':{},'average_slots_used':None}
 raw=t.copy();raw['realized_ret']=raw.raw_return_pct;months={str(m):float(g.realized_ret.sum()) for m,g in t.groupby(pd.to_datetime(t.exit_date,unit='s').dt.to_period('M'))};years={str(y):float(g.realized_ret.sum()) for y,g in t.groupby(pd.to_datetime(t.exit_date,unit='s').dt.year)}
 return {'trades':len(t),'raw_trade_metrics':metrics(raw),'capital_contribution_metrics':metrics(t),'total_return_pct':float(t.realized_ret.sum()),'positive_month_rate':sum(v>0 for v in months.values())/len(months),'positive_year_rate':sum(v>0 for v in years.values())/len(years),'monthly':months,'yearly':years}
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--source',default=r'G:\Tradex\long_selector_10y_research_v1\20260626T054716Z-long_selector_10y_research_v1\strict_trend_pullback_top3_events.csv');a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/'app')]
 from backend.services.codex_bridge_service import get_runtime_stock_db_status
 runtime=get_runtime_stock_db_status();src=pd.read_csv(a.source,dtype={'code':str});src['event_id']=range(len(src))
 with duckdb.connect(runtime['selected_runtime_db_path'],read_only=True) as c:
  c.register('src',src);e=c.execute("""select * exclude(next_rn) from (select s.event_id,s.code,s.date signal_date,s.rank,b.date entry_date,b.o entry_price,row_number() over(partition by s.event_id order by b.date) next_rn from src s join daily_bars b on b.code=s.code and b.date>s.date) where next_rn=1""").fetchdf();c.register('events',e)
  events=c.execute("""select * exclude(exit_rn) from (select e.*,i.market_code,b.date exit_date,b.c exit_price,row_number() over(partition by e.event_id order by b.date) exit_rn from events e join industry_master i using(code) join daily_bars b on b.code=e.code and b.date>=e.entry_date where i.market_code in ('プライム（内国株式）','スタンダード（内国株式）','グロース（内国株式）')) where exit_rn=20""").fetchdf()
 events['signal_dt']=pd.to_datetime(events.signal_date,unit='s');events['year']=events.signal_dt.dt.year;rows=[];ledgers={}
 for slots in [3,5,10]:
  t=simulate(events,slots);t['year']=pd.to_datetime(t.signal_date,unit='s').dt.year;ledgers[slots]=t;dev=t[t.year.between(2016,2023)];val=t[t.year.between(2024,2025)];rows.append({'max_positions':slots,'development':portfolio_summary(dev),'validation_2024_2025':portfolio_summary(val),'validation_years':{str(y):portfolio_summary(val[val.year.eq(y)]) for y in [2024,2025]},'accepted_rate':len(t)/len(events)})
 eligible=[x for x in rows if x['development']['trades']>=250 and x['development']['raw_trade_metrics']['mean_return_pct']>0 and x['development']['raw_trade_metrics']['win_rate']>=.50 and x['development']['capital_contribution_metrics']['severe_loss5_rate']<=.03 and x['development']['positive_month_rate']>.50 and x['development']['positive_year_rate']>=.75 and x['development']['capital_contribution_metrics']['top3_positive_profit_share']<=.35 and x['validation_2024_2025']['raw_trade_metrics']['mean_return_pct']>0 and x['validation_2024_2025']['raw_trade_metrics']['win_rate']>=.50 and x['validation_2024_2025']['capital_contribution_metrics']['severe_loss5_rate']<=.03 and x['validation_2024_2025']['positive_month_rate']>.50 and x['validation_2024_2025']['capital_contribution_metrics']['top3_positive_profit_share']<=.35 and all(v['total_return_pct']>0 for v in x['validation_years'].values())]
 chosen=max(eligible,key=lambda x:(x['validation_2024_2025']['total_return_pct'],x['development']['total_return_pct'])) if eligible else None;test=ledgers[chosen['max_positions']] if chosen else pd.DataFrame();test=test[test.year.eq(2026)] if len(test) else test;sm=portfolio_summary(test)
 severe=sm['capital_contribution_metrics']['severe_loss5_rate'];concentration=sm['capital_contribution_metrics']['top3_positive_profit_share'];win=sm['raw_trade_metrics']['win_rate'];month_rate=sm['positive_month_rate']
 checks={'selected_without_2026':chosen is not None,'test_n250_or_full_audit':chosen is not None,'test_raw_mean_positive':sm['raw_trade_metrics']['mean_return_pct'] is not None and sm['raw_trade_metrics']['mean_return_pct']>0,'test_win_at_least_50pct':win is not None and win>=.50,'test_capital_loss5_at_most_3pct':severe is not None and severe<=.03,'test_months_majority_positive':month_rate is not None and month_rate>.50,'profit_not_concentrated':concentration is not None and concentration<=.35,'test_total_return_positive':sm['total_return_pct']>0};decision='hold_for_final_baseline_and_freshness_gate' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_trend_pullback_portfolio_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'runtime':runtime,'fixed_evaluation_conditions':{'source':a.source,'source_events':len(src),'ordinary_mature_events':len(events),'universe':'ordinary domestic stocks only: Prime, Standard, Growth','family':'strict_trend_pullback_top3 fixed','entry':'next session open','exit':'session-20 close','round_trip_cost_pct':COST,'portfolio':'equal fixed slots; no same-code overlap; exits on same date do not free opening slot','max_positions_candidates':[3,5,10],'loss5_definition':'loss contribution to total portfolio capital; raw trade loss also reported','development':'2016-2023','validation':'2024-2025','full_audit_test':'2026 matured events','production_changed':False},'authoritative_result':{'candidates':rows,'eligible_without_2026':eligible,'chosen_without_2026':chosen,'test_2026':sm,'checks':checks},'observed_branching':{'changed_top5_members_count':None,'changed_top10_members_count':None,'changed_rank_count':sm['trades'],'selection_divergence_reason':'capital slots and overlapping holdings'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'long_history_capital_allocation_gate'},'remaining_risks':['same-condition existing practical baseline comparison pending only if gates pass','mark-to-market drawdown not yet measured']}
 if len(test):test.to_parquet(out/'test_2026_portfolio_ledger.parquet',index=False)
 (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'ordinary_events':[len(events),len(src)],'eligible_count':len(eligible),'chosen':chosen,'test':sm,'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
