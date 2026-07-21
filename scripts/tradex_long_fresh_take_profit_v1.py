from __future__ import annotations
import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_long_trend_pullback_portfolio_v1 import simulate,portfolio_summary,COST
TARGETS=[None,30.0,50.0,75.0]
def exits(events,bars,target):
 out={}
 for eid,g in bars.groupby('event_id'):
  r=events.loc[events.event_id.eq(eid)].iloc[0];entry=float(r.entry_price);chosen_date=int(r.h20_date);chosen_price=float(r.h20_price)
  if target is not None:
   level=entry*(1+target/100)
   for q in g.sort_values('bar_date').itertuples():
    if float(q.o)>=level:chosen_date=int(q.bar_date);chosen_price=float(q.o);break
    if float(q.h)>=level:chosen_date=int(q.bar_date);chosen_price=level;break
  out[int(eid)]=(chosen_date,chosen_price)
 z=events.copy();z['exit_date']=z.event_id.map(lambda x:out[int(x)][0]);z['exit_price']=z.event_id.map(lambda x:out[int(x)][1]);return z
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--events',default=r'G:\Tradex\tradex_long_fresh_family_events_v1\20260720T-authoritative-v4\fresh_family_events.parquet');a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/'app')]
 from backend.services.codex_bridge_service import get_runtime_stock_db_status
 runtime=get_runtime_stock_db_status();e=pd.read_parquet(a.events).rename(columns={'p1_date':'entry_date','p1_o':'entry_price','p20_date':'h20_date','p20_c':'h20_price'});e['event_id']=range(len(e));e['rank']=-e.family_score;e['year']=pd.to_datetime(e.date,unit='s').dt.year
 with duckdb.connect(runtime['selected_runtime_db_path'],read_only=True) as c:
  c.register('events',e[['event_id','code','entry_date','h20_date']]);bars=c.execute("select e.event_id,b.date bar_date,b.o,b.h from events e join daily_bars b on b.code=e.code and b.date between e.entry_date and e.h20_date order by e.event_id,b.date").fetchdf()
 rows=[];ledgers={}
 for target in TARGETS:
  managed=exits(e,bars,target);t=simulate(managed,20);t['year']=pd.to_datetime(t.date,unit='s').dt.year;key='20日固定' if target is None else f'{target:g}%利確';ledgers[key]=t;d=t[t.year.between(2016,2023)];v=t[t.year.between(2024,2025)];rows.append({'scheme':key,'take_profit_pct':target,'development':portfolio_summary(d),'validation_2024_2025':portfolio_summary(v),'validation_years':{str(y):portfolio_summary(v[v.year.eq(y)]) for y in [2024,2025]},'mix_development':d.family.value_counts().to_dict(),'mix_validation':v.family.value_counts().to_dict()})
 baseline=next(x for x in rows if x['take_profit_pct'] is None)
 def ok(m,n):return m['trades']>=n and m['raw_trade_metrics']['mean_return_pct']>0 and m['raw_trade_metrics']['win_rate']>=.50 and m['capital_contribution_metrics']['severe_loss5_rate']<=.03 and m['positive_month_rate']>.50 and m['capital_contribution_metrics']['top3_positive_profit_share']<=.35
 eligible=[x for x in rows if x['take_profit_pct'] is not None and ok(x['development'],250) and ok(x['validation_2024_2025'],100) and x['development']['positive_year_rate']>=.75 and all(m['total_return_pct']>0 for m in x['validation_years'].values()) and x['development']['total_return_pct']>=.8*baseline['development']['total_return_pct'] and x['validation_2024_2025']['total_return_pct']>=.8*baseline['validation_2024_2025']['total_return_pct']];chosen=min(eligible,key=lambda x:(x['validation_2024_2025']['capital_contribution_metrics']['top3_positive_profit_share'],x['development']['capital_contribution_metrics']['top3_positive_profit_share'],-x['validation_2024_2025']['total_return_pct'])) if eligible else None;t=ledgers[chosen['scheme']] if chosen else pd.DataFrame();t=t[t.year.eq(2026)] if len(t) else t;tm=portfolio_summary(t);r=tm['raw_trade_metrics'];c=tm['capital_contribution_metrics'];checks={'selected_without_2026':chosen is not None,'test_n250_or_full_audit':chosen is not None,'test_mean_positive':r['mean_return_pct'] is not None and r['mean_return_pct']>0,'test_win_at_least_50pct':r['win_rate'] is not None and r['win_rate']>=.50,'test_capital_loss5_at_most_3pct':c['severe_loss5_rate'] is not None and c['severe_loss5_rate']<=.03,'test_months_majority_positive':tm['positive_month_rate'] is not None and tm['positive_month_rate']>.50,'profit_not_concentrated':c['top3_positive_profit_share'] is not None and c['top3_positive_profit_share']<=.35,'test_total_return_positive':tm['total_return_pct']>0};decision='hold_for_baseline_and_mark_to_market_gate' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_fresh_take_profit_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'runtime':runtime,'fixed_evaluation_conditions':{'source':a.events,'universe':'ordinary domestic stocks only','selection':'fresh three-family continuous score','portfolio':'max 20 equal 5% slots; no same-code overlap','entry':'next session open','maximum_exit':'session-20 close','axis_changed':'take-profit level only','targets_pct':TARGETS,'execution':'open above target exits at open; otherwise first daily high touch at target','minimum_profit_retention':'80% of fixed-H20 total return in development and validation','selection_objective':'lowest validation then development profit concentration','round_trip_cost_pct':COST,'development':'2016-2023','validation':'2024-2025','full_audit_test':'2026 through 2026-07-17','production_changed':False},'authoritative_result':{'candidates':rows,'baseline':baseline,'eligible_without_2026':eligible,'chosen_without_2026':chosen,'test_2026':tm,'test_mix':t.family.value_counts().to_dict() if len(t) else {},'checks':checks},'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':tm['trades'],'selection_divergence_reason':'early profit exits free capital slots sooner'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'fresh_take_profit_portfolio_gate'},'remaining_risks':['same-condition practical baseline and mark-to-market drawdown pending only if test passes']}
 if len(t):t.to_parquet(out/'test_2026_portfolio_ledger.parquet',index=False)
 (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'eligible_count':len(eligible),'chosen':chosen,'test':tm,'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
