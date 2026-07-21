from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import numpy as np,pandas as pd
from tradex_long_trend_pullback_portfolio_v1 import simulate,portfolio_summary,COST
LOOKBACKS=[20,60,120]
def route(e,n):
 histories={}
 for f,g in e.sort_values('p20_date').groupby('family'):
  histories[f]=(g.p20_date.to_numpy(dtype='int64'),g.realized_ret.to_numpy(float))
 chosen=[]
 for day,g in e.groupby('date',sort=True):
  scores={}
  for f,(dates,rets) in histories.items():
   pos=np.searchsorted(dates,int(day),side='left')
   if pos>=n:scores[f]=float(rets[pos-n:pos].mean())
  if not scores:continue
  best=max(scores,key=scores.get)
  if scores[best]>0:chosen.extend(g[g.family.eq(best)].index.tolist())
 return e.loc[chosen].copy()
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--events',default=r'G:\Tradex\tradex_long_fresh_family_events_v1\20260720T-authoritative-v3\fresh_family_events.parquet');a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);e=pd.read_parquet(a.events).rename(columns={'p1_date':'entry_date','p1_o':'entry_price','p20_date':'exit_date','p20_c':'exit_price'});e['p20_date']=e.exit_date;e['rank']=-e.family_score;e['year']=pd.to_datetime(e.date,unit='s').dt.year;rows=[];ledgers={}
 for n in LOOKBACKS:
  allowed=route(e,n);t=simulate(allowed,10);t['year']=pd.to_datetime(t.date,unit='s').dt.year;ledgers[n]=t;d=t[t.year.between(2016,2023)];v=t[t.year.between(2024,2025)];rows.append({'lookback_completed_events':n,'allowed_events':len(allowed),'development':portfolio_summary(d),'validation_2024_2025':portfolio_summary(v),'validation_years':{str(y):portfolio_summary(v[v.year.eq(y)]) for y in [2024,2025]},'mix_development':d.family.value_counts().to_dict(),'mix_validation':v.family.value_counts().to_dict()})
 def ok(m,n):return m['trades']>=n and m['raw_trade_metrics']['mean_return_pct']>0 and m['raw_trade_metrics']['win_rate']>=.50 and m['capital_contribution_metrics']['severe_loss5_rate']<=.03 and m['positive_month_rate']>.50 and m['capital_contribution_metrics']['top3_positive_profit_share']<=.35
 eligible=[x for x in rows if ok(x['development'],250) and ok(x['validation_2024_2025'],100) and x['development']['positive_year_rate']>=.75 and all(m['total_return_pct']>0 for m in x['validation_years'].values())];chosen=max(eligible,key=lambda x:(x['validation_2024_2025']['total_return_pct'],x['development']['total_return_pct'])) if eligible else None;t=ledgers[chosen['lookback_completed_events']] if chosen else pd.DataFrame();t=t[t.year.eq(2026)] if len(t) else t;tm=portfolio_summary(t);r=tm['raw_trade_metrics'];c=tm['capital_contribution_metrics'];checks={'selected_without_2026':chosen is not None,'test_n250_or_full_audit':chosen is not None,'test_mean_positive':r['mean_return_pct'] is not None and r['mean_return_pct']>0,'test_win_at_least_50pct':r['win_rate'] is not None and r['win_rate']>=.50,'test_capital_loss5_at_most_3pct':c['severe_loss5_rate'] is not None and c['severe_loss5_rate']<=.03,'test_months_majority_positive':tm['positive_month_rate'] is not None and tm['positive_month_rate']>.50,'profit_not_concentrated':c['top3_positive_profit_share'] is not None and c['top3_positive_profit_share']<=.35,'test_total_return_positive':tm['total_return_pct']>0};decision='hold_for_baseline_and_mark_to_market_gate' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_fresh_adaptive_family_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'fixed_evaluation_conditions':{'source':a.events,'universe':'ordinary domestic stocks only','families':['ブレイクアウト','押し目','底反転'],'router':'highest positive mean among family last N fully matured H20 events','future_safety':'only p20_date strictly before signal date','axis_changed':'completed-event lookback only','lookback_candidates':LOOKBACKS,'entry':'next session open','exit':'session-20 close','portfolio':'max 10 equal slots; no same-code overlap','round_trip_cost_pct':COST,'development':'2016-2023','validation':'2024-2025','full_audit_test':'2026 matured through 2026-07-17','production_changed':False},'authoritative_result':{'candidates':rows,'eligible_without_2026':eligible,'chosen_without_2026':chosen,'test_2026':tm,'test_mix':t.family.value_counts().to_dict() if len(t) else {},'checks':checks},'observed_branching':{'changed_top5_members_count':None,'changed_top10_members_count':None,'changed_rank_count':tm['trades'],'selection_divergence_reason':'point-in-time completed outcomes route family capital'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'fresh_adaptive_family_portfolio_gate'},'remaining_risks':['baseline and mark-to-market drawdown pending only if test passes']}
 if len(t):t.to_parquet(out/'test_2026_portfolio_ledger.parquet',index=False)
 (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'eligible_count':len(eligible),'chosen':chosen,'test':tm,'test_mix':payload['authoritative_result']['test_mix'],'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
