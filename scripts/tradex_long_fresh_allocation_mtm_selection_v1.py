from __future__ import annotations

import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_long_fresh_mark_to_market_v1 import DEFAULT_DB,DEFAULT_EVENTS,accepted_trades,load_marks,nav_series,period_summary
from tradex_long_trend_pullback_portfolio_v1 import portfolio_summary

CAPS=[None,.03,.04,.05]
PERIODS={'development':(2016,2023),'validation_2024_2025':(2024,2025),'audit_2026':(2026,2026)}

def label(cap):return 'equal_5pct' if cap is None else f'volatility_cap_{100*cap:.1f}pct'
def weight(trades,cap):return pd.Series(.05,index=trades.index) if cap is None else .05*(cap/trades.realized_vol20.clip(lower=.001)).clip(upper=1)
def event_period(t,lo,hi):
 z=t[pd.to_datetime(t.date,unit='s').dt.year.between(lo,hi)].copy();return portfolio_summary(z)

def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--events',type=Path,default=DEFAULT_EVENTS);p.add_argument('--db',type=Path,default=DEFAULT_DB);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 trades=accepted_trades(a.events);marks,calendar=load_marks(a.db,trades);rows=[];daily_map={};ledger_map={}
 for cap in CAPS:
  name=label(cap);wcol=f'w_{name}';trades[wcol]=weight(trades,cap);daily,all_summary=nav_series(trades,marks,calendar,wcol);daily_map[name]=daily
  ledger=trades.copy();ledger['allocation_weight']=ledger[wcol];ledger['realized_ret']=ledger.raw_return_pct*ledger.allocation_weight;ledger_map[name]=ledger
  rows.append({'scheme':name,'volatility_cap':cap,'all_history_mark_to_market':all_summary,'mark_to_market_periods':{k:period_summary(daily,*v) for k,v in PERIODS.items()},'event_periods':{k:event_period(ledger,*v) for k,v in PERIODS.items()},'mean_allocation_pct':100*float(ledger.allocation_weight.mean())})
 baseline=next(x for x in rows if x['scheme']=='equal_5pct')
 def pre_gate(x):
  for key,n in [('development',250),('validation_2024_2025',100)]:
   ev=x['event_periods'][key];raw=ev['raw_trade_metrics'];capm=ev['capital_contribution_metrics'];mtm=x['mark_to_market_periods'][key];b=baseline['mark_to_market_periods'][key]
   if not(ev['trades']>=n and raw['mean_return_pct']>0 and raw['win_rate']>=.5 and capm['severe_loss5_rate']<=.03 and capm['top3_positive_profit_share']<=.35 and mtm['return_pct']>0 and mtm['positive_month_rate']>.5 and mtm['positive_year_rate']>=.75 and mtm['worst_month_pct']>b['worst_month_pct'] and mtm['return_pct']/abs(mtm['max_drawdown_pct'])>b['return_pct']/abs(b['max_drawdown_pct'])):return False
  return True
 eligible=[x for x in rows if x['scheme']!='equal_5pct' and pre_gate(x)];chosen=max(eligible,key=lambda x:x['mark_to_market_periods']['validation_2024_2025']['return_pct']) if eligible else None
 test=chosen['event_periods']['audit_2026'] if chosen else {};mtm=chosen['mark_to_market_periods']['audit_2026'] if chosen else {};bt=baseline['mark_to_market_periods']['audit_2026'];raw=test.get('raw_trade_metrics',{});cm=test.get('capital_contribution_metrics',{})
 checks={'selected_without_2026':chosen is not None,'test_n250_or_full_audit':chosen is not None and test.get('trades',0)>0,'test_mean_positive':raw.get('mean_return_pct',-99)>0,'test_win_at_least_50pct':raw.get('win_rate',0)>=.5,'test_capital_loss5_at_most_3pct':cm.get('severe_loss5_rate',1)<=.03,'test_profit_concentration_at_most_35pct':cm.get('top3_positive_profit_share',1)<=.35,'test_mark_to_market_return_positive':mtm.get('return_pct',-99)>0,'test_months_majority_positive':mtm.get('positive_month_rate',0)>.5,'test_drawdown_better_than_equal':mtm.get('max_drawdown_pct',-999)>bt['max_drawdown_pct'],'test_exposure_at_most_100pct':mtm.get('max_gross_exposure_pct',999)<=100+1e-9}
 decision='keep_for_final_lookahead_and_adoption_audit' if all(checks.values()) else 'drop';name=chosen['scheme'] if chosen else None
 payload={'schema_version':'tradex_long_fresh_allocation_mtm_selection_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'fixed_evaluation_conditions':{'source':str(a.events),'runtime_db':str(a.db),'universe':'ordinary domestic stocks only inherited from event ledger','selection':'fresh three-family continuous score unchanged','entry':'next session open','exit':'session-20 close','cost_pct':.3,'max_positions':20,'baseline':'equal 5% initial-capital allocation','allocation_candidates':{label(c):c for c in CAPS},'allocation_formula':'5% * min(1, volatility cap / prior 20-day realized volatility); residual cash','selection_without_2026':'pass all development and 2024-2025 gates; improve worst month and return/max-drawdown ratio over equal in both periods; then maximize validation return','daily_valuation':'close mark-to-market with exit cost charged on exit close','development':'2016-2023','validation':'2024-2025','test':'2026 full matured audit through 2026-07-17','production_changed':False},'authoritative_result':{'candidates':rows,'equal_baseline':baseline,'eligible_without_2026':[x['scheme'] for x in eligible],'chosen_without_2026':chosen,'checks':checks},'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'same accepted trades; capital weights only'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'fixed_condition_mark_to_market_allocation_gate'},'remaining_risks':['final point-in-time leakage audit and requirement-by-requirement adoption audit remain if kept','raw trade loss rate remains reported separately from capital contribution loss rate']}
 if chosen:
  ledger_map[name].to_parquet(out/'chosen_full_trade_ledger.parquet',index=False);daily_map[name].to_parquet(out/'chosen_daily_nav.parquet',index=False);daily_map['equal_5pct'].to_parquet(out/'equal_daily_nav.parquet',index=False)
 (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'eligible':[x['scheme'] for x in eligible],'chosen':name,'test':mtm,'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
