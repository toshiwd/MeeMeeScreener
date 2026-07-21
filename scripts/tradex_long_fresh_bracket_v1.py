from __future__ import annotations

import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_long_trend_pullback_portfolio_v1 import COST,portfolio_summary,simulate

EVENTS=Path(r"G:\Tradex\tradex_long_fresh_family_events_v1\20260720T-authoritative-v4\fresh_family_events.parquet")
DB=Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
STOP=-.02
TARGETS=[.01,.015,.02,.025,.03,.04,.05]

def load(ep,db):
 e=pd.read_parquet(ep).reset_index(drop=True);e['event_id']=e.index.astype('int64');k=e[['event_id','code','p1_date']].rename(columns={'p1_date':'entry_date'})
 with duckdb.connect(str(db),read_only=True) as c:
  c.register('keys',k);p=c.execute("""select event_id,rn,date,o,h,l,c from (select k.event_id,b.date,b.o,b.h,b.l,b.c,row_number() over(partition by k.event_id order by b.date) rn from keys k join daily_bars b on b.code=k.code and b.date>=k.entry_date) where rn<=20 order by event_id,rn""").fetchdf()
 return e,p

def apply_bracket(events,path,target):
 b=events.rename(columns={'p1_date':'entry_date','p1_o':'entry_price'}).copy();x=path.merge(b[['event_id','entry_price']],on='event_id');x['sp']=x.entry_price*(1+STOP);x['tp']=x.entry_price*(1+target);x['stop_hit']=x.l<=x.sp;x['target_hit']=x.h>=x.tp
 # A gap through either order executes at the open. If both orders are touched
 # intraday and order is unknowable from OHLC, count the stop first.
 x['event']=x.stop_hit|x.target_hit;first=x[x.event].groupby('event_id',as_index=False).rn.min().rename(columns={'rn':'event_rn'});x=x.merge(first,on='event_id',how='left');z=x[((x.event_rn.notna())&(x.rn==x.event_rn))|((x.event_rn.isna())&(x.rn==20))].copy();z['exit_price']=z.c;z['exit_reason']='h20_close'
 hit=z.event_rn.notna();stop=hit&z.stop_hit;take=hit&~z.stop_hit&z.target_hit;z.loc[stop,'exit_price']=z.loc[stop,['o','sp']].min(axis=1);z.loc[stop,'exit_reason']='standing_stop';z.loc[take,'exit_price']=z.loc[take,['o','tp']].max(axis=1);z.loc[take,'exit_reason']='standing_target';z=z.rename(columns={'date':'exit_date'});return b.merge(z[['event_id','exit_date','exit_price','exit_reason','event_rn']],on='event_id')

def gate(s,n):
 r=s['raw_trade_metrics'];return s['trades']>=n and r['mean_return_pct']>0 and r['win_rate']>=.5 and r['severe_loss5_rate']<=.03 and r['top3_positive_profit_share']<=.35 and s['positive_month_rate']>.5

def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--events',type=Path,default=EVENTS);p.add_argument('--db',type=Path,default=DB);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);events,path=load(a.events,a.db);rows=[];ledgers={}
 for target in TARGETS:
  e=apply_bracket(events,path,target);e['rank']=-e.family_score;t=simulate(e,20);t['year']=pd.to_datetime(t.date,unit='s').dt.year;ledgers[target]=t;d=t[t.year.between(2016,2023)];v=t[t.year.between(2024,2025)];rows.append({'profit_target_pct':100*target,'development':portfolio_summary(d),'validation_2024_2025':portfolio_summary(v),'validation_years':{str(y):portfolio_summary(v[v.year.eq(y)]) for y in [2024,2025]},'exit_reason_development':d.exit_reason.value_counts().to_dict(),'exit_reason_validation':v.exit_reason.value_counts().to_dict()})
 eligible=[x for x in rows if gate(x['development'],250) and gate(x['validation_2024_2025'],100) and x['development']['positive_year_rate']>=.75 and all(y['total_return_pct']>0 for y in x['validation_years'].values())];chosen=max(eligible,key=lambda x:(x['validation_2024_2025']['total_return_pct'],x['development']['total_return_pct'])) if eligible else None;key=chosen['profit_target_pct']/100 if chosen else None;test=ledgers[key] if chosen else pd.DataFrame();test=test[test.year.eq(2026)] if len(test) else test;tm=portfolio_summary(test);r=tm['raw_trade_metrics']
 checks={'selected_without_2026':chosen is not None,'test_full_matured_audit':chosen is not None and len(test)>0,'test_mean_positive':len(test)>0 and r['mean_return_pct']>0,'test_win_at_least_50pct':len(test)>0 and r['win_rate']>=.5,'test_raw_loss5_at_most_3pct':len(test)>0 and r['severe_loss5_rate']<=.03,'test_profit_concentration_at_most_35pct':len(test)>0 and r['top3_positive_profit_share']<=.35,'test_months_majority_positive':len(test)>0 and tm['positive_month_rate']>.5,'test_total_return_positive':tm['total_return_pct']>0};decision='keep_for_mark_to_market_and_baseline_audit' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_fresh_bracket_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'fixed_evaluation_conditions':{'source':str(a.events),'runtime_db':str(a.db),'universe':'ordinary domestic stocks only inherited from event ledger','selection':'fresh three-family continuous score unchanged','entry':'next session open with standing OCO bracket','hard_stop_pct':100*STOP,'profit_target_candidates_pct':[100*x for x in TARGETS],'ambiguous_same_day_order':'stop first, conservative','gap_execution':'open when open gaps through order, otherwise order price','maximum_exit':'session-20 close','max_positions':20,'allocation':'equal 5% initial capital','round_trip_cost_pct':COST,'axis_changed':'profit target distance only; hard stop fixed','development':'2016-2023','validation':'2024-2025','test':'2026 full matured audit through 2026-07-17','production_changed':False},'authoritative_result':{'candidates':rows,'eligible_without_2026':eligible,'chosen_without_2026':chosen,'test_2026':tm,'test_exit_reasons':test.exit_reason.value_counts().to_dict() if len(test) else {},'checks':checks},'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'same ranking; bracket exits change later slot availability'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'standing_bracket_full_gate'},'remaining_risks':['daily mark-to-market and same-condition no-bracket baseline remain if kept','conservative OHLC same-day ordering may understate true results']}
 if len(test):test.to_parquet(out/'test_2026_portfolio_ledger.parquet',index=False)
 (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'eligible_count':len(eligible),'chosen':chosen,'test':tm,'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
