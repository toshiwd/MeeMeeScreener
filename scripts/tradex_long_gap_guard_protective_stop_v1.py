from __future__ import annotations
import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd

STOPS=[3.0,5.0,7.5,10.0];COST=0.3
def metrics(x):
 if len(x)==0:return {'n':0,'codes':0,'mean_return_pct':None,'median_return_pct':None,'win_rate':None,'severe_loss5_rate':None,'top3_positive_profit_share':None}
 p=x[x.realized_ret>0].realized_ret;s=float(p.sum());return {'n':int(len(x)),'codes':int(x.code.nunique()),'mean_return_pct':float(x.realized_ret.mean()),'median_return_pct':float(x.realized_ret.median()),'win_rate':float((x.realized_ret>0).mean()),'severe_loss5_rate':float((x.realized_ret<=-5).mean()),'top3_positive_profit_share':None if s<=0 else float(p.nlargest(3).sum()/s)}
def simulate(g,sl):
 entry=float(g.entry_price.iloc[0]);stop=entry*(1-sl/100)
 for _,r in g.sort_values('session_no').iterrows():
  if float(r.o)<=stop:return 100*(float(r.o)/entry-1)-COST
  if float(r.l)<=stop:return -sl-COST
 return 100*(float(g.sort_values('session_no').iloc[-1].c)/entry-1)-COST
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--source',default=r'G:\Tradex\long_selector_10y_research_v1\20260626T054716Z-long_selector_10y_research_v1\strict_breakout_top2_events.csv');a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/'app')]
 from backend.services.codex_bridge_service import get_runtime_stock_db_status
 runtime=get_runtime_stock_db_status();source=pd.read_csv(a.source,dtype={'code':str})
 source=source[(source.ret1<=.045177686069181794)&(source.ret60>=.0255858526539346)&(source.market_ma60_slope20_median<=.0205884368895439)].copy();source['event_id']=range(len(source))
 with duckdb.connect(runtime['selected_runtime_db_path'],read_only=True) as c:
  c.register('source_input',source)
  events=c.execute("""select * exclude(rn) from (select s.event_id,s.code,s.date,b.date entry_date,b.o entry_price,s.c signal_close,b.o/s.c-1 gap,row_number() over(partition by s.event_id order by b.date) rn from source_input s join daily_bars b on b.code=s.code and b.date>s.date) where rn=1 and gap between 0 and .02""").fetchdf();events['entry_date']=events.entry_date.astype('int64');c.register('events_input',events)
  bars=c.execute("""select * from (select e.event_id,e.code,e.date signal_date,e.entry_date,e.entry_price,i.market_code,b.date bar_date,b.o,b.h,b.l,b.c,row_number() over(partition by e.event_id order by b.date) session_no from events_input e join industry_master i using(code) join daily_bars b on b.code=e.code and b.date>=e.entry_date where i.market_code in ('プライム（内国株式）','スタンダード（内国株式）','グロース（内国株式）')) where session_no<=50""").fetchdf()
 complete=bars.groupby('event_id').size();valid_ids=complete[complete.eq(50)].index;bars=bars[bars.event_id.isin(valid_ids)];base=events[events.event_id.isin(valid_ids)].copy();base['signal_dt']=pd.to_datetime(base.date,unit='s');base['year']=base.signal_dt.dt.year
 for sl in STOPS:
  vals={int(eid):simulate(g,sl) for eid,g in bars.groupby('event_id')};base[f'sl{sl}']=base.event_id.map(vals)
 rows=[]
 for sl in STOPS:
  z=base.copy();z['realized_ret']=z[f'sl{sl}'];dev=z[z.year.between(2016,2023)];val=z[z.year.between(2024,2025)];ym={str(y):metrics(dev[dev.year.eq(y)]) for y in range(2016,2024)};rows.append({'stop_loss_pct':sl,'development':metrics(dev),'development_years':ym,'positive_development_years':sum((m['mean_return_pct'] or -99)>0 for m in ym.values() if m['n']>0),'validation_2024_2025':metrics(val),'validation_years':{str(y):metrics(val[val.year.eq(y)]) for y in [2024,2025]}})
 eligible=[x for x in rows if x['development']['mean_return_pct']>0 and x['development']['win_rate']>=.50 and x['development']['severe_loss5_rate']<=.03 and x['positive_development_years']>=6 and x['validation_2024_2025']['mean_return_pct']>0 and x['validation_2024_2025']['win_rate']>=.50 and x['validation_2024_2025']['severe_loss5_rate']<=.03 and all((m['mean_return_pct'] or -99)>0 for m in x['validation_years'].values() if m['n']>0)]
 chosen=max(eligible,key=lambda x:(x['validation_2024_2025']['mean_return_pct'],x['development']['mean_return_pct'])) if eligible else None;test=base[base.year.eq(2026)].copy()
 if chosen:test['realized_ret']=test[f"sl{chosen['stop_loss_pct']}"]
 else:test=test.iloc[0:0].copy()
 sm=metrics(test);monthly={str(m):metrics(g) for m,g in test.groupby(test.signal_dt.dt.to_period('M'))};pos=sum((x['mean_return_pct'] or -99)>0 for x in monthly.values());checks={'ordinary_event_audit_complete':len(base)>0,'selected_without_2026':chosen is not None,'test_n250_or_full_audit':chosen is not None,'test_mean_positive':(sm['mean_return_pct'] or -99)>0,'test_win_at_least_50pct':(sm['win_rate'] or 0)>=.50,'test_severe5_at_most_3pct':(sm['severe_loss5_rate'] or 1)<=.03,'test_months_majority_positive':bool(monthly) and pos>len(monthly)/2,'profit_not_concentrated':(sm['top3_positive_profit_share'] or 1)<=.35};decision='hold_for_portfolio_gate' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_gap_guard_protective_stop_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'runtime':runtime,'fixed_evaluation_conditions':{'source':a.source,'source_rule_events':int(len(source)),'gap_0_to_2_events_current_db':int(len(events)),'ordinary_complete_events':int(len(base)),'universe':'ordinary domestic stocks only: Prime, Standard, Growth','selection':'existing strict_breakout_top2 gap guard contract fixed','entry':'next session open, signal-close gap 0% to 2%','exit':'daily protective stop or session-50 close','round_trip_cost_pct':COST,'same_day_order':'open gap first, then low stop','axis_changed':'protective stop width only','development':'2016-2023','validation':'2024-2025','full_audit_test':'2026 matured events through latest confirmed data','production_changed':False},'authoritative_result':{'candidates':rows,'eligible_without_2026':eligible,'chosen_without_2026':chosen,'test_2026':sm,'monthly_2026':monthly,'checks':checks},'observed_branching':{'changed_top5_members_count':None,'changed_top10_members_count':None,'changed_rank_count':sm['n'],'selection_divergence_reason':'selection fixed; exit stop width only'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'long_history_gap_guard_protective_stop_gate'},'remaining_risks':['overlap and capital allocation pending only if event gates pass']}
 test.to_parquet(out/'test_2026_ledger.parquet',index=False);base.to_parquet(out/'ordinary_event_exit_ledger.parquet',index=False);(out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'ordinary_events':[len(base),len(events)],'eligible_count':len(eligible),'chosen':chosen,'test':sm,'monthly':monthly,'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
