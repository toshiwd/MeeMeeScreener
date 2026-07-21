from __future__ import annotations

import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_long_fresh_mark_to_market_v1 import DEFAULT_DB,DEFAULT_EVENTS,accepted_trades,load_marks

CAPS=[None,.04,.05]
PERIODS={'development':(2016,2023),'validation_2024_2025':(2024,2025),'audit_2026':(2026,2026)}
COST=.003

def name(cap):return 'equal_5pct' if cap is None else f'volatility_cap_{100*cap:.1f}pct'
def scale(row,cap):return 1.0 if cap is None else min(1.0,cap/max(float(row.realized_vol20),.001))

def replay(trades,marks,calendar,cap,base_weight=.05,market_threshold=None):
    entries={int(k):g.sort_values(['rank','code']) for k,g in trades.groupby('entry_date')}
    marks_by_day={int(day):g for day,g in marks.groupby('date')}
    cash=1.0;positions={};last_price={};last_nav=1.0;daily=[];closed=[]
    for day in calendar.date.astype('int64'):
        start_nav=last_nav
        for r in entries.get(int(day),pd.DataFrame()).itertuples():
            market_scale=1.0
            if market_threshold is not None:
                strength=.5*float(r.market_breadth_ma20)+.5*float(r.market_advancers_ratio)
                market_scale=min(1.0,max(0.0,strength/market_threshold))
            target=start_nav*base_weight*scale(r,cap)*market_scale;notional=min(target,cash)
            if notional<=1e-12:continue
            cash-=notional;last_price[int(r.trade_id)]=float(r.entry_price);positions[int(r.trade_id)]={'trade_id':int(r.trade_id),'code':str(r.code),'entry_date':int(r.entry_date),'exit_date':int(r.exit_date),'entry_price':float(r.entry_price),'shares':notional/float(r.entry_price),'entry_notional':notional,'entry_nav':start_nav,'family':r.family,'raw_return_pct':float(r.raw_return_pct),'allocation_pct':100*notional/start_nav}
        for r in marks_by_day.get(int(day),pd.DataFrame()).itertuples():
            last_price[int(r.trade_id)]=float(r.close)
        market_value=0.0
        for tid,pos in positions.items():market_value+=pos['shares']*last_price[tid]
        pre_exit_nav=cash+market_value;pre_exit_exposure=market_value/pre_exit_nav if pre_exit_nav>0 else float('inf')
        exiting=[tid for tid,pos in positions.items() if pos['exit_date']==int(day)]
        for tid in exiting:
            pos=positions.pop(tid);gross=pos['shares']*last_price[tid];proceeds=gross-pos['entry_notional']*COST;cash+=proceeds;pnl=proceeds-pos['entry_notional'];pos.update({'exit_price':last_price[tid],'pnl':pnl,'capital_contribution_pct':100*pnl/pos['entry_nav']});closed.append(pos)
        remaining=sum(pos['shares']*last_price[tid] for tid,pos in positions.items());last_nav=cash+remaining
        daily.append({'date':int(day),'nav':last_nav,'cash':cash,'end_market_value':remaining,'end_exposure':remaining/last_nav if last_nav>0 else float('inf'),'max_intraday_exposure':pre_exit_exposure,'positions_before_exit':len(positions)+len(exiting),'positions_end':len(positions)})
    return pd.DataFrame(daily).assign(dt=lambda x:pd.to_datetime(x.date,unit='s')),pd.DataFrame(closed)

def period(daily,lo,hi):
    mask=daily.dt.dt.year.between(lo,hi);d=daily[mask].copy();before=daily[daily.date<d.date.min()];start=float(before.nav.iloc[-1]) if len(before) else 1.0
    curve=pd.concat([pd.DataFrame([{'nav':start}]),d[['nav']]],ignore_index=True);dd=curve.nav/curve.nav.cummax()-1
    me=d.groupby(d.dt.dt.to_period('M')).tail(1).copy();prev=start;mr=[]
    for v in me.nav:mr.append(float(v)/prev-1);prev=float(v)
    ye=d.groupby(d.dt.dt.year).tail(1).copy();prev=start;yr=[]
    for v in ye.nav:yr.append(float(v)/prev-1);prev=float(v)
    return {'return_pct':100*(float(d.nav.iloc[-1])/start-1),'max_drawdown_pct':100*float(dd.min()),'positive_month_rate':float((pd.Series(mr)>0).mean()),'positive_year_rate':float((pd.Series(yr)>0).mean()),'worst_month_pct':100*min(mr),'max_intraday_exposure_pct':100*float(d.max_intraday_exposure.max()),'max_positions':int(d.positions_before_exit.max()),'monthly_returns_pct':{str(m):100*r for m,r in zip(me.dt.dt.to_period('M'),mr)},'yearly_returns_pct':{str(y):100*r for y,r in zip(ye.dt.dt.year,yr)}}

def trade_metrics(ledger,lo,hi):
    z=ledger[pd.to_datetime(ledger.exit_date,unit='s').dt.year.between(lo,hi)].copy();pos=z[z.pnl>0];total=float(pos.pnl.sum());codes=z.code.nunique() if len(z) else 0
    return {'trades':int(len(z)),'codes':int(codes),'raw_mean_return_pct':float(z.raw_return_pct.mean()),'raw_win_rate':float(z.raw_return_pct.gt(0).mean()),'raw_loss5_rate':float(z.raw_return_pct.le(-5).mean()),'capital_loss5_rate':float(z.capital_contribution_pct.le(-5).mean()),'top3_positive_profit_share':None if total<=0 else float(pos.nlargest(3,'pnl').pnl.sum()/total),'mean_allocation_pct':float(z.allocation_pct.mean()),'total_pnl':float(z.pnl.sum())}

def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--events',type=Path,default=DEFAULT_EVENTS);p.add_argument('--db',type=Path,default=DEFAULT_DB);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);trades=accepted_trades(a.events);marks,calendar=load_marks(a.db,trades);rows=[];store={}
 for cap in CAPS:
  n=name(cap);d,l=replay(trades,marks,calendar,cap);store[n]=(d,l);rows.append({'scheme':n,'volatility_cap':cap,'periods':{k:period(d,*v) for k,v in PERIODS.items()},'trade_metrics':{k:trade_metrics(l,*v) for k,v in PERIODS.items()},'terminal_nav':float(d.nav.iloc[-1])})
 base=next(x for x in rows if x['scheme']=='equal_5pct')
 def pre(x):
  for k,minn in [('development',250),('validation_2024_2025',100)]:
   t=x['trade_metrics'][k];m=x['periods'][k];b=base['periods'][k]
   if not(t['trades']>=minn and t['raw_mean_return_pct']>0 and t['raw_win_rate']>=.5 and t['capital_loss5_rate']<=.03 and t['top3_positive_profit_share']<=.35 and m['return_pct']>0 and m['positive_month_rate']>.5 and m['positive_year_rate']>=.75 and m['worst_month_pct']>b['worst_month_pct'] and m['return_pct']/abs(m['max_drawdown_pct'])>b['return_pct']/abs(b['max_drawdown_pct']) and m['max_intraday_exposure_pct']<=100+1e-9):return False
  return True
 eligible=[x for x in rows if x['scheme']!='equal_5pct' and pre(x)];chosen=max(eligible,key=lambda x:x['periods']['validation_2024_2025']['return_pct']) if eligible else None;t=chosen['trade_metrics']['audit_2026'] if chosen else {};m=chosen['periods']['audit_2026'] if chosen else {};b=base['periods']['audit_2026']
 checks={'selected_without_2026':chosen is not None,'test_250_or_full_exit_audit':chosen is not None and t.get('trades',0)>0,'test_mean_positive':t.get('raw_mean_return_pct',-99)>0,'test_win_at_least_50pct':t.get('raw_win_rate',0)>=.5,'test_capital_loss5_at_most_3pct':t.get('capital_loss5_rate',1)<=.03,'test_profit_concentration_at_most_35pct':t.get('top3_positive_profit_share',1)<=.35,'test_months_majority_positive':m.get('positive_month_rate',0)>.5,'test_return_positive':m.get('return_pct',-99)>0,'test_drawdown_better_than_equal':m.get('max_drawdown_pct',-999)>b['max_drawdown_pct'],'test_exposure_at_most_100pct':m.get('max_intraday_exposure_pct',999)<=100+1e-9,'test_positions_at_most_20':m.get('max_positions',999)<=20};decision='keep_for_final_point_in_time_and_adoption_audit' if all(checks.values()) else 'drop';cn=chosen['scheme'] if chosen else None
 payload={'schema_version':'tradex_long_fresh_compounded_portfolio_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'fixed_evaluation_conditions':{'source':str(a.events),'runtime_db':str(a.db),'universe':'ordinary domestic stocks only inherited from event ledger','selection':'fresh three-family continuous score unchanged','entry':'next session open','exit':'session-20 close','round_trip_cost_pct':100*COST,'max_positions':20,'capital_model':'compound NAV; each new position targets 5% of prior close NAV; capped by available cash; no leverage','baseline':'equal 5% NAV allocation','challengers':{name(c):c for c in CAPS if c is not None},'allocation_formula':'5% NAV * min(1, volatility cap / prior 20-day realized volatility)','split_rule':'trade gates use exit year; daily gates use calendar NAV; no 2026 exit result used for selection','selection_without_2026':'improve worst month and return/max-drawdown over equal in development and validation, then maximize validation return','development':'2016-2023 exits/calendar','validation':'2024-2025 exits/calendar','test':'all exits/calendar in 2026 through 2026-07-17','production_changed':False},'authoritative_result':{'candidates':rows,'equal_baseline':base,'eligible_without_2026':[x['scheme'] for x in eligible],'chosen_without_2026':chosen,'checks':checks},'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'same accepted trades; only compounded capital weights differ'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'compounded_cash_constrained_portfolio_gate'},'remaining_risks':['final feature point-in-time audit and requirement-by-requirement adoption audit remain if kept','raw trade loss rate is diagnostic; goal loss gate is capital contribution under portfolio sizing']}
 if chosen:
  d,l=store[cn];d.to_parquet(out/'chosen_daily_nav.parquet',index=False);l.to_parquet(out/'chosen_trade_ledger.parquet',index=False);store['equal_5pct'][0].to_parquet(out/'equal_daily_nav.parquet',index=False)
 (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'eligible':[x['scheme'] for x in eligible],'chosen':cn,'test_trade':t,'test_nav':m,'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
