from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_full_universe_clean_breakout_breadth_oos_v1 import metrics,annual,portfolio,ready

AXIS_ID='full_universe_breakout_takeprofit_oos_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb');OUT=Path(r'G:\Tradex\full_universe_breakout_takeprofit_oos_v1');TPS=(None,.03,.05,.07,.10,.15);STOP=.03

def extract(db:Path)->pd.DataFrame:
 q="""
 WITH b AS (SELECT code,date,CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) ymd,o,h,l,c,row_number() OVER(PARTITION BY code ORDER BY date) rn,
 avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) ma7,avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20,avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) ma60,max(h) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) prior20_high,CASE WHEN h>l THEN (least(o,c)-l)/(h-l) ELSE 0 END lower_wick_ratio,CASE WHEN h>l THEN (c-l)/(h-l) ELSE 0 END close_pos,lead(c,20) OVER(PARTITION BY code ORDER BY date) exit_close,lead(CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER),20) OVER(PARTITION BY code ORDER BY date) exit_ymd FROM daily_bars WHERE source='pan'),
 f AS (SELECT *,lag(ma20,5) OVER(PARTITION BY code ORDER BY date) m20b,lag(ma60,20) OVER(PARTITION BY code ORDER BY date) m60b FROM b),
 cand0 AS (SELECT * FROM f WHERE ymd BETWEEN 20190101 AND 20251231 AND exit_close IS NOT NULL AND c>=prior20_high AND ma7>ma20 AND ma20>ma60 AND ma20>m20b AND ma60>m60b AND close_pos>=.8),
 cand AS (SELECT *,row_number() OVER(PARTITION BY ymd ORDER BY lower_wick_ratio,code) day_rank FROM cand0), top5 AS (SELECT * FROM cand WHERE day_rank<=5),
 path AS (SELECT s.code,s.ymd,s.c,s.exit_close,s.exit_ymd,s.lower_wick_ratio,p.rn-s.rn step,p.l,p.h FROM top5 s JOIN b p ON p.code=s.code AND p.rn>s.rn AND p.rn<=s.rn+20),
 agg AS (SELECT code,ymd,any_value(c) c,any_value(exit_close) exit_close,any_value(exit_ymd) exit_ymd,any_value(lower_wick_ratio) lower_wick_ratio,
 min(CASE WHEN l<=c*.97 THEN step END) stop_step,
 min(CASE WHEN h>=c*1.03 THEN step END) tp03_step,min(CASE WHEN h>=c*1.05 THEN step END) tp05_step,min(CASE WHEN h>=c*1.07 THEN step END) tp07_step,min(CASE WHEN h>=c*1.10 THEN step END) tp10_step,min(CASE WHEN h>=c*1.15 THEN step END) tp15_step
 FROM path GROUP BY code,ymd)
 SELECT * FROM agg ORDER BY ymd,lower_wick_ratio,code
 """
 with duckdb.connect(str(db),read_only=True) as c:return c.execute(q).fetchdf()

def events(raw:pd.DataFrame,tp:float|None)->pd.DataFrame:
 x=raw.copy();tpcol=None if tp is None else f'tp{int(tp*100):02d}_step'
 def outcome(r):
  ss=r.stop_step if pd.notna(r.stop_step) else 999
  ts=999 if tpcol is None or pd.isna(r[tpcol]) else r[tpcol]
  if ss<999 and ss<=ts:return (-STOP,True)
  if ts<999 and ts<ss:return (tp,False)
  return (r.exit_close/r.c-1,False)
 vals=x.apply(outcome,axis=1);x['ret']=[v[0] for v in vals];x['stopped']=[v[1] for v in vals];return x

def run(db:Path,out:Path):
 raw=extract(db);reports=[];sets={}
 for tp in TPS:
  x=events(raw,tp);sets[tp]=x;reports.append({'take_profit_pct':tp,'train_2019_2021':metrics(x[x.ymd.between(20190101,20211231)]),'validation_2022_2023':metrics(x[x.ymd.between(20220101,20231231)]),'test_2024_2025':metrics(x[x.ymd.between(20240101,20251231)])})
 eligible=[r for r in reports if r['train_2019_2021']['n']>=100 and (r['train_2019_2021']['profit_factor'] or 0)>=1.2 and (r['train_2019_2021']['expectancy'] or 0)>0];chosen=max(eligible,key=lambda r:r['train_2019_2021']['profit_factor']) if eligible else None;tp=chosen['take_profit_pct'] if chosen else 'none';final=sets[tp] if chosen else raw.iloc[0:0].copy();val=metrics(final[final.ymd.between(20220101,20231231)]) if chosen else metrics(final);test=metrics(final[final.ymd.between(20240101,20251231)]) if chosen else metrics(final);stable=bool(chosen and (val['profit_factor'] or 0)>=1.1 and (val['expectancy'] or 0)>0 and (test['profit_factor'] or 0)>=1.1 and (test['expectancy'] or 0)>0)
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);final.to_csv(root/'selected_events.csv',index=False)
 payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','boundary_owner':'TRADEX','fixed_evaluation_conditions':{'universe':'all PAN daily_bars codes','base_shape':'close>=prior20_high; MA7>MA20>MA60; MA20 rising5; MA60 rising20; close_pos>=0.8','changed_axis':'take profit only','take_profit_levels':TPS,'same_bar_policy':'stop_first_conservative','ranking':'lower wick ascending top5/day','entry':'signal close','stop':STOP,'max_hold_rows':20,'selection_period':'2019-2021 only','validation_period':'2022-2023','untouched_test':'2024-2025','costs':'not modeled'},'reports':reports,'selection':{'protocol':'highest train PF with n>=100 PF>=1.2 expectancy>0','selected_take_profit_pct':chosen['take_profit_pct'] if chosen else None},'selected_rule':{'annual':annual(final) if chosen else {},'all_events':metrics(final),'portfolio':portfolio(final) if chosen else None},'decision':{'candidate_local_decision':'keep_for_same_condition_comparison' if stable else 'drop','authoritative_rollup_decision':'research_only','reason_type':'all_validation_splits_positive_and_pf_at_least_1_1' if stable else 'failed_fixed_split_stability_gate'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False}
 (root/'compare.json').write_text(json.dumps(ready(payload),ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,default=DB);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();run(a.db,a.out)
