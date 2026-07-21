from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_full_universe_clean_breakout_breadth_oos_v1 import metrics,annual,portfolio,ready

AXIS_ID='full_universe_breakout_hold_oos_v1'; DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb');OUT=Path(r'G:\Tradex\full_universe_breakout_hold_oos_v1');HOLDS=(5,10,15,20,30)

def extract(db:Path)->pd.DataFrame:
 q="""
 WITH b AS (SELECT code,CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) ymd,o,h,l,c,
 avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) ma7,
 avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20,
 avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) ma60,
 max(h) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) prior20_high,
 CASE WHEN h>l THEN (least(o,c)-l)/(h-l) ELSE 0 END lower_wick_ratio,
 CASE WHEN h>l THEN (c-l)/(h-l) ELSE 0 END close_pos,
 lead(o,1) OVER(PARTITION BY code ORDER BY date) next_open,lead(c,5) OVER(PARTITION BY code ORDER BY date) c5,lead(c,10) OVER(PARTITION BY code ORDER BY date) c10,lead(c,15) OVER(PARTITION BY code ORDER BY date) c15,lead(c,20) OVER(PARTITION BY code ORDER BY date) c20,lead(c,30) OVER(PARTITION BY code ORDER BY date) c30,
 lead(CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER),5) OVER(PARTITION BY code ORDER BY date) d5,lead(CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER),10) OVER(PARTITION BY code ORDER BY date) d10,lead(CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER),15) OVER(PARTITION BY code ORDER BY date) d15,lead(CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER),20) OVER(PARTITION BY code ORDER BY date) d20,lead(CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER),30) OVER(PARTITION BY code ORDER BY date) d30,
 min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING) l5,min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 10 FOLLOWING) l10,min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING) l15,min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) l20,min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 30 FOLLOWING) l30
 FROM daily_bars WHERE source='pan'), f AS (SELECT *,lag(ma20,5) OVER(PARTITION BY code ORDER BY ymd) m20b,lag(ma60,20) OVER(PARTITION BY code ORDER BY ymd) m60b FROM b), s AS
 (SELECT *,row_number() OVER(PARTITION BY ymd ORDER BY lower_wick_ratio,code) day_rank FROM f WHERE ymd BETWEEN 20190101 AND 20251231 AND c30 IS NOT NULL AND c>=prior20_high AND ma7>ma20 AND ma20>ma60 AND ma20>m20b AND ma60>m60b AND close_pos>=.8)
 SELECT * FROM s WHERE day_rank<=5
 """
 with duckdb.connect(str(db),read_only=True) as c:return c.execute(q).fetchdf()

def events(raw:pd.DataFrame,h:int)->pd.DataFrame:
 x=raw[['code','ymd','c','lower_wick_ratio',f'c{h}',f'd{h}',f'l{h}']].copy();x['ret']=x.apply(lambda r:-.03 if r[f'l{h}']<=r.c*.97 else r[f'c{h}']/r.c-1,axis=1);x['stopped']=x[f'l{h}']<=x.c*.97;x['exit_ymd']=x[f'd{h}'];return x

def run(db:Path,out:Path):
 raw=extract(db); reports=[]; sets={}
 for h in HOLDS:
  x=events(raw,h);sets[h]=x;reports.append({'hold_rows':h,'train_2019_2021':metrics(x[x.ymd.between(20190101,20211231)]),'validation_2022_2023':metrics(x[x.ymd.between(20220101,20231231)]),'test_2024_2025':metrics(x[x.ymd.between(20240101,20251231)])})
 eligible=[r for r in reports if r['train_2019_2021']['n']>=100 and (r['train_2019_2021']['profit_factor'] or 0)>=1.2 and (r['train_2019_2021']['expectancy'] or 0)>0];chosen=max(eligible,key=lambda r:r['train_2019_2021']['profit_factor']) if eligible else None;h=chosen['hold_rows'] if chosen else None;final=sets[h] if h else raw.iloc[0:0].copy();val=metrics(final[final.ymd.between(20220101,20231231)]) if h else metrics(final);test=metrics(final[final.ymd.between(20240101,20251231)]) if h else metrics(final);stable=bool(chosen and (val['profit_factor'] or 0)>=1.1 and (val['expectancy'] or 0)>0 and (test['profit_factor'] or 0)>=1.1 and (test['expectancy'] or 0)>0)
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);final.to_csv(root/'selected_events.csv',index=False)
 payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','boundary_owner':'TRADEX','fixed_evaluation_conditions':{'universe':'all PAN daily_bars codes','base_shape':'close>=prior20_high; MA7>MA20>MA60; MA20 rising5; MA60 rising20; close_pos>=0.8','changed_axis':'hold rows only','holds':HOLDS,'ranking':'lower wick ascending top5/day','entry':'signal close','stop':.03,'selection_period':'2019-2021 only','validation_period':'2022-2023','untouched_test':'2024-2025','costs':'not modeled'},'reports':reports,'selection':{'protocol':'highest train PF with n>=100 PF>=1.2 expectancy>0','selected_hold_rows':h},'selected_rule':{'annual':annual(final) if h else {},'all_events':metrics(final),'portfolio':portfolio(final) if h else None},'decision':{'candidate_local_decision':'keep_for_same_condition_comparison' if stable else 'drop','authoritative_rollup_decision':'research_only','reason_type':'all_validation_splits_positive_and_pf_at_least_1_1' if stable else 'failed_fixed_split_stability_gate'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False}
 (root/'compare.json').write_text(json.dumps(ready(payload),ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,default=DB);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();run(a.db,a.out)
