from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_full_universe_breakout_hold_oos_v1 import extract
from tradex_full_universe_clean_breakout_breadth_oos_v1 import metrics,annual,portfolio,ready
AXIS_ID='full_universe_breakout_execution_oos_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb');OUT=Path(r'G:\Tradex\full_universe_breakout_execution_oos_v1');EXECUTIONS=('signal_close','next_open')
def events(raw:pd.DataFrame,mode:str)->pd.DataFrame:
 x=raw[['code','ymd','c','next_open','lower_wick_ratio','c20','d20','l20']].copy();x['entry']=x.c if mode=='signal_close' else x.next_open;x['ret']=x.apply(lambda r:-.03 if r.l20<=r.entry*.97 else r.c20/r.entry-1,axis=1);x['stopped']=x.l20<=x.entry*.97;x['exit_ymd']=x.d20;return x
def run(db:Path,out:Path):
 raw=extract(db);sets={m:events(raw,m) for m in EXECUTIONS};reports=[{'execution':m,'train_2019_2021':metrics(x[x.ymd.between(20190101,20211231)]),'validation_2022_2023':metrics(x[x.ymd.between(20220101,20231231)]),'test_2024_2025':metrics(x[x.ymd.between(20240101,20251231)])} for m,x in sets.items()]
 eligible=[r for r in reports if r['train_2019_2021']['n']>=100 and (r['train_2019_2021']['profit_factor'] or 0)>=1.2 and (r['train_2019_2021']['expectancy'] or 0)>0];chosen=max(eligible,key=lambda r:r['train_2019_2021']['profit_factor']) if eligible else None;mode=chosen['execution'] if chosen else None;final=sets[mode] if mode else raw.iloc[0:0].copy();val=metrics(final[final.ymd.between(20220101,20231231)]) if mode else metrics(final);test=metrics(final[final.ymd.between(20240101,20251231)]) if mode else metrics(final);stable=bool(chosen and (val['profit_factor'] or 0)>=1.1 and (val['expectancy'] or 0)>0 and (test['profit_factor'] or 0)>=1.1 and (test['expectancy'] or 0)>0)
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);final.to_csv(root/'selected_events.csv',index=False)
 payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','boundary_owner':'TRADEX','fixed_evaluation_conditions':{'universe':'all PAN daily_bars codes','base_shape':'close>=prior20_high; MA7>MA20>MA60; MA20 rising5; MA60 rising20; close_pos>=0.8','changed_axis':'execution price only','executions':EXECUTIONS,'ranking':'lower wick ascending top5/day','stop':.03,'hold':'signal row +20 close','selection_period':'2019-2021 only','validation_period':'2022-2023','untouched_test':'2024-2025','costs':'not modeled'},'reports':reports,'selection':{'protocol':'highest train PF with n>=100 PF>=1.2 expectancy>0','selected_execution':mode},'selected_rule':{'annual':annual(final) if mode else {},'all_events':metrics(final),'portfolio':portfolio(final) if mode else None},'decision':{'candidate_local_decision':'keep_for_same_condition_comparison' if stable else 'drop','authoritative_rollup_decision':'research_only','reason_type':'all_validation_splits_positive_and_pf_at_least_1_1' if stable else 'failed_fixed_split_stability_gate'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False}
 (root/'compare.json').write_text(json.dumps(ready(payload),ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,default=DB);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();run(a.db,a.out)
