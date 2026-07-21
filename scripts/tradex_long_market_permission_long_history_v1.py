from __future__ import annotations
import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows,metrics
from tradex_long_cross_sectional_rank_v1 import add_scores,select_daily
from tradex_long_stop_width_v1 import run

PERMISSIONS={
 '許可なし':lambda x:pd.Series(True,index=x.index),
 '値上がり比率中立':lambda x:x.market_advancers_ratio.between(.30,.55),
 '市場平均静穏':lambda x:x.market_mean_ret1.abs()<=.0075,
 '市場 breadth 中立':lambda x:x.market_breadth_ma20.between(.25,.60),
 '値上がり比率中立かつ市場平均静穏':lambda x:x.market_advancers_ratio.between(.30,.55)&(x.market_mean_ret1.abs()<=.01),
}
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/'app')]
 from backend.services.codex_bridge_service import get_runtime_stock_db_status
 runtime=get_runtime_stock_db_status();d=load_rows(runtime['selected_runtime_db_path'],broad_trigger=False,min_date='2018-01-01');d['signal_date']=pd.to_datetime(d.date,unit='s');d=add_scores(d);d=select_daily(d,'安定上昇',5);need=[f'p{x}_{y}' for x in range(1,6) for y in ['o','h','l','c']];d=d[d[need].notna().all(axis=1)].copy();d['realized_ret']=d.apply(lambda r:run(r,3.0),axis=1);d=d[d.signal_date.ge('2019-01-01')];d['year']=d.signal_date.dt.year
 rows=[]
 for name,fn in PERMISSIONS.items():
  z=d[fn(d)];train=z[z.year.between(2019,2024)];val=z[z.year.eq(2025)];ym={str(y):metrics(train[train.year.eq(y)]) for y in range(2019,2025)};rows.append({'permission':name,'development':metrics(train),'development_years':ym,'positive_development_years':sum((m['mean_return_pct'] or -99)>0 for m in ym.values()),'validation_2025':metrics(val),'validation_active_dates':int(val.date.nunique()),'validation_possible_dates':int(d[d.year.eq(2025)].date.nunique())})
 eligible=[x for x in rows if x['permission']!='許可なし' and x['development']['mean_return_pct']>0 and x['development']['win_rate']>=.50 and x['development']['severe_loss5_rate']<=.03 and x['positive_development_years']>=5 and x['validation_2025']['n']>=250 and x['validation_2025']['mean_return_pct']>0 and x['validation_2025']['win_rate']>=.50 and x['validation_2025']['severe_loss5_rate']<=.03 and x['validation_2025']['top3_positive_profit_share']<=.35 and x['validation_active_dates']>=.40*x['validation_possible_dates']]
 chosen=max(eligible,key=lambda x:(x['validation_2025']['mean_return_pct'],x['positive_development_years'])) if eligible else None;test=d[d.year.eq(2026)&PERMISSIONS[chosen['permission']](d)] if chosen else d.iloc[0:0].copy();sm=metrics(test);monthly={str(m):metrics(g) for m,g in test.groupby(test.signal_date.dt.to_period('M'))};pos=sum((x['mean_return_pct'] or -99)>0 for x in monthly.values());active=int(test.date.nunique());possible=int(d[d.year.eq(2026)].date.nunique());checks={'selected_without_2026':chosen is not None,'test_n250_or_full_audit':sm['n']>=250 or (chosen is not None and active==int(d[d.year.eq(2026)&PERMISSIONS[chosen['permission']](d)].date.nunique())),'test_mean_positive':(sm['mean_return_pct'] or -99)>0,'test_win_at_least_50pct':(sm['win_rate'] or 0)>=.50,'test_severe5_at_most_3pct':(sm['severe_loss5_rate'] or 1)<=.03,'test_months_majority_positive':bool(monthly) and pos>len(monthly)/2,'profit_not_concentrated':(sm['top3_positive_profit_share'] or 1)<=.35,'active_dates_at_least_40pct':possible>0 and active/possible>=.40};decision='hold_for_portfolio_gate' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_market_permission_long_history_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'runtime':runtime,'fixed_evaluation_conditions':{'universe':'ordinary domestic stocks only: Prime, Standard, Growth','stock_selection':'安定上昇 top5 fixed','exit':'TP +1.5%, SL -3%, same-day both hit stop first, max H5','axis_changed':'ordinary-stock market permission only','permissions':list(PERMISSIONS),'development':'2019-2024','validation_selection':'2025','full_audit_test':'2026 through latest mature signal','entry':'next session open','minimum_active_dates':'40%','costs':'ignored','production_changed':False},'authoritative_result':{'candidates':rows,'eligible_without_2026':eligible,'chosen_without_2026':chosen,'test_2026':sm,'monthly_2026':monthly,'test_active_dates':active,'test_possible_dates':possible,'checks':checks},'observed_branching':{'changed_top5_members_count':5 if chosen else 0,'changed_top10_members_count':5 if chosen else 0,'changed_rank_count':sm['n'],'selection_divergence_reason':'ordinary-stock market permission; stock rank and exit fixed'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'long_history_strict_temporal_permission_gate'},'remaining_risks':['capital allocation and overlap gate pending only if event gates pass']}
 test.to_parquet(out/'test_2026_ledger.parquet',index=False);(out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'eligible_count':len(eligible),'chosen':chosen,'test':sm,'monthly':monthly,'active_dates':[active,possible],'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
