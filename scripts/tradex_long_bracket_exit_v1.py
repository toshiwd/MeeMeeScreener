from __future__ import annotations
import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows,metrics
from tradex_long_cross_sectional_rank_v1 import add_scores,select_daily

TARGETS=[1.0,1.5,2.0,3.0]
def bracket(r,tp):
 e=float(r.p1_o);stop=e*.97;target=e*(1+tp/100)
 for n in range(1,6):
  o=float(r[f'p{n}_o']);h=float(r[f'p{n}_h']);l=float(r[f'p{n}_l'])
  if o<=stop:return 100*(o/e-1)
  if o>=target:return 100*(o/e-1)
  if l<=stop:return -3.0
  if h>=target:return tp
 return 100*(float(r.p5_c)/e-1)
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/'app')]
 from backend.services.codex_bridge_service import get_runtime_stock_db_status
 runtime=get_runtime_stock_db_status();d=load_rows(runtime['selected_runtime_db_path'],broad_trigger=False,min_date='2026-01-01');need=[f'p{x}_{y}' for x in range(1,6) for y in ['o','h','l','c']];d=d[d[need].notna().all(axis=1)].copy();d['signal_date']=pd.to_datetime(d.date,unit='s');d=add_scores(d)
 for tp in TARGETS:d[f'tp{tp}']=d.apply(lambda r:bracket(r,tp),axis=1)
 dev=d[d.signal_date.between('2026-01-01','2026-03-31')];val=d[d.signal_date.between('2026-04-01','2026-05-31')];test=d[d.signal_date.ge('2026-06-01')];rows=[]
 for tp in TARGETS:
  parts=[]
  for f in [dev,val]:
   z=select_daily(f,'安定上昇',5);z['realized_ret']=z[f'tp{tp}'];parts.append(metrics(z))
  rows.append({'take_profit_pct':tp,'discovery':parts[0],'validation':parts[1]})
 eligible=[x for x in rows if x['discovery']['mean_return_pct']>0 and x['discovery']['win_rate']>=.50 and x['discovery']['severe_loss5_rate']<=.03 and x['validation']['mean_return_pct']>0 and x['validation']['win_rate']>=.50 and x['validation']['severe_loss5_rate']<=.03]
 chosen=max(eligible,key=lambda x:(x['validation']['mean_return_pct'],x['discovery']['mean_return_pct'])) if eligible else None;sel=select_daily(test,'安定上昇',5) if chosen else test.iloc[0:0].copy()
 if chosen:sel['realized_ret']=sel[f"tp{chosen['take_profit_pct']}"]
 sm=metrics(sel);monthly={str(m):metrics(g) for m,g in sel.groupby(sel.signal_date.dt.to_period('M'))};pos=sum((x['mean_return_pct'] or -99)>0 for x in monthly.values());checks={'selected_without_test':chosen is not None,'test_n250_or_full_audit':sm['n']>=250 or (chosen is not None and sm['n']==5*test.date.nunique()),'test_mean_positive':(sm['mean_return_pct'] or -99)>0,'test_win_at_least_50pct':(sm['win_rate'] or 0)>=.50,'test_severe5_at_most_3pct':(sm['severe_loss5_rate'] or 1)<=.03,'test_months_majority_positive':bool(monthly) and pos>len(monthly)/2,'profit_not_concentrated':(sm['top3_positive_profit_share'] or 1)<=.35};decision='hold_for_long_history_and_portfolio_gate' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_bracket_exit_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'runtime':runtime,'fixed_evaluation_conditions':{'universe':'PAN ordinary stocks; ETF/ETN excluded','stock_selection':'安定上昇 top5 fixed','axis_changed':'take-profit target only','stop_loss_pct':3,'same_day_both_hit':'conservative stop first','targets_pct':TARGETS,'discovery':'2026-01-01..03-31','validation':'2026-04-01..05-31','untouched_test':'2026-06-01 through latest mature signal','entry':'next session open','maximum_holding':'5 sessions','production_changed':False},'authoritative_result':{'candidates':rows,'eligible_without_test':eligible,'chosen_without_test':chosen,'test_selected':sm,'monthly_test':monthly,'checks':checks},'observed_branching':{'changed_top5_members_count':5 if chosen else 0,'changed_top10_members_count':5 if chosen else 0,'changed_rank_count':sm['n'],'selection_divergence_reason':'stock selection fixed; bracket take-profit changed'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'strict_temporal_bracket_exit_gate'},'remaining_risks':['long-history and capital-allocation pending only if recent gate passes']}
 sel.to_parquet(out/'test_selected_ledger.parquet',index=False);(out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'eligible_count':len(eligible),'chosen':chosen,'test':sm,'monthly':monthly,'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
