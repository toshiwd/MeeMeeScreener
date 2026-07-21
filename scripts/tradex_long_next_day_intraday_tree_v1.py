from __future__ import annotations

import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeRegressor,_tree
from tradex_long_ordinary_pit_compound_tree_v1 import FEATURES,dedupe,load_rows,metrics

def paths(tree):
 t=tree.tree_;out={}
 def walk(n,c):
  if t.feature[n]==_tree.TREE_UNDEFINED:out[int(n)]=c;return
  f=FEATURES[t.feature[n]];v=float(t.threshold[n]);walk(t.children_left[n],c+[f"{f} <= {v:.10g}"]);walk(t.children_right[n],c+[f"{f} > {v:.10g}"])
 walk(0,[]);return out

def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args();root=Path(a.output);root.mkdir(parents=True,exist_ok=False)
 sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/'app')];from backend.services.codex_bridge_service import get_runtime_stock_db_status
 rt=get_runtime_stock_db_status();d=load_rows(rt['selected_runtime_db_path']);d['signal_date']=pd.to_datetime(d.date,unit='s');d['year']=d.signal_date.dt.year;d['realized_ret']=100*(d.p1_c/d.p1_o-1)
 train=d.year.between(2019,2024);imp=SimpleImputer(strategy='median');x=imp.fit_transform(d.loc[train,FEATURES]);tree=DecisionTreeRegressor(max_depth=5,min_samples_leaf=5000,random_state=20260720);tree.fit(x,d.loc[train,'realized_ret']);d['leaf']=tree.apply(imp.transform(d[FEATURES])).astype(int);rules=paths(tree)
 dev=[];val=[]
 for leaf,g in d.groupby('leaf'):
  rule=rules[int(leaf)];dev.append({'leaf':int(leaf),'rule':rule,**metrics(dedupe(g[g.year.between(2019,2024)]))});val.append({'leaf':int(leaf),'rule':rule,**metrics(dedupe(g[g.year.eq(2025)]))})
 eligible=[]
 for v in val:
  q=next(x for x in dev if x['leaf']==v['leaf']);distinct=len({z.split()[0] for z in v['rule']});years=[metrics(dedupe(d[(d.year.eq(y))&(d.leaf.eq(v['leaf']))])) for y in range(2019,2025)]
  if distinct>=2 and v['n']>=250 and (v['mean_return_pct'] or -99)>.10 and (v['win_rate'] or 0)>=.52 and (v['severe_loss5_rate'] or 1)<=.03 and (q['mean_return_pct'] or -99)>0 and sum((z['mean_return_pct'] or -99)>0 for z in years)>=5:eligible.append(v['leaf'])
 test=dedupe(d[(d.year.eq(2026))&d.leaf.isin(eligible)]);tm=metrics(test);monthly={str(m):metrics(g) for m,g in test.groupby(test.signal_date.dt.to_period('M'))};yearly={str(y):metrics(dedupe(d[(d.year.eq(y))&d.leaf.isin(eligible)])) for y in range(2019,2027)}
 pm=sum((x['mean_return_pct'] or -99)>0 for x in monthly.values());checks={'validation_selected_without_2026':bool(eligible),'test_n_at_least_250':tm['n']>=250,'test_mean_positive':(tm['mean_return_pct'] or -99)>0,'test_win_rate_at_least_50pct':(tm['win_rate'] or 0)>=.50,'test_severe_loss5_at_most_3pct':(tm['severe_loss5_rate'] or 1)<=.03,'test_top3_profit_share_at_most_35pct':(tm['top3_positive_profit_share'] or 1)<=.35,'test_months_majority_positive':bool(monthly) and pm/len(monthly)>=.70,'every_year_positive':all((x['mean_return_pct'] or -99)>0 for x in yearly.values())}
 decision='hold_for_portfolio_gate' if all(checks.values()) else 'drop';payload={'schema_version':'tradex_long_next_day_intraday_tree_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'runtime':rt,'fixed_evaluation_conditions':{'universe':'PAN ordinary stocks; ETF/ETN excluded','broad_trigger':'ret3 3%-20%, close>MA20, range20>3%','entry':'next open','exit':'same session close','discovery':'2019-2024','validation_selection':2025,'untouched_test':'2026 through 2026-07-17','features':FEATURES,'tree':{'max_depth':5,'min_samples_leaf':5000,'random_state':20260720},'compound_gate':'two or more distinct features','costs':'ignored','production_ranking_changed':False,'runtime_db_write':False},'authoritative_result':{'eligible_leaves':eligible,'development_leaves':dev,'validation_leaves':val,'test_2026':tm,'monthly_2026':monthly,'yearly':yearly,'checks':checks},'observed_branching':{'changed_top5_members_count':None,'changed_top10_members_count':None,'changed_rank_count':int(len(test)),'selection_divergence_reason':'train-only compound pre-entry chart-state leaves'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'next_day_initial_move_gate'},'remaining_risks':['portfolio allocation pending if event gate passes']}
 test.to_parquet(root/'test_signal_ledger.parquet',index=False);(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(root/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'eligible':eligible,'test':tm,'checks':checks,'decision':decision},ensure_ascii=False))
if __name__=='__main__':main()
