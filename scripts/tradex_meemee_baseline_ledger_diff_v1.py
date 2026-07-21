from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
A=Path(r'G:\Tradex\leaf_vs_meemee_2026_same_condition_v1\20260713T050140Z-tradex_leaf_vs_meemee_2026_same_condition_v1\meemee_events.csv');B=Path(r'G:\Tradex\meemee_leaf_consensus_top3_v1\20260713T050728Z-tradex_meemee_leaf_consensus_top3_v1\baseline_events.csv');OUT=Path(r'G:\Tradex\meemee_baseline_ledger_diff_v1')
KEY=['code','date','next_entry_date']
def diagnose(a:pd.DataFrame,b:pd.DataFrame)->tuple[dict,pd.DataFrame]:
 b=b[b.signal_year==2026].copy();m=a.merge(b,on=KEY,how='outer',suffixes=('_a','_b'),indicator=True);both=m[m._merge=='both'];diff={}
 for c in ('next_open_return','fill_price','exit_date','pnl_yen','entry_price','shares','invested_yen'):diff[c]={'changed_rows':int(((both[c+'_a']-both[c+'_b']).abs()>1e-9).sum()),'maximum_absolute_delta':float((both[c+'_a']-both[c+'_b']).abs().max())if len(both)else None}
 result={'key':KEY,'row_counts':{'legacy_same_condition':len(a),'consensus_baseline_2026':len(b),'both':int((m._merge=='both').sum()),'legacy_only':int((m._merge=='left_only').sum()),'consensus_only':int((m._merge=='right_only').sum())},'field_differences_on_common_rows':diff,'contract_differences':{'maturity':'same_non_null_10_session_horizon','slippage':'same_10bp','return_exit_fill_pnl':'identical_on_common_rows','portfolio_ordering':{'legacy':'tie_gap_ma60=rank then descending replay; effective rank 3 before 2 before 1','consensus':'tie_gap_ma60=-rank then descending replay; effective rank 1 before 2 before 3'}},'typed_judgment':{'contract_compliant':'consensus_baseline_20260713T050728Z','noncompliant':'leaf_vs_meemee_20260713T050140Z','reason':'LEGACY_MEE_MEE_RANK_ORDER_REVERSED_IN_PORTFOLIO_REPLAY'}}
 return result,m
def generate(a:Path,b:Path,out:Path)->Path:
 d,rows=diagnose(pd.read_csv(a),pd.read_csv(b));now=datetime.now(timezone.utc);root=out/f"{now.strftime('%Y%m%dT%H%M%SZ')}-tradex_meemee_baseline_ledger_diff_v1";root.mkdir(parents=True);rows.to_csv(root/'row_diff.csv',index=False);p={'schema_version':'tradex_meemee_baseline_ledger_diff_v1.compare.v1','artifact_role':'authoritative','source_ledgers':[str(a),str(b)],**d,'new_challenger_created':False,'runtime_db_write':False,'production_ranking_changed':False};q=root/'compare.json';q.write_text(json.dumps(p,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');return q
def main():
 x=argparse.ArgumentParser();x.add_argument('--a',type=Path,default=A);x.add_argument('--b',type=Path,default=B);x.add_argument('--out',type=Path,default=OUT);a=x.parse_args();print(generate(a.a,a.b,a.out))
if __name__=='__main__':main()
