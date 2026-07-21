from __future__ import annotations

import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd

from tradex_long_cross_sectional_rank_v1 import add_scores as add_base_scores,select_daily
from tradex_long_trend_maturity_rank_v1 import add_scores as add_maturity_scores
from tradex_long_daily_managed_exit_v1 import simulate
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows,metrics

SCORES=["押し目継続","短期反転","安定上昇","中期上昇の初押し","過熱回避押し目","安定トレンド継続"]

def main():
 pa=argparse.ArgumentParser();pa.add_argument("--output",required=True);a=pa.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/"app")]
 from backend.services.codex_bridge_service import get_runtime_stock_db_status
 runtime=get_runtime_stock_db_status();d=load_rows(runtime["selected_runtime_db_path"],broad_trigger=False,min_date="2026-01-01")
 need=[f"p{x}_{y}" for x in range(1,6) for y in ["o","l","c"]];d=d[d[need].notna().all(axis=1)].copy();d["signal_date"]=pd.to_datetime(d.date,unit="s");d=add_maturity_scores(add_base_scores(d));d["realized_ret"]=d.apply(lambda r:simulate(r,"日次3%ストップ"),axis=1)
 dev=d[d.signal_date.between("2026-01-01","2026-03-31")];val=d[d.signal_date.between("2026-04-01","2026-05-31")];test=d[d.signal_date.ge("2026-06-01")]
 rows=[]
 for s in SCORES:
  for k in [3,5,10]:
   ds=select_daily(dev,s,k);vs=select_daily(val,s,k);dm=metrics(ds);vm=metrics(vs)
   rows.append({"score":s,"top_k":k,"discovery":dm,"validation":vm,"validation_monthly":{str(m):metrics(g) for m,g in vs.groupby(vs.signal_date.dt.to_period("M"))}})
 eligible=[x for x in rows if x["discovery"]["n"]>=150 and x["discovery"]["mean_return_pct"]>0 and x["discovery"]["win_rate"]>=.50 and x["discovery"]["severe_loss5_rate"]<=.03 and x["validation"]["n"]>=100 and x["validation"]["mean_return_pct"]>0 and x["validation"]["win_rate"]>=.50 and x["validation"]["severe_loss5_rate"]<=.03 and x["validation"]["top3_positive_profit_share"]<=.35]
 chosen=max(eligible,key=lambda x:(x["validation"]["mean_return_pct"],x["discovery"]["mean_return_pct"])) if eligible else None
 sel=select_daily(test,chosen["score"],chosen["top_k"]) if chosen else test.iloc[0:0].copy();sm=metrics(sel);monthly={str(m):metrics(g) for m,g in sel.groupby(sel.signal_date.dt.to_period("M"))};positive=sum((x["mean_return_pct"] or -99)>0 for x in monthly.values())
 checks={"selected_without_test":chosen is not None,"test_n250_or_full_audit":sm["n"]>=250 or (chosen is not None and sm["n"]==chosen["top_k"]*test.date.nunique()),"test_mean_positive":(sm["mean_return_pct"] or -99)>0,"test_win_at_least_50pct":(sm["win_rate"] or 0)>=.50,"test_severe5_at_most_3pct":(sm["severe_loss5_rate"] or 1)<=.03,"test_months_majority_positive":bool(monthly) and positive>len(monthly)/2,"profit_not_concentrated":(sm["top3_positive_profit_share"] or 1)<=.35}
 decision="hold_for_long_history_and_portfolio_gate" if all(checks.values()) else "drop"
 payload={"schema_version":"tradex_long_rank_with_daily_stop_v1.compare.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,"fixed_evaluation_conditions":{"universe":"PAN ordinary stocks; ETF/ETN excluded","axis_changed":"compound rank family only","scores":SCORES,"top_k":[3,5,10],"exit":"daily 3% stop fixed, gap at open honored","discovery":"2026-01-01..03-31","validation":"2026-04-01..05-31 aggregate selection; monthly recorded but not used as a two-month veto","untouched_test":"2026-06-01 through latest mature signal","entry":"next session open","costs":"ignored","production_changed":False},"authoritative_result":{"candidates":rows,"eligible_without_test":eligible,"chosen_without_test":chosen,"test_selected":sm,"monthly_test":monthly,"checks":checks},"observed_branching":{"changed_top5_members_count":chosen["top_k"] if chosen else 0,"changed_top10_members_count":chosen["top_k"] if chosen else 0,"changed_rank_count":sm["n"],"selection_divergence_reason":"compound rank family under fixed daily stop"},"judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"strict_temporal_rank_under_fixed_exit_gate"},"remaining_risks":["long-history and capital-allocation portfolio gate pending only if recent gate passes"]}
 sel.to_parquet(out/"test_selected_ledger.parquet",index=False);(out/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8");print(json.dumps({"eligible_count":len(eligible),"chosen":chosen,"test":sm,"monthly":monthly,"checks":checks,"decision":decision},ensure_ascii=False))

if __name__=="__main__":main()
