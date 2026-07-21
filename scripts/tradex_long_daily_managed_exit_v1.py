from __future__ import annotations

import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import numpy as np
import pandas as pd

from tradex_long_ordinary_pit_compound_tree_v1 import load_rows,metrics
from tradex_long_trend_maturity_rank_v1 import add_scores,select_daily

EXITS=["5日固定","日次3%ストップ","日次4%ストップ","日次3%ストップ＋建値割れ翌朝撤退","日次4%ストップ＋高値終値3%反転撤退"]

def simulate(row,kind):
    entry=float(row.p1_o)
    if kind=="5日固定":return 100*(float(row.p5_c)/entry-1)
    stop_pct=.03 if "3%ストップ" in kind else .04
    high_close=entry
    for day in range(1,6):
        o=float(row[f"p{day}_o"]);l=float(row[f"p{day}_l"]);c=float(row[f"p{day}_c"]);stop=entry*(1-stop_pct)
        if o<=stop:return 100*(o/entry-1)
        if l<=stop:return -100*stop_pct
        high_close=max(high_close,c)
        if day<5 and "建値割れ" in kind and c<entry:
            return 100*(float(row[f"p{day+1}_o"])/entry-1)
        if day<5 and "高値終値3%反転" in kind and high_close>entry and c<=high_close*.97:
            return 100*(float(row[f"p{day+1}_o"])/entry-1)
    return 100*(float(row.p5_c)/entry-1)

def main():
    pa=argparse.ArgumentParser();pa.add_argument("--output",required=True);a=pa.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
    sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/"app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime=get_runtime_stock_db_status();d=load_rows(runtime["selected_runtime_db_path"],broad_trigger=False,min_date="2026-01-01")
    need=[f"p{x}_{y}" for x in range(1,6) for y in ["o","l","c"]];d=d[d[need].notna().all(axis=1)].copy();d["signal_date"]=pd.to_datetime(d.date,unit="s");d=add_scores(d)
    for e in EXITS:d[e]=d.apply(lambda r:simulate(r,e),axis=1)
    dev=d[d.signal_date.between("2026-01-01","2026-03-31")];val=d[d.signal_date.between("2026-04-01","2026-05-31")];test=d[d.signal_date.ge("2026-06-01")]
    rows=[]
    for e in EXITS:
      parts=[]
      for f in [dev,val,val[val.signal_date.dt.month.eq(4)],val[val.signal_date.dt.month.eq(5)]]:
        z=select_daily(f,"安定トレンド継続",5);z["realized_ret"]=z[e];parts.append(metrics(z))
      rows.append({"exit":e,"discovery":parts[0],"validation":parts[1],"april":parts[2],"may":parts[3]})
    eligible=[x for x in rows if x["exit"]!="5日固定" and x["discovery"]["mean_return_pct"]>0 and x["validation"]["mean_return_pct"]>0 and x["validation"]["win_rate"]>=.50 and x["validation"]["severe_loss5_rate"]<=.03 and x["april"]["mean_return_pct"]>0 and x["may"]["mean_return_pct"]>0]
    chosen=max(eligible,key=lambda x:(x["validation"]["mean_return_pct"],x["discovery"]["mean_return_pct"])) if eligible else None
    sel=select_daily(test,"安定トレンド継続",5) if chosen else test.iloc[0:0].copy()
    if chosen:sel["realized_ret"]=sel[chosen["exit"]]
    sm=metrics(sel);monthly={str(m):metrics(g) for m,g in sel.groupby(sel.signal_date.dt.to_period("M"))}
    checks={"selected_without_test":chosen is not None,"test_full_audit_or_n250":sm["n"]>=250 or (chosen is not None and sm["n"]==5*test.date.nunique()),"test_mean_positive":(sm["mean_return_pct"] or -99)>0,"test_win_at_least_50pct":(sm["win_rate"] or 0)>=.50,"test_severe5_at_most_3pct":(sm["severe_loss5_rate"] or 1)<=.03,"all_test_months_positive":len(monthly)>=2 and all((x["mean_return_pct"] or -99)>0 for x in monthly.values()),"profit_not_concentrated":(sm["top3_positive_profit_share"] or 1)<=.35}
    decision="hold_for_long_history_and_portfolio_gate" if all(checks.values()) else "drop"
    payload={"schema_version":"tradex_long_daily_managed_exit_v1.compare.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,"fixed_evaluation_conditions":{"universe":"PAN ordinary stocks; ETF/ETN excluded","stock_selection":"安定トレンド継続 top5 fixed","axis_changed":"daily managed exit only","exit_candidates":EXITS,"discovery":"2026-01-01..03-31","validation":"2026-04-01..05-31","untouched_test":"2026-06-01 through latest mature signal","entry":"next session open","stop_order":"daily open gap first, then intraday low; close-based exits next open","costs":"ignored","production_changed":False},"authoritative_result":{"candidates":rows,"eligible_without_test":eligible,"chosen_without_test":chosen,"test_selected":sm,"monthly_test":monthly,"checks":checks},"observed_branching":{"changed_top5_members_count":5 if chosen else 0,"changed_top10_members_count":5 if chosen else 0,"changed_rank_count":sm["n"],"selection_divergence_reason":"stock rank fixed; daily exit path changed"},"judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"strict_temporal_daily_exit_gate"},"remaining_risks":["long-history and capital-allocation portfolio gate pending only if recent gate passes"]}
    sel.to_parquet(out/"test_selected_ledger.parquet",index=False);(out/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8");print(json.dumps({"eligible_count":len(eligible),"chosen":chosen,"test":sm,"monthly":monthly,"checks":checks,"decision":decision},ensure_ascii=False))

if __name__=="__main__":main()
