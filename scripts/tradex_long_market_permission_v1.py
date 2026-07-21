from __future__ import annotations

import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd

from tradex_long_ordinary_pit_compound_tree_v1 import load_rows,metrics
from tradex_long_trend_maturity_rank_v1 import add_scores,select_daily

PERMISSIONS={
 "市場許可なし":lambda d: pd.Series(True,index=d.index),
 "市場平均上昇":lambda d:d.market_mean_ret1>0,
 "値上がり過半":lambda d:d.market_advancers_ratio>.50,
 "20MA上過半":lambda d:d.market_breadth_ma20>.50,
 "市場平均上昇かつ20MA上過半":lambda d:(d.market_mean_ret1>0)&(d.market_breadth_ma20>.50),
}

def pick(frame,permission):
    allowed=frame[PERMISSIONS[permission](frame)]
    return select_daily(allowed,"安定トレンド継続",5)

def main():
    pa=argparse.ArgumentParser();pa.add_argument("--output",required=True);a=pa.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
    sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/"app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime=get_runtime_stock_db_status();d=load_rows(runtime["selected_runtime_db_path"],broad_trigger=False,min_date="2026-01-01")
    d=d[d[["p1_o","p5_c"]].notna().all(axis=1)].copy();d["signal_date"]=pd.to_datetime(d.date,unit="s");d["realized_ret"]=100*(d.p5_c/d.p1_o-1);d=add_scores(d)
    dev=d[d.signal_date.between("2026-01-01","2026-03-31")];val=d[d.signal_date.between("2026-04-01","2026-05-31")];test=d[d.signal_date.ge("2026-06-01")]
    rows=[]
    for p in PERMISSIONS:
      ds=pick(dev,p);vs=pick(val,p);aps=pick(val[val.signal_date.dt.month.eq(4)],p);mps=pick(val[val.signal_date.dt.month.eq(5)],p)
      rows.append({"permission":p,"discovery":metrics(ds),"validation":metrics(vs),"april":metrics(aps),"may":metrics(mps),"validation_active_dates":int(vs.date.nunique()),"validation_possible_dates":int(val.date.nunique())})
    eligible=[x for x in rows if x["permission"]!="市場許可なし" and x["discovery"]["mean_return_pct"]>0 and x["validation"]["mean_return_pct"]>0 and x["validation"]["win_rate"]>=.50 and x["validation"]["severe_loss5_rate"]<=.03 and x["april"]["mean_return_pct"]>0 and x["may"]["mean_return_pct"]>0 and x["validation_active_dates"]>=.40*x["validation_possible_dates"]]
    chosen=max(eligible,key=lambda x:(x["validation"]["mean_return_pct"],x["validation_active_dates"])) if eligible else None
    sel=pick(test,chosen["permission"]) if chosen else test.iloc[0:0].copy();sm=metrics(sel);monthly={str(m):metrics(g) for m,g in sel.groupby(sel.signal_date.dt.to_period("M"))}
    active=int(sel.date.nunique());possible=int(test.date.nunique());checks={"selected_without_test":chosen is not None,"test_full_audit_or_n250":sm["n"]>=250 or (chosen is not None and active==sum(PERMISSIONS[chosen["permission"]](test).groupby(test.date).any())),"test_mean_positive":(sm["mean_return_pct"] or -99)>0,"test_win_at_least_50pct":(sm["win_rate"] or 0)>=.50,"test_severe5_at_most_3pct":(sm["severe_loss5_rate"] or 1)<=.03,"all_test_months_positive":len(monthly)>=2 and all((x["mean_return_pct"] or -99)>0 for x in monthly.values()),"profit_not_concentrated":(sm["top3_positive_profit_share"] or 1)<=.35,"active_dates_at_least_40pct":possible>0 and active/possible>=.40}
    decision="hold_for_long_history_and_portfolio_gate" if all(checks.values()) else "drop"
    payload={"schema_version":"tradex_long_market_permission_v1.compare.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,"fixed_evaluation_conditions":{"universe":"PAN ordinary stocks; ETF/ETN excluded","stock_selection":"安定トレンド継続 top5 fixed","axis_changed":"same-close market permission only","permissions":list(PERMISSIONS),"minimum_active_dates":"40%","discovery":"2026-01-01..03-31","validation":"2026-04-01..05-31","untouched_test":"2026-06-01 through latest mature signal","entry":"next session open","exit":"session-5 close","production_changed":False},"authoritative_result":{"candidates":rows,"eligible_without_test":eligible,"chosen_without_test":chosen,"test_selected":sm,"monthly_test":monthly,"test_active_dates":active,"test_possible_dates":possible,"checks":checks},"observed_branching":{"changed_top5_members_count":5 if chosen else 0,"changed_top10_members_count":5 if chosen else 0,"changed_rank_count":sm["n"],"selection_divergence_reason":"market permission gates new entries; stock rank fixed"},"judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"strict_temporal_market_permission_gate"},"remaining_risks":["long-history and capital-allocation portfolio gate pending only if recent gate passes"]}
    sel.to_parquet(out/"test_selected_ledger.parquet",index=False);(out/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8");print(json.dumps({"eligible_count":len(eligible),"chosen":chosen,"test":sm,"monthly":monthly,"active_dates":[active,possible],"checks":checks,"decision":decision},ensure_ascii=False))

if __name__=="__main__":main()
