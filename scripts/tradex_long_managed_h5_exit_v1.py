from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from tradex_long_cross_sectional_rank_v1 import add_scores, select_daily
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows, metrics


SCORES = ["押し目継続", "短期反転", "安定上昇"]
EXITS = ["5日固定", "初日3%ストップ", "初日陰転撤退", "初日1%逆行撤退", "3日建値割れ撤退", "初日ストップ＋陰転撤退"]


def add_exit_returns(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    entry = data.p1_o
    fixed = 100 * (data.p5_c / entry - 1)
    stop3 = data.p1_l <= entry * .97
    close_below = data.p1_c < entry
    close_below1 = data.p1_c <= entry * .99
    day3_below = data.p3_c < entry
    data["5日固定"] = fixed
    data["初日3%ストップ"] = np.where(stop3, -3.0, fixed)
    data["初日陰転撤退"] = np.where(close_below, 100*(data.p2_o/entry-1), fixed)
    data["初日1%逆行撤退"] = np.where(close_below1, 100*(data.p2_o/entry-1), fixed)
    data["3日建値割れ撤退"] = np.where(day3_below, 100*(data.p3_c/entry-1), fixed)
    data["初日ストップ＋陰転撤退"] = np.where(stop3, -3.0, np.where(close_below, 100*(data.p2_o/entry-1), fixed))
    return data


def score_metrics(frame: pd.DataFrame, exit_name: str) -> dict:
    work = frame.copy(); work["realized_ret"] = work[exit_name]
    return metrics(work)


def main() -> None:
    parser=argparse.ArgumentParser(); parser.add_argument("--output",required=True); args=parser.parse_args()
    output=Path(args.output); output.mkdir(parents=True,exist_ok=False)
    sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/"app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime=get_runtime_stock_db_status()
    data=load_rows(runtime["selected_runtime_db_path"],broad_trigger=False,min_date="2026-01-01")
    data=data[data[["p1_o","p1_l","p1_c","p2_o","p3_c","p5_c"]].notna().all(axis=1)].copy()
    data["signal_date"]=pd.to_datetime(data.date,unit="s"); data=add_exit_returns(add_scores(data))
    discovery=data[data.signal_date.between("2026-01-01","2026-03-31")]
    validation=data[data.signal_date.between("2026-04-01","2026-05-31")]
    test=data[data.signal_date.ge("2026-06-01")]
    candidates=[]
    for score in SCORES:
        for k in [3,5,10]:
            dsel=select_daily(discovery,score,k); vsel=select_daily(validation,score,k)
            asel=select_daily(validation[validation.signal_date.dt.month.eq(4)],score,k)
            msel=select_daily(validation[validation.signal_date.dt.month.eq(5)],score,k)
            for exit_name in EXITS:
                d=score_metrics(dsel,exit_name); v=score_metrics(vsel,exit_name); april=score_metrics(asel,exit_name); may=score_metrics(msel,exit_name)
                candidates.append({"score":score,"top_k":k,"exit":exit_name,"discovery":d,"validation":v,"april":april,"may":may})
    eligible=[x for x in candidates if x["discovery"]["mean_return_pct"]>0 and x["validation"]["mean_return_pct"]>0 and x["validation"]["win_rate"]>=.50 and x["validation"]["severe_loss5_rate"]<=.03 and x["april"]["mean_return_pct"]>0 and x["may"]["mean_return_pct"]>0]
    chosen=max(eligible,key=lambda x:(x["validation"]["mean_return_pct"],x["discovery"]["mean_return_pct"])) if eligible else None
    selected=select_daily(test,chosen["score"],chosen["top_k"]) if chosen else test.iloc[0:0].copy()
    if chosen: selected["realized_ret"]=selected[chosen["exit"]]
    sm=metrics(selected); monthly={str(m):metrics(g) for m,g in selected.groupby(selected.signal_date.dt.to_period("M"))}
    checks={"selected_without_test":chosen is not None,"test_n_at_least_60":sm["n"]>=60,"test_mean_positive":(sm["mean_return_pct"] or -99)>0,"test_win_at_least_50pct":(sm["win_rate"] or 0)>=.50,"test_severe5_at_most_3pct":(sm["severe_loss5_rate"] or 1)<=.03,"all_test_months_positive":len(monthly)>=2 and all((x["mean_return_pct"] or -99)>0 for x in monthly.values()),"test_profit_not_top3_concentrated":(sm["top3_positive_profit_share"] or 1)<=.35}
    decision="hold_for_long_history_and_portfolio_gate" if all(checks.values()) else "drop"
    payload={"schema_version":"tradex_long_managed_h5_exit_v1.compare.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,"fixed_evaluation_conditions":{"universe":"PAN ordinary stocks; ETF/ETN excluded","selection_scores":SCORES,"top_k":[3,5,10],"axis_changed":"exit management only versus fixed H5","exit_candidates":EXITS,"discovery":"2026-01-01..03-31","validation":"2026-04-01..05-31","untouched_test":"2026-06-01 through latest mature signal","entry":"next session open","costs":"ignored","production_changed":False},"authoritative_result":{"candidates":candidates,"eligible_without_test":eligible,"chosen_without_test":chosen,"test_selected":sm,"monthly_test":monthly,"checks":checks},"observed_branching":{"changed_top5_members_count":chosen["top_k"] if chosen else 0,"changed_top10_members_count":chosen["top_k"] if chosen else 0,"changed_rank_count":sm["n"],"selection_divergence_reason":"same cross-sectional selection; managed H5 exit only"},"judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"strict_temporal_managed_exit_gate"},"remaining_risks":["long-history and capital-allocation portfolio gate pending only if recent gate passes"]}
    selected.to_parquet(output/"test_selected_ledger.parquet",index=False); (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8"); (output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8")
    print(json.dumps({"eligible_count":len(eligible),"chosen":chosen,"test":sm,"monthly":monthly,"checks":checks,"decision":decision},ensure_ascii=False))


if __name__=="__main__": main()
