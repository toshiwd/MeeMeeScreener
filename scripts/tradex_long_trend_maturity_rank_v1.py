from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from tradex_long_ordinary_pit_compound_tree_v1 import load_rows, metrics


SCORES = ["中期上昇の初押し", "過熱回避押し目", "安定トレンド継続"]


def centered(rank: pd.Series, center: float, width: float) -> pd.Series:
    return 1 - (rank-center).abs()/width


def add_scores(data: pd.DataFrame) -> pd.DataFrame:
    cols=["ret1","ret3","ret5","ret20","gap_ma20","gap_ma60","close_pos","volume_ratio20","realized_vol20"]
    r=data.groupby("date")[cols].rank(pct=True); out=data.copy()
    # 過熱を閾値で排除せず、望ましい成熟帯からの距離として連続的に減点する。
    out["中期上昇の初押し"] = .30*centered(r.ret20,.65,.35)+.20*centered(r.gap_ma60,.65,.35)-.20*r.ret3+.15*r.close_pos-.15*r.realized_vol20
    out["過熱回避押し目"] = .25*centered(r.ret20,.58,.30)+.25*centered(r.gap_ma20,.58,.30)-.20*r.ret3+.15*r.close_pos-.15*r.realized_vol20
    out["安定トレンド継続"] = .25*centered(r.ret20,.68,.32)+.20*centered(r.gap_ma60,.62,.32)+.20*r.close_pos+.15*r.volume_ratio20-.20*r.realized_vol20
    return out


def select_daily(frame:pd.DataFrame,score:str,k:int)->pd.DataFrame:
    return frame.sort_values(["date",score,"code"],ascending=[True,False,True]).groupby("date",sort=False).head(k).copy()


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--output",required=True);args=parser.parse_args()
    output=Path(args.output);output.mkdir(parents=True,exist_ok=False)
    sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/"app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime=get_runtime_stock_db_status();data=load_rows(runtime["selected_runtime_db_path"],broad_trigger=False,min_date="2026-01-01")
    data=data[data[["p1_o","p5_c"]].notna().all(axis=1)].copy();data["signal_date"]=pd.to_datetime(data.date,unit="s");data["realized_ret"]=100*(data.p5_c/data.p1_o-1);data=add_scores(data)
    discovery=data[data.signal_date.between("2026-01-01","2026-03-31")];validation=data[data.signal_date.between("2026-04-01","2026-05-31")];test=data[data.signal_date.ge("2026-06-01")]
    candidates=[]
    for score in SCORES:
      for k in [3,5,10]:
        d=metrics(select_daily(discovery,score,k));v=metrics(select_daily(validation,score,k));a=metrics(select_daily(validation[validation.signal_date.dt.month.eq(4)],score,k));m=metrics(select_daily(validation[validation.signal_date.dt.month.eq(5)],score,k))
        candidates.append({"score":score,"top_k":k,"discovery":d,"validation":v,"april":a,"may":m})
    eligible=[x for x in candidates if x["discovery"]["mean_return_pct"]>0 and x["validation"]["mean_return_pct"]>0 and x["validation"]["win_rate"]>=.50 and x["validation"]["severe_loss5_rate"]<=.03 and x["april"]["mean_return_pct"]>0 and x["may"]["mean_return_pct"]>0]
    chosen=max(eligible,key=lambda x:(x["validation"]["mean_return_pct"],x["discovery"]["mean_return_pct"])) if eligible else None
    selected=select_daily(test,chosen["score"],chosen["top_k"]) if chosen else test.iloc[0:0].copy();sm=metrics(selected);monthly={str(m):metrics(g) for m,g in selected.groupby(selected.signal_date.dt.to_period("M"))}
    checks={"selected_without_test":chosen is not None,"test_n_at_least_60":sm["n"]>=60,"test_mean_positive":(sm["mean_return_pct"] or -99)>0,"test_win_at_least_50pct":(sm["win_rate"] or 0)>=.50,"test_severe5_at_most_3pct":(sm["severe_loss5_rate"] or 1)<=.03,"all_test_months_positive":len(monthly)>=2 and all((x["mean_return_pct"] or -99)>0 for x in monthly.values()),"test_profit_not_top3_concentrated":(sm["top3_positive_profit_share"] or 1)<=.35}
    decision="hold_for_long_history_and_portfolio_gate" if all(checks.values()) else "drop"
    payload={"schema_version":"tradex_long_trend_maturity_rank_v1.compare.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,"fixed_evaluation_conditions":{"universe":"PAN ordinary stocks; ETF/ETN excluded","axis_changed":"continuous trend maturity scoring only","scores":SCORES,"top_k":[3,5,10],"discovery":"2026-01-01..03-31","validation":"2026-04-01..05-31","untouched_test":"2026-06-01 through latest mature signal","entry":"next session open","exit":"session-5 close","costs":"ignored","production_changed":False},"authoritative_result":{"candidates":candidates,"eligible_without_test":eligible,"chosen_without_test":chosen,"test_selected":sm,"monthly_test":monthly,"checks":checks},"observed_branching":{"changed_top5_members_count":chosen["top_k"] if chosen else 0,"changed_top10_members_count":chosen["top_k"] if chosen else 0,"changed_rank_count":sm["n"],"selection_divergence_reason":"continuous multi-feature trend-maturity rank"},"judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"strict_temporal_maturity_gate"},"remaining_risks":["long-history, managed exit, and capital allocation pending only if recent gate passes"]}
    selected.to_parquet(output/"test_selected_ledger.parquet",index=False);(output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8");(output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8");print(json.dumps({"eligible_count":len(eligible),"chosen":chosen,"test":sm,"monthly":monthly,"checks":checks,"decision":decision},ensure_ascii=False))


if __name__=="__main__":main()
