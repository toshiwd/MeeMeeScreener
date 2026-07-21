from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from tradex_long_ordinary_pit_compound_tree_v1 import load_rows, metrics


def add_scores(data: pd.DataFrame) -> pd.DataFrame:
    rank_features = ["ret1", "ret3", "ret5", "ret20", "gap_ma20", "gap_ma60", "close_pos", "volume_ratio20", "realized_vol20"]
    ranks = data.groupby("date")[rank_features].rank(pct=True)
    data = data.copy()
    # 複数軸を連続値で合成し、個別条件による一律除外は行わない。
    data["押し目継続"] = .35*ranks.ret20 + .25*ranks.gap_ma60 - .25*ranks.ret3 + .15*ranks.close_pos
    data["短期反転"] = -.35*ranks.ret1 - .20*ranks.ret3 + .20*ranks.ret20 + .15*ranks.close_pos - .10*ranks.realized_vol20
    data["安定上昇"] = .30*ranks.ret20 + .20*ranks.gap_ma20 + .20*ranks.close_pos + .15*ranks.volume_ratio20 - .15*ranks.realized_vol20
    return data


def select_daily(frame: pd.DataFrame, score: str, k: int) -> pd.DataFrame:
    return frame.sort_values(["date", score, "code"], ascending=[True, False, True]).groupby("date", sort=False).head(k).copy()


def period_metrics(frame: pd.DataFrame) -> dict:
    return metrics(frame)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output", required=True); parser.add_argument("--holding", choices=["nextday", "h5"], default="nextday"); args = parser.parse_args()
    output = Path(args.output); output.mkdir(parents=True, exist_ok=False)
    sys.path[:0] = [str(Path.cwd()), str(Path.cwd()/"app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime = get_runtime_stock_db_status()
    data = load_rows(runtime["selected_runtime_db_path"], broad_trigger=False, min_date="2026-01-01")
    required = "p1_c" if args.holding == "nextday" else "p5_c"
    data = data[data[required].notna()].copy(); data["signal_date"] = pd.to_datetime(data.date, unit="s")
    exit_price = data.p1_c if args.holding == "nextday" else data.p5_c
    data["realized_ret"] = 100*(exit_price/data.p1_o-1); data = add_scores(data)
    discovery = data[data.signal_date.between("2026-01-01", "2026-03-31")]
    validation = data[data.signal_date.between("2026-04-01", "2026-05-31")]
    test = data[data.signal_date.ge("2026-06-01")]
    candidates=[]
    for score in ["押し目継続", "短期反転", "安定上昇"]:
        for k in [3, 5, 10]:
            d=period_metrics(select_daily(discovery, score, k)); v=period_metrics(select_daily(validation, score, k))
            april=period_metrics(select_daily(validation[validation.signal_date.dt.month.eq(4)], score, k))
            may=period_metrics(select_daily(validation[validation.signal_date.dt.month.eq(5)], score, k))
            candidates.append({"score":score,"top_k":k,"discovery":d,"validation":v,"april":april,"may":may})
    eligible=[x for x in candidates if x["discovery"]["mean_return_pct"]>0 and x["validation"]["mean_return_pct"]>0 and x["validation"]["win_rate"]>=.50 and x["validation"]["severe_loss5_rate"]<=.03 and x["april"]["mean_return_pct"]>0 and x["may"]["mean_return_pct"]>0]
    chosen=max(eligible, key=lambda x:(x["validation"]["mean_return_pct"],x["discovery"]["mean_return_pct"])) if eligible else None
    selected=select_daily(test, chosen["score"], chosen["top_k"]) if chosen else test.iloc[0:0].copy()
    sm=metrics(selected); bm=metrics(test)
    monthly={str(m):metrics(g) for m,g in selected.groupby(selected.signal_date.dt.to_period("M"))}
    checks={"selected_without_test":chosen is not None,"test_n_at_least_60":sm["n"]>=60,"test_mean_positive":(sm["mean_return_pct"] or -99)>0,"test_win_at_least_50pct":(sm["win_rate"] or 0)>=.50,"test_severe5_at_most_3pct":(sm["severe_loss5_rate"] or 1)<=.03,"both_test_months_positive":len(monthly)>=2 and all((x["mean_return_pct"] or -99)>0 for x in monthly.values()),"beats_all_stock_mean":sm["mean_return_pct"] is not None and sm["mean_return_pct"]>bm["mean_return_pct"],"beats_all_stock_win":sm["win_rate"] is not None and sm["win_rate"]>bm["win_rate"]}
    decision="hold_for_long_history_and_portfolio_gate" if all(checks.values()) else "drop"
    payload={"schema_version":"tradex_long_cross_sectional_rank_v1.compare.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,"fixed_evaluation_conditions":{"universe":"PAN ordinary stocks; ETF/ETN excluded","discovery":"2026-01-01..03-31","validation":"2026-04-01..05-31","untouched_test":"2026-06-01 through latest mature signal","execution":"next open to same-day close" if args.holding=="nextday" else "next open to session-5 close","holding":args.holding,"scores":["押し目継続","短期反転","安定上昇"],"top_k":[3,5,10],"costs":"ignored","production_changed":False},"authoritative_result":{"candidates":candidates,"chosen_without_test":chosen,"test_selected":sm,"test_baseline_all_ordinary":bm,"monthly_test":monthly,"checks":checks},"observed_branching":{"changed_top5_members_count":chosen["top_k"] if chosen else 0,"changed_top10_members_count":chosen["top_k"] if chosen else 0,"changed_rank_count":sm["n"],"selection_divergence_reason":"daily cross-sectional compound score rank"},"judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"strict_temporal_cross_sectional_gate"},"remaining_risks":["long-history and overlapping portfolio simulation pending only if recent test passes"]}
    selected.to_parquet(output/"test_selected_ledger.parquet",index=False); (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8"); (output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8")
    print(json.dumps({"chosen":chosen,"test":sm,"baseline":bm,"monthly":monthly,"checks":checks,"decision":decision},ensure_ascii=False))


if __name__ == "__main__": main()
