from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from tradex_gu_first_pullback_exit_geometry_latest_v1 import VARIANTS, load_signals, metrics, simulate


TOP_K = [3, 5, 10, 20]


def topk(frame: pd.DataFrame, k: int) -> pd.DataFrame:
    ordered = frame.sort_values(["signal_date", "quality_score", "code"], ascending=[True, False, True]).copy()
    ordered["daily_rank"] = ordered.groupby("signal_date").cumcount() + 1
    return ordered[ordered.daily_rank <= k].copy()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output); output.mkdir(parents=True, exist_ok=False)
    sys.path[:0] = [str(Path.cwd()), str(Path.cwd()/"app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime = get_runtime_stock_db_status()
    rows = load_signals(runtime["selected_runtime_db_path"])
    rows["signal_date"] = pd.to_datetime(rows.date, unit="s"); rows["year"] = rows.signal_date.dt.year
    rows["quality_score"] = (
        (rows.prior_gap7 / .03).clip(-1, 3)
        + (1 - (rows.c / rows.ma7 - 1).abs() / .03).clip(-3, 1)
        + ((rows.c / rows.ma20 - 1) / .03).clip(-3, 3)
    )

    exit_rows=[]
    for variant in VARIANTS:
        evaluated=simulate(rows,variant)
        exit_rows.append({"variant":variant,"development_2019_2023":metrics(evaluated[evaluated.year.between(2019,2023)]),
                          "exit_selection_2024":metrics(evaluated[evaluated.year.eq(2024)])})
    exit_selectable=[item for item in exit_rows if item["exit_selection_2024"]["n"]>=250
                     and (item["exit_selection_2024"]["mean_return_pct"] or -99)>0
                     and (item["development_2019_2023"]["mean_return_pct"] or -99)>0]
    selected_exit=max(exit_selectable,key=lambda x:x["exit_selection_2024"]["mean_return_pct"],default=None)
    evaluated=simulate(rows,selected_exit["variant"]) if selected_exit else rows.assign(realized_ret=float("nan"))

    rank_rows=[]
    for k in TOP_K:
        validation=topk(evaluated[evaluated.year.eq(2025)],k)
        rank_rows.append({"top_k":k,"rank_selection_2025":metrics(validation)})
    rank_selectable=[item for item in rank_rows if item["rank_selection_2025"]["n"]>=250
                     and (item["rank_selection_2025"]["mean_return_pct"] or -99)>0
                     and (item["rank_selection_2025"]["win_rate"] or 0)>=.50
                     and (item["rank_selection_2025"]["severe_loss5_rate"] or 1)<=.03]
    selected_rank=max(rank_selectable,key=lambda x:x["rank_selection_2025"]["mean_return_pct"],default=None)
    selected_k=selected_rank["top_k"] if selected_rank else None
    test=topk(evaluated[evaluated.year.eq(2026)],selected_k) if selected_k else evaluated.iloc[0:0].copy()
    test_metrics=metrics(test)
    monthly={str(month):metrics(group) for month,group in test.groupby(test.signal_date.dt.to_period("M"))}
    yearly={str(year):metrics(topk(evaluated[evaluated.year.eq(year)],selected_k)) for year in range(2019,2027)} if selected_k else {}
    positive_months=sum((item["mean_return_pct"] or -99)>0 for item in monthly.values())
    checks={"exit_selected_on_2024_only":selected_exit is not None,"rank_selected_on_2025_only":selected_rank is not None,
            "test_n_at_least_250_or_full_audit":test_metrics["n"]>=250,
            "test_mean_positive":(test_metrics["mean_return_pct"] or -99)>0,
            "test_win_rate_at_least_50pct":(test_metrics["win_rate"] or 0)>=.50,
            "test_severe_loss5_at_most_3pct":(test_metrics["severe_loss5_rate"] or 1)<=.03,
            "test_top3_profit_share_at_most_35pct":(test_metrics["top3_positive_profit_share"] or 1)<=.35,
            "test_months_majority_positive":bool(monthly) and positive_months/len(monthly)>=.70,
            "every_year_positive":bool(yearly) and all((item["mean_return_pct"] or -99)>0 for item in yearly.values())}
    decision="hold_for_portfolio_gate" if all(checks.values()) else "drop"
    payload={"schema_version":"tradex_gu_first_pullback_sequential_pit_v2.compare.v1","artifact_role":"authoritative",
             "generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,
             "fixed_evaluation_conditions":{"family":"fixed GU first-pullback transition","universe":"PAN ordinary stocks; ETF/ETN excluded",
                 "development":"2019-2023","exit_selection":2024,"rank_k_selection":2025,"untouched_test":"2026 through latest mature H10 signal",
                 "quality_score":"prior GU magnitude + MA7 proximity + distance above MA20; outcome-blind","entry":"next open","same_bar":"stop first",
                 "exit_variants":VARIANTS,"top_k_variants":TOP_K,"costs":"ignored","production_ranking_changed":False,"runtime_db_write":False},
             "authoritative_result":{"exit_variants":exit_rows,"selected_exit":selected_exit,"rank_variants":rank_rows,"selected_rank":selected_rank,
                 "test_2026":test_metrics,"monthly_2026":monthly,"yearly":yearly,"checks":checks},
             "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int(len(test)),
                 "selection_divergence_reason":"outcome-blind within-family daily quality rank"},
             "judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"sequential_point_in_time_gate"},
             "remaining_risks":["portfolio allocation pending if event gate passes"]}
    test.to_parquet(output/"test_signal_ledger.parquet",index=False)
    (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    (output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8")
    print(json.dumps({"selected_exit":selected_exit,"selected_rank":selected_rank,"test":test_metrics,"checks":checks,"decision":decision},ensure_ascii=False))


if __name__=="__main__": main()
