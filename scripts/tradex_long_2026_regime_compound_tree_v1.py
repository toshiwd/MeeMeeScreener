from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeRegressor, _tree

from tradex_long_ordinary_pit_compound_tree_v1 import FEATURES, dedupe, load_rows, metrics


def paths(tree: DecisionTreeRegressor, feature_names: list[str]) -> dict[int, list[str]]:
    t = tree.tree_; result: dict[int, list[str]] = {}
    def walk(node: int, clauses: list[str]) -> None:
        if t.feature[node] == _tree.TREE_UNDEFINED:
            result[int(node)] = clauses; return
        feature = feature_names[t.feature[node]]; threshold = float(t.threshold[node])
        walk(t.children_left[node], clauses + [f"{feature} <= {threshold:.10g}"])
        walk(t.children_right[node], clauses + [f"{feature} > {threshold:.10g}"])
    walk(0, []); return result


def main() -> None:
    parser=argparse.ArgumentParser();parser.add_argument("--output",required=True);parser.add_argument("--target",choices=["h10","nextday"],default="h10");parser.add_argument("--universe",choices=["momentum","all"],default="momentum");parser.add_argument("--exclude-market-breadth",action="store_true");args=parser.parse_args()
    output=Path(args.output);output.mkdir(parents=True,exist_ok=False)
    sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/"app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime=get_runtime_stock_db_status();data=load_rows(runtime["selected_runtime_db_path"],broad_trigger=args.universe=="momentum",min_date="2026-01-01" if args.universe=="all" else None)
    required="p10_c" if args.target=="h10" else "p1_c";data=data[data[required].notna()].copy();data["signal_date"]=pd.to_datetime(data.date,unit="s")
    entry=data.p1_o;immediate=data.p1_h.div(entry).sub(1).mul(100).ge(3)&data.p1_l.div(entry).sub(1).mul(100).gt(-3)
    stall=(data.p1_c.div(entry).sub(1).mul(100).le(-2)|data.p1_l.div(entry).sub(1).mul(100).le(-3))&~immediate
    if args.target=="nextday":
        data["realized_ret"]=100*(data.p1_c/entry-1)
    else:
        data["realized_ret"]=25*(data.p10_c/entry-1)
        data.loc[immediate,"realized_ret"]=100*(.25*(data.loc[immediate,"p10_c"]/entry.loc[immediate]-1)+.75*(data.loc[immediate,"p10_c"]/data.loc[immediate,"p2_o"]-1))
        data.loc[stall,"realized_ret"]=25*(data.loc[stall,"p2_o"]/entry.loc[stall]-1)
    discovery=data.signal_date.between("2026-01-01","2026-03-31")
    validation=data.signal_date.between("2026-04-01","2026-05-31")
    test=data.signal_date.ge("2026-06-01")
    feature_names=[x for x in FEATURES if x!="market_breadth_ma20"] if args.exclude_market_breadth else FEATURES
    imputer=SimpleImputer(strategy="median");x=imputer.fit_transform(data.loc[discovery,feature_names])
    tree=DecisionTreeRegressor(max_depth=4,min_samples_leaf=200,random_state=20260720);tree.fit(x,data.loc[discovery,"realized_ret"])
    data["leaf"]=tree.apply(imputer.transform(data[feature_names])).astype(int);rules=paths(tree,feature_names)
    leaves=[];eligible=[]
    for leaf,frame in data.groupby("leaf"):
        d=metrics(dedupe(frame[discovery.loc[frame.index]]));v=metrics(dedupe(frame[validation.loc[frame.index]]))
        april=metrics(dedupe(frame[frame.signal_date.between("2026-04-01","2026-04-30")]))
        may=metrics(dedupe(frame[frame.signal_date.between("2026-05-01","2026-05-31")]))
        rule=rules[int(leaf)];distinct=len({clause.split()[0] for clause in rule})
        row={"leaf":int(leaf),"rule":rule,"distinct_features":distinct,"discovery":d,"validation":v,"april":april,"may":may};leaves.append(row)
        if (distinct>=2 and d["n"]>=200 and (d["mean_return_pct"] or -99)>0 and v["n"]>=100
                and (v["mean_return_pct"] or -99)>0 and (v["win_rate"] or 0)>=.50 and (v["severe_loss5_rate"] or 1)<=.03
                and (april["mean_return_pct"] or -99)>0 and (may["mean_return_pct"] or -99)>0):eligible.append(int(leaf))
    selected=dedupe(data[test & data.leaf.isin(eligible)]);baseline=dedupe(data[test])
    sm=metrics(selected);bm=metrics(baseline)
    monthly={str(month):metrics(group) for month,group in selected.groupby(selected.signal_date.dt.to_period("M"))}
    positive_months=sum((x["mean_return_pct"] or -99)>0 for x in monthly.values())
    checks={"validation_selected_without_test":bool(eligible),"test_n_at_least_250_or_full_audit":sm["n"]>=250,
            "test_mean_positive":(sm["mean_return_pct"] or -99)>0,"test_win_rate_at_least_50pct":(sm["win_rate"] or 0)>=.50,
            "test_severe_loss5_at_most_3pct":(sm["severe_loss5_rate"] or 1)<=.03,"test_top3_profit_share_at_most_35pct":(sm["top3_positive_profit_share"] or 1)<=.35,
            "test_months_majority_positive":bool(monthly) and positive_months/len(monthly)>=.70,
            "beats_same_condition_baseline_mean":sm["mean_return_pct"] is not None and bm["mean_return_pct"] is not None and sm["mean_return_pct"]>bm["mean_return_pct"],
            "beats_same_condition_baseline_win":sm["win_rate"] is not None and bm["win_rate"] is not None and sm["win_rate"]>bm["win_rate"]}
    decision="hold_for_long_history_and_portfolio_gate" if all(checks.values()) else "drop"
    payload={"schema_version":"tradex_long_2026_regime_compound_tree_v1.compare.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,
      "fixed_evaluation_conditions":{"universe":"PAN ordinary stocks; ETF/ETN excluded","broad_trigger":"none; all ordinary daily rows" if args.universe=="all" else "ret3 3%-20%, close>MA20, range20>3%","discovery":"2026-01-01..03-31","validation":"2026-04-01..05-31","untouched_test":"2026-06-01 through latest mature signal","features":feature_names,"market_breadth_role":"excluded from stock selection; reserved for later permission gate" if args.exclude_market_breadth else "included","tree":{"max_depth":4,"min_samples_leaf":200,"random_state":20260720},"compound_gate":"at least two distinct features","execution":"next open to same-day close" if args.target=="nextday" else "next-open staged entry; exit H10 close","target":args.target,"costs":"ignored","production_ranking_changed":False,"runtime_db_write":False},
      "authoritative_result":{"leaves":leaves,"eligible_leaves":eligible,"test_selected":sm,"test_same_condition_baseline":bm,"monthly_test":monthly,"checks":checks},
      "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int(len(selected)),"selection_divergence_reason":"2026 discovery-only compound chart-state leaves"},
      "judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"recent_regime_strict_temporal_gate"},"remaining_risks":["long-history and portfolio gates pending only if recent test passes"]}
    selected.to_parquet(output/"test_selected_ledger.parquet",index=False);(output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8");(output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8")
    print(json.dumps({"eligible":eligible,"selected":sm,"baseline":bm,"checks":checks,"decision":decision},ensure_ascii=False))


if __name__=="__main__":main()
