from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


VARIANTS = [
    {"tp": 0.02, "sl": 0.02, "hold": 10},
    {"tp": 0.025, "sl": 0.02, "hold": 10},
    {"tp": 0.03, "sl": 0.02, "hold": 10},
    {"tp": 0.03, "sl": 0.025, "hold": 10},
    {"tp": 0.03, "sl": 0.03, "hold": 10},
    {"tp": 0.04, "sl": 0.03, "hold": 10},
    {"tp": 0.05, "sl": 0.03, "hold": 10},
    {"tp": 0.05, "sl": 0.04, "hold": 10},
    {"tp": 0.08, "sl": 0.05, "hold": 10},
]


def load_signals(db_path: str) -> pd.DataFrame:
    future = ",\n".join(
        f"lead(b.o,{k}) over w as o{k}, lead(b.h,{k}) over w as h{k}, "
        f"lead(b.l,{k}) over w as l{k}, lead(b.c,{k}) over w as c{k}, lead(b.date,{k}) over w as d{k}"
        for k in range(1, 11)
    )
    query = f"""
    WITH raw AS (
      SELECT b.code,b.date,b.o,b.h,b.l,b.c,b.v,
        row_number() over(partition by b.code order by b.date) bar_index,
        b.o/nullif(lag(b.c) over w,0)-1 gap_up,
        avg(b.c) over(partition by b.code order by b.date rows between 6 preceding and current row) ma7,
        avg(b.c) over(partition by b.code order by b.date rows between 19 preceding and current row) ma20,
        {future}
      FROM daily_bars b WHERE b.source='pan'
      WINDOW w AS (partition by b.code order by b.date)
    ), feature AS (
      SELECT *,max(gap_up) over(partition by code order by date rows between 7 preceding and 1 preceding) prior_gap7
      FROM raw
    ), state AS (
      SELECT *,cast(prior_gap7>=.03 and c between ma7*.97 and ma7*1.03 and c>ma20 as integer) family_flag
      FROM feature
    ), signal AS (
      SELECT *,lag(family_flag,1,0) over(partition by code order by date) prior_family FROM state
    )
    SELECT s.*,coalesce(i.name,m.name,'') stock_name
    FROM signal s LEFT JOIN industry_master i using(code) LEFT JOIN stock_meta m using(code)
    WHERE family_flag=1 and prior_family=0 and coalesce(i.market_code,'')<>'ETF・ETN'
      and c>=100 and v>0 and c10 is not null and abs(gap_up)<.5
    ORDER BY date,code
    """
    with duckdb.connect(db_path, read_only=True) as conn:
        return conn.execute(query).fetchdf()


def simulate(rows: pd.DataFrame, variant: dict) -> pd.DataFrame:
    result = rows.copy()
    returns = []
    exit_dates = []
    for row in result.itertuples(index=False):
        entry = float(row.o1)
        value = None
        exit_date = None
        for k in range(1, variant["hold"] + 1):
            o, h, l = float(getattr(row, f"o{k}")), float(getattr(row, f"h{k}")), float(getattr(row, f"l{k}"))
            if k > 1 and o <= entry * (1 - variant["sl"]):
                value, exit_date = (o / entry - 1) * 100, getattr(row, f"d{k}")
                break
            if k > 1 and o >= entry * (1 + variant["tp"]):
                value, exit_date = (o / entry - 1) * 100, getattr(row, f"d{k}")
                break
            stop = l <= entry * (1 - variant["sl"])
            take = h >= entry * (1 + variant["tp"])
            if stop:
                value, exit_date = -variant["sl"] * 100, getattr(row, f"d{k}")
                break
            if take:
                value, exit_date = variant["tp"] * 100, getattr(row, f"d{k}")
                break
        if value is None:
            value, exit_date = (float(row.c10) / entry - 1) * 100, row.d10
        returns.append(value)
        exit_dates.append(exit_date)
    result["realized_ret"] = returns
    result["exit_date"] = pd.to_datetime(exit_dates, unit="s")
    return result


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"n": 0, "codes": 0, "mean_return_pct": None, "median_return_pct": None,
                "win_rate": None, "severe_loss5_rate": None, "top3_positive_profit_share": None}
    positive = frame.loc[frame.realized_ret > 0, "realized_ret"]
    total = float(positive.sum())
    return {"n": int(len(frame)), "codes": int(frame.code.nunique()),
            "mean_return_pct": float(frame.realized_ret.mean()), "median_return_pct": float(frame.realized_ret.median()),
            "win_rate": float(frame.realized_ret.gt(0).mean()), "severe_loss5_rate": float(frame.realized_ret.le(-5).mean()),
            "top3_positive_profit_share": None if total <= 0 else float(positive.nlargest(3).sum()/total)}


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
    variants=[]
    for variant in VARIANTS:
        evaluated=simulate(rows,variant)
        train=metrics(evaluated[evaluated.year.between(2019,2024)])
        validation=metrics(evaluated[evaluated.year.eq(2025)])
        variants.append({"variant":variant,"train":train,"validation":validation})
    selectable=[item for item in variants if item["validation"]["n"]>=250
                and (item["validation"]["mean_return_pct"] or -99)>0
                and (item["validation"]["win_rate"] or 0)>=.50
                and (item["validation"]["severe_loss5_rate"] or 1)<=.03
                and (item["train"]["mean_return_pct"] or -99)>0]
    selected=max(selectable,key=lambda x:x["validation"]["mean_return_pct"],default=None)
    evaluated=simulate(rows,selected["variant"]) if selected else rows.assign(realized_ret=float("nan"))
    test=evaluated[evaluated.year.eq(2026)].copy() if selected else evaluated.iloc[0:0].copy()
    test_metrics=metrics(test)
    monthly={str(month):metrics(group) for month,group in test.groupby(test.signal_date.dt.to_period("M"))}
    yearly={str(year):metrics(evaluated[evaluated.year.eq(year)]) for year in range(2019,2027)} if selected else {}
    positive_months=sum((item["mean_return_pct"] or -99)>0 for item in monthly.values())
    checks={"selected_on_2025_only":selected is not None,"test_n_at_least_250":test_metrics["n"]>=250,
            "test_mean_positive":(test_metrics["mean_return_pct"] or -99)>0,
            "test_win_rate_at_least_50pct":(test_metrics["win_rate"] or 0)>=.50,
            "test_severe_loss5_at_most_3pct":(test_metrics["severe_loss5_rate"] or 1)<=.03,
            "test_top3_profit_share_at_most_35pct":(test_metrics["top3_positive_profit_share"] or 1)<=.35,
            "test_months_majority_positive":bool(monthly) and positive_months/len(monthly)>=.70,
            "every_year_positive":bool(yearly) and all((item["mean_return_pct"] or -99)>0 for item in yearly.values())}
    decision="hold_for_portfolio_gate" if all(checks.values()) else "drop"
    payload={"schema_version":"tradex_gu_first_pullback_exit_geometry_latest_v1.compare.v1","artifact_role":"authoritative",
             "generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,
             "fixed_evaluation_conditions":{"axis":"exit geometry only","family":"prior 1-7 sessions max GU>=3%; current close within MA7 +/-3%; close>MA20; first false-to-true",
                 "universe":"PAN ordinary stocks; ETF/ETN excluded","entry":"next open","same_bar":"stop first",
                 "train":"2019-2024","validation_selection":2025,"untouched_test":"2026 through latest mature H10 signal","variants":VARIANTS,
                 "costs":"ignored","production_ranking_changed":False,"runtime_db_write":False,"meemee_reflection_allowed":False},
             "authoritative_result":{"variants":variants,"selected":selected,"test_2026":test_metrics,"monthly_2026":monthly,"yearly":yearly,"checks":checks},
             "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int(len(test)),"selection_divergence_reason":"fixed GU first-pullback transition plus validation-selected exit geometry"},
             "judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"strict_point_in_time_exit_geometry_gate"},
             "remaining_risks":["portfolio capital allocation pending if event gate passes"]}
    test.to_parquet(output/"test_signal_ledger.parquet",index=False)
    (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
    (output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8")
    print(json.dumps({"selected":selected,"test":test_metrics,"checks":checks,"decision":decision},ensure_ascii=False))


if __name__=="__main__": main()
