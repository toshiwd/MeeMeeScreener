from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from tradex_gu_first_pullback_exit_geometry_latest_v1 import VARIANTS, metrics, simulate


TOP_K = [3, 5, 10, 20]


def load_signals(db_path: str) -> pd.DataFrame:
    future = ",\n".join(
        f"lead(b.o,{k}) over w o{k},lead(b.h,{k}) over w h{k},lead(b.l,{k}) over w l{k},"
        f"lead(b.c,{k}) over w c{k},lead(b.date,{k}) over w d{k}" for k in range(1, 11)
    )
    query=f"""
    WITH raw AS (
      SELECT b.code,b.date,b.o,b.h,b.l,b.c,b.v,
        avg(b.c) over(partition by b.code order by b.date rows between 6 preceding and current row) ma7,
        avg(b.c) over(partition by b.code order by b.date rows between 19 preceding and current row) ma20,
        avg(b.c) over(partition by b.code order by b.date rows between 59 preceding and current row) ma60,
        avg(b.c) over(partition by b.code order by b.date rows between 99 preceding and current row) ma100,
        avg(b.c) over(partition by b.code order by b.date rows between 199 preceding and current row) ma200,
        lag(b.c) over w prev_c,{future}
      FROM daily_bars b WHERE source='pan' WINDOW w AS(partition by b.code order by b.date)
    ), state AS (
      SELECT *,lag(ma20) over(partition by code order by date) prev_ma20,
        greatest(CASE WHEN ma7<c THEN ma7 END,CASE WHEN ma60<c THEN ma60 END,
                 CASE WHEN ma100<c THEN ma100 END,CASE WHEN ma200<c THEN ma200 END) nearest_lower_ma
      FROM raw
    )
    SELECT s.*,coalesce(i.name,m.name,'') stock_name
    FROM state s LEFT JOIN industry_master i using(code) LEFT JOIN stock_meta m using(code)
    WHERE prev_c<prev_ma20 AND c>=ma20
      AND (nearest_lower_ma IS NULL OR c/nearest_lower_ma-1>.05)
      AND coalesce(i.market_code,'')<>'ETF・ETN' AND c>=100 AND v>0 AND c10 IS NOT NULL
    ORDER BY date,code
    """
    with duckdb.connect(db_path,read_only=True) as conn:return conn.execute(query).fetchdf()


def topk(frame:pd.DataFrame,k:int)->pd.DataFrame:
    x=frame.sort_values(["signal_date","quality_score","code"],ascending=[True,False,True]).copy()
    x["daily_rank"]=x.groupby("signal_date").cumcount()+1
    return x[x.daily_rank<=k].copy()


def main()->None:
    p=argparse.ArgumentParser();p.add_argument("--output",required=True);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
    sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/"app")]
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime=get_runtime_stock_db_status();rows=load_signals(runtime["selected_runtime_db_path"])
    rows["signal_date"]=pd.to_datetime(rows.date,unit="s");rows["year"]=rows.signal_date.dt.year
    rng=(rows.h-rows.l).replace(0,float("nan"));rows["close_pos"]=(rows.c-rows.l)/rng
    rows["quality_score"]=(rows.c/rows.ma20-1).clip(0,.10)*10+rows.close_pos.fillna(.5)+(rows.c>rows.o).astype(float)
    exits=[]
    for variant in VARIANTS:
        e=simulate(rows,variant);exits.append({"variant":variant,"development_2019_2023":metrics(e[e.year.between(2019,2023)]),"exit_selection_2024":metrics(e[e.year.eq(2024)])})
    exok=[x for x in exits if x["exit_selection_2024"]["n"]>=250 and (x["exit_selection_2024"]["mean_return_pct"] or -99)>0 and (x["development_2019_2023"]["mean_return_pct"] or -99)>0]
    selected_exit=max(exok,key=lambda x:x["exit_selection_2024"]["mean_return_pct"],default=None)
    evaluated=simulate(rows,selected_exit["variant"]) if selected_exit else rows.assign(realized_ret=float("nan"))
    ranks=[]
    for k in TOP_K:
        z=topk(evaluated[evaluated.year.eq(2025)],k);ranks.append({"top_k":k,"rank_selection_2025":metrics(z)})
    rok=[x for x in ranks if x["rank_selection_2025"]["n"]>=250 and (x["rank_selection_2025"]["mean_return_pct"] or -99)>0 and (x["rank_selection_2025"]["win_rate"] or 0)>=.50 and (x["rank_selection_2025"]["severe_loss5_rate"] or 1)<=.03]
    selected_rank=max(rok,key=lambda x:x["rank_selection_2025"]["mean_return_pct"],default=None);k=selected_rank["top_k"] if selected_rank else None
    test=topk(evaluated[evaluated.year.eq(2026)],k) if k else evaluated.iloc[0:0].copy();tm=metrics(test)
    monthly={str(month):metrics(g) for month,g in test.groupby(test.signal_date.dt.to_period("M"))};yearly={str(y):metrics(topk(evaluated[evaluated.year.eq(y)],k)) for y in range(2019,2027)} if k else {}
    pm=sum((x["mean_return_pct"] or -99)>0 for x in monthly.values())
    checks={"exit_selected_on_2024_only":selected_exit is not None,"rank_selected_on_2025_only":selected_rank is not None,"test_n_at_least_250_or_full_audit":tm["n"]>=250,
            "test_mean_positive":(tm["mean_return_pct"] or -99)>0,"test_win_rate_at_least_50pct":(tm["win_rate"] or 0)>=.50,"test_severe_loss5_at_most_3pct":(tm["severe_loss5_rate"] or 1)<=.03,
            "test_top3_profit_share_at_most_35pct":(tm["top3_positive_profit_share"] or 1)<=.35,"test_months_majority_positive":bool(monthly) and pm/len(monthly)>=.70,
            "every_year_positive":bool(yearly) and all((x["mean_return_pct"] or -99)>0 for x in yearly.values())}
    decision="hold_for_portfolio_gate" if all(checks.values()) else "drop"
    payload={"schema_version":"tradex_ma20_reclaim_no_near_support_latest_v1.compare.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"runtime":runtime,
      "fixed_evaluation_conditions":{"family":"first close cross above MA20; no lower MA within 5pct","universe":"PAN ordinary stocks; ETF/ETN excluded","entry":"next open","development":"2019-2023","exit_selection":2024,"rank_selection":2025,"untouched_test":"2026 latest mature H10","quality_score":"reclaim depth + close position + bullish body","exit_variants":VARIANTS,"top_k_variants":TOP_K,"costs":"ignored","production_ranking_changed":False,"runtime_db_write":False},
      "authoritative_result":{"exit_variants":exits,"selected_exit":selected_exit,"rank_variants":ranks,"selected_rank":selected_rank,"test_2026":tm,"monthly_2026":monthly,"yearly":yearly,"checks":checks},
      "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int(len(test)),"selection_divergence_reason":"MA20 reclaim plus absence of nearby lower MA support"},
      "judgment":{"candidate_local_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"sequential_point_in_time_gate"},"remaining_risks":["portfolio allocation pending if event gate passes"]}
    test.to_parquet(out/"test_signal_ledger.parquet",index=False);(out/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}),encoding="utf-8")
    print(json.dumps({"selected_exit":selected_exit,"selected_rank":selected_rank,"test":tm,"checks":checks,"decision":decision},ensure_ascii=False))


if __name__=="__main__":main()
