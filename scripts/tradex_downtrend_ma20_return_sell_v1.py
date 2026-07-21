from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


OUT = Path(r"G:\Tradex\downtrend_ma20_return_sell_v1")
TP, SL, H = 0.08, 0.05, 10


def pf(values: pd.Series) -> float | None:
    pos, neg = values[values > 0].sum(), values[values < 0].sum()
    return None if neg == 0 else float(pos / abs(neg))


def metrics(data: pd.DataFrame) -> tuple[dict, list[dict]]:
    splits, years = {}, []
    for split, group in data.groupby("split"):
        daily = group.groupby("date", as_index=False)["short_return"].mean()
        splits[split] = {"sample_count": int(len(group)), "expectancy": float(group.short_return.mean()), "profit_factor": pf(group.short_return), "daily_profit_factor": pf(daily.short_return)}
    for year, group in data.groupby("year"):
        daily = group.groupby("date", as_index=False)["short_return"].mean()
        years.append({"year": int(year), "daily_profit_factor": pf(daily.short_return), "daily_expectancy": float(daily.short_return.mean())})
    return splits, years


def run() -> Path:
    sys.path.insert(0, str(Path.cwd())); sys.path.insert(0, "app")
    from backend.services.codex_bridge_service import get_runtime_stock_db_status
    runtime = get_runtime_stock_db_status(); db = runtime["selected_runtime_db_path"]
    output = OUT / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-downtrend_ma20_return_sell_v1"; output.mkdir(parents=True, exist_ok=False)
    future_h = ", ".join(f"LEAD(h,{i}) OVER w h{i}" for i in range(1,H+1)); future_l = ", ".join(f"LEAD(l,{i}) OVER w l{i}" for i in range(1,H+1))
    tp = "LEAST(" + ", ".join(f"CASE WHEN l{i} <= next_open * {1-TP} THEN {i} ELSE 99 END" for i in range(1,H+1)) + ")"
    sl = "LEAST(" + ", ".join(f"CASE WHEN h{i} >= next_open * {1+SL} THEN {i} ELSE 99 END" for i in range(1,H+1)) + ")"
    sql=f"""WITH latest AS (SELECT MAX(date) d FROM daily_bars WHERE source='pan'), e AS (SELECT code FROM daily_bars WHERE source='pan' GROUP BY code HAVING MAX(date)=(SELECT d FROM latest)),
    b AS (SELECT b.code,b.date,b.o,b.h,b.l,b.c,LAG(b.c,10) OVER w c10,AVG(b.c) OVER m20 ma20,AVG(b.c) OVER m60 ma60,LEAD(b.o,1) OVER w next_open,LEAD(b.c,{H}) OVER w cend,{future_h},{future_l} FROM daily_bars b JOIN e USING(code) WHERE source='pan' WINDOW w AS(PARTITION BY b.code ORDER BY b.date),m20 AS(PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),m60 AS(PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)),
    s AS (SELECT *,{tp} tpd,{sl} sld,CAST(year(to_timestamp(CAST(date AS BIGINT))) AS INTEGER) AS yr FROM b WHERE date BETWEEN 1546300800 AND 1767139200 AND next_open IS NOT NULL AND cend IS NOT NULL AND ma20<ma60 AND h>=ma20*.98 AND c<ma20 AND c<o AND c10 IS NOT NULL)
    SELECT code,date,yr AS year,CASE WHEN sld<={H} AND sld<=tpd THEN -{SL} WHEN tpd<={H} THEN {TP} ELSE(next_open/cend)-1 END short_return,CASE WHEN yr<=2021 THEN 'train' WHEN yr<=2023 THEN 'validation' ELSE 'test' END split FROM s"""
    conn=duckdb.connect(db,read_only=True)
    try: data=conn.execute(sql).fetchdf()
    finally: conn.close()
    splits, years=metrics(data)
    keep=all(item.get("sample_count",0)>=300 and (item.get("expectancy") or 0)>0 and (item.get("profit_factor") or 0)>=1.2 and (item.get("daily_profit_factor") or 0)>=1.15 for item in splits.values()) and len(years)==7 and all((row["daily_profit_factor"] or 0)>=1 for row in years)
    payload={"schema_version":"tradex_downtrend_ma20_return_sell_v1.compare.v1","authoritative_result":True,"research_phase":"branching_generation","fixed_evaluation_conditions":{"source_db":db,"source":"pan","confirmed_latest_date":runtime["latest_confirmed_daily_bars_date_iso"],"entry":"next_session_open","setup":"MA20 below MA60, intraday return to MA20, bearish close below MA20","take_profit":TP,"stop_loss":SL,"max_holding_days":H,"same_day_dual_hit":"stop first","costs":"excluded"},"metrics_by_split":splits,"yearly_daily_basket_metrics":years,"authoritative_rollup_decision":"candidate_for_portfolio_compare" if keep else "drop","runtime_db_write":False,"production_ranking_changed":False}
    (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); return output


if __name__=="__main__": print(run())
