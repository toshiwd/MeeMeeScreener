from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from tradex_full_universe_clean_breakout_breadth_oos_v1 import metrics, annual, portfolio, ready


AXIS_ID = "full_universe_breakout_closepos_oos_v1"
DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
OUT = Path(r"G:\Tradex\full_universe_breakout_closepos_oos_v1")
THRESHOLDS = (0.5, 0.6, 0.7, 0.8, 0.9, 0.95)


def extract(db: Path) -> pd.DataFrame:
    query = """
    WITH bars AS (
      SELECT code, CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) ymd,o,h,l,c,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) ma7,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) ma60,
        max(h) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) prior20_high,
        lead(c,20) OVER(PARTITION BY code ORDER BY date) exit_close,
        lead(CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER),20) OVER(PARTITION BY code ORDER BY date) exit_ymd,
        min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) future_low
      FROM daily_bars WHERE source='pan'
    ), features AS (
      SELECT *,lag(ma20,5) OVER(PARTITION BY code ORDER BY ymd) ma20_lag5,
        lag(ma60,20) OVER(PARTITION BY code ORDER BY ymd) ma60_lag20,
        CASE WHEN h>l THEN (least(o,c)-l)/(h-l) ELSE 0 END lower_wick_ratio,
        CASE WHEN h>l THEN (c-l)/(h-l) ELSE 0 END close_pos
      FROM bars
    )
    SELECT code,ymd,c,exit_close,exit_ymd,lower_wick_ratio,close_pos,
      CASE WHEN future_low<=c*.97 THEN -.03 ELSE exit_close/c-1 END ret,
      future_low<=c*.97 stopped
    FROM features
    WHERE ymd BETWEEN 20190101 AND 20251231 AND exit_close IS NOT NULL
      AND c>=prior20_high AND ma7>ma20 AND ma20>ma60 AND ma20>ma20_lag5 AND ma60>ma60_lag20
    """
    with duckdb.connect(str(db), read_only=True) as conn:
        return conn.execute(query).fetchdf()


def top5(rows: pd.DataFrame, threshold: float) -> pd.DataFrame:
    selected = rows[rows.close_pos >= threshold].copy()
    selected = selected.sort_values(["ymd", "lower_wick_ratio", "code"])
    selected["day_rank"] = selected.groupby("ymd").cumcount() + 1
    return selected[selected.day_rank <= 5].copy()


def run(db: Path, out: Path) -> Path:
    rows = extract(db)
    reports = []
    selected_by_floor = {}
    for threshold in THRESHOLDS:
        selected = top5(rows, threshold)
        selected_by_floor[threshold] = selected
        reports.append({"close_pos_floor": threshold,
            "train_2019_2021": metrics(selected[selected.ymd.between(20190101,20211231)]),
            "validation_2022_2023": metrics(selected[selected.ymd.between(20220101,20231231)]),
            "test_2024_2025": metrics(selected[selected.ymd.between(20240101,20251231)])})
    eligible = [r for r in reports if r["train_2019_2021"]["n"]>=100 and (r["train_2019_2021"]["profit_factor"] or 0)>=1.2 and (r["train_2019_2021"]["expectancy"] or 0)>0]
    chosen = max(eligible,key=lambda r:r["train_2019_2021"]["profit_factor"]) if eligible else None
    threshold = chosen["close_pos_floor"] if chosen else None
    final = selected_by_floor[threshold] if threshold is not None else rows.iloc[0:0].copy()
    val = metrics(final[final.ymd.between(20220101,20231231)])
    test = metrics(final[final.ymd.between(20240101,20251231)])
    stable = bool(chosen and (val["profit_factor"] or 0)>=1.1 and (val["expectancy"] or 0)>0 and (test["profit_factor"] or 0)>=1.1 and (test["expectancy"] or 0)>0)
    root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True)
    final.to_csv(root/'selected_events.csv',index=False)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","axis_id":AXIS_ID,"research_phase":"effectiveness_judgment","boundary_owner":"TRADEX",
      "fixed_evaluation_conditions":{"universe":"all PAN daily_bars codes","period":"2019-2025","base_shape":"close>=prior20_high; MA7>MA20>MA60; MA20 rising 5 rows; MA60 rising 20 rows","changed_axis":"signal candle close position only","thresholds":THRESHOLDS,"ranking":"lower wick ascending; top5/day","entry":"signal close","exit":"3% stop on following-20-row lows else row20 close","selection_period":"2019-2021 only","validation_period":"2022-2023","untouched_test":"2024-2025","costs":"not modeled"},
      "reports":reports,"selection":{"protocol":"highest train PF with n>=100 PF>=1.2 expectancy>0","selected_close_pos_floor":threshold},
      "selected_rule":{"annual":annual(final),"all_events":metrics(final),"portfolio":portfolio(final) if not final.empty else None},
      "decision":{"candidate_local_decision":"keep_for_same_condition_comparison" if stable else "drop","authoritative_rollup_decision":"research_only","reason_type":"all_validation_splits_positive_and_pf_at_least_1_1" if stable else "failed_fixed_split_stability_gate"},
      "runtime_db_write":False,"production_ranking_changed":False,"silent_fallback_used":False}
    (root/'compare.json').write_text(json.dumps(ready(payload),ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--db',type=Path,default=DB);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();run(a.db,a.out)
