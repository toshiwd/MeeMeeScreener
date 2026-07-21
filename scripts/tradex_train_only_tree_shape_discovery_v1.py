from __future__ import annotations

import json
from itertools import combinations
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeRegressor


DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
OUT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
TP, SL, H = 0.08, 0.05, 10
FEATURES = ["ret10", "ret60_pct", "gap_ma20", "gap_ma60", "ma_alignment", "ma20_slope", "upper_close", "lower_wick", "body_ratio", "volume_ratio", "range10", "prior_high1_gap", "prior_high20_gap"]


def tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def hit(col: str, op: str, threshold: str) -> str:
    return "LEAST(" + ", ".join(f"CASE WHEN {col}{i} {op} {threshold} THEN {i} ELSE 99 END" for i in range(1, H + 1)) + ")"


def pf(values: pd.Series) -> float | None:
    pos, neg = values[values > 0].sum(), values[values < 0].sum()
    return None if neg == 0 else float(pos / abs(neg))


def metrics(frame: pd.DataFrame) -> tuple[dict[str, dict], list[dict]]:
    splits, years = {}, []
    for split, g in frame.groupby("split"):
        daily = g.groupby("date", as_index=False)["realized_return"].mean()
        splits[split] = {"sample_count": int(len(g)), "signal_days": int(g.date.nunique()), "expectancy": float(g.realized_return.mean()), "profit_factor": pf(g.realized_return), "daily_profit_factor": pf(daily.realized_return), "daily_expectancy": float(daily.realized_return.mean())}
    for year, g in frame.groupby("year"):
        daily = g.groupby("date", as_index=False)["realized_return"].mean()
        years.append({"year": int(year), "daily_profit_factor": pf(daily.realized_return), "daily_expectancy": float(daily.realized_return.mean()), "trading_days": int(len(daily))})
    return splits, years


def leaf_rules(model: DecisionTreeRegressor) -> dict[int, list[str]]:
    tree = model.tree_
    rules: dict[int, list[str]] = {}
    def walk(node: int, path: list[str]) -> None:
        if tree.feature[node] < 0:
            rules[node] = path
            return
        name = FEATURES[tree.feature[node]]; threshold = tree.threshold[node]
        walk(tree.children_left[node], path + [f"{name} <= {threshold:.6g}"])
        walk(tree.children_right[node], path + [f"{name} > {threshold:.6g}"])
    walk(0, [])
    return rules


def visual_events(frame: pd.DataFrame) -> list[dict]:
    events: list[dict] = []
    for label, condition in (("take_profit", frame.realized_return == TP), ("stop_loss", frame.realized_return == -SL)):
        picked = frame[condition].sort_values(["date", "code"]).groupby("split", as_index=False).head(1)
        for row in picked.itertuples(index=False):
            events.append({"label": label, "code": str(row.code), "date_epoch": int(row.date), "year": int(row.year), "split": str(row.split), "realized_return": float(row.realized_return)})
    return events


def run() -> Path:
    output = OUT / f"{tag()}-train_only_tree_shape_discovery_v1"
    output.mkdir(parents=True, exist_ok=False)
    future = ", ".join(f"LEAD(b.h,{i}) OVER w AS h{i}, LEAD(b.l,{i}) OVER w AS l{i}" for i in range(1, H + 1))
    sql = f"""
    WITH eligible AS (SELECT code FROM daily_bars WHERE source='pan' GROUP BY code HAVING MAX(date)>=1783555200),
    bars AS (
      SELECT b.code,b.date,b.o,b.h,b.l,b.c,b.v,LAG(b.c,10) OVER w c10,LAG(b.c,60) OVER w c60,LAG(b.h,1) OVER w ph1,LAG(b.l,1) OVER w pl1,MAX(b.h) OVER p20 ph20,MIN(b.l) OVER p20 pl20,MAX(b.h) OVER l10 hi10,MIN(b.l) OVER l10 lo10,
        AVG(b.c) OVER m20 ma20,AVG(b.c) OVER m20l5 ma20_5,AVG(b.c) OVER m60 ma60,AVG(b.v) OVER m20 avgv,AVG(b.v) OVER v5prior avg_v5_prior,LEAD(b.c,{H}) OVER w cend,{future}
      FROM daily_bars b JOIN eligible e USING(code) WHERE b.source='pan'
      WINDOW w AS(PARTITION BY b.code ORDER BY b.date),p20 AS(PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING),l10 AS(PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW),m20 AS(PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),m20l5 AS(PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 24 PRECEDING AND 5 PRECEDING),m60 AS(PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),v5prior AS(PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
    ), breadth AS (SELECT date,AVG(CASE WHEN c>=ma20 THEN 1.0 ELSE 0.0 END) market_breadth FROM bars WHERE ma20 IS NOT NULL GROUP BY date),
    raw AS (SELECT b.*,(c/c10)-1 ret10,(c/c60)-1 ret60,(hi10/lo10)-1 range10,{hit('h','>=',f'c*{1+TP}')} tpday,{hit('l','<=',f'c*{1-SL}')} slday,CAST(year(to_timestamp(CAST(date AS BIGINT))) AS INTEGER) AS year,market_breadth FROM bars b JOIN breadth USING(date) WHERE date BETWEEN 1546300800 AND 1767139200 AND c10 IS NOT NULL AND c60 IS NOT NULL AND ph1 IS NOT NULL AND pl1 IS NOT NULL AND ph20 IS NOT NULL AND pl20 IS NOT NULL AND ma20 IS NOT NULL AND ma20_5 IS NOT NULL AND ma60 IS NOT NULL AND avgv>0 AND avg_v5_prior IS NOT NULL AND cend IS NOT NULL AND h>l AND ma20 > ma60 AND c >= ph20 * 0.95 AND l BETWEEN ma20 * 0.99 AND ma20 AND c > ma20 AND c > o AND (c-l)/(h-l) >= 0.70),
    ranked AS (SELECT *,CUME_DIST() OVER(PARTITION BY date ORDER BY ret60) ret60_pct FROM raw)
    SELECT code,ret10,ret60_pct,(c/ma20)-1 gap_ma20,(c/ma60)-1 gap_ma60,(ma20/ma60)-1 ma_alignment,(ma20/ma20_5)-1 ma20_slope,(c-l)/(h-l) upper_close,(LEAST(o,c)-l)/(h-l) lower_wick,(c-o)/(h-l) body_ratio,v/avgv volume_ratio,range10,(c/ph1)-1 prior_high1_gap,(c/ph20)-1 prior_high20_gap,market_breadth,date,year,
      CASE WHEN slday<={H} AND slday<=tpday THEN -{SL} WHEN tpday<={H} THEN {TP} ELSE(cend/c)-1 END realized_return,
      CASE WHEN year<=2021 THEN 'train' WHEN year<=2023 THEN 'validation' ELSE 'test' END split
    FROM ranked
    """
    conn = duckdb.connect(str(DB), read_only=True)
    try: data = conn.execute(sql).fetchdf()
    finally: conn.close()
    train = data[data.split == "train"].copy()
    model = DecisionTreeRegressor(max_depth=4, min_samples_leaf=300, random_state=7)
    model.fit(train[FEATURES], train.realized_return)
    data["leaf"] = model.apply(data[FEATURES]); train["leaf"] = model.apply(train[FEATURES])
    rules = leaf_rules(model); candidates = []; train_pass_leaves: list[int] = []
    for leaf in sorted(train.leaf.unique()):
        all_leaf = data[data.leaf == leaf]; split_metrics, yearly = metrics(all_leaf); t = split_metrics.get("train", {})
        train_pass = t.get("sample_count", 0) >= 300 and (t.get("expectancy") or 0) > 0 and (t.get("profit_factor") or 0) >= 1.2 and (t.get("daily_profit_factor") or 0) >= 1.15
        if not train_pass: continue
        train_pass_leaves.append(int(leaf))
        full = all(s in split_metrics and split_metrics[s]["sample_count"] >= 300 and (split_metrics[s]["expectancy"] or 0)>0 and (split_metrics[s]["profit_factor"] or 0)>=1.2 and (split_metrics[s]["daily_profit_factor"] or 0)>=1.15 for s in ("train","validation","test")) and len(yearly)==7 and all((x["daily_profit_factor"] or 0)>=1.0 for x in yearly)
        candidates.append({"leaf":int(leaf),"rules":rules[int(leaf)],"metrics_by_split":split_metrics,"yearly_daily_basket_metrics":yearly,"candidate_local_decision":"candidate_for_meemee_visual_review" if full else "drop"})
    for size in range(2, len(train_pass_leaves) + 1):
        for subset in combinations(train_pass_leaves, size):
            ensemble = data[data.leaf.isin(subset)]
            split_metrics, yearly = metrics(ensemble)
            full = all(s in split_metrics and split_metrics[s]["sample_count"] >= 300 and (split_metrics[s]["expectancy"] or 0)>0 and (split_metrics[s]["profit_factor"] or 0)>=1.2 and (split_metrics[s]["daily_profit_factor"] or 0)>=1.15 for s in ("train","validation","test")) and len(yearly)==7 and all((x["daily_profit_factor"] or 0)>=1.0 for x in yearly)
            candidates.append({"ensemble_leaves":[int(leaf) for leaf in subset],"rules_by_leaf":{str(leaf):rules[leaf] for leaf in subset},"metrics_by_split":split_metrics,"yearly_daily_basket_metrics":yearly,"visual_review_events":visual_events(ensemble) if full else [],"candidate_local_decision":"candidate_for_meemee_visual_review" if full else "drop"})
    payload={"schema_version":"tradex_train_only_tree_shape_discovery_v1.compare.v1","authoritative_result":True,"research_phase":"branching_generation","fixed_evaluation_conditions":{"source_db":str(DB),"source_filter":"pan","confirmed_latest_date":"2026-07-09","entry_trigger":"MA20 above MA60, close >=95% of prior 20-day high, <=1% MA20 undercut, bullish candle, and close in upper 30% of candle range","entry":"signal-day close","take_profit":TP,"stop_loss":SL,"max_holding_days":H,"same_day_dual_hit":"stop first","costs":"excluded","runtime_db_write":False,"selection_protocol":"tree trained only on 2019-2021; validation and test held out; multi-leaf ensembles are generated only from train-passing mutually exclusive leaves"},"tree":{"max_depth":4,"min_samples_leaf":300,"features":FEATURES},"train_screen_gate":"sample >=300, positive expectancy, trade PF >=1.20, daily PF >=1.15","full_adoption_gate":"same gate each split and all seven annual daily PF >=1.0","train_passing_leaves":train_pass_leaves,"screened_candidates":candidates,"authoritative_rollup_decision":"candidate_for_meemee_visual_review" if any(x["candidate_local_decision"]=="candidate_for_meemee_visual_review" for x in candidates) else "no_candidate","production_ranking_changed":False,"runtime_db_write":False}
    (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    return output


if __name__ == "__main__":
    print(run())
