from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "full_universe_clean_breakout_breadth_oos_v1"
DEFAULT_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUT = Path(r"G:\Tradex\full_universe_clean_breakout_breadth_oos_v1")
BREADTH_FLOORS = (0.0, 0.5, 0.6, 0.7, 0.8)
STOP = 0.03
HOLD = 20
CAPITAL = 10_000_000.0
SLOT_NOTIONAL = 2_000_000.0
MAX_SLOTS = 5


def ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [ready(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, "item"):
        return ready(value.item())
    return value


def metrics(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {"n": 0, "date_count": 0, "expectancy": None, "profit_factor": None, "win_rate": None, "stop_rate": None, "p05": None}
    ret = rows["ret"].astype(float)
    loss = float(-ret[ret < 0].sum())
    return {
        "n": int(len(rows)),
        "date_count": int(rows.ymd.nunique()),
        "code_count": int(rows.code.nunique()),
        "expectancy": float(ret.mean()),
        "profit_factor": float(ret[ret > 0].sum()) / loss if loss else None,
        "win_rate": float((ret > 0).mean()),
        "stop_rate": float(rows.stopped.mean()),
        "p05": float(ret.quantile(0.05)),
    }


def annual(rows: pd.DataFrame) -> dict[str, Any]:
    return {str(int(year)): metrics(part) for year, part in rows.groupby(rows.ymd // 10000)}


def portfolio(rows: pd.DataFrame) -> dict[str, Any]:
    cash = CAPITAL
    open_positions: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    curve = [(None, cash)]
    for ymd, day in rows.sort_values(["ymd", "lower_wick_ratio", "code"]).groupby("ymd", sort=True):
        still_open = []
        for pos in open_positions:
            if pos["exit_ymd"] <= ymd:
                pnl = SLOT_NOTIONAL * pos["ret"]
                cash += SLOT_NOTIONAL + pnl
                trades.append({**pos, "pnl_yen": pnl})
            else:
                still_open.append(pos)
        open_positions = still_open
        held = {p["code"] for p in open_positions}
        capacity = min(MAX_SLOTS - len(open_positions), int(cash // SLOT_NOTIONAL))
        for row in day.itertuples(index=False):
            if capacity <= 0:
                break
            if row.code in held:
                continue
            cash -= SLOT_NOTIONAL
            open_positions.append({"code": row.code, "entry_ymd": int(row.ymd), "exit_ymd": int(row.exit_ymd), "ret": float(row.ret), "stopped": bool(row.stopped)})
            held.add(row.code)
            capacity -= 1
        equity = cash + SLOT_NOTIONAL * len(open_positions)
        curve.append((int(ymd), equity))
    for pos in open_positions:
        pnl = SLOT_NOTIONAL * pos["ret"]
        cash += SLOT_NOTIONAL + pnl
        trades.append({**pos, "pnl_yen": pnl})
    values = pd.Series([v for _, v in curve] + [cash], dtype=float)
    drawdown = values - values.cummax()
    return {
        "starting_capital_yen": CAPITAL,
        "ending_capital_yen": float(cash),
        "net_pnl_yen": float(cash - CAPITAL),
        "max_drawdown_yen": float(drawdown.min()),
        "trade_count": len(trades),
        "slot_notional_yen": SLOT_NOTIONAL,
        "max_slots": MAX_SLOTS,
    }


def extract(db: Path) -> pd.DataFrame:
    query = f"""
    WITH bars AS (
      SELECT code, CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) ymd,
             o,h,l,c,v,
             avg(c) OVER w7 ma7, avg(c) OVER w20 ma20, avg(c) OVER w60 ma60,
             lag(c,5) OVER w ma20_slope_base,
             lag(c,20) OVER w ma60_slope_base,
             max(h) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) prior20_high,
             lead(c,{HOLD}) OVER w exit_close,
             lead(CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER),{HOLD}) OVER w exit_ymd,
             min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 1 FOLLOWING AND {HOLD} FOLLOWING) future_low,
             row_number() OVER w rn
      FROM daily_bars WHERE source='pan'
      WINDOW w AS (PARTITION BY code ORDER BY date),
             w7 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
             w20 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
             w60 AS (PARTITION BY code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    ), enriched AS (
      SELECT *, lag(ma20,5) OVER(PARTITION BY code ORDER BY ymd) ma20_lag5,
                lag(ma60,20) OVER(PARTITION BY code ORDER BY ymd) ma60_lag20,
                avg(CASE WHEN c>ma20 THEN 1.0 ELSE 0.0 END) OVER(PARTITION BY ymd) breadth,
                CASE WHEN h>l THEN (least(o,c)-l)/(h-l) ELSE 0 END lower_wick_ratio,
                CASE WHEN h>l THEN (c-l)/(h-l) ELSE 0 END close_pos
      FROM bars
    ), candidates AS (
      SELECT *, row_number() OVER(PARTITION BY ymd ORDER BY lower_wick_ratio, code) day_rank
      FROM enriched
      WHERE ymd BETWEEN 20190101 AND 20251231 AND exit_close IS NOT NULL
        AND c>=prior20_high AND ma7>ma20 AND ma20>ma60
        AND ma20>ma20_lag5 AND ma60>ma60_lag20 AND close_pos>=0.8
    )
    SELECT code,ymd,c,l,exit_close,exit_ymd,breadth,lower_wick_ratio,day_rank,
           CASE WHEN future_low<=c*(1-{STOP}) THEN -{STOP} ELSE exit_close/c-1 END AS return_value,
           future_low<=c*(1-{STOP}) AS stopped
    FROM candidates WHERE day_rank<=5 ORDER BY ymd,day_rank
    """
    with duckdb.connect(str(db), read_only=True) as conn:
        rows = conn.execute(query).fetchdf()
    return rows.rename(columns={"return_value": "ret"})


def run(db: Path, out: Path) -> Path:
    rows = extract(db)
    reports = []
    for floor in BREADTH_FLOORS:
        selected = rows[rows.breadth >= floor].copy()
        reports.append({
            "breadth_floor": floor,
            "train_2019_2021": metrics(selected[(selected.ymd>=20190101)&(selected.ymd<=20211231)]),
            "validation_2022_2023": metrics(selected[(selected.ymd>=20220101)&(selected.ymd<=20231231)]),
            "test_2024_2025": metrics(selected[(selected.ymd>=20240101)&(selected.ymd<=20251231)]),
        })
    eligible = [r for r in reports if r["train_2019_2021"]["n"]>=100 and (r["train_2019_2021"]["profit_factor"] or 0)>=1.2 and (r["train_2019_2021"]["expectancy"] or 0)>0]
    chosen = max(eligible, key=lambda r: r["train_2019_2021"]["profit_factor"]) if eligible else None
    floor = chosen["breadth_floor"] if chosen else None
    final_rows = rows[rows.breadth >= floor].copy() if floor is not None else rows.iloc[0:0].copy()
    val = metrics(final_rows[(final_rows.ymd>=20220101)&(final_rows.ymd<=20231231)])
    test = metrics(final_rows[(final_rows.ymd>=20240101)&(final_rows.ymd<=20251231)])
    stable = bool(chosen and (val["profit_factor"] or 0)>=1.1 and (val["expectancy"] or 0)>0 and (test["profit_factor"] or 0)>=1.1 and (test["expectancy"] or 0)>0)
    root = out / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True)
    final_rows.to_csv(root / "selected_events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment", "boundary_owner": "TRADEX",
        "fixed_evaluation_conditions": {
            "universe": "all PAN daily_bars codes in runtime DB", "period": "2019-2025",
            "shape_contract": "close>=prior20_high; MA7>MA20>MA60; MA20 rising 5 rows; MA60 rising 20 rows; close_pos>=0.8",
            "ranking": "lower_wick_ratio ascending then code; top5/day", "entry": "signal close",
            "exit": "3% stop when any low in following 20 rows breaches; otherwise 20th-row close",
            "changed_axis": "same-day all-stock breadth above MA20", "thresholds": BREADTH_FLOORS,
            "selection_period": "2019-2021 only", "validation_period": "2022-2023", "untouched_test": "2024-2025",
            "costs": "not modeled",
        },
        "reports": reports,
        "selection": {"protocol": "highest train PF with n>=100, PF>=1.2, expectancy>0", "selected_breadth_floor": floor},
        "selected_rule": {"annual": annual(final_rows), "all_events": metrics(final_rows), "portfolio": portfolio(final_rows) if not final_rows.empty else None},
        "decision": {"candidate_local_decision": "keep_for_same_condition_comparison" if stable else "drop", "authoritative_rollup_decision": "research_only", "reason_type": "all_validation_splits_positive_and_pf_at_least_1_1" if stable else "failed_fixed_split_stability_gate"},
        "limitations": ["clean_high_breakout original generator was unavailable; this is a separately named explicit shape contract", "intraday stop fill is modeled exactly at -3% after a low breach; gaps and costs are not modeled"],
        "runtime_db_write": False, "production_ranking_changed": False, "silent_fallback_used": False,
    }
    (root / "compare.json").write_text(json.dumps(ready(payload), ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(root / "compare.json")
    return root


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    run(args.db, args.out)
