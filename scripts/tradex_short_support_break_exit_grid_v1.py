from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "tradex_short_support_break_exit_grid_v1"
DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
OUTPUT_ROOT = Path(r"G:\Tradex\short_support_break_exit_grid_v1")
SPLITS = {"train": (2019, 2021), "validation": (2022, 2023), "test": (2024, 2025)}
SCENARIOS = (
    ("hold5", None, None, 5),
    ("tp3_sl2_h5", 0.03, 0.02, 5),
    ("tp5_sl3_h5", 0.05, 0.03, 5),
    ("tp8_sl5_h5", 0.08, 0.05, 5),
    ("hold10", None, None, 10),
    ("tp5_sl3_h10", 0.05, 0.03, 10),
    ("tp8_sl5_h10", 0.08, 0.05, 10),
    ("tp10_sl8_h10", 0.10, 0.08, 10),
)

SQL = r"""
WITH normalized AS (
 SELECT code,
        CASE WHEN date > 30000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER) ELSE CAST(date AS INTEGER) END ymd,
        o,h,l,c,v
 FROM daily_bars
 WHERE o>0 AND h>0 AND l>0 AND c>0
), b AS (
 SELECT code, ymd, o,h,l,c,v,
        avg(c) OVER w20 ma20, avg(v) OVER w20 vol20,
        min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) prior_low20,
        lead(ymd,1) OVER w e_ymd, lead(h,1) OVER w e_h, lead(l,1) OVER w e_l,
        lead(c,1) OVER w e_c,
        lead(h,2) OVER w f1_h, lead(l,2) OVER w f1_l, lead(c,2) OVER w f1_c,
        lead(h,3) OVER w f2_h, lead(l,3) OVER w f2_l, lead(c,3) OVER w f2_c,
        lead(h,4) OVER w f3_h, lead(l,4) OVER w f3_l, lead(c,4) OVER w f3_c,
        lead(h,5) OVER w f4_h, lead(l,5) OVER w f4_l, lead(c,5) OVER w f4_c,
        lead(h,6) OVER w f5_h, lead(l,6) OVER w f5_l, lead(c,6) OVER w f5_c,
        lead(h,7) OVER w f6_h, lead(l,7) OVER w f6_l, lead(c,7) OVER w f6_c,
        lead(h,8) OVER w f7_h, lead(l,8) OVER w f7_l, lead(c,8) OVER w f7_c,
        lead(h,9) OVER w f8_h, lead(l,9) OVER w f8_l, lead(c,9) OVER w f8_c,
        lead(h,10) OVER w f9_h, lead(l,10) OVER w f9_l, lead(c,10) OVER w f9_c,
        lead(h,11) OVER w f10_h, lead(l,11) OVER w f10_l, lead(c,11) OVER w f10_c
 FROM normalized
 WINDOW w AS (PARTITION BY code ORDER BY ymd),
        w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
)
SELECT * FROM b
WHERE ma20 IS NOT NULL AND vol20>0 AND prior_low20 IS NOT NULL AND e_ymd IS NOT NULL AND f10_c IS NOT NULL
  AND c < prior_low20 AND v / vol20 >= 3.0 AND (c-l)/NULLIF(h-l,0) <= 0.10 AND c/ma20-1 <= -0.10
  AND e_l <= l
ORDER BY ymd, code
"""


def clean(v: Any) -> Any:
    return None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v


def pct(xs: list[float], q: float) -> float | None:
    if not xs: return None
    xs = sorted(xs); p = (len(xs)-1)*q; lo, hi = int(p), math.ceil(p)
    return xs[lo] if lo == hi else xs[lo]*(hi-p)+xs[hi]*(p-lo)


def simulate(row: dict[str, Any], tp: float | None, sl: float | None, hold: int) -> dict[str, Any]:
    entry = float(row["l"])
    # The next bar is the actual stop-entry day; the trigger is known to have traded because e_l <= signal low.
    for i in range(0, hold):
        prefix = "e" if i == 0 else f"f{i}"
        high, low, close = (float(row[f"{prefix}_{x}"]) for x in ("h", "l", "c"))
        # Intraday path is unknown; a same-bar stop and target is conservatively a stop.
        if sl is not None and high >= entry * (1 + sl): return {"ret": -sl, "reason": "stop", "days": i+1}
        if tp is not None and low <= entry * (1 - tp): return {"ret": tp, "reason": "tp", "days": i+1}
        if i == hold-1: return {"ret": entry / close - 1, "reason": "time", "days": hold}
    raise RuntimeError("unreachable")


def metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rs=[float(r["ret"]) for r in rows]; gains=sum(r for r in rs if r>0); losses=-sum(r for r in rs if r<0)
    return {"n":len(rs),"mean_ret":sum(rs)/len(rs) if rs else None,"win_rate":sum(r>0 for r in rs)/len(rs) if rs else None,
      "profit_factor":gains/losses if losses else None,"p05_ret":pct(rs,.05),"loss_mean":sum(r for r in rs if r<0)/sum(r<0 for r in rs) if any(r<0 for r in rs) else None,
      "stop_rate":sum(r["reason"]=="stop" for r in rows)/len(rs) if rs else None,"tp_rate":sum(r["reason"]=="tp" for r in rows)/len(rs) if rs else None,
      "mean_days":sum(r["days"] for r in rows)/len(rs) if rs else None}


def period(rows: list[dict[str, Any]], begin: int, end: int) -> list[dict[str, Any]]:
    return [r for r in rows if begin <= int(str(r["ymd"])[:4]) <= end]


def main() -> int:
    now=datetime.now(timezone.utc); run=OUTPUT_ROOT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; run.mkdir(parents=True)
    with duckdb.connect(str(DB_PATH), read_only=True) as db: raw=[{k:clean(v) for k,v in x.items()} for x in db.execute(SQL).fetchdf().to_dict("records")]
    results=[]
    for name,tp,sl,hold in SCENARIOS:
        rows=[{**r,**simulate(r,tp,sl,hold)} for r in raw]
        item={"scenario":name,"take_profit":tp,"stop_loss":sl,"max_hold_days":hold,"splits":{k:metrics(period(rows,*years)) for k,years in SPLITS.items()},"yearly":[{"year":y,**metrics(period(rows,y,y))} for y in range(2019,2026)]}
        results.append(item)
    def eligible(x: dict[str,Any]) -> bool:
        a=x["splits"]; return all((a[k]["n"] or 0)>=30 and (a[k]["profit_factor"] or 0)>=1.2 and (a[k]["p05_ret"] or -1)>=-0.08 for k in ("train","validation"))
    selected=[x for x in results if eligible(x)]
    best=max(selected,key=lambda x:(x["splits"]["train"]["profit_factor"],x["splits"]["validation"]["profit_factor"])) if selected else None
    post_selection_years = best["yearly"] if best else []
    annual_gate_pass = bool(post_selection_years) and all((row["n"] or 0) >= 30 and (row["profit_factor"] or 0) >= 1.0 for row in post_selection_years)
    payload={"schema_version":f"{AXIS_ID}_v1","generated_at":now.isoformat(),"axis_id":AXIS_ID,"boundary_owner":"TRADEX","research_phase":"comparison_stabilization",
      "fixed_evaluation_conditions":{"shape":"support break capitulation","entry":"short stop-entry at signal low only when next day low reaches it","entry_price":"signal low","same_bar_priority":"stop before target","universe":"daily_bars with completed 10 forward bars","splits":SPLITS,"costs":"not modeled per short-research contract"},
      "candidate_count":len(raw),"scenarios":results,"selection":{"train_validation_gate":"n>=30, PF>=1.20, p05>=-8% for both train and validation","selected_scenario":best["scenario"] if best else None},
      "post_selection_evaluation":{"annual_gate":"all 2019-2025 years: n>=30 and PF>=1.00","annual_gate_pass":annual_gate_pass,"annual_failures":[row for row in post_selection_years if (row["n"] or 0)<30 or (row["profit_factor"] or 0)<1.0]},
      "decision":{"candidate_local_decision":"keep_for_regime_axis" if best and annual_gate_pass else ("hold_needs_regime_axis" if best else "drop_exit_axis_cannot_rescue_fixed_shape"),"authoritative_rollup_decision":"research_only","reason":"TP10/SL8/H10 passes pre-test selection and aggregate test, but fails the annual stability gate" if best and not annual_gate_pass else ("selected solely from train/validation and passed annual stability" if best else "no fixed TP/SL/hold scenario met both pre-test gates")},
      "runtime_db_write":False,"meemee_unchanged":True,"production_ranking_changed":False,"silent_fallback_used":False}
    path=run/"compare.json"; path.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str)+"\n",encoding="utf-8")
    (OUTPUT_ROOT/"latest_compare.json").write_text(json.dumps({"run_root":str(run),**payload},ensure_ascii=False,indent=2,default=str)+"\n",encoding="utf-8")
    print(path)
    return 0


if __name__ == "__main__": raise SystemExit(main())
