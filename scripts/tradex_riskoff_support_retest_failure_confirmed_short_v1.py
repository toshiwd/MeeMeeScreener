from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_long_short_weekly_coverage_v1 import metrics, weekly_coverage


AXIS_ID = "tradex_riskoff_support_retest_failure_confirmed_short_v1"
OUT = Path(r"G:\Tradex\riskoff_support_retest_failure_confirmed_short_v1")


def run() -> Path:
    sys.path.insert(0, "app")
    from backend.services.codex_bridge_service import get_runtime_stock_db_status

    runtime = get_runtime_stock_db_status()
    db_path = Path(runtime["selected_runtime_db_path"])
    leads = ",".join(
        [
            f"lead(o,{i}) over w o{i},lead(h,{i}) over w h{i},lead(l,{i}) over w l{i},"
            f"lead(c,{i}) over w c{i},lead(date,{i}) over w d{i}"
            for i in range(1, 11)
        ]
    )
    sql = f"""
    WITH b AS (
      SELECT code,date,o,h,l,c,v,row_number() OVER w rn,
        min(l) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) prior_low20,
        avg(c) OVER(PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) ma20,
        {leads}
      FROM daily_bars WHERE source='pan'
      WINDOW w AS(PARTITION BY code ORDER BY date)
    ), breaks AS (
      SELECT code,date break_date,rn break_rn,prior_low20 broken_support
      FROM b WHERE prior_low20 IS NOT NULL AND c<prior_low20
    ), breadth AS (
      SELECT date,avg(CASE WHEN c>ma20 THEN 1.0 ELSE 0.0 END) breadth_above_ma20
      FROM b WHERE ma20 IS NOT NULL GROUP BY date
    ), pairs AS (
      SELECT s.*,k.break_date,k.broken_support,breadth_above_ma20,
        row_number() OVER(PARTITION BY s.code,s.date ORDER BY k.break_rn DESC) anchor_rank
      FROM b s JOIN breaks k ON s.code=k.code AND s.rn-k.break_rn BETWEEN 1 AND 5
      JOIN breadth ON breadth.date=s.date
      WHERE s.d10 IS NOT NULL AND s.h>=k.broken_support*.98 AND s.h<=k.broken_support*1.03
        AND s.c<k.broken_support AND s.c<s.o AND (s.c-s.l)/nullif(s.h-s.l,0)<=.40
        AND breadth_above_ma20<=.45
    ), signals AS (
      SELECT *,row_number() OVER(PARTITION BY date ORDER BY (c-l)/nullif(h-l,0),code) day_rank
      FROM pairs WHERE anchor_rank=1
    )
    SELECT * FROM signals WHERE day_rank<=3 ORDER BY date,day_rank,code
    """
    with duckdb.connect(str(db_path), read_only=True) as db:
        raw = db.execute(sql).fetchdf()

    rows = []
    for item in raw.to_dict("records"):
        trigger = float(item["l"])
        if float(item["l1"]) > trigger:
            continue
        # A gap below the trigger cannot be filled at the higher trigger price.
        entry = min(trigger, float(item["o1"]))
        ret, exit_offset = None, 10
        for offset in range(1, 11):
            if float(item[f"h{offset}"]) >= entry * 1.05:
                ret, exit_offset = -.05, offset
                break
            if float(item[f"l{offset}"]) <= entry * .90:
                ret, exit_offset = .10, offset
                break
        if ret is None:
            ret = 1 - float(item["c10"]) / entry
        rows.append(
            {
                "side": "sell",
                "code": str(item["code"]),
                "signal_date": pd.to_datetime(int(item["date"]), unit="s"),
                "entry_date": pd.to_datetime(int(item["d1"]), unit="s"),
                "ret": ret,
                "rule": "riskoff_support_retest_failure_confirmed_short",
                "regime": "risk_off",
                "exit_offset": exit_offset,
                "breadth_above_ma20": float(item["breadth_above_ma20"]),
                "broken_support": float(item["broken_support"]),
                "trigger": trigger,
                "entry": entry,
                "gap_below_trigger": bool(float(item["o1"]) < trigger),
            }
        )
    events = pd.DataFrame(rows)
    reports = {}
    for name, start, end in [
        ("development_2019_2025", "2019-01-01", "2025-12-31"),
        ("validation_2026", "2026-01-01", "2026-07-10"),
    ]:
        part = events[(events.entry_date >= start) & (events.entry_date <= end)]
        yearly = []
        for year in sorted(part.entry_date.dt.year.unique()):
            year_part = part[part.entry_date.dt.year == year]
            yearly.append({"year": int(year), **metrics(year_part)})
        reports[name] = {
            "metrics": metrics(part),
            "weekly_coverage": weekly_coverage(part, start, end),
            "yearly": yearly,
        }
    dev, val = reports["development_2019_2025"]["metrics"], reports["validation_2026"]["metrics"]
    keep = bool(
        (dev.get("event_count") or 0) >= 100
        and (dev.get("daily_profit_factor") or 0) >= 1.2
        and (dev.get("daily_expectancy") or 0) > 0
        and (val.get("event_count") or 0) >= 5
        and (val.get("daily_profit_factor") or 0) >= 1.2
        and (val.get("daily_expectancy") or 0) > 0
    )
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    events.to_csv(output / "events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "anchor": "close below prior20 low",
            "signal": "1-5 sessions later retest old support -2%/+3%, close back below, bearish bottom40%",
            "regime": "breadth above MA20 <=45%",
            "entry": "next session signal-low break; fill=min(signal low,next open) to penalize gap-down; no entry without break",
            "exit": "short TP10 SL5 H10 stop-first including entry day",
            "ranking": "bottom close position top3/day before entry confirmation",
            "costs": "ignored",
        },
        "reports": reports,
        "decision": {
            "candidate_local_decision": "keep" if keep else "drop",
            "authoritative_rollup_decision": "research_only",
            "reason_type": "confirmed_entry_gate_pass" if keep else "fixed_gate_failed",
        },
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "automatic_trading": False,
    }
    path = output / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
