from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_long_short_weekly_coverage_v1 import metrics, weekly_coverage

AXIS_ID = "tradex_riskoff_two_family_union_v1"
SUPPORT_SOURCE = Path(r"G:\Tradex\long_short_weekly_coverage_v1\20260712T044005Z-tradex_long_short_weekly_coverage_v1\combined_events.csv")
REVERSAL_SOURCE = Path(r"G:\Tradex\riskoff_capitulation_reversal_adaptive_gate_v1\20260712T065635Z-tradex_riskoff_capitulation_reversal_adaptive_gate_v1\routed_events.csv")
OUT = Path(r"G:\Tradex\riskoff_two_family_union_v1")


def run() -> Path:
    support = pd.read_csv(SUPPORT_SOURCE, parse_dates=["signal_date", "entry_date"])
    support = support[support.rule == "support_break_breadth40"].copy()
    reversal = pd.read_csv(REVERSAL_SOURCE, parse_dates=["signal_date", "entry_date"])
    support["family"] = "support_break_short"
    reversal["family"] = "capitulation_reversal_long"
    common = ["side", "code", "signal_date", "entry_date", "ret", "rule", "family"]
    union = pd.concat([support[common], reversal[common]], ignore_index=True).sort_values(["entry_date", "family", "code"])
    support_keys = set(zip(support.signal_date.dt.date, support.code.astype(str)))
    reversal_keys = set(zip(reversal.signal_date.dt.date, reversal.code.astype(str)))
    overlap = support_keys & reversal_keys
    reports = {}
    for name, start, end in [("development_2019_2025", "2019-01-01", "2025-12-31"), ("validation_2026", "2026-01-01", "2026-07-10")]:
        part = union[(union.entry_date >= start) & (union.entry_date <= end)]
        reports[name] = {
            "union_metrics": metrics(part), "weekly_coverage": weekly_coverage(part, start, end),
            "support_break_short": metrics(part[part.family == "support_break_short"]),
            "capitulation_reversal_long": metrics(part[part.family == "capitulation_reversal_long"]),
        }
    val = reports["validation_2026"]
    gate = bool((val["union_metrics"].get("daily_profit_factor") or 0)>=1.2 and (val["union_metrics"].get("daily_expectancy") or 0)>0 and (val["weekly_coverage"].get("average_events_per_calendar_week") or 0)>=1.0)
    now=datetime.now(timezone.utc); output=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True)
    union.to_csv(output/"union_events.csv",index=False)
    payload={
        "schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment",
        "fixed_evaluation_conditions":{"families":["support_break_breadth40","riskoff_capitulation_reversal_long with point-in-time active gate"],"portfolio_daily_aggregation":"equal mean return across same-day events","periods":{"development":"2019-2025","validation":"2026-01-01 through 2026-07-10"},"costs":"ignored"},
        "independence":{"support_unique_signal_code_count":len(support_keys),"reversal_unique_signal_code_count":len(reversal_keys),"same_signal_date_code_overlap_count":len(overlap),"jaccard":len(overlap)/len(support_keys|reversal_keys) if support_keys|reversal_keys else None},
        "reports":reports,
        "decision":{"candidate_local_decision":"keep" if gate else "hold","authoritative_rollup_decision":"research_only","reason_type":"riskoff_two_family_2026_union_gate_pass" if gate else "riskoff_union_gate_incomplete"},
        "future_reference_used":False,"runtime_db_write":False,"production_ranking_changed":False,"automatic_trading":False,
    }
    path=output/"compare.json"; path.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(path); return path


if __name__ == "__main__":
    run()
