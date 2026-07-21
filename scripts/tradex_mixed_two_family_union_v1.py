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

AXIS_ID="tradex_mixed_two_family_union_v1"
SOURCE=Path(r"G:\Tradex\adaptive_rule_router_v1\20260712T063952Z-tradex_adaptive_rule_router_v1\unified_rule_event_ledger.csv")
OUT=Path(r"G:\Tradex\mixed_two_family_union_v1")
RULES={"ma60_weekly_reversal","volatility_contraction_breakout"}


def run()->Path:
    frame=pd.read_csv(SOURCE,parse_dates=["signal_date","entry_date"])
    frame=frame[(frame.regime=="mixed")&frame.rule.isin(RULES)].copy().sort_values(["entry_date","rule","code"])
    groups={rule:frame[frame.rule==rule].copy() for rule in RULES}
    keys={rule:set(zip(g.signal_date.dt.date,g.code.astype(str))) for rule,g in groups.items()}
    overlap=keys["ma60_weekly_reversal"]&keys["volatility_contraction_breakout"]
    reports={}
    for name,start,end in [("development_2019_2025","2019-01-01","2025-12-31"),("validation_2026","2026-01-01","2026-07-10")]:
        part=frame[(frame.entry_date>=start)&(frame.entry_date<=end)]
        reports[name]={"union_metrics":metrics(part),"weekly_coverage":weekly_coverage(part,start,end),"families":{rule:metrics(part[part.rule==rule]) for rule in sorted(RULES)}}
    val=reports["validation_2026"]
    keep=bool(all((val["families"][r].get("event_count") or 0)>=5 and (val["families"][r].get("daily_profit_factor") or 0)>=1.2 and (val["families"][r].get("daily_expectancy") or 0)>0 for r in RULES) and (val["union_metrics"].get("daily_profit_factor") or 0)>=1.2)
    now=datetime.now(timezone.utc); output=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True); frame.to_csv(output/"union_events.csv",index=False)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","source_artifact":str(SOURCE),
      "fixed_evaluation_conditions":{"regime":"mixed breadth above MA20 >45% and <60%","families":sorted(RULES),"shape_and_exit":"unchanged from source artifacts","costs":"ignored"},
      "independence":{"same_signal_date_code_overlap_count":len(overlap),"jaccard":len(overlap)/len(keys["ma60_weekly_reversal"]|keys["volatility_contraction_breakout"]) if keys["ma60_weekly_reversal"]|keys["volatility_contraction_breakout"] else None},
      "reports":reports,"decision":{"candidate_local_decision":"keep" if keep else "hold","authoritative_rollup_decision":"research_only","reason_type":"mixed_two_family_2026_gate_pass" if keep else "mixed_two_family_gate_incomplete"},
      "runtime_db_write":False,"production_ranking_changed":False,"automatic_trading":False}
    path=output/"compare.json"; path.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(path); return path


if __name__=="__main__":
    run()
