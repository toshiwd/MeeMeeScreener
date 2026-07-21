from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
from scripts.tradex_long_short_weekly_coverage_v1 import metrics,weekly_coverage

AXIS_ID="tradex_adaptive_no_momentum_stress_v2"
BASE=Path(r"G:\Tradex\adaptive_rule_router_v1\20260712T063952Z-tradex_adaptive_rule_router_v1\routed_events.csv")
RISK=Path(r"G:\Tradex\riskoff_capitulation_rolling_permission_v1\20260712T065917Z-tradex_riskoff_capitulation_rolling_permission_v1\routed_events.csv")
MIXED=Path(r"G:\Tradex\mixed_two_family_union_v1\20260712T070008Z-tradex_mixed_two_family_union_v1\union_events.csv")
OUT=Path(r"G:\Tradex\adaptive_no_momentum_stress_v2")


def run()->Path:
    base=pd.read_csv(BASE,parse_dates=["signal_date","entry_date"])
    base=base[(base.entry_date>="2026-01-01")&(base.entry_date<="2026-07-10")&~base.rule.str.contains("momentum",case=False,na=False)].copy()
    risk=pd.read_csv(RISK,parse_dates=["signal_date","entry_date"]); risk=risk[(risk.entry_date>="2026-01-01")&(risk.entry_date<="2026-07-10")].copy()
    mixed=pd.read_csv(MIXED,parse_dates=["signal_date","entry_date"]); mixed=mixed[(mixed.entry_date>="2026-01-01")&(mixed.entry_date<="2026-07-10")&(mixed.rule=="volatility_contraction_breakout")].copy()
    cols=["side","code","signal_date","entry_date","ret","rule"]
    union=pd.concat([base[cols],risk[cols],mixed[cols]],ignore_index=True).drop_duplicates(["rule","code","entry_date"]).sort_values(["entry_date","rule","code"])
    m=metrics(union); coverage=weekly_coverage(union,"2026-01-01","2026-07-10")
    gate=bool((m.get("daily_profit_factor") or 0)>=1.2 and (m.get("daily_expectancy") or 0)>0 and (coverage.get("average_events_per_calendar_week") or 0)>=1.0)
    now=datetime.now(timezone.utc); output=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True); union.to_csv(output/"stress_events.csv",index=False)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment",
      "fixed_evaluation_conditions":{"period":"2026-01-01 through 2026-07-10","momentum_rules":"disabled","base_router":"active-only top3 routed events","added_active_families":["riskoff_capitulation_reversal_long rolling permission","mixed volatility_contraction_breakout"],"same_day_aggregation":"equal mean return","costs":"ignored"},
      "metrics":m,"weekly_coverage":coverage,"rule_counts":union.rule.value_counts().to_dict(),
      "adoption_gate":{"daily_pf_gte_1_2":bool((m.get("daily_profit_factor") or 0)>=1.2),"daily_expectancy_positive":bool((m.get("daily_expectancy") or 0)>0),"average_events_per_week_gte_1":bool((coverage.get("average_events_per_calendar_week") or 0)>=1.0),"pass":gate},
      "decision":{"candidate_local_decision":"keep" if gate else "hold","authoritative_rollup_decision":"research_only","reason_type":"no_momentum_stress_gate_pass" if gate else "no_momentum_stress_gate_failed"},
      "future_reference_used":False,"runtime_db_write":False,"production_ranking_changed":False,"automatic_trading":False}
    path=output/"compare.json"; path.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(path); return path


if __name__=="__main__": run()
