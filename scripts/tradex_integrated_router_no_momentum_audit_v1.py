from __future__ import annotations

import json,sys
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
from scripts.tradex_long_short_weekly_coverage_v1 import metrics,weekly_coverage

AXIS_ID="tradex_integrated_router_no_momentum_audit_v1"
SOURCE_ROOT=Path(r"G:\Tradex\adaptive_rule_router_v1")
OUT=Path(r"G:\Tradex\integrated_router_no_momentum_audit_v1")

def run()->Path:
    sources=sorted(SOURCE_ROOT.glob("*/routed_events.csv"),key=lambda p:p.stat().st_mtime)
    if not sources: raise FileNotFoundError(f"routed_events.csv not found under {SOURCE_ROOT}")
    source=sources[-1]
    frame=pd.read_csv(source,parse_dates=["entry_date"])
    frame=frame[(frame.entry_date>="2026-01-01")&(frame.entry_date<="2026-07-10")&~frame.rule.str.contains("momentum",case=False,na=False)].copy()
    m=metrics(frame); w=weekly_coverage(frame,"2026-01-01","2026-07-10")
    gate=bool((m.get("daily_profit_factor") or 0)>=1.2 and (m.get("daily_expectancy") or 0)>0 and (w.get("average_events_per_calendar_week") or 0)>=1.0)
    now=datetime.now(timezone.utc); output=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True); frame.to_csv(output/"stress_events.csv",index=False)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","source_artifact":str(source),"fixed_evaluation_conditions":{"source":"integrated adaptive router routed events only","period":"2026-01-01 through 2026-07-10","stress":"exclude every rule containing momentum","costs":"ignored"},"metrics":m,"weekly_coverage":w,"rule_counts":frame.rule.value_counts().to_dict(),"adoption_gate":{"daily_pf_gte_1_2":bool((m.get("daily_profit_factor") or 0)>=1.2),"daily_expectancy_positive":bool((m.get("daily_expectancy") or 0)>0),"average_events_per_week_gte_1":bool((w.get("average_events_per_calendar_week") or 0)>=1.0),"pass":gate},"decision":{"candidate_local_decision":"keep" if gate else "hold","authoritative_rollup_decision":"research_only","reason_type":"integrated_no_momentum_gate_pass" if gate else "integrated_no_momentum_gate_failed"},"future_reference_used":False,"runtime_db_write":False,"production_ranking_changed":False,"automatic_trading":False}
    path=output/"compare.json"; path.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(path); return path

if __name__=="__main__": run()
