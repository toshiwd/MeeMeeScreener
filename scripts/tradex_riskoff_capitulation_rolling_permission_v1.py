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

AXIS_ID = "tradex_riskoff_capitulation_rolling_permission_v1"
SOURCE = Path(r"G:\Tradex\riskoff_capitulation_reversal_long_v1\20260712T065548Z-tradex_riskoff_capitulation_reversal_long_v1\events.csv")
OUT = Path(r"G:\Tradex\riskoff_capitulation_rolling_permission_v1")


def run() -> Path:
    events = pd.read_csv(SOURCE, parse_dates=["signal_date", "entry_date"]).sort_values(["entry_date", "code"]).reset_index(drop=True)
    events["outcome_known_date"] = events.entry_date + pd.Timedelta(days=20)
    selected, audit = [], []
    for row in events.itertuples(index=False):
        start = row.signal_date - pd.DateOffset(years=4)
        known = events[(events.outcome_known_date < row.signal_date) & (events.entry_date >= start)]
        m = metrics(known)
        allowed = bool((m.get("event_count") or 0)>=30 and (m.get("daily_profit_factor") or 0)>=1.2 and (m.get("daily_expectancy") or 0)>0)
        audit.append({"signal_date":row.signal_date,"code":str(row.code),"known_start":start,"known_n":m.get("event_count",0),"known_daily_profit_factor":m.get("daily_profit_factor"),"known_daily_expectancy":m.get("daily_expectancy"),"allowed":allowed})
        if allowed:
            selected.append(row._asdict())
    routed = pd.DataFrame(selected)
    reports = {}
    for name,start,end in [("development_2019_2025","2019-01-01","2025-12-31"),("validation_2026","2026-01-01","2026-07-10")]:
        part=routed[(routed.entry_date>=start)&(routed.entry_date<=end)] if not routed.empty else routed
        yearly=[{"year":int(y),**metrics(part[part.entry_date.dt.year==y])} for y in sorted(part.entry_date.dt.year.unique())] if not part.empty else []
        reports[name]={"metrics":metrics(part),"weekly_coverage":weekly_coverage(part,start,end),"yearly":yearly}
    val=reports["validation_2026"]["metrics"]
    keep=bool((val.get("event_count") or 0)>=5 and (val.get("daily_profit_factor") or 0)>=1.2 and (val.get("daily_expectancy") or 0)>0)
    now=datetime.now(timezone.utc); output=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; output.mkdir(parents=True)
    routed.to_csv(output/"routed_events.csv",index=False); pd.DataFrame(audit).to_csv(output/"point_in_time_permission_audit.csv",index=False)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","source_artifact":str(SOURCE),
      "fixed_evaluation_conditions":{"shape_and_exit":"unchanged","permission_window":"trailing four years","outcome_availability":"entry plus 20 calendar days strictly before signal","permission_gate":"n>=30 daily PF>=1.2 daily expectancy>0","costs":"ignored"},
      "reports":reports,"decision":{"candidate_local_decision":"keep" if keep else "drop","authoritative_rollup_decision":"research_only","reason_type":"rolling_permission_2026_gate_pass" if keep else "rolling_permission_gate_failed"},
      "future_reference_used":False,"runtime_db_write":False,"production_ranking_changed":False,"automatic_trading":False}
    path=output/"compare.json"; path.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(path); return path


if __name__=="__main__":
    run()
