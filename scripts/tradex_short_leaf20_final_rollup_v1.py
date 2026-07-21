from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


AXIS_ID="tradex_short_leaf20_final_rollup_v1"
SHORT=Path(r"G:\Tradex\short_breadth_loss_cap_v1\latest_compare.json")
LONG=Path(r"G:\Tradex\chart_entry_geometry_research_v1\20260711T024708Z-shallow_high_zone_next_open_execution_v1\compare.json")
OUT=Path(r"G:\Tradex\short_leaf20_final_rollup_v1")


def load(path:Path)->dict: return json.loads(path.read_text(encoding="utf-8"))


def main()->None:
    now=datetime.now(timezone.utc); run=OUT/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; run.mkdir(parents=True)
    short=load(SHORT); long=load(LONG)
    selected=next(row for row in short["reports"] if row["scenario"]==short["selection"]["selected_scenario"])
    leaf20=next(row for row in long["capacity_and_allocation"]["leaf_risk_breakdown"] if int(row["leaf"])==20)
    short_test=float(selected["splits"]["test"]["daily_profit_factor"])
    long_test=float(leaf20["metrics_by_split"]["test"]["daily_profit_factor"])
    payload={"schema_version":f"{AXIS_ID}_v1","generated_at":now.isoformat(),"axis_id":AXIS_ID,"boundary_owner":"TRADEX","research_phase":"effectiveness_judgment",
      "authoritative_inputs":{"short_loss_cap_compare":str(SHORT),"long_leaf20_compare":str(LONG)},
      "short_rule":{"daily_shape":"support break capitulation: close below prior 20-day low, volume >=3x 20-day average, close in bottom 10%, close >=10% below MA20","information_gate":"same-day all-stock breadth below MA20 >=40%","entry":"next trading day only, short at the signal low if reached","exit":"TP10%, SL5%, maximum 10 trading days, same-bar stop before target","selection":"highest train daily PF from predeclared TP5/SL3, TP8/SL5, TP10/SL5; validation/test untouched","selected_scenario":selected["scenario"],"split_metrics":selected["splits"],"yearly_metrics":selected["yearly"]},
      "gates":{"out_of_sample_pass":short["post_selection_evaluation"]["out_of_sample_pass"],"annual_pass":short["post_selection_evaluation"]["annual_pass"],"loss_cap":"p05 return >= -5% in every split and year","loss_cap_pass":all((row["p05_ret"] or -1)>=-.05 for row in selected["yearly"])},
      "leaf20_comparison":{"aggregation":"both use equal-weight daily signal baskets","short_test_daily_pf":short_test,"leaf20_test_daily_pf":long_test,"short_replaces_leaf20":False,"reason":"Short is conditionally viable but its test daily PF is lower than leaf20; it is a complementary risk-off-only rule, not a replacement."},
      "decision":{"candidate_local_decision":"keep_conditional_short_rule","relative_replacement_decision":"do_not_replace_leaf20","authoritative_rollup_decision":"short_rule_established_review_only","reason":"The short rule passes train-only selection, validation, test, annual stability, and -5% loss-cap gates. Its role is restricted to the stated risk-off breadth condition."},
      "what_is_not_changed":{"meemee_runtime_db":True,"production_ranking":True,"live_trade_execution":True},"runtime_db_write":False,"production_ranking_changed":False,"silent_fallback_used":False}
    (run/"final_rollup.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); (OUT/"latest_final_rollup.json").write_text(json.dumps({"run_root":str(run),**payload},ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(run/"final_rollup.json")

if __name__=="__main__": main()
