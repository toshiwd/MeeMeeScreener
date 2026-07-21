"""Evaluate ordered erasure sequence stages as probe/core/add under fixed3 h5."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

YEARS=(2023,2024,2025)
STAGES={"ERASURE_PROBE":("erasure_ymd","erasure_outcome_fixed3_h5"),"GD_CORE":("gd_ymd","gd_outcome_fixed3_h5"),"MA7_RECROSS_ADD":("ma7_recross_failure_ymd","ma7_recross_outcome_fixed3_h5"),"MA20_REBREAK_CORE":("ma20_rebreak_ymd","ma20_rebreak_outcome_fixed3_h5")}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def rates(x,col):return {"n":int(len(x)),"codes":int(x.code.nunique()),"down_first":None if x.empty else float(x[col].eq("down_first").mean()),"rebound_first":None if x.empty else float(x[col].eq("rebound_first").mean()),"neutral":None if x.empty else float(x[col].str.startswith("neutral").mean())}
def main():
 p=argparse.ArgumentParser();p.add_argument("--sequence-ledger",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False);x=pd.read_parquet(a.sequence_ledger);x=x[x.year.isin(YEARS)].copy()
 results={};families={}
 for stage,(date_col,out_col) in STAGES.items():
  results[stage]={}
  for y in YEARS:
   eligible=x.year.eq(y)&x[date_col].notna()&x[out_col].notna()
   if stage in {"MA7_RECROSS_ADD","MA20_REBREAK_CORE"}:eligible=eligible&x.gd_ymd.notna()&(x[date_col]>x.gd_ymd)
   z=x[eligible];c=rates(z,out_col);c["end_to_end_from_erasure_down"]=float(z[out_col].eq("down_first").sum()/max(1,(x.year.eq(y)).sum()));results[stage][str(y)]=c
  families[stage]={}
  for env in sorted(x.environment.dropna().unique()):
   families[stage][str(env)]={}
   for y in YEARS:
    eligible=x.year.eq(y)&x.environment.eq(env)&x[date_col].notna()&x[out_col].notna()
    if stage in {"MA7_RECROSS_ADD","MA20_REBREAK_CORE"}:eligible=eligible&x.gd_ymd.notna()&(x[date_col]>x.gd_ymd)
    families[stage][str(env)][str(y)]=rates(x[eligible],out_col)
 gates={}
 for stage,ys in results.items():
  breadth=all(ys[str(y)]["n"]>=30 for y in YEARS);positive=breadth and all(ys[str(y)]["down_first"]>ys[str(y)]["rebound_first"] for y in YEARS);gates[stage]={"breadth_pass":breadth,"down_exceeds_rebound_all_years":positive,"decision":"keep" if positive else "drop"}
 passing=[s for s,v in gates.items() if v["decision"]=="keep"]
 anchors={"2802":{"erasure_probe":"down_first","gd_core":"rebound_first"},"6532":{"erasure_probe":"down_first","gd_core":"down_first","ma7_recross_add":"down_first"}}
 data={"schema_version":"tradex_ordered_erasure_sequence_stage_oos_v1.compare.v2","artifact_role":"authoritative","axis":"ordered sequence stage action mapping","fixed_conditions":{"sequence_ledger":str(a.sequence_ledger),"action_mapping":{"ERASURE_PROBE":"erasure close","GD_CORE":"first >=0.5% GD within three bars","MA7_RECROSS_ADD":"bearish recross below MA7 strictly after a GD stage","MA20_REBREAK_CORE":"bearish recross below MA20 strictly after a GD stage"},"outcome":"exact OHLC symmetric fixed 3 percent h5","years":list(YEARS),"minimum_each_year":30,"costs":"ignored per project rule"},"year_stage_results":results,"environment_stage_results":families,"stage_gates":gates,"human_anchor_outcomes":anchors,"observed_branching":{"sequence_events":int(len(x)),"stages_compared":len(STAGES),"passing_stage_count":len(passing),"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":len(STAGES),"selection_divergence_reason":"one ordered sequence is evaluated at distinct probe/core/add stage dates with prior-stage integrity"},"judgment":{"decision":"keep" if passing else "hold","passing_stages":passing,"reason":"stage action is retained only when down-first exceeds rebound-first with >=30 events in every OOS year"},"not_changed":["sequence definition","stage dates","monthly environment","existing lifecycle","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"sequences":int(len(x)),"duplicate_sequences":int(x.duplicated(["code","erasure_ymd"]).sum()),"missing_outcomes":{s:int(x[d].notna().sum()-x[o].notna().sum()) for s,(d,o) in STAGES.items()},"future_used_for_selection":False,"future_used_for_outcome_only":True,"review_only":True,"source_sha256":sha(a.sequence_ledger)};(a.output/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"results":results,"gates":gates,"judgment":data["judgment"]},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
