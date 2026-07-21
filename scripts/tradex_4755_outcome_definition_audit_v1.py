"""Audit whether the 4755 branch failure is specific to the fixed3 outcome definition."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

YEARS=tuple(range(2019,2026))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument("--events",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False);x=pd.read_parquet(a.events)
 years={}
 for y in YEARS:
  z=x[x.year.eq(y)];years[str(y)]={"n":int(len(z)),"fixed3_down_first":float(z.outcome.eq("down_first").mean()),"fixed3_rebound_first":float(z.outcome.eq("rebound_first").mean()),"close5_down_rate":float(z.ret_close_5.lt(0).mean()),"close5_mean_return":float(z.ret_close_5.mean()),"mean_down_excursion5":float(z.down_exc_5.mean()),"mean_up_excursion5":float(z.up_exc_5.mean())}
 anchor=x[(x.code.astype(str).str.zfill(4)=="4755")&x.ymd.eq(20251114)]
 fixed_fail=any(years[str(y)]["fixed3_down_first"]<=years[str(y)]["fixed3_rebound_first"] for y in YEARS);close_fail=any(years[str(y)]["close5_down_rate"]<=.5 for y in YEARS)
 data={"schema_version":"tradex_4755_outcome_definition_audit_v1.compare.v1","artifact_role":"authoritative_diagnostic","axis":"fixed3-first-hit versus five-day close direction","fixed_conditions":{"events":"POSTBOX_HIGH_FAILURE_SUPPORT_BREAK_DIRECT_CORE unchanged","fixed3":"inherited","secondary_metrics":"ret_close_5, down_exc_5, up_exc_5; diagnostic only","selection_change":"none","years":list(YEARS)},"year_results":years,"human_anchor":{"4755_20251114":{"fixed3":None if anchor.empty else anchor.outcome.iloc[0],"ret_close_5":None if anchor.empty else float(anchor.ret_close_5.iloc[0]),"down_exc_5":None if anchor.empty else float(anchor.down_exc_5.iloc[0]),"up_exc_5":None if anchor.empty else float(anchor.up_exc_5.iloc[0])}},"judgment":{"decision":"selector_failure_not_metric_artifact" if fixed_fail and close_fail else "hold","fixed3_gate_fails":fixed_fail,"close5_direction_also_fails":close_fail,"reason":"the branch fails both first-hit and five-day close-direction tests; changing the outcome definition would not rescue it"},"not_changed":["events","thresholds","human anchor","other branches","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"events":int(len(x)),"duplicates":int(x.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"review_only":True,"source_sha256":sha(a.events)};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"years":years,"anchor":data["human_anchor"],"judgment":data["judgment"],"audit":audit},indent=2))
if __name__=="__main__":main()
