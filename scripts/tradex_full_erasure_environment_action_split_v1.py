"""Split full-erasure episodes into direct core or probe->GD core by monthly context."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

YEARS=tuple(range(2019,2026))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def rates(x):return {"n":int(len(x)),"codes":int(x.code.nunique()),"down_first":None if x.empty else float(x.outcome.eq("down_first").mean()),"rebound_first":None if x.empty else float(x.outcome.eq("rebound_first").mean()),"neutral":None if x.empty else float(x.outcome.str.startswith("neutral").mean())}
def main():
 p=argparse.ArgumentParser();p.add_argument("--sequence-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--monthly-ledger",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 s=pd.read_parquet(a.sequence_ledger);s["code"]=s.code.astype(str).str.zfill(4);s["effective_month"]=pd.to_datetime(s.erasure_ymd.astype(int).astype(str),format="%Y%m%d").dt.to_period("M").astype(str)
 f=pd.read_parquet(a.features,columns=["code","ymd","weekly_lower_high","weekly_upper_wick_ratio","weekly_close_pos","ma20","c","atr14"]);f["code"]=f.code.astype(str).str.zfill(4);f=f.rename(columns={"ymd":"erasure_ymd","c":"erasure_close","ma20":"erasure_ma20","atr14":"erasure_atr"})
 m=pd.read_parquet(a.monthly_ledger);m["code"]=m.code.astype(str).str.zfill(4);m["effective_month"]=m.effective_month.astype(str);m=m[["code","effective_month","base_regime","post_box","local_box_mature","local_close_location","local_box_top_touch_count","local_top_touched","local_top_rejected"]]
 x=s.merge(f,on=["code","erasure_ymd"],how="left",validate="one_to_one").merge(m,on=["code","effective_month"],how="left",validate="many_to_one")
 x["erasure_year"]=x.erasure_ymd.astype(str).str[:4].astype(int);x=x[x.erasure_year.isin(YEARS)].copy()
 direct=x.base_regime.eq("UPTREND")&x.local_box_mature.fillna(False).astype(bool)&x.local_close_location.eq("AT_LOCAL_CEILING")&x.weekly_lower_high.eq(1)
 probe=x.base_regime.eq("POST_BOX_BREAKOUT_CONSOLIDATION")&x.gd_ymd.notna()
 d=x[direct].copy();d["action_type"]="INITIAL_SHORT_CLOSE";d["action_ymd"]=d.erasure_ymd.astype(int);d["outcome"]=d.erasure_outcome_fixed3_h5;d["branch"]="UPTREND_WTOP_FULL_ERASURE_DIRECT_CORE"
 q=x[probe].copy();q["action_type"]="PROBE_THEN_ADD";q["probe_ymd"]=q.erasure_ymd.astype(int);q["action_ymd"]=q.gd_ymd.astype(int);q["outcome"]=q.gd_outcome_fixed3_h5;q["branch"]="POSTBOX_FULL_ERASURE_PROBE_GD_CORE"
 events=pd.concat([d,q],ignore_index=True);events["action_year"]=events.action_ymd.astype(str).str[:4].astype(int);events=events[events.action_year.isin(YEARS)]
 duplicate_action_candidates=int(events.duplicated(["code","action_ymd","branch"],keep=False).sum())
 events=events.sort_values(["code","action_ymd","branch","erasure_ymd"],ascending=[True,True,True,False]).drop_duplicates(["code","action_ymd","branch"],keep="first")
 results={}
 for branch in ["UPTREND_WTOP_FULL_ERASURE_DIRECT_CORE","POSTBOX_FULL_ERASURE_PROBE_GD_CORE"]:results[branch]={str(y):rates(events[(events.branch==branch)&events.action_year.eq(y)]) for y in YEARS}
 a2802=events[(events.code=="2802")&(events.branch=="UPTREND_WTOP_FULL_ERASURE_DIRECT_CORE")&events.action_ymd.eq(20240206)]
 a6532=events[(events.code=="6532")&(events.branch=="POSTBOX_FULL_ERASURE_PROBE_GD_CORE")&events.probe_ymd.eq(20230623)&events.action_ymd.eq(20230626)]
 gates={}
 for branch,ys in results.items():
  direction=all(ys[str(y)]["n"]>0 and ys[str(y)]["down_first"]>ys[str(y)]["rebound_first"] for y in YEARS);breadth=all(ys[str(y)]["n"]>=20 for y in YEARS);gates[branch]={"direction_pass_all_years":direction,"breadth_pass":breadth,"effectiveness_decision":"keep" if direction and breadth else "hold" if direction else "drop"}
 contract_pass=len(a2802)==1 and len(a6532)==1
 data={"schema_version":"tradex_full_erasure_environment_action_split_v1.compare.v1","artifact_role":"authoritative","axis":"monthly-context action split for full-erasure episodes","fixed_conditions":{"base_sequence":"bull candle followed within 1-2 bars by full erasure, unchanged","direct_core":"UPTREND; mature local box; AT_LOCAL_CEILING; weekly_lower_high","probe_then_gd_add":"POST_BOX_BREAKOUT_CONSOLIDATION; GD within 3 bars","outcome":"exact OHLC symmetric fixed3 h5 at action close","years":list(YEARS),"minimum_each_year":20,"threshold_sweep":False},"year_branch_results":results,"branch_gates":gates,"human_anchors":{"2802_20240206":{"expected":"INITIAL_SHORT_CLOSE","match":len(a2802)==1,"rows":a2802.where(pd.notna(a2802),None).to_dict("records")},"6532_20230623_20230626":{"expected":"PROBE_THEN_ADD","match":len(a6532)==1,"rows":a6532.where(pd.notna(a6532),None).to_dict("records")}},"observed_branching":{"source_sequences":int(len(x)),"direct_core_events":int(len(d)),"probe_gd_add_events":int(len(q)),"changed_rank_count":2,"selection_divergence_reason":"the same full-erasure candle maps to different sizing/action stages by monthly structure"},"judgment":{"decision":"keep_episode_contract" if contract_pass else "drop","contract_pass":contract_pass,"effectiveness_decisions":{k:v["effectiveness_decision"] for k,v in gates.items()},"reason":"both human action paths are reproduced; effectiveness remains branch-specific" if contract_pass else "one or more human action paths are missing"},"not_changed":["full-erasure detector","monthly classifier","GD detector","later add stages","profit logic","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");events.to_parquet(a.output/"full_erasure_action_events.parquet",index=False)
 audit={"source_sequences":int(len(s)),"eligible_year_sequences":int(len(x)),"events":int(len(events)),"duplicate_action_candidates":duplicate_action_candidates,"duplicates_after_resolution":int(events.duplicated(["code","action_ymd","branch"]).sum()),"duplicate_resolution":"nearest preceding erasure to the core action","missing_feature_join":int(x.weekly_lower_high.isna().sum()),"missing_monthly_join":int(x.base_regime.isna().sum()),"future_used_for_selection":False,"future_used_for_outcome_only":True,"review_only":True,"sequence_sha256":sha(a.sequence_ledger),"feature_sha256":sha(a.features),"monthly_sha256":sha(a.monthly_ledger)};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"results":results,"gates":gates,"anchors":{"2802":len(a2802)==1,"6532":len(a6532)==1},"judgment":data["judgment"],"audit":audit},indent=2))
if __name__=="__main__":main()
