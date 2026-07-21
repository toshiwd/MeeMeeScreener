"""Require an observable prior probe state before an uptrend ceiling try-fail core."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS=tuple(range(2019,2026))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def rates(x): return {"n":int(len(x)),"codes":int(x.code.nunique()),"down_first":None if x.empty else float(x.outcome.eq("down_first").mean()),"rebound_first":None if x.empty else float(x.outcome.eq("rebound_first").mean()),"neutral":None if x.empty else float(x.outcome.str.startswith("neutral").mean())}
def main():
 p=argparse.ArgumentParser();p.add_argument("--base-events",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 f=pd.read_parquet(a.features).sort_values(["code","ymd"]);f["code"]=f.code.astype(str).str.zfill(4)
 raw=(f.c<f.ma20)&(f.bear_count5>=4)
 f["prior_probe_within5"]=raw.groupby(f.code).transform(lambda s:s.shift(1).rolling(5,min_periods=1).max()).fillna(False).astype(bool)
 candidate_ymd=f.ymd.where(raw)
 f["probe_ymd"]=candidate_ymd.groupby(f.code).transform(lambda s:s.shift(1).rolling(5,min_periods=1).max()).astype("Int64")
 e=pd.read_parquet(a.base_events);e["code"]=e.code.astype(str).str.zfill(4)
 x=e.merge(f[["code","ymd","prior_probe_within5","probe_ymd"]],on=["code","ymd"],how="left",validate="one_to_one");q=x[x.prior_probe_within5].copy()
 years={str(y):rates(q[q.year.eq(y)]) for y in YEARS};anchor=q[(q.code=="6857")&q.probe_ymd.eq(20240827)&q.ymd.eq(20240903)]
 direction=all(years[str(y)]["n"]>0 and years[str(y)]["down_first"]>years[str(y)]["rebound_first"] for y in YEARS);breadth=all(years[str(y)]["n"]>=20 for y in YEARS)
 data={"schema_version":"tradex_uptrend_try_fail_prior_probe_oos_v1.compare.v1","artifact_role":"authoritative","axis":"prior probe state before UPTREND_CEILING_TRY_FAIL_CORE","fixed_conditions":{"base_branch":"UPTREND_CEILING_TRY_FAIL_CORE unchanged","prior_probe_state":"within prior 5 trading bars: close<MA20 and bear_count5>=4","action":"CORE_CLOSE","outcome":"inherited exact fixed3 h5","years":list(YEARS),"minimum_each_year":20,"threshold_sweep":False},"year_results":years,"human_anchor":{"6857_20240827_to_20240903":{"expected_probe":20240827,"expected_core":20240903,"core_match":len(anchor)==1,"rows":anchor.where(pd.notna(anchor),None).to_dict("records")}},"observed_branching":{"base_events":int(len(x)),"retained_events":int(len(q)),"removed_events":int(len(x)-len(q)),"changed_rank_count":int(len(x)-len(q)),"selection_divergence_reason":"same-day try-fail requires an earlier observable breakdown/probe state"},"judgment":{"decision":"keep_episode_contract" if len(anchor)==1 else "drop","effectiveness_decision":"hold" if len(anchor)==1 and not(direction and breadth) else "keep" if direction and breadth else "drop","direction_pass_all_years":direction,"breadth_pass":breadth,"human_anchor_preserved":len(anchor)==1,"reason":"6857 episode path is reproduced, but branch is too sparse for effectiveness adoption" if len(anchor)==1 and not breadth else "anchor missing" if len(anchor)!=1 else "all gates pass"},"not_changed":["base try-fail candle","monthly environment","probe action sizing","other entry families","add logic","profit logic","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");q.to_parquet(a.output/"prior_probe_try_fail_core_events.parquet",index=False)
 audit={"base_events":int(len(x)),"retained_events":int(len(q)),"missing_join":int(x.prior_probe_within5.isna().sum()),"duplicates":int(q.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"review_only":True,"base_sha256":sha(a.base_events),"feature_sha256":sha(a.features)};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"years":years,"judgment":data["judgment"],"audit":audit},indent=2))
if __name__=="__main__":main()
