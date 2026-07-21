from __future__ import annotations
import argparse,gc,hashlib,json,sys
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
AXIS_ID="tradex_nikkei225_exact_multitimeframe_model_v1"
def sha(p):
 h=hashlib.sha256()
 with open(p,"rb") as f:
  for b in iter(lambda:f.read(8<<20),b""):h.update(b)
 return h.hexdigest()
def dump(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2)+"\n",encoding="utf8")
def run(daily:Path,tf:Path,tf_audit:Path,outroot:Path,resume_root:Path|None=None):
 audit=json.loads(tf_audit.read_text(encoding="utf8"));complete=json.loads((tf_audit.parent/"_ARTIFACT_COMPLETE.json").read_text(encoding="utf8"));assert complete["complete"] and audit["tf_feature_columns"]==414 and audit["contract"]["model_total_after_join"]==854 and audit["prefix_invariance"]["passed"] and audit["future_mutation"]["passed"] and Path(audit["artifact"]).resolve()==tf.resolve()
 d=pd.read_parquet(daily);dailycols=list(d.columns);t=pd.read_parquet(tf);assert not t.duplicated(["code","ymd"]).any();assert set(map(tuple,d[["code","ymd"]].to_numpy()))==set(map(tuple,t[["code","ymd"]].to_numpy()));tfcols=[c for c in t if c not in ("code","ymd")];rawcols=[c for c in tfcols if not c.endswith("_missing")];common=t[rawcols].notna().all(1);coverage={"aggregate":float(common.mean()),"codes":int(t.loc[common,"code"].nunique()),"yearly":{str(y):float(common[t.ymd//10000==y].mean()) for y in sorted((t.ymd//10000).unique())}};coverage["passed"]=coverage["aggregate"]>=.95 and coverage["codes"]>=200 and all(v>=.90 for v in coverage["yearly"].values());assert coverage["passed"]
 t["common_eligible"]=common;c=d.merge(t,on=["code","ymd"],validate="one_to_one");assert len(c)==len(d)==len(t);c=c.loc[c.pop("common_eligible")].reset_index(drop=True);stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");root=resume_root or outroot/f"{stamp}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=True);commonp=root/"common_eligible_input.parquet"
 if not commonp.exists():c.to_parquet(commonp,index=False)
 del c,d,t,common;gc.collect()
 original=base.features
 original_model=base.model
 def bounded_model(v,n=300):
  fitted=original_model(v,n);fitted.set_params(n_jobs=2);return fitted
 base.model=bounded_model
 def daily_only(frame):return original(frame[dailycols])
 base.features=daily_only
 prior=sorted((root/"daily_common_baseline").glob("*/compare.json")) if (root/"daily_common_baseline").exists() else []
 if prior:daily_dir=prior[-1].parent
 else:base.AXIS_ID="mtf_db_v1";daily_dir=base.run(commonp,root/"daily_common_baseline")
 gc.collect()
 def multi(frame):
  extra=frame[tfcols].copy();g,x=original(frame[dailycols]);assert list(extra.index)==list(x.index);z=pd.concat([x,extra],axis=1);assert z.shape[1]==854;return g,z
 base.features=multi;base.AXIS_ID="mtf_cand_v1";candidate_dir=base.run(commonp,root/"candidate")
 base.features=original;base.model=original_model
 bj=json.loads((daily_dir/"compare.json").read_text(encoding="utf8"));cj=json.loads((candidate_dir/"compare.json").read_text(encoding="utf8"));paired={}
 for h in (1,3,5,10):
  b=bj["results"].get(str(h),{});q=cj["results"].get(str(h),{});bm=b.get("frozen_general");cm=q.get("frozen_general")
  if not bm or not cm:paired[str(h)]={"status":"not_pairable_candidate_or_baseline_failed_oof_selection","baseline_decision":b.get("decision"),"candidate_decision":q.get("decision"),"decision":"drop"};continue
  delta={"brier":cm["brier"]-bm["brier"],"logloss":cm["logloss"]-bm["logloss"],"relative_brier_reduction":(bm["brier"]-cm["brier"])/bm["brier"],"ece_delta_by_class":[a-z for a,z in zip(cm["ece_by_class"],bm["ece_by_class"])]};point=(delta["brier"]<=-.002 or delta["relative_brier_reduction"]>=.01) and delta["logloss"]<0 and max(delta["ece_delta_by_class"])<=.01;paired[str(h)]={"status":"aggregate_point_comparison_only","delta":delta,"point_gate":point,"decision":"hold_requires_paired_bootstrap"}
 payload={"schema_version":AXIS_ID+".compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","source":{"daily":{"path":str(daily),"sha256":sha(daily)},"tf":{"path":str(tf),"sha256":sha(tf)},"tf_audit":{"path":str(tf_audit),"sha256":sha(tf_audit)}},"feature_contract":{"total":854,"daily":440,"weekly_raw":144,"weekly_masks":132,"monthly_raw":72,"monthly_masks":66},"common_eligible":coverage,"daily_common_baseline":str(daily_dir/"compare.json"),"candidate":str(candidate_dir/"compare.json"),"paired_incremental":paired,"decision":{"candidate_local_decision":"drop" if all(x["decision"]=="drop" for x in paired.values()) else "hold_no_keep_without_paired_bootstrap","authoritative_rollup_decision":"review_only"},"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False}}
 dump(root/"compare.json",payload);dump(root/"_ARTIFACT_COMPLETE.json",{"complete":True,"compare":str(root/"compare.json")});return root
def main():
 a=argparse.ArgumentParser();a.add_argument("--daily",type=Path,required=True);a.add_argument("--tf",type=Path,required=True);a.add_argument("--tf-audit",type=Path,required=True);a.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_exact_multitimeframe_model_v1"));a.add_argument("--resume-root",type=Path);x=a.parse_args();print(run(x.daily,x.tf,x.tf_audit,x.output_root,x.resume_root))
if __name__=="__main__":main()
