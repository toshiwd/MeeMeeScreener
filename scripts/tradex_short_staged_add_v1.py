from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd

def metric(g):
 return {"n":int(len(g)),"codes":int(g.code.nunique()),"add_rate":float(g.added.mean()),"mean_planned_return_pct":float(g.planned_return.mean()),
 "median_planned_return_pct":float(g.planned_return.median()),"win_rate":float(g.planned_return.gt(0).mean()),"severe_loss5_rate":float(g.planned_return.le(-5).mean()),
 "mean_deployed_fraction":float(g.deployed_fraction.mean()),"drop_capture_rate":float(g.baseline_hit5.mean())}

def main():
 ap=argparse.ArgumentParser();ap.add_argument("--ledger",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args()
 out=Path(a.output);out.mkdir(parents=True,exist_ok=False);led=pd.read_parquet(a.ledger);led=led[led.action_tier.isin(["Core","Probe"])].copy()
 bars=pd.read_parquet(a.inventory,columns=["code","bar_index","o","h","l","c"]);x=led[["code","ymd","bar_index","period","action_tier"]].merge(bars,on=["code","bar_index"],validate="many_to_one")
 for k in range(1,6):
  z=bars.rename(columns={"bar_index":"fi","o":f"o{k}","h":f"h{k}","l":f"l{k}","c":f"c{k}"});x["fi"]=x.bar_index+k;x=x.merge(z,on=["code","fi"],how="left",validate="many_to_one").drop(columns="fi")
 x=x[x.o1.notna()&x.o2.notna()&x.c5.notna()].copy();x["added"]=x.c1.lt(x.l);x["baseline_hit5"]=((x.o1-x[[f'l{k}' for k in range(1,6)]].min(axis=1))/x.o1).ge(.05)
 details=[];rows=[];years=[]
 for initial in (.25,.5,.75,1.0):
  q=x.copy();q["initial_fraction"]=initial;q["add_fraction"]=(1-initial)*q.added if initial<1 else 0.0;q["deployed_fraction"]=initial+q.add_fraction
  q["planned_return"]=100*(initial*(q.o1-q.c5)/q.o1+q.add_fraction*(q.o2-q.c5)/q.o2);details.append(q)
  for (p,t),g in q.groupby(["period","action_tier"]):rows.append({"period":p,"action_tier":t,"initial_fraction":initial,**metric(g)})
  for (y,t),g in q.assign(year=q.ymd//10000).groupby(["year","action_tier"]):years.append({"year":int(y),"action_tier":t,"initial_fraction":initial,**metric(g)})
 d=pd.concat(details);m=pd.DataFrame(rows);yr=pd.DataFrame(years);d.to_parquet(out/"staged_add_episode_ledger.parquet",index=False);m.to_parquet(out/"staged_add_metrics.parquet",index=False);yr.to_parquet(out/"staged_add_yearly_metrics.parquet",index=False)
 selected={}
 for tier in ["Core","Probe"]:
  dev=m[(m.period.eq("development"))&(m.action_tier.eq(tier))];base=dev[dev.initial_fraction.eq(1)].iloc[0]
  eligible=dev[(dev.initial_fraction.lt(1))&(dev.mean_planned_return_pct>=base.mean_planned_return_pct*.75)&(dev.severe_loss5_rate<base.severe_loss5_rate)]
  selected[tier]=None if eligible.empty else float(eligible.sort_values(["mean_planned_return_pct","severe_loss5_rate"],ascending=[False,True]).iloc[0].initial_fraction)
 val=[]
 for tier,initial in selected.items():
  if initial is not None: val+=m[(m.period.eq("validation"))&(m.action_tier.eq(tier))&m.initial_fraction.isin([initial,1.0])].to_dict("records")
 checks={"both_tiers_selected":all(v is not None for v in selected.values())}
 for tier,initial in selected.items():
  z=m[(m.period.eq("validation"))&(m.action_tier.eq(tier))].set_index("initial_fraction")
  checks[f"{tier}_validation_severe_loss_reduced"]=bool(initial is not None and z.loc[initial,"severe_loss5_rate"]<z.loc[1.0,"severe_loss5_rate"])
  checks[f"{tier}_validation_return_retained_75pct"]=bool(initial is not None and z.loc[initial,"mean_planned_return_pct"]>=z.loc[1.0,"mean_planned_return_pct"]*.75)
  checks[f"{tier}_validation_mean_positive"]=bool(initial is not None and z.loc[initial,"mean_planned_return_pct"]>0)
 keep=all(checks.values())
 result={"schema_version":"tradex_short_staged_add_v1.compare.v1","artifact_role":"authoritative_short_staged_add","review_only":True,
 "fixed_conditions":{"entry":"next open","add_signal":"entry-day close below signal-day low","add_execution":"following open","exit":"fifth-session close","initial_candidates":[.25,.5,.75,1.0],"selection":"development return retention >=75% and severe loss reduction","costs":"ignored"},
 "authoritative_result":{"selected_initial_fraction":selected,"validation":val,"validation_years":yr[(yr.year>=2024)&yr.apply(lambda r:selected.get(r.action_tier)==r.initial_fraction,axis=1)].to_dict("records"),"gate_checks":checks},
 "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int(x.added.sum()),"selection_divergence_reason":"add remaining size only after observable weakness"},
 "judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_staged_add" if keep else "hold_staged_add","authoritative_rollup_decision":"keep_staged_add_v1_review_only" if keep else "hold_continue_staged_add","reason_type":"return_retention_and_severe_loss_gates"},
 "not_changed":["candidate membership","MeeMee","ranking","runtime DB","production logic"],"remaining_risks":["fixed fifth-close exit","no portfolio overlap","costs ignored"]}
 (out/"compare.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(result["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
