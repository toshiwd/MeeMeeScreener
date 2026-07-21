from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd

STOPS=[2.0,3.0,5.0,7.0,None]
def simulate(r,stop):
 e=r.o1
 if pd.isna(e) or e<=0:return None
 for d in range(1,6):
  lo=getattr(r,f"l{d}");cl=getattr(r,f"c{d}")
  if pd.notna(lo) and lo<=e*.95:return (5.0,"5%利確",d)
  if stop is not None and pd.notna(cl) and cl>=e*(1+stop/100):
   ex=getattr(r,f"o{d+1}") if d<5 else cl
   if pd.notna(ex):return (100*(e-ex)/e,"終値撤退",d+1 if d<5 else d)
 ex=r.c5
 return None if pd.isna(ex) else (100*(e-ex)/e,"5日終了",5)
def metric(g):
 return {"n":int(len(g)),"codes":int(g.code.nunique()),"mean_return_pct":float(g.return_pct.mean()),"median_return_pct":float(g.return_pct.median()),
 "win_rate":float(g.return_pct.gt(0).mean()),"target5_rate":float(g.exit_reason.eq("5%利確").mean()),"loss_rate":float(g.return_pct.lt(0).mean()),
 "severe_loss5_rate":float(g.return_pct.le(-5).mean()),"p10_return_pct":float(g.return_pct.quantile(.1)),"median_exit_day":float(g.exit_day.median())}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--ledger",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args()
 out=Path(a.output);out.mkdir(parents=True,exist_ok=False);led=pd.read_parquet(a.ledger);led=led[led.action_tier.isin(["Core","Probe"])].copy()
 bars=pd.read_parquet(a.inventory,columns=["code","bar_index","o","l","c"]);x=led[["code","ymd","bar_index","period","action_tier"]].copy()
 for k in range(1,7):
  z=bars.rename(columns={"bar_index":"fi","o":f"o{k}","l":f"l{k}","c":f"c{k}"});x["fi"]=x.bar_index+k;x=x.merge(z,on=["code","fi"],how="left",validate="many_to_one").drop(columns="fi")
 details=[];rows=[];years=[]
 for stop in STOPS:
  q=x.copy();vals=[simulate(r,stop) for r in q.itertuples(index=False)];q=q[[v is not None for v in vals]].copy();vals=[v for v in vals if v is not None]
  q["return_pct"]=[v[0] for v in vals];q["exit_reason"]=[v[1] for v in vals];q["exit_day"]=[v[2] for v in vals];q["close_stop_pct"]="none" if stop is None else str(stop);details.append(q)
  for (p,t),g in q.groupby(["period","action_tier"]):rows.append({"period":p,"action_tier":t,"close_stop_pct":q.close_stop_pct.iloc[0],**metric(g)})
  for (y,t),g in q.assign(year=q.ymd//10000).groupby(["year","action_tier"]):years.append({"year":int(y),"action_tier":t,"close_stop_pct":q.close_stop_pct.iloc[0],**metric(g)})
 d=pd.concat(details);m=pd.DataFrame(rows);yr=pd.DataFrame(years);d.to_parquet(out/"target5_stop_episode_ledger.parquet",index=False);m.to_parquet(out/"target5_stop_metrics.parquet",index=False);yr.to_parquet(out/"target5_stop_yearly_metrics.parquet",index=False)
 selected={}
 for tier in ["Core","Probe"]:
  dev=m[(m.period.eq("development"))&(m.action_tier.eq(tier))];eligible=dev[(dev.mean_return_pct>0)&(dev.severe_loss5_rate<=.10)]
  selected[tier]=None if eligible.empty else str(eligible.sort_values(["mean_return_pct","severe_loss5_rate"],ascending=[False,True]).iloc[0].close_stop_pct)
 val=[];checks={"both_tiers_selected_on_development":all(v is not None for v in selected.values())}
 for tier,stop in selected.items():
  z=m[(m.period.eq("validation"))&(m.action_tier.eq(tier))&(m.close_stop_pct.eq(stop))]
  if len(z):val+=z.to_dict("records")
  checks[f"{tier}_validation_mean_positive"]=bool(len(z)==1 and z.iloc[0].mean_return_pct>0)
  checks[f"{tier}_validation_severe_loss_le_10pct"]=bool(len(z)==1 and z.iloc[0].severe_loss5_rate<=.10)
  yy=yr[(yr.year>=2024)&(yr.action_tier.eq(tier))&(yr.close_stop_pct.eq(stop))]
  checks[f"{tier}_all_validation_years_positive"]=bool(len(yy)==3 and (yy.mean_return_pct>0).all())
 keep=all(checks.values())
 result={"schema_version":"tradex_short_target5_close_stop_v1.compare.v1","artifact_role":"authoritative_short_target5_close_stop","review_only":True,
 "fixed_conditions":{"entry":"next open","profit_target":"intraday low -5% from entry","target_priority":"touch locks win before close-stop evaluation","close_stop_candidates":[2,3,5,7,"none"],"stop_execution":"following open","horizon":"fifth close","selection":"development mean positive, severe loss <=10%, maximize mean","costs":"ignored"},
 "authoritative_result":{"selected_close_stop_pct":selected,"validation":val,"validation_years":yr[(yr.year>=2024)&yr.apply(lambda r:selected.get(r.action_tier)==str(r.close_stop_pct),axis=1)].to_dict("records"),"gate_checks":checks},
 "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int(d.exit_reason.eq("終値撤退").sum()),"selection_divergence_reason":"management only; membership unchanged"},
 "judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_target5_close_stop" if keep else "hold_target5_close_stop","authoritative_rollup_decision":"keep_target5_close_stop_v1_review_only" if keep else "hold_continue_exit_research","reason_type":"positive_yearly_and_tail_loss_gates"},
 "not_changed":["candidate membership","MeeMee","ranking","runtime DB","production logic"],"remaining_risks":["intraday target assumes executable touch","costs ignored","portfolio overlap absent"]}
 (out/"compare.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(result["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
