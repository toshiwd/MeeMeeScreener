from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd

INITIALS=[.25,.5,.75]
def pnl(i,a,o1,o2,px):return 100*(i*(o1-px)/o1+a*(o2-px)/o2)
def sim(r,i):
 if pd.isna(r.o1) or pd.isna(r.o2):return None
 if r.l1<=r.o1*.95:return (i*5,"初期玉5%利確",1,False)
 confirm=r.c1<r.signal_low
 if not confirm:return (100*i*(r.o1-r.o2)/r.o1,"弱化なし撤退",2,False)
 a=1-i;avg=1/(i/r.o1+a/r.o2)
 for d in range(2,6):
  if getattr(r,f"l{d}")<=avg*.95:return (pnl(i,a,r.o1,r.o2,avg*.95),"追加後5%利確",d,True)
  if getattr(r,f"c{d}")>=avg*1.02:
   ex=getattr(r,f"o{d+1}") if d<5 else getattr(r,f"c{d}");return (pnl(i,a,r.o1,r.o2,ex),"追加後2%終値撤退",d+1 if d<5 else d,True)
 return (pnl(i,a,r.o1,r.o2,r.c5),"5日終了",5,True)
def metric(g):return {"n":int(len(g)),"codes":int(g.code.nunique()),"add_rate":float(g.added.mean()),"mean_return_pct":float(g.return_pct.mean()),"median_return_pct":float(g.return_pct.median()),"win_rate":float(g.return_pct.gt(0).mean()),"severe_loss5_rate":float(g.return_pct.le(-5).mean()),"p10_return_pct":float(g.return_pct.quantile(.1)),"median_exit_day":float(g.exit_day.median())}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--ledger",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 led=pd.read_parquet(a.ledger);led=led[led.action_tier.isin(["Core","Probe"])].copy();bars=pd.read_parquet(a.inventory,columns=["code","bar_index","o","l","c"])
 x=led[["code","ymd","bar_index","period","action_tier"]].merge(bars.rename(columns={"l":"signal_low"})[["code","bar_index","signal_low"]],on=["code","bar_index"],validate="many_to_one")
 for k in range(1,7):
  z=bars.rename(columns={"bar_index":"fi","o":f"o{k}","l":f"l{k}","c":f"c{k}"});x["fi"]=x.bar_index+k;x=x.merge(z,on=["code","fi"],how="left",validate="many_to_one").drop(columns="fi")
 ds=[];rows=[];yrs=[]
 for initial in INITIALS:
  q=x.copy();v=[sim(r,initial) for r in q.itertuples(index=False)];q=q[[z is not None for z in v]].copy();v=[z for z in v if z is not None];q["initial_fraction"]=initial;q["return_pct"]=[z[0] for z in v];q["exit_reason"]=[z[1] for z in v];q["exit_day"]=[z[2] for z in v];q["added"]=[z[3] for z in v];ds.append(q)
  for (p,t),g in q.groupby(["period","action_tier"]):rows.append({"period":p,"action_tier":t,"initial_fraction":initial,**metric(g)})
  for (y,t),g in q.assign(year=q.ymd//10000).groupby(["year","action_tier"]):yrs.append({"year":int(y),"action_tier":t,"initial_fraction":initial,**metric(g)})
 d=pd.concat(ds);m=pd.DataFrame(rows);yr=pd.DataFrame(yrs);d.to_parquet(out/"managed_episode_ledger.parquet",index=False);m.to_parquet(out/"managed_metrics.parquet",index=False);yr.to_parquet(out/"managed_yearly_metrics.parquet",index=False)
 selected={}
 for tier in ["Core","Probe"]:
  z=m[(m.period.eq("development"))&(m.action_tier.eq(tier))];ok=z[(z.mean_return_pct>0)&(z.severe_loss5_rate<=.10)];selected[tier]=None if ok.empty else float(ok.sort_values(["mean_return_pct","severe_loss5_rate"],ascending=[False,True]).iloc[0].initial_fraction)
 val=[];checks={"both_tiers_selected":all(v is not None for v in selected.values())}
 for tier,i in selected.items():
  z=m[(m.period.eq("validation"))&(m.action_tier.eq(tier))&(m.initial_fraction.eq(i))];val+=z.to_dict("records");yy=yr[(yr.year>=2024)&(yr.action_tier.eq(tier))&(yr.initial_fraction.eq(i))]
  checks[f"{tier}_validation_mean_positive"]=bool(len(z)==1 and z.iloc[0].mean_return_pct>0);checks[f"{tier}_validation_tail_le10"]=bool(len(z)==1 and z.iloc[0].severe_loss5_rate<=.10);checks[f"{tier}_all_years_positive"]=bool(len(yy)==3 and (yy.mean_return_pct>0).all())
 keep=all(checks.values())
 result={"schema_version":"tradex_short_probe_confirm_add_exit_v1.compare.v1","artifact_role":"authoritative_short_probe_confirm_add_exit","review_only":True,"fixed_conditions":{"entry":"next open initial fraction","same_day_target":"5% on initial fraction","confirmation":"entry-day close below signal-day low","if_confirmed":"add remainder following open","if_not_confirmed":"exit initial following open","after_add_target":"5% from weighted entry","after_add_close_stop":"2%, execute following open","horizon":"fifth close","initial_candidates":INITIALS,"selected_on":"development only","costs":"ignored"},"authoritative_result":{"selected_initial_fraction":selected,"validation":val,"validation_years":yr[(yr.year>=2024)&yr.apply(lambda r:selected.get(r.action_tier)==r.initial_fraction,axis=1)].to_dict("records"),"gate_checks":checks},"observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int(d.added.sum()),"selection_divergence_reason":"management branching after entry-day confirmation"},"judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_probe_confirm_add_exit" if keep else "hold_probe_confirm_add_exit","authoritative_rollup_decision":"keep_probe_confirm_add_exit_v1_review_only" if keep else "hold_continue_management_research","reason_type":"positive_yearly_and_tail_gates"},"not_changed":["candidate membership","MeeMee","ranking","runtime DB","production logic"],"remaining_risks":["target touch execution","costs ignored","portfolio overlap absent"]}
 (out/"compare.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(result["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
