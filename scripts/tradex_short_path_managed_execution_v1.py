from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
INITIALS=[.25,.5]
def leg(i,o,px):return 100*i*(o-px)/o
def run(r,i):
 if any(pd.isna(v) for v in [r.o1,r.o2,r.c5]):return None
 if r.l1<=r.o1*.95:return (i*5,"初日5%利確",1,False)
 low=100*(r.l1/r.o1-1);high=100*(r.h1/r.o1-1);close=100*(r.c1/r.o1-1)
 flat=abs(close)<=1 and low>-2 and high<2;up=(close>=2 or high>=3) and low>-3;swift=low<=-3
 if flat or up:return (leg(i,r.o1,r.o2),"横ばい上昇撤退",2,False)
 add=1-i if swift else 0;avg=1/(i/r.o1+add/r.o2) if add else r.o1
 for d in range(2,6):
  lo=getattr(r,f"l{d}");cl=getattr(r,f"c{d}")
  if lo<=avg*.95:
   px=avg*.95;return (leg(i,r.o1,px)+(leg(add,r.o2,px) if add else 0),"5%利確",d,bool(add))
  if cl>=avg*1.02:
   px=getattr(r,f"o{d+1}") if d<5 else cl;return (leg(i,r.o1,px)+(leg(add,r.o2,px) if add else 0),"2%終値撤退",d+1 if d<5 else d,bool(add))
 px=r.c5;return (leg(i,r.o1,px)+(leg(add,r.o2,px) if add else 0),"5日終了",5,bool(add))
def met(g):return {"n":int(len(g)),"codes":int(g.code.nunique()),"add_rate":float(g.added.mean()),"mean_return_pct":float(g.ret.mean()),"median_return_pct":float(g.ret.median()),"win_rate":float(g.ret.gt(0).mean()),"severe_loss5_rate":float(g.ret.le(-5).mean()),"p10_return_pct":float(g.ret.quantile(.1)),"median_exit_day":float(g.exit_day.median())}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--ledger",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 led=pd.read_parquet(a.ledger);led=led[led.action_tier.isin(["Core","Probe"])];b=pd.read_parquet(a.inventory,columns=["code","bar_index","o","h","l","c"]);x=led[["code","ymd","bar_index","period","action_tier"]].copy()
 for k in range(1,7):z=b.rename(columns={"bar_index":"fi","o":f"o{k}","h":f"h{k}","l":f"l{k}","c":f"c{k}"});x["fi"]=x.bar_index+k;x=x.merge(z,on=["code","fi"],how="left",validate="many_to_one").drop(columns="fi")
 ds=[];rows=[];yrs=[]
 for i in INITIALS:
  q=x.copy();v=[run(r,i) for r in q.itertuples(index=False)];q=q[[z is not None for z in v]].copy();v=[z for z in v if z];q["initial_fraction"]=i;q["ret"]=[z[0] for z in v];q["reason"]=[z[1] for z in v];q["exit_day"]=[z[2] for z in v];q["added"]=[z[3] for z in v];ds.append(q)
  for (p,t),g in q.groupby(["period","action_tier"]):rows.append({"period":p,"action_tier":t,"initial_fraction":i,**met(g)})
  for (y,t),g in q.assign(year=q.ymd//10000).groupby(["year","action_tier"]):yrs.append({"year":int(y),"action_tier":t,"initial_fraction":i,**met(g)})
 d=pd.concat(ds);m=pd.DataFrame(rows);yr=pd.DataFrame(yrs);d.to_parquet(out/"managed_execution_ledger.parquet",index=False);m.to_parquet(out/"managed_execution_metrics.parquet",index=False);yr.to_parquet(out/"managed_execution_yearly_metrics.parquet",index=False)
 sel={}
 for t in ["Core","Probe"]:
  z=m[(m.period.eq("development"))&(m.action_tier.eq(t))];ok=z[(z.mean_return_pct>0)&(z.severe_loss5_rate<=.10)];sel[t]=None if ok.empty else float(ok.sort_values("mean_return_pct",ascending=False).iloc[0].initial_fraction)
 val=[];checks={"both_selected":all(v is not None for v in sel.values())}
 for t,i in sel.items():
  z=m[(m.period.eq("validation"))&(m.action_tier.eq(t))&(m.initial_fraction.eq(i))];val+=z.to_dict("records");yy=yr[(yr.year>=2024)&(yr.action_tier.eq(t))&(yr.initial_fraction.eq(i))]
  checks[f"{t}_positive"]=bool(len(z)==1 and z.iloc[0].mean_return_pct>0);checks[f"{t}_tail_le10"]=bool(len(z)==1 and z.iloc[0].severe_loss5_rate<=.1);checks[f"{t}_all_years_positive"]=bool(len(yy)==3 and (yy.mean_return_pct>0).all())
 keep=all(checks.values());res={"schema_version":"tradex_short_path_managed_execution_v1.compare.v1","artifact_role":"authoritative_short_path_managed_execution","review_only":True,"fixed_conditions":{"entry":"next open initial","initial_candidates":INITIALS,"entry_day":"swift low<=-3 add remainder; flat abs close<=1 low>-2 high<2 or up close>=2/high>=3 exit next open; other keep initial","target":"5%","close_stop":"2% following open","horizon":"fifth close","selection":"development only","costs":"ignored"},"authoritative_result":{"selected_initial":sel,"validation":val,"validation_years":yr[(yr.year>=2024)&yr.apply(lambda r:sel.get(r.action_tier)==r.initial_fraction,axis=1)].to_dict("records"),"gate_checks":checks},"judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_path_management" if keep else "hold_path_management","authoritative_rollup_decision":"keep_path_managed_execution_v1_review_only" if keep else "hold_continue_path_management","reason_type":"positive_yearly_tail_gates"},"not_changed":["membership","MeeMee","ranking","runtime DB","production logic"]}
 (out/"compare.json").write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(res["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
