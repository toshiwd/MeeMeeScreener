from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
STOPS=[2.,3.,5.,7.,None]
def sim(r,s):
 e=r.o1
 for d in range(1,6):
  if getattr(r,f"h{d}")>=e*1.05:return (5.,"5%利確",d)
  cl=getattr(r,f"c{d}")
  if s is not None and cl<=e*(1-s/100):
   px=getattr(r,f"o{d+1}") if d<5 else cl;return (100*(px/e-1),"終値撤退",d+1 if d<5 else d)
 return (100*(r.c5/e-1),"5日終了",5)
def met(g):return {"n":int(len(g)),"codes":int(g.code.nunique()),"mean_return_pct":float(g.ret.mean()),"median_return_pct":float(g.ret.median()),"win_rate":float(g.ret.gt(0).mean()),"target5_rate":float(g.reason.eq("5%利確").mean()),"severe_loss5_rate":float(g.ret.le(-5).mean()),"p10_return_pct":float(g.ret.quantile(.1)),"median_exit_day":float(g.day.median())}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--paths",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);x=pd.read_parquet(a.paths);b=pd.read_parquet(a.inventory,columns=["code","bar_index","o"]);z=b.rename(columns={"bar_index":"fi","o":"o6"});x["fi"]=x.bar_index+6;x=x.merge(z[["code","fi","o6"]],on=["code","fi"],how="left",validate="many_to_one").drop(columns="fi");x=x[x.o1.notna()&x.c5.notna()].copy();ds=[];rows=[];yrs=[]
 for s in STOPS:
  q=x.copy();v=[sim(r,s) for r in q.itertuples(index=False)];q["stop"]="none" if s is None else str(s);q["ret"]=[v[0] for v in v];q["reason"]=[v[1] for v in v];q["day"]=[v[2] for v in v];ds.append(q)
  for (p,f),g in q.groupby(["period","buy_family"]):rows.append({"period":p,"buy_family":f,"stop":q.stop.iloc[0],**met(g)})
  for (y,f),g in q.assign(year=q.ymd//10000).groupby(["year","buy_family"]):yrs.append({"year":int(y),"buy_family":f,"stop":q.stop.iloc[0],**met(g)})
 d=pd.concat(ds);m=pd.DataFrame(rows);yr=pd.DataFrame(yrs);d.to_parquet(out/"target_stop_ledger.parquet",index=False);m.to_parquet(out/"target_stop_metrics.parquet",index=False);yr.to_parquet(out/"target_stop_yearly_metrics.parquet",index=False);sel={}
 for f in ["急落反発","上昇継続"]:
  z=m[(m.period.eq("development"))&(m.buy_family.eq(f))];ok=z[(z.mean_return_pct>0)&(z.severe_loss5_rate<=.10)];sel[f]=None if ok.empty else str(ok.sort_values(["mean_return_pct","severe_loss5_rate"],ascending=[False,True]).iloc[0].stop)
 val=[];checks={"both_selected":all(v is not None for v in sel.values())}
 for f,s in sel.items():
  z=m[(m.period.eq("validation"))&(m.buy_family.eq(f))&(m.stop.eq(s))];val+=z.to_dict("records");yy=yr[(yr.year>=2024)&(yr.buy_family.eq(f))&(yr.stop.eq(s))];checks[f"{f}_positive"]=bool(len(z)==1 and z.iloc[0].mean_return_pct>0);checks[f"{f}_tail_le10"]=bool(len(z)==1 and z.iloc[0].severe_loss5_rate<=.1);checks[f"{f}_all_years_positive"]=bool(len(yy)==3 and (yy.mean_return_pct>0).all())
 keep=all(checks.values());res={"schema_version":"tradex_long_target5_close_stop_v1.compare.v1","artifact_role":"authoritative_long_target5_close_stop","review_only":True,"fixed_conditions":{"entry":"next open","target":"intraday +5%","close_stop_candidates":[2,3,5,7,"none"],"stop_execution":"following open","horizon":"fifth close","selected_on":"development only","costs":"ignored"},"authoritative_result":{"selected_stop":sel,"validation":val,"validation_years":yr[(yr.year>=2024)&yr.apply(lambda r:sel.get(r.buy_family)==r.stop,axis=1)].to_dict("records"),"gate_checks":checks},"judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_long_target_stop" if keep else "hold_long_target_stop","authoritative_rollup_decision":"keep_long_target5_close_stop_v1_review_only" if keep else "hold_continue_long_exit_research","reason_type":"positive_yearly_tail_gates"},"not_changed":["family membership","entry","MeeMee","ranking","runtime DB","production logic"]}
 (out/"compare.json").write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(res["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
