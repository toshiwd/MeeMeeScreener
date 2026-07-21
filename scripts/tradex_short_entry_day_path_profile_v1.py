from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
def metric(g):return {"n":int(len(g)),"codes":int(g.code.nunique()),"next4_drop5_rate":float(g.next4_drop5.mean()),"next4_rebound5_rate":float(g.next4_rebound5.mean()),"mean_close5_short_pct":float(g.close5_short.mean()),"median_close5_short_pct":float(g.close5_short.median())}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--ledger",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 led=pd.read_parquet(a.ledger);led=led[led.action_tier.isin(["Core","Probe"])];b=pd.read_parquet(a.inventory,columns=["code","bar_index","o","h","l","c"]);x=led[["code","ymd","bar_index","period","action_tier"]].copy()
 for k in range(1,6):
  z=b.rename(columns={"bar_index":"fi","o":f"o{k}","h":f"h{k}","l":f"l{k}","c":f"c{k}"});x["fi"]=x.bar_index+k;x=x.merge(z,on=["code","fi"],how="left",validate="many_to_one").drop(columns="fi")
 x=x[x.o1.notna()&x.c5.notna()].copy();x["day1_low"]=(x.l1/x.o1-1)*100;x["day1_high"]=(x.h1/x.o1-1)*100;x["day1_close"]=(x.c1/x.o1-1)*100
 x["entry_day_path"]="その他";x.loc[x.day1_low.le(-3),"entry_day_path"]="即下落";flat=x.day1_close.abs().le(1)&x.day1_low.gt(-2)&x.day1_high.lt(2);x.loc[flat,"entry_day_path"]="狭い横ばい";up=(x.day1_close.ge(2)|x.day1_high.ge(3))&~x.day1_low.le(-3);x.loc[up,"entry_day_path"]="上昇"
 x["next4_drop5"]=x[[f"l{k}" for k in range(2,6)]].min(axis=1).le(x.o1*.95);x["next4_rebound5"]=x[[f"h{k}" for k in range(2,6)]].max(axis=1).ge(x.o1*1.05);x["close5_short"]=(x.o1-x.c5)/x.o1*100
 rows=[];yrs=[]
 for (p,t,s),g in x.groupby(["period","action_tier","entry_day_path"]):rows.append({"period":p,"action_tier":t,"entry_day_path":s,**metric(g)})
 for (y,t,s),g in x.assign(year=x.ymd//10000).groupby(["year","action_tier","entry_day_path"]):yrs.append({"year":int(y),"action_tier":t,"entry_day_path":s,**metric(g)})
 m=pd.DataFrame(rows);yr=pd.DataFrame(yrs);x.to_parquet(out/"entry_day_path_ledger.parquet",index=False);m.to_parquet(out/"entry_day_path_metrics.parquet",index=False);yr.to_parquet(out/"entry_day_path_yearly_metrics.parquet",index=False)
 val=m[m.period.eq("validation")];checks={}
 for t in ["Core","Probe"]:
  z=val[val.action_tier.eq(t)].set_index("entry_day_path");checks[f"{t}_swift_drop_gt_flat_drop"]=bool(z.loc["即下落","next4_drop5_rate"]>z.loc["狭い横ばい","next4_drop5_rate"]);checks[f"{t}_flat_rebound_gt_swift"]=bool(z.loc["狭い横ばい","next4_rebound5_rate"]>z.loc["即下落","next4_rebound5_rate"])
 years=yr[yr.year>=2024];checks["all_years_core_swift_drop_gt_flat"]=bool(all(g.set_index("entry_day_path").loc["即下落","next4_drop5_rate"]>g.set_index("entry_day_path").loc["狭い横ばい","next4_drop5_rate"] for _,g in years[years.action_tier.eq("Core")].groupby("year")))
 keep=all(checks.values());result={"schema_version":"tradex_short_entry_day_path_profile_v1.compare.v1","artifact_role":"authoritative_short_entry_day_path_profile","review_only":True,"fixed_conditions":{"entry":"next open","classification":"entry day only","flat":"abs close <=1%, low >-2%, high <2%","swift_drop":"low <=-3%","up":"close >=2% or high >=3%, excluding swift drop","outcome":"sessions 2-5 relative to entry"},"authoritative_result":{"validation":val.to_dict("records"),"validation_years":years.to_dict("records"),"gate_checks":checks},"judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_flat_exit_axis" if keep else "hold_flat_exit_axis","authoritative_rollup_decision":"keep_entry_day_flat_exit_context_v1_review_only" if keep else "hold_continue_flat_definition","reason_type":"swift_drop_vs_flat_forward_path_gates"},"not_changed":["entry","membership","MeeMee","ranking","runtime DB","production logic"]}
 (out/"compare.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps({"checks":checks,"validation":val.to_dict("records")},ensure_ascii=False))
if __name__=="__main__":main()
