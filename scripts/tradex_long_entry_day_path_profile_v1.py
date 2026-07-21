from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
def met(g):return {"n":int(len(g)),"codes":int(g.code.nunique()),"next4_rise5_rate":float(g.next4_rise5.mean()),"next4_drop5_rate":float(g.next4_drop5.mean()),"mean_close5_pct":float(g.close5_pct_path.mean()),"median_low_next4_pct":float(g.low_next4_pct.median()),"p10_low_next4_pct":float(g.low_next4_pct.quantile(.1))}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--families",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 fam=pd.read_parquet(a.families,columns=["code","ymd","buy_family","period"]);fam=fam[fam.buy_family.isin(["急落反発","上昇継続"])];b=pd.read_parquet(a.inventory,columns=["code","ymd","bar_index","o","h","l","c"]);x=fam.merge(b[["code","ymd","bar_index"]],on=["code","ymd"],validate="many_to_one")
 for k in range(1,6):
  z=b.rename(columns={"bar_index":"fi","o":f"o{k}","h":f"h{k}","l":f"l{k}","c":f"c{k}"})[["code","fi",f"o{k}",f"h{k}",f"l{k}",f"c{k}"]];x["fi"]=x.bar_index+k;x=x.merge(z,on=["code","fi"],how="left",validate="many_to_one").drop(columns="fi")
 x=x[x.o1.notna()&x.c5.notna()].copy();x["d1_high"]=(x.h1/x.o1-1)*100;x["d1_low"]=(x.l1/x.o1-1)*100;x["d1_close"]=(x.c1/x.o1-1)*100;x["entry_day_path"]="その他";flat=x.d1_close.abs().le(1)&x.d1_low.gt(-2)&x.d1_high.lt(2);x.loc[flat,"entry_day_path"]="狭い横ばい";x.loc[x.d1_high.ge(3)&x.d1_low.gt(-3),"entry_day_path"]="即上昇";x.loc[(x.d1_close.le(-2)|x.d1_low.le(-3))&~x.d1_high.ge(3),"entry_day_path"]="失速"
 x["next4_rise5"]=x[[f"h{k}" for k in range(2,6)]].max(axis=1).ge(x.o1*1.05);x["next4_drop5"]=x[[f"l{k}" for k in range(2,6)]].min(axis=1).le(x.o1*.95);x["close5_pct_path"]=(x.c5/x.o1-1)*100;x["low_next4_pct"]=(x[[f"l{k}" for k in range(2,6)]].min(axis=1)/x.o1-1)*100
 rows=[];yrs=[]
 for (p,f,s),g in x.groupby(["period","buy_family","entry_day_path"]):rows.append({"period":p,"buy_family":f,"entry_day_path":s,**met(g)})
 for (y,f,s),g in x.assign(year=x.ymd//10000).groupby(["year","buy_family","entry_day_path"]):yrs.append({"year":int(y),"buy_family":f,"entry_day_path":s,**met(g)})
 m=pd.DataFrame(rows);yr=pd.DataFrame(yrs);x.to_parquet(out/"entry_day_path_ledger.parquet",index=False);m.to_parquet(out/"entry_day_path_metrics.parquet",index=False);yr.to_parquet(out/"entry_day_path_yearly_metrics.parquet",index=False);v=m[m.period.eq("validation")];checks={}
 for f in ["急落反発","上昇継続"]:
  z=v[v.buy_family.eq(f)].set_index("entry_day_path");checks[f"{f}_immediate_rise_gt_flat"]=bool(z.loc["即上昇","next4_rise5_rate"]>z.loc["狭い横ばい","next4_rise5_rate"]);checks[f"{f}_stall_drop_gt_immediate"]=bool(z.loc["失速","next4_drop5_rate"]>z.loc["即上昇","next4_drop5_rate"])
 keep=all(checks.values());res={"schema_version":"tradex_long_entry_day_path_profile_v1.compare.v1","artifact_role":"authoritative_long_entry_day_path_profile","review_only":True,"fixed_conditions":{"entry_reference":"next open","classification":"entry day only","immediate_rise":"high>=+3% and low>-3%","flat":"abs close<=1%, low>-2%, high<2%","stall":"close<=-2% or low<=-3%, excluding high>=3%","outcome":"sessions 2-5"},"authoritative_result":{"validation":v.to_dict("records"),"validation_years":yr[yr.year>=2024].to_dict("records"),"gate_checks":checks},"judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_long_path_map" if keep else "hold_long_path_map","authoritative_rollup_decision":"keep_long_entry_day_path_profile_v1_review_only" if keep else "hold_refine_path_definition","reason_type":"immediate_rise_and_stall_paths_diverge"},"not_changed":["family membership","entry","MeeMee","ranking","runtime DB","production logic"]}
 (out/"compare.json").write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps({"checks":checks,"validation":v.to_dict("records")},ensure_ascii=False))
if __name__=="__main__":main()
