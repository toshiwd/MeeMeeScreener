from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
BANDS=[(100,900,"100-899円"),(900,3000,"900-2999円"),(3000,5000,"3000-4999円"),(5000,10000,"5000-9999円"),(10000,100000,"10000円以上")]
def met(g):return {"n":int(len(g)),"codes":int(g.code.nunique()),"mean_return_pct":float(g.ret.mean()),"win_rate":float(g.ret.gt(0).mean()),"severe_loss5_rate":float(g.ret.le(-5).mean()),"p10_return_pct":float(g.ret.quantile(.1))}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--managed",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);d=pd.read_parquet(a.managed);d=d[(d.buy_family.eq("急落反発"))&d.initial_fraction.eq(.25)].copy();inv=pd.read_parquet(a.inventory,columns=["code","ymd","c"]);d=d.merge(inv,on=["code","ymd"],validate="many_to_one");d["price_band"]="範囲外"
 for lo,hi,name in BANDS:d.loc[d.c.ge(lo)&d.c.lt(hi),"price_band"]=name
 rows=[];yrs=[]
 for (p,b),g in d.groupby(["period","price_band"]):rows.append({"period":p,"price_band":b,**met(g)})
 for (y,b),g in d.assign(year=d.ymd//10000).groupby(["year","price_band"]):yrs.append({"year":int(y),"price_band":b,**met(g)})
 m=pd.DataFrame(rows);yr=pd.DataFrame(yrs);bad=[]
 for _,_,b in BANDS:
  z=m[m.price_band.eq(b)].set_index("period")
  if set(z.index)=={"development","validation"} and z.loc["development","n"]>=1000 and z.loc["validation","n"]>=500 and z.loc["development","mean_return_pct"]<=0 and z.loc["validation","mean_return_pct"]<=0:bad.append(b)
 d["policy"]="取引";d.loc[d.price_band.isin(bad),"policy"]="見送り";tr=d[d.policy.eq("取引")];ov=pd.DataFrame([{"period":p,**met(g)} for p,g in tr.groupby("period")]);an=pd.DataFrame([{"year":int(y),**met(g)} for y,g in tr.assign(year=tr.ymd//10000).groupby("year")]);d.to_parquet(out/"price_context_ledger.parquet",index=False);m.to_parquet(out/"price_band_metrics.parquet",index=False);yr.to_parquet(out/"price_band_yearly_metrics.parquet",index=False);checks={"bad_band_found":len(bad)>0,"development_positive":bool(ov[ov.period.eq("development")].iloc[0].mean_return_pct>0),"validation_positive":bool(ov[ov.period.eq("validation")].iloc[0].mean_return_pct>0),"all_years_positive":bool(len(an[an.year>=2024])==3 and (an[an.year>=2024].mean_return_pct>0).all()),"validation_tail_le10":bool(ov[ov.period.eq("validation")].iloc[0].severe_loss5_rate<=.1)};keep=all(checks.values())
 res={"schema_version":"tradex_long_rebound_price_context_v1.compare.v1","artifact_role":"authoritative_long_rebound_price_context","review_only":True,"fixed_conditions":{"family":"急落反発","management":"25% initial, add on immediate rise, exit on stall","bands":[b for _,_,b in BANDS],"bad_band":"dev and validation mean<=0 with dev n>=1000,val n>=500","costs":"ignored"},"authoritative_result":{"bad_bands":bad,"band_metrics":m.to_dict("records"),"policy_period":ov.to_dict("records"),"policy_years":an[an.year>=2024].to_dict("records"),"gate_checks":checks},"judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_rebound_price_gate" if keep else "hold_rebound_price_context","authoritative_rollup_decision":"keep_long_rebound_price_context_v1_review_only" if keep else "hold_no_stable_price_exclusion","reason_type":"stable_bad_price_band_and_yearly_gates"},"not_changed":["family","management","MeeMee","ranking","runtime DB","production logic"]}
 (out/"compare.json").write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(res["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
