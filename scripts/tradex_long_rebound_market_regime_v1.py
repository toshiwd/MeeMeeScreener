from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
def met(g):return {"n":int(len(g)),"codes":int(g.code.nunique()),"mean_return_pct":float(g.ret.mean()),"win_rate":float(g.ret.gt(0).mean()),"severe_loss5_rate":float(g.ret.le(-5).mean()),"p10_return_pct":float(g.ret.quantile(.1))}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--managed",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 d=pd.read_parquet(a.managed);d=d[(d.buy_family.eq("急落反発"))&d.initial_fraction.eq(.25)].copy();inv=pd.read_parquet(a.inventory,columns=["ymd","ret20"]);market=inv.groupby("ymd",as_index=False).ret20.median().rename(columns={"ret20":"market_ret20"});d=d.merge(market,on="ymd",validate="many_to_one");dev=d[d.period.eq("development")];edges=dev.market_ret20.quantile([0,.2,.4,.6,.8,1]).drop_duplicates().tolist();labels=[f"Q{i+1}" for i in range(len(edges)-1)];d["market_band"]=pd.cut(d.market_ret20,bins=edges,labels=labels,include_lowest=True,duplicates="drop");rows=[];yrs=[]
 for (p,b),g in d.groupby(["period","market_band"],observed=True):rows.append({"period":p,"market_band":str(b),**met(g)})
 for (y,b),g in d.assign(year=d.ymd//10000).groupby(["year","market_band"],observed=True):yrs.append({"year":int(y),"market_band":str(b),**met(g)})
 m=pd.DataFrame(rows);yr=pd.DataFrame(yrs);bad=[]
 for b in labels:
  z=m[m.market_band.eq(b)].set_index("period")
  if set(z.index)=={"development","validation"} and z.loc["development","n"]>=1000 and z.loc["validation","n"]>=500 and z.loc["development","mean_return_pct"]<=0 and z.loc["validation","mean_return_pct"]<=0:bad.append(b)
 d["policy"]="取引";d.loc[d.market_band.astype(str).isin(bad),"policy"]="見送り";trade=d[d.policy.eq("取引")];overall=[]
 for p,g in trade.groupby("period"):overall.append({"period":p,**met(g)})
 annual=[]
 for y,g in trade.assign(year=trade.ymd//10000).groupby("year"):annual.append({"year":int(y),**met(g)})
 ov=pd.DataFrame(overall);an=pd.DataFrame(annual);d.to_parquet(out/"market_regime_ledger.parquet",index=False);m.to_parquet(out/"market_regime_band_metrics.parquet",index=False);yr.to_parquet(out/"market_regime_yearly_band_metrics.parquet",index=False);checks={"bad_band_found":len(bad)>0,"development_positive_after_gate":bool(len(ov[ov.period.eq("development")])==1 and ov[ov.period.eq("development")].iloc[0].mean_return_pct>0),"validation_positive_after_gate":bool(len(ov[ov.period.eq("validation")])==1 and ov[ov.period.eq("validation")].iloc[0].mean_return_pct>0),"all_validation_years_positive":bool(len(an[an.year>=2024])==3 and (an[an.year>=2024].mean_return_pct>0).all()),"validation_tail_le10":bool(ov[ov.period.eq("validation")].iloc[0].severe_loss5_rate<=.1)};keep=all(checks.values())
 res={"schema_version":"tradex_long_rebound_market_regime_v1.compare.v1","artifact_role":"authoritative_long_rebound_market_regime","review_only":True,"fixed_conditions":{"family":"急落反発","management":"25% initial, add on immediate rise, exit on stall","market_proxy":"cross-sectional median ret20 by date","bands":"development quintiles","bad_band":"dev and validation mean<=0 with dev n>=1000,val n>=500","costs":"ignored"},"authoritative_result":{"contracts":{"edges":edges},"bad_bands":bad,"band_metrics":m.to_dict("records"),"policy_period":ov.to_dict("records"),"policy_years":an[an.year>=2024].to_dict("records"),"gate_checks":checks},"judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_rebound_market_gate" if keep else "hold_rebound_market_gate","authoritative_rollup_decision":"keep_long_rebound_market_regime_v1_review_only" if keep else "hold_no_stable_market_gate","reason_type":"development_validation_bad_regime_and_yearly_policy_gates"},"not_changed":["family definition","management thresholds","MeeMee","ranking","runtime DB","production logic"]}
 (out/"compare.json").write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(res["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
