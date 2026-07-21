"""Evaluate cross-sectional market-relative return as independent short-tier context."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import duckdb,numpy as np,pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--tiers",type=Path,required=True);ap.add_argument("--inventory",type=Path,required=True);ap.add_argument("--output",type=Path,required=True)
 a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False);con=duckdb.connect()
 q=f"""with m as(select ymd,median(ret1) m1,median(ret5) m5,median(ret20) m20 from read_parquet('{str(a.inventory.resolve())}') group by 1) select t.*,d.ret1-m.m1 rel1,d.ret5-m.m5 rel5,d.ret20-m.m20 rel20 from read_parquet('{str(a.tiers.resolve())}') t join read_parquet('{str(a.inventory.resolve())}') d using(code,ymd) join m using(ymd)"""
 x=con.execute(q).df();con.close();dev=x.ymd<20240101;contracts={};rows=[]
 for f in ["rel1","rel5","rel20"]:
  e=np.unique(x.loc[dev,f].dropna().quantile([0,.2,.4,.6,.8,1]).to_numpy(float));e[0],e[-1]=-np.inf,np.inf;contracts[f]=[None if np.isinf(v) else float(v) for v in e];x["band"]=pd.cut(x[f],e,labels=["Q1_weak","Q2","Q3","Q4","Q5_strong"],include_lowest=True)
  x["period"]=np.where(dev,"development","validation")
  for (period,band,tier),g in x.groupby(["period","band","tier"],observed=True):
   rows.append({"axis":f,"period":period,"relative_band":str(band),"tier":tier,"n":len(g),"codes":g.code.nunique(),"hit_rate":g.drop5_in5.mean(),"clean_rate":g.clean_drop5_in5.mean(),"severe10_rate":g.drop8_in10.mean(),"median_high5_pct":g.high5_pct.median(),"p90_high5_pct":g.high5_pct.quantile(.9)})
 p=pd.DataFrame(rows);p.to_parquet(a.output/"market_relative_tier_metrics.parquet",index=False)
 val=p[p.period.eq("validation")];ordered={}
 for f,g in val.groupby("axis"):
  w=g.pivot(index="relative_band",columns="tier",values="hit_rate");ordered[f]=bool(((w.Core>w.Probe)&(w.Probe>w.Risk)).all())
 checks={"tier_order_all_axes_bands":all(ordered.values()),"no_membership_filter":True,"development_only_band_edges":True}
 result={"schema_version":"tradex_short_market_relative_context_v1.compare.v1","artifact_role":"authoritative_short_market_relative_context","review_only":True,"research_phase":"effectiveness_judgment","fixed_conditions":{"market_proxy":"cross-sectional median PAN return by date","axes":["relative_ret1","relative_ret5","relative_ret20"],"bands":"development quintiles","policy":"independent context only"},"authoritative_result":{"contracts":contracts,"ordered":ordered,"gate_checks":checks,"validation_rows":val.to_dict("records")},"observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":0,"selection_divergence_reason":"relative weakness annotates tiers without changing membership"},"judgment":{"candidate_local_decision":"keep" if all(checks.values()) else "hold","session_aggregate_decision":"keep_market_relative_context" if all(checks.values()) else "hold_market_relative","authoritative_rollup_decision":"keep_market_relative_context_v1_review_only" if all(checks.values()) else "hold","reason_type":"relative_return_gradients_preserve_tier_order"},"not_changed":["tier membership","hard screens","MeeMee","ranking","runtime DB","production logic"],"remaining_risks":["cross-sectional median is not industry-neutral","market-wide shocks correlate rows","industry context remains absent"]}
 c=a.output/"compare.json";c.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=lambda z:float(z))+"\n",encoding="utf-8");arts=[c,a.output/"market_relative_tier_metrics.parquet"];(a.output/"audit.json").write_text(json.dumps({"sources":{"tiers":sha(a.tiers),"inventory":sha(a.inventory)},"artifacts":{p.name:sha(p) for p in arts}},indent=2)+"\n");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(c)},indent=2)+"\n");print(json.dumps({"checks":checks,"ordered":ordered}))
if __name__=="__main__":main()
