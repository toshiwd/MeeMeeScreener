from __future__ import annotations
import argparse,hashlib,json,math
from pathlib import Path
import duckdb,pandas as pd
from tradex_short_decline_axis_profile_v1 import NUMERIC_FEATURES,BINARY_FEATURES
def esc(p):return str(Path(p).resolve()).replace("'","''")
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def f(v):
 if v is None:return None
 v=float(v);return v if math.isfinite(v) else None
def agg(c,p,w):
 r=c.execute(f"SELECT count(*),avg(rise5_in5),avg(rise8_in10),avg(rise10_in20),avg(clean_rise3_before_drop5) FROM read_parquet('{esc(p)}') WHERE {w}").fetchone();return dict(zip(["n","event_rate","severe10_rate","severe20_rate","clean_rate"],[int(r[0]),*[f(x) for x in r[1:]]]))
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);c=duckdb.connect();devb=agg(c,a.inventory,"ymd<20240101");valb=agg(c,a.inventory,"ymd>=20240101");rows=[]
 for feature in NUMERIC_FEATURES:
  qs=c.execute(f"SELECT quantile_cont({feature},[0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1]) FROM read_parquet('{esc(a.inventory)}') WHERE ymd<20240101 AND {feature} IS NOT NULL AND isfinite({feature})").fetchone()[0];edges=[]
  for v in qs:
   v=float(v)
   if not edges or v>edges[-1]:edges.append(v)
  for i,(lo,hi) in enumerate(zip(edges[:-1],edges[1:])):
   op="<=" if i==len(edges)-2 else "<";cond=f"{feature}>={lo!r} AND {feature}{op}{hi!r}"
   for period,pw,base in [("development","ymd<20240101",devb),("validation","ymd>=20240101",valb)]:
    z=agg(c,a.inventory,f"{pw} AND {cond}");rows.append({"feature":feature,"bucket":i+1,"lower":lo,"upper":hi,"period":period,**z,"event_rate_lift":z["event_rate"]/base["event_rate"]})
 for feature in BINARY_FEATURES:
  vals=[r[0] for r in c.execute(f"SELECT DISTINCT {feature} FROM read_parquet('{esc(a.inventory)}') WHERE {feature} IS NOT NULL ORDER BY 1").fetchall()]
  for v in vals:
   for period,pw,base in [("development","ymd<20240101",devb),("validation","ymd>=20240101",valb)]:
    z=agg(c,a.inventory,f"{pw} AND {feature}={int(v)}");rows.append({"feature":feature,"bucket":int(v),"lower":float(v),"upper":float(v),"period":period,**z,"event_rate_lift":z["event_rate"]/base["event_rate"]})
 c.close();p=pd.DataFrame(rows);pp=out/"axis_band_profile.parquet";p.to_parquet(pp,index=False);d=p[p.period.eq("development")];v=p[p.period.eq("validation")];x=d.merge(v,on=["feature","bucket","lower","upper"],suffixes=("_dev","_val"));x["stable_positive_band"]=x.n_dev.ge(1000)&x.n_val.ge(500)&x.event_rate_lift_dev.ge(1.20)&x.event_rate_lift_val.ge(1.10);x["stable_negative_band"]=x.n_dev.ge(1000)&x.n_val.ge(500)&x.event_rate_lift_dev.le(.85)&x.event_rate_lift_val.le(.90);s=x[x.stable_positive_band|x.stable_negative_band].copy();sp=out/"stable_axis_bands.parquet";s.to_parquet(sp,index=False)
 fs=[]
 for feature,g in s.groupby("feature"):fs.append({"feature":feature,"stable_positive_bands":int(g.stable_positive_band.sum()),"stable_negative_bands":int(g.stable_negative_band.sum()),"max_validation_lift":f(g.event_rate_lift_val.max()),"min_validation_lift":f(g.event_rate_lift_val.min()),"validation_rows":int(g.n_val.sum())})
 fs.sort(key=lambda r:(r["stable_positive_bands"],r["max_validation_lift"] or 0),reverse=True);checks={"profile_rows_ge400":len(p)>=400,"stable_bands_ge10":len(s)>=10,"stable_features_ge5":len(fs)>=5,"development_n_ge500k":devb["n"]>=500000,"validation_n_ge200k":valb["n"]>=200000};keep=all(checks.values())
 res={"schema_version":"tradex_long_rise_axis_profile_v1.compare.v1","artifact_role":"authoritative_independent_rise_axis_band_profile","review_only":True,"research_phase":"comparison_stabilization","fixed_conditions":{"development":"2019-2023","validation":"2024-2026","target":"5-session intraday high >= +5% from next open","numeric_bands":"development deciles","axis_policy":"independent descriptive only","positive_gate":"dev n>=1000 lift>=1.20; validation n>=500 lift>=1.10","negative_gate":"dev n>=1000 lift<=.85; validation n>=500 lift<=.90"},"authoritative_result":{"development_baseline":devb,"validation_baseline":valb,"profile_rows":len(p),"stable_band_count":len(s),"stable_feature_count":len(fs),"feature_summary":fs,"gate_checks":checks},"observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":len(s),"selection_divergence_reason":"independent rise-rate gradients without membership changes","profiled_numeric_axes":len(NUMERIC_FEATURES),"profiled_categorical_axes":len(BINARY_FEATURES)},"judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_rise_axis_map" if keep else "hold_rise_axis_map","authoritative_rollup_decision":"keep_rise_axis_profile_v1_review_only" if keep else "hold_expand_rise_axis_profile","reason_type":"multiple_axes_show_stable_nonbinary_gradients" if keep else "insufficient_stable_axis_breadth"},"not_changed":["hard screens","combined score","MeeMee","ranking","runtime DB"],"remaining_risks":["single-axis lift is not combined usefulness","daily rows overlap","monthly and market context absent"]}
 cp=out/"compare.json";cp.write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding="utf-8");(out/"audit.json").write_text(json.dumps({"source":sha(a.inventory),"profile":sha(pp),"stable":sha(sp)},indent=2));(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(res["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
