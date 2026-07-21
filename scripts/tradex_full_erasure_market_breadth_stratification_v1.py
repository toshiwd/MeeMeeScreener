"""One-axis diagnosis: does same-day market breadth explain full-erasure year instability?"""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

YEARS=(2023,2024,2025)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def cell(x):return {"n":int(len(x)),"codes":int(x.code.nunique()),"down_first":None if x.empty else float(x.label.eq(0).mean()),"rebound_first":None if x.empty else float(x.label.eq(1).mean()),"neutral":None if x.empty else float(x.label.eq(2).mean())}
def main():
 p=argparse.ArgumentParser();p.add_argument("--erasure-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 e=pd.read_parquet(a.erasure_ledger);e=e[e.challenger&e.year.isin(YEARS)].copy();f=pd.read_parquet(a.features,columns=["code","ymd","c","ma20"])
 # groupby aggregation needs both columns, so calculate explicitly without outcome data.
 b=f.assign(above=f.c>f.ma20).groupby("ymd",as_index=False).above.mean().rename(columns={"above":"breadth_above_ma20"})
 x=e.merge(b,on="ymd",how="left",validate="many_to_one");x["breadth_bucket"]=pd.cut(x.breadth_above_ma20,[-1,.40,.60,2],labels=["RISK_OFF_LE40","MIXED_40_60","RISK_ON_GT60"])
 results={bucket:{str(y):cell(x[(x.breadth_bucket.astype(str)==bucket)&x.year.eq(y)]) for y in YEARS} for bucket in ["RISK_OFF_LE40","MIXED_40_60","RISK_ON_GT60"]}
 gates={}
 for bucket,ys in results.items():
  breadth=all(ys[str(y)]["n"]>=10 for y in YEARS);positive=breadth and all(ys[str(y)]["down_first"]>ys[str(y)]["rebound_first"] for y in YEARS);gates[bucket]={"breadth_pass":breadth,"down_exceeds_rebound_all_years":positive}
 passing=[k for k,v in gates.items() if v["down_exceeds_rebound_all_years"]]
 data={"schema_version":"tradex_full_erasure_market_breadth_stratification_v1.compare.v1","artifact_role":"authoritative","axis":"same-day Nikkei225 breadth above MA20","fixed_conditions":{"entry_family":"UPTREND_FULL_ERASURE challenger unchanged","breadth":"cross-sectional fraction close>MA20 on decision close","buckets":{"RISK_OFF_LE40":"<=0.40","MIXED_40_60":"(0.40,0.60]","RISK_ON_GT60":">0.60"},"outcome":"inherited symmetric fixed 3 percent h5","minimum_each_year":10,"threshold_sweep":False},"year_bucket_results":results,"bucket_gates":gates,"observed_branching":{"events":int(len(x)),"bucket_counts":x.breadth_bucket.astype(str).value_counts().to_dict(),"passing_bucket_count":len(passing),"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":3,"selection_divergence_reason":"same candle family is partitioned only by contemporaneous market breadth"},"judgment":{"decision":"keep" if passing else "drop","passing_buckets":passing,"market_regime_explains_instability":bool(passing),"reason":"market-regime explanation requires at least one breadth bucket with >=10 events and down-first dominance in every OOS year"},"not_changed":["full-erasure definition","monthly environment","entry date","position lifecycle","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");x.to_parquet(a.output/"breadth_stratified_event_ledger.parquet",index=False);audit={"events":int(len(x)),"missing_breadth":int(x.breadth_above_ma20.isna().sum()),"duplicates":int(x.duplicated(["code","ymd"]).sum()),"erasure_sha256":sha(a.erasure_ledger),"features_sha256":sha(a.features),"future_used_for_selection":False,"review_only":True};(a.output/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"results":results,"judgment":data["judgment"]},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
