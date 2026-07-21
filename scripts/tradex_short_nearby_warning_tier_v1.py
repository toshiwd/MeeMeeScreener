"""Authoritative Core/Probe/Risk comparison for nearby warning windows."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import numpy as np,pandas as pd
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def nearby(rows, other, w):
    arrays={c:np.sort(g.bar_index.unique()) for c,g in other.groupby("code")}
    out=[]
    for r in rows.itertuples(index=False):
        a=arrays.get(r.code)
        if a is None: out.append(False); continue
        i=np.searchsorted(a,r.bar_index)
        hit=(i<len(a) and abs(int(a[i])-r.bar_index)<=w) or (i>0 and abs(int(a[i-1])-r.bar_index)<=w)
        out.append(hit)
    return np.array(out,bool)
def metric(x):
    if x.empty:return {"n":0}
    win=x.drop5_in5.eq(1); days=x.loc[win&x.first_drop5_day.between(1,5),"first_drop5_day"]
    return {"n":int(len(x)),"codes":int(x.code.nunique()),"hit_rate":float(win.mean()),
      "clean_rate":float(x.clean_drop5_in5.mean()),"severe10_rate":float(x.drop8_in10.mean()),
      "median_high5_pct":float(x.high5_pct.median()),"p90_high5_pct":float(x.high5_pct.quantile(.9)),
      "median_hit_day":None if days.empty else float(days.median())}
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--metrics",type=Path,required=True);ap.add_argument("--signals",type=Path,required=True);ap.add_argument("--output",type=Path,required=True)
    a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    m=pd.read_parquet(a.metrics);s=pd.read_parquet(a.signals)
    pos=set(m[(m.direction=="positive")&(m.dev_n>=3000)&(m.val_n>=1500)&(m.dev_event_rate>=.25)&(m.val_event_rate>=.27)].cluster_id)
    neg=set(m[(m.direction=="negative")&(m.dev_n>=3000)&(m.val_n>=1500)&(m.dev_event_rate<=.15)&(m.val_event_rate<=.12)].cluster_id)
    keys=["code","ymd","bar_index"]; vals=["drop5_in5","clean_drop5_in5","drop8_in10","high5_pct","first_drop5_day"]
    strong=s[s.cluster_id.isin(pos)].groupby(keys,as_index=False)[vals].first()
    risk=s[s.cluster_id.isin(neg)].groupby(keys,as_index=False)[vals].first()
    comparisons=[];annual=[];selected=None
    for w in (0,1,2):
        q=strong.copy();q["warning_nearby"]=nearby(q,risk,w);q["tier"]=np.where(q.warning_nearby,"Probe","Core")
        r=risk.copy();r["strong_nearby"]=nearby(r,strong,w);r=r[~r.strong_nearby].copy();r["tier"]="Risk"
        ledger=pd.concat([q.drop(columns=["warning_nearby"]),r.drop(columns=["strong_nearby"])],ignore_index=True)
        ledger["period"]=np.where(ledger.ymd<20240101,"development","validation")
        for period,g in ledger.groupby("period"):
            total_strong=len(g[g.tier.isin(["Core","Probe"])])
            for tier,t in g.groupby("tier"):
                z={"window":w,"period":period,"tier":tier,**metric(t)}
                z["strong_candidate_share"]=None if not total_strong or tier=="Risk" else len(t)/total_strong
                comparisons.append(z)
        for year,g in ledger.assign(year=ledger.ymd//10000).groupby("year"):
            for tier,t in g.groupby("tier"): annual.append({"window":w,"year":int(year),"tier":tier,**metric(t)})
        if w==1:selected=ledger
    comp=pd.DataFrame(comparisons);yr=pd.DataFrame(annual)
    comp.to_parquet(a.output/"window_tier_comparison.parquet",index=False);yr.to_parquet(a.output/"yearly_tier_metrics.parquet",index=False)
    selected.to_parquet(a.output/"tier_signal_ledger.parquet",index=False)
    val=comp[(comp.window==1)&(comp.period=="validation")].set_index("tier")
    years=yr[(yr.window==1)&(yr.year>=2024)].pivot(index="year",columns="tier",values="hit_rate")
    checks={"validation_core_gt_probe_gt_risk":bool(val.loc["Core","hit_rate"]>val.loc["Probe","hit_rate"]>val.loc["Risk","hit_rate"]),
      "all_validation_years_ordered":bool(((years.Core>years.Probe)&(years.Probe>years.Risk)).all()),
      "probe_n_ge_1000":bool(val.loc["Probe","n"]>=1000),"core_retention_ge_85pct":bool(val.loc["Core","strong_candidate_share"]>=.85),
      "core_probe_gap_ge_3pp":bool(val.loc["Core","hit_rate"]-val.loc["Probe","hit_rate"]>=.03)}
    keep=all(checks.values())
    result={"schema_version":"tradex_short_nearby_warning_tier_v1.compare.v1","artifact_role":"authoritative_short_nearby_warning_tier",
      "review_only":True,"research_phase":"effectiveness_judgment",
      "fixed_conditions":{"positive_clusters":sorted(pos),"warning_clusters":sorted(neg),"development":"2019-2023","validation":"2024-2026",
        "windows":[0,1,2],"selected_window":1,"distance_unit":"trading bars","entry":"next session open","costs":"ignored"},
      "authoritative_result":{"selected_window":1,"validation":{k:metric(selected[(selected.ymd>=20240101)&(selected.tier==k)]) for k in ["Core","Probe","Risk"]},
        "validation_years":years.reset_index().to_dict("records"),"gate_checks":checks},
      "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int(val.loc["Probe","n"]),
        "selection_divergence_reason":"warning cluster within one trading bar downgrades strong evidence from Core to Probe"},
      "judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_within_1_session_tier" if keep else "hold_tier",
        "authoritative_rollup_decision":"keep_within_1_session_tier_v1_review_only" if keep else "hold_continue_tier_research",
        "reason_type":"yearly_order_candidate_retention_and_gap_gates_passed" if keep else "one_or_more_tier_gates_failed"},
      "not_changed":["MeeMee","ranking","runtime DB","production logic","hard screens"],
      "remaining_risks":["Core p90 adverse remains high","nearby warning mostly predicts non-decline rather than upside reversal","market and monthly contexts absent"]}
    c=a.output/"compare.json";c.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    arts=[c,a.output/"window_tier_comparison.parquet",a.output/"yearly_tier_metrics.parquet",a.output/"tier_signal_ledger.parquet"]
    (a.output/"audit.json").write_text(json.dumps({"sources":{"metrics":sha(a.metrics),"signals":sha(a.signals)},"artifacts":{p.name:sha(p) for p in arts}},indent=2)+"\n")
    (a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(c)},indent=2)+"\n")
    print(json.dumps(result["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
