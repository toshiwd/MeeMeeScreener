"""Compress stable candle-context cells by actual code-date membership overlap."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import duckdb, numpy as np, pandas as pd
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def edges(values):
    x=np.unique(values.dropna().quantile([0,.2,.4,.6,.8,1]).to_numpy(float))
    if len(x)>=2: x[0],x[-1]=-np.inf,np.inf
    return x
def uf_components(n, links):
    parent=list(range(n))
    def find(x):
        while parent[x]!=x:
            parent[x]=parent[parent[x]]; x=parent[x]
        return x
    def union(a,b):
        a,b=find(a),find(b)
        if a!=b: parent[b]=a
    for a,b in links: union(a,b)
    out={}
    for i in range(n): out.setdefault(find(i),[]).append(i)
    return list(out.values())
def metrics(x):
    if x.empty: return {"n":0}
    win=x.drop5_in5.eq(1)
    return {"n":int(len(x)),"codes":int(x.code.nunique()),
      "event_rate":float(win.mean()),"clean_rate":float(x.clean_drop5_in5.mean()),
      "severe10_rate":float(x.drop8_in10.mean()),
      "median_high5_pct":float(x.high5_pct.median()),
      "p90_high5_pct":float(x.high5_pct.quantile(.9)),
      "median_first_drop5_day":None if not win.any() else float(x.loc[win,"first_drop5_day"].median()),
      "years":{str(int(y)):{"n":int(len(g)),"event_rate":float(g.drop5_in5.mean())}
               for y,g in x.groupby(x.ymd//10000)}}
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--inventory",type=Path,required=True)
    ap.add_argument("--stable",type=Path,required=True); ap.add_argument("--output",type=Path,required=True)
    a=ap.parse_args(); a.output.mkdir(parents=True,exist_ok=False)
    stable=pd.read_parquet(a.stable).reset_index(drop=True)
    cols=sorted(set(["code","ymd","bar_index","drop5_in5","clean_drop5_in5","drop8_in10",
      "high5_pct","first_drop5_day"]+stable.candle_axis.tolist()+stable.context_axis.tolist()))
    con=duckdb.connect()
    try: frame=con.execute("select "+",".join(cols)+" from read_parquet(?)",[str(a.inventory.resolve())]).df()
    finally: con.close()
    dev=frame.ymd.lt(20240101); features=set(stable.candle_axis)|set(stable.context_axis)
    categorical={"candle_direction","close_below_prev_low","lower_high_1","lower_low_1",
      "lower_high_2","lower_low_2","two_bearish","two_bullish"}
    for f in features:
        if f in categorical: frame[f+"__band"]=frame[f]
        else:
            e=edges(frame.loc[dev,f]); frame[f+"__band"]=pd.cut(frame[f],e,labels=False,include_lowest=True)
    sets=[]; cell_rows=[]
    for i,r in stable.iterrows():
        mask=frame[r.candle_axis+"__band"].eq(r.candle_band)&frame[r.context_axis+"__band"].eq(r.context_band)
        ids=set(frame.index[mask].tolist()); sets.append(ids)
        cell_rows.append({"cell_id":i,"candle_axis":r.candle_axis,"context_axis":r.context_axis,
          "candle_band":int(r.candle_band),"context_band":int(r.context_band),
          "direction":"positive" if r.stable_positive else "negative","members":len(ids)})
    overlaps=[]; links=[]
    for i in range(len(stable)):
        for j in range(i+1,len(stable)):
            if bool(stable.loc[i,"stable_positive"])!=bool(stable.loc[j,"stable_positive"]): continue
            inter=len(sets[i]&sets[j])
            if not inter: continue
            union=len(sets[i]|sets[j]); jac=inter/union; contain=max(inter/len(sets[i]),inter/len(sets[j]))
            if jac>=.65 or contain>=.85: links.append((i,j))
            if jac>=.20 or contain>=.50:
                overlaps.append({"cell_a":i,"cell_b":j,"intersection":inter,"jaccard":jac,"max_containment":contain})
    comps=uf_components(len(stable),links); membership=[]; summaries=[]; signals=[]
    for cid,idxs in enumerate(comps,1):
        union=set().union(*(sets[i] for i in idxs)); sign="positive" if stable.loc[idxs[0],"stable_positive"] else "negative"
        context=set(stable.loc[idxs,"context_axis"])
        if any(str(x).startswith("dist_high") for x in context): family="position_context"
        elif any(x in {"ret1","ret3","ret5"} for x in context): family="recent_path"
        elif any(str(x).startswith("range") for x in context): family="volatility"
        elif "volume_ratio20" in context: family="volume"
        else: family="medium_path"
        raw=frame.loc[list(union)].sort_values(["code","bar_index"]).copy()
        raw["gap"]=raw.groupby("code").bar_index.diff()
        raw["episode"]=((raw["gap"].isna())|(raw["gap"]>5)).groupby(raw.code).cumsum()
        dedup=raw.groupby(["code","episode"],as_index=False).first()
        dedup["cluster_id"]=cid; dedup["direction"]=sign; dedup["semantic_family"]=family
        signals.append(dedup[["cluster_id","direction","semantic_family","code","ymd","bar_index",
          "drop5_in5","clean_drop5_in5","drop8_in10","high5_pct","first_drop5_day"]])
        d=metrics(dedup[dedup.ymd<20240101]); v=metrics(dedup[dedup.ymd>=20240101])
        summaries.append({"cluster_id":cid,"direction":sign,"semantic_family":family,
          "cell_count":len(idxs),"raw_members":len(union),"development":d,"validation":v})
        for i in idxs: membership.append({"cluster_id":cid,**cell_rows[i]})
    pd.DataFrame(membership).to_parquet(a.output/"cluster_membership.parquet",index=False)
    pd.DataFrame(overlaps).to_parquet(a.output/"cell_overlap_edges.parquet",index=False)
    pd.concat(signals,ignore_index=True).to_parquet(a.output/"cluster_signal_episodes.parquet",index=False)
    flat=[]
    for s in summaries:
        flat.append({"cluster_id":s["cluster_id"],"direction":s["direction"],"semantic_family":s["semantic_family"],
          "cell_count":s["cell_count"],"raw_members":s["raw_members"],
          **{"dev_"+k:v for k,v in s["development"].items() if k!="years"},
          **{"val_"+k:v for k,v in s["validation"].items() if k!="years"}})
    pd.DataFrame(flat).to_parquet(a.output/"cluster_metrics.parquet",index=False)
    useful=[s for s in summaries if s["validation"]["n"]>=1000 and
      ((s["direction"]=="positive" and s["validation"]["event_rate"]>=.22) or
       (s["direction"]=="negative" and s["validation"]["event_rate"]<=.16))]
    checks={"all_cells_assigned":len(membership)==len(stable),"cluster_count_lt_cell_count":len(comps)<len(stable),
      "validation_useful_clusters_ge_5":len(useful)>=5,"no_hard_screen_created":True}
    result={"schema_version":"tradex_short_candle_context_family_compression_v1.compare.v1",
      "artifact_role":"authoritative_candle_context_family_compression","review_only":True,
      "research_phase":"comparison_stabilization",
      "fixed_conditions":{"source_inventory":str(a.inventory.resolve()),"source_stable_cells":str(a.stable.resolve()),
        "link_rule":"same direction and (Jaccard>=0.65 or either containment>=0.85)",
        "signal_dedup":"same code cluster signals separated by <=5 trading bars; earliest retained",
        "development":"2019-2023","validation":"2024-2026","costs":"ignored",
        "policy":"compression and episode evaluation only; no screen or score"},
      "authoritative_result":{"source_cells":len(stable),"cluster_count":len(comps),
        "useful_validation_cluster_count":len(useful),"clusters":summaries,"gate_checks":checks},
      "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,
        "changed_rank_count":len(stable)-len(comps),
        "selection_divergence_reason":"actual code-date overlap merges redundant conditional cells",
        "source_cells":len(stable),"compressed_clusters":len(comps)},
      "judgment":{"candidate_local_decision":"keep" if all(checks.values()) else "hold",
        "session_aggregate_decision":"keep_family_compression" if all(checks.values()) else "hold_family_compression",
        "authoritative_rollup_decision":"keep_candle_context_family_compression_v1_review_only" if all(checks.values()) else "hold_refine_overlap_graph",
        "reason_type":"redundant_cells_compressed_with_validation_episode_metrics" if all(checks.values()) else "compression_or_breadth_gate_failed"},
      "not_changed":["hard screens","combined score","MeeMee","ranking","runtime DB","production logic"],
      "remaining_risks":["connected components can chain moderately different cells","market-date correlation remains",
        "corporate-action robustness remains","monthly range age is absent"]}
    compare=a.output/"compare.json"; compare.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    audit={"sources":{"inventory":{"path":str(a.inventory.resolve()),"sha256":sha(a.inventory)},
      "stable":{"path":str(a.stable.resolve()),"sha256":sha(a.stable)}},
      "artifacts":{p.name:sha(p) for p in [compare,a.output/"cluster_membership.parquet",
        a.output/"cell_overlap_edges.parquet",a.output/"cluster_metrics.parquet",
        a.output/"cluster_signal_episodes.parquet"]}}
    (a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8")
    (a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(compare)},indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"clusters":len(comps),"useful":len(useful),"checks":checks},ensure_ascii=False))
if __name__=="__main__": main()
