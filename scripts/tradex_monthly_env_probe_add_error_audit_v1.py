#!/usr/bin/env python
"""Diagnostic-only error audit for monthly environment probe/add events."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


FEATURES = [
    "ret3", "ret5", "ret10", "pos20", "range20_pct", "body_ratio",
    "upper_wick_ratio", "lower_wick_ratio", "close_pos", "dist_ma7_atr",
    "dist_ma20_atr", "dist_ma60_atr", "ma7_slope5_atr", "ma20_slope5_atr",
    "ma60_slope5_atr", "support_break_depth_atr", "volume_ratio20",
    "market_breadth_ma20", "market_breadth_ma60", "market_advancers_ratio",
]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dump(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    ap=argparse.ArgumentParser(); ap.add_argument("--events",type=Path,required=True); ap.add_argument("--features",type=Path,required=True); ap.add_argument("--output-root",type=Path,required=True); args=ap.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); out=args.output_root/f"{stamp}-tradex_monthly_env_probe_add_error_audit_v1"; out.mkdir(parents=True,exist_ok=False)
    ev=pd.read_parquet(args.events); ft=pd.read_parquet(args.features,columns=["code","ymd"]+FEATURES)
    x=ev.merge(ft,on=["code","ymd"],how="left",validate="one_to_one"); x["year"]=(x.ymd//10000).astype(int)
    rows=[]; diagnostics={}
    for kind,col in (("probe","probe_event"),("add1","add1_event"),("add2","add2_event")):
        diagnostics[kind]={}
        for year in (2023,2024,2025):
            z=x[x[col] & x.year.eq(year)].copy(); diagnostics[kind][str(year)]={"n":int(len(z)),"outcomes":{}}
            for name,label in (("down_first",0),("rebound_first",1),("neutral",2)):
                q=z[z.label_3.eq(label)]
                med={f:(None if q[f].dropna().empty else float(q[f].median())) for f in FEATURES}
                diagnostics[kind][str(year)]["outcomes"][name]={"n":int(len(q)),"medians":med}
            down=z[z.label_3.eq(0)]; rebound=z[z.label_3.eq(1)]
            diffs=[]
            for f in FEATURES:
                a=pd.to_numeric(down[f],errors="coerce").dropna(); b=pd.to_numeric(rebound[f],errors="coerce").dropna(); pooled=pd.to_numeric(z[f],errors="coerce").std()
                if len(a)>=10 and len(b)>=10 and np.isfinite(pooled) and pooled>0:
                    diffs.append({"feature":f,"down_median":float(a.median()),"rebound_median":float(b.median()),"standardized_median_gap":float((a.median()-b.median())/pooled)})
            diffs=sorted(diffs,key=lambda r:abs(r["standardized_median_gap"]),reverse=True)
            diagnostics[kind][str(year)]["largest_down_vs_rebound_gaps"]=diffs[:8]
            for r in diffs: rows.append({"event_kind":kind,"year":year,**r})
    ledger=out/"feature_gap_ledger.parquet"; pd.DataFrame(rows).to_parquet(ledger,index=False)
    compare={"schema_version":"tradex_monthly_env_probe_add_error_audit_v1.compare","artifact_role":"authoritative_diagnostic","decision":"diagnostic_only_no_rule_change","diagnostics":diagnostics,"next_axis_policy":"choose only a pre-existing structural feature with consistent gap sign in 2023/2024/2025; do not tune on 2024 alone","not_changed":["environment rules","probe rules","add rules","MeeMee","ranking","runtime DB"]}
    audit={"schema_version":"tradex_monthly_env_probe_add_error_audit_v1.audit","events":{"path":str(args.events),"sha256":sha(args.events)},"features":{"path":str(args.features),"sha256":sha(args.features)},"boundary":{"owner":"TRADEX","review_only":True,"runtime_db_write":False,"meemee_changed":False}}
    cp=out/"compare.json"; apath=out/"audit.json"; dump(cp,compare); dump(apath,audit); dump(out/"_ARTIFACT_COMPLETE.json",{"complete":True,"compare_sha256":sha(cp),"audit_sha256":sha(apath),"ledger":str(ledger)})
    print(out)


if __name__=="__main__": main()
