"""Validate the fixed MA7-recross + upper-wick add2 shape by setup family."""
from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd

def result(g,i):
    if i+5>=len(g):return None
    c=float(g.iloc[i].c);dn=c*.97;up=c*1.03
    for d in range(1,6):
        r=g.iloc[i+d]
        if float(r.o)<=dn:return 0
        if float(r.o)>=up:return 1
        lo=float(r.l)<=dn;hi=float(r.h)>=up
        if lo and hi:return 2
        if lo:return 0
        if hi:return 1
    return 2
def rates(x):
    return {"n":int(len(x)),"down_first":None if x.empty else float(x.label.eq(0).mean()),"rebound_first":None if x.empty else float(x.label.eq(1).mean()),"neutral":None if x.empty else float(x.label.eq(2).mean())}
def main():
    p=argparse.ArgumentParser();p.add_argument("--event-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-add2-ma7-recross-upper-wick-family-validation-v1";out.mkdir(parents=True,exist_ok=False)
    ev=pd.read_parquet(a.event_ledger,columns=["code","ymd","position_family","add2_event"]);ev=ev[ev.add2_event].copy();targets={(str(r.code).zfill(4),int(r.ymd)):str(r.position_family) for r in ev.itertuples()}
    ft=pd.read_parquet(a.features,columns=["code","ymd","o","h","l","c","ma7","upper_wick_ratio"]).sort_values(["code","ymd"]);rows=[]
    for code,g0 in ft.groupby("code",sort=False):
        g=g0.reset_index(drop=True);cc=str(code).zfill(4)
        for i in range(1,len(g)):
            y=int(g.iloc[i].ymd);fam=targets.get((cc,y))
            if fam is None:continue
            r=g.iloc[i];pr=g.iloc[i-1];base=bool(float(pr.c)>=float(pr.ma7) and float(r.c)<float(r.ma7) and float(r.c)<float(r.o));wick=bool(float(r.upper_wick_ratio)>=.25)
            rows.append({"code":cc,"ymd":y,"year":int(str(y)[:4]),"family":fam,"ma7_recross_failure":base,"upper_wick_rejection":wick,"selected":base and wick,"upper_wick_ratio":float(r.upper_wick_ratio),"label":result(g,i)})
    ledger=pd.DataFrame(rows);selected=ledger[ledger.selected&ledger.label.notna()]
    families={}
    for fam in sorted(ledger.family.unique()):
        s=selected[selected.family.eq(fam)];yrs={str(y):rates(s[s.year.eq(y)]) for y in range(2019,2027)};overall=rates(s);active=sum(v["n"]>0 for v in yrs.values());quality=overall["n"]>0 and overall["down_first"]>overall["rebound_first"] and all(v["n"]<3 or v["down_first"]>=v["rebound_first"] for v in yrs.values());breadth=overall["n"]>=30 and active>=6;decision="keep" if quality and breadth else ("hold" if quality else "drop");families[fam]={"year_results":yrs,"overall":overall,"active_years":active,"judgment":{"decision":decision,"quality_pass":quality,"breadth_pass":breadth}}
    data={"schema_version":"tradex_add2_ma7_recross_upper_wick_family_validation_v1.compare.v1","artifact_role":"authoritative","axis":"unchanged add2 MA7 recross bearish candle plus upper wick >=25%, evaluated by family","fixed_conditions":{"years":"2019-2026","outcome":"symmetric fixed 3% h5 first passage","threshold_provenance":"existing high-failure contract","scope_change_only":"apply unchanged shape to each existing position family"},"family_results":families,"judgment":{"decision":"family_specific","reason":"do not pool families; each receives independent keep/drop/hold"},"not_changed":["event generator","add2 dates","monthly environment","thresholds","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");ledger.to_parquet(out/"add2_ma7_recross_upper_wick_family_ledger.parquet",index=False)
    audit={"source_add2_events":int(len(ev)),"joined_events":int(len(ledger)),"selected_complete":int(len(selected)),"missing_outcomes":int(ledger.label.isna().sum()),"duplicates":int(ledger.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"future_used_for_outcome_only":True,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"families":families,"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
