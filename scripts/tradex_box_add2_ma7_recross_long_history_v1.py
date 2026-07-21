"""Long-history validation of the fixed BOX add2 MA7 recross rule."""
from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd

def outcome(g:pd.DataFrame,i:int):
    if i+5>=len(g):return None,None,None
    entry=float(g.iloc[i].c);dn=entry*.97;up=entry*1.03
    for day in range(1,6):
        r=g.iloc[i+day]
        if float(r.o)<=dn:return 0,"down_open_gap",day
        if float(r.o)>=up:return 1,"rebound_open_gap",day
        dh=float(r.l)<=dn;uh=float(r.h)>=up
        if dh and uh:return 2,"neutral_same_day_order_unknown",day
        if dh:return 0,"down_intraday",day
        if uh:return 1,"rebound_intraday",day
    return 2,"neutral_no_hit",0
def rates(x):
    return {"n":int(len(x)),"down_first":None if x.empty else float(x.label.eq(0).mean()),"rebound_first":None if x.empty else float(x.label.eq(1).mean()),"neutral":None if x.empty else float(x.label.eq(2).mean())}
def main():
    p=argparse.ArgumentParser();p.add_argument("--event-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-box-add2-ma7-recross-long-history-v1";out.mkdir(parents=True,exist_ok=False)
    ev=pd.read_parquet(a.event_ledger,columns=["code","ymd","position_family","add2_event"]);ev=ev[(ev.position_family=="BOX_CEILING_ERASURE")&ev.add2_event].copy()
    ft=pd.read_parquet(a.features,columns=["code","ymd","o","h","l","c","ma7"]).sort_values(["code","ymd"])
    rows=[]
    targets={(str(r.code).zfill(4),int(r.ymd)) for r in ev.itertuples()}
    for code,g0 in ft.groupby("code",sort=False):
        g=g0.reset_index(drop=True);keycode=str(code).zfill(4)
        for i in range(1,len(g)):
            ymd=int(g.iloc[i].ymd)
            if (keycode,ymd) not in targets:continue
            r=g.iloc[i];prev=g.iloc[i-1];signal=bool(float(prev.c)>=float(prev.ma7) and float(r.c)<float(r.ma7) and float(r.c)<float(r.o))
            label,kind,day=outcome(g,i)
            rows.append({"code":keycode,"ymd":ymd,"year":int(str(ymd)[:4]),"prev_c":float(prev.c),"prev_ma7":float(prev.ma7),"o":float(r.o),"c":float(r.c),"ma7":float(r.ma7),"ma7_recross_failure":signal,"label":label,"outcome_kind":kind,"hit_day":day})
    ledger=pd.DataFrame(rows);selected=ledger[ledger.ma7_recross_failure & ledger.label.notna()].copy()
    years={str(y):rates(selected[selected.year.eq(y)]) for y in range(2019,2027)};overall=rates(selected)
    active=sum(v["n"]>0 for v in years.values());stable=all(v["n"]<3 or v["down_first"]>=v["rebound_first"] for v in years.values())
    quality=overall["n"]>0 and overall["down_first"]>overall["rebound_first"] and stable
    breadth=overall["n"]>=30 and active>=6
    decision="keep" if quality and breadth else ("hold" if quality and active>=5 else "drop")
    data={"schema_version":"tradex_box_add2_ma7_recross_long_history_v1.compare.v1","artifact_role":"authoritative","axis":"unchanged BOX add2 MA7 recross failure, extended history only","fixed_conditions":{"years":"2019-2026","rule_unchanged":True,"outcome":"symmetric fixed 3% h5 first passage","keep_gate":{"total_n_min":30,"active_years_min":6,"overall_down_gt_rebound":True,"no_year_n_ge_3_with_rebound_gt_down":True}},"year_results":years,"overall":overall,"active_years":active,"judgment":{"decision":decision,"quality_pass":quality,"breadth_pass":breadth,"reason":"long-history validation changes period only; no signal threshold changed"},"not_changed":["event generator","probe/core/add2 timing","MA7 rule","monthly environment","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");ledger.to_parquet(out/"box_add2_ma7_recross_long_history_ledger.parquet",index=False)
    audit={"source_add2_events":int(len(ev)),"joined_events":int(len(ledger)),"selected_complete_outcomes":int(len(selected)),"missing_outcomes":int(ledger.label.isna().sum()),"duplicate_event":int(ledger.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"future_used_for_outcome_only":True,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"years":years,"overall":overall,"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
