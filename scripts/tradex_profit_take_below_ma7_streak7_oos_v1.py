"""One-axis profit-take test: first close completing seven bars below MA7."""
from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd

def outcome(g,i):
    if i+5>=len(g):return None,None,None
    c=float(g.iloc[i].c);dn=c*.97;up=c*1.03
    for d in range(1,6):
        r=g.iloc[i+d]
        if float(r.o)<=dn:return 0,"further_down_open",d
        if float(r.o)>=up:return 1,"rebound_open",d
        lo=float(r.l)<=dn;hi=float(r.h)>=up
        if lo and hi:return 2,"same_day_order_unknown",d
        if lo:return 0,"further_down_intraday",d
        if hi:return 1,"rebound_intraday",d
    return 2,"no_3pct_hit",0
def rates(x):
    return {"n":int(len(x)),"further_down_first":None if x.empty else float(x.label.eq(0).mean()),"rebound_first":None if x.empty else float(x.label.eq(1).mean()),"neutral":None if x.empty else float(x.label.eq(2).mean())}
def main():
    p=argparse.ArgumentParser();p.add_argument("--event-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-profit-take-below-ma7-streak7-oos-v1";out.mkdir(parents=True,exist_ok=False)
    ev=pd.read_parquet(a.event_ledger,columns=["code","ymd","position_stage","position_family"]);ft=pd.read_parquet(a.features,columns=["code","ymd","o","h","l","c","ma7"]).sort_values(["code","ymd"])
    state={(str(r.code).zfill(4),int(r.ymd)):(int(r.position_stage),str(r.position_family)) for r in ev.itertuples()};rows=[];anchor_raw=[]
    for code,g0 in ft.groupby("code",sort=False):
        g=g0.reset_index(drop=True);cc=str(code).zfill(4);below=(g.c<g.ma7).fillna(False);g["below7_streak"]=(below.groupby((~below).cumsum()).cumsum()).astype(int)
        for i,r in g.iterrows():
            stage,fam=state.get((cc,int(r.ymd)),(0,"NONE"))
            if cc=="9007" and 20231001<=int(r.ymd)<=20231011 and int(r.below7_streak)==7:
                anchor_raw.append({"code":cc,"ymd":int(r.ymd),"below7_streak":7,"position_stage":stage,"position_family":fam,"eligible_profit_take":stage>=2})
            if stage<2 or int(r.below7_streak)!=7:continue
            label,kind,day=outcome(g,i);rows.append({"code":cc,"ymd":int(r.ymd),"year":int(str(int(r.ymd))[:4]),"family":fam,"position_stage":stage,"below7_streak":7,"c":float(r.c),"ma7":float(r.ma7),"label":label,"outcome_kind":kind,"hit_day":day})
    ledger=pd.DataFrame(rows);valid=ledger[ledger.label.notna()]
    years={str(y):rates(valid[valid.year.eq(y)]) for y in range(2019,2027)};families={fam:rates(valid[valid.family.eq(fam)]) for fam in sorted(valid.family.unique())};overall=rates(valid)
    quality=overall["n"]>0 and overall["rebound_first"]>overall["further_down_first"];breadth=overall["n"]>=50 and sum(v["n"]>=5 for v in years.values())>=6
    stable_all=all(v["n"]==0 or v["rebound_first"]>v["further_down_first"] for v in years.values())
    recent_stable=all(years[str(y)]["rebound_first"]>years[str(y)]["further_down_first"] for y in (2023,2024,2025))
    decision="keep" if quality and breadth and stable_all else ("hold" if quality and recent_stable else "drop")
    anchor=ledger[(ledger.code=="9007")&ledger.ymd.between(20231001,20231011)]
    data={"schema_version":"tradex_profit_take_below_ma7_streak7_oos_v1.compare.v2","artifact_role":"authoritative","axis":"profit-take at first close completing 7 consecutive closes below MA7 while position stage >=2","fixed_conditions":{"years":"2019-2026","outcome":"symmetric fixed 3% h5 first passage","success_direction":"rebound_first exceeds further_down_first","threshold":"exactly streak 7 onset; no sweep"},"year_results":years,"family_results":families,"overall":overall,"human_anchor_9007":{"eligible_rows":anchor.where(pd.notna(anchor),None).to_dict("records"),"raw_feature_rows":anchor_raw,"agreement":"shape_detected_but_position_path_missing"},"judgment":{"decision":decision,"quality_pass":quality,"breadth_pass":breadth,"stable_all_years":stable_all,"recent_2023_2025_stable":recent_stable,"reason":"keep requires rebound dominance in every year; hold when overall and recent years pass but long-history years conflict"},"not_changed":["entry/probe/core/add2","MA/support contact","wick/candle filters","monthly environment","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");ledger.to_parquet(out/"profit_take_below7_ledger.parquet",index=False)
    audit={"events":int(len(ledger)),"complete_outcomes":int(len(valid)),"missing_outcomes":int(ledger.label.isna().sum()),"duplicates":int(ledger.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"future_used_for_outcome_only":True,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"years":years,"families":families,"overall":overall,"anchor":data["human_anchor_9007"],"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
