"""One-axis profit-take refinement: below7 onset near long support/MA."""
from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd

def rates(x):
    return {"n":int(len(x)),"further_down_first":None if x.empty else float(x.label.eq(0).mean()),"rebound_first":None if x.empty else float(x.label.eq(1).mean()),"neutral":None if x.empty else float(x.label.eq(2).mean())}
def main():
    p=argparse.ArgumentParser();p.add_argument("--below7-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args();stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-profit-take-below7-long-support-touch-oos-v1";out.mkdir(parents=True,exist_ok=False)
    b=pd.read_parquet(a.below7_ledger);f=pd.read_parquet(a.features,columns=["code","ymd","c","atr14","ma60","ma100","ma200","support20"]);x=b.merge(f,on=["code","ymd"],how="left",validate="one_to_one")
    sources=["ma60","ma100","ma200","support20"]
    def nearest(r):
        vals={k:abs(float(r.c_y)-float(r[k]))/float(r.atr14) for k in sources if pd.notna(r[k]) and pd.notna(r.atr14) and float(r.atr14)>0}
        return (min(vals.values()),min(vals,key=vals.get)) if vals else (None,None)
    n=x.apply(nearest,axis=1);x["nearest_long_support_abs_atr"]=[v[0] for v in n];x["nearest_long_support_source"]=[v[1] for v in n];x["long_support_touch"]=x.nearest_long_support_abs_atr.le(.35);q=x[x.long_support_touch]
    years={str(y):{"champion":rates(x[x.year.eq(y)]),"challenger":rates(q[q.year.eq(y)])} for y in range(2019,2027)};overall={"champion":rates(x),"challenger":rates(q)}
    stable=all(v["challenger"]["n"]==0 or v["challenger"]["rebound_first"]>v["challenger"]["further_down_first"] for v in years.values());improved=overall["challenger"]["rebound_first"]>overall["champion"]["rebound_first"] and overall["challenger"]["further_down_first"]<overall["champion"]["further_down_first"]
    decision="keep" if stable and improved else "drop"
    anchor=x[(x.code.astype(str).str.zfill(4)=="9007")&x.ymd.eq(20231004)]
    data={"schema_version":"tradex_profit_take_below7_long_support_touch_oos_v1.compare.v1","artifact_role":"authoritative","axis":"add proximity <=0.35 ATR to MA60/100/200 or prior support20 at below7 onset","threshold_provenance":"existing room-veto proximity; no sweep","year_results":years,"overall":overall,"human_anchor_9007":anchor[["code","ymd","nearest_long_support_abs_atr","nearest_long_support_source","long_support_touch"]].where(pd.notna(anchor),None).to_dict("records"),"judgment":{"decision":decision,"stable_all_years":stable,"improved_overall":improved,"reason":"keep requires higher rebound, lower further-down, and rebound dominance every year"},"not_changed":["below7 definition","entry/position path","lower wick","monthly environment","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");x.to_parquet(out/"profit_take_below7_long_support_touch_ledger.parquet",index=False)
    audit={"base_rows":int(len(x)),"challenger_rows":int(len(q)),"missing_distance":int(x.nearest_long_support_abs_atr.isna().sum()),"duplicates":int(x.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"years":years,"overall":overall,"anchor":data["human_anchor_9007"],"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
