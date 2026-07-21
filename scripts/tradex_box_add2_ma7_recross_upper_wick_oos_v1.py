"""One-axis refinement of MA7-recross add2: upper wick >= 25%."""
from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd

def rates(x):
    return {"n":int(len(x)),"down_first":None if x.empty else float(x.label.eq(0).mean()),"rebound_first":None if x.empty else float(x.label.eq(1).mean()),"neutral":None if x.empty else float(x.label.eq(2).mean())}
def main():
    p=argparse.ArgumentParser();p.add_argument("--recross-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-box-add2-ma7-recross-upper-wick-oos-v1";out.mkdir(parents=True,exist_ok=False)
    base=pd.read_parquet(a.recross_ledger);base=base[base.ma7_recross_failure&base.label.notna()].copy()
    ft=pd.read_parquet(a.features,columns=["code","ymd","upper_wick_ratio"]);x=base.merge(ft,on=["code","ymd"],how="left",validate="one_to_one")
    x["upper_wick_rejection"]=x.upper_wick_ratio.ge(.25);q=x[x.upper_wick_rejection]
    years={str(y):{"champion":rates(x[x.year.eq(y)]),"challenger":rates(q[q.year.eq(y)])} for y in range(2019,2027)}
    overall={"champion":rates(x),"challenger":rates(q)};quality=overall["challenger"]["n"]>0 and overall["challenger"]["down_first"]>overall["challenger"]["rebound_first"]
    active=sum(v["challenger"]["n"]>0 for v in years.values());breadth=overall["challenger"]["n"]>=30 and active>=6
    decision="keep" if quality and breadth else ("hold" if quality else "drop")
    anchor=q[(q.code.astype(str).str.zfill(4)=="6532")&q.ymd.eq(20230704)]
    data={"schema_version":"tradex_box_add2_ma7_recross_upper_wick_oos_v1.compare.v1","artifact_role":"authoritative","axis":"add upper_wick_ratio >= 0.25 to fixed BOX add2 MA7 recross failure","threshold_provenance":"0.25 reused from pre-existing high-failure chart contract; no threshold sweep in this run","fixed_conditions":{"years":"2019-2026","primary_outcome":"symmetric fixed 3% h5 first passage","rule_base":"BOX add2, prior close >= prior MA7, bearish close < current MA7","only_changed_axis":"upper wick rejection"},"year_results":years,"overall":overall,"human_anchor_6532":anchor[["code","ymd","upper_wick_ratio","label","hit_day"]].where(pd.notna(anchor),None).to_dict("records"),"judgment":{"decision":decision,"quality_pass":quality,"breadth_pass":breadth,"reason":"hold when direction quality improves but long-history event count remains below 30"},"not_changed":["event generator","MA7 recross rule","monthly environment","candle body/close position","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");x.to_parquet(out/"box_add2_ma7_recross_upper_wick_ledger.parquet",index=False)
    audit={"base_rows":int(len(x)),"challenger_rows":int(len(q)),"missing_upper_wick":int(x.upper_wick_ratio.isna().sum()),"duplicate_event":int(x.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"years":years,"overall":overall,"anchor":data["human_anchor_6532"],"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
