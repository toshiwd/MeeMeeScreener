"""One-axis OOS core promotion for BOX_CEILING_ERASURE.

Promote probe to core only when the later bar opens below the probe low and
also closes below that fixed low.  This reproduces the 6532 2023-06-26 path
without using later add dates.
"""
from __future__ import annotations
import argparse, json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

def rates(core:pd.DataFrame, probe_n:int)->dict:
    if core.empty:return {"core_n":0,"down_first_h5":None,"rebound_first_h5":None,"neutral_h5":None,"end_to_end_probe_core_down":0.0}
    down=core.core_label_5.eq(0);reb=core.core_label_5.eq(1)
    return {"core_n":int(len(core)),"down_first_h5":float(down.mean()),"rebound_first_h5":float(reb.mean()),"neutral_h5":float((~(down|reb)).mean()),"end_to_end_probe_core_down":float(down.sum()/probe_n) if probe_n else None}

def main()->None:
    p=argparse.ArgumentParser();p.add_argument("--episodes",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-box-erasure-probe-low-gap-core-oos-v1";out.mkdir(parents=True,exist_ok=False)
    ep=pd.read_parquet(a.episodes);box=ep[ep.family.eq("BOX_CEILING_ERASURE")].copy()
    ft=pd.read_parquet(a.features,columns=["code","ymd","o","l","c"])
    probe=ft.rename(columns={"ymd":"probe_ymd","l":"probe_l","o":"probe_o","c":"probe_close"})[["code","probe_ymd","probe_o","probe_l","probe_close"]]
    box=box.merge(probe,on=["code","probe_ymd"],how="left",validate="many_to_one")
    coreft=ft.rename(columns={"ymd":"core_ymd","o":"core_o","l":"core_l","c":"core_close"})[["code","core_ymd","core_o","core_l","core_close"]]
    have=box[box.core_ymd.notna()].copy();have["core_ymd"]=have.core_ymd.astype(int)
    have=have.merge(coreft,on=["code","core_ymd"],how="left",validate="one_to_one")
    have["probe_low_gap_break_hold"]=have.core_o.lt(have.probe_l) & have.core_close.lt(have.probe_l)
    years={}
    for y in (2023,2024,2025):
        probes=box[box.year.eq(y)];base=have[have.year.eq(y)];q=base[base.probe_low_gap_break_hold]
        years[str(y)]={"probe_n":int(len(probes)),"champion":rates(base,len(probes)),"challenger":rates(q,len(probes)),"coverage_of_current_core":None if base.empty else float(len(q)/len(base)),"excluded_current_core":rates(base[~base.probe_low_gap_break_hold],len(probes))}
    passed=all(years[str(y)]["challenger"]["core_n"]>=10 and years[str(y)]["challenger"]["down_first_h5"]>years[str(y)]["challenger"]["rebound_first_h5"] for y in (2023,2024,2025))
    anchor=have[(have.code.astype(str).str.zfill(4)=="6532") & have.probe_ymd.eq(20230623)]
    anchor_rows=anchor[["code","probe_ymd","core_ymd","probe_l","core_o","core_close","probe_low_gap_break_hold","core_label_5"]].where(pd.notna(anchor),None).to_dict("records")
    data={"schema_version":"tradex_box_erasure_probe_low_gap_core_oos_v1.compare.v1","artifact_role":"authoritative","axis":"BOX_CEILING_ERASURE core requires open and close below fixed probe-bar low","fixed_conditions":{"family":"BOX_CEILING_ERASURE","current_core_dates_only":True,"horizon":5,"years":[2023,2024,2025],"costs_ignored":True},"year_results":years,"human_anchor":{"case":"6532 probe 20230623 then add 20230626","rows":anchor_rows},"judgment":{"decision":"keep" if passed else "drop","reason":"n>=10 and down-first must exceed rebound-first in every year"},"not_changed":["probe trigger","monthly environment","core date timing","other families","add2","candle/MA gates","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");have.to_parquet(out/"box_erasure_probe_low_gap_core_ledger.parquet",index=False)
    audit={"box_probe_rows":int(len(box)),"current_core_rows":int(len(have)),"missing_probe_low":int(have.probe_l.isna().sum()),"missing_core_ohlc":int(have.core_o.isna().sum()),"duplicate_episode":int(have.duplicated(["code","probe_ymd"]).sum()),"future_used":False,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"years":years,"anchor":anchor_rows,"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
