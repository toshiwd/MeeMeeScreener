"""One-axis OOS test of MA7 recross failure for BOX add2."""
from __future__ import annotations
import argparse, json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

def rates(x:pd.DataFrame,label:str)->dict:
    if x.empty:return {"n":0,"down_first":None,"rebound_first":None,"neutral":None}
    return {"n":int(len(x)),"down_first":float(x[label].eq(0).mean()),"rebound_first":float(x[label].eq(1).mean()),"neutral":float(x[label].eq(2).mean())}

def main()->None:
    p=argparse.ArgumentParser();p.add_argument("--fixed3-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-box-add2-ma7-recross-failure-oos-v1";out.mkdir(parents=True,exist_ok=False)
    ep=pd.read_parquet(a.fixed3_ledger); ft=pd.read_parquet(a.features,columns=["code","ymd","o","c","ma7","body_ratio","close_pos"]).sort_values(["code","ymd"])
    grp=ft.groupby("code",sort=False);ft["prev_c"]=grp.c.shift(1);ft["prev_ma7"]=grp.ma7.shift(1)
    z=ep[(ep.family=="BOX_CEILING_ERASURE")&ep.add2_ymd.notna()].copy();z["add2_ymd"]=z.add2_ymd.astype(int)
    z=z.merge(ft,left_on=["code","add2_ymd"],right_on=["code","ymd"],how="left",validate="one_to_one")
    z["ma7_recross_failure"]=z.prev_c.ge(z.prev_ma7)&z.c.lt(z.ma7)&z.c.lt(z.o)
    years={}
    for y in (2023,2024,2025):
        b=z[z.add2_action_year.eq(y)];q=b[b.ma7_recross_failure]
        years[str(y)]={"champion_fixed3":rates(b,"add2_fixed3_label_5"),"challenger_fixed3":rates(q,"add2_fixed3_label_5"),"coverage":None if b.empty else float(len(q)/len(b))}
    quality_all=all(years[str(y)]["challenger_fixed3"]["n"]>0 and years[str(y)]["challenger_fixed3"]["down_first"]>years[str(y)]["challenger_fixed3"]["rebound_first"] for y in (2023,2024,2025))
    breadth_all=all(years[str(y)]["challenger_fixed3"]["n"]>=10 for y in (2023,2024,2025))
    decision="keep" if quality_all and breadth_all else ("hold" if quality_all else "drop")
    anchor=z[(z.code.astype(str).str.zfill(4)=="6532")&z.add2_ymd.eq(20230704)]
    cols=["code","probe_ymd","core_ymd","add2_ymd","prev_c","prev_ma7","o","c","ma7","ma7_recross_failure","add2_fixed3_label_5","add2_fixed3_hit_day_5"]
    data={"schema_version":"tradex_box_add2_ma7_recross_failure_oos_v1.compare.v2","artifact_role":"authoritative","axis":"BOX_CEILING_ERASURE add2 requires prior close above MA7 then bearish close below MA7","fixed_conditions":{"years":[2023,2024,2025],"year_attribution":"add2 action date","family":"BOX_CEILING_ERASURE","existing_add2_dates_only":True,"primary_outcome":"fixed symmetric 3% h5 first passage","threshold_sweep":False},"year_results":years,"human_anchor_6532":anchor[cols].where(pd.notna(anchor),None).to_dict("records"),"judgment":{"decision":decision,"quality_pass_all_years":quality_all,"breadth_pass_all_years":breadth_all,"reason":"hold when direction quality passes every year but any year has fewer than 10 events; keep requires both quality and breadth"},"metric_note":"add2 ATR-scaled outcome is not available in the source episode ledger; this axis is judged only by the authoritative fixed3 add2 outcome","not_changed":["probe/core/add2 dates","monthly environment","other families","support bands","candle thresholds","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");z.to_parquet(out/"box_add2_ma7_recross_ledger.parquet",index=False)
    audit={"box_add2_rows":int(len(z)),"missing_ma7":int(z.ma7.isna().sum()),"missing_prev_ma7":int(z.prev_ma7.isna().sum()),"duplicate_episode":int(z.duplicated(["code","probe_ymd"]).sum()),"future_used_for_selection":False,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"years":years,"anchor":data["human_anchor_6532"],"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
