"""One-axis OOS veto: seven consecutive closes below MA7."""
from __future__ import annotations
import argparse, json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

def rates(x: pd.DataFrame) -> dict:
    return {"n":int(len(x)), "down_first_h5":None if x.empty else float(x.core_label_5.eq(0).mean()),
            "rebound_first_h5":None if x.empty else float(x.core_label_5.eq(1).mean())}

def main() -> None:
    ap=argparse.ArgumentParser(); ap.add_argument("--band-ledger",type=Path,required=True)
    ap.add_argument("--features",type=Path,required=True); ap.add_argument("--output-root",type=Path,required=True)
    a=ap.parse_args(); stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out=a.output_root/f"{stamp}-tradex-support-break-below7-veto-oos-v1";out.mkdir(parents=True,exist_ok=False)
    core=pd.read_parquet(a.band_ledger)
    ft=pd.read_parquet(a.features,columns=["code","ymd","c","ma7"]).sort_values(["code","ymd"])
    def add_streak(g:pd.DataFrame)->pd.DataFrame:
        g=g.copy(); below=(g.c<g.ma7).fillna(False); g["below_ma7_streak"]=(below.groupby((~below).cumsum()).cumsum()).astype(int); return g
    ft=pd.concat([add_streak(g) for _,g in ft.groupby("code",sort=False)],ignore_index=True)
    sample=core[(core.family=="DOWNTREND_SUPPORT_BREAK") & core.local_band_decisive_break].merge(
        ft,left_on=["code","core_ymd"],right_on=["code","ymd"],how="left",validate="one_to_one")
    sample["below7_bottom_risk"]=sample.below_ma7_streak.ge(7)
    years={}
    for y in (2023,2024,2025):
        b=sample[sample.year.eq(y)]; q=b[~b.below7_bottom_risk]
        years[str(y)]={"champion":rates(b),"challenger":rates(q),"coverage":None if b.empty else float(len(q)/len(b)),
                       "vetoed":rates(b[b.below7_bottom_risk])}
    pass_all=all(years[str(y)]["challenger"]["n"]>=10 and years[str(y)]["challenger"]["down_first_h5"]>years[str(y)]["challenger"]["rebound_first_h5"] for y in (2023,2024,2025))
    payload={"schema_version":"tradex_support_break_below7_veto_oos_v1.compare.v1","artifact_role":"authoritative",
             "axis":"veto local-support-break core when close-below-MA7 streak is at least 7",
             "fixed_conditions":{"family":"DOWNTREND_SUPPORT_BREAK","local_band_decisive_break":True,"threshold_bars":7,"years":[2023,2024,2025],"horizon":5},
             "year_results":years,"judgment":{"decision":"keep" if pass_all else "drop","reason":"challenger must have n>=10 and down-first exceed rebound-first in every year"},
             "not_changed":["band contract","monthly environment","other families","probe","MA proximity","candle shape","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    sample.to_parquet(out/"support_break_below7_ledger.parquet",index=False)
    audit={"rows":int(len(sample)),"missing_streak":int(sample.below_ma7_streak.isna().sum()),"duplicates":int(sample.duplicated(["code","probe_ymd"]).sum()),"future_used":False,"review_only":True}
    (out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"years":years,"judgment":payload["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__": main()
