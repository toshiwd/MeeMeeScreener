"""One-axis OOS gate: support-break core only in weak market breadth."""
from __future__ import annotations
import argparse, json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

def rates(x:pd.DataFrame)->dict:
    return {"n":int(len(x)),"down_first_h5":None if x.empty else float(x.core_label_5.eq(0).mean()),
            "rebound_first_h5":None if x.empty else float(x.core_label_5.eq(1).mean()),
            "neutral_h5":None if x.empty else float(x.core_label_5.eq(2).mean())}

def main()->None:
    p=argparse.ArgumentParser();p.add_argument("--band-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-support-break-market-breadth-gate-oos-v1";out.mkdir(parents=True,exist_ok=False)
    core=pd.read_parquet(a.band_ledger);ft=pd.read_parquet(a.features,columns=["code","ymd","market_breadth_ma20"])
    s=core[(core.family=="DOWNTREND_SUPPORT_BREAK") & core.local_band_decisive_break].merge(ft,left_on=["code","core_ymd"],right_on=["code","ymd"],how="left",validate="one_to_one")
    s["weak_breadth_gate"]=s.market_breadth_ma20.lt(.50)
    years={}
    for y in (2023,2024,2025):
        b=s[s.year.eq(y)];q=b[b.weak_breadth_gate]
        years[str(y)]={"champion":rates(b),"challenger":rates(q),"coverage":None if b.empty else float(len(q)/len(b)),"excluded":rates(b[~b.weak_breadth_gate])}
    passed=all(years[str(y)]["challenger"]["n"]>=10 and years[str(y)]["challenger"]["down_first_h5"]>years[str(y)]["challenger"]["rebound_first_h5"] for y in (2023,2024,2025))
    data={"schema_version":"tradex_support_break_market_breadth_gate_oos_v1.compare.v1","artifact_role":"authoritative","axis":"market_breadth_ma20 below 0.50 gate for decisive local support break core","fixed_conditions":{"threshold":0.50,"threshold_basis":"neutral breadth midpoint; no sweep","years":[2023,2024,2025],"horizon":5,"family":"DOWNTREND_SUPPORT_BREAK"},"year_results":years,"judgment":{"decision":"keep" if passed else "drop","reason":"n>=10 and down-first must exceed rebound-first in every year"},"not_changed":["band contract","stock features","monthly environment","other families","probe","candle/MA gates","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");s.to_parquet(out/"support_break_market_breadth_ledger.parquet",index=False)
    audit={"rows":int(len(s)),"missing_breadth":int(s.market_breadth_ma20.isna().sum()),"duplicates":int(s.duplicated(["code","probe_ymd"]).sum()),"future_used":False,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"years":years,"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
