"""One-axis refinement: confirmed W-top before uptrend full erasure."""
from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
def rates(x):return {"n":int(len(x)),"down_first":None if x.empty else float(x.label.eq(0).mean()),"rebound_first":None if x.empty else float(x.label.eq(1).mean()),"neutral":None if x.empty else float(x.label.eq(2).mean())}
def main():
 p=argparse.ArgumentParser();p.add_argument("--erasure-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args();stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-uptrend-full-erasure-lower-high-wtop-probe-oos-v1";out.mkdir(parents=True,exist_ok=False)
 base=pd.read_parquet(a.erasure_ledger);base=base[base.challenger&base.label.notna()].copy();targets={(str(r.code).zfill(4),int(r.ymd)) for r in base.itertuples()};ft=pd.read_parquet(a.features,columns=["code","ymd","h"]).sort_values(["code","ymd"]);rows=[]
 for code,g0 in ft.groupby("code",sort=False):
  g=g0.reset_index(drop=True);cc=str(code).zfill(4)
  for i in range(len(g)):
   y=int(g.iloc[i].ymd)
   if (cc,y) not in targets:continue
   piv=[]
   for j in range(max(2,i-60),i-2):
    if float(g.iloc[j].h)>=float(g.iloc[j-2:j].h.max()) and float(g.iloc[j].h)>float(g.iloc[j+1:j+3].h.max()):piv.append(j)
   wtop=False;p1=p2=None
   if len(piv)>=2:
    p1,p2=piv[-2],piv[-1];h1=float(g.iloc[p1].h);h2=float(g.iloc[p2].h);wtop=bool(p2-p1>=5 and .95*h1<=h2<=h1 and i-p2<=10)
   rows.append({"code":cc,"ymd":y,"wtop":wtop,"pivot1_ymd":None if p1 is None else int(g.iloc[p1].ymd),"pivot1_h":None if p1 is None else float(g.iloc[p1].h),"pivot2_ymd":None if p2 is None else int(g.iloc[p2].ymd),"pivot2_h":None if p2 is None else float(g.iloc[p2].h),"bars_after_pivot2":None if p2 is None else i-p2})
 state=pd.DataFrame(rows);x=base.merge(state,on=["code","ymd"],how="left",validate="one_to_one");q=x[x.wtop.fillna(False)];years={str(y):{"champion":rates(x[x.year.eq(y)]),"challenger":rates(q[q.year.eq(y)])} for y in range(2019,2027)};overall={"champion":rates(x),"challenger":rates(q)};quality=all(v["challenger"]["n"]==0 or v["challenger"]["down_first"]>v["challenger"]["rebound_first"] for v in years.values());breadth=all(years[str(y)]["challenger"]["n"]>=10 for y in (2023,2024,2025));decision="keep" if quality and breadth else "drop";anchor=x[(x.code=="2802")&x.ymd.eq(20240206)]
 data={"schema_version":"tradex_uptrend_full_erasure_lower_high_wtop_probe_oos_v1.compare.v1","artifact_role":"authoritative","axis":"add confirmed lower-high W-top to monthly-UPTREND full erasure","fixed_conditions":{"pivot":"left2/right2 confirmed by t-1","lookback":60,"two_latest_pivots_separation_min":5,"second_high_ratio_to_first":[0.95,1.00],"max_bars_after_second":10,"outcome":"symmetric fixed 3% h5","threshold_sweep":False},"year_results":years,"overall":overall,"human_anchor_2802":anchor.where(pd.notna(anchor),None).to_dict("records"),"judgment":{"decision":decision,"quality_pass_all_years":quality,"breadth_pass_2023_2025":breadth,"reason":"lower-high W-top refinement must preserve down dominance every year with recent breadth"},"not_changed":["full-erasure definition","monthly environment","entry lifecycle","MeeMee","ranking","runtime DB"]};(out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");x.to_parquet(out/"uptrend_full_erasure_wtop_probe_ledger.parquet",index=False);audit={"base_rows":int(len(x)),"challenger_rows":int(len(q)),"missing_state":int(x.wtop.isna().sum()),"duplicates":int(x.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"pivot_confirmation_right_bars":2,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8");print(out);print(json.dumps({"years":years,"overall":overall,"anchor":data["human_anchor_2802"],"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
