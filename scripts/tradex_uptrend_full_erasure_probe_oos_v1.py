"""OOS challenger: mature-uptrend full erasure without large-bull prerequisite."""
from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd

def outcome(g,i):
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
def rates(x):return {"n":int(len(x)),"down_first":None if x.empty else float(x.label.eq(0).mean()),"rebound_first":None if x.empty else float(x.label.eq(1).mean()),"neutral":None if x.empty else float(x.label.eq(2).mean())}
def main():
 p=argparse.ArgumentParser();p.add_argument("--event-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args();stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-uptrend-full-erasure-probe-oos-v2";out.mkdir(parents=True,exist_ok=False)
 ev=pd.read_parquet(a.event_ledger,columns=["code","ymd","environment","probe_event","probe_family"]);emap={(str(r.code).zfill(4),int(r.ymd)):(str(r.environment),bool(r.probe_event),str(r.probe_family)) for r in ev.itertuples()};ft=pd.read_parquet(a.features,columns=["code","ymd","o","h","l","c","ma100"]).sort_values(["code","ymd"]);rows=[]
 for code,g0 in ft.groupby("code",sort=False):
  g=g0.reset_index(drop=True);cc=str(code).zfill(4);above=(g.c>g.ma100).fillna(False);g["above100_streak"]=(above.groupby((~above).cumsum()).cumsum()).astype(int)
  for i in range(1,len(g)):
   r=g.iloc[i];pr=g.iloc[i-1];env,probe,probe_family=emap.get((cc,int(r.ymd)),("AMBIGUOUS",False,"NONE"));candidate=bool(env=="UPTREND" and float(pr.c)>float(pr.o) and float(r.c)<float(r.o) and float(r.c)<=float(pr.o));champion=bool(probe and probe_family=="UPTREND_TOP_FAILED_TRY")
   if not(candidate or champion):continue
   rows.append({"code":cc,"ymd":int(r.ymd),"year":int(str(int(r.ymd))[:4]),"environment":env,"above100_streak":int(r.above100_streak),"prev_o":float(pr.o),"prev_c":float(pr.c),"o":float(r.o),"c":float(r.c),"challenger":candidate,"champion":champion,"label":outcome(g,i)})
 ledger=pd.DataFrame(rows);valid=ledger[ledger.label.notna()];years={}
 for y in range(2019,2027):
  z=valid[valid.year.eq(y)];years[str(y)]={"champion":rates(z[z.champion]),"challenger":rates(z[z.challenger]),"overlap":int((z.champion&z.challenger).sum())}
 q=valid[valid.challenger];quality=all(years[str(y)]["challenger"]["n"]==0 or years[str(y)]["challenger"]["down_first"]>years[str(y)]["challenger"]["rebound_first"] for y in range(2019,2027));breadth=all(years[str(y)]["challenger"]["n"]>=30 for y in (2023,2024,2025));decision="keep" if quality and breadth else "drop";anchor=ledger[(ledger.code=="2802")&ledger.ymd.eq(20240206)]
 data={"schema_version":"tradex_uptrend_full_erasure_probe_oos_v2.compare.v1","artifact_role":"authoritative","axis":"replace strong-retry probe with full erasure of any prior bull body in monthly uptrend, without MA100 streak gate","fixed_conditions":{"environment":"UPTREND","above_ma100_streak_min":None,"prior":"bull candle","current":"bear candle close <= prior open","outcome":"symmetric fixed 3% h5 first passage","no_top_zone_or_W_top_added":True},"year_results":years,"overall":{"champion":rates(valid[valid.champion]),"challenger":rates(q)},"human_anchor_2802":anchor.where(pd.notna(anchor),None).to_dict("records"),"judgment":{"decision":decision,"quality_pass_all_years":quality,"breadth_pass_2023_2025":breadth,"reason":"new probe must have down-first exceed rebound-first in every year and sufficient recent breadth"},"not_changed":["monthly environment","core/add/profit logic","MeeMee","ranking","runtime DB"]};(out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");ledger.to_parquet(out/"uptrend_full_erasure_probe_ledger.parquet",index=False);audit={"union_rows":int(len(ledger)),"challenger_rows":int(ledger.challenger.sum()),"champion_rows":int(ledger.champion.sum()),"missing_outcome":int(ledger.label.isna().sum()),"duplicates":int(ledger.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"future_used_for_outcome_only":True,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8");print(out);print(json.dumps({"years":years,"overall":data["overall"],"anchor":data["human_anchor_2802"],"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
