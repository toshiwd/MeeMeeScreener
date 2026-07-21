"""Profit-take event: long MA touched intraday and held at the close."""
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
def rates(x):return {"n":int(len(x)),"further_down_first":None if x.empty else float(x.label.eq(0).mean()),"rebound_first":None if x.empty else float(x.label.eq(1).mean()),"neutral":None if x.empty else float(x.label.eq(2).mean())}
def main():
 p=argparse.ArgumentParser();p.add_argument("--event-ledger",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args();stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-profit-take-long-ma-touch-hold-oos-v1";out.mkdir(parents=True,exist_ok=False)
 ev=pd.read_parquet(a.event_ledger,columns=["code","ymd","position_stage","position_family"]);state={(str(r.code).zfill(4),int(r.ymd)):(int(r.position_stage),str(r.position_family)) for r in ev.itertuples()};ft=pd.read_parquet(a.features,columns=["code","ymd","o","h","l","c","atr14","ma60","ma100","ma200"]).sort_values(["code","ymd"]);rows=[];anchor=[]
 for code,g0 in ft.groupby("code",sort=False):
  g=g0.reset_index(drop=True);cc=str(code).zfill(4);prev_raw=False;prev_stage=0
  for i,r in g.iterrows():
   stage,fam=state.get((cc,int(r.ymd)),(0,"NONE"));hits=[]
   for ma in ("ma60","ma100","ma200"):
    level=r[ma]
    if pd.notna(level) and float(r.l)<=float(level)+.15*float(r.atr14) and float(r.c)>=float(level):hits.append(ma)
   raw=bool(hits);onset=stage>=2 and raw and not(prev_stage>=2 and prev_raw)
   if cc=="2802" and int(r.ymd)==20240216:anchor.append({"code":cc,"ymd":int(r.ymd),"hit_mas":hits,"raw_touch_hold":raw,"position_stage":stage,"eligible":onset})
   if onset:
    label,kind,day=outcome(g,i);rows.append({"code":cc,"ymd":int(r.ymd),"year":int(str(int(r.ymd))[:4]),"family":fam,"position_stage":stage,"held_mas":"|".join(hits),"label":label,"outcome_kind":kind,"hit_day":day})
   prev_raw=raw;prev_stage=stage
 ledger=pd.DataFrame(rows);valid=ledger[ledger.label.notna()];years={str(y):rates(valid[valid.year.eq(y)]) for y in range(2019,2027)};families={fam:rates(valid[valid.family.eq(fam)]) for fam in sorted(valid.family.unique())};overall=rates(valid);strict=all(v["n"]==0 or v["rebound_first"]>v["further_down_first"] for v in years.values());quality=overall["n"]>0 and overall["rebound_first"]>overall["further_down_first"];breadth=len(valid)>=100;decision="keep" if strict and quality and breadth else ("hold" if quality else "drop")
 data={"schema_version":"tradex_profit_take_long_ma_touch_hold_oos_v1.compare.v1","artifact_role":"authoritative","axis":"first long-MA touch-hold onset while position stage>=2","fixed_conditions":{"touch":"low <= MA + 0.15 ATR and close >= MA","mas":[60,100,200],"outcome":"symmetric fixed 3% h5 first passage","threshold_sweep":False},"year_results":years,"family_results":families,"overall":overall,"human_anchor_2802":anchor,"judgment":{"decision":decision,"strict_rebound_dominance_all_years":strict,"quality_pass":quality,"breadth_pass":breadth,"reason":"profit-take event should precede rebound more often than another 3% decline"},"not_changed":["below7/lower-wick","entry/position generator","monthly environment","MeeMee","ranking","runtime DB"]};(out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");ledger.to_parquet(out/"profit_take_long_ma_touch_hold_ledger.parquet",index=False);audit={"events":int(len(ledger)),"complete":int(len(valid)),"missing":int(ledger.label.isna().sum()),"duplicates":int(ledger.duplicated(["code","ymd"]).sum()),"future_used_for_selection":False,"future_used_for_outcome_only":True,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8");print(out);print(json.dumps({"years":years,"families":families,"overall":overall,"anchor":anchor,"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
