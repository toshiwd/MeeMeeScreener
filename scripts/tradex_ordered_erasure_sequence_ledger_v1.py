"""Build ordered bull -> erasure -> GD -> weak rebound/MA recross sequence episodes."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def fixed3(g,i):
 c=float(g.iloc[i].c)
 for j in range(i+1,min(i+6,len(g))):
  r=g.iloc[j];dn=float(r.l)<=c*.97;up=float(r.h)>=c*1.03
  if dn and up:return "neutral_order_unknown"
  if dn:return "down_first"
  if up:return "rebound_first"
 return "neutral_no_hit"
def max_streak(vals):
 best=cur=0
 for v in vals:
  cur=cur+1 if bool(v) else 0;best=max(best,cur)
 return best
def main():
 p=argparse.ArgumentParser();p.add_argument("--features",type=Path,required=True);p.add_argument("--monthly-ledger",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 x=pd.read_parquet(a.features).sort_values(["code","ymd"]).reset_index(drop=True);x["code"]=x.code.astype(str).str.zfill(4);x["dt"]=pd.to_datetime(x.ymd.astype(str),format="%Y%m%d");x["effective_month"]=x.dt.dt.to_period("M").astype(str)
 m=pd.read_parquet(a.monthly_ledger);m["code"]=m.code.astype(str).str.zfill(4);m["effective_month"]=m.effective_month.astype(str);x=x.merge(m[["code","effective_month","source_month","environment"]],on=["code","effective_month"],how="left",validate="many_to_one")
 rows=[]
 for code,g0 in x.groupby("code",sort=False):
  g=g0.reset_index(drop=True);by_erasure={}
  for i,b in g.iterrows():
   bull_body_atr=(float(b.c)-float(b.o))/float(b.atr14)
   bull=bool(b.c>b.o and b.body_ratio>=.35 and bull_body_atr>=.30 and b.close_pos>=.65)
   if not bull:continue
   er_i=None
   for j in range(i+1,min(i+3,len(g))):
    r=g.iloc[j]
    if r.c<r.o and r.c<=b.o and r.close_pos<=.35:
     er_i=j;break
   if er_i is None:continue
   er=g.iloc[er_i];gd_i=None
   for j in range(er_i+1,min(er_i+4,len(g))):
    if float(g.iloc[j].o/g.iloc[j-1].c-1)<=-.005:
     gd_i=j;break
   start=er_i if gd_i is None else gd_i
   recross_i=ma20_rebreak_i=None
   for j in range(start+2,min(start+11,len(g))):
    prev=g.iloc[j-1];r=g.iloc[j]
    if recross_i is None and prev.c>=prev.ma7 and r.c<r.ma7 and r.c<r.o:
     recross_i=j
    if ma20_rebreak_i is None and prev.c>=prev.ma20 and r.c<r.ma20 and r.c<r.o:
     ma20_rebreak_i=j
   end=max([v for v in [start,recross_i,ma20_rebreak_i] if v is not None]);w=g.iloc[start+1:end+1] if end>start else g.iloc[0:0]
   rebound_ref=float(g.iloc[start].c);recovery_atr=0.0 if w.empty else float((w.c.max()-rebound_ref)/g.iloc[start].atr14);bull_atr=0.0 if w.empty else float(((w.c-w.o).clip(lower=0)/w.atr14).max());weak_rebound=bool(recovery_atr>=.30 and bull_atr<.80)
   above7=max_streak((w.c>w.ma7).tolist()) if not w.empty else 0
   support_room=float(min([v for v in [(er.c-er.ma60)/er.atr14,(er.c-er.ma100)/er.atr14,(er.c-er.ma200)/er.atr14,(er.c-er.support20)/er.atr14] if pd.notna(v) and v>=0],default=float("nan")))
   rec=None if recross_i is None else g.iloc[recross_i];m20=None if ma20_rebreak_i is None else g.iloc[ma20_rebreak_i]
   row={"code":str(code),"year":int(str(int(er.ymd))[:4]),"environment":str(er.environment),"bull_ymd":int(b.ymd),"bull_body_atr":bull_body_atr,"erasure_ymd":int(er.ymd),"erasure_bars_after_bull":int(er_i-i),"erasure_outcome_fixed3_h5":fixed3(g,er_i),"gd_ymd":None if gd_i is None else int(g.iloc[gd_i].ymd),"gd_gap_pct":None if gd_i is None else float(g.iloc[gd_i].o/g.iloc[gd_i-1].c-1),"gd_outcome_fixed3_h5":None if gd_i is None else fixed3(g,gd_i),"post_gd_recovery_atr":recovery_atr,"post_gd_max_bull_body_atr":bull_atr,"weak_rebound":weak_rebound,"post_gd_upper_rejection_count":int(((w.upper_wick_ratio>=.30)|(w.close_pos<=.20)).sum()) if not w.empty else 0,"post_gd_bear_count":int((w.c<w.o).sum()) if not w.empty else 0,"max_consecutive_closes_above_ma7":above7,"ma7_recross_failure_ymd":None if rec is None else int(rec.ymd),"ma7_recross_outcome_fixed3_h5":None if recross_i is None else fixed3(g,recross_i),"ma20_rebreak_ymd":None if m20 is None else int(m20.ymd),"ma20_rebreak_outcome_fixed3_h5":None if ma20_rebreak_i is None else fixed3(g,ma20_rebreak_i),"erasure_nearest_support_room_atr":None if pd.isna(support_room) else support_room}
   old=by_erasure.get(int(er.ymd));
   if old is None or row["bull_body_atr"]>old["bull_body_atr"]:by_erasure[int(er.ymd)]=row
  rows.extend(by_erasure.values())
 seq=pd.DataFrame(rows).sort_values(["code","erasure_ymd"]).reset_index(drop=True)
 anchors={
  "2802":{"available":True,"expected":{"bull_ymd":20240205,"erasure_ymd":20240206,"gd_ymd":20240207},"rows":seq[(seq.code=="2802")&seq.erasure_ymd.eq(20240206)].where(pd.notna(seq),None).to_dict("records")},
  "6532":{"available":True,"expected":{"bull_ymd":20230621,"erasure_ymd":20230623,"gd_ymd":20230626,"ma7_recross_failure_ymd":20230704},"rows":seq[(seq.code=="6532")&seq.erasure_ymd.eq(20230623)].where(pd.notna(seq),None).to_dict("records")},
  "9962":{"available":bool((x.code=="9962").any()),"expected":{"bull_ymd":20260703,"erasure_ymd":20260707,"gd_ymd":20260708,"ma20_rebreak_ymd":20260713},"rows":seq[(seq.code=="9962")&seq.erasure_ymd.eq(20260707)].where(pd.notna(seq),None).to_dict("records")},
 }
 def matches(k):
  z=anchors[k];
  if not z["rows"]:return False
  r=z["rows"][0];return all(r.get(a)==v for a,v in z["expected"].items())
 anchors["2802"]["match"]=matches("2802");anchors["6532"]["match"]=matches("6532");anchors["9962"]["match"]=matches("9962")
 counts={str(y):{"sequences":int((seq.year==y).sum()),"with_gd":int(((seq.year==y)&seq.gd_ymd.notna()).sum()),"with_ma7_recross":int(((seq.year==y)&seq.ma7_recross_failure_ymd.notna()).sum()),"with_ma20_rebreak":int(((seq.year==y)&seq.ma20_rebreak_ymd.notna()).sum())} for y in range(2019,2027)}
 data={"schema_version":"tradex_ordered_erasure_sequence_ledger_v1.compare.v1","artifact_role":"authoritative_diagnostic","review_only":True,"sequence_contract":{"bull_reference":"bullish body_ratio>=0.35, body>=0.30ATR, close_pos>=0.65","erasure":"within 1-2 bars bearish close<=bull open and close_pos<=0.35","gd":"within 3 bars after erasure, gap<=-0.5%","post_stage_window":"up to 10 trading bars","weak_rebound":"recovery>=0.30ATR and maximum bull body<0.80ATR","ma7_recross_failure":"prior close>=prior MA7 then bearish close<MA7","ma20_rebreak":"prior close>=prior MA20 then bearish close<MA20","threshold_sweep":False},"year_counts":counts,"human_anchors":anchors,"judgment":{"decision":"hold","infrastructure_pass":bool(anchors["2802"]["match"] and anchors["6532"]["match"] and anchors["9962"]["match"]),"missing_anchor_codes":[k for k,v in anchors.items() if not v["available"]],"reason":"sequence dates must match available human anchors before stage outcomes are evaluated OOS"},"not_changed":["existing entry families","position lifecycle","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");seq.to_parquet(a.output/"ordered_sequence_ledger.parquet",index=False);audit={"feature_rows":int(len(x)),"sequences":int(len(seq)),"duplicate_erasure":int(seq.duplicated(["code","erasure_ymd"]).sum()),"future_used_for_stage_detection":False,"future_used_for_outcome_only":True,"feature_sha256":sha(a.features),"monthly_sha256":sha(a.monthly_ledger),"review_only":True};(a.output/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"anchors":anchors,"judgment":data["judgment"],"year_counts":counts},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
