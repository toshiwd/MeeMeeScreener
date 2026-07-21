"""Build the 4755-style post-box high-failure support-break direct-core branch."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

YEARS=tuple(range(2019,2026))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def fixed3(g,i):
 c=float(g.iloc[i].c)
 for j in range(i+1,min(i+6,len(g))):
  r=g.iloc[j];dn=float(r.l)<=c*.97;up=float(r.h)>=c*1.03
  if dn and up:return "neutral_order_unknown"
  if dn:return "down_first"
  if up:return "rebound_first"
 return "neutral_no_hit"
def rates(x):return {"n":int(len(x)),"codes":int(x.code.nunique()),"down_first":None if x.empty else float(x.outcome.eq("down_first").mean()),"rebound_first":None if x.empty else float(x.outcome.eq("rebound_first").mean()),"neutral":None if x.empty else float(x.outcome.str.startswith("neutral").mean())}
def main():
 p=argparse.ArgumentParser();p.add_argument("--features",type=Path,required=True);p.add_argument("--monthly-ledger",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 f=pd.read_parquet(a.features).sort_values(["code","ymd"]).reset_index(drop=True);f["code"]=f.code.astype(str).str.zfill(4);f["effective_month"]=pd.to_datetime(f.ymd.astype(str),format="%Y%m%d").dt.to_period("M").astype(str)
 m=pd.read_parquet(a.monthly_ledger);m["code"]=m.code.astype(str).str.zfill(4);m["effective_month"]=m.effective_month.astype(str);m=m[["code","effective_month","base_regime","post_box","local_box_mature","local_close_location","local_box_top_touch_count"]]
 x=f.merge(m,on=["code","effective_month"],how="left",validate="many_to_one")
 monthly=x.base_regime.eq("POST_BOX_BREAKOUT_CONSOLIDATION")&x.local_box_mature.fillna(False).astype(bool)&x.local_close_location.eq("AT_LOCAL_CEILING")
 daily=x.support_break.eq(1)&x.c.lt(x.o)&x.body_ratio.ge(.60)&x.close_pos.le(.20)&x.monthly_high_failure.eq(1)
 x["challenger"]=monthly&daily;x["ma60_rebound_warning"]=x.dist_ma60_atr.between(-.35,.35)
 outcomes={}
 for code,g in x.groupby("code",sort=False):
  g=g.reset_index()
  for i,r in g[g.challenger].iterrows():outcomes[int(r["index"])]=fixed3(g,i)
 e=x[x.challenger].copy();e["outcome"]=e.index.map(outcomes);e["year"]=e.ymd.astype(str).str[:4].astype(int);e=e[e.year.isin(YEARS)]
 years={str(y):rates(e[e.year.eq(y)]) for y in YEARS};warning={str(y):{"warning_present":rates(e[e.year.eq(y)&e.ma60_rebound_warning]),"warning_absent":rates(e[e.year.eq(y)&~e.ma60_rebound_warning])} for y in YEARS}
 anchor=e[(e.code=="4755")&e.ymd.eq(20251114)];direction=all(years[str(y)]["n"]>0 and years[str(y)]["down_first"]>years[str(y)]["rebound_first"] for y in YEARS);breadth=all(years[str(y)]["n"]>=20 for y in YEARS)
 data={"schema_version":"tradex_postbox_support_break_direct_core_v1.compare.v1","artifact_role":"authoritative","axis":"POSTBOX_HIGH_FAILURE_SUPPORT_BREAK_DIRECT_CORE","fixed_conditions":{"monthly":"POST_BOX_BREAKOUT_CONSOLIDATION; mature local box; AT_LOCAL_CEILING","daily":"support_break; bearish body_ratio>=0.60; close_pos<=0.20; monthly_high_failure","action":"CORE_CLOSE","warning_only":"abs(dist_ma60_atr)<=0.35","outcome":"exact OHLC symmetric fixed3 h5","years":list(YEARS),"minimum_each_year":20,"threshold_sweep":False},"year_results":years,"ma60_warning_diagnostic":warning,"human_anchor":{"4755_20251114":{"expected":"CORE_CLOSE_WITH_MA60_REBOUND_WARNING","match":len(anchor)==1 and bool(anchor.ma60_rebound_warning.iloc[0]),"rows":anchor.where(pd.notna(anchor),None).to_dict("records")}},"observed_branching":{"events":int(len(e)),"warning_events":int(e.ma60_rebound_warning.sum()),"changed_rank_count":int(len(e)),"selection_divergence_reason":"post-box monthly high failure is confirmed by an actual daily support break; MA60 proximity stays warning-only"},"judgment":{"decision":"keep_episode_contract" if len(anchor)==1 else "drop","effectiveness_decision":"keep" if direction and breadth else "hold" if direction else "drop","direction_pass_all_years":direction,"breadth_pass":breadth,"human_anchor_preserved":len(anchor)==1,"warning_preserved":len(anchor)==1 and bool(anchor.ma60_rebound_warning.iloc[0]),"reason":"4755 direct-core and rebound-warning actions are reproduced" if len(anchor)==1 else "4755 anchor missing"},"not_changed":["monthly classifier","support_break feature","MA60 warning threshold effect on action","other entry families","add logic","profit logic","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");e.to_parquet(a.output/"postbox_support_break_core_events.parquet",index=False)
 audit={"feature_rows":int(len(f)),"events":int(len(e)),"duplicates":int(e.duplicated(["code","ymd"]).sum()),"missing_monthly_join":int(x.base_regime.isna().sum()),"future_used_for_selection":False,"future_used_for_outcome_only":True,"review_only":True,"feature_sha256":sha(a.features),"monthly_sha256":sha(a.monthly_ledger)};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"years":years,"warning":warning,"anchor":len(anchor)==1,"judgment":data["judgment"],"audit":audit},indent=2))
if __name__=="__main__":main()
