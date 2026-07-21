"""Direct-core short after post-box large GD and support break, with separate rebound risk."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS=(2023,2024,2025)
def sha(p:Path)->str:return hashlib.sha256(p.read_bytes()).hexdigest()
def outcome(g,i):
    c=float(g.iloc[i].c)
    for j in range(i+1,min(i+6,len(g))):
        r=g.iloc[j];dn=float(r.l)<=c*.97;up=float(r.h)>=c*1.03
        if dn and up:return "neutral_order_unknown"
        if dn:return "down_first"
        if up:return "rebound_first"
    return "neutral_no_hit"
def rates(x):return {"n":int(len(x)),"codes":int(x.code.nunique()),"down_first":None if x.empty else float(x.outcome_fixed3_h5.eq("down_first").mean()),"rebound_first":None if x.empty else float(x.outcome_fixed3_h5.eq("rebound_first").mean()),"neutral":None if x.empty else float(x.outcome_fixed3_h5.str.startswith("neutral").mean())}
def main():
    p=argparse.ArgumentParser();p.add_argument("--features",type=Path,required=True);p.add_argument("--monthly-ledger",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    x=pd.read_parquet(a.features).sort_values(["code","ymd"]).reset_index(drop=True);x["code"]=x.code.astype(str).str.zfill(4);x["dt"]=pd.to_datetime(x.ymd.astype(str),format="%Y%m%d");x["effective_month"]=x.dt.dt.to_period("M").astype(str)
    m=pd.read_parquet(a.monthly_ledger);m["code"]=m.code.astype(str).str.zfill(4);m["effective_month"]=m.effective_month.astype(str);x=x.merge(m[["code","effective_month","source_month","environment"]],on=["code","effective_month"],how="left",validate="many_to_one")
    grp=x.groupby("code",sort=False);pc=grp.c.shift(1);pma20=grp.ma20.shift(1);pma60=grp.ma60.shift(1);ppos20=grp.pos20.shift(1);x["gap_pct_calc"]=x.o/pc-1
    x["direct_core_raw"]=(x.environment.eq("POST_BOX_BREAKOUT_CONSOLIDATION")&(ppos20>=.70)&(pc>pma20)&(pc>pma60)&(x.gap_pct_calc<=-.02)&x.support_break.eq(1)&(x.c<x.ma20)&(x.c<x.o)&(x.body_ratio>=.60)&(x.close_pos<=.20))
    x["ma60_rebound_risk"]=((x.c-x.ma60).abs()/x.atr14<=.35)|x.oversold_risk.eq(1)
    rows=[]
    for code,g0 in x.groupby("code",sort=False):
        g=g0.reset_index(drop=True);last=-99
        for i,r in g.iterrows():
            if bool(r.direct_core_raw) and i-last>10:
                rows.append({"code":str(code),"ymd":int(r.ymd),"year":int(str(int(r.ymd))[:4]),"environment":str(r.environment),"gap_pct":float(r.gap_pct_calc),"dist_ma60_atr":float((r.c-r.ma60)/r.atr14),"ma60_rebound_risk":bool(r.ma60_rebound_risk),"outcome_fixed3_h5":outcome(g,i)});last=i
    ev=pd.DataFrame(rows);results={str(y):{"all":rates(ev[ev.year.eq(y)]),"risk_flagged":rates(ev[ev.year.eq(y)&ev.ma60_rebound_risk]),"risk_clear":rates(ev[ev.year.eq(y)&~ev.ma60_rebound_risk])} for y in YEARS}
    anchor=ev[(ev.code=="4755")&ev.ymd.eq(20251114)].where(pd.notna(ev),None).to_dict("records");breadth=all(results[str(y)]["all"]["n"]>=30 for y in YEARS);positive=breadth and all(results[str(y)]["all"]["down_first"]>results[str(y)]["all"]["rebound_first"] for y in YEARS);anchor_hit=len(anchor)==1 and anchor[0]["ma60_rebound_risk"] and anchor[0]["outcome_fixed3_h5"]=="down_first"
    data={"schema_version":"tradex_post_box_gap_support_break_direct_core_oos_v1.compare.v1","artifact_role":"authoritative","axis":"POST_BOX large GD support-break direct core with separate MA60 rebound-risk flag","fixed_conditions":{"direct_core":"prior pos20>=0.70 and close above MA20/60; gap<=-2%; support_break; bearish body>=60%; close_pos<=20%; close below MA20","risk_flag":"abs(close-MA60)<=0.35ATR or oversold_risk","cooldown":"more than 10 trading rows","outcome":"exact OHLC symmetric fixed 3 percent t+1 through t+5","threshold_sweep":False,"years":list(YEARS)},"year_results":results,"human_anchor_4755":anchor,"observed_branching":{"event_count":int(len(ev)),"risk_flagged_count":int(ev.ma60_rebound_risk.sum()),"selection_divergence_reason":"direct breakdown core is permitted while nearby MA60 is retained as a management warning","changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":None},"judgment":{"decision":"keep" if positive and anchor_hit else "drop","breadth_pass":breadth,"h5_down_exceeds_rebound_all_years":positive,"human_anchor_full_match":anchor_hit,"reason":"keep requires anchor match, >=30 events and down-first dominance in every OOS year"},"not_changed":["probe families","existing lifecycle","monthly classifier","MeeMee","ranking","runtime DB"]}
    cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");ev.to_parquet(a.output/"direct_core_event_ledger.parquet",index=False);audit={"feature_rows":int(len(x)),"events":int(len(ev)),"duplicates":int(ev.duplicated(["code","ymd"]).sum()),"feature_sha256":sha(a.features),"monthly_sha256":sha(a.monthly_ledger),"future_used_for_selection":False,"review_only":True};(a.output/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","compare_sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"year_results":results,"anchor":anchor,"judgment":data["judgment"]},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
