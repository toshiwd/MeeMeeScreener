"""One-axis exit refinement: simultaneous support from at least two long MAs."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

def sha(p: Path) -> str: return hashlib.sha256(p.read_bytes()).hexdigest()
def rates(x: pd.DataFrame) -> dict:
    return {"n":int(len(x)),"rebound_first":None if x.empty else float(x.outcome_fixed3_h5.eq("rebound_first").mean()),"further_down_first":None if x.empty else float(x.outcome_fixed3_h5.eq("further_down_first").mean()),"neutral":None if x.empty else float(x.outcome_fixed3_h5.str.startswith("neutral").mean())}
def main():
    p=argparse.ArgumentParser(); p.add_argument("--exit-ledger",type=Path,required=True); p.add_argument("--output",type=Path,required=True); a=p.parse_args(); a.output.mkdir(parents=True,exist_ok=False)
    x=pd.read_parquet(a.exit_ledger); x["year"]=x.ymd.astype(str).str[:4].astype(int)
    base=x[x.exit_reason.eq("LONG_MA_TOUCH_HOLD")].copy(); base["held_ma_count"]=base.held_mas.fillna("").str.count(r"\|")+1
    q=base[base.held_ma_count.ge(2)].copy()
    years={str(y):{"baseline":rates(base[base.year.eq(y)]),"challenger":rates(q[q.year.eq(y)])} for y in range(2019,2027)}
    strict=all(v["challenger"]["n"]==0 or v["challenger"]["rebound_first"]>v["challenger"]["further_down_first"] for v in years.values())
    oos_breadth=all(years[str(y)]["challenger"]["n"]>=20 for y in (2023,2024,2025))
    anchor=q[(q.code.astype(str).str.zfill(4)=="2802")&q.ymd.eq(20240216)].where(pd.notna(q),None).to_dict("records")
    decision="keep" if strict and oos_breadth else ("hold" if strict else "drop")
    data={"schema_version":"tradex_profit_take_multi_long_ma_support_oos_v1.compare.v2","artifact_role":"authoritative","axis":"at least two of MA60 MA100 MA200 touched intraday and held at close","fixed_conditions":{"base":"executable LONG_MA_TOUCH_HOLD first onset while stage>=2","single_filter":"held_ma_count>=2","outcome":"inherited symmetric fixed 3 percent h5","threshold_sweep":False},"year_results":years,"overall":{"baseline":rates(base),"challenger":rates(q)},"human_anchor_2802":{"rows":anchor,"evaluable_in_executable_ledger":False,"reason":"current machine entry path is absent; raw opportunity match is established in the upstream artifact"},"observed_branching":{"removed_event_count":int(len(base)-len(q)),"kept_event_count":int(len(q)),"selection_divergence_reason":"single long MA contact is separated from a multi-MA support band","changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":None},"judgment":{"decision":decision,"strict_rebound_dominance_all_years":strict,"oos_2023_2025_breadth_pass":oos_breadth,"human_anchor_evaluable":False,"reason":"hold when rebound dominates every year but any 2023-2025 year has fewer than 20 events; the known missing 2802 entry path is not scored as an exit-shape miss"},"not_changed":["position connector","exit opportunity rules","entry lifecycle","MeeMee","ranking","runtime DB"]}
    cp=a.output/"compare.json"; cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); q.to_parquet(a.output/"multi_long_ma_support_exit_ledger.parquet",index=False)
    audit={"base_rows":int(len(base)),"challenger_rows":int(len(q)),"duplicates":int(q.duplicated(["code","ymd","exit_reason"]).sum()),"source_sha256":sha(a.exit_ledger),"future_used_for_selection":False,"review_only":True}; (a.output/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); (a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","compare_sha256":sha(cp)},indent=2)+"\n",encoding="utf-8"); print(json.dumps({"output":str(a.output),"overall":data["overall"],"judgment":data["judgment"],"branching":data["observed_branching"]},ensure_ascii=False,indent=2))
if __name__=="__main__": main()
