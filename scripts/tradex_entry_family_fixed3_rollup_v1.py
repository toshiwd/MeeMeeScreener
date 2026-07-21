"""Cross-family rollup under one fixed-3%-h5 core-entry measurement contract."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path

YEARS=(2023,2024,2025)
SOURCES={
 "existing":Path(r"G:\Tradex\trade_reproduction_h5_fixed3_contract_v2\20260715T035825Z-tradex-trade-reproduction-h5-fixed3-contract-v2\compare.json"),
 "support_retry":Path(r"G:\Tradex\support_adjacent_probe_retry_core_oos_v1\20260715T135500Z-tradex-support-adjacent-probe-retry-core-oos-v1\compare.json"),
 "ma200_reject":Path(r"G:\Tradex\box_ma200_rejection_probe_core_add_oos_v1\20260715T142000Z-tradex-box-ma200-rejection-probe-core-add-oos-v1\compare.json"),
 "direct_gap_break":Path(r"G:\Tradex\post_box_gap_support_break_direct_core_oos_v1\20260715T162500Z-tradex-post-box-gap-support-break-direct-core-oos-v1\compare.json"),
 "full_erasure":Path(r"G:\Tradex\uptrend_full_erasure_probe_oos_v2\20260715T041328Z-tradex-uptrend-full-erasure-probe-oos-v2\compare.json"),
 "wtop_erasure":Path(r"G:\Tradex\uptrend_full_erasure_wtop_probe_oos_v1\20260715T041629Z-tradex-uptrend-full-erasure-wtop-probe-oos-v1\compare.json"),
}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def row(fid,year,n,down,rebound,e2e=None,teacher=None):return {"family_id":fid,"year":year,"n":int(n),"down_first":down,"rebound_first":rebound,"margin":None if down is None or rebound is None else down-rebound,"end_to_end_probe_core_down":e2e,"teacher_anchor":teacher}
def main():
 p=argparse.ArgumentParser();p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 d={k:json.loads(v.read_text(encoding="utf-8")) for k,v in SOURCES.items()};rows=[]
 # Current lifecycle, aggregate and family-specific core stages.
 for y in YEARS:
  z=d["existing"]["year_results"][str(y)];c=z["core_by_action_year"];e=z["probe_cohort_core_end_to_end"]
  rows.append(row("CURRENT_CORE_ALL",y,c["n"],c["down_first"],c["rebound_first"],e["end_to_end_probe_action_down"]))
 for fam,ys in d["existing"]["family_results"].items():
  for y in YEARS:
   z=ys[str(y)];c=z["core_by_action_year"];e=z["probe_cohort_core_end_to_end"]
   rows.append(row("CURRENT_"+fam,y,c["n"],c["down_first"],c["rebound_first"],e["end_to_end_probe_action_down"],"6532" if fam=="BOX_CEILING_ERASURE" else None))
 for y in YEARS:
  z=d["support_retry"]["year_results"][str(y)];rows.append(row("SUPPORT_ADJACENT_RETRY_CORE",y,z["cores"],z["h5_down_first"],z["h5_rebound_first"],z["end_to_end_probe_core_down"],"6857"))
  z=d["ma200_reject"]["year_results"][str(y)];rows.append(row("BOX_MA200_SECOND_REJECTION_CORE",y,z["cores"],z["core_h5_down_first"],z["core_h5_rebound_first"],z["end_to_end_probe_core_down"],"9107"))
  z=d["direct_gap_break"]["year_results"][str(y)]["all"];rows.append(row("POST_BOX_GD_SUPPORT_BREAK_DIRECT_CORE",y,z["n"],z["down_first"],z["rebound_first"],None,"4755"))
  z=d["full_erasure"]["year_results"][str(y)]["challenger"];rows.append(row("UPTREND_FULL_ERASURE",y,z["n"],z["down_first"],z["rebound_first"],None,"2802"))
  z=d["wtop_erasure"]["year_results"][str(y)]["challenger"];rows.append(row("UPTREND_FULL_ERASURE_WTOP",y,z["n"],z["down_first"],z["rebound_first"],None,"2802"))
 fams=[]
 for fid in sorted({r["family_id"] for r in rows}):
  rr=[r for r in rows if r["family_id"]==fid];total=sum(r["n"] for r in rr);wd=sum(r["down_first"]*r["n"] for r in rr if r["down_first"] is not None)/total if total else None;wr=sum(r["rebound_first"]*r["n"] for r in rr if r["rebound_first"] is not None)/total if total else None;mins=min(r["n"] for r in rr);margins=[r["margin"] for r in rr if r["margin"] is not None];worst=min(margins) if margins else None;allpos=len(margins)==3 and all(v>0 for v in margins);breadth=mins>=30
  fams.append({"family_id":fid,"year_results":rr,"total_n":total,"min_year_n":mins,"weighted_down_first":wd,"weighted_rebound_first":wr,"weighted_margin":None if wd is None else wd-wr,"worst_year_margin":worst,"down_exceeds_rebound_all_years":allpos,"breadth_pass":breadth,"teacher_anchors":sorted({r["teacher_anchor"] for r in rr if r["teacher_anchor"]})})
 fams.sort(key=lambda z:(z["down_exceeds_rebound_all_years"],z["breadth_pass"],z["worst_year_margin"] if z["worst_year_margin"] is not None else -9,z["weighted_margin"] if z["weighted_margin"] is not None else -9),reverse=True)
 for i,z in enumerate(fams,1):z["rank"]=i
 passing=[z for z in fams if z["down_exceeds_rebound_all_years"] and z["breadth_pass"]]
 next_axis=fams[0]["family_id"] if fams else None
 data={"schema_version":"tradex_entry_family_fixed3_rollup_v1.compare.v1","artifact_role":"authoritative_rollup","review_only":True,"fixed_conditions":{"universe":"same Nikkei225 feature universe","years":list(YEARS),"entry":"core at close","outcome":"exact OHLC symmetric fixed 3 percent first passage t+1 through t+5","minimum_events_each_year":30,"primary":"down_first > rebound_first in every year","costs":"ignored per project rule"},"source_artifacts":{k:{"path":str(v),"sha256":sha(v)} for k,v in SOURCES.items()},"family_leaderboard":fams,"observed_branching":{"families_compared":len(fams),"families_passing":len(passing),"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":len(fams),"selection_divergence_reason":"all independently generated core-entry families are normalized to the same fixed3 h5 contract"},"judgment":{"decision":"keep" if passing else "hold","passing_family_ids":[z["family_id"] for z in passing],"next_single_axis_base":next_axis,"reason":"no family is promoted unless every OOS year has >=30 cores and down-first exceeds rebound-first"},"not_changed":["source family rules","monthly classifier","position lifecycle","MeeMee","ranking","runtime DB"]}
 cp=a.output/"family_leaderboard.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(a.output/"compare.json").write_text(json.dumps({"schema_version":data["schema_version"],"artifact_role":"authoritative_pointer","authoritative":"family_leaderboard.json","judgment":data["judgment"]},ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"families":len(fams),"rows":len(rows),"missing_year_cells":sum(len(z["year_results"])!=3 for z in fams),"future_used_for_selection":False,"review_only":True};(a.output/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"family_leaderboard.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"judgment":data["judgment"],"leaderboard":[{k:z[k] for k in ("rank","family_id","min_year_n","worst_year_margin","weighted_margin","down_exceeds_rebound_all_years","breadth_pass")} for z in fams]},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
