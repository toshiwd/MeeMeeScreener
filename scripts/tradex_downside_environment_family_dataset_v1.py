"""Build a PIT downside-event dataset, matched controls, and overlapping shape families."""
import argparse,hashlib,json
from pathlib import Path
import duckdb,pandas as pd,numpy as np

SELL={"PROBE","CORE","ADD","REENTRY_PROBE"};FAMILIES=["fresh_breakdown","failed_rebound","high_zone_failure","post_box_return_sell","continuation_below_ma","capitulation_risk","unclear"]
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def evaluate(g,ymd):
 hit=g.index[g.ymd.eq(ymd)]
 if len(hit)!=1 or int(hit[0])+5>=len(g):return {"status":"censored"}
 w=g.iloc[int(hit[0])+1:int(hit[0])+6];entry=float(w.iloc[0].o);target=entry*.97;stop=entry*1.03;price=float(w.iloc[-1].c);out="N";reason="horizon_close";exit_ymd=int(w.iloc[-1].ymd)
 for r in w.itertuples():
  if r.o>=stop:price,out,reason,exit_ymd=float(r.o),"R","gap_stop",int(r.ymd);break
  if r.o<=target:price,out,reason,exit_ymd=float(r.o),"D","gap_target",int(r.ymd);break
  if r.h>=stop and r.l<=target:price,out,reason,exit_ymd=stop,"R","same_bar_stop_first",int(r.ymd);break
  if r.h>=stop:price,out,reason,exit_ymd=stop,"R","stop",int(r.ymd);break
  if r.l<=target:price,out,reason,exit_ymd=target,"D","target",int(r.ymd);break
 return {"status":"complete","entry_ymd":int(w.iloc[0].ymd),"exit_ymd":exit_ymd,"outcome_fixed3":out,"return_fixed3_pct":100*(entry-price)/entry,"exit_reason_fixed3":reason}
def rates(x):
 n=len(x);return {"n":int(n),"D":int(x.outcome_fixed3.eq("D").sum()),"R":int(x.outcome_fixed3.eq("R").sum()),"N":int(x.outcome_fixed3.eq("N").sum()),"D_rate":float(x.outcome_fixed3.eq("D").mean()) if n else None,"R_rate":float(x.outcome_fixed3.eq("R").mean()) if n else None,"mean_fixed3_pct":float(x.return_fixed3_pct.mean()) if n else None}
def flags(d):
 d=d.copy();d["fresh_breakdown"]=(d.support_break.eq(1)|d.cross_ma20.eq(1)|((d.c<d.support20)&(d.close_pos<=.35)))
 d["failed_rebound"]=((d.h>=d.ma7)&(d.c<d.ma7)&(d.upper_wick_ratio>=.20))|((d.ret3>0)&(d.c<d.ma20)&(d.close_pos<=.35))
 d["high_zone_failure"]=d.monthly_state.eq("HIGH_ZONE_FAILURE")|d.monthly_high_failure.eq(1)|(d.current_month_close_position>=.70)&(d.close_pos<=.35)
 d["post_box_return_sell"]=d.monthly_state.eq("POST_BOX_RETURN_SELL")
 d["continuation_below_ma"]=(d.c<d.ma20)&(d.c<d.ma60)&(d.ma20_slope5_atr<0)&(d.ret5<0)
 d["capitulation_risk"]=d.oversold_risk.eq(1)|(d.dist_ma20_atr<=-2)|((d.volume_ratio20>=2)&(d.close_pos<=.20)&(d.ret5<=-.08))
 order=["capitulation_risk","fresh_breakdown","failed_rebound","post_box_return_sell","high_zone_failure","continuation_below_ma"]
 d["family_overlap_count"]=d[order].sum(axis=1).astype(int);d["family_flags"]=["|".join([f for f in order if bool(r[f])]) or "unclear" for _,r in d.iterrows()]
 d["primary_family"]="unclear"
 for f in reversed(order):d.loc[d[f],"primary_family"]=f
 return d
def match_controls(d):
 x=d.copy();x["atr_pct"]=x.atr14/x.c;x["vol_bin"]=x.groupby("year")["atr_pct"].transform(lambda s:pd.qcut(s.rank(method="first"),5,labels=False,duplicates="drop"));downs=x[x.outcome_fixed3.eq("D")].copy();pool=x[~x.outcome_fixed3.eq("D")].copy();used=set();pairs=[]
 for r in downs.sort_values(["year","ymd","code"]).itertuples():
  c=pool[(pool.year==r.year)&~pool.index.isin(used)].copy()
  if c.empty:continue
  c["regime_penalty"]=(c.base_regime.astype(str)!=str(r.base_regime)).astype(int);c["monthly_penalty"]=(c.monthly_state.astype(str)!=str(r.monthly_state)).astype(int);c["vol_penalty"]=(c.vol_bin-float(r.vol_bin)).abs();c["date_penalty"]=(c.ymd-int(r.ymd)).abs()/100000000
  c["score"]=4*c.regime_penalty+2*c.monthly_penalty+c.vol_penalty+c.date_penalty;z=c.sort_values(["score","ymd","code"]).iloc[0];used.add(z.name)
  tier="year_regime_monthly_vol" if z.regime_penalty==0 and z.monthly_penalty==0 and z.vol_penalty==0 else "year_regime_vol" if z.regime_penalty==0 and z.vol_penalty==0 else "year_vol" if z.vol_penalty==0 else "year_nearest"
  pairs.append({"pair_id":f"P{len(pairs)+1:05d}","D_index":r.Index,"control_index":z.name,"match_tier":tier,"match_score":float(z.score)})
 rows=[]
 for p in pairs:
  for role,idx in [("D",p["D_index"]),("CONTROL",p["control_index"])]:rows.append({**p,"pair_role":role,"row_index":idx})
 return x,pd.DataFrame(rows)
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--actions",type=Path,required=True);ap.add_argument("--daily",type=Path,required=True);ap.add_argument("--monthly",type=Path,required=True);ap.add_argument("--db",type=Path,required=True);ap.add_argument("--output",type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 actions=pd.read_parquet(a.actions);actions.code=actions.code.astype(str).str.zfill(4);actions.ymd=actions.ymd.astype(int);actions=actions[(actions.ymd//10000).between(2020,2025)&actions.action.isin(SELL)].drop_duplicates(["code","ymd"]).copy();actions["model_action"]=actions.action
 dcols=["code","ymd","o","h","l","c","v","atr14","ma7","ma20","ma60","ma100","ma200","vol20","support20","resistance20","body_ratio","upper_wick_ratio","lower_wick_ratio","close_pos","ret3","ret5","ret10","pos20","range20_pct","bear_count5","bear_body5_atr","upper_supply_count5","lower_rejection_count5","low_close_count3","dist_ma7_atr","dist_ma20_atr","dist_ma60_atr","ma7_slope5_atr","ma20_slope5_atr","ma60_slope5_atr","volume_ratio20","cross_ma7","cross_ma20","reclaim_ma7","reclaim_ma20","support_break","support_break_depth_atr","oversold_risk","monthly_high_failure","market_breadth_ma20","market_breadth_ma60","market_advancers_ratio","market_mean_ret1"]
 mcols=["code","ymd","base_regime","confirmed_environment","monthly_selection_state","selection_age_sessions","current_local_box_position","current_month_body_pct","current_month_close_position","monthly_ret12","monthly_pos24","ma7m","ma20m","ma60m","above_ma20m_run","above_ma60m_run"]
 daily=pd.read_parquet(a.daily,columns=dcols);monthly=pd.read_parquet(a.monthly,columns=mcols)
 for z in (daily,monthly):z.code=z.code.astype(str).str.zfill(4);z.ymd=z.ymd.astype(int)
 pit=actions.merge(daily,on=["code","ymd"],validate="one_to_one").merge(monthly,on=["code","ymd"],validate="one_to_one");pit["year"]=pit.ymd//10000
 con=duckdb.connect(str(a.db),read_only=True);prices=con.execute("select code,strftime(to_timestamp(date),'%Y%m%d')::integer ymd,o,h,l,c from daily_bars where code in (select unnest(?)) order by code,date",[pit.code.unique().tolist()]).fetchdf();con.close();prices.code=prices.code.astype(str).str.zfill(4);hist={c:g.reset_index(drop=True) for c,g in prices.groupby("code")}
 outcomes=[]
 for r in pit.itertuples():outcomes.append(evaluate(hist[r.code],int(r.ymd)))
 full=pd.concat([pit.reset_index(drop=True),pd.DataFrame(outcomes)],axis=1);full=full[full.status.eq("complete")].reset_index(drop=True);full=flags(full);matched_source,pairs=match_controls(full);matched=pairs.merge(matched_source,left_on="row_index",right_index=True,validate="many_to_one")
 full.to_parquet(a.output/"all_eligible_event_ledger.parquet",index=False);matched.to_parquet(a.output/"matched_D_control_dataset.parquet",index=False)
 fam={}
 for f in FAMILIES:
  mask=full[f] if f!="unclear" else full.primary_family.eq("unclear")
  dmask=matched[matched.pair_role.eq("D")][f] if f!="unclear" else matched[matched.pair_role.eq("D")].primary_family.eq("unclear")
  cmask=matched[matched.pair_role.eq("CONTROL")][f] if f!="unclear" else matched[matched.pair_role.eq("CONTROL")].primary_family.eq("unclear")
  dp=float(dmask.mean());cpv=float(cmask.mean())
  fam[f]={"all":rates(full[mask]),"D":int((mask&full.outcome_fixed3.eq("D")).sum()),"control_prevalence":cpv,"D_prevalence":dp,"prevalence_diff":dp-cpv,"enrichment_ratio":dp/cpv if cpv else None}
 primary={str(k):rates(v) for k,v in full.groupby("primary_family")};overlap=pd.crosstab(full.primary_family,full.family_overlap_count).to_dict();years={str(k):rates(v) for k,v in full.groupby("year")};match_tiers={str(k):int(v) for k,v in pairs.drop_duplicates("pair_id").match_tier.value_counts().items()}
 dcount=int(full.outcome_fixed3.eq("D").sum());unclear_d=int((full.outcome_fixed3.eq("D")&full.primary_family.eq("unclear")).sum());active=sum(v["D"]>=10 for v in fam.values());meaningful=[f for f,v in fam.items() if f!="unclear" and v["D"]>=20 and v["prevalence_diff"]>=.03]
 branching_ok=len(meaningful)>=3
 control_outcomes=rates(matched[matched.pair_role.eq("CONTROL")])
 result={"schema_version":"tradex_downside_environment_family_dataset_v1.compare.v2","artifact_role":"authoritative_downside_environment_family_dataset","review_only":True,"research_phase":"branching_generation","fixed_conditions":{"universe":"all deduped eligible model sell events 2020-2025","execution":"next_session_open","horizon_sessions":5,"barriers":"short target -3%, stop +3%, same-bar stop-first","costs":"ignored","weekly_inputs":[],"PIT_feature_columns":dcols+mcols,"future_feature_columns_used":[],"matched_control_contract":"one unused non-D control per D; same year; minimize regime, monthly state, ATR-percentile and date distance","meaningful_branch_contract":"D>=20 and matched D-minus-control prevalence >= 0.03; at least 3 non-unclear families"},"authoritative_result":{"all_events":rates(full),"years":years,"D_events":dcount,"matched_pairs":int(pairs.pair_id.nunique()),"matched_control_outcomes":control_outcomes,"match_tiers":match_tiers},"family_results":fam,"primary_family_results":primary,"observed_branching":{"families":FAMILIES,"families_with_D_ge_10":active,"meaningful_contrast_families":meaningful,"meaningful_contrast_family_count":len(meaningful),"multi_family_D_events":int((full.outcome_fixed3.eq("D")&(full.family_overlap_count>1)).sum()),"unclear_D_events":unclear_d,"unclear_D_rate":unclear_d/dcount if dcount else None,"overlap_table":overlap,"selection_divergence_reason":"outcome-first D environments split by overlapping PIT shape evidence"},"judgment":{"candidate_local_decision":"keep_dataset_hold_current_families","session_aggregate_decision":"branching_established" if branching_ok else "branching_insufficient","authoritative_rollup_decision":"review_only_family_dataset_ready" if branching_ok else "hold_family_definitions_refine_one_axis","reason_type":"multiple_matched_control_contrasts" if branching_ok else "broad_membership_without_sufficient_matched_control_contrast"},"not_changed":["family definitions","family score weights","MeeMee","ranking","runtime DB","production trading logic"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"sources":{"actions":{"path":str(a.actions.resolve()),"sha256":sha(a.actions)},"daily":{"path":str(a.daily.resolve()),"sha256":sha(a.daily)},"monthly":{"path":str(a.monthly.resolve()),"sha256":sha(a.monthly)},"db":{"path":str(a.db.resolve()),"read_only":True}},"input_events":len(actions),"complete_events":len(full),"unique_events":full[["code","ymd"]].drop_duplicates().shape[0],"future_columns_used":[],"weekly_columns_used":[],"all_ledger_sha256":sha(a.output/"all_eligible_event_ledger.parquet"),"matched_sha256":sha(a.output/"matched_D_control_dataset.parquet"),"compare_sha256":sha(cp)};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"authoritative_result":result["authoritative_result"],"branching":result["observed_branching"],"judgment":result["judgment"]},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
