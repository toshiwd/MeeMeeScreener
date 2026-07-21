"""Reveal and compare the frozen close-below-MA60 unused short sample."""
import argparse,hashlib,json
from pathlib import Path
import duckdb,pandas as pd
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
def stats(x):
 x=x[x.status.eq("complete")].copy();v=x.return_fixed3_pct;gain=v[v>0].sum();loss=-v[v<0].sum();eq=v.cumsum();dd=eq-eq.cummax() if len(eq) else pd.Series(dtype=float)
 return {"n":int(len(x)),"D":int(x.outcome_fixed3.eq("D").sum()),"R":int(x.outcome_fixed3.eq("R").sum()),"N":int(x.outcome_fixed3.eq("N").sum()),"D_rate":float(x.outcome_fixed3.eq("D").mean()) if len(x) else None,"R_rate":float(x.outcome_fixed3.eq("R").mean()) if len(x) else None,"mean_fixed3_pct":float(v.mean()) if len(v) else None,"profit_factor":None if loss==0 else float(gain/loss),"max_loss_pct":float(v.min()) if len(v) else None,"sum_return_units_pct":float(v.sum()),"max_drawdown_units_pct":float(dd.min()) if len(dd) else 0.0}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--sample",type=Path,required=True);ap.add_argument("--discovery-board",type=Path,required=True);ap.add_argument("--discovery-ledger",type=Path,required=True);ap.add_argument("--db",type=Path,required=True);ap.add_argument("--output",type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 sample=pd.read_parquet(a.sample/"unused_sample_frozen.parquet");contract=json.loads((a.sample/"compare.json").read_text(encoding="utf-8"));
 if contract["status"]!="frozen_before_outcome_reveal" or bool(sample.outcome_joined.any()):raise RuntimeError("sample not frozen outcome-blind")
 con=duckdb.connect(str(a.db),read_only=True);prices=con.execute("select code,strftime(to_timestamp(date),'%Y%m%d')::integer ymd,o,h,l,c from daily_bars where code in (select unnest(?)) order by code,date",[sample.code.tolist()]).fetchdf();con.close();prices.code=prices.code.astype(str).str.zfill(4);hist={c:g.reset_index(drop=True) for c,g in prices.groupby("code")}
 rows=[]
 for r in sample.itertuples():rows.append({**r._asdict(),**evaluate(hist[r.code],int(r.ymd))})
 ledger=pd.DataFrame(rows);ledger.to_parquet(a.output/"validation_ledger.parquet",index=False);base=stats(ledger);challenger=stats(ledger[ledger.below_ma60]);other=stats(ledger[~ledger.below_ma60]);dret=challenger["D"]/base["D"] if base["D"] else None
 discovery_board=pd.read_parquet(a.discovery_board,columns=["case_id","c","ma60"]);discovery_ledger=pd.read_parquet(a.discovery_ledger);discovery_rows=discovery_ledger.merge(discovery_board,on="case_id",validate="one_to_one");disc=stats(discovery_rows[discovery_rows.c<discovery_rows.ma60])
 conditions={"challenger_n_ge_15":challenger["n"]>=15,"R_rate_strictly_lower":challenger["R_rate"]<base["R_rate"],"mean_expectancy_positive":challenger["mean_fixed3_pct"]>0,"profit_factor_gt_1":challenger["profit_factor"] is not None and challenger["profit_factor"]>1,"max_loss_not_worse":challenger["max_loss_pct"]>=base["max_loss_pct"]}
 stable=disc["mean_fixed3_pct"]>0 and challenger["mean_fixed3_pct"]>0 and disc["profit_factor"]>1 and challenger["profit_factor"]>1 and disc["R_rate"]>challenger["R_rate"]
 if all(conditions.values()) and stable:decision="keep_review_only"
 elif challenger["n"]<15:decision="hold"
 else:decision="drop"
 result={"schema_version":"tradex_close_below_ma60_unused_validation_v1.compare.v1","artifact_role":"authoritative_unused_close_below_ma60_validation","review_only":True,"research_phase":"effectiveness_judgment","fixed_conditions":{"sample_frozen_before_reveal":True,"sample_design":"outcome-blind one-event-per-code axis-balanced case-control","aggregate_pf_is_natural_frequency_estimate":False,"current_champion":"ungated_model_sell","current_challenger":"close_below_PIT_MA60_priority_lane","execution":"next_session_open","horizon_sessions":5,"barriers":"short target -3%, stop +3%, same-bar stop-first","costs":"ignored","weekly_inputs":[],"changed_axis_only":"close < PIT MA60","challenger_is_priority_lane_not_hard_veto":True},"authoritative_results":{"model_ungated":base,"close_below_ma60":challenger,"close_at_or_above_ma60":other},"observed_branching":{"model_candidates":base["n"],"challenger_candidates":challenger["n"],"non_challenger_candidates":other["n"],"D_retention":dret,"selection_divergence_reason":"PIT close below MA60 only"},"by_year":{"all":{str(k):stats(v) for k,v in ledger.groupby(ledger.ymd//10000)},"challenger":{str(k):stats(v) for k,v in ledger[ledger.below_ma60].groupby(ledger[ledger.below_ma60].ymd//10000)}},"sample_stability":{"discovery32":disc,"unused":challenger,"directionally_stable":stable},"judgment":{"candidate_local_decision":decision,"session_aggregate_decision":decision,"authoritative_rollup_decision":decision,"reason_type":"unused_sample_reversed_discovery_and_worsened_short_quality" if decision=="drop" else "fixed_keep_contract_passed" if decision=="keep_review_only" else "insufficient_challenger_sample","keep_conditions":conditions,"D_retention_diagnostic_only":True},"not_changed":["MeeMee","ranking","runtime DB","production trading logic"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"sources":{"sample":{"path":str((a.sample/"compare.json").resolve()),"sha256":sha(a.sample/"compare.json")},"discovery_board":{"path":str(a.discovery_board.resolve()),"sha256":sha(a.discovery_board)},"discovery_ledger":{"path":str(a.discovery_ledger.resolve()),"sha256":sha(a.discovery_ledger)},"db":{"path":str(a.db.resolve()),"read_only":True}},"rows":len(ledger),"completed":int(ledger.status.eq("complete").sum()),"weekly_columns_used":[],"ledger_sha256":sha(a.output/"validation_ledger.parquet"),"compare_sha256":sha(cp)};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"results":result["authoritative_results"],"stability":result["sample_stability"],"judgment":result["judgment"]},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
