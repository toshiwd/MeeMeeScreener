from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd

def sim_confirm(r):
 e=r.o2
 if pd.isna(e) or e<=0:return None
 for d in range(2,6):
  if getattr(r,f"l{d}")<=e*.95:return (5.0,"5%利確",d)
  if getattr(r,f"c{d}")>=e*1.02:
   px=getattr(r,f"o{d+1}") if d<5 else getattr(r,f"c{d}");return (100*(e-px)/e,"2%終値撤退",d+1 if d<5 else d)
 return (100*(e-r.c5)/e,"5日終了",5)
def metrics(all_rows,signals):
 traded=all_rows[all_rows.executed]
 return {"signals":int(signals),"trades":int(len(traded)),"execution_rate":float(len(traded)/signals),"codes":int(traded.code.nunique()),
 "mean_return_per_signal_pct":float(all_rows.ret.mean()),"mean_return_per_trade_pct":float(traded.ret.mean()),"win_rate_trades":float(traded.ret.gt(0).mean()),
 "severe_loss5_rate_signals":float(all_rows.ret.le(-5).mean()),"severe_loss5_rate_trades":float(traded.ret.le(-5).mean()),"p10_signal_return_pct":float(all_rows.ret.quantile(.1))}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--path-ledger",required=True);ap.add_argument("--managed-ledger",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 path=pd.read_parquet(a.path_ledger);path=path[path.action_tier.isin(["Core","Probe"])].copy();managed=pd.read_parquet(a.managed_ledger)
 frames=[]
 for frac,name in [(0.25,"25%先行"),(0.5,"50%先行")]:
  q=managed[managed.initial_fraction.eq(frac)][["code","ymd","period","action_tier","ret"]].copy();q["executed"]=True;q["policy"]=name;frames.append(q)
 q=path[["code","ymd","period","action_tier","entry_day_path","o2","o3","o4","o5","l2","l3","l4","l5","c2","c3","c4","c5"]].copy();q=q[q.o2.notna()&q.c5.notna()].copy();q["executed"]=q.entry_day_path.eq("即下落");vals=[sim_confirm(r) if r.executed else (0.0,"見送り",0) for r in q.itertuples(index=False)];q["ret"]=[v[0] for v in vals];q["policy"]="即下落確認後";frames.append(q[["code","ymd","period","action_tier","ret","executed","policy"]])
 d=pd.concat(frames,ignore_index=True);rows=[];yrs=[]
 for (p,t,s),g in d.groupby(["period","action_tier","policy"]):rows.append({"period":p,"action_tier":t,"policy":s,**metrics(g,len(g))})
 for (y,t,s),g in d.assign(year=d.ymd//10000).groupby(["year","action_tier","policy"]):yrs.append({"year":int(y),"action_tier":t,"policy":s,**metrics(g,len(g))})
 m=pd.DataFrame(rows);yr=pd.DataFrame(yrs);d.to_parquet(out/"final_entry_policy_ledger.parquet",index=False);m.to_parquet(out/"final_entry_policy_metrics.parquet",index=False);yr.to_parquet(out/"final_entry_policy_yearly_metrics.parquet",index=False)
 sel={}
 for t in ["Core","Probe"]:
  z=m[(m.period.eq("development"))&(m.action_tier.eq(t))];ok=z[(z.mean_return_per_signal_pct>0)&(z.severe_loss5_rate_signals<=.10)&(z.trades>=1000)];sel[t]=None if ok.empty else str(ok.sort_values("mean_return_per_signal_pct",ascending=False).iloc[0].policy)
 val=[];checks={"both_tiers_have_development_winner":all(v is not None for v in sel.values())}
 for t,s in sel.items():
  z=m[(m.period.eq("validation"))&(m.action_tier.eq(t))&(m.policy.eq(s))];val+=z.to_dict("records");yy=yr[(yr.year>=2024)&(yr.action_tier.eq(t))&(yr.policy.eq(s))]
  checks[f"{t}_validation_positive"]=bool(len(z)==1 and z.iloc[0].mean_return_per_signal_pct>0);checks[f"{t}_validation_tail_le10"]=bool(len(z)==1 and z.iloc[0].severe_loss5_rate_signals<=.10);checks[f"{t}_validation_trades_ge300"]=bool(len(z)==1 and z.iloc[0].trades>=300);checks[f"{t}_all_years_positive"]=bool(len(yy)==3 and (yy.mean_return_per_signal_pct>0).all())
 keep=all(checks.values());res={"schema_version":"tradex_short_final_entry_policy_v1.compare.v1","artifact_role":"authoritative_short_final_entry_policy","review_only":True,"fixed_conditions":{"policies":["25%先行","50%先行","即下落確認後"],"swift_drop":"entry-day low <= -3%","confirm_entry":"following open","target":"5%","close_stop":"2%, following open","horizon":"fifth close","development":"2019-2023","validation":"2024-2026","costs":"ignored","no_further_threshold_search":True},"authoritative_result":{"selected_policy":sel,"validation":val,"validation_years":yr[(yr.year>=2024)&yr.apply(lambda r:sel.get(r.action_tier)==r.policy,axis=1)].to_dict("records"),"gate_checks":checks},"observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int((~d.executed).sum()),"selection_divergence_reason":"final execution-policy comparison only"},"judgment":{"candidate_local_decision":"keep" if keep else "drop","session_aggregate_decision":"keep_practical_short_policy" if keep else "drop_practical_short_policy","authoritative_rollup_decision":"keep_practical_short_entry_management_v1_review_only" if keep else "drop_short_live_actionability_move_to_buy_research","reason_type":"final_positive_yearly_tail_breadth_gates"},"not_changed":["membership","MeeMee","ranking","runtime DB","production logic"],"remaining_risks":["costs ignored","target touch execution","portfolio overlap absent"]}
 (out/"compare.json").write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(res["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
