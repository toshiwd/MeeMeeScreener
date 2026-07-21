"""Evaluate monthly-selected daily CORE shorts at next open, review-only."""
import argparse, hashlib, json
from pathlib import Path
import numpy as np, pandas as pd

OHLC=["code","ymd","o","h","l","c"]
TEACHER_CODES={"9107","7733","3405","4208","7004","9531"}
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def evaluate(g, signal_ymd, horizon):
 idx=g.index[g.ymd.eq(signal_ymd)]
 if len(idx)!=1 or idx[0]+1>=len(g): return {"status":"censored_no_entry"}
 ei=int(idx[0])+1; entry=float(g.iloc[ei].o); target=.97*entry; stop=1.03*entry
 end=min(ei+horizon,len(g)); max_h=entry; same=False
 for j in range(ei,end):
  r=g.iloc[j]; max_h=max(max_h,float(r.h))
  if r.o>=stop: return result(g,ei,j,entry,float(r.o),"R","gap_stop",max_h,same)
  if r.o<=target: return result(g,ei,j,entry,float(r.o),"D","gap_target",max_h,same)
  hit_s=r.h>=stop; hit_t=r.l<=target
  if hit_s and hit_t: same=True; return result(g,ei,j,entry,stop,"R","same_bar_stop_first",max_h,same)
  if hit_s: return result(g,ei,j,entry,stop,"R","stop",max_h,same)
  if hit_t: return result(g,ei,j,entry,target,"D","target",max_h,same)
 if end-ei<horizon: return {"status":"censored_tail","entry_ymd":int(g.iloc[ei].ymd),"entry":entry}
 j=end-1; return result(g,ei,j,entry,float(g.iloc[j].c),"N","horizon_close",max_h,same)

def result(g,ei,xi,entry,exit_price,outcome,reason,max_h,same):
 return {"status":"complete","entry_ymd":int(g.iloc[ei].ymd),"exit_ymd":int(g.iloc[xi].ymd),"entry":entry,"exit":exit_price,"outcome":outcome,"exit_reason":reason,"return_pct":100*(entry-exit_price)/entry,"mae_pct":100*(entry-max_h)/entry,"same_bar":same}

def stats(q):
 complete=q[q.status.eq("complete")]; v=complete.return_pct.dropna(); loss=-v[v<0].sum(); gain=v[v>0].sum(); tail=v.nsmallest(max(1,int(np.ceil(len(v)*.05)))) if len(v) else v
 return {"eligible":len(q),"completed":len(complete),"censored":int(q.status.str.startswith("censored").sum()),"D":int(complete.outcome.eq("D").sum()),"R":int(complete.outcome.eq("R").sum()),"N":int(complete.outcome.eq("N").sum()),"D_minus_R":int(complete.outcome.eq("D").sum()-complete.outcome.eq("R").sum()),"mean_pct":None if v.empty else float(v.mean()),"median_pct":None if v.empty else float(v.median()),"sum_pct":float(v.sum()),"max_loss_pct":None if v.empty else float(v.min()),"mae_mean_pct":None if complete.empty else float(complete.mae_pct.mean()),"worst5_tail_mean_pct":None if tail.empty else float(tail.mean()),"profit_factor":None if loss==0 else float(gain/loss),"gap_stop":int(complete.exit_reason.eq("gap_stop").sum()),"gap_target":int(complete.exit_reason.eq("gap_target").sum()),"same_bar_unknown":int(complete.same_bar.sum())}

def streaks(q):
 x=q[q.status.eq("complete")].sort_values(["exit_ymd","code"]); run=mx=0
 for v in x.return_pct: run=run+1 if v<0 else 0; mx=max(mx,run)
 daily=x.groupby("exit_ymd").return_pct.sum(); run=dmx=0
 for v in daily: run=run+1 if v<0 else 0; dmx=max(dmx,run)
 return {"trade_loss_streak":mx,"losing_exit_day_streak":dmx}

def executable(raw):
 accepted=[]; suppressed=0; active={}
 for r in raw.sort_values(["entry_ymd","code","signal_ymd"]).itertuples():
  if pd.isna(r.entry_ymd): suppressed+=1; continue
  if active.get(r.code,0)>=r.entry_ymd: suppressed+=1; continue
  accepted.append({k:v for k,v in r._asdict().items() if k!="Index"}); active[r.code]=r.exit_ymd if pd.notna(r.exit_ymd) else 99999999
 return pd.DataFrame(accepted,columns=raw.columns),suppressed

def portfolio(q, price):
 x=q[q.status.eq("complete")].copy(); pnl={}; concurrent={}
 hist={c:g.reset_index(drop=True) for c,g in price.groupby("code")}
 for r in x.itertuples():
  g=hist[r.code]; rows=g[(g.ymd>=r.entry_ymd)&(g.ymd<=r.exit_ymd)]; prev=float(r.entry)
  for z in rows.itertuples():
   mark=float(r.exit) if z.ymd==r.exit_ymd else float(z.c); pnl[z.ymd]=pnl.get(z.ymd,0)+100*(prev-mark)/float(r.entry); prev=mark; concurrent[z.ymd]=concurrent.get(z.ymd,0)+1
 if not pnl:return {"max_drawdown_units_pct":0,"max_concurrent":0,"position_days":0}
 s=pd.Series(pnl).sort_index(); eq=s.cumsum(); peak=eq.cummax(); dd=eq-peak; trough=int(dd.idxmin()); start=int(eq.loc[:trough].idxmax()); recovered=eq[(eq.index>trough)&(eq>=peak.loc[trough])]
 return {"max_drawdown_units_pct":float(dd.min()),"drawdown_start_ymd":start,"drawdown_trough_ymd":trough,"drawdown_recovery_ymd":None if recovered.empty else int(recovered.index[0]),"max_concurrent":max(concurrent.values()),"median_concurrent_active_days":float(np.median(list(concurrent.values()))),"position_days":int(sum(concurrent.values()))}

def main():
 ap=argparse.ArgumentParser();ap.add_argument("--actions",required=True);ap.add_argument("--daily",required=True);ap.add_argument("--output",type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 actions=pd.read_parquet(a.actions); cores=actions[actions.action.eq("CORE")][["code","ymd","reason","monthly_state"]].rename(columns={"ymd":"signal_ymd"}); cores.code=cores.code.astype(str).str.zfill(4)
 price=pd.read_parquet(a.daily,columns=OHLC);price.code=price.code.astype(str).str.zfill(4);price=price.sort_values(["code","ymd"]);hist={c:g.reset_index(drop=True) for c,g in price.groupby("code")}
 ledgers=[]; summaries={}
 for h in (5,10,20):
  rows=[]
  for r in cores.itertuples(): rows.append({"code":r.code,"signal_ymd":int(r.signal_ymd),"signal_year":int(r.signal_ymd)//10000,"reason":r.reason,"monthly_state":r.monthly_state,"horizon":h,**evaluate(hist[r.code],int(r.signal_ymd),h)})
  raw=pd.DataFrame(rows); exe,supp=executable(raw); exe["entry_year"]=(exe.entry_ymd//10000).astype("Int64");exe["exit_year"]=(exe.exit_ymd//10000).astype("Int64")
  clean26=exe[exe.signal_year.eq(2026)&~exe.code.isin(TEACHER_CODES)]
  summaries[str(h)]={"raw":stats(raw),"executable":stats(exe),"clean_2026_excluding_teacher_codes":stats(clean26),"clean_2026_codes":sorted(clean26.code.unique().tolist()),"suppressed_same_code":supp,"by_signal_year":{str(y):stats(exe[exe.signal_year.eq(y)]) for y in range(2020,2027)},"by_exit_year":{str(y):stats(exe[exe.exit_year.eq(y)]) for y in range(2020,2027)},"loss_streaks":streaks(exe),"portfolio":portfolio(exe,price)};exe["executable"]=True;ledgers.append(exe)
 ledger=pd.concat(ledgers,ignore_index=True); probe_count=int(actions.action.eq("PROBE").sum()); core_count=len(cores)
 data={"schema_version":"tradex_monthly_daily_core_outcome_v1.compare.v2","artifact_role":"authoritative_challenger","review_only":True,"fixed_conditions":{"signal":"CORE at confirmed close","execution":"next session open","horizons_sessions":[5,10,20],"barriers":"short target -3%, stop +3%","same_bar_primary":"stop_first","N":"horizon final close","overlap":"same-code active CORE suppressed per horizon","unit":"one fixed unit per executable CORE","clean_2026":"exclude all six teacher codes from OOS metrics","teacher_codes":sorted(TEACHER_CODES),"weekly_inputs":[],"costs":"ignored"},"probe_to_core":{"probe_events":probe_count,"core_events":core_count,"raw_conversion_pct":None if probe_count==0 else 100*core_count/probe_count},"horizon_results":summaries,"observed_branching":{"raw_core_events":core_count,"codes":int(cores.code.nunique()),"horizon_rows":len(ledger)},"judgment":{"decision":"hold_pending_interpretation"},"not_changed":["state rules","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False)+"\n",encoding="utf-8");ledger.to_parquet(a.output/"executable_core_ledger.parquet",index=False);(a.output/"audit.json").write_text(json.dumps({"core_duplicates":int(cores.duplicated(["code","signal_ymd"]).sum()),"weekly_columns_used":[],"future_feature_columns_used":[],"actions_sha256":sha(a.actions),"daily_sha256":sha(a.daily)},indent=2)+"\n");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n");print(json.dumps({"output":str(a.output),"probe_to_core":data["probe_to_core"],"results":summaries},ensure_ascii=False))
if __name__=="__main__":main()
