"""Reveal fixed outcomes and compare human, model, MA7 exhaustion, and complement union."""
import argparse, hashlib, json
from pathlib import Path
import duckdb, pandas as pd

SELL_ACTIONS={"PROBE","CORE","ADD","REENTRY_PROBE"}; THRESHOLD=7
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def evaluate(g,ymd):
    hit=g.index[g.ymd.eq(ymd)]
    if len(hit)!=1 or int(hit[0])+5>=len(g): return {"status":"censored"}
    w=g.iloc[int(hit[0])+1:int(hit[0])+6];entry=float(w.iloc[0].o);target=entry*.97;stop=entry*1.03;price=float(w.iloc[-1].c);out="N";reason="horizon_close";exit_ymd=int(w.iloc[-1].ymd)
    for r in w.itertuples():
        if r.o>=stop: price,out,reason,exit_ymd=float(r.o),"R","gap_stop",int(r.ymd);break
        if r.o<=target: price,out,reason,exit_ymd=float(r.o),"D","gap_target",int(r.ymd);break
        if r.h>=stop and r.l<=target: price,out,reason,exit_ymd=stop,"R","same_bar_stop_first",int(r.ymd);break
        if r.h>=stop: price,out,reason,exit_ymd=stop,"R","stop",int(r.ymd);break
        if r.l<=target: price,out,reason,exit_ymd=target,"D","target",int(r.ymd);break
    return {"status":"complete","entry_ymd":int(w.iloc[0].ymd),"exit_ymd":exit_ymd,"entry_open":entry,"exit_price_fixed3":price,"exit_reason_fixed3":reason,"outcome_fixed3":out,"return_fixed3_pct":100*(entry-price)/entry,"return_h5_close_pct":100*(entry-float(w.iloc[-1].c))/entry}
def stats(f):
    x=f[f.status.eq("complete")].sort_values(["exit_ymd","code"]);v=x.return_fixed3_pct.dropna();gain=v[v>0].sum();loss=-v[v<0].sum();equity=v.cumsum();dd=equity-equity.cummax() if len(equity) else pd.Series(dtype=float);run=maxrun=0
    for z in v: run=run+1 if z<0 else 0;maxrun=max(maxrun,run)
    conc=0
    if len(x):
        for day in sorted(set(x.entry_ymd).union(x.exit_ymd)):conc=max(conc,int(((x.entry_ymd<=day)&(x.exit_ymd>=day)).sum()))
    return {"n":len(f),"completed":len(x),"D":int(x.outcome_fixed3.eq("D").sum()),"R":int(x.outcome_fixed3.eq("R").sum()),"N":int(x.outcome_fixed3.eq("N").sum()),"D_rate":None if x.empty else float(x.outcome_fixed3.eq("D").mean()),"R_rate":None if x.empty else float(x.outcome_fixed3.eq("R").mean()),"mean_fixed3_pct":None if v.empty else float(v.mean()),"mean_h5_close_pct":None if x.empty else float(x.return_h5_close_pct.mean()),"profit_factor":None if loss==0 else float(gain/loss),"max_loss_pct":None if v.empty else float(v.min()),"sum_return_units_pct":float(v.sum()),"max_drawdown_units_pct":0 if dd.empty else float(dd.min()),"max_loss_streak":maxrun,"max_concurrent":conc}
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--human-freeze",type=Path,required=True);ap.add_argument("--sealed",type=Path,required=True);ap.add_argument("--db",type=Path,required=True);ap.add_argument("--output",type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    human=pd.read_parquet(a.human_freeze/"human_direction_frozen.parquet");sealed=pd.read_parquet(a.sealed/"machine_annotation_sealed.parquet");data=human.merge(sealed,on=["case_id","code","ymd"],validate="one_to_one");codes=data.code.astype(str).str.zfill(4).unique().tolist();con=duckdb.connect(str(a.db),read_only=True);prices=con.execute("select code,strftime(to_timestamp(date),'%Y%m%d')::integer ymd,o,h,l,c from daily_bars where code in (select unnest(?)) order by code,date",[codes]).fetchdf();prices.code=prices.code.astype(str).str.zfill(4);hist={c:g.reset_index(drop=True) for c,g in prices.groupby("code")}
    rows=[]
    for r in data.itertuples():
        g=hist[r.code];hit=g.index[g.ymd.eq(int(r.ymd))]
        if len(hit)!=1:raise RuntimeError(f"missing signal {r.code} {r.ymd}")
        z=g.iloc[:int(hit[0])+1].copy();z["ma7_pit"]=z.c.rolling(7).mean();streak=0
        for below in z.c.lt(z.ma7_pit).iloc[::-1]:
            if not below:break
            streak+=1
        rows.append({**r._asdict(),"below_ma7_close_streak":streak,"exhaustion_veto":streak>=THRESHOLD,**evaluate(g,int(r.ymd))})
    ledger=pd.DataFrame(rows);ledger["model_direction"]=ledger.model_action.map(lambda x:"SELL" if x in SELL_ACTIONS else "NO_SELL");ledger["human_gate_sell"]=ledger.human_direction.eq("SELL")&~ledger.exhaustion_veto;ledger["model_sell"]=ledger.model_direction.eq("SELL");ledger["complement_union_sell"]=ledger.human_gate_sell|ledger.model_sell
    lp=a.output/"complement_outcome_ledger.parquet";ledger.to_parquet(lp,index=False)
    groups={"human_alone":ledger[ledger.human_direction.eq("SELL")],"human_with_7below_gate":ledger[ledger.human_gate_sell],"model_alone":ledger[ledger.model_sell],"complement_union":ledger[ledger.complement_union_sell],"human_sell_model_avoid":ledger[ledger.human_direction.eq("SELL")&ledger.model_direction.eq("NO_SELL")],"human_nosell_model_sell":ledger[ledger.human_direction.eq("NO_SELL")&ledger.model_direction.eq("SELL")]}
    metrics={k:stats(v) for k,v in groups.items()};hb=metrics["human_alone"];hg=metrics["human_with_7below_gate"];dret=None if hb["D"]==0 else hg["D"]/hb["D"];keep=hg["R_rate"]<hb["R_rate"] and hg["max_loss_pct"]>hb["max_loss_pct"] and dret>=.70 and metrics["model_alone"]["mean_fixed3_pct"]>0
    environment_key=ledger.monthly_state.fillna("UNKNOWN_OR_BREAKDOWN")
    result={"schema_version":"tradex_blind_complement_outcome_rollup_v1.compare.v3","artifact_role":"authoritative_human_model_complement_outcome_rollup","review_only":True,"fixed_conditions":{"human_frozen_before_reveal":True,"threshold":"7 consecutive closes below PIT MA7","execution":"next_session_open","horizon_sessions":5,"barriers":"short target -3%, stop +3%, same-bar stop-first","costs":"ignored","weekly_inputs":[],"keep_requires_strict_R_rate_and_max_loss_improvement":True},"authoritative_results":metrics,"observed_branching":{"human_candidates":hb["n"],"human_gate_candidates":hg["n"],"D_retention":dret,"R_removed":hb["R"]-hg["R"],"complement_added_model_sell_over_human_nosell":metrics["human_nosell_model_sell"]["n"]},"by_model_action":{str(k):stats(v) for k,v in ledger[ledger.model_sell].groupby("model_action")},"by_environment":{str(k):stats(v) for k,v in ledger.groupby(environment_key)},"direction_pairs":{str(k):stats(v) for k,v in ledger.groupby(ledger.model_direction+"__"+ledger.human_direction)},"judgment":{"candidate_local_decision":"keep" if keep else "drop","authoritative_rollup_decision":"keep_review_only" if keep else "drop","keep_conditions":{"user_R_rate_strictly_lower":hg["R_rate"]<hb["R_rate"],"user_max_loss_strictly_better":hg["max_loss_pct"]>hb["max_loss_pct"],"user_D_retention_ge_0_70":dret>=.70,"model_positive_expectancy":metrics["model_alone"]["mean_fixed3_pct"]>0}},"not_changed":["MeeMee","ranking","runtime DB","production trading logic"]}
    cp=a.output/"compare.json";cp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"human_freeze_sha256":sha(a.human_freeze/"compare.json"),"sealed_sha256":sha(a.sealed/"machine_annotation_sealed.parquet"),"db_path":str(a.db.resolve()),"db_read_only":True,"rows":len(ledger),"completed":int(ledger.status.eq("complete").sum()),"weekly_columns_used":[],"ledger_sha256":sha(lp)};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"results":metrics,"branching":result["observed_branching"],"judgment":result["judgment"]},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
