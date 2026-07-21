from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd

def metric(g):
    return {"n":int(len(g)),"codes":int(g.code.nunique()),
      "exit_next_open_mean_pct":float(g.exit_next_open_ret.mean()),"hold5_mean_pct":float(g.hold5_ret.mean()),
      "exit_next_open_loss_rate":float(g.exit_next_open_ret.lt(0).mean()),"hold5_loss_rate":float(g.hold5_ret.lt(0).mean()),
      "exit_next_open_severe5_rate":float(g.exit_next_open_ret.le(-5).mean()),"hold5_severe5_rate":float(g.hold5_ret.le(-5).mean()),
      "exit_minus_hold_mean_pct":float((g.exit_next_open_ret-g.hold5_ret).mean())}

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--ledger",required=True);ap.add_argument("--inventory",required=True);ap.add_argument("--output",required=True);a=ap.parse_args()
    out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
    led=pd.read_parquet(a.ledger);led=led[led.action_tier.isin(["Core","Probe"])].copy()
    bars=pd.read_parquet(a.inventory,columns=["code","bar_index","o","h","l","c"])
    x=led[["code","ymd","bar_index","period","action_tier"]].merge(bars,on=["code","bar_index"],validate="many_to_one")
    for k in range(1,6):
      z=bars.rename(columns={"bar_index":"fi","o":f"o{k}","h":f"h{k}","l":f"l{k}","c":f"c{k}"});x["fi"]=x.bar_index+k
      x=x.merge(z,on=["code","fi"],how="left",validate="many_to_one").drop(columns="fi")
    x["gap_pct"]=(x.o1/x.c-1)*100;x["exit_next_open_ret"]=(x.o1-x.o2)/x.o1*100;x["hold5_ret"]=(x.o1-x.c5)/x.o1*100
    rules={"GU1_陽線継続":x.gap_pct.ge(1)&x.c1.gt(x.o1),"GU2_陽線継続":x.gap_pct.ge(2)&x.c1.gt(x.o1),
      "GU1_前日高値突破":x.gap_pct.ge(1)&x.c1.gt(x.h),"GU2_前日高値突破":x.gap_pct.ge(2)&x.c1.gt(x.h)}
    rows=[];year=[];detail=[]
    for name,mask in rules.items():
      q=x[mask&x.o2.notna()&x.c5.notna()].copy();q["exit_rule"]=name;detail.append(q)
      for (p,t),g in q.groupby(["period","action_tier"]):rows.append({"period":p,"action_tier":t,"exit_rule":name,**metric(g)})
      for (y,t),g in q.assign(year=q.ymd//10000).groupby(["year","action_tier"]):year.append({"year":int(y),"action_tier":t,"exit_rule":name,**metric(g)})
    d=pd.concat(detail,ignore_index=True);m=pd.DataFrame(rows);yr=pd.DataFrame(year)
    d.to_parquet(out/"gu_exit_episode_ledger.parquet",index=False);m.to_parquet(out/"gu_exit_metrics.parquet",index=False);yr.to_parquet(out/"gu_exit_yearly_metrics.parquet",index=False)
    dev=m[m.period.eq("development")];eligible=dev[(dev.n>=500)&(dev.exit_minus_hold_mean_pct>0)&(dev.exit_next_open_severe5_rate<=dev.hold5_severe5_rate)]
    selected=None if eligible.empty else eligible.groupby("exit_rule").exit_minus_hold_mean_pct.mean().idxmax()
    val=m[(m.period.eq("validation"))&(m.exit_rule.eq(selected))] if selected else m.iloc[0:0]
    checks={"development_candidate_exists":selected is not None,"validation_both_tiers_improve_mean":bool(len(val)==2 and (val.exit_minus_hold_mean_pct>0).all()),
      "validation_both_tiers_reduce_severe_loss":bool(len(val)==2 and (val.exit_next_open_severe5_rate<=val.hold5_severe5_rate).all())}
    keep=all(checks.values())
    result={"schema_version":"tradex_short_gu_continuation_exit_v1.compare.v1","artifact_role":"authoritative_short_gu_continuation_exit","review_only":True,
      "fixed_conditions":{"population":"fixed Core and Probe","entry":"next open","decision_time":"entry-day close","exit":"following open","baseline":"fifth-session close","costs":"ignored","selected_on":"development only","selected_rule":selected},
      "authoritative_result":{"validation":val.to_dict("records"),"validation_years":yr[(yr.year>=2024)&yr.exit_rule.eq(selected)].to_dict("records") if selected else [],"gate_checks":checks},
      "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":int(len(d[d.exit_rule.eq(selected)])) if selected else 0,"selection_divergence_reason":"GU continuation triggers early exit only"},
      "judgment":{"candidate_local_decision":"keep" if keep else "drop","session_aggregate_decision":"keep_gu_early_exit" if keep else "drop_gu_early_exit","authoritative_rollup_decision":"keep_gu_continuation_exit_v1_review_only" if keep else "drop_gu_continuation_exit_v1","reason_type":"mean_and_severe_loss_gates"},
      "not_changed":["entry","candidate membership","MeeMee","ranking","runtime DB","production logic"],"remaining_risks":["close decision executes next open","event GU labels absent","costs ignored"]}
    (out/"compare.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}))
    print(json.dumps({"selected":selected,"checks":checks,"validation":result["authoritative_result"]["validation"]},ensure_ascii=False))
if __name__=="__main__":main()
