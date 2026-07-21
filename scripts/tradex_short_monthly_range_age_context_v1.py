"""Evaluate completed-month body-box age as independent context for short tiers."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import duckdb,pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def box(rows):
 for n in range(min(14,len(rows)),2,-1):
  w=rows[-n:];up=max(max(r[1],r[4]) for r in w);lo=min(min(r[1],r[4]) for r in w)
  if (up-lo)/max(abs(lo),1e-9)>.2:continue
  wild=any(r[2]>up*1.1 or r[3]<lo*.9 for r in w)
  return n,(w[-1][4]-lo)/max(up-lo,1e-9),wild,(up-lo)/max(abs(lo),1e-9)
 return None,None,None,None
def age_band(n):
 if n is None:return "none"
 if n<=4:return "3-4"
 if n<=7:return "5-7"
 if n<=11:return "8-11"
 return "12-14"
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--tiers",type=Path,required=True);ap.add_argument("--db",type=Path,required=True);ap.add_argument("--output",type=Path,required=True)
 a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False);t=pd.read_parquet(a.tiers)
 con=duckdb.connect(str(a.db),read_only=True)
 q="""select code::varchar code,cast(strftime(to_timestamp(date),'%Y%m') as integer) ym,first(o order by date) o,max(h) h,min(l) l,last(c order by date) c from daily_bars where lower(coalesce(source,''))='pan' group by 1,2 order by 1,2"""
 m=con.execute(q).df();con.close();hist={c:[tuple(r) for r in g[["ym","o","h","l","c"]].itertuples(index=False,name=None)] for c,g in m.groupby("code")}
 rows=[]
 for r in t.itertuples(index=False):
  sm=int(r.ymd)//100;prior=[x for x in hist.get(str(r.code),[]) if x[0]<sm]
  n,pos,wild,width=box(prior);d=r._asdict();d.update({"box_months":n,"box_age_band":age_band(n),"box_position":pos,"box_wild":wild,"box_width":width});rows.append(d)
 x=pd.DataFrame(rows);x.to_parquet(a.output/"monthly_range_context_ledger.parquet",index=False);x["period"]=x.ymd.lt(20240101).map({True:"development",False:"validation"});x["year"]=x.ymd//10000
 prof=x.groupby(["period","box_age_band","tier"]).agg(n=("code","size"),codes=("code","nunique"),hit_rate=("drop5_in5","mean"),clean_rate=("clean_drop5_in5","mean"),severe10_rate=("drop8_in10","mean"),median_high5_pct=("high5_pct","median"),p90_high5_pct=("high5_pct",lambda z:z.quantile(.9)),median_box_position=("box_position","median")).reset_index();prof.to_parquet(a.output/"monthly_range_tier_metrics.parquet",index=False)
 yr=x.groupby(["year","box_age_band","tier"]).agg(n=("code","size"),hit_rate=("drop5_in5","mean")).reset_index();yr.to_parquet(a.output/"monthly_range_yearly_metrics.parquet",index=False)
 val=prof[prof.period.eq("validation")];wide=val.pivot(index="box_age_band",columns="tier",values=["n","hit_rate"]);ordered={b:bool(r[("hit_rate","Core")]>r[("hit_rate","Probe")]>r[("hit_rate","Risk")]) for b,r in wide.dropna().iterrows()}
 checks={"three_or_more_age_bands":len(wide)>=3,"ordered_in_all_comparable_bands":all(ordered.values()),"box_age_is_context_not_filter":True}
 result={"schema_version":"tradex_short_monthly_range_age_context_v1.compare.v1","artifact_role":"authoritative_short_monthly_range_age_context","review_only":True,"research_phase":"effectiveness_judgment",
 "fixed_conditions":{"tier":"within_1_session v1","monthly_source":"completed PAN months before signal month","box_contract":"MeeMee body range <=20%; 3-14 months; wild wick >10% diagnostic","age_bands":["none","3-4","5-7","8-11","12-14"],"policy":"independent context only"},
 "authoritative_result":{"validation_ordered":ordered,"gate_checks":checks,"validation_rows":val.to_dict("records")},
 "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":0,"selection_divergence_reason":"monthly box age annotates tier without membership changes"},
 "judgment":{"candidate_local_decision":"keep" if all(checks.values()) else "hold","session_aggregate_decision":"keep_monthly_range_age_context" if all(checks.values()) else "hold_monthly_context","authoritative_rollup_decision":"keep_monthly_range_age_context_v1_review_only" if all(checks.values()) else "hold","reason_type":"completed_month_box_age_preserves_tier_order"},
 "not_changed":["MeeMee detector","tier membership","hard screens","ranking","runtime DB","production logic"],"remaining_risks":["current partial month excluded","body box contract may miss wider visual ranges","thin long-age bands possible"]}
 c=a.output/"compare.json";c.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=lambda z:float(z))+"\n",encoding="utf-8");arts=[c,a.output/"monthly_range_context_ledger.parquet",a.output/"monthly_range_tier_metrics.parquet",a.output/"monthly_range_yearly_metrics.parquet"];(a.output/"audit.json").write_text(json.dumps({"sources":{"tiers":sha(a.tiers),"db":str(a.db.resolve())},"artifacts":{p.name:sha(p) for p in arts}},indent=2)+"\n");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(c)},indent=2)+"\n");print(json.dumps({"checks":checks,"ordered":ordered}))
if __name__=="__main__":main()
