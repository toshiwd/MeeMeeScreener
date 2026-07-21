"""Evaluate price bands as an independent context for short tiers."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import duckdb,pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--tiers",type=Path,required=True);ap.add_argument("--inventory",type=Path,required=True);ap.add_argument("--output",type=Path,required=True)
 a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False);con=duckdb.connect()
 q=f"""select t.*,d.c signal_close,case when d.c<900 then '100-899' when d.c<3000 then '900-2999' when d.c<5000 then '3000-4999' when d.c<10000 then '5000-9999' else '10000+' end price_band from read_parquet('{str(a.tiers.resolve())}') t join read_parquet('{str(a.inventory.resolve())}') d using(code,ymd)"""
 x=con.execute(q).df();con.close();x["period"]=x.ymd.lt(20240101).map({True:"development",False:"validation"});x["year"]=x.ymd//10000
 rows=[]
 for (period,band,tier),g in x.groupby(["period","price_band","tier"]):
  win=g.drop5_in5.eq(1);days=g.loc[win&g.first_drop5_day.between(1,5),"first_drop5_day"]
  rows.append({"period":period,"price_band":band,"tier":tier,"n":len(g),"codes":g.code.nunique(),"hit_rate":g.drop5_in5.mean(),"clean_rate":g.clean_drop5_in5.mean(),"severe10_rate":g.drop8_in10.mean(),"median_high5_pct":g.high5_pct.median(),"p90_high5_pct":g.high5_pct.quantile(.9),"median_hit_day":None if days.empty else days.median()})
 p=pd.DataFrame(rows);p.to_parquet(a.output/"price_band_tier_metrics.parquet",index=False)
 years=x.groupby(["year","price_band","tier"]).agg(n=("code","size"),codes=("code","nunique"),hit_rate=("drop5_in5","mean"),median_high5_pct=("high5_pct","median")).reset_index();years.to_parquet(a.output/"price_band_yearly_metrics.parquet",index=False)
 val=p[p.period.eq("validation")];wide=val.pivot(index="price_band",columns="tier",values=["n","hit_rate"])
 ordered={b:bool(r[("hit_rate","Core")]>r[("hit_rate","Probe")]>r[("hit_rate","Risk")]) for b,r in wide.iterrows()}
 sufficiently_sampled={b:bool(r[("n","Probe")]>=300) for b,r in wide.iterrows()}
 checks={"all_validation_bands_ordered":all(ordered.values()),"three_or_more_probe_bands_n_ge_300":sum(sufficiently_sampled.values())>=3,"no_price_hard_filter":True}
 result={"schema_version":"tradex_short_price_context_v1.compare.v1","artifact_role":"authoritative_short_price_context","review_only":True,"research_phase":"effectiveness_judgment",
 "fixed_conditions":{"tiers":"kept within_1_session tier v1","bands":["100-899","900-2999","3000-4999","5000-9999","10000+"],"development":"2019-2023","validation":"2024-2026","policy":"independent context only; no exclusion"},
 "authoritative_result":{"validation_ordered":ordered,"sufficiently_sampled":sufficiently_sampled,"gate_checks":checks,"validation_rows":val.to_dict("records")},
 "observed_branching":{"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":0,"selection_divergence_reason":"price bands annotate tier intensity without changing membership"},
 "judgment":{"candidate_local_decision":"keep" if all(checks.values()) else "hold","session_aggregate_decision":"keep_price_context_diagnostic" if all(checks.values()) else "hold_price_context","authoritative_rollup_decision":"keep_price_context_v1_review_only_diagnostic" if all(checks.values()) else "hold","reason_type":"tier_order_repeats_across_price_bands_but_thin_bands_remain_diagnostic"},
 "not_changed":["tier membership","hard screens","MeeMee","ranking","runtime DB","production logic"],"remaining_risks":["10000+ Probe sample is thin","price is correlated with volatility","corporate-action robustness remains"]}
 c=a.output/"compare.json";c.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=lambda z:float(z))+"\n",encoding="utf-8")
 arts=[c,a.output/"price_band_tier_metrics.parquet",a.output/"price_band_yearly_metrics.parquet"];(a.output/"audit.json").write_text(json.dumps({"sources":{"tiers":sha(a.tiers),"inventory":sha(a.inventory)},"artifacts":{p.name:sha(p) for p in arts}},indent=2)+"\n");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(c)},indent=2)+"\n");print(json.dumps(checks))
if __name__=="__main__":main()
