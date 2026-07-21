from __future__ import annotations
import argparse,json
from pathlib import Path
import duckdb,pandas as pd
def box(rows):
 for n in range(min(14,len(rows)),2,-1):
  w=rows[-n:];up=max(max(r[1],r[4]) for r in w);lo=min(min(r[1],r[4]) for r in w)
  if (up-lo)/max(abs(lo),1e-9)>.2:continue
  return n,(w[-1][4]-lo)/max(up-lo,1e-9)
 return None,None
def band(n):
 if n is None:return "なし"
 if n<=4:return "3-4か月"
 if n<=7:return "5-7か月"
 if n<=11:return "8-11か月"
 return "12-14か月"
def met(g):return {"n":int(len(g)),"codes":int(g.code.nunique()),"mean_return_pct":float(g.ret.mean()),"win_rate":float(g.ret.gt(0).mean()),"severe_loss5_rate":float(g.ret.le(-5).mean()),"p10_return_pct":float(g.ret.quantile(.1))}
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--managed",required=True);ap.add_argument("--db",required=True);ap.add_argument("--output",required=True);a=ap.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);d=pd.read_parquet(a.managed);d=d[(d.buy_family.eq("急落反発"))&d.initial_fraction.eq(.25)].copy();c=duckdb.connect(a.db,read_only=True);m=c.execute("select code::varchar code,cast(strftime(to_timestamp(date),'%Y%m') as integer) ym,first(o order by date) o,max(h) h,min(l) l,last(c order by date) c from daily_bars where lower(coalesce(source,''))='pan' group by 1,2 order by 1,2").df();c.close();hist={k:[tuple(r) for r in g[["ym","o","h","l","c"]].itertuples(index=False,name=None)] for k,g in m.groupby("code")};ages=[];pos=[]
 for r in d.itertuples(index=False):
  n,p=box([x for x in hist.get(str(r.code),[]) if x[0]<int(r.ymd)//100]);ages.append(band(n));pos.append(p)
 d["range_age"]=ages;d["range_position"]=pos;rows=[];yrs=[]
 for (p,b),g in d.groupby(["period","range_age"]):rows.append({"period":p,"range_age":b,**met(g)})
 for (y,b),g in d.assign(year=d.ymd//10000).groupby(["year","range_age"]):yrs.append({"year":int(y),"range_age":b,**met(g)})
 mt=pd.DataFrame(rows);yr=pd.DataFrame(yrs);bad=[]
 for b in d.range_age.unique():
  z=mt[mt.range_age.eq(b)].set_index("period")
  if set(z.index)=={"development","validation"} and z.loc["development","n"]>=1000 and z.loc["validation","n"]>=500 and z.loc["development","mean_return_pct"]<=0 and z.loc["validation","mean_return_pct"]<=0:bad.append(b)
 d["policy"]="取引";d.loc[d.range_age.isin(bad),"policy"]="見送り";tr=d[d.policy.eq("取引")];ov=pd.DataFrame([{"period":p,**met(g)} for p,g in tr.groupby("period")]);an=pd.DataFrame([{"year":int(y),**met(g)} for y,g in tr.assign(year=tr.ymd//10000).groupby("year")]);d.to_parquet(out/"monthly_range_ledger.parquet",index=False);mt.to_parquet(out/"monthly_range_metrics.parquet",index=False);yr.to_parquet(out/"monthly_range_yearly_metrics.parquet",index=False);checks={"bad_band_found":len(bad)>0,"development_positive":bool(ov[ov.period.eq("development")].iloc[0].mean_return_pct>0),"validation_positive":bool(ov[ov.period.eq("validation")].iloc[0].mean_return_pct>0),"all_years_positive":bool(len(an[an.year>=2024])==3 and (an[an.year>=2024].mean_return_pct>0).all()),"validation_tail_le10":bool(ov[ov.period.eq("validation")].iloc[0].severe_loss5_rate<=.1)};keep=all(checks.values())
 res={"schema_version":"tradex_long_rebound_monthly_range_v1.compare.v1","artifact_role":"authoritative_long_rebound_monthly_range","review_only":True,"fixed_conditions":{"family":"急落反発","management":"25% initial, add on immediate rise, exit on stall","monthly_source":"completed PAN months before signal month","box":"body range<=20%,3-14 months","age_bands":["なし","3-4か月","5-7か月","8-11か月","12-14か月"],"bad_band":"dev and validation mean<=0 with dev n>=1000,val n>=500"},"authoritative_result":{"bad_bands":bad,"band_metrics":mt.to_dict("records"),"policy_period":ov.to_dict("records"),"policy_years":an[an.year>=2024].to_dict("records"),"gate_checks":checks},"judgment":{"candidate_local_decision":"keep" if keep else "hold","session_aggregate_decision":"keep_rebound_monthly_gate" if keep else "hold_rebound_monthly_context","authoritative_rollup_decision":"keep_long_rebound_monthly_range_v1_review_only" if keep else "hold_no_stable_monthly_exclusion","reason_type":"stable_bad_monthly_band_and_yearly_gates"},"not_changed":["family","management","MeeMee","ranking","runtime DB","production logic"]}
 (out/"compare.json").write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"}));print(json.dumps(res["authoritative_result"],ensure_ascii=False))
if __name__=="__main__":main()
