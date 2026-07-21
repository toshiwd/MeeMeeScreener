from __future__ import annotations
import argparse,hashlib,json,sys
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
import duckdb,numpy as np,pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.backend.services.market_watch_tags import NIKKEI_225_CODES
from scripts.tradex_market_calendar_v1 import is_japan_market_business_day,market_calendar_metadata
AXIS_ID="tradex_nikkei225_exact_multitimeframe_features_v1"
MORPH=["ret","gap","range_atr","signed_body","upper_wick_atr","lower_wick_atr","close_pos","dist_ma7_atr","dist_ma20_atr","dist_ma60_atr","volume_pace"]
CHANNELS=MORPH+["completion_ratio"]
def sha(p):
 h=hashlib.sha256()
 with open(p,"rb") as f:
  for b in iter(lambda:f.read(8<<20),b""):h.update(b)
 return h.hexdigest()
def dump(p,x):p.write_text(json.dumps(x,ensure_ascii=False,indent=2)+"\n",encoding="utf8")
def load(db:Path):
 c=duckdb.connect(str(db),read_only=True);codes=",".join("?" for _ in NIKKEI_225_CODES)
 q=f"""select cast(code as varchar) code,cast(strftime(to_timestamp(cast(date as bigint)),'%Y%m%d') as int) ymd,o,h,l,c,v from daily_bars where cast(code as varchar) in ({codes}) and coalesce(source,'pan')<>'yahoo' and o>0 and c>0 and h>=greatest(o,c) and l<=least(o,c) and date>=epoch(date '2013-01-01') order by code,date"""
 d=c.execute(q,sorted(NIKKEI_225_CODES)).fetchdf();old=c.execute("select distinct cast(strftime(to_timestamp(cast(date as bigint)),'%Y%m%d') as int) ymd from daily_bars where coalesce(source,'pan')<>'yahoo' and o>0 and c>0 and h>=greatest(o,c) and l<=least(o,c) and date>=epoch(date '2013-01-01') and date<epoch(date '2018-01-01') order by 1").fetchdf();c.close()
 days=[];cur=date(2018,1,1);end=date(2031,12,31)
 while cur<=end:
  if is_japan_market_business_day(cur):days.append(int(cur.strftime("%Y%m%d")))
  cur+=timedelta(days=1)
 cal=pd.DataFrame({"ymd":sorted(set(old.ymd.astype(int)).union(days))});return d,cal
def period_features(d,cal,freq,lags):
 x=d.copy();x["dt"]=pd.to_datetime(x.ymd.astype(str));x["period"]=x.dt.dt.to_period(freq).astype(str);calendar=cal.copy();calendar["dt"]=pd.to_datetime(calendar.ymd.astype(str));calendar["period"]=calendar.dt.dt.to_period(freq).astype(str);sched=calendar.groupby("period").size().rename("scheduled")
 g=x.groupby(["code","period"],sort=False);x["po"]=g.o.transform("first");x["ph"]=g.h.cummax();x["pl"]=g.l.cummin();x["pc"]=x.c;x["pv"]=g.v.cumsum();x["elapsed"]=x.period.map(calendar.groupby("period").apply(lambda z:dict(zip(z.ymd,range(1,len(z)+1)))).to_dict()).combine(x.ymd,lambda m,y:m.get(y,np.nan));x["scheduled"]=x.period.map(sched)
 agg=g.agg(o=("o","first"),h=("h","max"),l=("l","min"),c=("c","last"),v=("v","sum")).reset_index();agg["sessions"]=agg.period.map(sched);agg["ord"]=agg.groupby("code").cumcount();x=x.merge(agg[["code","period","ord"]],on=["code","period"],how="left")
 ag=agg.groupby("code",group_keys=False);prev=ag.c.shift();tr=np.maximum.reduce([(agg.h-agg.l).to_numpy(),abs(agg.h-prev).to_numpy(),abs(agg.l-prev).to_numpy()]);agg["tr"]=tr
 for n in (7,20,60):agg[f"ma{n}"]=ag.c.transform(lambda s:s.rolling(n,min_periods=n).mean())
 agg["atr14"]=ag.tr.transform(lambda s:s.rolling(14,min_periods=14).mean());agg["base_vol_rate"]=ag.v.transform(lambda s:s.shift().rolling(20,min_periods=20).sum())/ag.sessions.transform(lambda s:s.shift().rolling(20,min_periods=20).sum())
 def calc(o,h,l,c,v,sessions,completion,prev_c,atr,ma7,ma20,ma60,base):
  rng=np.asarray(h)-np.asarray(l);safe=np.where(rng!=0,rng,np.nan)
  return {"ret":c/prev_c-1,"gap":o/prev_c-1,"range_atr":rng/atr,"signed_body":(c-o)/atr,"upper_wick_atr":(h-np.maximum(o,c))/atr,"lower_wick_atr":(np.minimum(o,c)-l)/atr,"close_pos":(c-l)/safe,"dist_ma7_atr":(c-ma7)/atr,"dist_ma20_atr":(c-ma20)/atr,"dist_ma60_atr":(c-ma60)/atr,"volume_pace":(v/sessions)/base,"completion_ratio":completion}
 # Completed-period feature table.
 vals=calc(agg.o,agg.h,agg.l,agg.c,agg.v,agg.sessions,np.ones(len(agg)),prev,agg.atr14,agg.ma7,agg.ma20,agg.ma60,agg.base_vol_rate)
 for k,v in vals.items():agg[k]=v
 # Current partial uses current bar plus strictly prior completed periods. Precompute state by period ordinal.
 ag=agg.groupby("code",group_keys=False);agg["prev_c"]=ag.c.shift();agg["prior_tr13_sum"]=ag.tr.transform(lambda s:s.shift().rolling(13,min_periods=13).sum())
 for n in (7,20,60):agg[f"prior_c{n-1}_sum"]=ag.c.transform(lambda s,n=n:s.shift().rolling(n-1,min_periods=n-1).sum())
 agg["prior_v20_sum"]=ag.v.transform(lambda s:s.shift().rolling(20,min_periods=20).sum());agg["prior_s20_sum"]=ag.sessions.transform(lambda s:s.shift().rolling(20,min_periods=20).sum())
 state=["code","ord","prev_c","prior_tr13_sum","prior_c6_sum","prior_c19_sum","prior_c59_sum","prior_v20_sum","prior_s20_sum"]
 cur=x.merge(agg[state],on=["code","ord"],how="left",validate="many_to_one");curtr=np.maximum.reduce([(cur.ph-cur.pl).to_numpy(),abs(cur.ph-cur.prev_c).to_numpy(),abs(cur.pl-cur.prev_c).to_numpy()]);atr=(cur.prior_tr13_sum+curtr)/14;ma7=(cur.prior_c6_sum+cur.pc)/7;ma20=(cur.prior_c19_sum+cur.pc)/20;ma60=(cur.prior_c59_sum+cur.pc)/60;base=cur.prior_v20_sum/cur.prior_s20_sum
 vals=calc(cur.po,cur.ph,cur.pl,cur.pc,cur.pv,cur.elapsed,cur.elapsed/cur.scheduled,cur.prev_c,atr,ma7,ma20,ma60,base)
 for k,v in vals.items():cur[k]=v
 out=cur[["code","ymd"]].copy()
 for lag in range(lags):
  if lag==0:q=cur
  else:
   q=cur[["code","ymd","ord"]].assign(ord=lambda z:z.ord-lag).merge(agg[["code","ord"]+CHANNELS],on=["code","ord"],how="left")
  for ch in CHANNELS:out[f"{freq}_{ch}_lag{lag}"]=q[ch].to_numpy()
  for ch in MORPH:out[f"{freq}_{ch}_lag{lag}_missing"]=q[ch].isna().astype("int8").to_numpy()
 return out,x,agg
def run(db,outroot):
 d,cal=load(db);w,wx,wa=period_features(d,cal,"W-SUN",12);m,mx,ma=period_features(d,cal,"M",6);out=w.merge(m,on=["code","ymd"],validate="one_to_one");out=out[out.ymd>=20190101];rawcols=2+12*12+6*12;maskcols=12*11+6*11
 # PIT audits: current partial equals prefix aggregates; future mutation cannot alter stored prefix cumulative values by construction.
 sample=wx[wx.ymd>=20190101].sample(min(100,len(wx[wx.ymd>=20190101])),random_state=20260714);prefix=[];wg={(c,p):z for (c,p),z in wx.groupby(["code","period"],sort=False)}
 for r in sample.itertuples():
  z=wg[(r.code,r.period)];z=z[z.ymd<=r.ymd];prefix.append(bool(len(z) and z.o.iloc[0]==r.po and z.h.max()==r.ph and z.l.min()==r.pl and z.c.iloc[-1]==r.pc and z.v.sum()==r.pv))
 # Full derived-feature invariance on a deterministic multi-code cutoff.
 acodes=sorted(d.code.unique())[:2];cutoff=20240715;small=d[d.code.isin(acodes)].copy();base=out[(out.code.isin(acodes))&(out.ymd<=cutoff)].sort_values(["code","ymd"]).reset_index(drop=True)
 tw,*_=period_features(small[small.ymd<=cutoff],cal,"W-SUN",12);tm,*_=period_features(small[small.ymd<=cutoff],cal,"M",6);trunc=tw.merge(tm,on=["code","ymd"]).sort_values(["code","ymd"]).reset_index(drop=True);trunc=trunc[trunc.ymd>=20190101].reset_index(drop=True)
 mutated=small.copy();future=mutated.ymd>cutoff;mutated.loc[future,"o"]*=1.7;mutated.loc[future,"h"]*=2.1;mutated.loc[future,"l"]*=.4;mutated.loc[future,"c"]*=1.8;mutated.loc[future,"v"]*=9
 uw,*_=period_features(mutated,cal,"W-SUN",12);um,*_=period_features(mutated,cal,"M",6);mut=uw.merge(um,on=["code","ymd"]);mut=mut[(mut.ymd>=20190101)&(mut.ymd<=cutoff)].sort_values(["code","ymd"]).reset_index(drop=True)
 feat=[c for c in base if c not in ("code","ymd")];prefix_full=base[["code","ymd"]].equals(trunc[["code","ymd"]]) and np.allclose(base[feat],trunc[feat],equal_nan=True);future_full=base[["code","ymd"]].equals(mut[["code","ymd"]]) and np.allclose(base[feat],mut[feat],equal_nan=True)
 latest=outroot/(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")+"-"+AXIS_ID);latest.mkdir(parents=True);p=latest/"exact_multitimeframe_features.parquet";out.to_parquet(p,index=False)
 calfile=Path(__file__).with_name("tradex_market_calendar_v1.py");july=cal[(cal.ymd>=20260701)&(cal.ymd<=20260731)];july_elapsed=int((july.ymd<=20260713).sum());july_sched=len(july)
 audit={"schema_version":AXIS_ID+".audit.v2","source_db":str(db),"source_db_sha256":sha(db),"calendar":{"source":"tradex_market_calendar_v1_2018_2031_plus_pre2018_pan_union","canonical_metadata":market_calendar_metadata(),"canonical_source_sha256":sha(calfile),"session_vector_sha256":hashlib.sha256(",".join(map(str,cal.ymd)).encode()).hexdigest(),"sessions":len(cal),"july_2026":{"elapsed_through_20260713":july_elapsed,"scheduled_full_month":july_sched,"completion_ratio":july_elapsed/july_sched}},"contract":{"artifact_scope":"TF414_only; exact model854 is daily440 join TF414","daily_raw":220,"weekly_raw":144,"monthly_raw":72,"raw_total_after_join":436,"daily_masks":220,"weekly_masks":132,"monthly_masks":66,"mask_total_after_join":418,"model_total_after_join":854,"weekly_lags":12,"monthly_lags":6,"partial_in_current":True,"week":"JST Monday-start represented W-SUN period"},"rows":len(out),"codes":out.code.nunique(),"columns":len(out.columns),"tf_feature_columns":len(out.columns)-2,"prefix_invariance":{"ohlcv_sample_n":len(prefix),"ohlcv_passed":all(prefix),"full_derived_truncated_regeneration_codes":acodes,"cutoff":cutoff,"passed":bool(prefix_full)},"future_mutation":{"codes":acodes,"cutoff":cutoff,"mutated":"all OHLCV after cutoff","passed":bool(future_full)},"artifact":str(p),"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False}}
 complete=all(prefix) and prefix_full and future_full and len(out.columns)-2==414 and july_elapsed<july_sched;dump(latest/"audit.json",audit);dump(latest/"_ARTIFACT_COMPLETE.json",{"complete":bool(complete),"audit":str(latest/"audit.json")});return latest
def main():
 a=argparse.ArgumentParser();a.add_argument("--db",type=Path,required=True);a.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_exact_multitimeframe_features_v1"));x=a.parse_args();print(run(x.db,x.output_root))
if __name__=="__main__":main()
