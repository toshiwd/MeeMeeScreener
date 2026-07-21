from __future__ import annotations

import argparse, hashlib, itertools, json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

try:
    from scripts.tradex_early_signal_first_detect_v1 import _branching, _metrics
except ModuleNotFoundError:
    from tradex_early_signal_first_detect_v1 import _branching, _metrics

AXIS_ID="tradex_shallow_high_zone_universe_repair_v1"
DEFAULT_OUT=Path(r"G:\Tradex\tradex_shallow_high_zone_universe_repair_v1")
INVALID_SOURCE=Path(r"G:\Tradex\chart_entry_geometry_research_v1\20260711T104710Z-shallow_high_zone_next_open_execution_v1\compare.json")
PERIODS={"train":(20240101,20241231),"validation":(20250101,20251231),"shadow":(20260101,20261231)}

def _ready(v:Any)->Any:
    if isinstance(v,dict): return {str(k):_ready(x) for k,x in v.items()}
    if isinstance(v,(list,tuple)): return [_ready(x) for x in v]
    if isinstance(v,np.integer): return int(v)
    if isinstance(v,np.floating): return None if not np.isfinite(v) else float(v)
    if isinstance(v,float): return None if not np.isfinite(v) else v
    if isinstance(v,Path): return str(v)
    return v
def _write(p:Path,x:dict[str,Any])->None:p.write_text(json.dumps(_ready(x),ensure_ascii=False,indent=2,sort_keys=True)+"\n",encoding="utf-8")
def _sha(p:Path)->str:
    h=hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda:f.read(8<<20),b""):h.update(b)
    return h.hexdigest()

def load_source(db:Path):
    with duckdb.connect(str(db),read_only=True) as c:
        f=c.execute("""select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code
          from ml_feature_daily where dt between epoch(date '2023-09-01') and epoch(date '2026-12-31') order by code,signal_ymd""").fetchdf()
        b=c.execute("""select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code,o,h,l,c,v
          from daily_bars where source='pan' order by code,signal_ymd""").fetchdf()
        r=c.execute("""select dt signal_ymd,cast(code as varchar) code,rank baseline_rank from ranking_appearance_daily
          where ranking_logic_version='ranking:trade:top50:v1' and dir='up' and rank<=10 and dt between 20240101 and 20261231 order by dt,rank,code""").fetchdf()
    cov={"feature_min_date":int(f.signal_ymd.min()),"feature_max_date":int(f.signal_ymd.max()),"feature_rows":len(f),"feature_codes":f.code.nunique(),"pan_min_date":int(b.signal_ymd.min()),"pan_max_date":int(b.signal_ymd.max()),"pan_rows":len(b),"pan_codes":b.code.nunique(),"ranking_rows_top10":len(r)}
    return f,b,r,cov

def build_leaf_frame(features:pd.DataFrame,bars:pd.DataFrame)->pd.DataFrame:
    parts=[]
    for _,p in bars.groupby("code",sort=False):
        p=p.sort_values("signal_ymd").copy(); g=p
        p["prior_high1"]=g.h.shift(1);p["prior_high20"]=g.h.shift(1).rolling(20,20).max();p["ma20"]=g.c.rolling(20,20).mean();p["ma20_5ago"]=p.ma20.shift(5);p["ma60"]=g.c.rolling(60,60).mean();p["avg_volume20"]=g.v.rolling(20,20).mean();p["high10"]=g.h.rolling(10,10).max();p["low10"]=g.l.rolling(10,10).min();parts.append(p)
    x=pd.concat(parts,ignore_index=True).merge(features,on=["signal_ymd","code"],how="inner",validate="one_to_one")
    x["gap_ma60"]=x.c/x.ma60-1;x["ma20_slope5"]=x.ma20/x.ma20_5ago-1;x["range10"]=x.high10/x.low10-1;x["volume_ratio20"]=x.v/x.avg_volume20;x["prior_high1_gap"]=x.c/x.prior_high1-1
    base=(x.ma20>x.ma60)&(x.c>=x.prior_high20*.95)&x.l.between(x.ma20*.99,x.ma20)&(x.c>x.ma20)&(x.c>x.o)&((x.c-x.l)/(x.h-x.l)>=.70)
    x["shape_leaf"]=np.select([base&(x.gap_ma60<=.0370751)&(x.ma20_slope5>.0107939),base&(x.gap_ma60>.0370751)&(x.range10<=.0544289)&(x.volume_ratio20<=.995031)&(x.prior_high1_gap>.000984905),base&(x.gap_ma60>.0370751)&(x.range10>.0544289)&(x.prior_high1_gap>.0233188)],[9,14,20],default=0)
    return x

def attach_gap_outcomes(frame:pd.DataFrame,bars:pd.DataFrame)->pd.DataFrame:
    rows=[]
    for code,p in bars.groupby("code",sort=False):
        p=p.sort_values("signal_ymd").reset_index(drop=True);n=len(p);entry=p.o.shift(-1).to_numpy(float)
        ret=np.full(n,np.nan);off=np.zeros(n,dtype=np.int16);mover=np.zeros(n,dtype=np.int8);target_first=np.zeros(n,dtype=np.int8)
        for d in range(1,11):
            op=p.o.shift(-d).to_numpy(float);hi=p.h.shift(-d).to_numpy(float);lo=p.l.shift(-d).to_numpy(float)
            mover|=(hi>=entry*1.08).astype(np.int8);unresolved=off==0
            gap_stop=op<=entry*.95;gap_tp=op>=entry*1.08;stop=lo<=entry*.95;tp=hi>=entry*1.08
            event=unresolved&(gap_stop|gap_tp|stop|tp);off[event]=d
            rv=np.where(gap_stop,op/entry-1,np.where(gap_tp,op/entry-1,np.where(stop,-.05,.08)))
            ret[event]=rv[event];target_first[event]=(gap_tp|(~gap_stop&~stop&tp))[event].astype(np.int8)
        valid=np.arange(n)+10<n;terminal=p.c.shift(-10).to_numpy(float)/entry-1
        unresolved=(off==0)&valid;ret[unresolved]=terminal[unresolved];off[unresolved]=10
        ret=np.where(valid,ret-.001,np.nan);off_out=np.where(valid,off,np.nan);mover_out=np.where(valid,mover,np.nan);win=np.where(valid,target_first,np.nan)
        rows.append(pd.DataFrame({"code":str(code),"signal_ymd":p.signal_ymd,"side":"BUY","trade_return_h10":ret,"exit_day_h10":off_out,"realized_mover20":mover_out,"target_before_stop20":win}))
    return frame.merge(pd.concat(rows,ignore_index=True),on=["code","signal_ymd"],how="left",validate="one_to_one")

def score(frame:pd.DataFrame,subset:tuple[int,...],priority:tuple[int,...])->pd.DataFrame:
    x=frame.copy();weights={leaf:len(priority)-i for i,leaf in enumerate(priority)}
    x["family_hit"]=x.shape_leaf.isin(subset);x["score"]=x.shape_leaf.map(weights).fillna(0)*100+x.gap_ma60.fillna(-1).clip(-1,1)
    x=x.sort_values(["signal_ymd","score","code"],ascending=[True,False,True]);x["rank"]=x.groupby("signal_ymd").cumcount()+1;x["top10"]=x["rank"]<=10;x["percentile"]=1-(x["rank"]-1)/x.groupby("signal_ymd").code.transform("size");x["side"]="BUY";return x

def no_reentry(events:pd.DataFrame,rank_col:str)->pd.DataFrame:
    accepted=[];active={}
    for day,g in events.sort_values(["signal_ymd",rank_col,"code"]).groupby("signal_ymd"):
        for i,r in g.iterrows():
            if active.get(str(r.code),0)>=int(day):continue
            accepted.append(i);active[str(r.code)]=int(r.signal_ymd)+int(r.exit_day_h10 or 10) # overwritten below with calendar position approximation
    return events.loc[accepted].copy()

def no_reentry_sessions(events:pd.DataFrame,calendar:list[int],rank_col:str)->pd.DataFrame:
    pos={d:i for i,d in enumerate(calendar)};accepted=[];until={}
    for day,g in events.sort_values(["signal_ymd",rank_col,"code"]).groupby("signal_ymd"):
        for i,r in g.iterrows():
            c=str(r.code)
            if until.get(c,-1)>=pos[int(day)]:continue
            accepted.append(i);until[c]=pos[int(day)]+int(r.exit_day_h10)
    return events.loc[accepted].copy()

def extra_metrics(f:pd.DataFrame,eligible:pd.DataFrame)->dict[str,Any]:
    return {"precision":float(f.target_before_stop20.mean()) if len(f) else None,"recall":float(f.realized_mover20.sum()/eligible.realized_mover20.sum()) if len(eligible) and eligible.realized_mover20.sum() else None}

def generate(db:Path,out_root:Path)->Path:
    features,bars,ranks,cov=load_source(db);base=attach_gap_outcomes(build_leaf_frame(features,bars),bars);base=base[base.signal_ymd>=20240101].copy();calendar=sorted(base.signal_ymd.unique());train_days=sum(d<=20241231 for d in calendar)
    variants=[]
    for n in (1,2,3):
      for subset in itertools.combinations((9,14,20),n):
       for priority in itertools.permutations(subset):
        s=score(base,subset,priority);e=s[s.top10&s.trade_return_h10.notna()];e=no_reentry_sessions(e,calendar,"rank");m=_metrics(e[e.signal_ymd<=20241231],train_days)
        variants.append({"subset":list(subset),"priority":list(priority),"train_metrics":m})
    ok=[v for v in variants if v["train_metrics"]["expectancy"] is not None and v["train_metrics"]["expectancy"]>0 and v["train_metrics"]["profit_factor"] is not None]
    if not ok:raise ValueError("NO_POSITIVE_2024_VARIANT")
    chosen=max(ok,key=lambda v:(v["train_metrics"]["profit_factor"],v["train_metrics"]["expectancy"]));s=score(base,tuple(chosen["subset"]),tuple(chosen["priority"]));s["split"]=np.select([s.signal_ymd<20250101,s.signal_ymd<20260101],["train","validation"],default="shadow")
    top=no_reentry_sessions(s[s.top10&s.trade_return_h10.notna()],calendar,"rank")
    baseline=ranks.merge(s[["signal_ymd","code","side","trade_return_h10","exit_day_h10","target_before_stop20","realized_mover20"]],on=["signal_ymd","code"],how="left",validate="many_to_one");baseline=baseline[baseline.trade_return_h10.notna()];baseline=no_reentry_sessions(baseline,calendar,"baseline_rank")
    latest=int(features.signal_ymd.max());periods={**PERIODS,"shadow":(20260101,latest)};metrics={};branch={};coverage={}
    for split,(a,z) in periods.items():
        days=sum(a<=d<=z for d in calendar);ch=top[top.signal_ymd.between(a,z)];bl=baseline[baseline.signal_ymd.between(a,z)];elig=s[s.signal_ymd.between(a,z)&s.trade_return_h10.notna()]
        metrics[split]={"challenger_top10":{**_metrics(ch,days),**extra_metrics(ch,elig)},"meemee_buy_top10":{**_metrics(bl,days),**extra_metrics(bl,elig)}};branch[split]=_branching(s[s.top10],ranks.assign(side="BUY"),"BUY",a,z);cnt=s[s.signal_ymd.between(a,z)].groupby("signal_ymd").size();coverage[split]={"days":len(cnt),"min_ranked":int(cnt.min()),"all_days_ranked":bool(cnt.gt(0).all())}
    root=out_root/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);s.to_parquet(root/"all_symbol_daily_scores.parquet",index=False);top.to_parquet(root/"challenger_top10_events.parquet",index=False);baseline.to_parquet(root/"meemee_buy_top10_events.parquet",index=False)
    payload={"schema_version":AXIS_ID+".compare.v1","artifact_role":"authoritative","research_phase":"comparison_stabilization","fixed_evaluation_conditions":{"tree_thresholds":"fixed existing leaves 9,14,20","leaf_subset_priority_selection":"2024 only; maximum PF among expectancy>0","validation":"2025","untouched_shadow":"2026","universe":"each-day feature-covered PAN; no latest-survivor filter","ranking":"all symbols daily including nonmatches","reentry":"same code prohibited while prior selected trade open","execution":"next-open TP8/SL5/H10/10bp; gap-through filled at actual future open; intraday dual hit stop-first","fallback":False},"invalid_source_evidence":{"path":str(INVALID_SOURCE),"status":"invalid_for_adoption_due_to_latest_survivor_filter","sha256":_sha(INVALID_SOURCE)},"source_artifacts":[{"path":str(db),"sha256":_sha(db)}],"source_coverage":cov,"train_variants":variants,"selected_variant":chosen,"metrics":metrics,"branching":branch,"rank_coverage":coverage,"shadow_tuning_used":False,"silent_fallback_used":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    p=root/"compare.json";_write(p,payload);_write(root/"_ARTIFACT_COMPLETE.json",{"complete":True,"compare":str(p)});return p

def main():
    p=argparse.ArgumentParser();p.add_argument("--db",type=Path,required=True);p.add_argument("--out",type=Path,default=DEFAULT_OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=="__main__":main()
