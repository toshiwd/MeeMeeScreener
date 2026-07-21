from __future__ import annotations
import argparse, hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import duckdb, numpy as np, pandas as pd
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
from scripts.tradex_early_signal_first_detect_v1 import _metrics, _branching

AXIS_ID="tradex_riskoff_capitulation_current_contract_v1"
OUT=Path(r"G:\Tradex\tradex_riskoff_capitulation_current_contract_v1")
PERIODS={"train":(20240101,20241231),"validation":(20250101,20251231),"shadow":(20260101,20261231)}

def _ready(x:Any)->Any:
 if isinstance(x,dict):return {str(k):_ready(v) for k,v in x.items()}
 if isinstance(x,(list,tuple)):return [_ready(v) for v in x]
 if isinstance(x,(np.integer,)):return int(x)
 if isinstance(x,(np.floating,)):return None if np.isnan(x) else float(x)
 if isinstance(x,(np.bool_,)):return bool(x)
 return x
def _write(p:Path,x:dict[str,Any])->None:p.write_text(json.dumps(_ready(x),ensure_ascii=False,indent=2,sort_keys=True)+"\n",encoding="utf-8")
def _sha(p:Path)->str:
 h=hashlib.sha256()
 with p.open("rb") as f:
  for b in iter(lambda:f.read(8<<20),b""):h.update(b)
 return h.hexdigest()

def load(db:Path):
 with duckdb.connect(str(db),read_only=True) as q:
  f=q.execute("select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code from ml_feature_daily where dt between epoch(date '2020-01-01') and epoch(date '2026-12-31')").fetchdf()
  b=q.execute("select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,cast(code as varchar) code,o,h,l,c,v from daily_bars where source='pan' order by code,signal_ymd").fetchdf()
  r=q.execute("select dt signal_ymd,cast(code as varchar) code,rank baseline_rank from ranking_appearance_daily where ranking_logic_version='ranking:trade:top50:v1' and dir='up' and rank<=10 and dt between 20240101 and 20261231").fetchdf()
 cov={"feature_rows":len(f),"feature_codes":f.code.nunique(),"feature_min_date":int(f.signal_ymd.min()),"feature_max_date":int(f.signal_ymd.max()),"pan_rows":len(b),"pan_codes":b.code.nunique(),"ranking_rows_top10":len(r)}
 return f,b,r,cov

def build_scores(features:pd.DataFrame,bars:pd.DataFrame)->pd.DataFrame:
 parts=[]
 for _,p in bars.groupby("code",sort=False):
  p=p.sort_values("signal_ymd").copy();p["prev_c"]=p.c.shift(1);p["c5"]=p.c.shift(5);p["vol20"]=p.v.shift(1).rolling(20,20).mean();p["ma20"]=p.c.rolling(20,20).mean();p["ret5"]=p.c/p.c5-1;p["vol_ratio20"]=p.v/p.vol20;p["close_pos"]=(p.c-p.l)/(p.h-p.l);parts.append(p)
 x=pd.concat(parts,ignore_index=True).merge(features,on=["signal_ymd","code"],how="inner",validate="one_to_one")
 breadth=x.groupby("signal_ymd").apply(lambda q:float((q.c>q.ma20).mean()),include_groups=False).rename("breadth_above_ma20")
 x=x.merge(breadth,on="signal_ymd");x["family_hit"]=(x.ret5<=-.08)&(x.vol_ratio20>=2)&(x.c>x.o)&(x.close_pos>=.60)&(x.c>=x.prev_c*.99)&(x.breadth_above_ma20<=.45)
 x["score"]=x.family_hit.astype(float)*1000+(-x.ret5).fillna(-1)*10+x.vol_ratio20.fillna(0).clip(0,20)+x.close_pos.fillna(0)/100
 x=x.sort_values(["signal_ymd","score","code"],ascending=[True,False,True]);x["rank"]=x.groupby("signal_ymd").cumcount()+1;x["top10"]=x["rank"]<=10;x["side"]="BUY";return x

def attach_outcomes(frame:pd.DataFrame,bars:pd.DataFrame)->pd.DataFrame:
 rows=[]
 for code,p in bars.groupby("code",sort=False):
  p=p.sort_values("signal_ymd").reset_index(drop=True);n=len(p);entry=p.o.shift(-1).to_numpy(float);ret=np.full(n,np.nan);off=np.zeros(n,np.int16);target=np.zeros(n,np.int8);mover=np.zeros(n,np.int8)
  for d in range(1,11):
   op=p.o.shift(-d).to_numpy(float);hi=p.h.shift(-d).to_numpy(float);lo=p.l.shift(-d).to_numpy(float);mover|=(hi>=entry*1.08).astype(np.int8);u=off==0;gs=op<=entry*.95;gt=op>=entry*1.08;sl=lo<=entry*.95;tp=hi>=entry*1.08;ev=u&(gs|gt|sl|tp);off[ev]=d;rv=np.where(gs,op/entry-1,np.where(gt,op/entry-1,np.where(sl,-.05,.08)));ret[ev]=rv[ev];target[ev]=(gt|(~gs&~sl&tp))[ev]
  valid=np.arange(n)+10<n;u=(off==0)&valid;ret[u]=p.c.shift(-10).to_numpy(float)[u]/entry[u]-1;off[u]=10
  dates=p.signal_ymd.to_numpy();exit_ymd=np.array([dates[i+off[i]] if valid[i] else 0 for i in range(n)])
  rows.append(pd.DataFrame({"signal_ymd":p.signal_ymd,"code":str(code),"next_open":entry,"trade_return_h10":np.where(valid,ret-.001,np.nan),"exit_day_h10":np.where(valid,off,np.nan),"exit_ymd":np.where(valid,exit_ymd,np.nan),"target_before_stop20":np.where(valid,target,np.nan),"realized_mover20":np.where(valid,mover,np.nan)}))
 return frame.merge(pd.concat(rows,ignore_index=True),on=["signal_ymd","code"],how="left",validate="one_to_one")

def rolling_permission(raw:pd.DataFrame)->pd.DataFrame:
 out=[]
 for _,r in raw.sort_values("signal_ymd").iterrows():
  d=int(r.signal_ymd);start=int((pd.Timestamp(str(d))-pd.DateOffset(years=4)).strftime("%Y%m%d"));known=raw[(raw.signal_ymd>=start)&(raw.exit_ymd<d)&raw.entry_gate]
  m=_metrics(known, max(1,known.signal_ymd.nunique()));allowed=bool(m["n"]>=30 and (m["profit_factor"] or 0)>=1.2 and (m["expectancy"] or 0)>0)
  z=r.to_dict();z.update({"permission_n":m["n"],"permission_pf":m["profit_factor"],"permission_expectancy":m["expectancy"],"permission":allowed});out.append(z)
 return pd.DataFrame(out)

def no_reentry(events:pd.DataFrame,calendar:list[int],rank_col:str)->pd.DataFrame:
 pos={d:i for i,d in enumerate(calendar)};keep=[];until={}
 for i,r in events.sort_values(["signal_ymd",rank_col,"code"]).iterrows():
  c=str(r.code);p=pos[int(r.signal_ymd)]
  if until.get(c,-1)>=p:continue
  keep.append(i);until[c]=p+int(r.exit_day_h10)
 return events.loc[keep].copy()
def extra(f:pd.DataFrame,eligible:pd.DataFrame)->dict[str,Any]:
 return {"precision":float(f.target_before_stop20.mean()) if len(f) else None,"recall":float(f.realized_mover20.sum()/eligible.realized_mover20.sum()) if len(eligible) and eligible.realized_mover20.sum() else None}

def generate(db:Path,out:Path)->Path:
 f,b,r,cov=load(db);s=attach_outcomes(build_scores(f,b),b);s=s[s.signal_ymd>=20200101].copy();cal=sorted(s.signal_ymd.unique());raw=s[s.family_hit&s.trade_return_h10.notna()].sort_values(["signal_ymd","rank","code"]).groupby("signal_ymd",as_index=False).head(1).copy();raw["entry_gate"]=raw.next_open<=raw.c*1.03;paper=rolling_permission(raw);selected=no_reentry(paper[paper.entry_gate&paper.permission],cal,"rank");latest=int(f.signal_ymd.max());periods={**PERIODS,"shadow":(20260101,latest)};metrics={};branch={};coverage={};baselines=[]
 for split,(a,z) in periods.items():
  elig_days=set(selected[selected.signal_ymd.between(a,z)].signal_ymd);mm=r[r.signal_ymd.isin(elig_days)].sort_values(["signal_ymd","baseline_rank","code"]).groupby("signal_ymd",as_index=False).head(1).merge(s[["signal_ymd","code","trade_return_h10","exit_day_h10","target_before_stop20","realized_mover20"]],on=["signal_ymd","code"],how="left");mm=no_reentry(mm[mm.trade_return_h10.notna()],cal,"baseline_rank");baselines.append(mm);ch=selected[selected.signal_ymd.between(a,z)];days=sum(a<=d<=z for d in cal);allx=s[s.signal_ymd.between(a,z)&s.trade_return_h10.notna()];metrics[split]={"challenger_raw_top1_permission":{**_metrics(ch,days),**extra(ch,allx)},"meemee_buy_top1_same_eligible_days":{**_metrics(mm,days),**extra(mm,allx)}};branch[split]=_branching(s[s.top10],r.assign(side="BUY"),"BUY",a,z);cnt=s[s.signal_ymd.between(a,z)].groupby("signal_ymd").size();coverage[split]={"days":len(cnt),"min_ranked":int(cnt.min()) if len(cnt) else 0,"all_days_ranked":bool(cnt.gt(0).all()),"raw_hit_days":int(raw.signal_ymd.between(a,z).sum()),"permission_entry_days":len(ch)}
 root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);s.to_parquet(root/"all_symbol_daily_buy_scores.parquet",index=False);paper.to_parquet(root/"raw_top1_permission_audit.parquet",index=False);selected.to_parquet(root/"challenger_entry_ledger.parquet",index=False);pd.concat(baselines,ignore_index=True).to_parquet(root/"meemee_buy_same_eligible_ledger.parquet",index=False)
 vm=metrics["validation"]["challenger_raw_top1_permission"];keep=bool(vm["n"]>=5 and (vm["profit_factor"] or 0)>=1.2 and (vm["expectancy"] or 0)>0)
 payload={"schema_version":AXIS_ID+".compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"split":"2024 train, 2025 validation, 2026 untouched shadow","raw_hit":"ret5<=-8%, volume/prior20>=2, c>o, close_pos>=.60, c>=prev_c*.99, same-day breadth_above_ma20<=.45","ranking":"all symbols scored and ranked daily; raw family-hit top1 paper only","entry":"next open gap<=3%","exit":"TP8/SL5/H10/10bp; gap-through actual open; stop-first","permission":"trailing four years, raw top1 entry-gated paper outcomes with exit_ymd strictly before d; n>=30 daily PF>=1.2 expectancy>0","reentry":"prohibited only after permission","baseline":"MeeMee BUY top1 on challenger eligible days","fallback":False,"tuning":False,"survivorship_filter":False},"source_artifacts":[{"path":str(db),"sha256":_sha(db)}],"source_coverage":cov,"metrics":metrics,"branching":branch,"rank_coverage":coverage,"decision":{"candidate_local_decision":"keep" if keep else "drop","authoritative_rollup_decision":"review_only"},"shadow_tuning_used":False,"silent_fallback_used":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False};p=root/"compare.json";_write(p,payload);_write(root/"_ARTIFACT_COMPLETE.json",{"complete":True,"compare":str(p)});return p
def main():
 p=argparse.ArgumentParser();p.add_argument("--db",type=Path,required=True);p.add_argument("--out",type=Path,default=OUT);a=p.parse_args();print(generate(a.db,a.out))
if __name__=="__main__":main()
