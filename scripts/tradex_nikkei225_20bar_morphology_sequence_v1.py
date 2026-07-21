from __future__ import annotations

import argparse, hashlib, json, math, os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar
from sklearn.metrics import log_loss

AXIS_ID = "tradex_nikkei225_20bar_morphology_sequence_v1"
SEED = 20260714
HORIZONS = {1:(.75,.01,.03),3:(1.25,.02,.05),5:(1.5,.03,.07),10:(2.,.05,.10)}
CHANNELS = ["ret1","gap","range_atr","signed_body","upper_wick_atr","lower_wick_atr","close_pos_seq","dist_ma7_atr","dist_ma20_atr","dist_ma60_atr","log_volume"]
VARIANTS = {
 "V1":dict(num_leaves=7,max_depth=3,min_child_samples=400,reg_lambda=10),
 "V2":dict(num_leaves=15,max_depth=4,min_child_samples=400,reg_lambda=10),
 "V3":dict(num_leaves=31,max_depth=5,min_child_samples=600,reg_lambda=20),
 "V4":dict(num_leaves=15,max_depth=4,min_child_samples=400,reg_lambda=10,feature_fraction=.70,bagging_fraction=.70,bagging_freq=1),
}
Q4 = {"SELL":{1:(40,.55,.28),3:(35,.57,.25),5:(30,.57,.23),10:(25,.57,.23)},"REBOUND_RISK":{1:(40,.52,.23),3:(35,.55,.21),5:(30,.57,.18),10:(25,.57,.18)}}
FINAL = {"SELL":{1:(300,.58,.25,0),3:(240,.60,.22,-.005),5:(200,.60,.20,-.01),10:(160,.60,.20,-.015)},"REBOUND_RISK":{1:(300,.55,.20,0),3:(240,.58,.18,0),5:(200,.60,.15,0),10:(160,.60,.15,0)}}

def sha(path:Path)->str:
 h=hashlib.sha256()
 with path.open("rb") as f:
  for b in iter(lambda:f.read(8<<20),b""):h.update(b)
 return h.hexdigest()
def dump(path:Path,x:Any)->None:path.write_text(json.dumps(x,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
def canon_sha(x:Any)->str:return hashlib.sha256(json.dumps(x,sort_keys=True,separators=(",",":"),default=str).encode()).hexdigest()
def atomic_json(path:Path,x:Any)->None:
 tmp=path.with_suffix(path.suffix+f".{os.getpid()}.tmp");dump(tmp,x);os.replace(tmp,path)
def atomic_npz(path:Path,**arrays:Any)->None:
 tmp=path.with_suffix(path.suffix+f".{os.getpid()}.tmp");np.savez_compressed(tmp,**arrays);actual=Path(str(tmp)+".npz");os.replace(actual,path)
def labels(f:pd.DataFrame,h:int)->np.ndarray:
 m,lo,hi=HORIZONS[h]; d=np.clip(m*f.atr14.to_numpy()/f.c.to_numpy(),lo,hi); r=.8*d; ret=f[f"ret_close_{h}"].to_numpy(); de=f[f"down_exc_{h}"].to_numpy(); ue=f[f"up_exc_{h}"].to_numpy(); reb=((ue>=r)&(ret>=0))|(ret>=.4*r); down=(de<=-d)&(ret<=-.4*d)&~reb;return np.where(reb,1,np.where(down,0,2)).astype(np.int8)
def ece(y,p,k):
 q=np.unique(np.quantile(p,np.linspace(0,1,11)));out=0.
 for a,b in zip(q[:-1],q[1:]):
  z=(p>=a)&(p< b if b<q[-1] else p<=b)
  if z.any():out+=z.mean()*abs(p[z].mean()-(y[z]==k).mean())
 return float(out)
def scores(y,p):
 one=np.eye(3)[y];prev=np.bincount(y,minlength=3)/len(y);const=np.tile(prev,(len(y),1));gaps=[];slopes=[];intercepts=[]
 for k in range(3):
  q=np.unique(np.quantile(p[:,k],np.linspace(0,1,11)));bg=[]
  for a,b in zip(q[:-1],q[1:]):
   z=(p[:,k]>=a)&(p[:,k]<(b) if b<q[-1] else p[:,k]<=b)
   if z.sum()>=100:bg.append((float(p[z,k].mean()),float((y[z]==k).mean()),int(z.sum())))
  gaps.append(max((abs(a-b) for a,b,_ in bg),default=1.))
  if len(bg)>=2:
   xx=np.array([a for a,_,_ in bg]);yy=np.array([b for _,b,_ in bg]);w=np.array([n for *_,n in bg]);s,i=np.polyfit(xx,yy,1,w=np.sqrt(w));slopes.append(float(s));intercepts.append(float(i))
  else:slopes.append(None);intercepts.append(None)
 return {"n":len(y),"logloss":float(log_loss(y,p,labels=[0,1,2])),"constant_logloss":float(log_loss(y,const,labels=[0,1,2])),"brier":float(np.mean(np.sum((p-one)**2,axis=1))),"constant_brier":float(np.mean(np.sum((const-one)**2,axis=1))),"ece_by_class":[ece(y,p[:,k],k) for k in range(3)],"max_gap_by_class":gaps,"slope_by_class":slopes,"intercept_by_class":intercepts,"argmax_share":(np.bincount(np.argmax(p,axis=1),minlength=3)/len(y)).tolist(),"mean_probability_gap":(p.mean(0)-prev).tolist(),"class_counts":np.bincount(y,minlength=3).tolist()}
def model(v:str,n=300):return lgb.LGBMClassifier(objective="multiclass",num_class=3,n_estimators=int(n),learning_rate=.03,verbosity=-1,n_jobs=2,random_state=SEED,**VARIANTS[v])
def temp(p,t):
 z=np.log(np.clip(p,1e-9,1))/t;z-=z.max(1,keepdims=True);z=np.exp(z);return z/z.sum(1,keepdims=True)
def fit_temp(y,p):return float(minimize_scalar(lambda t:log_loss(y,temp(p,t),labels=[0,1,2]),bounds=(.25,4),method="bounded").x)

def features(f):
 g=f.sort_values(["code","ymd"]).copy(); prev=g.groupby("code").c.shift(); atr=g.atr14.replace(0,np.nan); rng=(g.h-g.l).replace(0,np.nan)
 g["ret1"]=g.c/prev-1;g["gap"]=g.o/prev-1;g["range_atr"]=(g.h-g.l)/atr;g["signed_body"]=(g.c-g.o)/atr;g["upper_wick_atr"]=(g.h-g[["o","c"]].max(axis=1))/atr;g["lower_wick_atr"]=(g[["o","c"]].min(axis=1)-g.l)/atr;g["close_pos_seq"]=(g.c-g.l)/rng;g["log_volume"]=np.log1p(g.v/g.vol20.replace(0,np.nan))
 out={}
 for c in CHANNELS:
  for lag in range(20):
   x=g.groupby("code")[c].shift(lag);out[f"{c}_lag{lag}"]=x.astype("float32");out[f"{c}_lag{lag}_missing"]=x.isna().astype("int8")
 x=pd.DataFrame(out,index=g.index);return g,x
def blocks(d):
 ms=sorted(d.astype(str).str[:6].astype(int).unique());return [(ms[:i],ms[i:i+3]) for i in range(12,len(ms),3) if ms[i:i+3]]
def event_mask(raw,code,dates):
 out=np.zeros(len(raw),bool);last={};pos={}
 for i in np.lexsort((dates,code)):
  c=code[i];j=pos.get(c,-1)+1;pos[c]=j
  if raw[i] and (c not in last or j-last[c]>10):out[i]=True;last[c]=j
 return out
def metric(f,y,p,lane,thr,h):
 main,opp=(0,1) if lane=="SELL" else (1,0);raw=(p[:,main]>=thr[0])&(p[:,opp]<=thr[1]);z=event_mask(raw,f.code.astype(str).to_numpy(),f.ymd.to_numpy());s=f.loc[z];n=int(z.sum());return {"n":n,"codes":int(s.code.nunique()) if n else 0,"months":int(s.ymd.astype(str).str[:6].nunique()) if n else 0,"coverage":float(z.mean()),"precision":float((y[z]==main).mean()) if n else None,"opposite":float((y[z]==opp).mean()) if n else None,"mean_return":float(s[f"ret_close_{h}"].mean()) if n else None,"max_code":float(s.groupby("code").size().max()/n) if n else None,"max_month":float(s.assign(month=s.ymd.astype(str).str[:6]).groupby("month").size().max()/n) if n else None,"mask":z}
def choose_threshold(f,y,p,lane,h):
 nmin,prec,opp=Q4[lane][h];cand=[]
 for a in np.arange(.30,.8001,.025):
  for b in np.arange(.10,.4001,.025):
   m=metric(f,y,p,lane,(float(a),float(b)),h)
   if nmin<=m["n"] and .01<=m["coverage"]<=.15 and m["precision"]>=prec and m["opposite"]<=opp:cand.append((m["precision"]-m["opposite"],m["coverage"],-a,b,(float(a),float(b)),m))
 return max(cand,key=lambda x:x[:4])[4:] if cand else (None,None)
def cluster_boot(groups,values,stat,seed):
 u,inv=np.unique(groups.astype(str),return_inverse=True);sums={k:np.bincount(inv,weights=v,minlength=len(u)) for k,v in values.items()};rng=np.random.default_rng(seed);vals=[]
 for _ in range(2000):
  w=np.bincount(rng.integers(0,len(u),len(u)),minlength=len(u));vals.append(stat({k:float(np.dot(v,w)) for k,v in sums.items()}))
 a=np.asarray(vals);return {"ci":[float(np.quantile(a,.025)),float(np.quantile(a,.975))],"p_ge0":float((1+(a>=0).sum())/2001),"p_le0":float((1+(a<=0).sum())/2001)}
def holm(ps):
 order=sorted(ps,key=ps.get);adj={};run=0.
 for rank,h in enumerate(order):run=max(run,(len(order)-rank)*ps[h]);adj[h]=min(1.,run)
 return {h:{"raw_p":ps[h],"holm_p":adj[h],"pass":adj[h]<=.05} for h in ps}

def run(inp:Path,outroot:Path)->Path:
 source_sha=sha(inp);raw=pd.read_parquet(inp);f,X=features(raw); names=list(X); train=f.ymd.between(20190101,20211231); med=X.loc[train].median().fillna(0);X=X.fillna(med).astype("float32"); results={};prob_rows=[];models={}
 feature_sha=canon_sha(names);median_sha=canon_sha({k:float(v) for k,v in med.items()});ckroot=outroot/"_ck"/source_sha[:8];ckroot.mkdir(parents=True,exist_ok=True);checkpoint_audit=[]
 for h in HORIZONS:
  valid=f[[f"ret_close_{h}",f"down_exc_{h}",f"up_exc_{h}","atr14","c"]].notna().all(axis=1);y=labels(f.loc[valid],h);fv=f.loc[valid].reset_index(drop=True);xv=X.loc[valid].reset_index(drop=True);tr=fv.ymd.between(20190101,20211231); months=fv.loc[tr,"ymd"].astype(str).str[:6].astype(int); variants={}
  for v in VARIANTS:
   pp=np.full((len(fv),3),np.nan);its=[]
   for fold,(fitm,testm) in enumerate(blocks(months)):
    a=tr&months.reindex(fv.index).isin(fitm).fillna(False);b=tr&months.reindex(fv.index).isin(testm).fillna(False)
    if not b.any():continue
    idx=np.flatnonzero(b);rowkeys_sha=canon_sha([[str(fv.code.iloc[j]),int(fv.ymd.iloc[j])] for j in idx]);label_sha=hashlib.sha256(y[idx].tobytes()).hexdigest();params={"variant":v,"variant_params":VARIANTS[v],"learning_rate":.03,"seed":SEED,"n_jobs":2,"early_stopping":30,"fit_months":list(map(int,fitm)),"test_months":list(map(int,testm))};meta={"source_sha256":source_sha,"feature_sha256":feature_sha,"exclusion_sha256":canon_sha({"valid_columns":[f"ret_close_{h}",f"down_exc_{h}",f"up_exc_{h}","atr14","c"],"valid_rows":int(valid.sum())}),"rowkeys_sha256":rowkeys_sha,"median_sha256":median_sha,"label_sha256":label_sha,"params":params,"lightgbm_version":lgb.__version__,"horizon":h,"variant":v,"fold":fold};key=canon_sha(meta);npzp=ckroot/f"h{h}{v}f{fold}_{key[:8]}.npz";jsonp=npzp.with_suffix(".json");reused=False
    if npzp.exists() and jsonp.exists():
     saved=json.loads(jsonp.read_text(encoding="utf8"));z=np.load(npzp);pred=z["prediction"];best_iteration=int(z["best_iteration"][0]);sample_sha=hashlib.sha256(np.ascontiguousarray(pred[:min(100,len(pred))]).tobytes()).hexdigest();reused=saved.get("contract_sha256")==key and saved.get("prediction_sample_sha256")==sample_sha and pred.shape==(len(idx),3)
    if not reused:
     mod=model(v);mod.set_params(n_jobs=2);mod.fit(xv.loc[a,names],y[a],eval_set=[(xv.loc[b,names],y[b])],callbacks=[lgb.early_stopping(30,verbose=False)]);best_iteration=int(mod.best_iteration_);pred=mod.predict_proba(xv.loc[b,names],num_iteration=best_iteration);sample_sha=hashlib.sha256(np.ascontiguousarray(pred[:min(100,len(pred))]).tobytes()).hexdigest();atomic_npz(npzp,prediction=pred,best_iteration=np.asarray([best_iteration],dtype=np.int32));atomic_json(jsonp,{**meta,"contract_sha256":key,"prediction_sample_sha256":sample_sha,"prediction_rows":len(idx),"complete":True})
    pp[idx]=pred;its.append(best_iteration);checkpoint_audit.append({"horizon":h,"variant":v,"fold":fold,"path":str(jsonp),"contract_sha256":key,"reused":reused,"prediction_sample_sha256":sample_sha})
   z=np.isfinite(pp).all(1);s=scores(y[z],pp[z]);variants[v]={"metrics":s,"median_iteration":int(np.median(its)),"oof_rows":int(z.sum())}
  eligible=[v for v,a in variants.items() if a["metrics"]["brier"]<a["metrics"]["constant_brier"] and max(a["metrics"]["ece_by_class"])<=.08]
  if eligible:
   min_loss=min(variants[v]["metrics"]["logloss"] for v in eligible);best="V1" if "V1" in eligible and variants["V1"]["metrics"]["logloss"]-min_loss<.001 else min(eligible,key=lambda v:variants[v]["metrics"]["logloss"])
  else:best=None
  if best is None:results[str(h)]={"variants":variants,"decision":{"general":"drop_no_oof_variant","SELL":"drop","REBOUND_RISK":"drop"}};continue
  n=variants[best]["median_iteration"];mod=model(best,n);mod.fit(xv.loc[tr,names],y[tr]);models[h]=mod;cal1=fv.ymd.between(20220101,20220630);sel=fv.ymd.between(20220701,20220930);ref=fv.ymd.between(20220101,20220930);q4=fv.ymd.between(20221001,20221231);ev=fv.ymd.between(20230101,20251231);ex=fv.ymd.between(20260101,20261231);p=mod.predict_proba(xv[names]);t1=fit_temp(y[cal1],p[cal1]);use="temperature" if log_loss(y[sel],temp(p[sel],t1),labels=[0,1,2])+1e-12<log_loss(y[sel],p[sel],labels=[0,1,2]) else "identity";T=fit_temp(y[ref],p[ref]) if use=="temperature" else 1.;pc=temp(p,T); lanes={}
  for lane in ("SELL","REBOUND_RISK"):
   thr,qm=choose_threshold(fv.loc[q4].reset_index(drop=True),y[q4],pc[q4],lane,h)
   if thr is None:lanes[lane]={"decision":"drop_no_2022_threshold"};continue
   ef=fv.loc[ev].assign(month=fv.loc[ev].ymd.astype(str).str[:6]).reset_index(drop=True);ey=y[ev];ep=pc[ev];em=metric(ef,ey,ep,lane,thr,h);main=0 if lane=="SELL" else 1;opp=1-main;fg=FINAL[lane][h]
   years={};absolute_years=0;direction=True
   for yr in (2023,2024,2025):
    z=fv.ymd.between(yr*10000+101,yr*10000+1231);m=metric(fv.loc[z].reset_index(drop=True),y[z],pc[z],lane,thr,h);bm=float((y[z]==main).mean());bo=float((y[z]==opp).mean());m["main_uplift"]=None if not m["n"] else m["precision"]-bm;m["opposite_delta"]=None if not m["n"] else m["opposite"]-bo;direction &= bool(m["n"] and m["main_uplift"]>0 and m["opposite_delta"]<=.02 and (lane!="SELL" or m["mean_return"]<0));absolute_years+=int(bool(m["n"] and m["precision"]>=fg[1] and m["opposite"]<=fg[2] and (lane!="SELL" or m["mean_return"]<=fg[3])));years[str(yr)]=m
   base_main=float((ey==main).mean());base_opp=float((ey==opp).mean());absok=em["n"]>=fg[0] and em["codes"]>=100 and em["months"]>=24 and .01<=em["coverage"]<=.15 and em["max_code"]<=.05 and em["max_month"]<=.15 and em["precision"]>=fg[1] and em["opposite"]<=fg[2] and (lane!="SELL" or em["mean_return"]<=fg[3]) and em["precision"]>=base_main+.05 and em["opposite"]<=base_opp-.03 and absolute_years>=2
   sel=em["mask"].astype(float);vals={"n":np.ones(len(ey)),"sel":sel,"main":(ey==main).astype(float),"opp":(ey==opp).astype(float),"sm":sel*(ey==main),"so":sel*(ey==opp),"ret":ef[f"ret_close_{h}"].to_numpy(),"sr":sel*ef[f"ret_close_{h}"].to_numpy()}
   def astat(d,key):
    selected=d["sel"];return (d["sm"]/selected-d["main"]/d["n"]) if key=="main" else (d["so"]/selected-d["opp"]/d["n"]) if key=="opp" else (d["sr"]/selected-d["ret"]/d["n"])
   boots={}
   for cluster,groups in (("code",ef.code.to_numpy()),("month",ef.month.to_numpy())):
    boots[cluster]={k:cluster_boot(groups,vals,lambda d,k=k:astat(d,k),SEED+h+(0 if cluster=="code" else 100)+(0 if k=="main" else 10 if k=="opp" else 20)) for k in (("main","opp","ret") if lane=="SELL" else ("main","opp"))}
   bootok=all(b["main"]["ci"][0]>0 and b["opp"]["ci"][1]<0 and (lane!="SELL" or b["ret"]["ci"][1]<0) for b in boots.values());primary=max([b["main"]["p_le0"] for b in boots.values()]+[b["opp"]["p_ge0"] for b in boots.values()]+([b["ret"]["p_ge0"] for b in boots.values()] if lane=="SELL" else []));lanes[lane]={"threshold":{"p_main":thr[0],"p_opposite":thr[1]},"q4":{k:v for k,v in qm.items() if k!="mask"},"frozen_2023_2025":{k:v for k,v in em.items() if k!="mask"},"yearly":{yr:{k:v for k,v in m.items() if k!="mask"} for yr,m in years.items()},"bootstrap":boots,"primary_p":primary,"baseline":{"main":base_main,"opposite":base_opp},"decision":"provisional_keep" if absok and direction and bootok else "drop","gate":{"absolute":absok,"direction":direction,"bootstrap":bootok}}
  sm=scores(y[ev],pc[ev]);one=np.eye(3)[y[ev]];prev=np.bincount(y[ev],minlength=3)/ev.sum();const=np.tile(prev,(ev.sum(),1));bd=np.sum((pc[ev]-one)**2,1)-np.sum((const-one)**2,1);ld=-np.log(np.clip(pc[ev][np.arange(ev.sum()),y[ev]],1e-12,1))+np.log(np.clip(prev[y[ev]],1e-12,1));ef=fv.loc[ev].assign(month=fv.loc[ev].ymd.astype(str).str[:6]).reset_index(drop=True);gb={}
  for cluster,groups in (("code",ef.code.to_numpy()),("month",ef.month.to_numpy())):
   gb[cluster]={"brier":cluster_boot(groups,{"x":bd,"n":np.ones(len(bd))},lambda d:d["x"]/d["n"],SEED+h+(0 if cluster=="code" else 1000)),"logloss":cluster_boot(groups,{"x":ld,"n":np.ones(len(ld))},lambda d:d["x"]/d["n"],SEED+h+10+(0 if cluster=="code" else 1000))}
  ys=[]
  for yr in (2023,2024,2025):
   z=fv.ymd.between(yr*10000+101,yr*10000+1231);s=scores(y[z],pc[z]);ys.append({"year":yr,**s,"brier_diff":s["brier"]-s["constant_brier"],"logloss_diff":s["logloss"]-s["constant_logloss"]})
  calok=max(sm["ece_by_class"])<=.05 and max(sm["max_gap_by_class"])<=.10 and all(x is not None and .8<=x<=1.2 for x in sm["slope_by_class"]) and all(x is not None and abs(x)<=.10 for x in sm["intercept_by_class"]) and min(sm["argmax_share"])>=.05 and max(abs(x) for x in sm["mean_probability_gap"])<=.05;yearok=all(x["brier_diff"]<0 and x["brier_diff"]<.005 for x in ys) and sum(x["logloss_diff"]<0 and max(x["ece_by_class"])<=.08 for x in ys)>=2;bootok=all(v[k]["ci"][1]<0 for v in gb.values() for k in ("brier","logloss"));primary=max(v[k]["p_ge0"] for v in gb.values() for k in ("brier","logloss"));general=sm["brier"]<sm["constant_brier"] and sm["logloss"]<sm["constant_logloss"] and calok and yearok and bootok;results[str(h)]={"variants":variants,"selected_variant":best,"iteration":n,"calibration":{"method":use,"temperature":T},"frozen_general":sm,"general_yearly":ys,"general_bootstrap":gb,"general_primary_p":primary,"lanes":lanes,"decision":{"general":"provisional_keep" if general else "drop","SELL":lanes["SELL"]["decision"] if general else "diagnostic_hold_general_failed","REBOUND_RISK":lanes["REBOUND_RISK"]["decision"] if general else "diagnostic_hold_general_failed"}}
  idx=np.flatnonzero(ex)
  for j in idx:prob_rows.append({"code":fv.code.iloc[j],"ymd":int(fv.ymd.iloc[j]),"horizon":h,"p_down":pc[j,0],"p_rebound":pc[j,1],"p_neutral":pc[j,2],"selected_variant":best,"calibration":use})
 gp={int(h):r["general_primary_p"] for h,r in results.items() if "general_primary_p" in r};gh=holm(gp) if gp else {}
 for h,a in gh.items():results[str(h)]["general_holm"]=a;results[str(h)]["decision"]["general"]="keep" if a["pass"] and results[str(h)]["decision"]["general"]=="provisional_keep" else "drop"
 for lane in ("SELL","REBOUND_RISK"):
  ps={int(h):r["lanes"][lane]["primary_p"] for h,r in results.items() if lane in r.get("lanes",{}) and "primary_p" in r["lanes"][lane]};hh=holm(ps) if ps else {}
  for h,a in hh.items():
   item=results[str(h)];item["lanes"][lane]["holm"]=a;ok=a["pass"] and item["decision"]["general"]=="keep" and item["lanes"][lane]["decision"]=="provisional_keep";item["lanes"][lane]["decision"]="keep" if ok else ("diagnostic_hold_general_failed" if item["decision"]["general"]!="keep" else "drop");item["decision"][lane]=item["lanes"][lane]["decision"]
  hs=sorted(hh)
  for i,h in enumerate(hs):
   neighbors=[x for x in hs if abs(hs.index(x)-i)==1];bad=False
   for x in neighbors:
    q=results[str(x)]["lanes"][lane];m=q["frozen_2023_2025"];b=q["baseline"];bad|=(m["precision"]-b["main"]<0 or m["opposite"]-b["opposite"]>.05)
   results[str(h)]["lanes"][lane]["adjacent_horizon_veto"]=bad
   if bad:results[str(h)]["lanes"][lane]["decision"]="drop";results[str(h)]["decision"][lane]="drop"
 stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=outroot/f"{stamp}-{AXIS_ID}";out.mkdir(parents=True);pd.DataFrame(prob_rows).to_parquet(out/"probability_ledger_2026.parquet",index=False);dump(out/"feature_manifest.json",{"channels":CHANNELS,"lags":list(range(20)),"missing_masks":True,"feature_names":names,"median_imputation":med.to_dict()})
 for h,m in models.items():joblib.dump(m,out/f"model_h{h}.joblib")
 artifacts=[]
 for pth in sorted(out.iterdir()):artifacts.append({"name":pth.name,"sha256":sha(pth),"size":pth.stat().st_size})
 payload={"schema_version":AXIS_ID+".compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","source":{"path":str(inp),"sha256":source_sha},"fixed_contract":{"channels":CHANNELS,"lags":20,"masks":True,"variants":VARIANTS,"splits":{"oof":"2019-2021 expanding min12m next3m","calibration":"2022 Jan-Jun fit; Jul-Sep select; Oct-Dec threshold","frozen":"2023-2025","exploratory":"2026"},"execution":{"n_jobs":2}},"checkpoint_audit":checkpoint_audit,"results":results,"probability_ledger":str(out/"probability_ledger_2026.parquet"),"artifacts":artifacts,"decision":{"candidate_local_decision":"review_results_only","authoritative_rollup_decision":"review_only"},"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False}}
 dump(out/"compare.json",payload);dump(out/"_ARTIFACT_COMPLETE.json",{"complete":True,"compare":str(out/"compare.json")});return out
def main():
 a=argparse.ArgumentParser();a.add_argument("--input",type=Path,required=True);a.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_20bar_morphology_sequence_v1"));x=a.parse_args();print(run(x.input,x.output_root))
if __name__=="__main__":main()
