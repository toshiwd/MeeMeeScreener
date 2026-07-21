from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.tradex_nikkei225_daily_assessment_baseline_v1 import FEATURES,_ece,_labels,_model


AXIS_ID="tradex_nikkei225_h1_probability_validation_v1"
BASE_FEATURES=[feature for feature in FEATURES if not feature.startswith("market_")]


def run(input_parquet:Path,output_root:Path)->Path:
    frame=duckdb.connect().execute(f"SELECT * FROM read_parquet('{input_parquet.as_posix()}')").fetchdf();required=["ret_close_1","down_exc_1","up_exc_1","atr14","c"]
    train=frame[(frame.ymd>=20240101)&(frame.ymd<=20241231)].dropna(subset=required).copy();validation=frame[(frame.ymd>=20250101)&(frame.ymd<=20251231)].dropna(subset=required).copy();exploratory=frame[(frame.ymd>=20260101)&(frame.ymd<=20260713)].dropna(subset=required).copy()
    train_y=_labels(train,1);validation_y=_labels(validation,1);exploratory_y=_labels(exploratory,1);model=_model();model.fit(train[BASE_FEATURES],train_y);validation_p=model.predict_proba(validation[BASE_FEATURES]);exploratory_p=model.predict_proba(exploratory[BASE_FEATURES])
    prevalence=np.bincount(train_y,minlength=3)/len(train_y);onehot=np.eye(3)[validation_y];row_model=np.sum((validation_p-onehot)**2,axis=1);row_constant=np.sum((prevalence-onehot)**2,axis=1);difference=row_model-row_constant
    by_code={code:np.flatnonzero(validation.code.to_numpy()==code) for code in validation.code.unique()};codes=sorted(by_code);rng=random.Random(20260714);boot=[]
    for _ in range(2000):
        indices=np.concatenate([by_code[code] for code in rng.choices(codes,k=len(codes))]);boot.append(float(difference[indices].mean()))
    boot.sort();brier=float(row_model.mean());constant=float(row_constant.mean());ll=float(log_loss(validation_y,validation_p,labels=[0,1,2]));ll_constant=float(log_loss(validation_y,np.tile(prevalence,(len(validation_y),1)),labels=[0,1,2]));accuracy=float((validation_p.argmax(axis=1)==validation_y).mean());majority=float((validation_y==int(prevalence.argmax())).mean())
    gates={"brier_skill_positive":brier<constant,"brier_skill_ci":boot[1949]<0,"log_loss_better":ll<ll_constant,"ece_down":_ece(validation_y,validation_p[:,0],0)<=.05,"ece_rebound":_ece(validation_y,validation_p[:,1],1)<=.05,"accuracy_better_majority":accuracy>majority}
    decision="hold_probability_surface_pending_clean_shadow" if all(gates.values()) else "drop_probability_surface"
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");output=output_root/f"{stamp}-{AXIS_ID}";output.mkdir(parents=True,exist_ok=False)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","source_parquet":str(input_parquet),"fixed_conditions":{"horizon":1,"train":2024,"validation":2025,"exploratory":2026,"features":"base chart features without market context","constant_benchmark":"train prevalence","bootstrap":"2000 code-cluster resamples"},"validation_2025":{"n":len(validation),"brier":brier,"brier_constant":constant,"brier_difference":brier-constant,"brier_difference_bootstrap95":{"low":boot[49],"high":boot[1949]},"log_loss":ll,"log_loss_constant":ll_constant,"ece_down":_ece(validation_y,validation_p[:,0],0),"ece_rebound":_ece(validation_y,validation_p[:,1],1),"accuracy":accuracy,"majority_accuracy":majority},"exploratory_2026":{"n":len(exploratory_y),"brier":float(np.mean(np.sum((exploratory_p-np.eye(3)[exploratory_y])**2,axis=1)))},"gate_audit":gates,"decision":{"candidate_local_decision":decision,"authoritative_rollup_decision":"review_only"},"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False},"remaining_risks":["2026 is contaminated exploratory","future clean shadow required for keep"]}
    (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"compare":str(output/"compare.json")},indent=2)+"\n",encoding="utf-8");return output


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--input-parquet",required=True,type=Path);parser.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_h1_probability_validation_v1"));args=parser.parse_args();print(run(args.input_parquet,args.output_root))


if __name__=="__main__":main()
