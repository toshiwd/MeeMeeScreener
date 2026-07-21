from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_nikkei225_daily_assessment_baseline_v1 import CLASS_NAMES, FEATURES, HORIZONS, _labels, _model


AXIS_ID = "tradex_nikkei225_daily_assessment_walkforward_v1"


def _threshold(labels: np.ndarray, probabilities: np.ndarray, minimum: int = 30) -> tuple[tuple[float, float] | None, dict[str, Any]]:
    candidates=[]
    for down in np.arange(.20,.81,.025):
        for rebound in np.arange(.10,.61,.025):
            mask=(probabilities[:,0]>=down)&(probabilities[:,1]<=rebound); n=int(mask.sum())
            if n<minimum: continue
            coverage=n/len(labels); precision=float((labels[mask]==0).mean()); false=float((labels[mask]==1).mean())
            if .01<=coverage<=.30 and precision>=.60 and false<=.20:
                candidates.append((coverage,(float(down),float(rebound)),{"n":n,"coverage":coverage,"precision":precision,"false_action":false}))
    if not candidates:return None,{"gate_passed":False,"reason":"no_threshold_meets_calibration_action_gate"}
    _,threshold,metric=max(candidates);return threshold,{"gate_passed":True,**metric}


def _action_metric(frame:pd.DataFrame,labels:np.ndarray,probabilities:np.ndarray,threshold:tuple[float,float]|None,horizon:int)->dict[str,Any]:
    if threshold is None:return {"eligible_n":len(frame),"action_n":0,"coverage":0.0,"precision":None,"false_action":None,"mean_return":None,"codes":0,"months":0}
    mask=(probabilities[:,0]>=threshold[0])&(probabilities[:,1]<=threshold[1]);selected=frame.loc[mask];n=int(mask.sum())
    return {"eligible_n":len(frame),"action_n":n,"coverage":n/len(frame),"precision":float((labels[mask]==0).mean()) if n else None,"false_action":float((labels[mask]==1).mean()) if n else None,"mean_return":float(selected[f"ret_close_{horizon}"].mean()) if n else None,"codes":int(selected.code.nunique()) if n else 0,"months":int(selected.ymd.astype(str).str[:6].nunique()) if n else 0}


def _prepare(frame:pd.DataFrame,horizon:int)->tuple[pd.DataFrame,np.ndarray]:
    part=frame.dropna(subset=[f"ret_close_{horizon}",f"down_exc_{horizon}",f"up_exc_{horizon}","atr14","c"]).copy();return part,_labels(part,horizon)


def run(input_parquet:Path,output_root:Path,evaluation_start:int,evaluation_end:int,horizons:list[int])->Path:
    frame=duckdb.connect().execute(f"SELECT * FROM read_parquet('{input_parquet.as_posix()}') ORDER BY ymd,code").fetchdf()
    validation=frame[(frame.ymd>=evaluation_start)&(frame.ymd<=evaluation_end)].copy();months=sorted(validation.ymd.astype(str).str[:6].unique())
    results={};assessment_rows=[]
    for horizon in horizons:
        monthly=[];all_eval=[];all_labels=[];all_prob=[];all_action=[]
        for month in months:
            test=validation[validation.ymd.astype(str).str[:6]==month].copy();first=int(test.ymd.min())
            eligible_history=frame[frame.ymd<first].copy();dates=np.array(sorted(eligible_history.ymd.unique()))[-504:]
            if len(dates)<300:
                monthly.append({"month":month,"status":"blocked_insufficient_history"});continue
            calibration_dates=dates[-60:];fit_dates=dates[:-60]
            fit,fit_y=_prepare(eligible_history[eligible_history.ymd.isin(fit_dates)],horizon);cal,cal_y=_prepare(eligible_history[eligible_history.ymd.isin(calibration_dates)],horizon);test_eval,test_y=_prepare(test,horizon)
            if len(set(fit_y))<3 or len(cal_y)<100:
                monthly.append({"month":month,"status":"blocked_class_or_calibration_sample"});continue
            model=_model();model.fit(fit[FEATURES],fit_y);cal_p=model.predict_proba(cal[FEATURES]);threshold,cal_metric=_threshold(cal_y,cal_p)
            final_fit,final_y=_prepare(eligible_history[eligible_history.ymd.isin(dates)],horizon);model=_model();model.fit(final_fit[FEATURES],final_y);test_p=model.predict_proba(test_eval[FEATURES]);metric=_action_metric(test_eval,test_y,test_p,threshold,horizon)
            passed_threshold=threshold is not None
            monthly.append({"month":month,"status":"assessed" if passed_threshold else "blocked_no_calibrated_threshold","threshold":None if threshold is None else {"p_down_min":threshold[0],"p_rebound_max":threshold[1]},"calibration":cal_metric,"oos":metric})
            all_eval.append(test_eval);all_labels.append(test_y);all_prob.append(test_p);all_action.append(np.zeros(len(test_eval),dtype=bool) if threshold is None else (test_p[:,0]>=threshold[0])&(test_p[:,1]<=threshold[1]))
            for idx,row in test_eval.reset_index(drop=True).iterrows():
                action=False if threshold is None else bool(test_p[idx,0]>=threshold[0] and test_p[idx,1]<=threshold[1])
                assessment_rows.append({"code":row.code,"ymd":int(row.ymd),"horizon":horizon,"p_down":test_p[idx,0],"p_rebound":test_p[idx,1],"p_neutral":test_p[idx,2],"diagnostic_state":CLASS_NAMES[int(np.argmax(test_p[idx]))],"assessment_state":"short_review" if action else "no_short_action" if threshold is not None else "unjudgeable_model_quality","threshold_available":threshold is not None})
        eval_frame=pd.concat(all_eval,ignore_index=True);labels=np.concatenate(all_labels);prob=np.vstack(all_prob);action=np.concatenate(all_action);n=int(action.sum())
        aggregate={"eligible_n":len(labels),"action_n":n,"coverage":n/len(labels),"precision":float((labels[action]==0).mean()) if n else None,"false_action":float((labels[action]==1).mean()) if n else None,"mean_return":float(eval_frame.loc[action,f"ret_close_{horizon}"].mean()) if n else None,"action_codes":int(eval_frame.loc[action,"code"].nunique()) if n else 0,"assessed_months":sum(item.get("status")=="assessed" for item in monthly),"blocked_months":sum(item.get("status")!="assessed" for item in monthly)}
        gate=HORIZONS[horizon];checks={"months":aggregate["assessed_months"]>=8,"sample":n>=gate["n"] and aggregate["action_codes"]>=gate["codes"],"coverage":.03<=aggregate["coverage"]<=.30,"precision":aggregate["precision"] is not None and aggregate["precision"]>=gate["precision"],"false_action":aggregate["false_action"] is not None and aggregate["false_action"]<=gate["false"],"mean_return":aggregate["mean_return"] is not None and aggregate["mean_return"]<=gate["mean"]}
        results[str(horizon)]={"monthly":monthly,"aggregate_2025":aggregate,"gate_audit":checks,"decision":"hold_pending_clean_shadow" if all(checks.values()) else "drop_walkforward"}
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");output=output_root/f"{stamp}-{AXIS_ID}";output.mkdir(parents=True,exist_ok=False);pd.DataFrame(assessment_rows).to_csv(output/"walkforward_2025_assessment.csv",index=False,encoding="utf-8-sig")
    spec=json.dumps({"features":FEATURES,"history_days":504,"calibration_days":60,"threshold_gate":{"precision":.60,"false":.20}},sort_keys=True)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"comparison_stabilization","source_parquet":str(input_parquet),"evaluation_period":[evaluation_start,evaluation_end],"candidate_spec_hash":hashlib.sha256(spec.encode()).hexdigest(),"point_in_time_contract":"each month uses only dates before month start; threshold calibrated on prior 60 dates","results":results,"assessment_csv":str(output/"walkforward_2025_assessment.csv"),"decision":{"candidate_local_decision":"walkforward_hold" if any(v["decision"].startswith("hold") for v in results.values()) else "walkforward_drop_all_horizons","authoritative_rollup_decision":"review_only"},"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False},"remaining_risks":["probability calibration metrics and block-bootstrap CI remain required","2026 remains exploratory"]}
    (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"compare":str(output/"compare.json")},indent=2)+"\n",encoding="utf-8");return output


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--input-parquet",required=True,type=Path);parser.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_walkforward_v1"));parser.add_argument("--evaluation-start",type=int,default=20250101);parser.add_argument("--evaluation-end",type=int,default=20251231);parser.add_argument("--horizons",default="1,3,5,10");args=parser.parse_args();print(run(args.input_parquet,args.output_root,args.evaluation_start,args.evaluation_end,[int(x) for x in args.horizons.split(',')]))


if __name__=="__main__":main()
