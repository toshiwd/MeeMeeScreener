from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_nikkei225_daily_assessment_baseline_v1 import CLASS_NAMES, FEATURES, HORIZONS, _labels, _model
from scripts.tradex_nikkei225_daily_assessment_walkforward_v1 import _threshold


AXIS_ID="tradex_nikkei225_current_daily_assessment_v1"


def run(feature_parquet:Path,state_parquet:Path,output_root:Path)->Path:
    frame=duckdb.connect().execute(f"SELECT * FROM read_parquet('{feature_parquet.as_posix()}') ORDER BY ymd,code").fetchdf()
    states=duckdb.connect().execute(f"SELECT code,ymd,ma20_structure_state,ma7_sequence_state,support_transition_state,compression_state,lower_rejection_state,stretch_state,pressure_state FROM read_parquet('{state_parquet.as_posix()}')").fetchdf()
    latest_ymd=int(frame.ymd.max());latest=frame[frame.ymd==latest_ymd].copy().reset_index(drop=True);latest=latest.merge(states[states.ymd==latest_ymd],on=["code","ymd"],how="left")
    rows=[];horizon_audits={}
    for horizon in HORIZONS:
        required=[f"ret_close_{horizon}",f"down_exc_{horizon}",f"up_exc_{horizon}","atr14","c"]
        history=frame.dropna(subset=required).copy();dates=np.array(sorted(history.ymd.unique()))[-504:];cal_dates=dates[-60:];fit_dates=dates[:-60]
        fit=history[history.ymd.isin(fit_dates)].copy();cal=history[history.ymd.isin(cal_dates)].copy();fit_y=_labels(fit,horizon);cal_y=_labels(cal,horizon)
        model=_model();model.fit(fit[FEATURES],fit_y);cal_p=model.predict_proba(cal[FEATURES]);threshold,calibration=_threshold(cal_y,cal_p)
        final=history[history.ymd.isin(dates)].copy();final_y=_labels(final,horizon);model=_model();model.fit(final[FEATURES],final_y);prob=model.predict_proba(latest[FEATURES])
        horizon_audits[str(horizon)]={"history_start":int(dates[0]),"history_end":int(dates[-1]),"fit_rows":len(fit),"calibration_rows":len(cal),"threshold":None if threshold is None else {"p_down_min":threshold[0],"p_rebound_max":threshold[1]},"calibration_gate":calibration}
        for index,row in latest.iterrows():
            action=False if threshold is None else bool(prob[index,0]>=threshold[0] and prob[index,1]<=threshold[1])
            evidence=[f"ma20:{row.ma20_structure_state}",f"ma7:{row.ma7_sequence_state}",f"support:{row.support_transition_state}",f"compression:{row.compression_state}",f"rejection:{row.lower_rejection_state}",f"stretch:{row.stretch_state}",f"pressure:{row.pressure_state}"]
            rows.append({"code":row.code,"reference_ymd":latest_ymd,"horizon":horizon,"p_down":prob[index,0],"p_rebound":prob[index,1],"p_neutral":prob[index,2],"diagnostic_state":CLASS_NAMES[int(np.argmax(prob[index]))],"assessment_state":"short_review" if action else "no_short_action" if threshold is not None else "unjudgeable_model_quality","threshold_available":threshold is not None,"evidence":"|".join(evidence)})
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");output=output_root/f"{stamp}-{AXIS_ID}";output.mkdir(parents=True,exist_ok=False);csv_path=output/"current_daily_assessment.csv";pd.DataFrame(rows).to_csv(csv_path,index=False,encoding="utf-8-sig")
    payload={"schema_version":f"{AXIS_ID}.audit.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"reference_ymd":latest_ymd,"feature_source":str(feature_parquet),"state_source":str(state_parquet),"rows":len(rows),"codes":int(latest.code.nunique()),"horizon_audits":horizon_audits,"assessment_counts":pd.DataFrame(rows).groupby(["horizon","assessment_state"]).size().to_dict(),"output_csv":str(csv_path),"status":"review_only","boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False}}
    # JSON object keys cannot be tuples.
    payload["assessment_counts"]=[{"horizon":int(h),"state":state,"n":int(n)} for (h,state),n in pd.DataFrame(rows).groupby(["horizon","assessment_state"]).size().items()]
    (output/"audit.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"audit":str(output/"audit.json"),"csv":str(csv_path)},indent=2)+"\n",encoding="utf-8");return output


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--feature-parquet",required=True,type=Path);parser.add_argument("--state-parquet",required=True,type=Path);parser.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_current_daily_assessment_v1"));args=parser.parse_args();print(run(args.feature_parquet,args.state_parquet,args.output_root))


if __name__=="__main__":main()
