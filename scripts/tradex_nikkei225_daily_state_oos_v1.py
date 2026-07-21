from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_nikkei225_daily_assessment_baseline_v1 import HORIZONS, _labels


AXIS_ID = "tradex_nikkei225_daily_state_oos_v1"
STATE_COLUMNS = ["ma20_structure_state","ma7_sequence_state","support_transition_state","compression_state","lower_rejection_state","stretch_state","pressure_state"]


def _metric(frame, labels: np.ndarray) -> dict[str, Any]:
    n = len(frame)
    if not n: return {"n": 0, "codes": 0, "months": 0}
    return {"n": n, "codes": int(frame.code.nunique()), "months": int(frame.ymd.astype(str).str[:6].nunique()), "downside_rate": float((labels == 0).mean()), "rebound_rate": float((labels == 1).mean()), "neutral_rate": float((labels == 2).mean())}


def run(input_parquet: Path, output_root: Path) -> Path:
    frame = duckdb.connect().execute(f"SELECT * FROM read_parquet('{input_parquet.as_posix()}')").fetchdf()
    results = {}
    candidate_decisions = []
    for horizon in HORIZONS:
        required=[f"ret_close_{horizon}",f"down_exc_{horizon}",f"up_exc_{horizon}"]
        part=frame.dropna(subset=required).copy(); labels=_labels(part,horizon)
        train_mask=(part.ymd>=20240101)&(part.ymd<=20241231); validation_mask=(part.ymd>=20250101)&(part.ymd<=20251231); exploratory_mask=(part.ymd>=20260101)&(part.ymd<=20260713)
        baseline={"train_2024":_metric(part.loc[train_mask],labels[train_mask]),"validation_2025":_metric(part.loc[validation_mask],labels[validation_mask]),"exploratory_2026":_metric(part.loc[exploratory_mask],labels[exploratory_mask])}
        states={}
        for column in STATE_COLUMNS:
            states[column]={}
            for value in sorted(part[column].dropna().unique()):
                masks={"train_2024":train_mask&(part[column]==value),"validation_2025":validation_mask&(part[column]==value),"exploratory_2026":exploratory_mask&(part[column]==value)}
                metrics={name:_metric(part.loc[mask],labels[mask]) for name,mask in masks.items()}
                train,validation=metrics["train_2024"],metrics["validation_2025"]
                train_down=train.get("n",0)>=100 and train.get("downside_rate",0)>=baseline["train_2024"]["downside_rate"]+.05 and train.get("rebound_rate",1)<=baseline["train_2024"]["rebound_rate"]-.03
                train_rebound=train.get("n",0)>=100 and train.get("rebound_rate",0)>=baseline["train_2024"]["rebound_rate"]+.05 and train.get("downside_rate",1)<=baseline["train_2024"]["downside_rate"]-.03
                validation_down=train_down and validation.get("n",0)>=80 and validation.get("downside_rate",0)>=baseline["validation_2025"]["downside_rate"]+.05 and validation.get("rebound_rate",1)<=baseline["validation_2025"]["rebound_rate"]-.03
                validation_rebound=train_rebound and validation.get("n",0)>=80 and validation.get("rebound_rate",0)>=baseline["validation_2025"]["rebound_rate"]+.05 and validation.get("downside_rate",1)<=baseline["validation_2025"]["downside_rate"]-.03
                classification="downside_state_hold" if validation_down else "rebound_risk_state_hold" if validation_rebound else "drop" if train_down or train_rebound else "neutral_context"
                states[column][str(value)]={"metrics":metrics,"train_candidate":"downside" if train_down else "rebound" if train_rebound else None,"classification":classification}
                if classification.endswith("_hold"): candidate_decisions.append({"horizon":horizon,"column":column,"value":str(value),"classification":classification,"metrics":metrics})
        results[str(horizon)]={"baseline":baseline,"states":states}
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");output=output_root/f"{stamp}-{AXIS_ID}";output.mkdir(parents=True,exist_ok=False)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"branching_generation","source_parquet":str(input_parquet),"fixed_conditions":{"changed_axis":"one state value at a time","train":2024,"validation":2025,"exploratory":2026,"minimum_train_n":100,"minimum_validation_n":80,"uplift_gate":.05,"opposite_state_reduction_gate":.03},"results":results,"observed_branching":{"validated_state_count":len(candidate_decisions),"validated_states":candidate_decisions},"decision":{"candidate_local_decision":"hold_validated_states_for_combination" if candidate_decisions else "drop_all_single_states","authoritative_rollup_decision":"review_only"},"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False},"remaining_risks":["single-state tests are not action rules","multiple comparisons are diagnostic","clean future shadow is absent"]}
    (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"compare":str(output/"compare.json")},indent=2)+"\n",encoding="utf-8");return output


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--input-parquet",required=True,type=Path);parser.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_daily_state_oos_v1"));args=parser.parse_args();print(run(args.input_parquet,args.output_root))


if __name__=="__main__":main()
