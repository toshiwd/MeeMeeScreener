from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from scripts.tradex_high_zone_initial_exposure_v1 import metrics
except ModuleNotFoundError:
    from tradex_high_zone_initial_exposure_v1 import metrics


AXIS_ID="tradex_high_zone_temporal_validation_v1"
ERAS=(("2016_2021",2016,2021),("2022_2024",2022,2024),("2025_2026",2025,2026))


def _full_exposure(frame:pd.DataFrame)->pd.DataFrame:
    result=frame.copy()
    for horizon in (5,10,20):
        result[f"ret{horizon}"]=result[f"ret{horizon}"]/result.exposure
        result[f"mae{horizon}"]=result[f"mae{horizon}"]/result.exposure
    result["exposure"]=1.0
    return result


def paired_month_bootstrap(champion:pd.DataFrame,full:pd.DataFrame,iterations:int=2000,seed:int=20260717)->dict[str,Any]:
    paired=champion[["code","signal_ymd","ret10"]].merge(full[["code","signal_ymd","ret10"]],on=["code","signal_ymd"],suffixes=("_champion","_full"))
    paired["month"]=paired.signal_ymd.astype(str).str[:6]
    monthly=[group.ret10_champion.to_numpy()-group.ret10_full.to_numpy() for _,group in paired.groupby("month")]
    rng=np.random.default_rng(seed); draws=[]
    for _ in range(iterations):
        sampled=rng.integers(0,len(monthly),len(monthly)); values=np.concatenate([monthly[i] for i in sampled]); draws.append(float(values.mean()))
    series=pd.Series(draws)
    return {"iterations":iterations,"seed":seed,"observed_mean_difference":float((paired.ret10_champion-paired.ret10_full).mean()),"probability_champion_mean_better":float((series>0).mean()),"mean_difference_p05":float(series.quantile(.05)),"mean_difference_p50":float(series.quantile(.50)),"mean_difference_p95":float(series.quantile(.95))}


def validate(ledger:pd.DataFrame)->dict[str,Any]:
    champion=ledger[ledger.policy=="initial_expansion25"].copy(); champion["year"]=champion.signal_ymd.astype(str).str[:4].astype(int)
    full=_full_exposure(champion); full["year"]=champion.year
    eras=[]
    for name,start,end in ERAS:
        c=champion[champion.year.between(start,end)]; f=full[full.year.between(start,end)]; cm,fm=metrics(c),metrics(f)
        eras.append({"era":name,"signal_count":len(c),"champion":cm,"full100":fm,"mean_ret10_difference":cm["h10"]["mean"]-fm["h10"]["mean"],"champion_mean_positive":cm["h10"]["mean"]>0,"champion_pf_at_least_1_2":(cm["h10"]["profit_factor"] or 0)>=1.2,"champion_mean_not_worse":cm["h10"]["mean"]>=fm["h10"]["mean"]})
    years=sorted(champion.year.unique()); leave_one_out=[]
    for year in years:
        c=champion[champion.year!=year]; f=full[full.year!=year]; cm,fm=metrics(c),metrics(f)
        leave_one_out.append({"excluded_year":int(year),"signal_count":len(c),"champion_mean_ret10":cm["h10"]["mean"],"champion_pf10":cm["h10"]["profit_factor"],"mean_ret10_difference":cm["h10"]["mean"]-fm["h10"]["mean"],"champion_mean_positive":cm["h10"]["mean"]>0})
    bootstrap=paired_month_bootstrap(champion,full)
    checks={"all_eras_positive_mean":all(x["champion_mean_positive"] for x in eras),"all_eras_pf_at_least_1_2":all(x["champion_pf_at_least_1_2"] for x in eras),"champion_not_worse_in_at_least_two_eras":sum(x["champion_mean_not_worse"] for x in eras)>=2,"all_leave_one_year_out_positive":all(x["champion_mean_positive"] for x in leave_one_out),"bootstrap_probability_better_at_least_75pct":bootstrap["probability_champion_mean_better"]>=.75}
    decision="keep" if all(checks.values()) else ("hold" if checks["all_eras_positive_mean"] and checks["all_leave_one_year_out_positive"] else "drop")
    return {"eras":eras,"leave_one_year_out":leave_one_out,"paired_month_bootstrap":bootstrap,"checks":checks,"decision":decision}


def run(ledger_path:Path,champion_compare:Path,output_root:Path)->Path:
    ledger=pd.read_parquet(ledger_path); validation=validate(ledger); source=json.loads(champion_compare.read_text(encoding="utf-8")); decision=validation.pop("decision")
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"champion_policy":"initial_expansion25","rule_changes":False,"signal_membership_changes":False,"entry_timing_changes":False,"period":"20160101_20260617","eras":[x[0] for x in ERAS],"bootstrap_unit":"calendar_month","bootstrap_iterations":2000},"source_artifacts":{"champion_compare":str(champion_compare),"champion_ledger":str(ledger_path),"champion_rollup_decision":source["decision"]["authoritative_rollup_decision"]},**validation,"observed_branching":{"changed_top5_members_count":0,"changed_top10_members_count":0,"changed_rank_count":0,"selection_divergence_reason":"validation only; champion frozen"},"decision":{"candidate_local_decision":decision,"session_aggregate_decision":decision,"authoritative_rollup_decision":f"{decision}_high_zone_temporal_validation","selected_policy":"initial_expansion25" if decision=="keep" else None,"reason_type":"frozen_champion_passes_temporal_robustness" if decision=="keep" else ("positive_absolute_edge_but_relative_stability_incomplete" if decision=="hold" else "frozen_champion_fails_temporal_robustness")},"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    run_dir=output_root/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";run_dir.mkdir(parents=True,exist_ok=False);(run_dir/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(run_dir/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"status":"complete","required_files":["compare.json","_ARTIFACT_COMPLETE.json"]},indent=2)+"\n",encoding="utf-8");return run_dir


def main()->int:
    p=argparse.ArgumentParser();p.add_argument("--ledger",type=Path,required=True);p.add_argument("--champion-compare",type=Path,required=True);p.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_high_zone_temporal_validation_v1"));a=p.parse_args();print(run(a.ledger,a.champion_compare,a.output_root));return 0


if __name__=="__main__":raise SystemExit(main())
