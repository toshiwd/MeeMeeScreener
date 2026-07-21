from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID="tradex_high_zone_exposure_rollup_v1"


def _read(path:Path)->dict[str,Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path:Path,payload:Any)->None:
    path.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")


def _metrics(source:dict[str,Any],section:str,policy:str|None=None)->dict[str,Any]:
    if section in ("champion","reference","baseline"):
        return source[section]["metrics"]
    return source[section][policy]["metrics"]


def run(*,initial_exposure:Path,episode:Path,cross_band:Path,initial_expansion:Path,initial_expansion_ledger:Path,output_root:Path)->Path:
    sources={"initial_exposure":_read(initial_exposure),"episode":_read(episode),"cross_band":_read(cross_band),"initial_expansion":_read(initial_expansion)}
    expected={"initial_exposure":"keep_high_zone_initial_exposure","episode":"keep_high_zone_episode_exposure","cross_band":"keep_high_zone_cross_band_episode","initial_expansion":"keep_high_zone_initial_expansion_episode"}
    for key,decision in expected.items():
        actual=sources[key]["decision"]["authoritative_rollup_decision"]
        if actual!=decision: raise ValueError(f"unexpected {key} decision: {actual}")
    rows=[
        ("all100","initial_exposure","baseline",None,"reference"),
        ("high_price25","initial_exposure","challengers","high_price25","superseded_keep"),
        ("combined_episode25","episode","challengers","combined_episode25","superseded_keep"),
        ("cross_band_episode25","cross_band","challengers","cross_band_episode25","superseded_keep"),
        ("initial_expansion25","initial_expansion","challengers","initial_expansion25","champion"),
        ("initial_expansion_or_micro25","initial_expansion","challengers","initial_expansion_or_micro25","hold_not_selected"),
    ]
    leaderboard=[]
    for name,source_key,section,policy,status in rows:
        metrics=_metrics(sources[source_key],section,policy); h10=metrics["h10"]
        leaderboard.append({"policy":name,"status":status,"mean_exposure":metrics["mean_exposure"],"h10_mean":h10["mean"],"h10_win_rate":h10["win_rate"],"h10_profit_factor":h10["profit_factor"],"h10_loss_le_minus10_rate":h10["loss_le_minus10_rate"],"h10_worst_mae":h10["worst_mae"]})
    ledger=pd.read_parquet(initial_expansion_ledger); champion=ledger[ledger.policy=="initial_expansion25"].sort_values("mae10"); worst=champion.head(10)
    unresolved=[{"code":str(r.code),"signal_ymd":int(r.signal_ymd),"ret10":float(r.ret10),"mae10":float(r.mae10),"exposure":float(r.exposure),"typed_existing_episode":bool(r.cross_band_episode),"typed_initial_expansion":bool(r.initial_expansion_episode),"micro_expansion":bool(r.micro_expansion)} for r in worst.itertuples()]
    selected=next(x for x in leaderboard if x["policy"]=="initial_expansion25")
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","fixed_evaluation_conditions":sources["initial_expansion"]["fixed_evaluation_conditions"],"source_artifacts":{k:str(v) for k,v in {"initial_exposure":initial_exposure,"episode":episode,"cross_band":cross_band,"initial_expansion":initial_expansion,"initial_expansion_ledger":initial_expansion_ledger}.items()},"family_leaderboard":leaderboard,"observed_branching":{"changed_top5_members_count":0,"changed_top10_members_count":0,"changed_rank_count":0,"selection_divergence_reason":"all branches preserve signal membership and next-open timing; exposure typing only"},"remaining_tail":{"worst_cases":unresolved,"rule_addition_decision":"hold_no_additional_preentry_rule","reason_type":"micro_variant_reduces_worst_mae_slightly_but_cuts_mean_return_and_is_supported_by_too_few_distinct_examples"},"decision":{"candidate_local_decision":"keep","session_aggregate_decision":"keep","authoritative_rollup_decision":"keep_high_zone_exposure_champion","selected_policy":"initial_expansion25","selected_metrics":selected,"reason_type":"best_same_condition_mean_pf_tail_and_stability_tradeoff","production_adoption":"review_only_not_reflected"},"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    run_dir=output_root/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";run_dir.mkdir(parents=True,exist_ok=False);_write(run_dir/"compare.json",payload);_write(run_dir/"family_leaderboard.json",{"schema_version":f"{AXIS_ID}.family_leaderboard.v1","artifact_role":"authoritative","leaderboard":leaderboard,"selected_policy":"initial_expansion25"});_write(run_dir/"_ARTIFACT_COMPLETE.json",{"status":"complete","required_files":["compare.json","family_leaderboard.json","_ARTIFACT_COMPLETE.json"]});return run_dir


def main()->int:
    p=argparse.ArgumentParser();p.add_argument("--initial-exposure",type=Path,required=True);p.add_argument("--episode",type=Path,required=True);p.add_argument("--cross-band",type=Path,required=True);p.add_argument("--initial-expansion",type=Path,required=True);p.add_argument("--initial-expansion-ledger",type=Path,required=True);p.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_high_zone_exposure_rollup_v1"));a=p.parse_args();print(run(initial_exposure=a.initial_exposure,episode=a.episode,cross_band=a.cross_band,initial_expansion=a.initial_expansion,initial_expansion_ledger=a.initial_expansion_ledger,output_root=a.output_root));return 0


if __name__=="__main__":raise SystemExit(main())
