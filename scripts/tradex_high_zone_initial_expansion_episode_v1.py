from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.tradex_high_zone_cross_band_episode_v1 import HORIZONS, annotate, load_events
    from scripts.tradex_high_zone_initial_exposure_v1 import metrics, price_band, stability
except ModuleNotFoundError:
    from tradex_high_zone_cross_band_episode_v1 import HORIZONS, annotate, load_events
    from tradex_high_zone_initial_exposure_v1 import metrics, price_band, stability


AXIS_ID="tradex_high_zone_initial_expansion_episode_v1"
FAMILY="high_zone_climax"
POLICIES=("cross_band_episode25_champion","initial_expansion25","initial_expansion_or_micro25")


def _value(row:pd.Series,name:str)->float|None:
    value=row.get(name); return None if value is None or pd.isna(value) else float(value)


def annotate_expansion(events:pd.DataFrame)->pd.DataFrame:
    result=annotate(events)
    result["signal_body"]=result.c/result.o-1.0
    result["signal_range"]=(result.h-result.l)/result.c
    result["expansion_score"]=(result.dist_ma20>=1.0).astype(int)+(result.signal_body>=0.20).astype(int)+(result.gap1>=0.10).astype(int)+(result.ret20>=1.80).astype(int)
    result["initial_expansion_episode"]=(result.c<1500)&~result.cross_band_episode&(result.expansion_score>=2)
    result["micro_expansion"]=(result.c<300)&~result.cross_band_episode&(result.signal_body>=0.09)&(result.signal_range>=0.14)
    return result


def exposure(row:pd.Series,policy:str)->float:
    if float(row["c"])>=10000 or bool(row["cross_band_episode"]): return 0.25
    if policy=="cross_band_episode25_champion": return 1.0
    if policy=="initial_expansion25": return 0.25 if bool(row["initial_expansion_episode"]) else 1.0
    return 0.25 if bool(row["initial_expansion_episode"]) or bool(row["micro_expansion"]) else 1.0


def replay(events:pd.DataFrame)->pd.DataFrame:
    rows=[]
    for _,source in annotate_expansion(events).iterrows():
        open1=_value(source,"o1")
        for policy in POLICIES:
            weight=exposure(source,policy)
            record={"family":FAMILY,"code":str(source["code"]),"signal_ymd":int(source["signal_ymd"]),"signal_close":float(source["c"]),"price_band":price_band(float(source["c"])),"continuation_risk_high":bool(source["cross_band_episode"] or source["initial_expansion_episode"]),"cross_band_episode":bool(source["cross_band_episode"]),"expansion_score":int(source["expansion_score"]),"initial_expansion_episode":bool(source["initial_expansion_episode"]),"micro_expansion":bool(source["micro_expansion"]),"policy":policy,"state":"entry" if open1 else "unavailable","exposure":weight,"entry_offset":1 if open1 else None,"entry_price":open1,"wait_days":1 if open1 else None}
            for horizon in HORIZONS:
                exit_close=_value(source,f"c{1+horizon}"); highs=[_value(source,f"h{i}") for i in range(1,2+horizon)]; highs=[x for x in highs if x is not None]
                record[f"ret{horizon}"]=None if open1 is None or exit_close is None else weight*(1-exit_close/open1)
                record[f"mae{horizon}"]=None if open1 is None or not highs else weight*(1-max(highs)/open1)
            rows.append(record)
    return pd.DataFrame(rows)


def _json_ready(value:Any)->Any:
    if isinstance(value,dict): return {str(k):_json_ready(v) for k,v in value.items()}
    if isinstance(value,list): return [_json_ready(v) for v in value]
    if isinstance(value,Path): return str(value)
    if hasattr(value,"item"): return _json_ready(value.item())
    if isinstance(value,float) and (math.isnan(value) or math.isinf(value)): return None
    return value


def run(db_path:Path,output_root:Path,start_ymd:int,end_ymd:int)->Path:
    events=load_events(db_path,start_ymd,end_ymd); events=events[events.family==FAMILY].copy(); ledger=replay(events); results={p:metrics(ledger[ledger.policy==p]) for p in POLICIES}; stable={p:stability(ledger[ledger.policy==p]) for p in POLICIES}; champion=results[POLICIES[0]]
    challengers={}
    for policy in POLICIES[1:]:
        item=results[policy]; checks={"participation_capture_100pct":item["participation_capture_rate"]==1.0,"mean_ret10_not_worse_than_champion":item["h10"]["mean"]>=champion["h10"]["mean"],"pf10_not_worse_than_champion":(item["h10"]["profit_factor"] or 0)>=(champion["h10"]["profit_factor"] or 0),"loss10_rate_not_worse_than_champion":item["h10"]["loss_le_minus10_rate"]<=champion["h10"]["loss_le_minus10_rate"],"worst_mae_not_worse_than_champion":item["h10"]["worst_mae"]>=champion["h10"]["worst_mae"],"positive_year_rate_at_least_75pct":stable[policy]["positive_mean_ret10_year_rate"]>=0.75,"positive_price_band_rate_at_least_75pct":stable[policy]["positive_mean_ret10_price_band_rate"]>=0.75}; decision="keep" if all(checks.values()) else ("hold" if checks["pf10_not_worse_than_champion"] and checks["loss10_rate_not_worse_than_champion"] else "drop"); challengers[policy]={"candidate_local_decision":decision,"checks":checks,"metrics":item,"stability":stable[policy]}
    keepers=[p for p,x in challengers.items() if x["candidate_local_decision"]=="keep"]; holds=[p for p,x in challengers.items() if x["candidate_local_decision"]=="hold"]; pool=keepers or holds; leader=max(pool,key=lambda p:results[p]["h10"]["mean"]) if pool else None; decision="keep" if keepers else ("hold" if holds else "drop"); expansion=ledger[(ledger.policy==POLICIES[0])&ledger.initial_expansion_episode]
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"universe":"same high_zone_climax top5/day signals","period":{"start_ymd":start_ymd,"end_ymd":end_ymd},"changed_axis":"initial-expansion episode exposure only","episode_definition":"under1500, outside existing cross-band episode, at least two of MA20 distance>=100pct, signal body>=20pct, next-open GU>=10pct, ret20>=180pct","micro_variant":"under300, outside existing episode, signal body>=9pct and range>=14pct","horizons":list(HORIZONS),"costs":"ignored_by_user_request","runtime_db_write":False,"meemee_reflection":False},"source":{"db_path":str(db_path),"event_count":int(len(events)),"ledger_count":int(len(ledger)),"initial_expansion_episode_count":int(len(expansion)),"initial_expansion_tail_loss_count_at_champion_exposure":int((expansion.ret10<=-.10).sum())},"champion":{"policy":POLICIES[0],"metrics":champion,"stability":stable[POLICIES[0]]},"challengers":challengers,"observed_branching":{"changed_top5_members_count":0,"changed_top10_members_count":0,"changed_rank_count":0,"selection_divergence_reason":"membership and timing fixed; only typed initial-expansion exposure changes"},"decision":{"candidate_local_decision":decision,"session_aggregate_decision":decision,"authoritative_rollup_decision":f"{decision}_high_zone_initial_expansion_episode","selected_policy":leader if decision=="keep" else None,"research_leader":leader,"reason_type":"initial_expansion_episode_beats_champion" if decision=="keep" else ("tail_improves_but_expectancy_gate_fails" if decision=="hold" else "initial_expansion_episode_does_not_improve_champion")},"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    run_dir=output_root/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";run_dir.mkdir(parents=True,exist_ok=False);ledger.to_parquet(run_dir/"initial_expansion_ledger.parquet",index=False);(run_dir/"compare.json").write_text(json.dumps(_json_ready(payload),ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(run_dir/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"status":"complete","required_files":["compare.json","initial_expansion_ledger.parquet","_ARTIFACT_COMPLETE.json"]},indent=2)+"\n",encoding="utf-8");return run_dir


def main()->int:
    p=argparse.ArgumentParser();p.add_argument("--db-path",type=Path,required=True);p.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_high_zone_initial_expansion_episode_v1"));p.add_argument("--start-ymd",type=int,default=20160101);p.add_argument("--end-ymd",type=int,default=20260617);a=p.parse_args();print(run(a.db_path,a.output_root,a.start_ymd,a.end_ymd));return 0


if __name__=="__main__":raise SystemExit(main())
