from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from app.backend.services.tradex_research_contracts import build_run_manifest


AXIS_ID = "tradex_phase5_ma_interaction_miner_v1"
DEFAULT_SOURCE = Path(r"G:\Tradex\ma_phase_feature_base_v1\20260603T121215Z-ma-phase-feature-base-v1\ma_phase_features.parquet")
DEFAULT_OUTPUT = Path(r"G:\Tradex\phase5_ma_interaction_miner_v1")
TRAIN = (20200101, 20221231)
VALIDATION = (20230101, 20241231)
TEST = (20250101, 20261231)
CANDIDATES = (
    {"candidate_id": "short_below_ma20_run_x_upper_ma_count_3pct", "side": "short", "cell": "coalesce(cast(below_ma20_run_bucket as varchar),'null') || '|' || coalesce(cast(upper_ma_count_within_3pct as varchar),'null')", "eligible": "below_ma20_run_bucket IS NOT NULL AND upper_ma_count_within_3pct IS NOT NULL"},
    {"candidate_id": "short_stack_transition_x_upper_shadow", "side": "short", "cell": "coalesce(prev_stack,'null') || '->' || coalesce(cast(ma_stack_state as varchar),'null') || '|' || cast(is_upper_shadow_long as varchar)", "eligible": "prev_stack IS NOT NULL AND is_upper_shadow_long=true AND stack_gap_days<=7"},
    {"candidate_id": "long_bars_since_cross_ma20_x_lower_support", "side": "long", "cell": "cross_bucket || '|' || coalesce(cast(lower_support_bucket as varchar),'null')", "eligible": "lower_support_bucket IS NOT NULL"},
)


def _sha(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""):h.update(chunk)
    return h.hexdigest()


def _pf(gain: float | None, loss: float | None) -> float | None:
    return float(gain)/float(loss) if loss and float(loss)>0 else None


def _query(source: Path) -> list[dict[str, Any]]:
    parts=[]
    for c in CANDIDATES:
        signed="ret_20b" if c["side"]=="long" else "-ret_20b"
        parts.append(f"SELECT '{c['candidate_id']}' candidate_id,'{c['side']}' side,code,ymd,{c['cell']} cell_id,{signed} signed_ret FROM prepared WHERE ymd BETWEEN {TRAIN[0]} AND {VALIDATION[1]} AND ret_20b IS NOT NULL AND ({c['eligible']})")
    sql=" UNION ALL ".join(parts)
    con=duckdb.connect()
    try:
        con.execute(f"CREATE TEMP VIEW source_rows AS SELECT * FROM read_parquet('{source.as_posix()}')")
        rows=con.execute(f"""
          WITH sequenced AS (
            SELECT *,lag(ma_stack_state) OVER(PARTITION BY code ORDER BY ymd) prev_stack,
              date_diff('day',strptime(cast(lag(ymd) OVER(PARTITION BY code ORDER BY ymd) as varchar),'%Y%m%d'),strptime(cast(ymd as varchar),'%Y%m%d')) stack_gap_days
            FROM source_rows WHERE ymd<={VALIDATION[1]}
          ), prepared AS (
            SELECT *,CASE WHEN bars_since_cross_above_ma20 IS NULL THEN 'null' WHEN bars_since_cross_above_ma20=0 THEN '0' WHEN bars_since_cross_above_ma20<=2 THEN '1-2' WHEN bars_since_cross_above_ma20<=5 THEN '3-5' WHEN bars_since_cross_above_ma20<=10 THEN '6-10' WHEN bars_since_cross_above_ma20<=20 THEN '11-20' ELSE '21+' END cross_bucket FROM sequenced
          ), matched AS ({sql}), labeled AS (
            SELECT *,CASE WHEN ymd BETWEEN {TRAIN[0]} AND {TRAIN[1]} THEN 'train' ELSE 'validation' END split,
                   CAST(ymd/10000 AS INTEGER) year_bucket
            FROM matched
          )
          SELECT candidate_id,side,cell_id,split,count(*) n,count(DISTINCT code) unique_codes,
                 avg(signed_ret) expectancy,
                 sum(CASE WHEN signed_ret>0 THEN signed_ret ELSE 0 END) gain,
                 -sum(CASE WHEN signed_ret<0 THEN signed_ret ELSE 0 END) loss,
                 max(year_n)*1.0/count(*) max_year_share
          FROM (SELECT *,count(*) OVER(PARTITION BY candidate_id,cell_id,split,year_bucket) year_n FROM labeled)
          GROUP BY candidate_id,side,cell_id,split ORDER BY candidate_id,cell_id,split
        """).fetchall()
        cols=[x[0] for x in con.description]
        return [dict(zip(cols,row)) for row in rows]
    finally:con.close()


def _gate(row: dict[str, Any] | None, *, min_n:int,min_codes:int,max_year_share:float) -> tuple[bool,list[str]]:
    if not row:return False,["missing_split_rows"]
    reasons=[]
    if int(row["n"])<min_n:reasons.append("n_below_gate")
    if int(row["unique_codes"])<min_codes:reasons.append("unique_codes_below_gate")
    if float(row["max_year_share"])>max_year_share:reasons.append("year_concentration_above_gate")
    pf=_pf(row["gain"],row["loss"])
    if float(row["expectancy"])<=0:reasons.append("expectancy_not_positive")
    if pf is None or pf<=1.0:reasons.append("profit_factor_not_above_one")
    return not reasons,reasons


def build(source:Path,*,min_n:int=30,min_codes:int=10,max_year_share:float=.60)->tuple[dict[str,Any],dict[str,Any]]:
    stats=_query(source);reports=[];unlocked=[]
    for c in CANDIDATES:
        train_cells=[]
        for x in stats:
            if x["candidate_id"]==c["candidate_id"] and x["split"]=="train":
                ok,reasons=_gate(x,min_n=min_n,min_codes=min_codes,max_year_share=max_year_share)
                if ok:train_cells.append((_pf(x["gain"],x["loss"]) or 0,float(x["expectancy"]),str(x["cell_id"]),x))
        train_cells.sort(key=lambda z:(-z[0],-z[1],z[2]));train=train_cells[0][3] if train_cells else None
        train_pass=bool(train);train_reasons=[] if train else ["no_train_cell_passed_fixed_gates"]
        validation=next((x for x in stats if train and x["candidate_id"]==c["candidate_id"] and x["split"]=="validation" and x["cell_id"]==train["cell_id"]),None)
        val_pass,val_reasons=_gate(validation,min_n=min_n,min_codes=min_codes,max_year_share=max_year_share) if train_pass else (False,["train_gate_failed_validation_not_eligible"])
        def visible(x):
            if not x:return None
            return {"n":int(x["n"]),"unique_codes":int(x["unique_codes"]),"expectancy":float(x["expectancy"]),"profit_factor":_pf(x["gain"],x["loss"]),"max_year_share":float(x["max_year_share"])}
        reports.append({"candidate_id":c["candidate_id"],"side":c["side"],"interaction_cell_expression":c["cell"],"selected_cell_id":str(train["cell_id"]) if train else None,"train":visible(train),"train_gate_pass":train_pass,"train_gate_reasons":train_reasons,"validation":visible(validation) if train_pass else "locked_by_train_gate","validation_gate_pass":val_pass,"validation_gate_reasons":val_reasons})
        if train_pass and val_pass:unlocked.append(c["candidate_id"])
    compare={"schema_version":f"{AXIS_ID}.compare.v2","artifact_role":"authoritative","axis_id":AXIS_ID,"research_phase":"branching_generation","fixed_evaluation_conditions":{"source":str(source),"candidates":[c["candidate_id"] for c in CANDIDATES],"train":TRAIN,"validation":VALIDATION,"test":TEST,"complete_horizon":"ret_20b non-null","outcome_usage":"evaluation_only_never_cell_definition","short_sign":"-ret_20b","long_sign":"ret_20b","cell_selection":"highest train PF then expectancy then lexical cell among fixed-gate passers","gates":{"min_n":min_n,"min_unique_codes":min_codes,"max_year_share":max_year_share,"expectancy":">0","profit_factor":">1"}},"candidate_reports":reports,"test_access":{"status":"locked","rows_read":False,"metrics_computed":False,"unlocked_candidate_ids":unlocked},"decision":{"candidate_local_decision":"freeze_validation_passers_for_test_unlock" if unlocked else "hold_no_validation_passer","authoritative_rollup_decision":"review_only"},"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    freeze={"schema_version":f"{AXIS_ID}.freeze.v1","artifact_role":"authoritative","frozen_candidate_ids":unlocked[:3],"test_period":TEST,"test_status":"not_opened","test_metrics":None,"selection_source":"train_2020_2022_then_validation_2023_2024_only"}
    return compare,freeze


def run(source:Path=DEFAULT_SOURCE,output:Path=DEFAULT_OUTPUT,*,min_n:int=30,min_codes:int=10,max_year_share:float=.60)->Path:
    compare,freeze=build(source,min_n=min_n,min_codes=min_codes,max_year_share=max_year_share);stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");root=output/f"{stamp}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False)
    (root/"compare.json").write_text(json.dumps(compare,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (root/"test_unlock_freeze.json").write_text(json.dumps(freeze,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    manifest=build_run_manifest(session_id=root.name,seed=0,random_seed=0,input_artifacts=[{"path":str(source),"sha256":_sha(source)}],asof=str(VALIDATION[1]),config=compare["fixed_evaluation_conditions"],universe=["source_parquet_all_codes"],period={"train":TRAIN,"validation":VALIDATION,"test_locked":TEST},horizon="20_business_days",artifact_detail_level="authoritative_full",fallback_status="authoritative")
    manifest.update({"artifact_role":"authoritative","compare_path":str(root/"compare.json"),"freeze_path":str(root/"test_unlock_freeze.json"),"test_rows_read":False,"invalid_predecessor_path":r"G:\Tradex\phase5_ma_interaction_miner_v1\20260713T040729Z-tradex_phase5_ma_interaction_miner_v1","invalid_predecessor_reason":"wrong source position_state_forward_path_rows and wrong bull_volume_regime candidate contract","runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False})
    (root/"run_manifest.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");return root


if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--source",type=Path,default=DEFAULT_SOURCE);p.add_argument("--output-root",type=Path,default=DEFAULT_OUTPUT);p.add_argument("--min-n",type=int,default=30);p.add_argument("--min-codes",type=int,default=10);p.add_argument("--max-year-share",type=float,default=.60);a=p.parse_args();print(run(a.source,a.output_root,min_n=a.min_n,min_codes=a.min_codes,max_year_share=a.max_year_share))
