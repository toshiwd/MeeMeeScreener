from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:sys.path.insert(0,str(REPO))
from app.backend.services.tradex_research_contracts import build_run_manifest

AXIS_ID="tradex_phase5_long_cell_stability_v1"
SOURCE=Path(r"G:\Tradex\ma_phase_feature_base_v1\20260603T121215Z-ma-phase-feature-base-v1\ma_phase_features.parquet")
OUT=Path(r"G:\Tradex\phase5_long_cell_stability_v1")
TRAIN=(20200101,20221231);VALIDATION=(20230101,20241231);TEST=(20250101,20261231)


def _sha(p:Path)->str:
    h=hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""):h.update(b)
    return h.hexdigest()


def _pf(gain:Any,loss:Any)->float|None:return float(gain)/float(loss) if loss and float(loss)>0 else None


def _metric(row:tuple[Any,...],cols:list[str])->dict[str,Any]:
    d=dict(zip(cols,row));return {"n":int(d["n"]),"unique_codes":int(d["unique_codes"]),"expectancy":float(d["expectancy"]),"profit_factor":_pf(d["gain"],d["loss"])}


def analyze(source:Path)->dict[str,Any]:
    con=duckdb.connect()
    try:
        schema={r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source.as_posix()}')").fetchall()}
        regime_col=next((x for x in ("regime_label","market_regime","regime") if x in schema),None)
        regime_expr=f"cast({regime_col} as varchar)" if regime_col else "'unavailable_in_source'"
        con.execute(f"""CREATE TEMP VIEW fixed_cell AS SELECT code,ymd,ret_20b,cast(ymd/10000 as integer) year_bucket,cast(ymd/100 as integer) month_bucket,{regime_expr} regime_bucket FROM read_parquet('{source.as_posix()}') WHERE ymd<={VALIDATION[1]} AND ret_20b IS NOT NULL AND bars_since_cross_above_ma20=0 AND lower_support_bucket='none_near'""")
        def query(groups:str="",where:str="true"):
            select=(groups+"," if groups else "")+"count(*) n,count(distinct code) unique_codes,avg(ret_20b) expectancy,sum(case when ret_20b>0 then ret_20b else 0 end) gain,-sum(case when ret_20b<0 then ret_20b else 0 end) loss"
            cur=con.execute(f"SELECT {select} FROM fixed_cell WHERE {where}"+(f" GROUP BY {groups} ORDER BY {groups}" if groups else ""));return cur.fetchall(),[x[0] for x in cur.description]
        def rows(groups,where):
            raw,cols=query(groups,where);keys=[x.strip() for x in groups.split(',') if x.strip()]
            return [{**{k:dict(zip(cols,r))[k] for k in keys},**_metric(r,cols)} for r in raw]
        train_where=f"ymd between {TRAIN[0]} and {TRAIN[1]}";val_where=f"ymd between {VALIDATION[0]} and {VALIDATION[1]}"
        train_raw,train_cols=query("",train_where);val_raw,val_cols=query("",val_where)
        years=rows("year_bucket",train_where);months=rows("month_bucket",f"ymd between {TRAIN[0]} and {VALIDATION[1]}")
        regimes=rows("regime_bucket",f"ymd between {TRAIN[0]} and {VALIDATION[1]}")
        symbols=rows("code",f"ymd between {TRAIN[0]} and {VALIDATION[1]}")
        loo=[]
        for year in (2020,2021,2022):
            raw,cols=query("",f"{train_where} and year_bucket<>{year}")
            loo.append({"left_out_year":year,"remaining_train":_metric(raw[0],cols) if raw else None})
        active_years=sum(int(x["n"])>0 for x in years)
        stability_pass=active_years>=3 and all((x["profit_factor"] or 0)>1 for x in years) and all(((x["remaining_train"] or {}).get("profit_factor") or 0)>1 for x in loo)
        return {"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","axis_id":AXIS_ID,"research_phase":"effectiveness_judgment","fixed_candidate":{"candidate_id":"long_bars_since_cross_ma20_x_lower_support:0|none_near","side":"long","condition":"bars_since_cross_above_ma20=0 AND lower_support_bucket='none_near'","outcome":"ret_20b evaluation_only","condition_reselection":False,"threshold_reselection":False},"fixed_periods":{"train":TRAIN,"validation":VALIDATION,"test":TEST,"maximum_observed_date":VALIDATION[1]},"stability_contract":{"minimum_active_train_years":3,"reason":"train period 2020-2022 contains exactly three possible active years; a five-year requirement is impossible under the fixed split","active_train_years":active_years,"all_train_year_pf_above_one":all((x["profit_factor"] or 0)>1 for x in years),"all_leave_one_year_out_pf_above_one":all(((x["remaining_train"] or {}).get("profit_factor") or 0)>1 for x in loo),"pass":stability_pass},"test_access":{"status":"not_opened","rows_read":False,"metrics":None},"train":_metric(train_raw[0],train_cols) if train_raw else None,"validation":_metric(val_raw[0],val_cols) if val_raw else None,"by_train_year":years,"by_month_train_and_validation":months,"by_regime_train_and_validation":regimes,"regime_column":regime_col or "unavailable_in_source","by_symbol_train_and_validation":symbols,"leave_one_train_year_out":loo,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    finally:con.close()


def run(source:Path=SOURCE,out:Path=OUT)->Path:
    compare=analyze(source);stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");root=out/f"{stamp}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);(root/"compare.json").write_text(json.dumps(compare,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    manifest=build_run_manifest(session_id=root.name,seed=0,random_seed=0,input_artifacts=[{"path":str(source),"sha256":_sha(source)}],asof=str(VALIDATION[1]),config={"fixed_candidate":compare["fixed_candidate"],"test_not_opened":True},universe=["source_parquet_all_codes"],period={"train":TRAIN,"validation":VALIDATION,"test_locked":TEST},horizon="20_business_days",artifact_detail_level="authoritative_full",fallback_status="authoritative")
    manifest.update({"artifact_role":"authoritative","compare_path":str(root/"compare.json"),"test_rows_read":False,"regime_instrumentation":compare["regime_column"],"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False});(root/"run_manifest.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");return root


if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--source",type=Path,default=SOURCE);p.add_argument("--output-root",type=Path,default=OUT);a=p.parse_args();print(run(a.source,a.output_root))
