from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from sklearn.tree import DecisionTreeRegressor

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.tradex_point_in_time_side_permission_router_v1 import DEFAULT_DB, build_corrected_baseline, metrics
from scripts.tradex_point_in_time_side_priority_top3_v1 import branching
from scripts import tradex_point_in_time_chart_shape_priority_top3_v1 as v1


AXIS_ID = "tradex_point_in_time_rolling_shape_priority_top3_v1"
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_rolling_shape_priority_top3_v1")
LOOKBACK_SESSIONS = 252
MIN_TRAIN_N = 300


def _hash_doc(doc: dict) -> str:
    return hashlib.sha256(json.dumps(doc, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def monthly_models(frame: pd.DataFrame, pan_calendar: list[int], evaluation_start: int = 20250101):
    evaluation = frame[frame.signal_ymd >= evaluation_start].copy()
    evaluation["month"] = evaluation.signal_ymd.astype(str).str[:6]
    models, blocked = {}, []
    cal = pd.Series(pan_calendar)
    for month, part in evaluation.groupby("month", sort=True):
        first_signal = int(part.signal_ymd.min())
        position = int(cal.searchsorted(first_signal, side="left"))
        lower = int(cal.iloc[max(0, position - LOOKBACK_SESSIONS)])
        train = frame[(frame.signal_ymd >= lower) & (frame.signal_ymd < first_signal) & (frame.eligible_from_date <= first_signal)].copy()
        if len(train) < MIN_TRAIN_N:
            blocked.append({"month": month, "first_signal_date": first_signal, "train_n": int(len(train)), "typed_reason": "INSUFFICIENT_ROLLING_TRAIN_COVERAGE"})
            continue
        medians = {feature: float(pd.to_numeric(train[feature], errors="coerce").median()) for feature in v1.FEATURES}
        model = DecisionTreeRegressor(max_depth=2, min_samples_leaf=50, random_state=0)
        model.fit(train[v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians), train.side_return.astype(float))
        doc = v1.model_payload(model, medians)
        models[month] = {"model": model, "medians": medians, "doc": doc, "model_hash": _hash_doc(doc), "first_signal_date": first_signal, "lookback_start": lower, "train_n": int(len(train)), "latest_train_signal_date": int(train.signal_ymd.max()), "latest_train_eligible_from_date": int(train.eligible_from_date.max())}
    return models, blocked


def select(frame: pd.DataFrame, models: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    evaluation = frame[frame.signal_ymd >= 20250101].copy(); evaluation["month"] = evaluation.signal_ymd.astype(str).str[:6]; parts=[]
    for month, part in evaluation.groupby("month", sort=True):
        spec=models[month];x=part[v1.FEATURES].apply(pd.to_numeric,errors="coerce").fillna(spec["medians"]);p=part.copy();p["rolling_shape_score"]=spec["model"].predict(x);parts.append(p)
    scored=pd.concat(parts,ignore_index=True).sort_values(["signal_ymd","rolling_shape_score","rank","code"],ascending=[True,False,True,True]);selected=scored.groupby("signal_ymd",sort=True).head(3).copy();selected["global_rank"]=selected.groupby("signal_ymd").cumcount()+1;return scored,selected


def generate(db_path: Path, out_root: Path) -> Path:
    with duckdb.connect(str(db_path),read_only=True) as con: calendar=[int(x[0]) for x in con.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1").fetchall()]
    events,ranking_coverage=build_corrected_baseline(db_path,calendar);frame,feature_coverage=v1.attach_features(events,db_path);models,blocked=monthly_models(frame,calendar)
    now=datetime.now(timezone.utc);root=out_root/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False)
    model_manifest={month:{k:v for k,v in spec.items() if k not in ('model','medians')}|{"medians":spec['medians']} for month,spec in models.items()}
    if blocked:
        payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","axis_id":AXIS_ID,"research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"lookback_pan_sessions":LOOKBACK_SESSIONS,"minimum_train_n":MIN_TRAIN_N,"outcome_known":"signal plus H10 PAN sessions then one-session embargo","fallback":False},"coverage":{"ranking":ranking_coverage,"features":feature_coverage,"blocked_months":blocked},"monthly_models":model_manifest,"decision":{"candidate_local_decision":"blocked","authoritative_rollup_decision":"review_only","typed_reason":"INSUFFICIENT_ROLLING_TRAIN_COVERAGE"},"silent_fallback_used":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False};p=root/'compare.json';p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');return p
    baseline=v1.fixed_baseline(frame);scored,challenger=select(frame,models);end=int(ranking_coverage['ranking_history_end']);counts={"validation":sum(20250101<=d<=20251231 for d in calendar),"shadow":sum(20260101<=d<=end for d in calendar)};baseline_eval=baseline[baseline.signal_ymd>=20250101]
    bm={s:metrics(baseline_eval,s,counts[s]) for s in counts};cm={s:metrics(challenger,s,counts[s]) for s in counts};branch=branching(baseline_eval,challenger);v,b=cm['validation'],bm['validation']
    gates={"daily_pf_ge_1_30":v['daily_profit_factor'] is not None and v['daily_profit_factor']>=1.30,"daily_pf_delta_ge_0_10":v['daily_profit_factor'] is not None and b['daily_profit_factor'] is not None and v['daily_profit_factor']-b['daily_profit_factor']>=.10,"calendar_expectancy_improves":v['calendar_expectancy'] is not None and v['calendar_expectancy']>b['calendar_expectancy'],"frequency_ge_one_day_week":v['signals_per_week']>=1,"cvar_non_degrade":v['cvar10'] is not None and v['cvar10']>=b['cvar10']-1e-12,"drawdown_non_degrade":v['max_drawdown_equal_weight'] is not None and v['max_drawdown_equal_weight']>=b['max_drawdown_equal_weight']-1e-12,"branch_ge_20pct":(branch['summary']['validation']['changed_day_rate'] or 0)>=.20}
    decision='keep_shadow_2026' if all(gates.values()) else 'drop_no_meaningful_branching' if (branch['summary']['validation']['changed_day_rate'] or 0)<.20 else 'drop_effectiveness' if (v['daily_profit_factor'] or 0)<1 else 'hold'
    baseline_eval.to_csv(root/'baseline_fixed_interleave_top3.csv',index=False);challenger.to_csv(root/'challenger_rolling_shape_top3.csv',index=False);scored[['signal_ymd','code','side','rank','split','month','rolling_shape_score']].to_csv(root/'candidate_scores.csv',index=False);(root/'monthly_models.json').write_text(json.dumps(model_manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","axis_id":AXIS_ID,"research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"only_axis":"monthly point-in-time refit","features":v1.FEATURES,"model":"DecisionTreeRegressor max_depth2 min_samples_leaf50 random_state0","lookback_pan_sessions":LOOKBACK_SESSIONS,"minimum_train_n":MIN_TRAIN_N,"outcome_known":"signal plus H10 PAN sessions then one-session embargo","refit":"first signal date each month","candidate_generation_execution_top3_cost_baseline":"unchanged","candidate_suppression":False,"fallback":False,"splits":{"history":"2024","validation":"2025","shadow":"2026 untouched"}},"coverage":{"ranking":ranking_coverage,"features":feature_coverage,"blocked_months":[]},"monthly_models":model_manifest,"baseline_fixed_interleave":bm,"challenger_rolling_shape":cm,"branching":branch,"validation_keep_gates":gates,"decision":{"candidate_local_decision":decision,"authoritative_rollup_decision":"review_only","reason_type":"single_axis_monthly_point_in_time_refit_validation"},"shadow_tuning_used":False,"silent_fallback_used":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False};p=root/'compare.json';p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');return p


def main():
    a=argparse.ArgumentParser();a.add_argument('--db',type=Path,default=DEFAULT_DB);a.add_argument('--out',type=Path,default=DEFAULT_OUT);x=a.parse_args();print(generate(x.db,x.out))
if __name__=='__main__':main()
