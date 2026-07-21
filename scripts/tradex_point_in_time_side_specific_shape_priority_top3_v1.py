from __future__ import annotations

import argparse
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


AXIS_ID = "tradex_point_in_time_side_specific_shape_priority_top3_v1"
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_side_specific_shape_priority_top3_v1")
MIN_SIDE_TRAIN_N = 100


def fit_side_models(frame: pd.DataFrame):
    train = frame[frame.split == "train"]
    counts = train.groupby("side").size().to_dict()
    missing = {side: int(counts.get(side, 0)) for side in ("buy", "sell") if counts.get(side, 0) < MIN_SIDE_TRAIN_N}
    if missing:
        raise ValueError("INSUFFICIENT_SIDE_TRAIN_COVERAGE:" + json.dumps(missing, sort_keys=True))
    models, medians = {}, {}
    for side in ("buy", "sell"):
        part = train[train.side == side]
        med = {feature: float(pd.to_numeric(part[feature], errors="coerce").median()) for feature in v1.FEATURES}
        model = DecisionTreeRegressor(max_depth=2, min_samples_leaf=50, random_state=0)
        model.fit(part[v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(med), part.side_return.astype(float))
        models[side], medians[side] = model, med
    return models, medians, {side: int(counts[side]) for side in ("buy", "sell")}


def score_select(frame, models, medians):
    scored = frame.copy(); scored["side_shape_score"] = 0.0
    for side, model in models.items():
        mask = scored.side == side
        x = scored.loc[mask, v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians[side])
        scored.loc[mask, "side_shape_score"] = model.predict(x)
    scored = scored.sort_values(["signal_ymd", "side_shape_score", "rank", "code"], ascending=[True, False, True, True])
    selected = scored.groupby("signal_ymd", sort=True).head(3).copy(); selected["global_rank"] = selected.groupby("signal_ymd").cumcount() + 1
    return scored, selected


def generate(db_path: Path, out_root: Path) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as con:
        calendar = [int(x[0]) for x in con.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1").fetchall()]
    events, ranking_coverage = build_corrected_baseline(db_path, calendar); frame, feature_coverage = v1.attach_features(events, db_path)
    try: models, medians, side_n = fit_side_models(frame)
    except ValueError as exc:
        now=datetime.now(timezone.utc);root=out_root/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False);p=root/'compare.json';p.write_text(json.dumps({"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","decision":{"candidate_local_decision":"blocked","typed_reason":str(exc)},"silent_fallback_used":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False},indent=2)+"\n");return p
    baseline=v1.fixed_baseline(frame); unified_model, unified_medians=v1.fit_train_model(frame);_,unified=v1.select(frame,unified_model,unified_medians);scored,challenger=score_select(frame,models,medians)
    end=int(ranking_coverage['ranking_history_end']);counts={"train":sum(20240101<=d<=20241231 for d in calendar),"validation":sum(20250101<=d<=20251231 for d in calendar),"shadow":sum(20260101<=d<=end for d in calendar)}
    bm={s:metrics(baseline,s,counts[s]) for s in counts};um={s:metrics(unified,s,counts[s]) for s in counts};cm={s:metrics(challenger,s,counts[s]) for s in counts};branch_base=branching(baseline,challenger);branch_unified=branching(unified,challenger);v,b=cm['validation'],bm['validation']
    gates={"daily_pf_ge_1_30":v['daily_profit_factor'] is not None and v['daily_profit_factor']>=1.30,"daily_pf_delta_ge_0_10":v['daily_profit_factor'] is not None and b['daily_profit_factor'] is not None and v['daily_profit_factor']-b['daily_profit_factor']>=.10,"calendar_expectancy_improves":v['calendar_expectancy'] is not None and v['calendar_expectancy']>b['calendar_expectancy'],"frequency_ge_one_day_week":v['signals_per_week']>=1,"cvar_non_degrade":v['cvar10'] is not None and v['cvar10']>=b['cvar10']-1e-12,"drawdown_non_degrade":v['max_drawdown_equal_weight'] is not None and v['max_drawdown_equal_weight']>=b['max_drawdown_equal_weight']-1e-12,"branch_ge_20pct":(branch_base['summary']['validation']['changed_day_rate'] or 0)>=.20}
    decision='keep_shadow_2026' if all(gates.values()) else 'drop_no_meaningful_branching' if (branch_base['summary']['validation']['changed_day_rate'] or 0)<.20 else 'drop_effectiveness' if (v['daily_profit_factor'] or 0)<1 else 'hold'
    docs={side:v1.model_payload(models[side],medians[side]) for side in ('buy','sell')};now=datetime.now(timezone.utc);root=out_root/f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True,exist_ok=False)
    baseline.to_csv(root/'baseline_fixed_interleave_top3.csv',index=False);unified.to_csv(root/'v1_unified_shape_top3.csv',index=False);challenger.to_csv(root/'challenger_side_specific_shape_top3.csv',index=False);scored[['signal_ymd','code','side','rank','split','side_shape_score']].to_csv(root/'candidate_scores.csv',index=False);(root/'frozen_side_models.json').write_text(json.dumps(docs,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","axis_id":AXIS_ID,"research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"only_axis":"fit separate BUY and SELL v1 trees on 2024","features":v1.FEATURES,"model_each_side":"DecisionTreeRegressor max_depth2 min_samples_leaf50 random_state0","minimum_side_train_n":MIN_SIDE_TRAIN_N,"candidate_generation_execution_top3_cost_baseline_splits":"unchanged","candidate_suppression":False,"shadow_tuning":False},"coverage":{"ranking":ranking_coverage,"features":feature_coverage,"side_train_n":side_n},"frozen_side_models":docs,"baseline_fixed_interleave":bm,"v1_unified_model":um,"challenger_side_specific":cm,"comparison_validation":{"side_specific_minus_baseline_daily_pf":cm['validation']['daily_profit_factor']-bm['validation']['daily_profit_factor'],"side_specific_minus_unified_daily_pf":cm['validation']['daily_profit_factor']-um['validation']['daily_profit_factor'],"side_specific_minus_baseline_calendar_expectancy":cm['validation']['calendar_expectancy']-bm['validation']['calendar_expectancy'],"side_specific_minus_unified_calendar_expectancy":cm['validation']['calendar_expectancy']-um['validation']['calendar_expectancy']},"branching_vs_baseline":branch_base,"branching_vs_v1_unified":branch_unified,"validation_keep_gates":gates,"decision":{"candidate_local_decision":decision,"authoritative_rollup_decision":"review_only","reason_type":"single_axis_side_specific_shape_tree_validation"},"silent_fallback_used":False,"shadow_tuning_used":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    p=root/'compare.json';p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');return p


def main():
    a=argparse.ArgumentParser();a.add_argument('--db',type=Path,default=DEFAULT_DB);a.add_argument('--out',type=Path,default=DEFAULT_OUT);x=a.parse_args();print(generate(x.db,x.out))
if __name__=='__main__':main()
