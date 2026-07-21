from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import log_loss


AXIS_ID = "tradex_nikkei225_daily_assessment_baseline_v1"
FEATURES = [
    "body_ratio","upper_wick_ratio","lower_wick_ratio","close_pos","ret3","ret5","ret10","pre_ret10","pos20","range20_pct",
    "bear_count5","bear_body5_atr","upper_supply_count5","lower_rejection_count5","low_close_count3",
    "dist_ma7_atr","dist_ma20_atr","dist_ma60_atr","ma7_slope5_atr","ma20_slope5_atr","ma60_slope5_atr","volume_ratio20",
    "cross_ma7","cross_ma20","reclaim_ma7","reclaim_ma20","support_break","support_break_depth_atr","oversold_risk",
    "market_breadth_ma20","market_breadth_ma60","market_advancers_ratio","market_mean_ret1",
    "weekly_lower_high","weekly_upper_wick_ratio","weekly_close_pos","monthly_high_failure",
]
HORIZONS = {
    1: {"mult": .75, "min": .01, "max": .03, "n": 150, "codes": 50, "precision": .58, "false": .25, "mean": 0.0},
    3: {"mult": 1.25, "min": .02, "max": .05, "n": 120, "codes": 45, "precision": .60, "false": .22, "mean": -.005},
    5: {"mult": 1.5, "min": .03, "max": .07, "n": 100, "codes": 40, "precision": .60, "false": .20, "mean": -.010},
    10: {"mult": 2.0, "min": .05, "max": .10, "n": 80, "codes": 35, "precision": .60, "false": .20, "mean": -.015},
}
CLASS_NAMES = ["downside_continuation", "rebound", "neutral"]


def _labels(frame: pd.DataFrame, horizon: int) -> np.ndarray:
    contract = HORIZONS[horizon]
    down_barrier = np.clip(contract["mult"] * frame["atr14"].to_numpy() / frame["c"].to_numpy(), contract["min"], contract["max"])
    rebound_barrier = .8 * down_barrier
    ret = frame[f"ret_close_{horizon}"].to_numpy()
    down_exc = frame[f"down_exc_{horizon}"].to_numpy()
    up_exc = frame[f"up_exc_{horizon}"].to_numpy()
    rebound = ((up_exc >= rebound_barrier) & (ret >= 0)) | (ret >= .4 * rebound_barrier)
    downside = (down_exc <= -down_barrier) & (ret <= -.4 * down_barrier) & ~rebound
    return np.where(rebound, 1, np.where(downside, 0, 2)).astype(int)


def _model() -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(max_iter=160, learning_rate=.06, max_leaf_nodes=15, max_depth=4, min_samples_leaf=120, l2_regularization=2.0, class_weight=None, random_state=20260714)


def _ece(y: np.ndarray, probability: np.ndarray, target: int) -> float:
    truth = (y == target).astype(float); edges = np.quantile(probability, np.linspace(0, 1, 11)); total = len(y); value = 0.0
    for low, high in zip(edges[:-1], edges[1:]):
        mask = (probability >= low) & (probability <= high if high == edges[-1] else probability < high)
        if mask.any(): value += mask.sum() / total * abs(probability[mask].mean() - truth[mask].mean())
    return float(value)


def _metrics(frame: pd.DataFrame, labels: np.ndarray, probabilities: np.ndarray, threshold: tuple[float, float]) -> dict[str, Any]:
    pdown, prebound = probabilities[:, 0], probabilities[:, 1]
    action = (pdown >= threshold[0]) & (prebound <= threshold[1])
    selected = frame.loc[action]
    count = int(action.sum())
    precision = float((labels[action] == 0).mean()) if count else None
    false_action = float((labels[action] == 1).mean()) if count else None
    mean_return = float(selected["target_return"].mean()) if count else None
    down_capture = float((selected["target_down_exc"] <= -selected["target_down_barrier"]).mean()) if count else None
    prevalence = np.bincount(labels, minlength=3) / len(labels)
    constant = np.tile(prevalence, (len(labels), 1))
    brier = float(np.mean(np.sum((probabilities - np.eye(3)[labels]) ** 2, axis=1)))
    brier_constant = float(np.mean(np.sum((constant - np.eye(3)[labels]) ** 2, axis=1)))
    months = selected["ymd"].astype(str).str[:6].nunique() if count else 0
    concentration_code = float(selected.groupby("code").size().max() / count) if count else None
    concentration_month = float(selected.assign(month=selected["ymd"].astype(str).str[:6]).groupby("month").size().max() / count) if count else None
    return {"eligible_n": len(labels), "action_n": count, "action_codes": int(selected["code"].nunique()) if count else 0, "coverage": count / len(labels), "precision_downside": precision, "false_action_rebound": false_action, "mean_return": mean_return, "downside_capture": down_capture, "months": int(months), "max_code_contribution": concentration_code, "max_month_contribution": concentration_month, "ece_down": _ece(labels, pdown, 0), "ece_rebound": _ece(labels, prebound, 1), "brier": brier, "brier_constant": brier_constant, "brier_skill_positive": brier < brier_constant, "log_loss": float(log_loss(labels, probabilities, labels=[0,1,2])), "class_prevalence": {CLASS_NAMES[i]: float(prevalence[i]) for i in range(3)}}


def _choose_threshold(frame: pd.DataFrame, labels: np.ndarray, probabilities: np.ndarray, horizon: int) -> tuple[tuple[float, float], bool]:
    gate = HORIZONS[horizon]; candidates = []
    for down in np.arange(.30, .76, .025):
        for rebound in np.arange(.10, .46, .025):
            metric = _metrics(frame, labels, probabilities, (float(down), float(rebound)))
            if .03 <= metric["coverage"] <= .30 and metric["action_n"] >= gate["n"] and metric["precision_downside"] >= gate["precision"] and metric["false_action_rebound"] <= gate["false"]:
                candidates.append((metric["coverage"], (float(down), float(rebound))))
    if candidates: return max(candidates)[1], True
    fallback = []
    for down in np.arange(.30, .76, .05):
        for rebound in np.arange(.10, .46, .05):
            metric = _metrics(frame, labels, probabilities, (float(down), float(rebound)))
            if metric["coverage"] >= .01 and metric["action_n"]:
                fallback.append(((metric["precision_downside"] or 0) - (metric["false_action_rebound"] or 1), (float(down), float(rebound))))
    return (max(fallback)[1] if fallback else (.75, .10)), False


def _prepare(frame: pd.DataFrame, horizon: int) -> tuple[pd.DataFrame, np.ndarray]:
    required = [f"ret_close_{horizon}", f"down_exc_{horizon}", f"up_exc_{horizon}", "atr14", "c"]
    part = frame.dropna(subset=required).copy()
    labels = _labels(part, horizon)
    contract = HORIZONS[horizon]
    part["target_return"] = part[f"ret_close_{horizon}"]
    part["target_down_exc"] = part[f"down_exc_{horizon}"]
    part["target_down_barrier"] = np.clip(contract["mult"] * part["atr14"] / part["c"], contract["min"], contract["max"])
    return part, labels


def run(input_parquet: Path, output_root: Path) -> Path:
    frame = duckdb.connect().execute(f"SELECT * FROM read_parquet('{input_parquet.as_posix()}') ORDER BY ymd,code").fetchdf()
    train_all = frame[(frame.ymd >= 20190101) & (frame.ymd <= 20241231)].copy()
    validation_all = frame[(frame.ymd >= 20250101) & (frame.ymd <= 20251231)].copy()
    exploratory_all = frame[(frame.ymd >= 20260101) & (frame.ymd <= 20260713)].copy()
    latest = frame[frame.ymd == frame.ymd.max()].copy()
    results, latest_rows = {}, []
    for horizon in HORIZONS:
        train, train_y = _prepare(train_all, horizon); validation, validation_y = _prepare(validation_all, horizon); exploratory, exploratory_y = _prepare(exploratory_all, horizon)
        dates = np.array(sorted(train.ymd.unique())); boundaries = np.array_split(dates, 5); oof_p = np.full((len(train), 3), np.nan)
        for fold in range(1, 5):
            test_dates = boundaries[fold]; prior_dates = np.concatenate(boundaries[:fold]); fit_mask = train.ymd.isin(prior_dates).to_numpy(); test_mask = train.ymd.isin(test_dates).to_numpy()
            model = _model(); model.fit(train.loc[fit_mask, FEATURES], train_y[fit_mask]); oof_p[test_mask] = model.predict_proba(train.loc[test_mask, FEATURES])
        oof_mask = ~np.isnan(oof_p).any(axis=1); threshold, threshold_gate = _choose_threshold(train.loc[oof_mask], train_y[oof_mask], oof_p[oof_mask], horizon)
        final_model = _model(); final_model.fit(train[FEATURES], train_y)
        validation_p = final_model.predict_proba(validation[FEATURES]); exploratory_p = final_model.predict_proba(exploratory[FEATURES]); latest_p = final_model.predict_proba(latest[FEATURES])
        train_metric = _metrics(train.loc[oof_mask], train_y[oof_mask], oof_p[oof_mask], threshold); validation_metric = _metrics(validation, validation_y, validation_p, threshold); exploratory_metric = _metrics(exploratory, exploratory_y, exploratory_p, threshold)
        gate = HORIZONS[horizon]
        checks = {"train_threshold_gate": threshold_gate, "sample": validation_metric["action_n"] >= gate["n"] and validation_metric["action_codes"] >= gate["codes"], "coverage": .03 <= validation_metric["coverage"] <= .30, "precision": validation_metric["precision_downside"] is not None and validation_metric["precision_downside"] >= gate["precision"], "false_action": validation_metric["false_action_rebound"] is not None and validation_metric["false_action_rebound"] <= gate["false"], "mean_return": validation_metric["mean_return"] is not None and validation_metric["mean_return"] <= gate["mean"], "breadth": validation_metric["months"] >= 8 and (validation_metric["max_code_contribution"] or 1) <= .05 and (validation_metric["max_month_contribution"] or 1) <= .20, "calibration": validation_metric["ece_down"] <= .05 and validation_metric["ece_rebound"] <= .05 and validation_metric["brier_skill_positive"]}
        passed = all(checks.values()); decision = "hold_pending_clean_shadow" if passed else "drop_baseline"
        results[str(horizon)] = {"threshold": {"p_down_min": threshold[0], "p_rebound_max": threshold[1]}, "train_oof": train_metric, "validation_2025": validation_metric, "exploratory_2026": exploratory_metric, "gate_audit": checks, "decision": decision}
        for index, row in latest.reset_index(drop=True).iterrows():
            action = bool(latest_p[index,0] >= threshold[0] and latest_p[index,1] <= threshold[1])
            latest_rows.append({"code": row.code, "ymd": int(row.ymd), "horizon": horizon, "p_down": latest_p[index,0], "p_rebound": latest_p[index,1], "p_neutral": latest_p[index,2], "diagnostic_state": CLASS_NAMES[int(np.argmax(latest_p[index]))], "assessment_state": "short_review" if passed and action else "no_short_action" if passed else "unjudgeable_model_quality", "quality_gate_passed": passed})
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); output=output_root/f"{stamp}-{AXIS_ID}"; output.mkdir(parents=True,exist_ok=False)
    pd.DataFrame(latest_rows).to_csv(output/"latest_daily_assessment.csv",index=False,encoding="utf-8-sig")
    spec=json.dumps({"features":FEATURES,"horizons":HORIZONS,"model_variant":"unweighted_multi_regime_2019_2024"},sort_keys=True)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"comparison_stabilization","source_parquet":str(input_parquet),"model_variant":"unweighted_multi_regime_2019_2024","candidate_spec_hash":hashlib.sha256(spec.encode()).hexdigest(),"label_contract":{"states":CLASS_NAMES,"priority":"rebound then clean downside then neutral","barriers":"ATR adjusted by horizon"},"split_roles":{"2019_2024":"rolling OOF multi-regime train and threshold selection","2025":"locked validation","2026":"exploratory only"},"results":results,"latest_assessment":str(output/"latest_daily_assessment.csv"),"decision":{"candidate_local_decision":"baseline_ready_for_challenger" if any(v["decision"].startswith("hold") for v in results.values()) else "baseline_dropped_all_horizons","authoritative_rollup_decision":"review_only"},"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False},"remaining_risks":["bootstrap confidence intervals not yet applied","probabilities are uncalibrated HistGradientBoosting outputs","2026 is contaminated exploratory"]}
    (output/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"compare":str(output/"compare.json")},indent=2)+"\n",encoding="utf-8");return output


def main()->None:
    parser=argparse.ArgumentParser();parser.add_argument("--input-parquet",required=True,type=Path);parser.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_baseline_v1"));args=parser.parse_args();print(run(args.input_parquet,args.output_root))


if __name__=="__main__":main()
