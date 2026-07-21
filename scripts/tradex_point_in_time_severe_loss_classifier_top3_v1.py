from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss
from sklearn.tree import DecisionTreeClassifier, export_text

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.tradex_point_in_time_side_permission_router_v1 import DEFAULT_DB, build_corrected_baseline, metrics
from scripts.tradex_point_in_time_side_priority_top3_v1 import branching
from scripts import tradex_point_in_time_chart_shape_priority_top3_v1 as v1


AXIS_ID = "tradex_point_in_time_severe_loss_classifier_top3_v1"
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_severe_loss_classifier_top3_v1")


def fit_model(frame: pd.DataFrame):
    train = frame[frame.split == "train"].copy()
    medians = {feature: float(pd.to_numeric(train[feature], errors="coerce").median()) for feature in v1.FEATURES}
    if len(train) < 100 or any(np.isnan(value) for value in medians.values()):
        raise ValueError("TRAIN_FEATURE_COVERAGE_INSUFFICIENT")
    target = train.side_return.astype(float).le(-0.05).astype(int)
    if target.nunique() < 2:
        raise ValueError("SEVERE_LOSS_TARGET_SINGLE_CLASS")
    model = DecisionTreeClassifier(max_depth=2, min_samples_leaf=50, random_state=0)
    model.fit(train[v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians), target)
    return model, medians


def loss_probability(model, x: pd.DataFrame) -> np.ndarray:
    classes = list(model.classes_)
    return model.predict_proba(x)[:, classes.index(1)] if 1 in classes else np.zeros(len(x))


def select(frame: pd.DataFrame, model, medians: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored = frame.copy()
    x = scored[v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians)
    scored["severe_loss_probability"] = loss_probability(model, x)
    scored["baseline_order"] = scored["rank"].astype(int) * 2 - scored.side.eq("buy").astype(int)
    scored = scored.sort_values(["signal_ymd", "severe_loss_probability", "baseline_order", "code"], ascending=[True, True, True, True])
    selected = scored.groupby("signal_ymd", sort=True).head(3).copy()
    selected["global_rank"] = selected.groupby("signal_ymd").cumcount() + 1
    return scored, selected


def model_payload(model, medians: dict) -> dict:
    tree = model.tree_; nodes = []
    for idx in range(tree.node_count):
        raw = tree.value[idx].reshape(-1).tolist(); total = sum(raw); proportions = [value / total for value in raw]; samples = int(tree.n_node_samples[idx])
        nodes.append({"node": idx, "feature": v1.FEATURES[tree.feature[idx]] if tree.feature[idx] >= 0 else None, "threshold": float(tree.threshold[idx]) if tree.feature[idx] >= 0 else None, "left": int(tree.children_left[idx]), "right": int(tree.children_right[idx]), "samples": samples, "class_counts": {str(int(c)): int(round(proportions[pos] * samples)) for pos, c in enumerate(model.classes_)}, "class_proportions": {str(int(c)): float(proportions[pos]) for pos, c in enumerate(model.classes_)}, "severe_loss_probability": float(proportions[list(model.classes_).index(1)]) if 1 in model.classes_ else 0.0})
    return {"estimator": "sklearn.tree.DecisionTreeClassifier", "parameters": {"max_depth": 2, "min_samples_leaf": 50, "random_state": 0}, "features": v1.FEATURES, "classes": [int(x) for x in model.classes_], "train_medians": medians, "nodes": nodes, "leaf_counts": [node for node in nodes if node["feature"] is None], "text": export_text(model, feature_names=v1.FEATURES)}


def calibration(frame: pd.DataFrame, model, medians: dict) -> dict:
    result = {}
    for split in ("train", "validation", "shadow"):
        part = frame[frame.split == split]
        y = part.side_return.astype(float).le(-0.05).astype(int)
        p = loss_probability(model, part[v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians))
        bins = pd.DataFrame({"y": y.to_numpy(), "p": p}).groupby("p", sort=True).agg(n=("y", "size"), predicted_probability=("p", "first"), observed_rate=("y", "mean")).reset_index(drop=True).to_dict("records")
        result[split] = {"n": int(len(part)), "prevalence": float(y.mean()), "mean_predicted_probability": float(np.mean(p)), "brier_score": float(brier_score_loss(y, p)), "probability_leaf_bins": bins}
    return result


def generate(db_path: Path, out_root: Path) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as con:
        calendar = [int(x[0]) for x in con.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1").fetchall()]
    events, ranking_coverage = build_corrected_baseline(db_path, calendar)
    frame, feature_coverage = v1.attach_features(events, db_path)
    model, medians = fit_model(frame); scored, challenger = select(frame, model, medians); baseline = v1.fixed_baseline(frame)
    end = int(ranking_coverage["ranking_history_end"]); counts = {"train": sum(20240101 <= d <= 20241231 for d in calendar), "validation": sum(20250101 <= d <= 20251231 for d in calendar), "shadow": sum(20260101 <= d <= end for d in calendar)}
    bm = {s: metrics(baseline, s, counts[s]) for s in counts}; cm = {s: metrics(challenger, s, counts[s]) for s in counts}; branch = branching(baseline, challenger); v, b = cm["validation"], bm["validation"]
    gates = {"daily_pf_ge_1_30": v["daily_profit_factor"] is not None and v["daily_profit_factor"] >= 1.30, "daily_pf_delta_ge_0_10": v["daily_profit_factor"] is not None and b["daily_profit_factor"] is not None and v["daily_profit_factor"] - b["daily_profit_factor"] >= .10, "calendar_expectancy_improves": v["calendar_expectancy"] is not None and v["calendar_expectancy"] > b["calendar_expectancy"], "frequency_ge_one_day_week": v["signals_per_week"] >= 1, "cvar_non_degrade": v["cvar10"] is not None and v["cvar10"] >= b["cvar10"] - 1e-12, "drawdown_non_degrade": v["max_drawdown_equal_weight"] is not None and v["max_drawdown_equal_weight"] >= b["max_drawdown_equal_weight"] - 1e-12, "branch_ge_20pct": (branch["summary"]["validation"]["changed_day_rate"] or 0) >= .20}
    decision = "keep_shadow_2026" if all(gates.values()) else "drop_no_meaningful_branching" if (branch["summary"]["validation"]["changed_day_rate"] or 0) < .20 else "drop_effectiveness" if (v["daily_profit_factor"] or 0) <= (b["daily_profit_factor"] or 0) or not gates["calendar_expectancy_improves"] else "hold"
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; root.mkdir(parents=True, exist_ok=False)
    doc = model_payload(model, medians); cal = calibration(frame, model, medians)
    baseline.to_csv(root / "baseline_fixed_interleave_top3.csv", index=False); challenger.to_csv(root / "challenger_severe_loss_top3.csv", index=False); scored[["signal_ymd", "code", "side", "rank", "split", "baseline_order", "severe_loss_probability"]].to_csv(root / "candidate_scores.csv", index=False); (root / "frozen_model.json").write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"only_axis": "target severe_loss=(next_open side_return<=-0.05)", "features": v1.FEATURES, "train": "2024 only", "model": "DecisionTreeClassifier max_depth2 min_samples_leaf50 random_state0", "selection": "predict_proba severe_loss ascending; fixed baseline order tie; unified top3", "candidate_generation_execution_baseline": "unchanged", "candidate_suppression": False, "threshold_search": False, "splits": {"validation": "2025", "shadow": "2026 untouched"}}, "coverage": {"ranking": ranking_coverage, "features": feature_coverage}, "frozen_model": doc, "calibration": cal, "baseline_fixed_interleave": bm, "challenger_severe_loss_classifier": cm, "branching": branch, "validation_keep_gates": gates, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "single_axis_severe_loss_classifier_validation"}, "threshold_search_used": False, "shadow_tuning_used": False, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    path = root / "compare.json"; path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); return path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--db", type=Path, default=DEFAULT_DB); parser.add_argument("--out", type=Path, default=DEFAULT_OUT); args = parser.parse_args(); print(generate(args.db, args.out))


if __name__ == "__main__": main()
