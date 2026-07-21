from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeRegressor, export_text

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.tradex_point_in_time_side_permission_router_v1 import DEFAULT_DB, build_corrected_baseline, metrics
from scripts.tradex_point_in_time_side_priority_top3_v1 import branching
from scripts import tradex_point_in_time_chart_shape_priority_top3_v1 as v1


AXIS_ID = "tradex_point_in_time_chart_shape_breadth_priority_top3_v2"
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_chart_shape_breadth_priority_top3_v2")
FEATURES = v1.FEATURES + ["breadth_above_ma20"]


def attach_breadth(frame: pd.DataFrame, db_path: Path) -> tuple[pd.DataFrame, dict]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        columns = {row[0] for row in con.execute("describe ml_feature_daily").fetchall()}
        if "breadth_above_ma20" not in columns:
            raise ValueError("FEATURE_COVERAGE_MISSING:breadth_above_ma20")
        breadth = con.execute(
            """select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) signal_ymd,
                      cast(code as varchar) code,breadth_above_ma20
               from ml_feature_daily
               where dt between epoch(strptime('20240101','%Y%m%d')) and epoch(strptime('20261231','%Y%m%d'))"""
        ).fetchdf()
    result = frame.merge(breadth, on=["signal_ymd", "code"], how="left", validate="many_to_one")
    coverage = float(result.breadth_above_ma20.notna().mean())
    if coverage < 0.95:
        raise ValueError(f"FEATURE_COVERAGE_INSUFFICIENT:breadth_above_ma20={coverage:.6f}")
    return result, {"breadth_above_ma20_non_null_rate": coverage, "point_in_time": "ml_feature_daily signal-date confirmed feature"}


def fit_v2(frame: pd.DataFrame) -> tuple[DecisionTreeRegressor, dict[str, float]]:
    train = frame[frame.split == "train"]
    medians = {feature: float(pd.to_numeric(train[feature], errors="coerce").median()) for feature in FEATURES}
    if len(train) < 100 or any(np.isnan(value) for value in medians.values()):
        raise ValueError("TRAIN_FEATURE_COVERAGE_INSUFFICIENT")
    model = DecisionTreeRegressor(max_depth=2, min_samples_leaf=50, random_state=0)
    model.fit(train[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians), train.side_return.astype(float))
    return model, medians


def score_select(frame: pd.DataFrame, model: DecisionTreeRegressor, medians: dict[str, float]) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored = frame.copy()
    scored["shape_breadth_priority_score"] = model.predict(scored[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians))
    scored = scored.sort_values(["signal_ymd", "shape_breadth_priority_score", "rank", "code"], ascending=[True, False, True, True])
    selected = scored.groupby("signal_ymd", sort=True).head(3).copy()
    selected["global_rank"] = selected.groupby("signal_ymd").cumcount() + 1
    return scored, selected


def tree_doc(model: DecisionTreeRegressor, medians: dict[str, float]) -> dict:
    tree = model.tree_
    nodes = []
    for idx in range(tree.node_count):
        feature = FEATURES[tree.feature[idx]] if tree.feature[idx] >= 0 else None
        nodes.append({"node": idx, "feature": feature, "threshold": float(tree.threshold[idx]) if feature else None, "left": int(tree.children_left[idx]), "right": int(tree.children_right[idx]), "samples": int(tree.n_node_samples[idx]), "value": float(tree.value[idx].reshape(-1)[0])})
    return {"estimator": "sklearn.tree.DecisionTreeRegressor", "parameters": {"max_depth": 2, "min_samples_leaf": 50, "random_state": 0}, "features": FEATURES, "only_added_feature_vs_v1": "breadth_above_ma20", "train_medians": medians, "nodes": nodes, "text": export_text(model, feature_names=FEATURES)}


def gates(candidate: dict, baseline: dict, branch_rate: float | None) -> dict:
    return {"daily_pf_ge_1_30": candidate["daily_profit_factor"] is not None and candidate["daily_profit_factor"] >= 1.30, "daily_pf_delta_ge_0_10": candidate["daily_profit_factor"] is not None and baseline["daily_profit_factor"] is not None and candidate["daily_profit_factor"] - baseline["daily_profit_factor"] >= 0.10, "calendar_expectancy_improves": candidate["calendar_expectancy"] is not None and candidate["calendar_expectancy"] > baseline["calendar_expectancy"], "frequency_ge_one_day_week": candidate["signals_per_week"] >= 1.0, "cvar_non_degrade": candidate["cvar10"] is not None and baseline["cvar10"] is not None and candidate["cvar10"] >= baseline["cvar10"] - 1e-12, "drawdown_non_degrade": candidate["max_drawdown_equal_weight"] is not None and baseline["max_drawdown_equal_weight"] is not None and candidate["max_drawdown_equal_weight"] >= baseline["max_drawdown_equal_weight"] - 1e-12, "branch_ge_20pct": (branch_rate or 0) >= 0.20}


def generate(db_path: Path, out_root: Path) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as con:
        calendar = [int(row[0]) for row in con.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1").fetchall()]
    events, ranking_coverage = build_corrected_baseline(db_path, calendar)
    v1_frame, v1_coverage = v1.attach_features(events, db_path)
    frame, breadth_coverage = attach_breadth(v1_frame, db_path)
    baseline = v1.fixed_baseline(frame)
    v1_model, v1_medians = v1.fit_train_model(frame)
    _, v1_selected = v1.select(frame, v1_model, v1_medians)
    model, medians = fit_v2(frame)
    scored, selected = score_select(frame, model, medians)
    ranking_end = int(ranking_coverage["ranking_history_end"])
    counts = {"train": sum(20240101 <= d <= 20241231 for d in calendar), "validation": sum(20250101 <= d <= 20251231 for d in calendar), "shadow": sum(20260101 <= d <= ranking_end for d in calendar)}
    bm = {s: metrics(baseline, s, counts[s]) for s in counts}
    m1 = {s: metrics(v1_selected, s, counts[s]) for s in counts}
    m2 = {s: metrics(selected, s, counts[s]) for s in counts}
    branch_base = branching(baseline, selected)
    branch_v1 = branching(v1_selected, selected)
    keep_gates = gates(m2["validation"], bm["validation"], branch_base["summary"]["validation"]["changed_day_rate"])
    v1_branch_rate = branch_v1["summary"]["validation"]["changed_day_rate"] or 0
    if all(keep_gates.values()):
        decision = "keep_shadow_2026"
    elif v1_branch_rate == 0:
        decision = "drop_added_feature_no_branch_vs_v1"
    elif (branch_base["summary"]["validation"]["changed_day_rate"] or 0) < 0.20:
        decision = "drop_no_meaningful_branching"
    elif m2["validation"]["daily_profit_factor"] is not None and m2["validation"]["daily_profit_factor"] < 1.0:
        decision = "drop_effectiveness"
    else:
        decision = "hold"
    now = datetime.now(timezone.utc)
    root = out_root / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    baseline.to_csv(root / "baseline_fixed_interleave_top3.csv", index=False)
    v1_selected.to_csv(root / "v1_chart_shape_priority_top3.csv", index=False)
    selected.to_csv(root / "v2_chart_shape_breadth_priority_top3.csv", index=False)
    scored[["signal_ymd", "code", "side", "rank", "split", "shape_breadth_priority_score"]].to_csv(root / "candidate_scores.csv", index=False)
    model_payload = tree_doc(model, medians)
    (root / "frozen_model_v2.json").write_text(json.dumps(model_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"v1_contract_frozen": True, "only_axis": "add signal-date breadth_above_ma20", "features_v1_count": len(v1.FEATURES), "features_v2_count": len(FEATURES), "model": "DecisionTreeRegressor max_depth2 min_samples_leaf50 random_state0", "candidate_generation_execution_top3_cost": "unchanged", "candidate_suppression": False, "splits": {"train": "2024", "validation": "2025", "shadow": "2026 untouched through ranking-history end"}, "shadow_tuning": False}, "coverage": {"ranking": ranking_coverage, "v1_features": v1_coverage, "added_feature": breadth_coverage}, "source_artifacts": [{"path": str(db_path), "sha256": v1.sha256(db_path)}], "frozen_model_v2": model_payload, "baseline_fixed_interleave": bm, "v1_challenger": m1, "v2_breadth_challenger": m2, "comparison_validation": {"v2_minus_baseline_daily_pf": m2["validation"]["daily_profit_factor"] - bm["validation"]["daily_profit_factor"], "v2_minus_v1_daily_pf": m2["validation"]["daily_profit_factor"] - m1["validation"]["daily_profit_factor"], "v2_minus_baseline_calendar_expectancy": m2["validation"]["calendar_expectancy"] - bm["validation"]["calendar_expectancy"], "v2_minus_v1_calendar_expectancy": m2["validation"]["calendar_expectancy"] - m1["validation"]["calendar_expectancy"]}, "branching_vs_baseline": branch_base, "branching_vs_v1": branch_v1, "validation_keep_gates": keep_gates, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "breadth_feature_not_selected_by_frozen_tree_and_v2_identical_to_v1" if v1_branch_rate == 0 else "single_added_point_in_time_breadth_feature_validation"}, "shadow_tuning_used": False, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    path = root / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
