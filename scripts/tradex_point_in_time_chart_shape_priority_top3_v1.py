from __future__ import annotations

import argparse
import hashlib
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
from scripts.tradex_point_in_time_side_priority_top3_v1 import _interleave, branching


AXIS_ID = "tradex_point_in_time_chart_shape_priority_top3_v1"
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_chart_shape_priority_top3_v1")
FEATURES = [
    "side_code", "close_vs_ma7", "close_vs_ma20", "close_vs_ma60",
    "ma7_slope1_pct", "ma20_slope1_pct", "ma60_slope1_pct",
    "candle_body_ratio", "candle_upper_wick_ratio", "candle_lower_wick_ratio",
    "close_ret2", "close_ret3", "gap_pct", "vol_ratio5_20", "atr14_pct", "range_pct",
]
RAW_COLUMNS = [
    "dt", "code", "close", "ma7", "ma20", "ma60", "ma7_prev1", "ma20_prev1", "ma60_prev1",
    "candle_body_ratio", "candle_upper_wick_ratio", "candle_lower_wick_ratio", "close_ret2", "close_ret3",
    "gap_pct", "vol_ratio5_20", "atr14_pct", "range_pct",
]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def attach_features(events: pd.DataFrame, db_path: Path) -> tuple[pd.DataFrame, dict]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        available = {row[0] for row in con.execute("describe ml_feature_daily").fetchall()}
        missing = sorted(set(RAW_COLUMNS) - available)
        if missing:
            raise ValueError("FEATURE_COVERAGE_MISSING:" + ",".join(missing))
        select_cols = ",".join(column for column in RAW_COLUMNS if column != "dt")
        raw = con.execute(
            f"select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) dt,{select_cols} "
            "from ml_feature_daily where dt between epoch(strptime('20240101','%Y%m%d')) and epoch(strptime('20261231','%Y%m%d'))"
        ).fetchdf()
    raw["code"] = raw.code.astype(str)
    frame = events.copy()
    frame["code"] = frame.code.astype(str)
    frame = frame.merge(raw, left_on=["signal_ymd", "code"], right_on=["dt", "code"], how="left", validate="many_to_one")
    frame["side_code"] = frame.side.map({"buy": 1.0, "sell": -1.0})
    for ma in (7, 20, 60):
        frame[f"close_vs_ma{ma}"] = frame["close"] / frame[f"ma{ma}"] - 1.0
        frame[f"ma{ma}_slope1_pct"] = frame[f"ma{ma}"] / frame[f"ma{ma}_prev1"] - 1.0
    coverage = {feature: float(frame[feature].notna().mean()) for feature in FEATURES}
    missing_rows = int(frame[FEATURES].isna().all(axis=1).sum())
    if missing_rows:
        raise ValueError(f"FEATURE_COVERAGE_MISSING:all_features_null_rows={missing_rows}")
    return frame, {"feature_row_match_rate": float(frame.dt.notna().mean()), "feature_non_null_rate": coverage, "all_feature_null_rows": missing_rows}


def fit_train_model(frame: pd.DataFrame) -> tuple[DecisionTreeRegressor, dict[str, float]]:
    train = frame[frame.split == "train"]
    if len(train) < 100:
        raise ValueError(f"TRAIN_FEATURE_COVERAGE_INSUFFICIENT:n={len(train)}")
    medians = {feature: float(pd.to_numeric(train[feature], errors="coerce").median()) for feature in FEATURES}
    if any(np.isnan(value) for value in medians.values()):
        raise ValueError("TRAIN_MEDIAN_MISSING")
    x = train[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians)
    model = DecisionTreeRegressor(max_depth=2, min_samples_leaf=50, random_state=0)
    model.fit(x, train.side_return.astype(float))
    return model, medians


def select(frame: pd.DataFrame, model: DecisionTreeRegressor, medians: dict[str, float]) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored = frame.copy()
    x = scored[FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians)
    scored["shape_priority_score"] = model.predict(x)
    scored = scored.sort_values(["signal_ymd", "shape_priority_score", "rank", "code"], ascending=[True, False, True, True])
    selected = scored.groupby("signal_ymd", sort=True).head(3).copy()
    selected["global_rank"] = selected.groupby("signal_ymd").cumcount() + 1
    return scored, selected


def fixed_baseline(frame: pd.DataFrame) -> pd.DataFrame:
    parts = [_interleave(day, "buy") for _, day in frame.groupby("signal_ymd", sort=True)]
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def model_payload(model: DecisionTreeRegressor, medians: dict[str, float]) -> dict:
    tree = model.tree_
    nodes = []
    for idx in range(tree.node_count):
        nodes.append({"node": idx, "feature": FEATURES[tree.feature[idx]] if tree.feature[idx] >= 0 else None, "threshold": float(tree.threshold[idx]) if tree.feature[idx] >= 0 else None, "left": int(tree.children_left[idx]), "right": int(tree.children_right[idx]), "samples": int(tree.n_node_samples[idx]), "value": float(tree.value[idx].reshape(-1)[0])})
    return {"estimator": "sklearn.tree.DecisionTreeRegressor", "parameters": {"max_depth": 2, "min_samples_leaf": 50, "random_state": 0}, "features": FEATURES, "train_medians": medians, "nodes": nodes, "text": export_text(model, feature_names=FEATURES)}


def generate(db_path: Path, out_root: Path) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as con:
        calendar = [int(row[0]) for row in con.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1").fetchall()]
    events, source_coverage = build_corrected_baseline(db_path, calendar)
    frame, feature_coverage = attach_features(events, db_path)
    model, medians = fit_train_model(frame)
    scored, challenger = select(frame, model, medians)
    baseline = fixed_baseline(frame)
    ranking_end = int(source_coverage["ranking_history_end"])
    calendar_counts = {"train": sum(20240101 <= d <= 20241231 for d in calendar), "validation": sum(20250101 <= d <= 20251231 for d in calendar), "shadow": sum(20260101 <= d <= ranking_end for d in calendar)}
    bm = {split: metrics(baseline, split, calendar_counts[split]) for split in calendar_counts}
    cm = {split: metrics(challenger, split, calendar_counts[split]) for split in calendar_counts}
    branch = branching(baseline, challenger)
    v, b = cm["validation"], bm["validation"]
    gates = {"daily_pf_ge_1_30": v["daily_profit_factor"] is not None and v["daily_profit_factor"] >= 1.30, "daily_pf_delta_ge_0_10": v["daily_profit_factor"] is not None and b["daily_profit_factor"] is not None and v["daily_profit_factor"] - b["daily_profit_factor"] >= 0.10, "calendar_expectancy_improves": v["calendar_expectancy"] is not None and v["calendar_expectancy"] > b["calendar_expectancy"], "frequency_ge_one_day_week": v["signals_per_week"] >= 1.0, "cvar_non_degrade": v["cvar10"] is not None and b["cvar10"] is not None and v["cvar10"] >= b["cvar10"] - 1e-12, "drawdown_non_degrade": v["max_drawdown_equal_weight"] is not None and b["max_drawdown_equal_weight"] is not None and v["max_drawdown_equal_weight"] >= b["max_drawdown_equal_weight"] - 1e-12, "branch_ge_20pct": (branch["summary"]["validation"]["changed_day_rate"] or 0) >= 0.20}
    if all(gates.values()):
        decision = "keep_shadow_2026"
    elif (branch["summary"]["validation"]["changed_day_rate"] or 0) < 0.20:
        decision = "drop_no_meaningful_branching"
    elif v["daily_profit_factor"] is not None and v["daily_profit_factor"] < 1.0:
        decision = "drop_effectiveness"
    else:
        decision = "hold"
    now = datetime.now(timezone.utc)
    root = out_root / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    baseline.to_csv(root / "baseline_fixed_interleave_top3.csv", index=False)
    challenger.to_csv(root / "challenger_shape_priority_top3.csv", index=False)
    scored[["signal_ymd", "code", "side", "rank", "split", "shape_priority_score"]].to_csv(root / "candidate_scores.csv", index=False)
    model_doc = model_payload(model, medians)
    (root / "frozen_model.json").write_text(json.dumps(model_doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"candidate_pool": "corrected MeeMee BUY/SELL each execution-eligible rank-asc top3; no new candidates", "baseline": "fixed interleave buy1,sell1,buy2 then unified top3", "challenger": "single 2024-trained depth2 tree score descending then rank/code", "target": "frozen next-open TP8/SL5/H10/10bp side return", "candidate_suppression": False, "execution_changed": False, "splits": {"train": "2024 only", "validation": "2025", "shadow": "2026 through ranking-history end untouched"}, "forbidden_features": ["future outcomes", "prediction probabilities", "symbol ID", "year", "month", "side health"]}, "coverage": {"ranking": source_coverage, "features": feature_coverage}, "source_artifacts": [{"path": str(db_path), "sha256": sha256(db_path)}], "frozen_model": model_doc, "baseline_fixed_interleave": bm, "challenger_shape_priority": cm, "branching": {**branch, "top5_top10": {"changed_top5_members_count": None, "changed_top10_members_count": None, "typed_reason": "candidate pool is maximum six and selected output is unified top3; no comparable global top5/top10 baseline"}}, "validation_keep_gates": gates, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "single_axis_train_only_chart_shape_priority_validation"}, "shadow_tuning_used": False, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    path = root / "compare.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
