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
from scripts.tradex_point_in_time_chart_shape_breadth_priority_top3_v2 import attach_breadth


AXIS_ID = "tradex_point_in_time_breadth_regime_shape_priority_top3_v1"
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_breadth_regime_shape_priority_top3_v1")
REGIMES = {"breadth_lt_0_40": (None, 0.40), "breadth_ge_0_40": (0.40, None)}
MIN_REGIME_TRAIN_N = 100


def regime_name(value: float) -> str:
    return "breadth_lt_0_40" if float(value) < 0.40 else "breadth_ge_0_40"


def fit_regime_models(frame: pd.DataFrame) -> tuple[dict[str, DecisionTreeRegressor], dict[str, dict[str, float]], dict[str, int]]:
    train = frame[frame.split == "train"].copy()
    train["breadth_regime"] = train.breadth_above_ma20.map(regime_name)
    counts = train.groupby("breadth_regime").size().to_dict()
    missing = {name: int(counts.get(name, 0)) for name in REGIMES if counts.get(name, 0) < MIN_REGIME_TRAIN_N}
    if missing:
        raise ValueError("INSUFFICIENT_REGIME_TRAIN_COVERAGE:" + json.dumps(missing, sort_keys=True))
    models, medians = {}, {}
    for name in REGIMES:
        part = train[train.breadth_regime == name]
        med = {feature: float(pd.to_numeric(part[feature], errors="coerce").median()) for feature in v1.FEATURES}
        model = DecisionTreeRegressor(max_depth=2, min_samples_leaf=50, random_state=0)
        model.fit(part[v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(med), part.side_return.astype(float))
        models[name], medians[name] = model, med
    return models, medians, {name: int(counts[name]) for name in REGIMES}


def score_select(frame: pd.DataFrame, models: dict, medians: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored = frame.copy()
    scored["breadth_regime"] = scored.breadth_above_ma20.map(regime_name)
    scored["regime_shape_score"] = 0.0
    for name, model in models.items():
        mask = scored.breadth_regime == name
        x = scored.loc[mask, v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(medians[name])
        scored.loc[mask, "regime_shape_score"] = model.predict(x)
    scored = scored.sort_values(["signal_ymd", "regime_shape_score", "rank", "code"], ascending=[True, False, True, True])
    selected = scored.groupby("signal_ymd", sort=True).head(3).copy()
    selected["global_rank"] = selected.groupby("signal_ymd").cumcount() + 1
    return scored, selected


def generate(db_path: Path, out_root: Path) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as con:
        calendar = [int(row[0]) for row in con.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1").fetchall()]
    events, ranking_coverage = build_corrected_baseline(db_path, calendar)
    base_features, v1_coverage = v1.attach_features(events, db_path)
    frame, breadth_coverage = attach_breadth(base_features, db_path)
    try:
        models, medians, regime_n = fit_regime_models(frame)
    except ValueError as exc:
        now = datetime.now(timezone.utc); root = out_root / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; root.mkdir(parents=True, exist_ok=False)
        payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"breadth_regimes": "<0.40 vs >=0.40", "features": v1.FEATURES, "model": "depth2 leaf50", "minimum_regime_train_n": MIN_REGIME_TRAIN_N, "silent_fallback": False}, "coverage": {"ranking": ranking_coverage, "v1_features": v1_coverage, "breadth": breadth_coverage}, "decision": {"candidate_local_decision": "blocked", "authoritative_rollup_decision": "review_only", "typed_reason": str(exc)}, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
        path = root / "compare.json"; path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); return path
    baseline = v1.fixed_baseline(frame)
    scored, challenger = score_select(frame, models, medians)
    ranking_end = int(ranking_coverage["ranking_history_end"])
    counts = {"train": sum(20240101 <= d <= 20241231 for d in calendar), "validation": sum(20250101 <= d <= 20251231 for d in calendar), "shadow": sum(20260101 <= d <= ranking_end for d in calendar)}
    bm = {s: metrics(baseline, s, counts[s]) for s in counts}; cm = {s: metrics(challenger, s, counts[s]) for s in counts}
    branch = branching(baseline, challenger); v, b = cm["validation"], bm["validation"]
    gates = {"daily_pf_ge_1_30": v["daily_profit_factor"] is not None and v["daily_profit_factor"] >= 1.30, "daily_pf_delta_ge_0_10": v["daily_profit_factor"] is not None and b["daily_profit_factor"] is not None and v["daily_profit_factor"] - b["daily_profit_factor"] >= 0.10, "calendar_expectancy_improves": v["calendar_expectancy"] is not None and v["calendar_expectancy"] > b["calendar_expectancy"], "frequency_ge_one_day_week": v["signals_per_week"] >= 1.0, "cvar_non_degrade": v["cvar10"] is not None and b["cvar10"] is not None and v["cvar10"] >= b["cvar10"] - 1e-12, "drawdown_non_degrade": v["max_drawdown_equal_weight"] is not None and b["max_drawdown_equal_weight"] is not None and v["max_drawdown_equal_weight"] >= b["max_drawdown_equal_weight"] - 1e-12, "branch_ge_20pct": (branch["summary"]["validation"]["changed_day_rate"] or 0) >= 0.20}
    decision = "keep_shadow_2026" if all(gates.values()) else "drop_no_meaningful_branching" if (branch["summary"]["validation"]["changed_day_rate"] or 0) < 0.20 else "drop_effectiveness" if (v["daily_profit_factor"] or 0) < 1.0 else "hold"
    model_docs = {name: v1.model_payload(models[name], medians[name]) for name in REGIMES}
    now = datetime.now(timezone.utc); root = out_root / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; root.mkdir(parents=True, exist_ok=False)
    baseline.to_csv(root / "baseline_fixed_interleave_top3.csv", index=False); challenger.to_csv(root / "challenger_breadth_regime_shape_top3.csv", index=False); scored[["signal_ymd", "code", "side", "rank", "split", "breadth_regime", "regime_shape_score"]].to_csv(root / "candidate_scores.csv", index=False)
    (root / "frozen_regime_models.json").write_text(json.dumps(model_docs, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"only_axis": "predeclared signal-date breadth_above_ma20 <0.40 vs >=0.40 regime-specific v1 trees", "features": v1.FEATURES, "model_each_regime": "DecisionTreeRegressor max_depth2 min_samples_leaf50 random_state0", "minimum_regime_train_n": MIN_REGIME_TRAIN_N, "candidate_generation_entry_top3_cost_baseline_splits": "unchanged", "candidate_suppression": False, "shadow_tuning": False}, "coverage": {"ranking": ranking_coverage, "features": v1_coverage, "breadth": breadth_coverage, "regime_train_n": regime_n}, "frozen_regime_models": model_docs, "baseline_fixed_interleave": bm, "challenger_breadth_regime_shape": cm, "branching": branch, "validation_keep_gates": gates, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "single_axis_predeclared_breadth_regime_shape_validation"}, "silent_fallback_used": False, "shadow_tuning_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    path = root / "compare.json"; path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); return path


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--db", type=Path, default=DEFAULT_DB); parser.add_argument("--out", type=Path, default=DEFAULT_OUT); args = parser.parse_args(); print(generate(args.db, args.out))


if __name__ == "__main__": main()
