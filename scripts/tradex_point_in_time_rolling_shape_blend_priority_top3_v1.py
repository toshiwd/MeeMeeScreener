from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.tradex_point_in_time_side_permission_router_v1 import DEFAULT_DB, build_corrected_baseline, metrics
from scripts.tradex_point_in_time_side_priority_top3_v1 import branching
from scripts import tradex_point_in_time_chart_shape_priority_top3_v1 as shape_v1
from scripts import tradex_point_in_time_rolling_shape_priority_top3_v1 as rolling_v1


AXIS_ID = "tradex_point_in_time_rolling_shape_blend_priority_top3_v1"
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_rolling_shape_blend_priority_top3_v1")


def select(frame: pd.DataFrame, models: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    evaluation = frame[frame.signal_ymd >= 20250101].copy()
    evaluation["month"] = evaluation.signal_ymd.astype(str).str[:6]
    parts = []
    for month, part in evaluation.groupby("month", sort=True):
        spec = models[month]
        x = part[shape_v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(spec["medians"])
        scored = part.copy()
        scored["rolling_shape_score"] = spec["model"].predict(x)
        # Fixed-interleave baseline order is BUY1, SELL1, BUY2, SELL2, BUY3, SELL3.
        scored["baseline_order"] = scored["rank"].astype(int) * 2 - (scored["side"].eq("buy").astype(int))
        scored["baseline_priority_percentile"] = scored.groupby("signal_ymd")["baseline_order"].rank(method="first", ascending=False, pct=True)
        scored["rolling_score_percentile"] = scored.groupby("signal_ymd")["rolling_shape_score"].rank(method="average", ascending=True, pct=True)
        scored["blend_priority_score"] = (scored["baseline_priority_percentile"] + scored["rolling_score_percentile"]) / 2.0
        parts.append(scored)
    scored = pd.concat(parts, ignore_index=True).sort_values(
        ["signal_ymd", "blend_priority_score", "rank", "code"], ascending=[True, False, True, True]
    )
    selected = scored.groupby("signal_ymd", sort=True).head(3).copy()
    selected["global_rank"] = selected.groupby("signal_ymd").cumcount() + 1
    return scored, selected


def generate(db_path: Path, out_root: Path) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as con:
        calendar = [int(row[0]) for row in con.execute(
            "select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1"
        ).fetchall()]
    events, ranking_coverage = build_corrected_baseline(db_path, calendar)
    frame, feature_coverage = shape_v1.attach_features(events, db_path)
    models, blocked = rolling_v1.monthly_models(frame, calendar)
    now = datetime.now(timezone.utc)
    root = out_root / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    model_manifest = {month: {k: v for k, v in spec.items() if k not in ("model", "medians")} | {"medians": spec["medians"]} for month, spec in models.items()}
    fixed = {"features": shape_v1.FEATURES, "model": "DecisionTreeRegressor max_depth2 min_samples_leaf50 random_state0", "lookback_pan_sessions": rolling_v1.LOOKBACK_SESSIONS, "minimum_train_n": rolling_v1.MIN_TRAIN_N, "refit": "first signal date each month", "only_axis": "priority fixed 50:50 percentile blend", "baseline_priority": "fixed interleave BUY1 SELL1 BUY2 SELL2 BUY3 SELL3 percentile within signal-date candidate pool", "rolling_priority": "rolling predicted score percentile within signal-date candidate pool", "blend": "simple mean, no weight search", "candidate_suppression": False, "fallback": False, "execution_changed": False, "splits": {"validation": "2025", "shadow": "2026 untouched"}}
    if blocked:
        payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": fixed, "coverage": {"ranking": ranking_coverage, "features": feature_coverage, "blocked_months": blocked}, "monthly_models": model_manifest, "decision": {"candidate_local_decision": "blocked", "authoritative_rollup_decision": "review_only", "typed_reason": "INSUFFICIENT_ROLLING_TRAIN_COVERAGE"}, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
        path = root / "compare.json"; path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); return path
    baseline = shape_v1.fixed_baseline(frame)
    rolling_scored, rolling_selected = rolling_v1.select(frame, models)
    scored, challenger = select(frame, models)
    end = int(ranking_coverage["ranking_history_end"])
    counts = {"validation": sum(20250101 <= day <= 20251231 for day in calendar), "shadow": sum(20260101 <= day <= end for day in calendar)}
    baseline_eval = baseline[baseline.signal_ymd >= 20250101]
    bm = {split: metrics(baseline_eval, split, counts[split]) for split in counts}
    rm = {split: metrics(rolling_selected, split, counts[split]) for split in counts}
    cm = {split: metrics(challenger, split, counts[split]) for split in counts}
    branch_baseline = branching(baseline_eval, challenger)
    branch_rolling = branching(rolling_selected, challenger)
    v, b = cm["validation"], bm["validation"]
    gates = {"daily_pf_ge_1_30": v["daily_profit_factor"] is not None and v["daily_profit_factor"] >= 1.30, "daily_pf_delta_ge_0_10": v["daily_profit_factor"] is not None and b["daily_profit_factor"] is not None and v["daily_profit_factor"] - b["daily_profit_factor"] >= .10, "calendar_expectancy_improves": v["calendar_expectancy"] is not None and v["calendar_expectancy"] > b["calendar_expectancy"], "frequency_ge_one_day_week": v["signals_per_week"] >= 1, "cvar_non_degrade": v["cvar10"] is not None and v["cvar10"] >= b["cvar10"] - 1e-12, "drawdown_non_degrade": v["max_drawdown_equal_weight"] is not None and v["max_drawdown_equal_weight"] >= b["max_drawdown_equal_weight"] - 1e-12, "branch_ge_20pct": (branch_baseline["summary"]["validation"]["changed_day_rate"] or 0) >= .20}
    decision = "keep_shadow_2026" if all(gates.values()) else "drop_no_meaningful_branching" if (branch_baseline["summary"]["validation"]["changed_day_rate"] or 0) < .20 else "drop_effectiveness" if (v["daily_profit_factor"] or 0) <= (b["daily_profit_factor"] or 0) or not gates["calendar_expectancy_improves"] else "hold"
    baseline_eval.to_csv(root / "baseline_fixed_interleave_top3.csv", index=False)
    rolling_selected.to_csv(root / "rolling_shape_top3.csv", index=False)
    challenger.to_csv(root / "challenger_blend_top3.csv", index=False)
    scored[["signal_ymd", "code", "side", "rank", "split", "month", "baseline_order", "baseline_priority_percentile", "rolling_shape_score", "rolling_score_percentile", "blend_priority_score"]].to_csv(root / "candidate_scores.csv", index=False)
    (root / "monthly_models.json").write_text(json.dumps(model_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": fixed, "coverage": {"ranking": ranking_coverage, "features": feature_coverage, "blocked_months": []}, "monthly_models": model_manifest, "baseline_fixed_interleave": bm, "rolling_shape_v1": rm, "challenger_blend": cm, "branching": {"versus_baseline": branch_baseline, "versus_rolling_shape_v1": branch_rolling}, "validation_keep_gates": gates, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "single_axis_fixed_50_50_priority_blend_validation"}, "shadow_tuning_used": False, "weight_search_used": False, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    path = root / "compare.json"; path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); return path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--db", type=Path, default=DEFAULT_DB); parser.add_argument("--out", type=Path, default=DEFAULT_OUT); args = parser.parse_args(); print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
