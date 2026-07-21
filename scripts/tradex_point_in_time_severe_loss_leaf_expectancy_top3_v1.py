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
from scripts import tradex_point_in_time_chart_shape_priority_top3_v1 as regression_v1
from scripts import tradex_point_in_time_severe_loss_classifier_top3_v1 as loss_v1


AXIS_ID = "tradex_point_in_time_severe_loss_leaf_expectancy_top3_v1"
DEFAULT_OUT = Path(r"G:\Tradex\point_in_time_severe_loss_leaf_expectancy_top3_v1")


def select(frame: pd.DataFrame, loss_model, loss_medians: dict, regression_model, regression_medians: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    scored = frame.copy()
    loss_x = scored[regression_v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(loss_medians)
    regression_x = scored[regression_v1.FEATURES].apply(pd.to_numeric, errors="coerce").fillna(regression_medians)
    scored["severe_loss_probability"] = loss_v1.loss_probability(loss_model, loss_x)
    scored["frozen_expectancy_score"] = regression_model.predict(regression_x)
    scored["baseline_order"] = scored["rank"].astype(int) * 2 - scored.side.eq("buy").astype(int)
    scored = scored.sort_values(
        ["signal_ymd", "severe_loss_probability", "frozen_expectancy_score", "baseline_order", "code"],
        ascending=[True, True, False, True, True],
    )
    selected = scored.groupby("signal_ymd", sort=True).head(3).copy()
    selected["global_rank"] = selected.groupby("signal_ymd").cumcount() + 1
    return scored, selected


def leaf_branching(classifier: pd.DataFrame, challenger: pd.DataFrame, scored: pd.DataFrame) -> dict:
    key = ["signal_ymd", "code", "side"]
    cset = set(map(tuple, classifier[key].to_records(index=False)))
    hset = set(map(tuple, challenger[key].to_records(index=False)))
    rows = []
    for (split, probability), part in scored.groupby(["split", "severe_loss_probability"], sort=True):
        keys = set(map(tuple, part[key].to_records(index=False)))
        rows.append({"split": str(split), "severe_loss_probability": float(probability), "candidate_n": int(len(part)), "classifier_selected_n": int(len(keys & cset)), "hierarchical_selected_n": int(len(keys & hset)), "added_n": int(len((keys & hset) - cset)), "removed_n": int(len((keys & cset) - hset))})
    return {"leaf_rows": rows, "overall": branching(classifier, challenger)}


def generate(db_path: Path, out_root: Path) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as con:
        calendar = [int(row[0]) for row in con.execute("select distinct cast(strftime(to_timestamp(date),'%Y%m%d') as int) from daily_bars where source='pan' order by 1").fetchall()]
    events, ranking_coverage = build_corrected_baseline(db_path, calendar)
    frame, feature_coverage = regression_v1.attach_features(events, db_path)
    loss_model, loss_medians = loss_v1.fit_model(frame)
    regression_model, regression_medians = regression_v1.fit_train_model(frame)
    _, classifier = loss_v1.select(frame, loss_model, loss_medians)
    scored, challenger = select(frame, loss_model, loss_medians, regression_model, regression_medians)
    baseline = regression_v1.fixed_baseline(frame)
    end = int(ranking_coverage["ranking_history_end"]); counts = {"train": sum(20240101 <= d <= 20241231 for d in calendar), "validation": sum(20250101 <= d <= 20251231 for d in calendar), "shadow": sum(20260101 <= d <= end for d in calendar)}
    bm = {s: metrics(baseline, s, counts[s]) for s in counts}; lm = {s: metrics(classifier, s, counts[s]) for s in counts}; cm = {s: metrics(challenger, s, counts[s]) for s in counts}
    branch_baseline = branching(baseline, challenger); branch_classifier = leaf_branching(classifier, challenger, scored); v, b = cm["validation"], bm["validation"]
    gates = {"daily_pf_ge_1_30": v["daily_profit_factor"] is not None and v["daily_profit_factor"] >= 1.30, "daily_pf_delta_ge_0_10": v["daily_profit_factor"] is not None and b["daily_profit_factor"] is not None and v["daily_profit_factor"] - b["daily_profit_factor"] >= .10, "calendar_expectancy_improves": v["calendar_expectancy"] is not None and v["calendar_expectancy"] > b["calendar_expectancy"], "frequency_ge_one_day_week": v["signals_per_week"] >= 1, "cvar_non_degrade": v["cvar10"] is not None and v["cvar10"] >= b["cvar10"] - 1e-12, "drawdown_non_degrade": v["max_drawdown_equal_weight"] is not None and v["max_drawdown_equal_weight"] >= b["max_drawdown_equal_weight"] - 1e-12, "branch_ge_20pct": (branch_baseline["summary"]["validation"]["changed_day_rate"] or 0) >= .20}
    decision = "keep_shadow_2026" if all(gates.values()) else "drop_no_meaningful_branching" if (branch_baseline["summary"]["validation"]["changed_day_rate"] or 0) < .20 else "drop_effectiveness" if (v["daily_profit_factor"] or 0) <= (b["daily_profit_factor"] or 0) or not gates["calendar_expectancy_improves"] else "hold"
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; root.mkdir(parents=True, exist_ok=False)
    loss_doc = loss_v1.model_payload(loss_model, loss_medians); regression_doc = regression_v1.model_payload(regression_model, regression_medians)
    baseline.to_csv(root / "baseline_fixed_interleave_top3.csv", index=False); classifier.to_csv(root / "classifier_v1_top3.csv", index=False); challenger.to_csv(root / "challenger_leaf_expectancy_top3.csv", index=False); scored[["signal_ymd", "code", "side", "rank", "split", "severe_loss_probability", "frozen_expectancy_score", "baseline_order"]].to_csv(root / "candidate_scores.csv", index=False)
    (root / "frozen_models.json").write_text(json.dumps({"severe_loss_classifier": loss_doc, "expectancy_regression": regression_doc}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID, "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"only_axis": "within identical severe-loss probability leaf use frozen regression expectancy descending", "primary_key": "frozen severe-loss probability ascending", "secondary_key": "frozen regression expectancy descending only within identical probability", "final_tie": "fixed interleave order", "models": "existing deterministic 2024 train, 16 features, depth2, minleaf50, random_state0", "model_or_weight_search": False, "candidate_generation_execution_splits": "unchanged", "candidate_suppression": False, "splits": {"validation": "2025", "shadow": "2026 untouched"}}, "coverage": {"ranking": ranking_coverage, "features": feature_coverage}, "frozen_models": {"severe_loss_classifier": loss_doc, "expectancy_regression": regression_doc}, "baseline_fixed_interleave": bm, "classifier_v1": lm, "challenger_leaf_expectancy": cm, "branching": {"versus_baseline": branch_baseline, "within_classifier_probability_leaves": branch_classifier}, "validation_keep_gates": gates, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "single_axis_within_loss_leaf_expectancy_tiebreak_validation"}, "model_search_used": False, "weight_search_used": False, "shadow_tuning_used": False, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    path = root / "compare.json"; path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); return path


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--db", type=Path, default=DEFAULT_DB); parser.add_argument("--out", type=Path, default=DEFAULT_OUT); args = parser.parse_args(); print(generate(args.db, args.out))


if __name__ == "__main__": main()
