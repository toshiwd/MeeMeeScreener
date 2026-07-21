from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_nikkei225_daily_assessment_baseline_v1 import FEATURES, HORIZONS, _labels


AXIS_ID = "tradex_nikkei225_train_only_sparse_interaction_v1"
TARGETS = {"SELL": (0, 1), "REBOUND_RISK": (1, 0)}
SELL_MEAN_GATES = {1: 0.0, 3: -0.005, 5: -0.010, 10: -0.015}
SELL_PRECISION_GATES = {1: .58, 3: .60, 5: .60, 10: .60}
SELL_REBOUND_GATES = {1: .25, 3: .22, 5: .20, 10: .20}
REBOUND_PRECISION_GATES = {1: .55, 3: .58, 5: .60, 10: .60}
REBOUND_DOWN_GATES = {1: .20, 3: .18, 5: .15, 10: .15}
TREE_VARIANTS = (
    (2, 200, "gini", None), (3, 200, "gini", None),
    (2, 400, "gini", None), (3, 400, "gini", None),
    (2, 200, "entropy", None), (3, 200, "entropy", None),
    (2, 200, "gini", "balanced"), (3, 200, "gini", "balanced"),
)


def _leaf_paths(model: DecisionTreeClassifier) -> dict[int, list[dict[str, Any]]]:
    tree = model.tree_
    paths: dict[int, list[dict[str, Any]]] = {}

    def walk(node: int, path: list[dict[str, Any]]) -> None:
        if tree.feature[node] < 0:
            paths[node] = path
            return
        feature = FEATURES[tree.feature[node]]
        threshold = float(tree.threshold[node])
        walk(tree.children_left[node], path + [{"feature": feature, "operator": "<=", "threshold": threshold}])
        walk(tree.children_right[node], path + [{"feature": feature, "operator": ">", "threshold": threshold}])

    walk(0, [])
    return paths


def _rule_mask(frame: pd.DataFrame, conditions: list[dict[str, Any]]) -> np.ndarray:
    mask = np.ones(len(frame), dtype=bool)
    for condition in conditions:
        values = frame[condition["feature"]].to_numpy()
        if condition["operator"] == "<=":
            mask &= values <= condition["threshold"]
        else:
            mask &= values > condition["threshold"]
    return mask


def _event_mask(frame: pd.DataFrame, raw_mask: np.ndarray, cooldown: int = 10) -> np.ndarray:
    event = np.zeros(len(frame), dtype=bool)
    for _, indices in frame.groupby("code", sort=False).indices.items():
        ordered = np.asarray(indices, dtype=int)
        previous = False
        last_event_position = -10_000
        for position, row_index in enumerate(ordered):
            current = bool(raw_mask[row_index])
            if current and not previous and position - last_event_position > cooldown:
                event[row_index] = True
                last_event_position = position
            previous = current
    return event


def _rates(frame: pd.DataFrame, labels: np.ndarray, target_class: int, opposite_class: int) -> dict[str, Any]:
    if len(frame) == 0:
        return {"n": 0, "codes": 0, "months": 0, "target_rate": None, "opposite_rate": None, "mean_return": None, "max_code": None, "max_month": None}
    months = frame["ymd"].astype(str).str[:6]
    return {
        "n": int(len(frame)),
        "codes": int(frame["code"].nunique()),
        "months": int(frame["ymd"].astype(str).str[:6].nunique()),
        "target_rate": float((labels == target_class).mean()),
        "opposite_rate": float((labels == opposite_class).mean()),
        "mean_return": float(frame["_target_return"].mean()),
        "max_code": float(frame.groupby("code").size().max() / len(frame)),
        "max_month": float(frame.assign(_month=months).groupby("_month").size().max() / len(frame)),
    }


def _cluster_bootstrap(
    frame: pd.DataFrame,
    labels: np.ndarray,
    selected: np.ndarray,
    target_class: int,
    opposite_class: int,
    cluster: str,
    iterations: int,
    seed: int,
) -> dict[str, Any]:
    work = frame[["code", "ymd"]].copy()
    work["target"] = (labels == target_class).astype(int)
    work["opposite"] = (labels == opposite_class).astype(int)
    work["selected"] = selected.astype(int)
    work["selected_target"] = work["target"] * work["selected"]
    work["selected_opposite"] = work["opposite"] * work["selected"]
    work["return"] = frame["_target_return"].to_numpy()
    work["selected_return"] = work["return"] * work["selected"]
    work["cluster"] = work["code"].astype(str) if cluster == "code" else work["ymd"].astype(str).str[:6]
    grouped = work.groupby("cluster", sort=False).agg(
        n=("target", "size"), target=("target", "sum"), opposite=("opposite", "sum"),
        selected_n=("selected", "sum"), selected_target=("selected_target", "sum"),
        selected_opposite=("selected_opposite", "sum"),
        returns=("return", "sum"), selected_returns=("selected_return", "sum"),
    )
    values = grouped.to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    target_delta: list[float] = []
    opposite_reduction: list[float] = []
    mean_return_delta: list[float] = []
    for _ in range(iterations):
        sampled = values[rng.integers(0, len(values), len(values))].sum(axis=0)
        n, target, opposite, selected_n, selected_target, selected_opposite, returns, selected_returns = sampled
        if n <= 0 or selected_n <= 0:
            continue
        target_delta.append(selected_target / selected_n - target / n)
        opposite_reduction.append(opposite / n - selected_opposite / selected_n)
        mean_return_delta.append(selected_returns / selected_n)
    target_values = np.asarray(target_delta)
    opposite_values = np.asarray(opposite_reduction)
    return_values = np.asarray(mean_return_delta)
    return {
        "cluster": cluster,
        "iterations": int(len(target_values)),
        "target_uplift_ci95": [float(np.quantile(target_values, .025)), float(np.quantile(target_values, .975))],
        "opposite_reduction_ci95": [float(np.quantile(opposite_values, .025)), float(np.quantile(opposite_values, .975))],
        "target_uplift_p_one_sided": float((np.sum(target_values <= 0) + 1) / (len(target_values) + 1)),
        "opposite_reduction_p_one_sided": float((np.sum(opposite_values <= 0) + 1) / (len(opposite_values) + 1)),
        "target_direction_rate": float((target_values > 0).mean()),
        "opposite_guardrail_direction_rate": float((opposite_values > 0).mean()),
        "selected_mean_return_ci95": [float(np.quantile(return_values, .025)), float(np.quantile(return_values, .975))],
        "mean_return_delta_p_nonnegative": float((np.sum(return_values >= 0) + 1) / (len(return_values) + 1)),
    }


def _holm(p_values: dict[str, float]) -> dict[str, float]:
    ordered = sorted(p_values.items(), key=lambda item: item[1])
    adjusted: dict[str, float] = {}
    running = 0.0
    count = len(ordered)
    for rank, (key, value) in enumerate(ordered):
        running = max(running, min(1.0, (count - rank) * value))
        adjusted[key] = running
    return adjusted


def _candidate_metrics(
    frame: pd.DataFrame,
    labels: np.ndarray,
    event: np.ndarray,
    target_class: int,
    opposite_class: int,
) -> dict[str, Any]:
    baseline = _rates(frame, labels, target_class, opposite_class)
    selected = _rates(frame.loc[event], labels[event], target_class, opposite_class)
    selected["target_uplift"] = (
        selected["target_rate"] - baseline["target_rate"] if selected["target_rate"] is not None else None
    )
    selected["opposite_reduction"] = (
        baseline["opposite_rate"] - selected["opposite_rate"] if selected["opposite_rate"] is not None else None
    )
    selected["mean_return_delta"] = selected["mean_return"] - baseline["mean_return"] if selected["mean_return"] is not None else None
    selected["coverage"] = len(frame.loc[event]) / len(frame) if len(frame) else 0.0
    return {"baseline": baseline, "selected": selected}


def run(input_parquet: Path, output_root: Path) -> Path:
    frame = pd.read_parquet(input_parquet).sort_values(["code", "ymd"]).reset_index(drop=True)
    usable = frame.dropna(subset=FEATURES).copy().reset_index(drop=True)
    periods = {
        "train_2019_2021": (20190101, 20211231),
        "calibration_2022": (20220101, 20221231),
        "fixed_evaluation_2023_2025": (20230101, 20251231),
        "exploratory_2026": (20260101, 20260713),
    }
    discoveries: dict[str, Any] = {}
    calibrated: list[dict[str, Any]] = []
    assignment_parts: list[pd.DataFrame] = []

    for horizon in HORIZONS:
        required = [f"ret_close_{horizon}", f"down_exc_{horizon}", f"up_exc_{horizon}"]
        horizon_frame = usable.dropna(subset=required).copy().reset_index(drop=True)
        horizon_frame["_target_return"] = horizon_frame[f"ret_close_{horizon}"]
        labels = _labels(horizon_frame, horizon)
        train_mask = horizon_frame["ymd"].between(*periods["train_2019_2021"]).to_numpy()
        train = horizon_frame.loc[train_mask].reset_index(drop=True)
        train_labels = labels[train_mask]
        for target_name, (target_class, opposite_class) in TARGETS.items():
            raw_by_expression: dict[str, dict[str, Any]] = {}
            event_cache: dict[str, np.ndarray] = {}
            binary = (train_labels == target_class).astype(int)
            for variant_index, (depth, min_leaf, criterion, class_weight) in enumerate(TREE_VARIANTS):
                model = DecisionTreeClassifier(
                    max_depth=depth, min_samples_leaf=min_leaf, criterion=criterion,
                    class_weight=class_weight, random_state=20260714 + variant_index,
                )
                model.fit(train[FEATURES], binary)
                paths = _leaf_paths(model)
                leaves = model.apply(train[FEATURES])
                for leaf, conditions in paths.items():
                    if not 1 <= len(conditions) <= 3:
                        continue
                    expression = " AND ".join(
                        f"{item['feature']} {item['operator']} {item['threshold']:.12g}" for item in conditions
                    )
                    if expression in raw_by_expression:
                        continue
                    raw = leaves == leaf
                    event = _event_mask(train, raw)
                    metric = _candidate_metrics(train, train_labels, event, target_class, opposite_class)
                    selected = metric["selected"]
                    if (
                        selected["n"] < 200 or selected["codes"] < 80 or selected["months"] < 18
                        or not .005 <= selected["coverage"] <= .20
                        or selected["max_code"] > .05 or selected["max_month"] > .15
                    ):
                        continue
                    score = float((selected["target_uplift"] or 0) + (selected["opposite_reduction"] or 0))
                    candidate_id = hashlib.sha256(f"{target_name}|{horizon}|{expression}".encode()).hexdigest()[:16]
                    raw_by_expression[expression] = {
                        "candidate_id": candidate_id, "target": target_name, "horizon": horizon,
                        "conditions": conditions, "expression": expression, "train_metrics": metric,
                        "train_score": score,
                    }
            raw20 = sorted(raw_by_expression.values(), key=lambda item: (-item["train_score"], item["candidate_id"]))[:20]
            stable: list[dict[str, Any]] = []
            for candidate in raw20:
                raw = _rule_mask(train, candidate["conditions"])
                event = _event_mask(train, raw)
                code_boot = _cluster_bootstrap(train, train_labels, event, target_class, opposite_class, "code", 100, int(candidate["candidate_id"][:8], 16))
                month_boot = _cluster_bootstrap(train, train_labels, event, target_class, opposite_class, "month", 100, int(candidate["candidate_id"][8:], 16))
                candidate["train_bootstrap"] = {"code": code_boot, "month": month_boot}
                candidate["train_stability_pass"] = bool(
                    code_boot["target_direction_rate"] >= .70
                    and code_boot["opposite_guardrail_direction_rate"] >= .70
                )
                if candidate["train_stability_pass"]:
                    event_cache[candidate["candidate_id"]] = event
                    stable.append(candidate)
            deduplicated: list[dict[str, Any]] = []
            for candidate in sorted(stable, key=lambda item: (len(item["conditions"]), -item["train_score"], item["candidate_id"])):
                event = event_cache[candidate["candidate_id"]]
                redundant = False
                for kept in deduplicated:
                    other = event_cache[kept["candidate_id"]]
                    union = int((event | other).sum())
                    jaccard = float((event & other).sum() / union) if union else 1.0
                    if jaccard >= .85:
                        redundant = True
                        break
                if not redundant:
                    deduplicated.append(candidate)
            stable5 = sorted(deduplicated, key=lambda item: (-item["train_score"], item["candidate_id"]))[:5]

            calibration_mask = horizon_frame["ymd"].between(*periods["calibration_2022"]).to_numpy()
            calibration = horizon_frame.loc[calibration_mask].reset_index(drop=True)
            calibration_labels = labels[calibration_mask]
            calibration_results: list[dict[str, Any]] = []
            p_values: dict[str, float] = {}
            for train_rank, candidate in enumerate(stable5, start=1):
                raw = _rule_mask(calibration, candidate["conditions"])
                event = _event_mask(calibration, raw)
                metric = _candidate_metrics(calibration, calibration_labels, event, target_class, opposite_class)
                code_boot = _cluster_bootstrap(calibration, calibration_labels, event, target_class, opposite_class, "code", 2000, int(candidate["candidate_id"][:8], 16) + 22)
                month_boot = _cluster_bootstrap(calibration, calibration_labels, event, target_class, opposite_class, "month", 2000, int(candidate["candidate_id"][8:], 16) + 22)
                combined_p = max(
                    code_boot["target_uplift_p_one_sided"], code_boot["opposite_reduction_p_one_sided"],
                    code_boot["mean_return_delta_p_nonnegative"] if target_name == "SELL" else 0.0,
                )
                p_values[candidate["candidate_id"]] = combined_p
                calibration_results.append({
                    **candidate, "train_rank": train_rank, "calibration_metrics": metric,
                    "calibration_bootstrap": {"code": code_boot, "month": month_boot}, "calibration_raw_p": combined_p,
                })
            adjusted = _holm(p_values)
            passing: list[dict[str, Any]] = []
            for candidate in calibration_results:
                selected = candidate["calibration_metrics"]["selected"]
                candidate["calibration_holm_p"] = adjusted[candidate["candidate_id"]]
                candidate["calibration_pass"] = bool(
                    selected["n"] >= 80 and selected["codes"] >= 40 and selected["months"] >= 8
                    and selected["max_month"] <= .25
                    and selected["target_uplift"] >= .05 and selected["opposite_reduction"] >= .03
                    and (target_name != "SELL" or selected["mean_return"] <= SELL_MEAN_GATES[horizon])
                    and candidate["calibration_holm_p"] <= .10
                )
                if candidate["calibration_pass"]:
                    passing.append(candidate)
            chosen = min(passing, key=lambda item: item["train_rank"]) if passing else None
            if chosen is not None:
                calibrated.append(chosen)
            discoveries[f"{target_name}_h{horizon}"] = {
                "raw20": raw20, "stable5": stable5, "calibration_results": calibration_results,
                "calibrated1_candidate_id": chosen["candidate_id"] if chosen else None,
            }

    final_p_values: dict[str, float] = {}
    final_rows: list[dict[str, Any]] = []
    for candidate in calibrated:
        horizon = int(candidate["horizon"])
        target_class, opposite_class = TARGETS[candidate["target"]]
        required = [f"ret_close_{horizon}", f"down_exc_{horizon}", f"up_exc_{horizon}"]
        horizon_frame = usable.dropna(subset=required).copy().reset_index(drop=True)
        horizon_frame["_target_return"] = horizon_frame[f"ret_close_{horizon}"]
        labels = _labels(horizon_frame, horizon)
        candidate_result: dict[str, Any] = {}
        for period_name in ("fixed_evaluation_2023_2025", "exploratory_2026"):
            period_mask = horizon_frame["ymd"].between(*periods[period_name]).to_numpy()
            part = horizon_frame.loc[period_mask].reset_index(drop=True)
            part_labels = labels[period_mask]
            raw = _rule_mask(part, candidate["conditions"])
            event = _event_mask(part, raw)
            metric = _candidate_metrics(part, part_labels, event, target_class, opposite_class)
            candidate_result[period_name] = metric
            assignments = part.loc[event, ["code", "ymd"]].copy()
            assignments["candidate_id"] = candidate["candidate_id"]
            assignments["target"] = candidate["target"]
            assignments["horizon"] = horizon
            assignments["period"] = period_name
            assignments["label"] = part_labels[event]
            assignment_parts.append(assignments)
            if period_name == "fixed_evaluation_2023_2025":
                yearly: dict[str, Any] = {}
                for year in (2023, 2024, 2025):
                    year_mask = (part["ymd"].to_numpy() // 10000) == year
                    year_event = event & year_mask
                    yearly[str(year)] = _candidate_metrics(
                        part.loc[year_mask].reset_index(drop=True), part_labels[year_mask],
                        event[year_mask], target_class, opposite_class,
                    )
                candidate_result["yearly_2023_2025"] = yearly
                code_boot = _cluster_bootstrap(part, part_labels, event, target_class, opposite_class, "code", 2000, int(candidate["candidate_id"][:8], 16) + 2325)
                month_boot = _cluster_bootstrap(part, part_labels, event, target_class, opposite_class, "month", 2000, int(candidate["candidate_id"][8:], 16) + 2325)
                combined_p = max(
                    code_boot["target_uplift_p_one_sided"], code_boot["opposite_reduction_p_one_sided"],
                    month_boot["target_uplift_p_one_sided"], month_boot["opposite_reduction_p_one_sided"],
                    code_boot["mean_return_delta_p_nonnegative"] if candidate["target"] == "SELL" else 0.0,
                    month_boot["mean_return_delta_p_nonnegative"] if candidate["target"] == "SELL" else 0.0,
                )
                candidate_result["final_bootstrap"] = {"code": code_boot, "month": month_boot, "raw_p": combined_p}
                final_p_values[candidate["candidate_id"]] = combined_p
        final_rows.append({
            "candidate_id": candidate["candidate_id"], "target": candidate["target"], "horizon": horizon,
            "conditions": candidate["conditions"], "expression": candidate["expression"],
            "train_rank": candidate["train_rank"], "evaluation": candidate_result,
        })

    final_adjusted = _holm(final_p_values)
    for candidate in final_rows:
        selected = candidate["evaluation"]["fixed_evaluation_2023_2025"]["selected"]
        yearly = candidate["evaluation"]["yearly_2023_2025"]
        boot = candidate["evaluation"]["final_bootstrap"]
        adjusted_p = final_adjusted[candidate["candidate_id"]]
        candidate["final_holm_p"] = adjusted_p
        if candidate["target"] == "SELL":
            absolute_pass = selected["target_rate"] >= SELL_PRECISION_GATES[candidate["horizon"]] and selected["opposite_rate"] <= SELL_REBOUND_GATES[candidate["horizon"]] and selected["mean_return"] <= SELL_MEAN_GATES[candidate["horizon"]]
            ci_pass = all(
                item["target_uplift_ci95"][0] > 0 and item["opposite_reduction_ci95"][0] > 0 and item["selected_mean_return_ci95"][1] < 0
                for item in (boot["code"], boot["month"])
            )
        else:
            absolute_pass = selected["target_rate"] >= REBOUND_PRECISION_GATES[candidate["horizon"]] and selected["opposite_rate"] <= REBOUND_DOWN_GATES[candidate["horizon"]]
            ci_pass = all(
                item["target_uplift_ci95"][0] > 0 and item["opposite_reduction_ci95"][0] > 0
                for item in (boot["code"], boot["month"])
            )
        yearly_values = [yearly[str(year)]["selected"] for year in (2023, 2024, 2025)]
        yearly_size_pass = all(item["n"] >= 60 and item["codes"] >= 30 and item["months"] >= 6 for item in yearly_values)
        yearly_direction_pass = all(item["target_uplift"] > 0 and item["opposite_reduction"] >= -.02 for item in yearly_values)
        if candidate["target"] == "SELL":
            yearly_absolute_count = sum(item["target_rate"] >= SELL_PRECISION_GATES[candidate["horizon"]] and item["opposite_rate"] <= SELL_REBOUND_GATES[candidate["horizon"]] and item["mean_return"] <= SELL_MEAN_GATES[candidate["horizon"]] for item in yearly_values)
        else:
            yearly_absolute_count = sum(item["target_rate"] >= REBOUND_PRECISION_GATES[candidate["horizon"]] and item["opposite_rate"] <= REBOUND_DOWN_GATES[candidate["horizon"]] for item in yearly_values)
        candidate["final_gate"] = {
            "n_ge_240": selected["n"] >= 240,
            "codes_ge_100": selected["codes"] >= 100,
            "months_ge_24": selected["months"] >= 24,
            "max_code_le_5pct": selected["max_code"] <= .05,
            "max_month_le_15pct": selected["max_month"] <= .15,
            "target_uplift_ge_5pp": selected["target_uplift"] >= .05,
            "opposite_reduction_ge_3pp": selected["opposite_reduction"] >= .03,
            "absolute_gate": absolute_pass,
            "cluster_ci_gate": ci_pass,
            "each_year_size_gate": yearly_size_pass,
            "each_year_direction_gate": yearly_direction_pass,
            "two_of_three_year_absolute_gate": yearly_absolute_count >= 2,
            "holm_p_le_05": adjusted_p <= .05,
        }
        candidate["candidate_local_decision"] = "hold_pending_clean_shadow" if all(candidate["final_gate"].values()) else "drop"

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    assignment_path = output / "candidate_assignment_ledger.parquet"
    assignments = pd.concat(assignment_parts, ignore_index=True) if assignment_parts else pd.DataFrame(
        columns=["code", "ymd", "candidate_id", "target", "horizon", "period", "label"]
    )
    assignments.to_parquet(assignment_path, index=False, compression="zstd")
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment", "source_parquet": str(input_parquet),
        "supersedes": "G:\\Tradex\\tradex_nikkei225_train_only_sparse_interaction_v1\\20260714T042607Z-tradex_nikkei225_train_only_sparse_interaction_v1\\compare.json",
        "supersedes_reason": "prior artifact used provisional gates before quant fixed contract arrived",
        "source_sha256": hashlib.sha256(input_parquet.read_bytes()).hexdigest(),
        "assignment_ledger": str(assignment_path),
        "fixed_conditions": {
            "targets": TARGETS, "horizons": list(HORIZONS), "features": FEATURES,
            "candidate_generation": "2019-2021 only; maximum 20 raw then maximum 5 stable per target-horizon",
            "calibration": "2022 expression frozen; Holm within maximum 5; maximum 1 retained by frozen train rank",
            "final_evaluation": "2023-2025 once, all candidates cross-Holm alpha 0.05",
            "exploratory_only": "2026 through 2026-07-13", "max_conditions": 3,
            "event": "false-to-true first entry with 10-bar cooldown", "discovery_bootstrap": 100,
            "calibration_and_final_bootstrap": 2000, "clusters": ["code", "month"],
            "discovery_gate": "n>=200,codes>=80,months>=18,coverage 0.5%-20%,max code<=5%,max month<=15%; code bootstrap target and guardrail direction >=70%; Jaccard>=0.85 keeps simpler rule",
            "calibration_gate": "2022 n>=80,codes>=40,months>=8,max month<=25%,target uplift>=5pp,opposite reduction>=3pp,SELL mean-return horizon gate; code bootstrap Holm alpha=.10",
            "final_gate": "2023-25 n>=240,codes>=100,months>=24; each year n>=60,codes>=30,months>=6; concentration, absolute, difference, annual, code/month CI, cross-candidate Holm alpha=.05",
            "sell_rebound_symmetry": "separate trees and candidate ids; no sign reversal reuse",
            "future_columns": "labels and evaluation only",
        },
        "discovery": discoveries,
        "calibrated_candidate_count": len(calibrated),
        "final_candidates": final_rows,
        "observed_branching": {
            "raw_candidate_count": sum(len(item["raw20"]) for item in discoveries.values()),
            "stable_candidate_count": sum(len(item["stable5"]) for item in discoveries.values()),
            "calibrated_candidate_count": len(calibrated), "assignment_rows": len(assignments),
        },
        "decision": {
            "candidate_local_decision": "hold_candidates_pending_clean_shadow" if any(item["candidate_local_decision"].startswith("hold") for item in final_rows) else "drop_all_sparse_rules",
            "authoritative_rollup_decision": "review_only",
        },
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
        "remaining_risks": ["current Nikkei225 registry is survivorship-biased", "2026 remains exploratory", "tree variants create correlated train hypotheses"],
    }
    compare = output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "compare": str(compare), "assignment_ledger": str(assignment_path)}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-parquet", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_train_only_sparse_interaction_v1"))
    args = parser.parse_args()
    print(run(args.input_parquet, args.output_root))


if __name__ == "__main__":
    main()
