from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeRegressor, _tree


FEATURES = ["ret1", "ret3", "ret5", "ret10", "ret20", "range20", "range60", "close_vs_ma20", "close_vs_ma60", "dist_low20", "dist_high20", "close_position", "lower_wick_ratio", "upper_wick_ratio", "volume_ratio20"]
CHAMPION_YEAR = {2019: 0.0596479533, 2020: 0.0120472756, 2021: 0.0375209006, 2022: 0.1259728410, 2023: 0.3244676996, 2024: 0.6793201524, 2025: 0.3830549366, 2026: 0.0024812332}


def simulate(frame: pd.DataFrame) -> pd.Series:
    entry = frame["p1_o"]
    d1_high = (frame["p1_h"] / entry - 1) * 100
    d1_low = (frame["p1_l"] / entry - 1) * 100
    d1_close = (frame["p1_c"] / entry - 1) * 100
    immediate = d1_high.ge(3) & d1_low.gt(-3)
    stall = (d1_close.le(-2) | d1_low.le(-3)) & ~immediate
    ret = 25 * (frame["p3_c"] / entry - 1)
    ret.loc[immediate] = 100 * (0.25 * (frame.loc[immediate, "p3_c"] / entry.loc[immediate] - 1) + 0.75 * (frame.loc[immediate, "p3_c"] / frame.loc[immediate, "p2_o"] - 1))
    ret.loc[stall] = 25 * (frame.loc[stall, "p2_o"] / entry.loc[stall] - 1)
    return ret


def dedupe(frame: pd.DataFrame) -> pd.DataFrame:
    keep = []
    for _, group in frame.sort_values(["code", "bar_index"]).groupby("code", sort=False):
        last = -10**9
        for idx, bar_index in zip(group.index, group["bar_index"]):
            if int(bar_index) - last > 5:
                keep.append(idx)
                last = int(bar_index)
    return frame.loc[keep]


def metric(frame: pd.DataFrame) -> dict:
    return {"n": int(len(frame)), "codes": int(frame["code"].nunique()), "mean_return_pct": float(frame["realized_ret"].mean()), "median_return_pct": float(frame["realized_ret"].median()), "win_rate": float(frame["realized_ret"].gt(0).mean()), "severe_loss5_rate": float(frame["realized_ret"].le(-5).mean()), "p10_return_pct": float(frame["realized_ret"].quantile(0.1))}


def paths(tree: DecisionTreeRegressor) -> dict[int, list[str]]:
    t = tree.tree_
    result = {}
    def walk(node: int, clauses: list[str]) -> None:
        if t.feature[node] == _tree.TREE_UNDEFINED:
            result[int(node)] = clauses
            return
        feature = FEATURES[t.feature[node]]
        threshold = float(t.threshold[node])
        walk(t.children_left[node], clauses + [f"{feature} <= {threshold:.10g}"])
        walk(t.children_right[node], clauses + [f"{feature} > {threshold:.10g}"])
    walk(0, [])
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    cols = ["code", "ymd", "bar_index", "c", "p1_o", "p1_h", "p1_l", "p1_c", "p2_o", *FEATURES]
    data = pd.read_parquet(a.inventory, columns=cols)
    data["code"] = data["code"].astype(str)
    close_lookup = data[["code", "bar_index", "c"]].rename(
        columns={"bar_index": "future_index", "c": "p3_c"}
    )
    data["future_index"] = data["bar_index"] + 3
    data = data.merge(
        close_lookup,
        on=["code", "future_index"],
        how="left",
        validate="many_to_one",
    ).drop(columns="future_index")
    data = data[data[["p1_o", "p1_h", "p1_l", "p1_c", "p2_o", "p3_c"]].notna().all(axis=1)].copy()
    data["year"] = data["ymd"] // 10000
    data["realized_ret"] = simulate(data)
    discovery = data["year"].between(2024, 2025)
    imputer = SimpleImputer(strategy="median")
    x_discovery = imputer.fit_transform(data.loc[discovery, FEATURES])
    tree = DecisionTreeRegressor(max_depth=4, min_samples_leaf=2000, random_state=20260720)
    tree.fit(x_discovery, data.loc[discovery, "realized_ret"])
    data["leaf"] = tree.apply(imputer.transform(data[FEATURES])).astype(int)
    path_map = paths(tree)
    discovery_baseline = (CHAMPION_YEAR[2024] * 4929 + CHAMPION_YEAR[2025] * 4664) / (4929 + 4664)
    discovery_rows = []
    for leaf, frame in data[discovery].groupby("leaf"):
        clean = dedupe(frame)
        discovery_rows.append({"leaf": int(leaf), "rule": path_map[int(leaf)], **metric(clean)})
    candidates = [
        r["leaf"]
        for r in discovery_rows
        if r["n"] >= 250
        and r["mean_return_pct"] >= discovery_baseline + 2.0
        and len({clause.split()[0] for clause in r["rule"]}) >= 2
    ]
    tests = []
    for leaf in candidates:
        recent = dedupe(data[(data["year"].eq(2026)) & data["leaf"].eq(leaf)])
        recent_metrics = metric(recent)
        long_years = {}
        for year in range(2019, 2024):
            long_years[str(year)] = metric(dedupe(data[(data["year"].eq(year)) & data["leaf"].eq(leaf)]))
        checks = {
            "test_2026_n_at_least_250": recent_metrics["n"] >= 250,
            "test_2026_mean_improvement_at_least_2pp": recent_metrics["mean_return_pct"] >= CHAMPION_YEAR[2026] + 2.0,
            "test_2026_severe_not_worse": recent_metrics["severe_loss5_rate"] <= 0.03035056074033409,
            "long_history_each_year_positive": all(v["n"] >= 250 and v["mean_return_pct"] > 0 for v in long_years.values()),
        }
        tests.append({"leaf": leaf, "rule": path_map[leaf], "discovery": next(r for r in discovery_rows if r["leaf"] == leaf), "test_2026": recent_metrics, "long_history": long_years, "checks": checks, "pass": all(checks.values())})
    passed = [x for x in tests if x["pass"]]
    chosen = max(passed, key=lambda x: x["test_2026"]["mean_return_pct"])["leaf"] if passed else None
    decision = "keep" if chosen is not None else "drop"
    payload = {
        "schema_version": "tradex_long_recent_first_tree_shape_v1.compare.v1", "artifact_role": "authoritative",
        "fixed_evaluation_conditions": {"universe": "all eligible PAN daily rows", "discovery": "2024-2025", "untouched_test": 2026, "long_history": "2019-2023 after test pass only", "features": FEATURES, "tree": {"max_depth": 4, "min_samples_leaf": 2000, "random_state": 20260720}, "compound_gate": "at least two distinct pre-entry features in leaf rule", "signal_dedup": "same code signals within 5 rows keep earliest", "management": "same 25% initial, immediate-rise add75%, stall exit, day3 close", "improvement_gate": "mean +2pp, n>=250, severe loss not worse, every long-history year positive", "costs": "ignored", "production_ranking_changed": False, "runtime_db_write": False, "meemee_reflection_allowed": False},
        "authoritative_result": {"champion_discovery_baseline_mean_pct": discovery_baseline, "discovery_leaves": discovery_rows, "development_candidates": candidates, "tests": tests, "chosen_leaf": chosen},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": int(sum(r["n"] for r in discovery_rows)), "selection_divergence_reason": "shallow train-only multi-feature tree partitions full-universe pre-entry chart states"},
        "judgment": {"candidate_local_decision": decision, "authoritative_rollup_decision": f"recent_first_tree_{decision}", "reason_type": "recent_discovery_untouched_2026_then_long_history_gate"},
        "remaining_risks": ["daily rows before dedup remain correlated during tree fitting", "costs ignored", "corporate-action robustness not applied"],
    }
    data[["code", "ymd", "bar_index", "year", "leaf", "realized_ret"]].to_parquet(out / "tree_leaf_ledger.parquet", index=False)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps({"baseline": discovery_baseline, "candidates": candidates, "tests": tests, "chosen": chosen}, ensure_ascii=False))


if __name__ == "__main__":
    main()
