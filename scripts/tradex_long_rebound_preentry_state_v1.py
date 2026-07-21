from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import RobustScaler


FEATURES = ["ret1", "ret3", "ret5", "ret20", "range20", "close_vs_ma20", "dist_low20", "close_position", "lower_wick_ratio", "volume_ratio20"]


def metric(frame: pd.DataFrame) -> dict:
    yearly = frame.assign(year=frame["ymd"] // 10000).groupby("year")["ret"].agg(["count", "mean"])
    return {
        "n": int(len(frame)), "codes": int(frame["code"].nunique()),
        "mean_return_pct": float(frame["ret"].mean()), "median_return_pct": float(frame["ret"].median()),
        "win_rate": float(frame["ret"].gt(0).mean()), "severe_loss5_rate": float(frame["ret"].le(-5).mean()),
        "p10_return_pct": float(frame["ret"].quantile(0.1)),
        "years": {str(int(y)): {"n": int(v["count"]), "mean_return_pct": float(v["mean"])} for y, v in yearly.iterrows()},
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--practical-ledger", required=True)
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    practical = pd.read_parquet(a.practical_ledger)
    practical["code"] = practical["code"].astype(str)
    inventory = pd.read_parquet(a.inventory, columns=["code", "ymd", *FEATURES])
    inventory["code"] = inventory["code"].astype(str)
    data = practical.merge(inventory, on=["code", "ymd"], how="left", validate="many_to_one")
    development = data["period"].eq("development")
    model = make_pipeline(SimpleImputer(strategy="median"), RobustScaler(), KMeans(n_clusters=8, random_state=20260720, n_init=20))
    model.fit(data.loc[development, FEATURES])
    data["preentry_state"] = model.predict(data[FEATURES]).astype(int)
    baseline = {period: metric(data[data["period"].eq(period)]) for period in ["development", "validation"]}
    rows = []
    for period in ["development", "validation"]:
        for state, frame in data[data["period"].eq(period)].groupby("preentry_state"):
            rows.append({"period": period, "state": int(state), **metric(frame)})
    dev = {r["state"]: r for r in rows if r["period"] == "development"}
    val = {r["state"]: r for r in rows if r["period"] == "validation"}
    development_candidates = [
        state for state, row in dev.items()
        if row["n"] >= 250 and (
            row["mean_return_pct"] >= baseline["development"]["mean_return_pct"] + 2.0
            or row["severe_loss5_rate"] <= max(0.0, baseline["development"]["severe_loss5_rate"] - 0.03)
        )
    ]
    tests = []
    for state in development_candidates:
        row = val[state]
        year_means = [v["mean_return_pct"] for v in row["years"].values()]
        checks = {
            "validation_n_at_least_250": row["n"] >= 250,
            "validation_mean_improvement_at_least_2pp_or_tail_improvement_3pp": (
                row["mean_return_pct"] >= baseline["validation"]["mean_return_pct"] + 2.0
                or row["severe_loss5_rate"] <= max(0.0, baseline["validation"]["severe_loss5_rate"] - 0.03)
            ),
            "all_validation_years_positive": len(year_means) == 3 and all(x > 0 for x in year_means),
        }
        tests.append({"state": state, "metrics": row, "checks": checks, "pass": all(checks.values())})
    passed = [x for x in tests if x["pass"]]
    chosen = max(passed, key=lambda x: x["metrics"]["mean_return_pct"])["state"] if passed else None
    decision = "keep" if chosen is not None else "drop"
    centroids = model.named_steps["kmeans"].cluster_centers_
    payload = {
        "schema_version": "tradex_long_rebound_preentry_state_v1.compare.v1",
        "artifact_role": "authoritative",
        "fixed_evaluation_conditions": {
            "population": "existing practical rebound-buy ledger", "development": "2019-2023", "validation": "2024-2026",
            "classification": "outcome-blind KMeans k=8 fitted on development pre-entry features only",
            "features": FEATURES, "management": "existing practical actionability realized return unchanged",
            "improvement_gate": "mean +2pp or severe-loss rate -3pp; validation n>=250 and all years positive",
            "costs": "ignored", "production_ranking_changed": False, "runtime_db_write": False, "meemee_reflection_allowed": False,
        },
        "authoritative_result": {"baseline": baseline, "states": rows, "development_candidates": development_candidates, "validation_tests": tests, "chosen_state": chosen, "scaled_centroids": centroids.tolist()},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": int(len(data)), "selection_divergence_reason": "pre-entry multi-feature chart state partitions all practical rebound events"},
        "judgment": {"candidate_local_decision": decision, "authoritative_rollup_decision": f"preentry_state_{decision}", "reason_type": "development_selected_validation_mean_tail_breadth_year_gate"},
        "remaining_risks": ["unsupervised state labels need chart interpretation", "costs ignored", "portfolio overlap not simulated"],
    }
    data.to_parquet(out / "preentry_state_ledger.parquet", index=False)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps({"baseline": baseline, "development_candidates": development_candidates, "validation_tests": tests, "chosen_state": chosen}, ensure_ascii=False))


if __name__ == "__main__":
    main()
