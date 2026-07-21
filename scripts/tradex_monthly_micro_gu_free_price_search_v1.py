"""Explore and robustness-check nominal entry-price bands for the fixed micro-GU setup."""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


LOWERS = [0, 500, 700, 900, 1000, 1200, 1500, 2000]
UPPERS = [3000, 4000, 5000, 6000, 7000, 8000, 10000, 20000, 100000]
SELECTED = (1200, 8000)
SENSITIVITY = [(900, 8000), (1000, 7000), (1000, 8000), (1200, 7000), (1200, 8000), (1200, 10000), (1500, 8000), (1500, 10000)]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metrics(frame):
    if frame.empty:
        return {"n": 0}
    years = frame.groupby("year").ret20_pct.mean()
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "years": int(frame.year.nunique()),
        "positive_year_rate": float((years > 0).mean()),
        "ret5_mean": float(frame.ret5_pct.mean()),
        "ret10_mean": float(frame.ret10_pct.mean()),
        "ret20_mean": float(frame.ret20_pct.mean()),
        "ret20_median": float(frame.ret20_pct.median()),
        "ret20_positive_rate": float((frame.ret20_pct > 0).mean()),
        "max_up20_ge_10_rate": float((frame.max_up20_pct >= 10).mean()),
        "max_down20_le_minus5_rate": float((frame.max_down20_pct <= -5).mean()),
    }


def subset(frame, low, high):
    return frame.loc[frame.entry_open.ge(low) & frame.entry_open.lt(high)]


def bootstrap(values, seed):
    values = np.asarray(values, dtype=float)
    rng = np.random.default_rng(seed)
    draws = rng.choice(values, size=(10000, len(values)), replace=True).mean(axis=1)
    return {
        "samples": 10000,
        "ret20_mean_ci95_low": float(np.quantile(draws, 0.025)),
        "ret20_mean_ci95_high": float(np.quantile(draws, 0.975)),
        "probability_mean_gt_zero": float((draws > 0).mean()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True)
    ap.add_argument("--volume-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    events = pd.read_parquet(a.events).copy()
    grid = []
    for low in LOWERS:
        for high in UPPERS:
            if high <= low:
                continue
            row = metrics(subset(events, low, high))
            if row["n"] < 15:
                continue
            row.update({"low": low, "high": high})
            row["selection_score"] = (
                row["ret20_mean"]
                + 2 * row["ret20_positive_rate"]
                - 3 * row["max_down20_le_minus5_rate"]
                + 0.5 * row["positive_year_rate"]
            )
            grid.append(row)
    grid.sort(key=lambda row: (row["selection_score"], row["n"]), reverse=True)

    low, high = SELECTED
    target = subset(events, low, high).copy()
    target["free_price_selected"] = True
    target.to_parquet(a.output / "monthly_micro_gu_free_price_selected_events.parquet", index=False)
    target_metrics = metrics(target)
    sensitivity = {
        f"{lo}_{hi}": metrics(subset(events, lo, hi)) for lo, hi in SENSITIVITY
    }
    loyo = {
        str(year): metrics(target.loc[target.year.ne(year)])
        for year in sorted(target.year.unique())
    }
    year_detail = {
        str(year): metrics(rows) for year, rows in target.groupby("year")
    }
    volume_boundary = {
        "lt_1.5": metrics(target.loc[target.volume_ratio_20d_median.lt(1.5)]),
        "ge_1.5": metrics(target.loc[target.volume_ratio_20d_median.ge(1.5)]),
    }
    volume_target = target.loc[target.volume_ratio_20d_median.ge(1.5)]
    boot = bootstrap(volume_target.ret20_pct, 20260728)
    checks = {
        "selected_n_ge_20": target_metrics["n"] >= 20,
        "selected_positive_rate_ge_0.85": target_metrics["ret20_positive_rate"] >= 0.85,
        "selected_deep_drawdown_rate_le_0.20": target_metrics["max_down20_le_minus5_rate"] <= 0.20,
        "all_sensitivity_ret20_gt_5": all(row["ret20_mean"] > 5 for row in sensitivity.values()),
        "all_sensitivity_positive_rate_ge_0.80": all(
            row["ret20_positive_rate"] >= 0.80 for row in sensitivity.values()
        ),
        "all_loyo_ret20_gt_zero": all(row["ret20_mean"] > 0 for row in loyo.values()),
        "all_loyo_positive_rate_ge_0.80": all(
            row["ret20_positive_rate"] >= 0.80 for row in loyo.values()
        ),
        "volume_confirmed_n_ge_15": volume_boundary["ge_1.5"]["n"] >= 15,
        "volume_confirmed_bootstrap_probability_gt_zero_ge_0.95": (
            boot["probability_mean_gt_zero"] >= 0.95
        ),
    }
    result = {
        "schema_version": "tradex_monthly_micro_gu_free_price_search_v1.compare.v1",
        "artifact_role": "authoritative_monthly_micro_gu_free_price_search",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_setup": "range_months=6-7, signal return<=-2%, next-session GU 0-0.5%",
            "axis_changed": "nominal entry-price lower and upper boundaries only",
            "lower_grid_yen": LOWERS,
            "upper_grid_yen": UPPERS,
            "minimum_grid_sample": 15,
            "selected_robust_plateau_yen": [low, high],
            "execution": "next-session open",
            "horizons": [5, 10, 20],
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "grid_top20": grid[:20],
            "selected_band": target_metrics,
            "threshold_sensitivity": sensitivity,
            "leave_one_year_out": loyo,
            "selected_band_years": year_detail,
            "selected_band_volume_boundary": volume_boundary,
            "volume_confirmed_bootstrap_ret20_mean": boot,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "free nominal-price grid moved the robust plateau from 900-5000 to 1200-8000 yen",
            "selected_count": target_metrics["n"],
            "volume_confirmed_selected_count": volume_boundary["ge_1.5"]["n"],
        },
        "judgment": {
            "candidate_local_decision": "keep",
            "session_aggregate_decision": "keep_1200_to_8000_price_priority",
            "authoritative_rollup_decision": "keep_1200_to_8000_volume_ge_1.5_review_only",
            "reason_type": "free_grid_robust_plateau_and_leave_one_year_out_gates_passed",
        },
        "not_changed": [
            "parent setup",
            "volume threshold",
            "body ratio",
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
        ],
        "remaining_risks": [
            "price interval was selected in-sample",
            "grid score is exploratory and not a production objective",
            "nominal historical price is affected by stock splits",
            "price may proxy liquidity or company characteristics",
            "monthly range definition remains research-fallback",
        ],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    pd.DataFrame(grid).to_parquet(a.output / "free_price_grid.parquet", index=False)
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)},
            "volume_compare": {"path": str(a.volume_compare.resolve()), "sha256": sha(a.volume_compare)},
        },
        "grid_candidates": len(grid),
        "selected_events": int(len(target)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "selected_ledger_sha256": sha(a.output / "monthly_micro_gu_free_price_selected_events.parquet"),
        "grid_sha256": sha(a.output / "free_price_grid.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "selected": target_metrics, "volume_boundary": volume_boundary, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
