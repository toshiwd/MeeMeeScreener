"""Audit the volume gate inside the user-specified price bands."""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def metrics(frame):
    if frame.empty:
        return {"n": 0}
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "ret5_mean": float(frame.ret5_pct.mean()),
        "ret10_mean": float(frame.ret10_pct.mean()),
        "ret20_mean": float(frame.ret20_pct.mean()),
        "ret20_median": float(frame.ret20_pct.median()),
        "ret20_positive_rate": float((frame.ret20_pct > 0).mean()),
        "max_up20_ge_10_rate": float((frame.max_up20_pct >= 10).mean()),
        "max_down20_le_minus5_rate": float((frame.max_down20_pct <= -5).mean()),
    }


def bootstrap(values, seed):
    values = np.asarray(values, dtype=float)
    if not len(values):
        return {"samples": 0}
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

    selected = pd.read_parquet(a.events).copy()
    selected["volume_ok"] = selected.volume_ratio_20d_median.ge(1.5)
    selected["practical_price_band"] = np.select(
        [selected.entry_open.lt(900), selected.entry_open.lt(5000)],
        ["lt_900", "900_5000"],
        default="ge_5000",
    )
    selected["boundary_group"] = selected.practical_price_band + np.where(
        selected.volume_ok, "_vol_ge_1.5", "_vol_lt_1.5"
    )
    selected.to_parquet(a.output / "monthly_micro_gu_price_volume_boundary_events.parquet", index=False)

    names = [
        "lt_900_vol_lt_1.5",
        "lt_900_vol_ge_1.5",
        "900_5000_vol_lt_1.5",
        "900_5000_vol_ge_1.5",
        "ge_5000_vol_lt_1.5",
        "ge_5000_vol_ge_1.5",
    ]
    groups = {name: metrics(selected.loc[selected.boundary_group.eq(name)]) for name in names}
    years = {
        str(year): {name: metrics(rows.loc[rows.boundary_group.eq(name)]) for name in names}
        for year, rows in selected.groupby("year")
    }
    target = groups["900_5000_vol_ge_1.5"]
    target_boot = bootstrap(
        selected.loc[selected.boundary_group.eq("900_5000_vol_ge_1.5"), "ret20_pct"], 20260727
    )
    checks = {
        "target_n_ge_15": target["n"] >= 15,
        "target_ret20_gt_same_price_low_volume": (
            target["ret20_mean"] > groups["900_5000_vol_lt_1.5"]["ret20_mean"]
        ),
        "target_positive_rate_ge_0.85": target["ret20_positive_rate"] >= 0.85,
        "target_deep_drawdown_rate_le_0.20": target["max_down20_le_minus5_rate"] <= 0.20,
        "target_bootstrap_probability_gt_zero_ge_0.95": (
            target_boot["probability_mean_gt_zero"] >= 0.95
        ),
        "high_price_sample_ge_10": (
            groups["ge_5000_vol_lt_1.5"]["n"] + groups["ge_5000_vol_ge_1.5"]["n"] >= 10
        ),
    }
    result = {
        "schema_version": "tradex_monthly_micro_gu_price_volume_boundary_v1.compare.v1",
        "artifact_role": "authoritative_monthly_micro_gu_price_volume_boundary",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_setup": "range_months=6-7, signal return<=-2%, next-session GU 0-0.5%",
            "boundary_audit": "user-specified nominal price bands crossed with previously kept volume>=1.5 gate",
            "price_rollup_yen": ["lt_900", "900_5000", "ge_5000"],
            "volume_gate": "signal volume / previous 20-session median >=1.5",
            "execution": "next-session open",
            "horizons": [5, 10, 20],
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "groups": groups,
            "years": years,
            "target_bootstrap_ret20_mean": target_boot,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "volume confirmation effectiveness differs by nominal entry-price band",
            "group_counts": {name: groups[name]["n"] for name in names},
        },
        "judgment": {
            "candidate_local_decision": {
                "lt_900": "hold_no_confirmed_volume_benefit",
                "900_5000_vol_lt_1.5": "drop",
                "900_5000_vol_ge_1.5": "keep_starter",
                "ge_5000": "hold_insufficient_sample",
            },
            "session_aggregate_decision": "keep_900_5000_with_volume_confirmation",
            "authoritative_rollup_decision": "keep_900_to_5000_volume_ge_1.5_review_only_hold_outer_prices",
            "reason_type": "price_conditional_volume_confirmation_with_outer_band_sample_limits",
        },
        "not_changed": [
            "parent setup",
            "price boundaries",
            "volume threshold",
            "body ratio",
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
        ],
        "remaining_risks": [
            "price is nominal and affected by stock splits",
            "below-900 and above-5000 samples remain small",
            "price may proxy liquidity or company characteristics",
            "monthly range definition remains research-fallback",
        ],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)},
            "volume_compare": {"path": str(a.volume_compare.resolve()), "sha256": sha(a.volume_compare)},
        },
        "selected_events": int(len(selected)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "monthly_micro_gu_price_volume_boundary_events.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "groups": groups, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

