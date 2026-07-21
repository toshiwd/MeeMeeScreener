"""Compare user-specified entry price bands for the fixed monthly micro-GU setup."""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


BANDS = ["lt_900", "900_3000", "3000_5000", "5000_10000", "ge_10000"]


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
        "volume_ratio_mean": float(frame.volume_ratio_20d_median.mean()),
        "volume_ratio_ge_1.5_rate": float((frame.volume_ratio_20d_median >= 1.5).mean()),
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
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    selected = pd.read_parquet(a.events).copy()
    selected["price_band"] = pd.cut(
        selected.entry_open,
        [-np.inf, 900, 3000, 5000, 10000, np.inf],
        labels=BANDS,
        right=False,
    )
    selected.to_parquet(a.output / "monthly_micro_gu_price_band_events.parquet", index=False)

    groups = {band: metrics(selected.loc[selected.price_band.eq(band)]) for band in BANDS}
    practical = {
        "lt_900": metrics(selected.loc[selected.entry_open.lt(900)]),
        "900_5000": metrics(selected.loc[selected.entry_open.ge(900) & selected.entry_open.lt(5000)]),
        "ge_5000": metrics(selected.loc[selected.entry_open.ge(5000)]),
    }
    years = {
        str(year): {band: metrics(rows.loc[rows.price_band.eq(band)]) for band in BANDS}
        for year, rows in selected.groupby("year")
    }
    bootstrap_by_band = {
        band: bootstrap(selected.loc[selected.price_band.eq(band), "ret20_pct"], 20260722 + i)
        for i, band in enumerate(BANDS)
    }
    middle = practical["900_5000"]
    checks = {
        "middle_n_ge_20": middle["n"] >= 20,
        "middle_ret20_mean_gt_low": middle["ret20_mean"] > practical["lt_900"]["ret20_mean"],
        "middle_positive_rate_ge_0.80": middle["ret20_positive_rate"] >= 0.80,
        "middle_deep_drawdown_rate_le_0.25": middle["max_down20_le_minus5_rate"] <= 0.25,
        "high_price_sample_ge_10": practical["ge_5000"]["n"] >= 10,
    }
    result = {
        "schema_version": "tradex_monthly_micro_gu_price_band_v1.compare.v1",
        "artifact_role": "authoritative_monthly_micro_gu_price_band",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_setup": "range_months=6-7, signal return<=-2%, next-session GU 0-0.5%",
            "axis_changed": "entry-open nominal yen price band only",
            "user_specified_boundaries_yen": [900, 3000, 5000, 10000],
            "price_bands": BANDS,
            "execution": "next-session open",
            "horizons": [5, 10, 20],
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "all_events": metrics(selected),
            "groups": groups,
            "practical_rollup": practical,
            "years": years,
            "bootstrap_ret20_mean": bootstrap_by_band,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "fixed micro-GU events split only by user-specified nominal price bands",
            "group_counts": {band: groups[band]["n"] for band in BANDS},
        },
        "judgment": {
            "candidate_local_decision": {
                "lt_900": "hold_low_upside",
                "900_3000": "keep",
                "3000_5000": "keep",
                "5000_10000": "hold_insufficient_sample",
                "ge_10000": "hold_insufficient_sample",
            },
            "session_aggregate_decision": "keep_900_to_5000_review_band",
            "authoritative_rollup_decision": "keep_900_to_5000_price_band_review_only_hold_high_prices",
            "reason_type": "middle_price_breadth_and_stability_high_price_sample_insufficient",
        },
        "not_changed": [
            "monthly range definition",
            "capitulation threshold",
            "GU magnitude",
            "body ratio",
            "volume threshold",
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
        ],
        "remaining_risks": [
            "nominal historical price is affected by stock splits and price-level regime",
            "prices at or above 5000 yen have only four events",
            "price effect may partly proxy liquidity and company characteristics",
            "monthly range definition remains research-fallback",
        ],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)},
        },
        "selected_events": int(len(selected)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "monthly_micro_gu_price_band_events.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "groups": groups, "practical": practical, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
