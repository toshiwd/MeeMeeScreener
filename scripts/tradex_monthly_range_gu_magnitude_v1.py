"""Compare next-session GU magnitude for the fixed 6-7 month rebound setup."""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


BANDS = ["0_0.5", "0.5_1", "1_2", "2_plus"]


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
        "max_up20_mean": float(frame.max_up20_pct.mean()),
        "max_up20_ge_10_rate": float((frame.max_up20_pct >= 10).mean()),
        "max_down20_mean": float(frame.max_down20_pct.mean()),
        "max_down20_le_minus5_rate": float((frame.max_down20_pct <= -5).mean()),
    }


def bootstrap_mean_ci(values, seed):
    values = np.asarray(values, dtype=float)
    if len(values) == 0:
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

    events = pd.read_parquet(a.events)
    selected = events.loc[events.range_months.isin([6, 7])].copy()
    selected["gu_band"] = pd.cut(
        selected.next_open_gap_pct,
        bins=[0, 0.5, 1, 2, np.inf],
        labels=BANDS,
        right=False,
        include_lowest=True,
    )
    if selected.gu_band.isna().any():
        raise ValueError("fixed target contains an unassigned GU magnitude")
    selected.to_parquet(a.output / "monthly_range_gu_magnitude_events.parquet", index=False)

    groups = {band: metrics(selected.loc[selected.gu_band.eq(band)]) for band in BANDS}
    years = {
        str(year): {band: metrics(rows.loc[rows.gu_band.eq(band)]) for band in BANDS}
        for year, rows in selected.groupby("year")
    }
    bootstrap = {
        band: bootstrap_mean_ci(selected.loc[selected.gu_band.eq(band), "ret20_pct"], 20260717 + i)
        for i, band in enumerate(BANDS)
    }
    gate_checks = {}
    tiers = {}
    for band in BANDS:
        row = groups[band]
        checks = {
            "n_ge_30": row["n"] >= 30,
            "ret20_mean_gt_zero": row["ret20_mean"] > 0,
            "ret20_positive_rate_ge_0_65": row["ret20_positive_rate"] >= 0.65,
            "deep_drawdown_rate_le_0_40": row["max_down20_le_minus5_rate"] <= 0.40,
        }
        gate_checks[band] = checks
        if all(checks.values()) and band == "0_0.5":
            tiers[band] = "starter"
        elif all(checks.values()):
            tiers[band] = "probe"
        else:
            tiers[band] = "avoid"

    result = {
        "schema_version": "tradex_monthly_range_gu_magnitude_v1.compare.v1",
        "artifact_role": "authoritative_monthly_range_gu_magnitude",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_setup": "monthly close range bottom, range_months=6-7, signal-day return<=-2%, next-session GU",
            "axis_changed": "next-session GU magnitude only",
            "gu_bands_pct": BANDS,
            "execution": "next-session open",
            "horizons": [5, 10, 20],
            "costs": "ignored_by_project_rule",
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "all_target_events": metrics(selected),
            "groups": groups,
            "years": years,
            "bootstrap_ret20_mean": bootstrap,
            "gate_checks": gate_checks,
            "operational_tiers": tiers,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "fixed setup split only by observed next-session GU magnitude",
            "group_counts": {band: groups[band]["n"] for band in BANDS},
        },
        "judgment": {
            "candidate_local_decision": {
                "0_0.5": "keep_starter",
                "0.5_1": "drop",
                "1_2": "keep_probe",
                "2_plus": "drop_high_drawdown",
            },
            "session_aggregate_decision": "keep_micro_gu_as_primary_confirmation",
            "authoritative_rollup_decision": "keep_0_to_0.5_gu_starter_and_1_to_2_gu_probe_review_only",
            "reason_type": "non_monotonic_gu_magnitude_with_drawdown_gate",
        },
        "not_changed": [
            "MeeMee box detector",
            "MeeMee display",
            "existing downside selector",
            "ranking",
            "runtime DB",
            "production logic",
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
        "band_counts": {band: groups[band]["n"] for band in BANDS},
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "monthly_range_gu_magnitude_events.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "groups": groups, "tiers": tiers}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
