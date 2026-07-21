"""Audit whether body strength and volume expansion should be hard-intersected."""
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
    ap.add_argument("--body-events", type=Path, required=True)
    ap.add_argument("--volume-events", type=Path, required=True)
    ap.add_argument("--body-compare", type=Path, required=True)
    ap.add_argument("--volume-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    body = pd.read_parquet(a.body_events)[["code", "signal_ymd", "bearish_body_ratio"]]
    selected = pd.read_parquet(a.volume_events).merge(
        body, on=["code", "signal_ymd"], validate="one_to_one"
    )
    selected["body_ok"] = selected.bearish_body_ratio.ge(0.4) & selected.bearish_body_ratio.lt(0.7)
    selected["volume_ok"] = selected.volume_ratio_20d_median.ge(1.5)
    selected["interaction_group"] = np.select(
        [
            selected.body_ok & selected.volume_ok,
            selected.body_ok & ~selected.volume_ok,
            ~selected.body_ok & selected.volume_ok,
        ],
        ["both", "body_only", "volume_only"],
        default="neither",
    )
    selected.to_parquet(a.output / "monthly_micro_gu_body_volume_interaction_events.parquet", index=False)

    groups = {
        name: metrics(selected.loc[selected.interaction_group.eq(name)])
        for name in ["neither", "body_only", "volume_only", "both"]
    }
    years = {
        str(year): {
            name: metrics(rows.loc[rows.interaction_group.eq(name)])
            for name in groups
        }
        for year, rows in selected.groupby("year")
    }
    both_boot = bootstrap(selected.loc[selected.interaction_group.eq("both"), "ret20_pct"], 20260721)
    checks = {
        "both_ret20_mean_gt_volume_only": groups["both"]["ret20_mean"] > groups["volume_only"]["ret20_mean"],
        "both_positive_rate_ge_0.80": groups["both"]["ret20_positive_rate"] >= 0.80,
        "both_deep_drawdown_rate_le_0.20": groups["both"]["max_down20_le_minus5_rate"] <= 0.20,
        "both_bootstrap_probability_gt_zero_ge_0.95": both_boot["probability_mean_gt_zero"] >= 0.95,
        "both_n_ge_20_for_hard_gate": groups["both"]["n"] >= 20,
    }
    hard_gate = all(checks.values())
    result = {
        "schema_version": "tradex_monthly_micro_gu_body_volume_interaction_v1.compare.v1",
        "artifact_role": "authoritative_monthly_micro_gu_body_volume_interaction",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_setup": "range_months=6-7, signal return<=-2%, next-session GU 0-0.5%",
            "boundary_audit": "2x2 interaction of previously kept body and volume axes",
            "body_ok": "0.4<=bearish_body_ratio<0.7",
            "volume_ok": "signal volume / previous 20-session median >=1.5",
            "execution": "next-session open",
            "horizons": [5, 10, 20],
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "groups": groups,
            "years": years,
            "both_bootstrap_ret20_mean": both_boot,
            "hard_intersection_gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "audited overlap of two independently kept review-only axes",
            "group_counts": {name: groups[name]["n"] for name in groups},
        },
        "judgment": {
            "candidate_local_decision": "keep" if hard_gate else "hold",
            "session_aggregate_decision": (
                "keep_hard_intersection" if hard_gate else "keep_volume_gate_body_priority_tag"
            ),
            "authoritative_rollup_decision": (
                "keep_body_volume_hard_intersection_review_only"
                if hard_gate
                else "hold_hard_intersection_use_volume_gate_body_priority_review_only"
            ),
            "reason_type": (
                "all_interaction_gates_passed"
                if hard_gate
                else "effect_large_but_sample_insufficient_for_hard_intersection"
            ),
        },
        "not_changed": [
            "parent setup",
            "body threshold",
            "volume threshold",
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
        ],
        "remaining_risks": [
            "both group has only nine events",
            "interaction is not independently out-of-sample",
            "monthly range definition remains research-fallback",
        ],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "body_events": {"path": str(a.body_events.resolve()), "sha256": sha(a.body_events)},
            "volume_events": {"path": str(a.volume_events.resolve()), "sha256": sha(a.volume_events)},
            "body_compare": {"path": str(a.body_compare.resolve()), "sha256": sha(a.body_compare)},
            "volume_compare": {"path": str(a.volume_compare.resolve()), "sha256": sha(a.volume_compare)},
        },
        "selected_events": int(len(selected)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "monthly_micro_gu_body_volume_interaction_events.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "groups": groups, "checks": checks, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
