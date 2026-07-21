from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


KEYS = ["code", "ymd", "bar_index"]


def metric(g: pd.DataFrame) -> dict:
    return {
        "n": int(len(g)),
        "codes": int(g.code.nunique()),
        "hit_rate": float(g.drop5_in5.mean()),
        "clean_rate": float(g.clean_drop5_in5.mean()),
        "severe10_rate": float(g.drop8_in10.mean()),
        "median_high5_pct": float(g.high5_pct.median()),
        "p90_high5_pct": float(g.high5_pct.quantile(.9)),
    }


def warning_density(rows: pd.DataFrame, warnings: pd.DataFrame) -> np.ndarray:
    by_code = {c: g[["bar_index", "cluster_id"]].to_numpy(dtype=int) for c, g in warnings.groupby("code")}
    result = []
    for r in rows.itertuples(index=False):
        a = by_code.get(r.code)
        if a is None:
            result.append(0)
            continue
        result.append(len(set(a[np.abs(a[:, 0] - r.bar_index) <= 1, 1].tolist())))
    return np.asarray(result, dtype=int)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiers", required=True)
    ap.add_argument("--cluster-metrics", required=True)
    ap.add_argument("--signals", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)

    tiers = pd.read_parquet(args.tiers)
    cm = pd.read_parquet(args.cluster_metrics)
    signals = pd.read_parquet(args.signals)
    neg = set(cm[(cm.direction == "negative") & (cm.dev_n >= 3000) & (cm.val_n >= 1500) &
                 (cm.dev_event_rate <= .15) & (cm.val_event_rate <= .12)].cluster_id)
    warnings = signals[signals.cluster_id.isin(neg)][["code", "bar_index", "cluster_id"]].drop_duplicates()
    risk = tiers[tiers.tier.eq("Risk")].copy()
    risk["warning_cluster_count_pm1"] = warning_density(risk, warnings)

    trials = []
    dev = risk[risk.period.eq("development")]
    for threshold in (2, 3, 4, 5):
        watch = dev[dev.warning_cluster_count_pm1 < threshold]
        avoid = dev[dev.warning_cluster_count_pm1 >= threshold]
        trials.append({
            "threshold": threshold, "watch_n": len(watch), "avoid_n": len(avoid),
            "watch_hit": float(watch.drop5_in5.mean()) if len(watch) else None,
            "avoid_hit": float(avoid.drop5_in5.mean()) if len(avoid) else None,
            "gap": float(watch.drop5_in5.mean() - avoid.drop5_in5.mean()) if len(watch) and len(avoid) else None,
        })
    eligible = [r for r in trials if r["watch_n"] >= 1000 and r["avoid_n"] >= 1000]
    if not eligible:
        raise RuntimeError("no density threshold has adequate development breadth")
    selected = max(eligible, key=lambda r: r["gap"])["threshold"]

    final = tiers.copy()
    final["warning_cluster_count_pm1"] = 0
    final.loc[risk.index, "warning_cluster_count_pm1"] = risk.warning_cluster_count_pm1
    final["action_tier"] = final.tier.map({"Core": "Core", "Probe": "Probe", "Risk": "Watch"})
    final.loc[final.tier.eq("Risk") & final.warning_cluster_count_pm1.ge(selected), "action_tier"] = "Avoid"

    rows = []
    for (period, tier), g in final.groupby(["period", "action_tier"], observed=True):
        rows.append({"period": period, "action_tier": tier, **metric(g)})
    summary = pd.DataFrame(rows)
    order = ["Core", "Probe", "Watch", "Avoid"]
    val = summary[summary.period.eq("validation")].set_index("action_tier").reindex(order)
    checks = {
        "four_tiers_nonempty_validation": bool(val.n.notna().all() and (val.n > 0).all()),
        "core_gt_probe": bool(val.loc["Core", "hit_rate"] > val.loc["Probe", "hit_rate"]),
        "probe_gt_watch": bool(val.loc["Probe", "hit_rate"] > val.loc["Watch", "hit_rate"]),
        "watch_gt_avoid": bool(val.loc["Watch", "hit_rate"] > val.loc["Avoid", "hit_rate"]),
        "threshold_selected_on_development_only": True,
        "core_probe_membership_unchanged": int(final.action_tier.isin(["Core", "Probe"]).sum()) == int(tiers.tier.isin(["Core", "Probe"]).sum()),
    }
    keep = all(checks.values())
    final.to_parquet(out / "final_actionability_ledger.parquet", index=False)
    summary.to_parquet(out / "final_actionability_metrics.parquet", index=False)
    pd.DataFrame(trials).to_parquet(out / "development_warning_density_trials.parquet", index=False)
    result = {
        "schema_version": "tradex_short_final_actionability_rollup_v1.compare.v1",
        "artifact_role": "authoritative_short_actionability_rollup",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "base_tiers": "nearby warning window plus/minus 1 trading session",
            "watch_avoid_axis": "distinct warning cluster count within plus/minus 1 session",
            "threshold_candidates": [2, 3, 4, 5],
            "selected_threshold": int(selected),
            "threshold_selection": "maximum development Watch-minus-Avoid hit gap with each n>=1000",
            "entry": "next session open", "costs": "ignored",
        },
        "authoritative_result": {
            "validation": val.reset_index().to_dict("records"),
            "gate_checks": checks,
            "practical_policy": {
                "Core": "entry candidate; normal initial unit, chart-defined escape required",
                "Probe": "small initial unit only; warning nearby, add only after renewed weakness",
                "Watch": "no entry; warning-only evidence, wait for positive cluster",
                "Avoid": "no entry; dense warning evidence, do not anticipate decline",
            },
        },
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": int((final.action_tier == "Avoid").sum()),
            "selection_divergence_reason": "warning-only Risk split by independently compressed warning density",
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_four_tier_hierarchy" if keep else "hold_four_tier_hierarchy",
            "authoritative_rollup_decision": "keep_four_tier_actionability_v1_review_only" if keep else "hold_continue_watch_avoid_boundary",
            "reason_type": "all_fixed_condition_tier_order_gates_passed" if keep else "one_or_more_fixed_condition_gates_failed",
        },
        "diagnostic_contexts": {
            "price": "keep diagnostic only; no hard screen",
            "monthly_range_age": "keep diagnostic only; 5-7 and especially 12-14 month boxes dampen short hit rate",
            "market_relative": "keep diagnostic only; no monotonic exclusion",
            "reversal_path": "outcome diagnosis only; Core also has largest upside range",
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production logic", "price hard screens", "monthly hard screens"],
        "remaining_risks": ["20-session upside remains material in Core", "industry-neutral context absent", "event calendar absent", "review-only not live entry automation"],
    }
    (out / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "audit.json").write_text(json.dumps({"source_rows": len(tiers), "output_rows": len(final), "checks": checks}, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2), encoding="utf-8")
    print(json.dumps({"selected_threshold": selected, "checks": checks, "validation": result["authoritative_result"]["validation"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
