"""Assemble the validated review-only monthly micro-GU operating rule."""
import argparse
import hashlib
import json
from pathlib import Path

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--liquidity-events", type=Path, required=True)
    ap.add_argument("--body-events", type=Path, required=True)
    ap.add_argument("--selection-compare", type=Path, required=True)
    ap.add_argument("--management-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    selected = pd.read_parquet(a.liquidity_events)
    selected = selected.loc[selected.price_ok & selected.liquidity_ok].copy()
    body = pd.read_parquet(a.body_events)[["code", "signal_ymd", "bearish_body_ratio"]]
    selected = selected.merge(body, on=["code", "signal_ymd"], validate="one_to_one")
    selected["volume_priority_tag"] = selected.volume_ratio_20d_median.ge(1.5)
    selected["body_priority_tag"] = selected.bearish_body_ratio.ge(0.4) & selected.bearish_body_ratio.lt(0.7)
    selected["priority_tag_count"] = (
        selected.volume_priority_tag.astype(int) + selected.body_priority_tag.astype(int)
    )
    selected["review_tier"] = selected.priority_tag_count.map(
        {0: "starter", 1: "starter_tagged", 2: "priority_review"}
    )
    selected.to_parquet(a.output / "operational_rule_event_ledger.parquet", index=False)

    tier_metrics = {
        tier: metrics(selected.loc[selected.review_tier.eq(tier)])
        for tier in ["starter", "starter_tagged", "priority_review"]
    }
    tag_interaction = {
        "neither": metrics(selected.loc[~selected.volume_priority_tag & ~selected.body_priority_tag]),
        "body_only": metrics(selected.loc[~selected.volume_priority_tag & selected.body_priority_tag]),
        "volume_only": metrics(selected.loc[selected.volume_priority_tag & ~selected.body_priority_tag]),
        "both": metrics(selected.loc[selected.volume_priority_tag & selected.body_priority_tag]),
    }
    management = json.loads(a.management_compare.read_text(encoding="utf-8"))
    management_result = management["authoritative_result"]
    checks = {
        "eligible_cohort_n_ge_15": len(selected) >= 15,
        "eligible_cohort_20d_positive_rate_ge_0.90": metrics(selected)["ret20_positive_rate"] >= 0.90,
        "five_pct_stop_preserves_all_in_sample_events": (
            management_result["fixed_stops"]["5"]["hit_count"] == 0
        ),
        "twenty_day_exit_dominates_five_and_ten_day_mean": (
            management_result["fixed_exits"]["20"]["mean"]
            > max(
                management_result["fixed_exits"]["5"]["mean"],
                management_result["fixed_exits"]["10"]["mean"],
            )
        ),
        "priority_review_sample_ge_10_for_size_escalation": tier_metrics["priority_review"]["n"] >= 10,
    }
    result = {
        "schema_version": "tradex_monthly_micro_gu_operational_rule_v1.compare.v1",
        "artifact_role": "authoritative_monthly_micro_gu_operational_rule",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "operational_rule": {
            "eligibility": [
                "monthly close range age 6-7 months at range bottom",
                "signal-day return <= -2%",
                "next-session GU >=0% and <0.5%",
                "next-session entry open >=1200 yen and <8000 yen",
                "previous 20-session median traded value >=2,000,000 yen",
            ],
            "entry": "review entry at next-session open after confirmed 0-0.5% GU",
            "hold": "fixed 20 trading sessions for research comparison",
            "risk": {
                "tight_stop_prohibited": "do not use fixed stops from -2% through -4%",
                "catastrophe_stop": "-5% is provisional and unvalidated for loss reduction",
                "reason": "the worst in-sample 20-session MAE was -4.90%; no cohort loser exists",
            },
            "priority_annotations": {
                "volume_priority_tag": "signal volume >=1.5x previous 20-session median",
                "body_priority_tag": "0.4<=bearish body ratio<0.7",
                "both_tags": "priority review only; no automatic size escalation",
            },
            "position_size": "not validated; no full-size claim",
            "production_status": "READY_REVIEW_ONLY",
        },
        "authoritative_result": {
            "eligible_cohort": metrics(selected),
            "tier_metrics": tier_metrics,
            "tag_interaction": tag_interaction,
            "management_summary": {
                "mae20": management_result["mae20"],
                "fixed_exits": management_result["fixed_exits"],
                "stop_hit_counts": {
                    stop: row["hit_count"] for stop, row in management_result["fixed_stops"].items()
                },
            },
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "final rule freezes the validated cohort and annotates priority without adding hard selection gates",
            "tier_counts": {tier: tier_metrics[tier]["n"] for tier in tier_metrics},
        },
        "judgment": {
            "candidate_local_decision": {
                "eligibility": "keep",
                "next_open_entry": "keep_review_only",
                "20_session_exit": "keep",
                "minus5_stop": "hold_provisional",
                "priority_tags": "keep_annotation_only",
                "size_escalation": "drop_insufficient_sample",
            },
            "session_aggregate_decision": "ready_review_only_operational_rule",
            "authoritative_rollup_decision": "READY_REVIEW_ONLY_monthly_micro_gu_starter_rule",
            "reason_type": "selection_and_fixed_exit_validated_stop_and_sizing_not_fully_validated",
        },
        "not_changed": [
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
            "automatic order execution",
            "position sizing",
        ],
        "remaining_risks": [
            "all fifteen final-cohort events are in-sample 20-session winners",
            "catastrophe stop lacks losing-event validation",
            "priority-review tier has only five events",
            "nominal price and traded value may be affected by corporate actions and data units",
            "monthly range definition remains research-fallback",
        ],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "liquidity_events": {"path": str(a.liquidity_events.resolve()), "sha256": sha(a.liquidity_events)},
            "body_events": {"path": str(a.body_events.resolve()), "sha256": sha(a.body_events)},
            "selection_compare": {"path": str(a.selection_compare.resolve()), "sha256": sha(a.selection_compare)},
            "management_compare": {"path": str(a.management_compare.resolve()), "sha256": sha(a.management_compare)},
        },
        "selected_events": int(len(selected)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "operational_rule_event_ledger.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "cohort": metrics(selected), "tiers": tier_metrics, "checks": checks, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
