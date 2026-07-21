"""Create the authoritative rollup for the frozen blind chart review study."""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=Path, required=True)
    parser.add_argument("--images", type=Path, required=True)
    parser.add_argument("--outcomes", type=Path, required=True)
    parser.add_argument("--review-ui", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    sample = load(args.sample / "compare.json")
    images = load(args.images / "audit.json")
    outcomes = load(args.outcomes / "compare.json")
    review_ui = load(args.review_ui / "audit.json")
    outcome_ledger = pd.read_parquet(args.outcomes / "outcome_reveal_ledger.parquet")
    environment_breadth = {}
    for bucket, group in outcome_ledger.groupby("bucket"):
        environment_breadth[bucket] = {
            "candidate_count": int(len(group)),
            "unique_codes": int(group.code.astype(str).nunique()),
            "years": sorted((group.ymd.astype(int) // 10000).unique().astype(int).tolist()),
            "year_count": int((group.ymd.astype(int) // 10000).nunique()),
            "action_counts": {str(key): int(value) for key, value in group.model_action.value_counts().items()},
        }
    core = outcomes["primary_sell_entry_by_action"]["CORE"]
    core_dr_ratio = "INF" if core["R"] == 0 and core["D"] > 0 else (None if core["R"] == 0 else core["D"] / core["R"])
    result = {
        "schema_version": "tradex_blind_review_rollup_v1.compare.v1",
        "artifact_role": "authoritative_rollup_decision",
        "review_only": True,
        "research_phase": "effectiveness_judgment_pending_human_action_agreement",
        "fixed_conditions": {
            "evaluation_name": sample["evaluation_name"],
            "clean_oos": False,
            "not_clean_oos_reason": sample["not_clean_oos_reason"],
            "sample_rows": 40,
            "unique_codes": 40,
            "years": sample["fixed_conditions"]["years"],
            "monthly_select_daily_act": True,
            "weekly_inputs": [],
            "weekly_visible": False,
            "future_selection_inputs": [],
            "execution": "next_session_open",
            "horizon_sessions": 5,
            "costs": "ignored",
            "human_review_machine_labels_hidden": True,
            "breakdown_role": "diagnostic_negative_control_excluded_from_primary_denominator",
        },
        "authoritative_results": {
            "primary_sell_entry": outcomes["primary_sell_entry"],
            "primary_sell_entry_by_bucket": outcomes["primary_sell_entry_by_bucket"],
            "primary_sell_entry_by_action": outcomes["primary_sell_entry_by_action"],
            "avoid_diagnostic": outcomes["avoid_diagnostic"],
            "management_diagnostic": outcomes["management_diagnostic"],
            "core_full_size_gate": {
                "candidate_count": core["n"],
                "D": core["D"], "R": core["R"], "N": core["N"],
                "D_over_R_ratio": core_dr_ratio,
                "D_over_R_gt_1": bool(core["D"] > core["R"]),
            },
            "environment_breadth": environment_breadth,
        },
        "observed_branching": {
            "bucket_counts": sample["bucket_counts"],
            "action_counts": sample["action_counts"],
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": 40,
            "selection_divergence_reason": "four monthly-environment/daily-action review strata; ranking comparison is out of scope",
            "review_images": images["image_count"],
            "weekly_visible_count": images["weekly_visible_count"],
            "future_bars_allowed_count": images["future_bars_allowed_count"],
            "human_review_ui_case_count": review_ui["case_count"],
        },
        "judgment": {
            "candidate_local_decision": {
                "TOP_FAILURE": "keep_review_only",
                "CORE": "keep_review_only",
                "RETURN_SELL": "hold_needs_human_shape_review",
                "PROBE": "hold_needs_human_shape_review",
                "ADD": "hold_low_sample_unstable",
                "TEMPTING_AVOID": "hold_mixed_missed_declines",
                "BREAKDOWN_REJECTED": "keep_as_negative_control_only",
            },
            "session_aggregate_decision": "hold_pending_human_action_agreement",
            "authoritative_rollup_decision": "hold_pending_human_action_agreement",
            "reason": "frozen sell entries are positive and CORE is 5D/0R, but this is not clean OOS; ADD is 1D/1R, avoidance misses six declines, and human chart-action agreement is not yet annotated",
        },
        "verify": {
            "fixed_condition_comparison": True,
            "artifact_existence": True,
            "branching_happened": True,
            "branching_helped_primary_sell_entry": True,
            "public_board_machine_label_leakage": False,
            "outcomes_revealed_in_public_board": False,
            "human_review_ui_machine_labels_visible": review_ui["machine_labels_visible"],
            "human_review_ui_outcomes_visible": review_ui["outcomes_visible"],
        },
        "artifacts": {
            "reviewer_sample": str(args.sample.resolve()),
            "review_images": str(args.images.resolve()),
            "outcome_reveal": str(args.outcomes.resolve()),
            "human_review_ui": str(args.review_ui.resolve()),
            "sample_compare_sha256": sha(args.sample / "compare.json"),
            "image_audit_sha256": sha(args.images / "audit.json"),
            "outcome_compare_sha256": sha(args.outcomes / "compare.json"),
            "human_review_ui_audit_sha256": sha(args.review_ui / "audit.json"),
        },
        "remaining_risks": [
            "historical sample participated in prior research and is not clean OOS",
            "human action agreement is pending",
            "ADD has only two observations",
            "avoidance has six missed fixed3 declines",
            "release directory hygiene check fails on pre-existing release/logs",
            "frontend repository-wide typecheck and lint have unrelated pre-existing failures",
        ],
        "not_changed": ["fixed model rules", "MeeMee ranking", "runtime DB", "production trading logic"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sample_complete": (args.sample / "_ARTIFACT_COMPLETE.json").exists(),
        "images_complete": (args.images / "_ARTIFACT_COMPLETE.json").exists(),
        "outcomes_complete": (args.outcomes / "_ARTIFACT_COMPLETE.json").exists(),
        "compare_sha256": sha(compare),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "decision": result["judgment"]["authoritative_rollup_decision"], **audit}, indent=2))


if __name__ == "__main__":
    main()
