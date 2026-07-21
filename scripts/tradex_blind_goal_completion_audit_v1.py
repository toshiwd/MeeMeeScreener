"""Audit every explicit requirement of the monthly-to-daily blind-review goal."""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=Path, required=True)
    parser.add_argument("--images", type=Path, required=True)
    parser.add_argument("--review-ui", type=Path, required=True)
    parser.add_argument("--outcomes", type=Path, required=True)
    parser.add_argument("--rollup", type=Path, required=True)
    parser.add_argument("--agreement", type=Path)
    parser.add_argument("--joint-rollup", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    sample = load(args.sample / "compare.json")
    images = load(args.images / "audit.json")
    ui = load(args.review_ui / "audit.json")
    outcomes = load(args.outcomes / "compare.json")
    rollup = load(args.rollup / "compare.json")
    agreement_present = bool(args.agreement and (args.agreement / "compare.json").exists())
    agreement = load(args.agreement / "compare.json") if agreement_present else None
    joint_present = bool(args.joint_rollup and (args.joint_rollup / "compare.json").exists())
    joint = load(args.joint_rollup / "compare.json") if joint_present else None
    excluded = set(sample["fixed_conditions"]["excluded_codes"])
    required_actions = {"AVOID", "PROBE", "CORE", "ADD", "TAKE_PROFIT_FULL_HEDGE", "REENTRY_PROBE"}
    action_counts = sample["action_counts"]
    breadth = rollup["authoritative_results"]["environment_breadth"]
    risk = rollup["authoritative_results"]["primary_sell_entry"]

    requirements = [
        {"id": "review_only", "status": "pass" if sample["review_only"] and outcomes["review_only"] and rollup["review_only"] else "fail"},
        {"id": "weekly_completely_excluded", "status": "pass" if sample["fixed_conditions"]["weekly_inputs"] == [] and images["weekly_visible_count"] == 0 and rollup["fixed_conditions"]["weekly_inputs"] == [] else "fail"},
        {"id": "monthly_selection_daily_action_fixed", "status": "pass" if rollup["fixed_conditions"]["monthly_select_daily_act"] else "fail"},
        {"id": "teacher_and_known_adjustments_excluded", "status": "pass" if {"9107", "7733", "3405", "4208", "7004", "9531", "4188", "5631"}.issubset(excluded) else "fail", "excluded_codes": sorted(excluded)},
        {"id": "blind_cases_at_least_30", "status": "pass" if sample["gates"]["rows_40"] and sample["gates"]["unique_codes_40"] else "fail", "cases": len(sample["case_ids"])},
        {"id": "four_environment_strata_10_each", "status": "pass" if all(sample["bucket_counts"].get(bucket) == 10 for bucket in ["TOP_FAILURE", "RETURN_SELL", "BREAKDOWN_REJECTED", "TEMPTING_AVOID"]) else "fail", "counts": sample["bucket_counts"]},
        {"id": "all_required_actions_represented", "status": "pass" if required_actions.issubset(action_counts) else "fail", "counts": action_counts},
        {"id": "future_blind_selection", "status": "pass" if sample["fixed_conditions"]["selection_uses_future_outcomes"] is False and sample["gates"]["outcome_joined_false"] else "fail"},
        {"id": "next_open_five_session_outcomes", "status": "pass" if outcomes["fixed_conditions"]["execution"] == "next_session_open" and outcomes["fixed_conditions"]["horizon_sessions"] == 5 else "fail"},
        {"id": "human_review_board_confirmable", "status": "pass" if ui["case_count"] == 40 and not ui["machine_labels_visible"] and not ui["outcomes_visible"] and not ui["weekly_visible"] else "fail"},
        {"id": "actual_human_action_agreement_recorded", "status": "pass" if agreement_present and agreement["primary"]["direction_applicable"] > 0 else "pending_user_annotation", "evidence": None if not agreement_present else agreement["primary"]},
        {"id": "human_model_outcome_joint_rollup_recorded", "status": "pass" if joint_present and joint["judgment"]["authoritative_rollup_decision"] else "fail"},
        {"id": "core_full_size_D_over_R_gt_1", "status": "pass" if rollup["authoritative_results"]["core_full_size_gate"]["D_over_R_gt_1"] else "fail", "evidence": rollup["authoritative_results"]["core_full_size_gate"]},
        {"id": "environment_breadth_recorded", "status": "pass" if len(breadth) == 4 and all(value["candidate_count"] >= 10 and value["unique_codes"] >= 10 for value in breadth.values()) else "fail", "evidence": breadth},
        {"id": "risk_metrics_recorded", "status": "pass" if all(key in risk for key in ["max_loss_pct", "max_loss_streak", "max_drawdown_units_pct", "max_concurrent"]) else "fail", "evidence": {key: risk.get(key) for key in ["max_loss_pct", "max_loss_streak", "max_drawdown_units_pct", "max_concurrent"]}},
        {"id": "no_automatic_meemee_ranking_runtime_reflection", "status": "pass" if {"MeeMee ranking", "runtime DB", "production trading logic"}.issubset(set(rollup["not_changed"])) else "fail"},
        {"id": "rules_unchanged_until_blind_result_frozen", "status": "pass" if joint_present and joint["fixed_conditions"]["model_frozen_before_human_review"] and "fixed benchmark model" in joint["not_changed"] else "fail"},
    ]
    blocking = [item["id"] for item in requirements if item["status"] != "pass"]
    result = {
        "schema_version": "tradex_blind_goal_completion_audit_v1",
        "artifact_role": "authoritative_goal_completion_audit",
        "goal_complete": not blocking,
        "requirements": requirements,
        "blocking_requirements": blocking,
        "judgment": "complete" if not blocking else "hold",
        "artifact_hashes": {
            "sample_compare": sha(args.sample / "compare.json"),
            "image_audit": sha(args.images / "audit.json"),
            "review_ui_audit": sha(args.review_ui / "audit.json"),
            "outcome_compare": sha(args.outcomes / "compare.json"),
            "rollup_compare": sha(args.rollup / "compare.json"),
            "agreement_compare": None if not agreement_present else sha(args.agreement / "compare.json"),
            "joint_rollup_compare": None if not joint_present else sha(args.joint_rollup / "compare.json"),
        },
    }
    audit_path = args.output / "completion_audit.json"
    audit_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "completion_audit.json", "sha256": sha(audit_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "goal_complete": result["goal_complete"], "blocking_requirements": blocking}, indent=2))


if __name__ == "__main__":
    main()
