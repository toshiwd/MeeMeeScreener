"""Join human/model agreement with frozen outcomes without changing the model."""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def outcome_stats(frame: pd.DataFrame) -> dict:
    completed = frame[frame.status.eq("complete")]
    return {
        "n": int(len(frame)),
        "D": int(completed.outcome_fixed3.eq("D").sum()),
        "R": int(completed.outcome_fixed3.eq("R").sum()),
        "N": int(completed.outcome_fixed3.eq("N").sum()),
        "mean_fixed3_pct": None if completed.empty else float(completed.return_fixed3_pct.mean()),
        "mean_h5_close_pct": None if completed.empty else float(completed.return_h5_close_pct.mean()),
        "max_loss_fixed3_pct": None if completed.empty else float(completed.return_fixed3_pct.min()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--annotations", type=Path, required=True)
    parser.add_argument("--agreement", type=Path, required=True)
    parser.add_argument("--outcomes", type=Path, required=True)
    parser.add_argument("--base-rollup", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    annotation_payload = json.loads(args.annotations.read_text(encoding="utf-8-sig"))
    annotations = pd.DataFrame(annotation_payload["annotations"])[["case_id", "reviewer_note", "reason_codes", "reviewed_at"]]
    agreement = pd.read_parquet(args.agreement / "human_agreement_ledger.parquet")
    outcomes = pd.read_parquet(args.outcomes / "outcome_reveal_ledger.parquet")
    joined = agreement.merge(
        outcomes[["case_id", "status", "evaluation_role", "outcome_fixed3", "return_fixed3_pct", "return_h5_close_pct", "mfe_short_5_pct", "mae_short_5_pct"]],
        on="case_id", validate="one_to_one",
    ).merge(annotations, on="case_id", validate="one_to_one")
    joined["human_direction"] = joined.human_new_entry_decision.map({"SELL": "SELL", "WAIT": "NO_SELL", "AVOID": "NO_SELL"}).fillna("")
    joined["model_direction"] = joined.model_action.map(
        lambda value: "SELL" if value in {"PROBE", "CORE", "ADD", "REENTRY_PROBE"} else "NO_SELL" if value == "AVOID" else "MANAGEMENT"
    )
    answered = joined[joined.human_direction.ne("")].copy()
    answered["direction_pair"] = answered.model_direction + "__" + answered.human_direction
    ledger_path = args.output / "human_model_outcome_ledger.parquet"
    joined.to_parquet(ledger_path, index=False)

    pair_stats = {pair: outcome_stats(group) for pair, group in answered.groupby("direction_pair")}
    human_sell = answered[answered.human_direction.eq("SELL")]
    human_no_sell = answered[answered.human_direction.eq("NO_SELL")]
    model_sell_answered = answered[answered.model_direction.eq("SELL")]
    model_avoid_answered = answered[answered.model_direction.eq("NO_SELL")]
    base = json.loads((args.base_rollup / "compare.json").read_text(encoding="utf-8"))
    agreement_compare = json.loads((args.agreement / "compare.json").read_text(encoding="utf-8"))
    primary_direction_agreement = agreement_compare["primary"]["direction_agreement"]
    result = {
        "schema_version": "tradex_blind_human_outcome_rollup_v1.compare.v1",
        "artifact_role": "authoritative_human_model_outcome_rollup",
        "review_only": True,
        "fixed_conditions": {
            "model_frozen_before_human_review": True,
            "agreement_contract_frozen_before_outcome_join": True,
            "execution": "next_session_open",
            "horizon_sessions": 5,
            "weekly_inputs": [],
            "costs": "ignored",
            "unanswered_fields_excluded_not_scored_as_mismatch": True,
        },
        "human_agreement": agreement_compare,
        "outcome_by_direction_pair": pair_stats,
        "human_sell_hypothetical": outcome_stats(human_sell),
        "human_no_sell_hypothetical_short": outcome_stats(human_no_sell),
        "model_sell_with_human_answer": outcome_stats(model_sell_answered),
        "model_avoid_with_human_answer": outcome_stats(model_avoid_answered),
        "model_standalone": base["authoritative_results"],
        "judgment": {
            "model_outcome_edge": "keep_review_only",
            "user_method_reproduction": "drop_current_action_mapping",
            "authoritative_rollup_decision": "hold_review_only_split_model_edge_from_user_replication",
            "reason": f"standalone CORE is 5D/0R and primary entries are positive, but primary human direction agreement is {100 * primary_direction_agreement:.1f}%; profitable model-only entries and weak human-only entries show that forcing agreement would change the tested edge rather than reproduce it safely",
        },
        "next_research_axis": {
            "axis": "support_and_downside_room_interpretation_on_direction_disagreements",
            "rule_change_allowed_now": True,
            "why": "blind outcomes and human notes are now frozen; analyze support proximity, MA slope/spacing, prior-low room, and overextension one axis at a time",
            "must_preserve": ["frozen benchmark", "weekly exclusion", "monthly selection then daily action", "review-only boundary"],
        },
        "not_changed": ["fixed benchmark model", "MeeMee", "ranking", "runtime DB", "production trading logic"],
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "annotations_sha256": sha(args.annotations),
        "agreement_compare_sha256": sha(args.agreement / "compare.json"),
        "outcome_compare_sha256": sha(args.outcomes / "compare.json"),
        "base_rollup_sha256": sha(args.base_rollup / "compare.json"),
        "joined_rows": len(joined), "direction_answered": len(answered),
        "outcome_ledger_sha256": sha(ledger_path),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "direction_pairs": pair_stats, "judgment": result["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
