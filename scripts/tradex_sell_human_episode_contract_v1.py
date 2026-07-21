"""Normalize frozen human sell annotations into a leakage-aware episode contract."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

SOURCE_DEFAULT = Path(r"G:\Tradex\sell_active_learning_annotations_v1\20260715T122500Z-sell-active-learning-annotations-v1\frozen_annotations.json")

ACTION_MAP = {
    "UNJUDGEABLE_OR_AVOID_NEW_SHORT": ("AVOID", "NONE"),
    "AVOID_NEW_SHORT": ("NO_ENTRY", "NONE"),
    "PROFIT_TAKE_SHORT": ("UNLABELED", "TAKE_PROFIT"),
    "ADD_SHORT": ("UNLABELED", "ADD_UNSPECIFIED"),
    "NEW_SHORT_WITH_REBOUND_RISK": ("CORE_CLOSE", "NONE"),
    "PROBE_SHORT_ONLY": ("PROBE", "NONE"),
    "AVOID_NEW_SHORT_AND_PROFIT_TAKE": ("NO_ENTRY", "TAKE_PROFIT"),
}

# Explicit wording in the review conversation distinguishes risk-based AVOID
# from a structurally insufficient NO_ENTRY even where the frozen legacy enum
# used the same AVOID_NEW_SHORT value.
NEW_ACTION_CASE_OVERRIDE = {
    "SELL-AL-02": "AVOID",
    "SELL-AL-08": "AVOID",
}

ROLE_MAP = {
    "long_ma_cluster_sideways_risk": "VETO",
    "ma200_short_location_risk": "VETO",
    "downside_room_to_support_risk": "VETO",
    "below_ma7_seven_bar_bottom_risk": "TAKE_PROFIT_REASON",
    "ma200_rejection_initial_short": "TRIGGER",
    "gap_down_prior_low_break_add_short": "ADD_TRIGGER",
    "new_short_structure_trigger": "TRIGGER",
    "large_drop_ma60_contact_rebound_risk": "RISK_WARNING",
    "unbroken_prior_low_zone_blocks_new_short": "VETO",
    "lower_wick_rejection_at_ma60_ma100": "VETO",
    "close_holds_above_ma60_ma100": "VETO",
    "probe_only_before_retry_failure": "SIZE_LIMIT",
    "failed_try_full_short_entry": "CORE_TRIGGER",
    "prior_interval_overextension_long_ma_support": "VETO",
    "full_erasure_bear_initial_short": "TRIGGER",
    "unbroken_ma100_profit_take": "TAKE_PROFIT_REASON",
    "bear_candle_inside_range_not_entry": "VETO",
    "monthly_post_box_breakout_consolidation": "ENVIRONMENT_EVIDENCE",
    "no_monthly_box_reentry": "ENVIRONMENT_EVIDENCE",
    "nearby_multitouch_price_band_blocks_short": "VETO",
    "monthly_box_ceiling_short_environment": "ENVIRONMENT_EVIDENCE",
    "full_erasure_probe_then_staged_add": "POSITION_PATH_EVIDENCE",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def environment(row: dict) -> dict:
    c = row.get("concepts", {})
    if c.get("monthly_post_box_breakout_consolidation") and c.get("no_monthly_box_reentry"):
        return {"monthly_regime": "POST_BOX_BREAKOUT_CONSOLIDATION", "location": "ABOVE_OLD_BOX", "confidence": "HUMAN_EXPLICIT"}
    if c.get("monthly_box_ceiling_short_environment"):
        return {"monthly_regime": "BOX", "location": "BOX_CEILING", "confidence": "HUMAN_EXPLICIT"}
    return {"monthly_regime": "UNLABELED", "location": "UNLABELED", "confidence": "NONE"}


def leakage(row: dict) -> str:
    status = row.get("blind_status", "BLIND_UNSPECIFIED")
    if status == "OUTCOME_AWARE_EXCLUDE_FROM_ACCURACY":
        return "IMITATION_ONLY_EXCLUDE_ACCURACY"
    if status.startswith("PARTIALLY_OUTCOME_AWARE"):
        return "PARTIAL_EXCLUDE_AFFECTED_FUTURE_ACTIONS"
    return "BLIND_UNSPECIFIED"


def normalize(row: dict) -> dict:
    new_action, position_action = ACTION_MAP[row["human_decision"]]
    new_action = NEW_ACTION_CASE_OVERRIDE.get(row["case_id"], new_action)
    reasons = [
        {"code": code, "role": ROLE_MAP.get(code, "UNMAPPED"), "timeframe": "MONTHLY" if ROLE_MAP.get(code) == "ENVIRONMENT_EVIDENCE" else "MULTI_TIMEFRAME"}
        for code, enabled in row.get("concepts", {}).items() if enabled
    ]
    return {
        "episode_id": row["case_id"], "code": row["code"], "decision_ymd": row["ymd"],
        "environment": environment(row),
        "position_state": "EXISTING_SHORT" if position_action != "NONE" else "UNKNOWN_OR_FLAT",
        "new_short_action": new_action, "position_action": position_action,
        "risk_flags": [r["code"] for r in reasons if r["role"] in {"RISK_WARNING", "SIZE_LIMIT"}],
        "reasons": reasons,
        "next_stage": {
            "preferred_initial_entry_ymd": row.get("preferred_initial_entry_ymd"),
            "preferred_initial_entry_window": row.get("preferred_initial_entry_window"),
            "preferred_full_entry_ymd": row.get("preferred_full_entry_ymd"),
            "next_day_management": row.get("next_day_management"),
        },
        "position_path": row.get("position_path", []),
        "leakage_status": leakage(row),
        "reason_text_usable": False,
        "data_reconciliation": row.get("data_reconciliation"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=SOURCE_DEFAULT)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    source = json.loads(args.source.read_text(encoding="utf-8"))
    rows = [normalize(r) for r in source["annotations"]]
    payload = {
        "schema_version": "tradex_sell_human_episode_contract_v1",
        "artifact_role": "authoritative_human_label_contract",
        "review_only": True,
        "label_policy": {
            "environment_unlabeled_is_not_ambiguous": True,
            "new_short_and_position_management_are_separate": True,
            "avoid_is_not_buy_add": True,
            "future_preferred_dates_do_not_relabel_original_date": True,
            "mojibake_reason_text_excluded": True,
        },
        "episodes": rows,
    }
    contract = args.output / "human_episode_contract.json"
    contract.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    monthly_labeled = sum(r["environment"]["monthly_regime"] != "UNLABELED" for r in rows)
    accuracy_eligible = sum(r["leakage_status"] == "BLIND_UNSPECIFIED" for r in rows)
    audit = {
        "source": str(args.source), "source_sha256": sha(args.source), "episodes": len(rows),
        "monthly_environment_labeled": monthly_labeled,
        "monthly_environment_unlabeled": len(rows) - monthly_labeled,
        "accuracy_eligible_provisional": accuracy_eligible,
        "composite_action_split_count": sum(r["new_short_action"] != "UNLABELED" and r["position_action"] != "NONE" for r in rows),
        "unmapped_reason_roles": sum(x["role"] == "UNMAPPED" for r in rows for x in r["reasons"]),
        "boundary": {"owner": "TRADEX", "review_only": True, "meemee_changed": False, "runtime_db_write": False},
    }
    audit_path = args.output / "audit.json"
    audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "human_episode_contract.json", "sha256": sha(contract)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "audit": audit}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
