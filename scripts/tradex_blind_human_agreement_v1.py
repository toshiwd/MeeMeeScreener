"""Score frozen human annotations against the sealed model actions."""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


SELL_ACTIONS = {"PROBE", "CORE", "ADD", "REENTRY_PROBE"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def expected(action: str) -> dict:
    if action == "AVOID":
        return {"direction": "NO_SELL", "stages": {"NA"}, "management": {"NA"}}
    if action == "PROBE":
        return {"direction": "SELL", "stages": {"PROBE"}, "management": {"NA", "HOLD"}}
    if action == "CORE":
        return {"direction": "SELL", "stages": {"CORE"}, "management": {"NA", "HOLD"}}
    if action == "ADD":
        return {"direction": "SELL", "stages": {"ADD"}, "management": {"HOLD"}}
    if action == "REENTRY_PROBE":
        return {"direction": "SELL", "stages": {"PROBE"}, "management": {"REENTRY"}}
    if action == "TAKE_PROFIT_FULL_HEDGE":
        return {"direction": "MANAGEMENT", "stages": {"NA"}, "management": {"TAKE_PROFIT", "FULL_HEDGE"}}
    raise ValueError(action)


def rate(series: pd.Series) -> float | None:
    return None if len(series) == 0 else float(series.mean())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--annotations", type=Path, required=True)
    parser.add_argument("--sealed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    payload = json.loads(args.annotations.read_text(encoding="utf-8-sig"))
    if payload.get("schema_version") != "tradex_blind_human_annotation_v1":
        raise RuntimeError("annotation schema mismatch")
    human = pd.DataFrame(payload.get("annotations", []))
    required = ["case_id", "code", "ymd", "new_entry_decision", "existing_short_management", "entry_stage", "confidence"]
    missing = [column for column in required if column not in human.columns]
    if missing or len(human) != 40:
        raise RuntimeError(f"40 board rows required; missing_columns={missing}")
    for column in required[3:]:
        human[column] = human[column].fillna("").astype(str).str.strip()
    if human.case_id.duplicated().any():
        raise RuntimeError("duplicate case_id")

    sealed = pd.read_parquet(args.sealed)
    sealed.code = sealed.code.astype(str).str.zfill(4)
    human.code = human.code.astype(str).str.zfill(4)
    joined = sealed.merge(human, on=["case_id", "code", "ymd"], how="inner", validate="one_to_one")
    if len(joined) != 40:
        raise RuntimeError("human/sealed key parity failed")

    scored = []
    for row in joined.itertuples():
        contract = expected(row.model_action)
        human_direction = "SELL" if row.new_entry_decision == "SELL" else "NO_SELL"
        direction_required = contract["direction"] != "MANAGEMENT"
        direction_applicable = direction_required and bool(row.new_entry_decision)
        direction_match = None if not direction_applicable else human_direction == contract["direction"]
        stage_required = row.model_action in SELL_ACTIONS
        stage_applicable = stage_required and bool(row.entry_stage)
        stage_match = None if not stage_applicable else row.entry_stage in contract["stages"]
        management_required = row.model_action in {"ADD", "REENTRY_PROBE", "TAKE_PROFIT_FULL_HEDGE"}
        management_applicable = management_required and bool(row.existing_short_management)
        management_match = None if not management_applicable else row.existing_short_management in contract["management"]
        required_values = []
        if direction_required: required_values.append(direction_match)
        if stage_required: required_values.append(stage_match)
        if management_required: required_values.append(management_match)
        exact = None if any(value is None for value in required_values) else all(required_values)
        scored.append({
            "case_id": row.case_id, "code": row.code, "ymd": int(row.ymd),
            "bucket": row.bucket, "model_action": row.model_action,
            "human_new_entry_decision": row.new_entry_decision,
            "human_existing_short_management": row.existing_short_management,
            "human_entry_stage": row.entry_stage, "human_confidence": row.confidence,
            "direction_match": direction_match, "stage_match": stage_match,
            "management_match": management_match, "exact_action_match": exact,
        })
    ledger = pd.DataFrame(scored)
    ledger_path = args.output / "human_agreement_ledger.parquet"
    ledger.to_parquet(ledger_path, index=False)

    def summary(frame: pd.DataFrame) -> dict:
        direction = frame.direction_match.dropna().astype(bool)
        stage = frame.stage_match.dropna().astype(bool)
        management = frame.management_match.dropna().astype(bool)
        exact = frame.exact_action_match.dropna().astype(bool)
        return {
            "n": len(frame),
            "annotated_cases_any": int(frame[["human_new_entry_decision", "human_existing_short_management", "human_entry_stage", "human_confidence"]].astype(bool).any(axis=1).sum()),
            "direction_applicable": len(direction), "direction_agreement": rate(direction),
            "stage_applicable": len(stage), "stage_agreement": rate(stage),
            "management_applicable": len(management), "management_agreement": rate(management),
            "exact_action_applicable": len(exact), "exact_action_agreement": rate(exact),
        }

    primary = ledger[ledger.bucket != "BREAKDOWN_REJECTED"]
    compare = {
        "schema_version": "tradex_blind_human_agreement_v1.compare.v1",
        "artifact_role": "authoritative_human_action_agreement",
        "review_only": True,
        "fixed_conditions": {
            "agreement_contract_frozen_before_user_annotations": True,
            "breakdown_role": "diagnostic_negative_control_excluded_from_primary",
            "weekly_inputs": [], "outcomes_used": False,
        },
        "primary": summary(primary),
        "all_cases": summary(ledger),
        "by_bucket": {bucket: summary(group) for bucket, group in ledger.groupby("bucket")},
        "by_model_action": {action: summary(group) for action, group in ledger.groupby("model_action")},
        "judgment": {"decision": "agreement_recorded_pending_joint_outcome_rollup"},
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(compare, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "annotations_sha256": sha(args.annotations), "sealed_sha256": sha(args.sealed),
        "rows": len(ledger), "key_parity": True, "outcomes_used": False,
        "partial_annotations_allowed": True,
        "ledger_sha256": sha(ledger_path),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), **compare["primary"]}, indent=2))


if __name__ == "__main__":
    main()
