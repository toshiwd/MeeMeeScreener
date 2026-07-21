from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


VALID_DECISIONS = {"SIDEWAYS", "NOT_SIDEWAYS", "BORDERLINE"}
VALID_CONFIDENCE = {"LOW", "MEDIUM", "HIGH"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_annotations(board: pd.DataFrame, payload: dict) -> pd.DataFrame:
    annotations = pd.DataFrame(payload.get("annotations", []))
    required = {"case_id", "code", "ymd", "sideways_decision", "confidence"}
    missing = required - set(annotations.columns)
    if missing:
        raise RuntimeError(f"annotation columns missing: {sorted(missing)}")
    annotations.code = annotations.code.astype(str).str.zfill(4)
    annotations.ymd = annotations.ymd.astype(int)
    board = board.copy()
    board.code = board.code.astype(str).str.zfill(4)
    if annotations.case_id.duplicated().any():
        raise RuntimeError("duplicate case_id")
    joined = board[["case_id", "code", "ymd"]].merge(annotations, on=["case_id", "code", "ymd"], how="left", validate="one_to_one")
    invalid_decision = joined[~joined.sideways_decision.isin(VALID_DECISIONS)]
    invalid_confidence = joined[~joined.confidence.isin(VALID_CONFIDENCE)]
    if len(invalid_decision):
        raise RuntimeError(f"incomplete/invalid sideways cases: {invalid_decision.case_id.tolist()}")
    if len(invalid_confidence):
        raise RuntimeError(f"incomplete/invalid confidence cases: {invalid_confidence.case_id.tolist()}")
    if len(joined) != len(board) or len(annotations) != len(board):
        raise RuntimeError("annotation key parity failed")
    return joined


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--board", type=Path, required=True)
    parser.add_argument("--annotations", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    board = pd.read_parquet(args.board)
    payload = json.loads(args.annotations.read_text(encoding="utf-8"))
    joined = validate_annotations(board, payload)
    ledger = args.output / "human_sideways_frozen.parquet"
    joined.to_parquet(ledger, index=False)
    result = {
        "schema_version": "tradex_sideways_blind_freeze_v1.compare.v1", "artifact_role": "authoritative_outcome_free_human_sideways_freeze",
        "review_only": True, "status": "frozen_before_machine_label_and_outcome_join", "rows": len(joined),
        "decision_counts": {str(k): int(v) for k, v in joined.sideways_decision.value_counts().items()},
        "confidence_counts": {str(k): int(v) for k, v in joined.confidence.value_counts().items()},
        "fixed_conditions": {"outcomes_loaded": False, "machine_labels_loaded": False, "weekly_inputs": []},
        "judgment": {"decision": "keep_frozen_pending_reveal"}, "not_changed": ["sideways detector", "MeeMee", "ranking", "runtime DB", "trade rules"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "audit.json").write_text(json.dumps({"board_sha256": sha(args.board), "annotations_sha256": sha(args.annotations), "ledger_sha256": sha(ledger), "key_parity": True, "invalid_count": 0, "outcome_columns_present": False, "machine_columns_present": False}, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), **result}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
