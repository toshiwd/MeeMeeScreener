"""Split a frozen blind sample into a public reviewer bundle and private seal."""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--reviewer-output", type=Path, required=True)
    parser.add_argument("--sealed-output", type=Path, required=True)
    args = parser.parse_args()
    args.reviewer_output.mkdir(parents=True, exist_ok=False)
    args.sealed_output.mkdir(parents=True, exist_ok=False)

    source_compare = json.loads((args.source / "compare.json").read_text(encoding="utf-8"))
    source_complete = json.loads((args.source / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    if source_complete.get("sha256") != sha(args.source / "compare.json"):
        raise RuntimeError("source complete marker mismatch")

    board = pd.read_parquet(args.source / "blind_review_board.parquet")
    sealed = pd.read_parquet(args.source / "machine_annotation_sealed.parquet")
    key_columns = ["case_id", "code", "ymd"]
    if not board[key_columns].equals(sealed[key_columns]):
        raise RuntimeError("public/sealed key order mismatch")

    hidden_columns = ["monthly_selection_state", "new_entry_blocked_reason"]
    board = board.drop(columns=[column for column in hidden_columns if column in board.columns])
    forbidden = [
        column for column in board.columns
        if column in {"bucket", "model_action", "reason", "source_family", *hidden_columns}
        or column.startswith(("weekly_", "ret_close_", "down_exc_", "up_exc_"))
    ]
    if forbidden:
        raise RuntimeError(f"reviewer leakage columns: {forbidden}")
    if bool(board["outcome_joined"].any()):
        raise RuntimeError("outcomes already joined")

    board_path = args.reviewer_output / "blind_review_board.parquet"
    csv_path = args.reviewer_output / "blind_review_board.csv"
    sealed_path = args.sealed_output / "machine_annotation_sealed.parquet"
    board.to_parquet(board_path, index=False)
    board.to_csv(csv_path, index=False, encoding="utf-8-sig")
    sealed.to_parquet(sealed_path, index=False)

    display_payload = "|".join(
        f"{row.case_id}|{row.code}|{int(row.ymd)}|{row.display_hash}"
        for row in board.itertuples()
    )
    compare = dict(source_compare)
    compare["schema_version"] = "tradex_blind_review_sample_v1.compare.v3"
    compare["display_order_sha256"] = hashlib.sha256(display_payload.encode()).hexdigest()
    compare["fixed_conditions"] = dict(compare["fixed_conditions"])
    compare["fixed_conditions"]["sealed_annotation_not_in_reviewer_bundle"] = True
    compare["source_frozen_artifact_sha256"] = sha(args.source / "compare.json")
    compare_path = args.reviewer_output / "compare.json"
    compare_path.write_text(json.dumps(compare, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    audit = {
        "rows": len(board),
        "unique_codes": int(board.code.nunique()),
        "reviewer_forbidden_columns": forbidden,
        "outcome_joined": bool(board.outcome_joined.any()),
        "reviewer_board_sha256": sha(board_path),
        "sealed_annotation_sha256": sha(sealed_path),
        "source_artifact": str(args.source.resolve()),
        "source_compare_sha256": sha(args.source / "compare.json"),
        "public_private_key_parity": True,
    }
    (args.reviewer_output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.sealed_output / "seal_audit.json").write_text(
        json.dumps({"reviewer_bundle": str(args.reviewer_output.resolve()), **audit}, indent=2) + "\n",
        encoding="utf-8",
    )
    (args.reviewer_output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"reviewer_output": str(args.reviewer_output), "sealed_output": str(args.sealed_output), **audit}, indent=2))


if __name__ == "__main__":
    main()
