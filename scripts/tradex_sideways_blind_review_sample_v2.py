from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

if __package__:
    from scripts import tradex_sideways_blind_review_sample_v1 as base
else:
    import tradex_sideways_blind_review_sample_v1 as base


SEED = "sideways-human-blind-review-120-v3-independent"


def build_independent_sample(candidates: pd.DataFrame, previous: pd.DataFrame):
    previous_codes = set(previous.code.astype(str).str.zfill(4))
    previous_events = set(zip(previous.code.astype(str).str.zfill(4), previous.ymd.astype(int)))
    eligible = candidates[~candidates.code.isin(previous_codes)].copy()
    base.SEED = SEED
    board, sealed = base.build_sample(eligible)
    overlap_codes = set(sealed.code).intersection(previous_codes)
    overlap_events = set(zip(sealed.code, sealed.ymd.astype(int))).intersection(previous_events)
    if overlap_codes or overlap_events:
        raise RuntimeError("independent sample overlaps prior freeze")
    return board, sealed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--previous-sealed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sealed-output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    args.sealed_output.mkdir(parents=True, exist_ok=False)
    previous = pd.read_parquet(args.previous_sealed)
    candidates = base.load_candidates(args.db)
    board, sealed = build_independent_sample(candidates, previous)
    board_path = args.output / "blind_review_board.parquet"
    sealed_path = args.sealed_output / "machine_labels_sealed.parquet"
    board.to_parquet(board_path, index=False)
    board.to_csv(args.output / "blind_review_board.csv", index=False, encoding="utf-8-sig")
    sealed.to_parquet(sealed_path, index=False)
    counts = sealed.sample_group.value_counts().to_dict()
    gates = {
        "rows_120": len(board) == 120,
        "unique_codes_120": board.code.nunique() == 120,
        "prior_code_overlap_0": len(set(sealed.code).intersection(set(previous.code.astype(str).str.zfill(4)))) == 0,
        "group_counts_40_each": all(int(counts.get(group, 0)) == count for group, count in base.GROUP_COUNTS.items()),
        "year_counts_20_each": all(int((sealed.year == year).sum()) == 20 for year in base.YEARS),
        "outcome_joined_false": not bool(board.outcome_joined.any()),
        "machine_columns_hidden_from_board": not bool({"sample_group", "sideways_state", "direction_efficiency", "slope_share"} & set(board.columns)),
    }
    if not all(gates.values()):
        raise RuntimeError(gates)
    compare = {
        "schema_version": "tradex_sideways_blind_review_sample_v2",
        "artifact_role": "authoritative_independent_outcome_blind_sideways_sample",
        "review_only": True, "status": "frozen_pending_human_sideways_review",
        "fixed_conditions": {
            "years": list(base.YEARS), "seed": SEED, "one_case_per_code": True,
            "selection_uses_future_outcomes": False, "prior_codes_excluded": True,
            "previous_sealed_sha256": base.sha(args.previous_sealed),
            "human_labels": ["SIDEWAYS", "NOT_SIDEWAYS", "BORDERLINE"],
        },
        "sealed_group_counts": {str(key): int(value) for key, value in counts.items()},
        "year_counts": {str(year): int((sealed.year == year).sum()) for year in base.YEARS},
        "gates": gates,
        "not_changed": ["sideways detector", "MeeMee", "ranking", "runtime DB", "trade rules", "day2 rule"],
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(compare, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "audit.json").write_text(json.dumps({
        "source_db": str(args.db), "source_db_sha256": base.sha(args.db),
        "board_sha256": base.sha(board_path), "sealed_sha256": base.sha(sealed_path), "gates": gates,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": base.sha(compare_path)}, indent=2) + "\n", encoding="utf-8")
    (args.sealed_output / "seal_audit.json").write_text(json.dumps({
        "reviewer_bundle": str(args.output), "sealed_sha256": base.sha(sealed_path), "outcome_joined": False,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "sealed": str(args.sealed_output), "gates": gates}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
