from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare", type=Path, required=True)
    parser.add_argument("--screenshot-audit", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--leaves", required=True, help="Comma-separated ensemble leaf ids")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--verdict", choices=("keep", "drop"), required=True)
    args = parser.parse_args()
    compare = json.loads(args.compare.read_text(encoding="utf-8"))
    audit = json.loads(args.screenshot_audit.read_text(encoding="utf-8"))
    leaves = [int(value) for value in args.leaves.split(",")]
    candidate = next(item for item in compare["screened_candidates"] if item.get("ensemble_leaves") == leaves)
    payload = {
        "schema_version": "tradex_tree_visual_review_decision_v1",
        "authoritative_result": True,
        "numeric_candidate_decision": candidate["candidate_local_decision"],
        "visual_source": {
            "screenshot_audit": str(args.screenshot_audit),
            "screenshot_judgment": audit["judgment"],
            "requested_sample_count": audit["requested_sample_count"],
            "exported_image_count": audit["exported_image_count"],
            "failed_capture_count": audit["failed_capture_count"],
        },
        "candidate": {"ensemble_leaves": candidate["ensemble_leaves"], "rules_by_leaf": candidate["rules_by_leaf"]},
        "visual_review": {
            "sample_periods": ["train", "validation", "test"],
            "take_profit_and_stop_loss_examples_reviewed": True,
            "shape_consistency": "pass" if args.verdict == "keep" else "fail",
            "reason": args.reason,
        },
        "candidate_local_decision": "keep_review_only" if args.verdict == "keep" else "drop_visual_incoherence",
        "authoritative_rollup_decision": "adoptable_shape_review_only" if args.verdict == "keep" else "no_candidate",
        "production_ranking_changed": False,
        "runtime_db_write": False,
    }
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
