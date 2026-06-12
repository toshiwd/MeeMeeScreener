from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.current_short_regime_permission_board_v1 import classify, load_regime_rows
from scripts.tradex_short_continuation_strength_stability_v1 import _json_ready, _summary


AXIS_ID = "short_early_impulse6_regime_gate_v1"
DEFAULT_SOURCE = Path(
    r"G:\Tradex\short_visual_setup_to_continuation_replay_v1"
    r"\20260605T023154Z-short_visual_setup_to_continuation_replay_v1"
    r"\visual_setup_to_continuation_replay.json"
)
DEFAULT_REGIME_DB = Path(
    r"G:\Tradex\market_regime_artifact_rebuild_and_rerun_v1"
    r"\20260605T004630Z-market_regime_artifact_rebuild_and_rerun_v1"
    r"\stocks_regime_rebuild_copy.duckdb"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_early_impulse6_regime_gate_v1")


def build_payload(source: dict[str, Any], source_path: Path, regime_db_path: Path) -> dict[str, Any]:
    challenger = [
        dict(row)
        for row in source.get("events", [])
        if row.get("setup_state") == "SetupReady"
        and row.get("to_visual_continuation_permit")
        and row.get("early_bucket") == "EarlyImpulse6NoDenial"
    ]
    dates = {int(row["signal_ymd"]) for row in challenger}
    regime_rows, regime_meta = load_regime_rows(dates, regime_db_path)
    classified: list[dict[str, Any]] = []
    for row in challenger:
        status, reason = classify(row, regime_rows.get(int(row["signal_ymd"])))
        classified.append({**row, "regime_permission_status": status, "regime_permission_reason": reason})
    states = {
        status: _summary([row for row in classified if row["regime_permission_status"] == status], status)
        for status in ("PermitShort", "BlockShort", "Avoid", "RegimeMissing")
    }
    covered = [row for row in classified if row["regime_permission_status"] != "RegimeMissing"]
    coverage_ratio = len(covered) / len(classified) if classified else 0.0
    permit = states["PermitShort"]
    block = states["BlockShort"]
    avoid = states["Avoid"]
    enough_coverage = coverage_ratio >= 0.80 and len(covered) >= 15
    enough_branching = permit.get("n", 0) >= 5 and (block.get("n", 0) + avoid.get("n", 0)) >= 5
    gate_helped = bool(
        enough_coverage
        and enough_branching
        and permit.get("mean_short_ret", -1.0) > block.get("mean_short_ret", 1.0)
        and permit.get("win_rate", 0.0) > block.get("win_rate", 1.0)
    )
    return {
        "axis_id": AXIS_ID,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_path": str(source_path),
        "regime_db_path": str(regime_db_path),
        "research_phase": "effectiveness judgment",
        "fixed_evaluation_conditions": {
            "base_candidate": "SetupReady -> ContinuationPermit -> EarlyImpulse6NoDenial",
            "changed_axis": "existing fixed market regime permission classification only",
            "entry_exit_costs_and_universe": "inherit source replay unchanged",
        },
        "coverage": {
            "total_candidates": len(classified),
            "covered_candidates": len(covered),
            "coverage_ratio": coverage_ratio,
            "required_coverage_ratio": 0.80,
            "regime_meta": regime_meta,
        },
        "states": states,
        "research_decision": {
            "candidate_local_decision": "keep" if gate_helped else "hold",
            "session_aggregate_decision": "keep_regime_gate_review_only" if gate_helped else "hold_regime_gate_insufficient_evidence",
            "authoritative_rollup_decision": "KEEP_REVIEW_ONLY" if gate_helped else "HOLD_REVIEW_ONLY",
            "production_promotion_allowed": False,
            "reason": (
                "Existing market regime gate improved EarlyImpulse6 profitability with sufficient coverage and branching."
                if gate_helped
                else "Existing market regime gate lacks sufficient covered branching or did not improve EarlyImpulse6 profitability."
            ),
        },
        "classified_examples": classified,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }


def run(source_path: Path, regime_db_path: Path, output_root: Path) -> Path:
    payload = build_payload(json.loads(source_path.read_text(encoding="utf-8")), source_path, regime_db_path)
    run_dir = output_root / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}")
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "compare.json").write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (run_dir / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"status": "complete", "authoritative_artifact": "compare.json"}, indent=2) + "\n",
        encoding="utf-8",
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--regime-db-path", type=Path, default=DEFAULT_REGIME_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.source_path, args.regime_db_path, args.output_root))


if __name__ == "__main__":
    main()
