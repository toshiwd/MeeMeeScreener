from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "short_entry_timing_watch_board_v1"
DEFAULT_PROVISIONAL_SCAN = Path(
    r"G:\Tradex\short_entry_timing_rule_probe_v1\current_candidates\latest_provisional_entry_timing_candidates.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\current_candidates")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _verdict(row: dict[str, Any]) -> tuple[str, int]:
    rules = row.get("matched_rules", [])
    features = row.get("numeric_features", {})
    primary = any(rule.get("review_strength") == "primary" for rule in rules)
    multi = len(rules) >= 2
    close_vs_ma20 = float(features.get("close_vs_ma20", 0.0))
    close_vs_ma7 = float(features.get("close_vs_ma7", 0.0))
    volume_ratio20 = float(features.get("volume_ratio20", 0.0))
    if close_vs_ma20 >= 0.18 or close_vs_ma7 >= 0.12:
        verdict = "avoid_too_extended_provisional_blowoff"
    elif multi and primary:
        verdict = "top_watch_rejection_needed"
    elif primary and volume_ratio20 <= 1.3:
        verdict = "watch_rejection_needed"
    elif primary:
        verdict = "thin_watch_volume_high"
    else:
        verdict = "secondary_watch_only"
    score = (2 if multi else 0) + (2 if primary else 0) - (3 if "avoid" in verdict else 0) - (1 if volume_ratio20 > 1.3 else 0)
    return verdict, score


def run(*, provisional_scan_path: Path, output_root: Path) -> Path:
    scan = json.loads(provisional_scan_path.read_text(encoding="utf-8"))
    rows = []
    for row in scan.get("current_candidates", []):
        verdict, score = _verdict(row)
        rows.append({**row, "provisional_board_verdict": verdict, "board_score": score})
    rows.sort(
        key=lambda row: (
            row["board_score"],
            max(rule["oos_reference"]["entry_now_rate"] for rule in row.get("matched_rules", [])),
        ),
        reverse=True,
    )
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "source_artifact": str(provisional_scan_path),
        "provisional_as_of": scan.get("provisional_as_of"),
        "row_count": len(rows),
        "rows": rows,
        "decision": {
            "candidate_local_decision": "provisional_watchlist_present_no_confirmed_entry" if rows else "no_provisional_watch_candidate",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "provisional scan converted to review-only watch board with extension guardrails",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "provisional_watch_board.json", report)
    _write_json(output_root / "latest_provisional_watch_board.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provisional-scan", type=Path, default=DEFAULT_PROVISIONAL_SCAN)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(provisional_scan_path=args.provisional_scan, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
