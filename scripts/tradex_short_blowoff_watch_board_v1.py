from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "short_blowoff_watch_board_v1"
DEFAULT_CURRENT_SCAN = Path(
    r"G:\Tradex\short_watch_to_entry_retest_probe_v1\current_scan\latest_current_short_blowoff_candidates.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_watch_to_entry_retest_probe_v1\current_scan")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _verdict(row: dict[str, Any]) -> tuple[str, int]:
    rules = row.get("matched_rules", [])
    has_strict = any(rule.get("profile") == "strict_core" for rule in rules)
    has_n20 = any(rule.get("profile") == "n20_main" for rule in rules)
    rr_to_low20 = float(row.get("rr_to_low20") or 0.0)
    entry_vs_ma7_pct = float(row.get("entry_vs_ma7_pct") or 0.0)
    entry_vs_high60_pct = float(row.get("entry_vs_high60_pct") or 0.0)

    if has_strict:
        verdict = "strict_core_short_review"
    elif has_n20:
        verdict = "n20_main_short_review"
    else:
        verdict = "research_watch_only"

    score = 0
    score += 5 if has_strict else 0
    score += 3 if has_n20 else 0
    score += 1 if rr_to_low20 >= 1.5 else 0
    score += 1 if entry_vs_ma7_pct < 0 else 0
    score += 1 if entry_vs_high60_pct <= -3.0 else 0
    return verdict, score


def _entry_contract(row: dict[str, Any], verdict: str) -> dict[str, Any]:
    entry_c = float(row.get("entry_c") or 0.0)
    peak_h = float(row.get("peak_h") or 0.0)
    stop_buffer_pct = 0.005 if verdict == "strict_core_short_review" else 0.01
    target_down_pct = 0.05
    return {
        "review_only": True,
        "entry_basis": "confirmed_entry_date_close_from_research_scan",
        "entry_price_reference": entry_c or None,
        "target_policy": "first_touch_down_5pct_within_20_sessions",
        "target_price_reference": round(entry_c * (1.0 - target_down_pct), 2) if entry_c else None,
        "stop_policy": "peak_high_plus_buffer",
        "stop_buffer_pct": round(stop_buffer_pct * 100, 2),
        "stop_price_reference": round(peak_h * (1.0 + stop_buffer_pct), 2) if peak_h else None,
        "holding_horizon_sessions": 20,
        "position_policy": "probe_only_until_manual_chart_review",
        "invalid_if": [
            "price breaks stop_price_reference before target",
            "manual chart review rejects high-zone peak failure shape",
            "latest confirmed bar no longer matches rule on next refresh",
        ],
    }


def run(*, current_scan_path: Path, output_root: Path) -> Path:
    scan = json.loads(current_scan_path.read_text(encoding="utf-8"))
    rows = []
    for row in scan.get("current_candidates", []):
        verdict, score = _verdict(row)
        rows.append(
            {
                **row,
                "board_verdict": verdict,
                "board_score": score,
                "actionability": "review_only",
                "entry_contract": _entry_contract(row, verdict),
            }
        )
    rows.sort(
        key=lambda row: (
            row["board_score"],
            any(rule.get("profile") == "strict_core" for rule in row.get("matched_rules", [])),
            row.get("rr_to_low20") or 0,
        ),
        reverse=True,
    )
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_axis_id": scan.get("research_axis_id"),
        "boundary_owner": "TRADEX",
        "source_artifact": str(current_scan_path),
        "confirmed_as_of": scan.get("confirmed_as_of"),
        "confirmed_source_policy": scan.get("confirmed_source_policy"),
        "row_count": len(rows),
        "rows": rows,
        "blocked_near_miss_count": scan.get("blocked_near_miss_count", 0),
        "blocked_near_misses": scan.get("blocked_near_misses", []),
        "decision": {
            "candidate_local_decision": "review_only_watch_board_present" if rows else "no_watch_board_candidate",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": (
                "confirmed current scan converted to high-zone blowoff short watch board"
                if rows
                else "no confirmed current candidate to place on high-zone blowoff short watch board"
            ),
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    _write_json(output_dir / "short_blowoff_watch_board.json", report)
    _write_json(output_root / "latest_short_blowoff_watch_board.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current-scan", type=Path, default=DEFAULT_CURRENT_SCAN)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(current_scan_path=args.current_scan, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
