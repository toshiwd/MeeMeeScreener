from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


AXIS_ID = "tradex_leaf20_short_comparability_audit_v1"
LONG = Path(r"G:\Tradex\chart_entry_geometry_research_v1\20260711T024708Z-shallow_high_zone_next_open_execution_v1\compare.json")
SHORT = Path(r"G:\Tradex\short_support_break_breadth_gate_v1\latest_compare.json")
OUT = Path(r"G:\Tradex\leaf20_short_comparability_audit_v1")


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    now = datetime.now(timezone.utc)
    run = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    run.mkdir(parents=True)
    long = load(LONG)
    short = load(SHORT)
    leaf20 = next(row for row in long["capacity_and_allocation"]["leaf_risk_breakdown"] if int(row["leaf"]) == 20)
    gate = short["selection"]["selected_gate"]
    short_row = next(row for row in short["reports"] if row["gate"] == gate)
    payload = {
        "schema_version": f"{AXIS_ID}_v1", "generated_at": now.isoformat(), "axis_id": AXIS_ID, "boundary_owner": "TRADEX",
        "source_artifacts": {"long_leaf20": str(LONG), "short_breadth": str(SHORT)},
        "fixed_comparison_scope": "2019-2025, reported test split plus annual stability; no costs modeled",
        "long_leaf20": leaf20,
        "short_candidate": {"gate": gate, "splits": short_row["splits"], "yearly": short_row["yearly"], "annual_gate": short["post_selection_evaluation"]},
        "comparability": {
            "same_universe": False, "same_entry": False, "same_exit": False, "same_aggregation": False,
            "long_aggregation": "daily basket profit factor", "short_aggregation": "per-trade profit factor",
            "short_entry": "next-day signal-low stop entry", "long_entry": "next open when not above signal close",
        },
        "decision": {
            "candidate_local_decision": "hold_not_comparable_to_leaf20_yet",
            "authoritative_rollup_decision": "research_only",
            "reason": "Short aggregate test PF is positive, but it has annual sample shortfalls and is not measured on the same execution or daily-basket basis as leaf20; superiority cannot be claimed.",
        },
        "required_next_axis": "replay the selected short candidate into the same daily-basket aggregation and concurrent-capacity convention as leaf20 without changing its entry, exit, or breadth gate",
        "runtime_db_write": False, "meemee_unchanged": True, "production_ranking_changed": False, "silent_fallback_used": False,
    }
    (run / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (OUT / "latest_compare.json").write_text(json.dumps({"run_root": str(run), **payload}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(run / "compare.json")


if __name__ == "__main__": main()
