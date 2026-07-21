from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_short_watch_to_entry_retest_probe_v1 import (
    AXIS_ID as RESEARCH_AXIS_ID,
    BLOWOFF_ENTRY_RULES,
    _build_entry_candidates,
    _build_features,
    _default_db_path,
    _json_value,
    _latest_pan_date,
)


AXIS_ID = "short_blowoff_current_scan_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_watch_to_entry_retest_probe_v1\current_scan")

SCAN_RULE_IDS = [
    "post_peak_fast_ma7_fail_low20_rr_ge_1_5_drop_midrange_weak_entry_volume",
    "post_peak_fast_ma7_fail_low20_rr_ge_1_3_drop_midrange_weak_entry_volume",
]
BLOCKED_RELAXED_RULE_ID = "post_peak_fast_ma7_fail_low20_rr_ge_1_1_drop_midrange_weak_entry_volume"

RULE_PROFILES = {
    "post_peak_fast_ma7_fail_low20_rr_ge_1_5_drop_midrange_weak_entry_volume": {
        "profile": "strict_core",
        "reference": {
            "n": 17,
            "target_first_rate": 1.0,
            "stop_policy": "peak_high_plus_0.5pct",
            "target_policy": "down_5pct_within_20_sessions",
        },
    },
    "post_peak_fast_ma7_fail_low20_rr_ge_1_3_drop_midrange_weak_entry_volume": {
        "profile": "n20_main",
        "reference": {
            "n": 21,
            "target_first_rate": 0.9523809523809523,
            "stop_policy": "peak_high_plus_1.0pct",
            "target_policy": "down_5pct_within_20_to_30_sessions",
        },
    },
}

BLOCKED_RULE_PROFILE = {
    "profile": "blocked_rr_relaxation",
    "reference": {
        "n": 7,
        "target_first_rate": 0.42857142857142855,
        "stop_first_rate": 0.5714285714285714,
        "judgment": "drop_relaxation",
        "reason": "RR1.1 relaxed-only branch added too many stop-first cases",
    },
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _freshness(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = con.execute(
        """
SELECT
  coalesce(source, 'pan') AS source,
  max(date) AS max_date,
  strftime(to_timestamp(max(date)), '%Y-%m-%d') AS max_date_iso,
  count(*) AS rows
FROM daily_bars
GROUP BY 1
ORDER BY 1
"""
    ).fetchall()
    return [
        {
            "source": source,
            "max_date": _json_value(max_date),
            "max_date_iso": max_date_iso,
            "rows": int(rows_count or 0),
        }
        for source, max_date, max_date_iso, rows_count in rows
    ]


def _scan_rows_for_rule(con: duckdb.DuckDBPyConnection, rule_id: str, confirmed_as_of: str) -> list[dict[str, Any]]:
    rule = BLOWOFF_ENTRY_RULES[rule_id]
    rows = con.execute(
        f"""
SELECT
  e.code,
  coalesce(t.name, '') AS name,
  strftime(to_timestamp(e.signal_date), '%Y-%m-%d') AS signal_date,
  strftime(to_timestamp(e.peak_date), '%Y-%m-%d') AS peak_date,
  strftime(to_timestamp(e.entry_date), '%Y-%m-%d') AS entry_date,
  round(e.entry_c, 2) AS entry_c,
  round(e.peak_h, 2) AS peak_h,
  round((e.entry_c - e.low20) / NULLIF(e.peak_h - e.entry_c, 0), 2) AS rr_to_low20,
  round((e.entry_c - e.entry_low60) / NULLIF(e.entry_high60 - e.entry_low60, 0), 2) AS entry_range60_pos,
  round((e.entry_c / NULLIF(e.entry_high60, 0) - 1.0) * 100, 2) AS entry_vs_high60_pct,
  round((e.entry_c / NULLIF(e.entry_ma7, 0) - 1.0) * 100, 2) AS entry_vs_ma7_pct,
  round((e.entry_c / NULLIF(e.entry_ma20, 0) - 1.0) * 100, 2) AS entry_vs_ma20_pct,
  round(e.entry_v / NULLIF(e.peak_v, 0), 2) AS entry_v_vs_peak_v,
  round(e.entry_v / NULLIF(e.signal_v, 0), 2) AS entry_v_vs_signal_v
FROM short_watch_blowoff_entry_candidates e
LEFT JOIN tickers t ON t.code = e.code
WHERE {rule["where"]}
  AND strftime(to_timestamp(e.entry_date), '%Y-%m-%d') = ?
QUALIFY row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY e.entry_date) = 1
ORDER BY e.code
""",
        [confirmed_as_of],
    ).fetchall()
    keys = [
        "code",
        "name",
        "signal_date",
        "peak_date",
        "entry_date",
        "entry_c",
        "peak_h",
        "rr_to_low20",
        "entry_range60_pos",
        "entry_vs_high60_pct",
        "entry_vs_ma7_pct",
        "entry_vs_ma20_pct",
        "entry_v_vs_peak_v",
        "entry_v_vs_signal_v",
    ]
    profile = RULE_PROFILES[rule_id]
    return [
        {
            **{key: _json_value(value) for key, value in zip(keys, row)},
            "rule_id": rule_id,
            "profile": profile["profile"],
            "reference": profile["reference"],
            "research_axis_id": RESEARCH_AXIS_ID,
            "review_only": True,
        }
        for row in rows
    ]


def _blocked_near_misses(con: duckdb.DuckDBPyConnection, confirmed_as_of: str) -> list[dict[str, Any]]:
    if BLOCKED_RELAXED_RULE_ID not in BLOWOFF_ENTRY_RULES:
        return []
    relaxed = BLOWOFF_ENTRY_RULES[BLOCKED_RELAXED_RULE_ID]["where"]
    accepted_conditions = " OR ".join(f"({BLOWOFF_ENTRY_RULES[rule_id]['where']})" for rule_id in SCAN_RULE_IDS)
    rows = con.execute(
        f"""
SELECT
  e.code,
  coalesce(t.name, '') AS name,
  strftime(to_timestamp(e.signal_date), '%Y-%m-%d') AS signal_date,
  strftime(to_timestamp(e.peak_date), '%Y-%m-%d') AS peak_date,
  strftime(to_timestamp(e.entry_date), '%Y-%m-%d') AS entry_date,
  round(e.entry_c, 2) AS entry_c,
  round(e.peak_h, 2) AS peak_h,
  round((e.entry_c - e.low20) / NULLIF(e.peak_h - e.entry_c, 0), 2) AS rr_to_low20,
  round((e.entry_c - e.entry_low60) / NULLIF(e.entry_high60 - e.entry_low60, 0), 2) AS entry_range60_pos,
  round((e.entry_c / NULLIF(e.entry_high60, 0) - 1.0) * 100, 2) AS entry_vs_high60_pct,
  round((e.entry_c / NULLIF(e.entry_ma7, 0) - 1.0) * 100, 2) AS entry_vs_ma7_pct,
  round((e.entry_c / NULLIF(e.entry_ma20, 0) - 1.0) * 100, 2) AS entry_vs_ma20_pct,
  round(e.entry_v / NULLIF(e.peak_v, 0), 2) AS entry_v_vs_peak_v,
  round(e.entry_v / NULLIF(e.signal_v, 0), 2) AS entry_v_vs_signal_v
FROM short_watch_blowoff_entry_candidates e
LEFT JOIN tickers t ON t.code = e.code
WHERE {relaxed}
  AND NOT ({accepted_conditions})
  AND strftime(to_timestamp(e.entry_date), '%Y-%m-%d') = ?
QUALIFY row_number() OVER (PARTITION BY e.code, e.signal_date ORDER BY e.entry_date) = 1
ORDER BY e.code
""",
        [confirmed_as_of],
    ).fetchall()
    keys = [
        "code",
        "name",
        "signal_date",
        "peak_date",
        "entry_date",
        "entry_c",
        "peak_h",
        "rr_to_low20",
        "entry_range60_pos",
        "entry_vs_high60_pct",
        "entry_vs_ma7_pct",
        "entry_vs_ma20_pct",
        "entry_v_vs_peak_v",
        "entry_v_vs_signal_v",
    ]
    return [
        {
            **{key: _json_value(value) for key, value in zip(keys, row)},
            "rule_id": BLOCKED_RELAXED_RULE_ID,
            "profile": BLOCKED_RULE_PROFILE["profile"],
            "reference": BLOCKED_RULE_PROFILE["reference"],
            "blocked_reason": "rr_to_low20_below_validated_main_threshold",
            "review_only": True,
        }
        for row in rows
    ]


def run(*, db_path: Path, output_root: Path, start: str, end: str) -> Path:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if end == "latest":
            end = _latest_pan_date(con)
        freshness = _freshness(con)
        _build_features(con, start, end, table_name="three_window_events", require_forward_labels=False)
        _build_entry_candidates(con)
        rule_outputs = []
        merged: dict[str, dict[str, Any]] = {}
        for rule_id in SCAN_RULE_IDS:
            rows = _scan_rows_for_rule(con, rule_id, end)
            rule_outputs.append(
                {
                    "rule_id": rule_id,
                    "profile": RULE_PROFILES[rule_id]["profile"],
                    "reference": RULE_PROFILES[rule_id]["reference"],
                    "selected_count": len(rows),
                    "rows": rows,
                }
            )
            for row in rows:
                key = str(row["code"])
                merged.setdefault(
                    key,
                    {
                        **row,
                        "matched_rules": [],
                    },
                )
                merged[key]["matched_rules"].append(
                    {
                        "rule_id": rule_id,
                        "profile": RULE_PROFILES[rule_id]["profile"],
                        "reference": RULE_PROFILES[rule_id]["reference"],
                    }
                )
        blocked_near_misses = _blocked_near_misses(con, end)
        current_candidates = sorted(
            merged.values(),
            key=lambda row: (
                any(item["profile"] == "strict_core" for item in row["matched_rules"]),
                max(item["reference"]["n"] for item in row["matched_rules"]),
                row["code"],
            ),
            reverse=True,
        )
        report = {
            "schema_version": f"{AXIS_ID}_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "research_axis_id": RESEARCH_AXIS_ID,
            "boundary_owner": "TRADEX",
            "db_path": str(db_path),
            "runtime_freshness_by_source": freshness,
            "confirmed_as_of": end,
            "confirmed_source_policy": "pan only; yahoo provisional excluded",
            "scan_rule_ids": SCAN_RULE_IDS,
            "blocked_rule_ids": [BLOCKED_RELAXED_RULE_ID],
            "rule_outputs": rule_outputs,
            "current_candidate_count": len(current_candidates),
            "current_candidates": current_candidates,
            "blocked_near_miss_count": len(blocked_near_misses),
            "blocked_near_misses": blocked_near_misses,
            "decision": {
                "candidate_local_decision": "review_only_current_candidates_present" if current_candidates else "no_current_candidate",
                "authoritative_rollup_decision": "research_candidate_not_trade_signal",
                "reason": (
                    "confirmed latest bar matches high-zone blowoff short research axis"
                    if current_candidates
                    else "no confirmed latest bar matched high-zone blowoff short research axis"
                ),
            },
            "production_ranking_changed": False,
            "runtime_db_write": False,
            "meemee_unchanged": True,
        }
    finally:
        con.close()

    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    _write_json(output_dir / "current_short_blowoff_candidates.json", report)
    _write_json(output_root / "latest_current_short_blowoff_candidates.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=_default_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end", default="latest")
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root, start=args.start, end=args.end))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
