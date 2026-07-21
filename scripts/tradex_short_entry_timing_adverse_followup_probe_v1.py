from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from tradex_short_entry_timing_current_scan_v1 import DEFAULT_DB_PATH, RULES
from tradex_short_entry_timing_rule_probe_v1 import _scan_timing_events
from tradex_short_shape_bad_avoidance_probe_v1 import _daily_rows, _numeric_features_for
from tradex_short_shape_numeric_rule_probe_v1 import _apply_rule


AXIS_ID = "short_entry_timing_adverse_followup_probe_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\adverse_followup")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _bucket_name(adverse_high_2d: float) -> str:
    if adverse_high_2d < 0.025:
        return "clean_or_small_adverse_lt_2_5pct"
    if adverse_high_2d < 0.045:
        return "medium_adverse_2_5_to_4_5pct"
    return "large_adverse_ge_4_5pct"


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0}
    return {
        "n": len(rows),
        "entry_now_from_followup_rate": sum(1 for row in rows if row["followup_MAE5"] <= -0.035 and row["followup_MFE5"] <= 0.025 and row["followup_ret5"] <= -0.015) / len(rows),
        "wrong_from_followup_rate": sum(1 for row in rows if row["followup_MFE5"] >= 0.045 or row["followup_ret5"] >= 0.025) / len(rows),
        "avg_followup_ret5": sum(float(row["followup_ret5"]) for row in rows) / len(rows),
        "avg_followup_MAE5": sum(float(row["followup_MAE5"]) for row in rows) / len(rows),
        "avg_followup_MFE5": sum(float(row["followup_MFE5"]) for row in rows) / len(rows),
        "avg_adverse_high_2d": sum(float(row["adverse_high_2d"]) for row in rows) / len(rows),
        "avg_close_2d_ret": sum(float(row["close_2d_ret"]) for row in rows) / len(rows),
    }


def _event_followup_rows(db_path: Path, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cache: dict[str, list[tuple[int, float, float, float, float, float]]] = {}
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        for row in rows:
            code = str(row["code"])
            as_of = int(row["as_of"])
            if code not in cache:
                cache[code] = _daily_rows(conn, code)
            bars = cache[code]
            ymds = [int(item[0]) for item in bars]
            if as_of not in ymds:
                continue
            index = ymds.index(as_of)
            if len(bars) <= index + 7:
                continue
            entry_close = float(bars[index][4])
            followup_close = float(bars[index + 2][4])
            first_2d = bars[index + 1 : index + 3]
            future5 = bars[index + 3 : index + 8]
            if len(first_2d) != 2 or len(future5) != 5:
                continue
            adverse_high_2d = max(float(item[2]) for item in first_2d) / entry_close - 1.0
            close_2d_ret = followup_close / entry_close - 1.0
            followup_ret5 = float(future5[-1][4]) / followup_close - 1.0
            followup_mae5 = min(float(item[3]) for item in future5) / followup_close - 1.0
            followup_mfe5 = max(float(item[2]) for item in future5) / followup_close - 1.0
            out.append(
                {
                    **row,
                    "followup_as_of": int(bars[index + 2][0]),
                    "adverse_high_2d": round(adverse_high_2d, 8),
                    "close_2d_ret": round(close_2d_ret, 8),
                    "followup_ret5": round(followup_ret5, 8),
                    "followup_MAE5": round(followup_mae5, 8),
                    "followup_MFE5": round(followup_mfe5, 8),
                    "adverse_bucket": _bucket_name(adverse_high_2d),
                }
            )
    finally:
        conn.close()
    return out


def run(*, db_path: Path, output_root: Path, start_ymd: int, max_rows: int) -> Path:
    events = _scan_timing_events(db_path, start_ymd=start_ymd, max_rows=max_rows)
    x, kept = _numeric_features_for(events, db_path)
    matched: list[dict[str, Any]] = []
    for rule in RULES:
        mask = _apply_rule(x, rule["clauses"]) if len(kept) else []
        for row, keep in zip(kept, mask):
            if keep:
                matched.append({**row, "rule_id": rule["rule_id"], "review_strength": rule["review_strength"]})
    followup = _event_followup_rows(db_path, matched)
    by_rule: dict[str, Any] = {}
    for rule in RULES:
        rule_rows = [row for row in followup if row["rule_id"] == rule["rule_id"]]
        by_bucket = {}
        for bucket in ("clean_or_small_adverse_lt_2_5pct", "medium_adverse_2_5_to_4_5pct", "large_adverse_ge_4_5pct"):
            by_bucket[bucket] = _summarize([row for row in rule_rows if row["adverse_bucket"] == bucket])
        by_rule[rule["rule_id"]] = {"all": _summarize(rule_rows), "by_adverse_bucket": by_bucket}
    all_by_bucket = {
        bucket: _summarize([row for row in followup if row["adverse_bucket"] == bucket])
        for bucket in ("clean_or_small_adverse_lt_2_5pct", "medium_adverse_2_5_to_4_5pct", "large_adverse_ge_4_5pct")
    }
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "fixed_evaluation_conditions": {
            "source": "confirmed non-yahoo daily_bars",
            "start_ymd": start_ymd,
            "max_rows": max_rows,
            "followup_entry": "two trading days after the original timing signal",
            "bucket_contract": "adverse high during first two trading days after signal",
        },
        "matched_signal_count": len(matched),
        "followup_row_count": len(followup),
        "all_by_adverse_bucket": all_by_bucket,
        "by_rule": by_rule,
        "decision": {
            "candidate_local_decision": "use_adverse_overlay_as_filter",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "adverse move after timing signal materially changes followup entry quality and should gate current candidates",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "adverse_followup_probe.json", report)
    _write_json(output_root / "latest_adverse_followup_probe.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20150101)
    parser.add_argument("--max-rows", type=int, default=5000)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root, start_ymd=args.start_ymd, max_rows=args.max_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
