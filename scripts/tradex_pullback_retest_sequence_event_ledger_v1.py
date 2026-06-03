from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "pullback_retest_sequence_event_ledger_v1"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\pattern_family_source_rows_v1\20260525T101220Z-pattern-family-source-rows-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\pullback_retest_sequence_event_ledger_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_events(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code, group in frame.sort_values(["code", "as_of_date"]).groupby("code", sort=False):
        pullback_start = pullback_low = reclaim = retest = None
        pullback_low_dist: float | None = None
        prev_dist: float | None = None
        for row in group.itertuples(index=False):
            as_of = int(row.as_of_date)
            dist = float(row.close_vs_ma20_pct) if pd.notna(row.close_vs_ma20_pct) else None
            if dist is None:
                prev_dist = None
                continue
            if dist <= 0:
                if pullback_start is None or reclaim is not None:
                    pullback_start, pullback_low, reclaim, retest = as_of, as_of, None, None
                    pullback_low_dist = dist
                elif pullback_low_dist is None or dist < pullback_low_dist:
                    pullback_low, pullback_low_dist = as_of, dist
            crossed_reclaim = prev_dist is not None and prev_dist <= 0 < dist
            if crossed_reclaim and pullback_start is not None:
                reclaim, retest = as_of, None
            if reclaim is not None and retest is None and as_of > reclaim and 0 <= dist <= 0.03:
                retest = as_of
            bullish = bool(row.bullish_body_flag) or (float(row.lower_wick_ratio or 0) >= 0.25 and not bool(row.bearish_body_flag))
            if retest is not None and as_of >= retest and bullish and dist > 0:
                rows.append({
                    "code": str(code),
                    "candidate_as_of": as_of,
                    "pullback_start_as_of": pullback_start,
                    "pullback_low_as_of": pullback_low,
                    "reclaim_as_of": reclaim,
                    "retest_as_of": retest,
                    "confirmation_as_of": as_of,
                    "confirmation_reason": "post_reclaim_retest_hold_with_constructive_daily_candle",
                    "invalidation_distance_pct_as_of_confirmation": float(row.recent_low_distance_pct) if pd.notna(row.recent_low_distance_pct) else None,
                    "invalidation_price_as_of_confirmation": None,
                    "weekly_supportive_flag": bool(row.weekly_supportive_flag),
                    "monthly_supportive_flag": bool(row.monthly_supportive_flag),
                    "volume_vs_20d_avg": float(row.volume_vs_20d_avg) if pd.notna(row.volume_vs_20d_avg) else None,
                })
                pullback_start = pullback_low = reclaim = retest = None
                pullback_low_dist = None
            prev_dist = dist
    return pd.DataFrame(rows)


def run(*, source_root: Path, output_root: Path) -> Path:
    source_path = source_root / "pattern_family_source_rows.parquet"
    source_audit = json.loads((source_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    source = pd.read_parquet(source_path)
    events = build_events(source)
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    events.to_parquet(output_dir / "pullback_retest_sequence_events.parquet", index=False)
    events.head(1000).to_csv(output_dir / "pullback_retest_sequence_events_sample.csv", index=False)
    absolute_invalidation_available = bool(not events.empty and events["invalidation_price_as_of_confirmation"].notna().all())
    decision = {
        "axis_id": AXIS_ID,
        "decision_class": "READY" if absolute_invalidation_available else "HOLD",
        "research_decision": "pullback_retest_sequence_ledger_ready" if absolute_invalidation_available else "sequence_ledger_created_but_absolute_invalidation_price_source_missing",
        "reason_typed": [] if absolute_invalidation_available else ["source_rows_expose_recent_low_distance_but_not_absolute_close_or_invalidation_price"],
        "candidate_generation_changed": False,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "validated_buy_count": 0,
    }
    audit = {
        "audit_result": "pass" if source_audit.get("audit_result") == "pass" else "blocked",
        "source_no_lookahead_audit": source_audit.get("audit_result"),
        "source_row_count": len(source),
        "sequence_event_count": len(events),
        "sequence_fields_materialized": ["pullback_start_as_of", "pullback_low_as_of", "reclaim_as_of", "retest_as_of", "confirmation_as_of", "confirmation_reason"],
        "diagnostic_invalidation_distance_materialized": True,
        "absolute_invalidation_price_materialized": absolute_invalidation_available,
        "forward_outcomes_used_in_event_construction": False,
        "research_fallback_used": False,
        "runtime_db_write": False,
    }
    _write_json(output_dir / "sequence_event_ledger_audit.json", audit)
    _write_json(output_dir / "research_decision.json", decision)
    _write_json(output_root / "latest_research_decision.json", decision)
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(source_root=args.source_root, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
