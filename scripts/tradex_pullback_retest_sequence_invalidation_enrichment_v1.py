from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "pullback_retest_sequence_invalidation_enrichment_v1"
DEFAULT_SEQUENCE_ROOT = Path(r"G:\Tradex\pullback_retest_sequence_event_ledger_v1\20260602T103004Z-pullback_retest_sequence_event_ledger_v1")
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\pullback_retest_sequence_invalidation_enrichment_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def enrich(*, sequence_root: Path, db_path: Path, output_root: Path) -> Path:
    events = pd.read_parquet(sequence_root / "pullback_retest_sequence_events.parquet")
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        con.register("events", events)
        enriched = con.execute(
            """
            WITH bars AS (
                SELECT
                    code,
                    CASE
                        WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                        WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                        WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                        ELSE NULL
                    END AS ymd,
                    l, c
                FROM daily_bars
                WHERE COALESCE(source, 'pan') <> 'yahoo'
            )
            SELECT
                e.* EXCLUDE (invalidation_price_as_of_confirmation),
                MIN(b.l) AS invalidation_price_as_of_confirmation,
                MAX(CASE WHEN b.ymd = e.confirmation_as_of THEN b.c END) AS confirmation_close,
                COUNT(*) AS invalidation_source_bar_count
            FROM events e
            LEFT JOIN bars b
              ON b.code = e.code
             AND b.ymd BETWEEN e.pullback_start_as_of AND e.confirmation_as_of
            GROUP BY ALL
            ORDER BY e.confirmation_as_of, e.code
            """
        ).df()
    finally:
        con.close()
    enriched["confirmation_close_available"] = enriched["confirmation_close"].notna()
    enriched["fixed_condition_pretest_eligible"] = (
        enriched["invalidation_price_as_of_confirmation"].notna()
        & enriched["confirmation_close_available"]
        & (enriched["invalidation_source_bar_count"] > 0)
    )
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    enriched.to_parquet(output_dir / "pullback_retest_sequence_events_enriched.parquet", index=False)
    enriched.head(1000).to_csv(output_dir / "pullback_retest_sequence_events_enriched_sample.csv", index=False)
    ready = bool(
        not enriched.empty
        and enriched["invalidation_price_as_of_confirmation"].notna().all()
        and enriched["fixed_condition_pretest_eligible"].any()
    )
    eligible_count = int(enriched["fixed_condition_pretest_eligible"].sum())
    excluded_missing_confirmation_close_count = int((~enriched["confirmation_close_available"]).sum())
    audit = {
        "audit_result": "pass" if ready else "hold",
        "boundary_owner": "TRADEX",
        "sequence_root": str(sequence_root),
        "db_path": str(db_path),
        "event_count": len(enriched),
        "fixed_condition_pretest_eligible_count": eligible_count,
        "fixed_condition_pretest_excluded_count": len(enriched) - eligible_count,
        "excluded_missing_confirmation_close_count": excluded_missing_confirmation_close_count,
        "absolute_invalidation_price_materialized": bool(enriched["invalidation_price_as_of_confirmation"].notna().all()) if not enriched.empty else False,
        "confirmation_close_materialized": bool(enriched["confirmation_close"].notna().all()) if not enriched.empty else False,
        "missing_confirmation_close_handling": "explicit_exclusion_no_fallback",
        "bars_used_only_through_confirmation_as_of": True,
        "forward_outcomes_used": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }
    decision = {
        "axis_id": AXIS_ID,
        "decision_class": "READY" if ready else "HOLD",
        "research_decision": "pullback_retest_confirmation_source_ready_for_fixed_condition_pretest" if ready else "hold_missing_invalidation_enrichment_rows",
        "candidate_generation_changed": False,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "validated_buy_count": 0,
    }
    _write_json(output_dir / "invalidation_enrichment_audit.json", audit)
    _write_json(output_dir / "research_decision.json", decision)
    _write_json(output_root / "latest_research_decision.json", decision)
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sequence-root", type=Path, default=DEFAULT_SEQUENCE_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(enrich(sequence_root=args.sequence_root, db_path=args.db_path, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
