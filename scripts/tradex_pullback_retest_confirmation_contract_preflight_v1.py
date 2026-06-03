from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "pullback_retest_confirmation_contract_preflight_v1"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\pattern_family_source_rows_v1\20260525T101220Z-pattern-family-source-rows-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\pullback_retest_confirmation_contract_preflight_v1")

SNAPSHOT_COLUMNS = (
    "as_of_date", "code", "close_vs_ma7_pct", "close_vs_ma20_pct", "close_vs_ma60_pct",
    "ma7_slope_5d", "ma20_slope_10d", "volume_vs_20d_avg", "atr14_pct", "realized_vol20",
    "lower_wick_ratio", "upper_wick_ratio", "bullish_body_flag", "bearish_body_flag",
    "failed_high_flag", "recent_high_distance_pct", "recent_low_distance_pct",
    "weekly_supportive_flag", "monthly_supportive_flag",
)
REQUIRED_SEQUENCE_FIELDS = (
    "pullback_start_as_of", "pullback_low_as_of", "reclaim_as_of", "retest_as_of",
    "confirmation_as_of", "confirmation_reason", "invalidation_price_as_of_confirmation",
)


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build(*, source_root: Path, output_root: Path) -> Path:
    source_path = source_root / "pattern_family_source_rows.parquet"
    no_lookahead = json.loads((source_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    frame = pd.read_parquet(source_path)
    available = sorted(col for col in SNAPSHOT_COLUMNS if col in frame.columns)
    missing_snapshot = sorted(col for col in SNAPSHOT_COLUMNS if col not in frame.columns)
    sequence_available = sorted(col for col in REQUIRED_SEQUENCE_FIELDS if col in frame.columns)
    sequence_missing = sorted(col for col in REQUIRED_SEQUENCE_FIELDS if col not in frame.columns)
    ready = not missing_snapshot and not sequence_missing and no_lookahead.get("audit_result") == "pass"
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    contract = {
        "schema_version": "tradex_pullback_retest_confirmation_contract_preflight_v1",
        "boundary_owner": "TRADEX",
        "purpose": "freeze a point-in-time pullback/retest confirmation source contract before candidate generation",
        "row_key": ["as_of_date", "code"],
        "source_rows": str(source_path),
        "snapshot_features_available": available,
        "snapshot_features_missing": missing_snapshot,
        "required_sequence_fields": list(REQUIRED_SEQUENCE_FIELDS),
        "sequence_fields_available": sequence_available,
        "sequence_fields_missing": sequence_missing,
        "required_event_order": [
            "pullback_start_as_of <= pullback_low_as_of",
            "pullback_low_as_of <= reclaim_as_of",
            "reclaim_as_of <= retest_as_of",
            "retest_as_of <= confirmation_as_of",
            "confirmation_as_of <= candidate_as_of",
        ],
        "confirmation_policy": "candidate eligibility starts only on or after confirmation_as_of",
        "label_policy": "forward outcomes are evaluation-only and forbidden in event construction",
    }
    decision = {
        "axis_id": AXIS_ID,
        "decision_class": "READY" if ready else "BLOCKED",
        "research_decision": "pullback_retest_source_contract_ready" if ready else "build_point_in_time_sequence_event_ledger_before_candidate_generation",
        "reason_typed": [] if ready else ["snapshot_features_exist_but_pullback_retest_event_order_and_confirmation_date_are_not_materialized"],
        "candidate_generation_changed": False,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "validated_buy_count": 0,
    }
    audit = {
        "audit_result": "pass" if no_lookahead.get("audit_result") == "pass" else "blocked",
        "source_no_lookahead_audit": no_lookahead.get("audit_result"),
        "snapshot_feature_count": len(available),
        "missing_snapshot_feature_count": len(missing_snapshot),
        "missing_sequence_field_count": len(sequence_missing),
        "future_outcomes_used_in_contract": False,
        "research_fallback_used": False,
        "runtime_db_write": False,
    }
    _write_json(output_dir / "pullback_retest_confirmation_contract.json", contract)
    _write_json(output_dir / "no_lookahead_audit.json", audit)
    _write_json(output_dir / "research_decision.json", decision)
    _write_json(output_root / "latest_research_decision.json", decision)
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(build(source_root=args.source_root, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
