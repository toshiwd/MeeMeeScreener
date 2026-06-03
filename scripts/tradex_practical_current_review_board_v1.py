from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import tradex_entry_actionability_and_avoidance_surface_v1 as base
from scripts import tradex_pattern_family_source_rows_v1 as source
from scripts import tradex_practical_decision_support_bundle_v1 as bundle


AXIS_ID = "practical_current_review_board_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_TRAINING_SOURCE = bundle.DEFAULT_SOURCE
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\practical_current_review_board_v1")
BAR_START = 20200101


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(*, db_path: Path, training_source: Path, output_root: Path) -> Path:
    historical = pd.read_parquet(training_source)
    historical["as_of_date"] = historical["as_of_date"].astype(int)
    features = base.FEATURES + bundle.ADDITIONAL_FEATURES
    historical, historical_non_finite = bundle._prepare(historical, features)
    train = historical.loc[historical["as_of_date"] <= bundle.FINAL_TRAIN_END]
    raw = source.load_confirmed_daily_bars(db_path, BAR_START, 20991231)
    latest_as_of = int(raw["as_of_date"].max())
    featured = source.add_family_flags(source.attach_period_features(source.add_daily_features(raw)))
    current = featured.loc[featured["as_of_date"] == latest_as_of].copy()
    current, current_non_finite = bundle._prepare(current, features)
    required = features + ["code"]
    current = current.dropna(subset=required).copy()
    current["upside_probability_20d"] = bundle._fit_score(train, current, features, "winner_ret20_gt_10pct")
    current["downside_risk_probability_20d"] = bundle._fit_score(train, current, features, "bad_ret20_lt_minus_5pct")
    current["entry_actionability_score"] = current["upside_probability_20d"] - current["downside_risk_probability_20d"]
    current["avoid_reason_codes"] = current.apply(base._reason_codes, axis=1)
    current["event_risk_contract_status"] = "unavailable_not_silently_fallbacked"
    current["avoid_level"] = "none"
    current.loc[current["avoid_reason_codes"].map(len).gt(0), "avoid_level"] = "caution"
    current.loc[current["avoid_reason_codes"].map(len).ge(2) | current["downside_risk_probability_20d"].ge(0.65), "avoid_level"] = "avoid"
    current["review_bucket"] = current.apply(bundle._bucket, axis=1)
    current["avoid_reason_codes"] = current["avoid_reason_codes"].map(json.dumps)
    current["review_bucket_sort_order"] = current["review_bucket"].map({"Starter": 0, "Watch": 1, "Wait": 2, "Avoid": 3})
    board = current[[
        "as_of_date", "code", "review_bucket", "upside_probability_20d",
        "downside_risk_probability_20d", "entry_actionability_score", "avoid_level",
        "avoid_reason_codes", "event_risk_contract_status", "review_bucket_sort_order",
        "failed_high_flag", "close_vs_ma20_pct", "weekly_supportive_flag",
        "volatility_compression_breakout_preparation_candidate",
        "constructive_pullback_support_bullish_confirmation_reference_match",
        "early_trend_reclaim_controlled_extension_candidate",
        "monthly_weekly_supportive_daily_confirmation_candidate",
    ]].sort_values(["review_bucket_sort_order", "entry_actionability_score"], ascending=[True, False])
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    board.to_csv(output / "current_review_board.csv", index=False)
    board.to_json(output / "current_review_board.json", orient="records", indent=2)
    distribution = board["review_bucket"].value_counts().sort_index().to_dict()
    _write_json(output / "current_review_board_audit.json", {
        "axis_id": AXIS_ID, "runtime_db_path": str(db_path), "runtime_db_write": False,
        "latest_as_of": latest_as_of, "board_row_count": len(board), "bucket_distribution": distribution,
        "historical_training_end": bundle.FINAL_TRAIN_END, "offline_outcomes_used_for_current_scoring": False,
        "historical_training_non_finite_replaced_with_nan": historical_non_finite,
        "current_non_finite_replaced_with_nan": current_non_finite,
        "event_risk_contract_status": "unavailable_not_silently_fallbacked",
        "meemee_unchanged": True, "production_ranking_changed": False, "candidate_generation_changed": False,
    })
    _write_json(output / "research_decision.json", {
        "decision_class": "READY_REVIEW_ONLY", "research_decision": "current_review_board_ready_for_manual_support",
        "latest_as_of": latest_as_of, "current_recommendation_allowed": False,
        "reason": "review support only; no validated buy claims", "runtime_db_write": False,
        "meemee_unchanged": True, "production_ranking_changed": False, "candidate_generation_changed": False,
        "validated_buy_count": 0,
    })
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output), "decision_class": "READY_REVIEW_ONLY"})
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--training-source", type=Path, default=DEFAULT_TRAINING_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db_path, training_source=args.training_source, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
