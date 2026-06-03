from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, roc_auc_score

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import tradex_entry_actionability_and_avoidance_surface_v1 as base


AXIS_ID = "practical_decision_support_bundle_v1"
DEFAULT_SOURCE = base.DEFAULT_SOURCE
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\practical_decision_support_bundle_v1")
TRAIN_SELECTION_END = 20221231
VALIDATION_START = 20230101
VALIDATION_END = 20231231
FINAL_TRAIN_END = 20231231
REVIEW_START = 20240101
ADDITIONAL_FEATURES = [
    "close_above_ma7", "close_above_ma20", "close_above_ma60",
    "ma7_above_ma20", "ma20_above_ma60",
    "bullish_body_flag", "bearish_body_flag", "failed_high_flag",
    "gap_up_flag", "gap_down_flag", "weekly_failed_high_flag",
    "weekly_supportive_flag", "monthly_supportive_flag",
    "monthly_box_width_pct", "monthly_box_month_count",
    "high_upside_reserve_reference_match",
    "constructive_pullback_support_bullish_confirmation_reference_match",
    "early_trend_reclaim_controlled_extension_candidate",
    "volatility_compression_breakout_preparation_candidate",
    "monthly_weekly_supportive_daily_confirmation_candidate",
]


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _prepare(rows: pd.DataFrame, features: list[str]) -> tuple[pd.DataFrame, int]:
    prepared = rows.copy()
    prepared[features] = prepared[features].astype(float)
    count = int(np.isinf(prepared[features].to_numpy(dtype=float)).sum())
    prepared[features] = prepared[features].replace([np.inf, -np.inf], np.nan)
    return prepared, count


def _fit_score(train: pd.DataFrame, scored: pd.DataFrame, features: list[str], label: str) -> np.ndarray:
    model = base._model()
    model.fit(train[features], train[label].astype(int))
    return model.predict_proba(scored[features])[:, 1]


def _auc(frame: pd.DataFrame, label: str, score: str) -> float:
    return float(roc_auc_score(frame[label].astype(int), frame[score]))


def _candidate_metrics(train: pd.DataFrame, scored: pd.DataFrame, features: list[str]) -> dict[str, Any]:
    out = scored.copy()
    out["up"] = _fit_score(train, out, features, "winner_ret20_gt_10pct")
    out["down"] = _fit_score(train, out, features, "bad_ret20_lt_minus_5pct")
    return {"upside_auc": _auc(out, "winner_ret20_gt_10pct", "up"), "downside_auc": _auc(out, "bad_ret20_lt_minus_5pct", "down")}


def _bucket(row: pd.Series) -> str:
    if row["avoid_level"] == "avoid":
        return "Avoid"
    if row["upside_probability_20d"] >= 0.62 and row["downside_risk_probability_20d"] <= 0.42 and row["entry_actionability_score"] >= 0.18:
        return "Starter"
    if row["upside_probability_20d"] >= 0.52 and row["downside_risk_probability_20d"] <= 0.52:
        return "Watch"
    return "Wait"


def _cohort(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"row_count": 0, "ret20_mean": None, "winner_rate": None, "bad_rate": None, "severe_rate": None}
    return {"row_count": len(frame), "ret20_mean": float(frame["ret20"].mean()), "winner_rate": float(frame["winner_ret20_gt_10pct"].mean()), "bad_rate": float(frame["bad_ret20_lt_minus_5pct"].mean()), "severe_rate": float(frame["severe_ret20_lt_minus_10pct"].mean())}


def run(*, source_path: Path, output_root: Path) -> Path:
    rows = pd.read_parquet(source_path)
    rows["as_of_date"] = rows["as_of_date"].astype(int)
    expanded = base.FEATURES + ADDITIONAL_FEATURES
    rows, non_finite = _prepare(rows, expanded)
    selection_train = rows.loc[rows["as_of_date"] <= TRAIN_SELECTION_END]
    validation = rows.loc[rows["as_of_date"].between(VALIDATION_START, VALIDATION_END)]
    final_train = rows.loc[rows["as_of_date"] <= FINAL_TRAIN_END]
    review = rows.loc[rows["as_of_date"] >= REVIEW_START].copy()
    if any(frame.empty for frame in [selection_train, validation, final_train, review]):
        raise ValueError("chronological split produced an empty frame")
    candidates = {"baseline": base.FEATURES, "expanded": expanded}
    validation_rows = {name: _candidate_metrics(selection_train, validation, features) for name, features in candidates.items()}
    for metrics in validation_rows.values():
        metrics["composite_auc"] = (metrics["upside_auc"] + metrics["downside_auc"]) / 2
    selected = max(validation_rows, key=lambda name: validation_rows[name]["composite_auc"])
    features = candidates[selected]
    review["upside_probability_20d"] = _fit_score(final_train, review, features, "winner_ret20_gt_10pct")
    review["downside_risk_probability_20d"] = _fit_score(final_train, review, features, "bad_ret20_lt_minus_5pct")
    review["entry_actionability_score"] = review["upside_probability_20d"] - review["downside_risk_probability_20d"]
    review["avoid_reason_codes"] = review.apply(base._reason_codes, axis=1)
    review["event_risk_contract_status"] = "unavailable_not_silently_fallbacked"
    review["avoid_level"] = "none"
    review.loc[review["avoid_reason_codes"].map(len).gt(0), "avoid_level"] = "caution"
    review.loc[review["avoid_reason_codes"].map(len).ge(2) | review["downside_risk_probability_20d"].ge(0.65), "avoid_level"] = "avoid"
    review["review_bucket"] = review.apply(_bucket, axis=1)
    review["avoid_reason_codes"] = review["avoid_reason_codes"].map(json.dumps)
    baseline_eval = _candidate_metrics(final_train, review, base.FEATURES)
    selected_eval = {"upside_auc": _auc(review, "winner_ret20_gt_10pct", "upside_probability_20d"), "downside_auc": _auc(review, "bad_ret20_lt_minus_5pct", "downside_risk_probability_20d")}
    selected_eval["upside_auc_delta_vs_baseline"] = selected_eval["upside_auc"] - baseline_eval["upside_auc"]
    selected_eval["downside_auc_delta_vs_baseline"] = selected_eval["downside_auc"] - baseline_eval["downside_auc"]
    selected_eval["upside_brier_score"] = float(brier_score_loss(review["winner_ret20_gt_10pct"], review["upside_probability_20d"]))
    selected_eval["downside_brier_score"] = float(brier_score_loss(review["bad_ret20_lt_minus_5pct"], review["downside_risk_probability_20d"]))
    by_bucket = {name: _cohort(part) for name, part in review.groupby("review_bucket", sort=True)}
    yearly = []
    for year, part in review.groupby(review["as_of_date"].astype(str).str[:4].astype(int), sort=True):
        yearly.append({"year": int(year), "all": _cohort(part), "by_bucket": {name: _cohort(group) for name, group in part.groupby("review_bucket", sort=True)}})
    latest_as_of = int(review["as_of_date"].max())
    latest = review.loc[review["as_of_date"] == latest_as_of].copy()
    latest["review_bucket_sort_order"] = latest["review_bucket"].map({"Starter": 0, "Watch": 1, "Wait": 2, "Avoid": 3})
    latest = latest.sort_values(["review_bucket_sort_order", "entry_actionability_score"], ascending=[True, False])
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    review.to_parquet(output / "decision_support_surface.parquet", index=False)
    latest.to_csv(output / "latest_review_board.csv", index=False)
    _write_json(output / "feature_selection_audit.json", {"split_contract": {"selection_train_end": TRAIN_SELECTION_END, "validation_start": VALIDATION_START, "validation_end": VALIDATION_END, "final_train_end": FINAL_TRAIN_END, "review_start": REVIEW_START}, "candidate_feature_sets": {key: value for key, value in candidates.items()}, "validation_metrics": validation_rows, "selected_feature_set": selected, "non_finite_replaced_with_nan": non_finite})
    _write_json(output / "decision_support_metrics.json", {"selected_feature_set": selected, "review_oos_metrics": selected_eval, "baseline_review_oos_metrics": baseline_eval, "review_bucket_metrics": by_bucket, "yearly": yearly, "latest_as_of": latest_as_of, "latest_board_row_count": len(latest), "event_risk_contract_status": "unavailable_not_silently_fallbacked"})
    _write_json(output / "research_decision.json", {"decision_class": "READY_REVIEW_ONLY", "research_decision": "practical_decision_support_bundle_ready_for_manual_use_with_freshness_check", "latest_as_of": latest_as_of, "current_recommendation_allowed": False, "reason": "historical source artifact only; refresh required before current use", "meemee_unchanged": True, "production_ranking_changed": False, "candidate_generation_changed": False, "runtime_db_write": False, "validated_buy_count": 0})
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output), "decision_class": "READY_REVIEW_ONLY"})
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(source_path=args.source_path, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
