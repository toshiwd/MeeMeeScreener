from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


AXIS_ID = "entry_actionability_and_avoidance_surface_v1"
DEFAULT_SOURCE = Path(
    r"G:\Tradex\pattern_family_source_rows_v1"
    r"\20260525T101220Z-pattern-family-source-rows-v1\pattern_family_source_rows.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\entry_actionability_and_avoidance_surface_v1")
TRAIN_END = 20231231
EVAL_START = 20240101
FEATURES = [
    "close_vs_ma7_pct", "close_vs_ma20_pct", "close_vs_ma60_pct",
    "ma7_slope_5d", "ma20_slope_10d", "ma60_slope_20d",
    "body_ratio", "upper_wick_ratio", "lower_wick_ratio",
    "recent_high_distance_pct", "recent_low_distance_pct",
    "volume_vs_20d_avg", "atr14_pct", "realized_vol20",
    "weekly_close_vs_ma20_pct", "weekly_ma20_slope",
    "monthly_close_vs_ma20_pct", "monthly_ma20_slope", "monthly_box_position",
]


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _model() -> Pipeline:
    return Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("model", LogisticRegression(max_iter=300, class_weight="balanced", random_state=7)),
    ])


def _reason_codes(row: pd.Series) -> list[str]:
    reasons = []
    if row["close_vs_ma20_pct"] > 0.12 or row["recent_high_distance_pct"] > -0.01:
        reasons.append("overextended_near_recent_high")
    if row["volume_vs_20d_avg"] < 0.55:
        reasons.append("low_volume_participation")
    if row["close_vs_ma20_pct"] < -0.05 and row["ma20_slope_10d"] < 0:
        reasons.append("falling_knife_bounce_risk")
    if not bool(row["weekly_supportive_flag"]) or not bool(row["monthly_supportive_flag"]):
        reasons.append("higher_timeframe_misalignment")
    if row["atr14_pct"] > 0.07 or row["recent_low_distance_pct"] > 0.12:
        reasons.append("wide_invalidation_distance")
    return reasons


def _calibration(y: pd.Series, p: pd.Series) -> list[dict[str, Any]]:
    bins = pd.cut(p, bins=np.linspace(0, 1, 11), include_lowest=True)
    rows = []
    for bucket, indexes in bins.groupby(bins, observed=False).groups.items():
        part_y, part_p = y.loc[indexes], p.loc[indexes]
        rows.append({
            "probability_bucket": str(bucket),
            "row_count": len(indexes),
            "predicted_mean": float(part_p.mean()) if len(indexes) else None,
            "observed_rate": float(part_y.mean()) if len(indexes) else None,
        })
    return rows


def _probability_metrics(y: pd.Series, p: pd.Series) -> dict[str, Any]:
    pred = p >= 0.5
    return {
        "row_count": len(y),
        "positive_rate": float(y.mean()),
        "roc_auc": float(roc_auc_score(y, p)),
        "brier_score": float(brier_score_loss(y, p)),
        "precision_at_0_5": float(precision_score(y, pred, zero_division=0)),
        "recall_at_0_5": float(recall_score(y, pred, zero_division=0)),
        "calibration": _calibration(y, p),
    }


def _cohort(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "row_count": len(frame),
        "ret20_mean": float(frame["ret20"].mean()),
        "bad_ret20_lt_minus_5pct_rate": float(frame["bad_ret20_lt_minus_5pct"].mean()),
        "severe_ret20_lt_minus_10pct_rate": float(frame["severe_ret20_lt_minus_10pct"].mean()),
        "winner_ret20_gt_10pct_rate": float(frame["winner_ret20_gt_10pct"].mean()),
    }


def run(*, source_path: Path, output_root: Path) -> Path:
    rows = pd.read_parquet(source_path)
    rows["as_of_date"] = rows["as_of_date"].astype(int)
    non_finite_count = int(np.isinf(rows[FEATURES].to_numpy(dtype=float)).sum())
    rows[FEATURES] = rows[FEATURES].replace([np.inf, -np.inf], np.nan)
    train = rows.loc[rows["as_of_date"] <= TRAIN_END].copy()
    review = rows.loc[rows["as_of_date"] >= EVAL_START].copy()
    if train.empty or review.empty:
        raise ValueError("chronological train/eval split produced an empty frame")
    up_model, down_model = _model(), _model()
    up_model.fit(train[FEATURES], train["winner_ret20_gt_10pct"].astype(int))
    down_model.fit(train[FEATURES], train["bad_ret20_lt_minus_5pct"].astype(int))
    review["upside_probability_20d"] = up_model.predict_proba(review[FEATURES])[:, 1]
    review["downside_risk_probability_20d"] = down_model.predict_proba(review[FEATURES])[:, 1]
    review["entry_actionability_score"] = (
        review["upside_probability_20d"] - review["downside_risk_probability_20d"]
    )
    review["avoid_reason_codes"] = review.apply(_reason_codes, axis=1)
    review["event_risk_contract_status"] = "unavailable_not_silently_fallbacked"
    review["avoid_level"] = "none"
    review.loc[review["avoid_reason_codes"].map(len).gt(0), "avoid_level"] = "caution"
    review.loc[
        review["avoid_reason_codes"].map(len).ge(2)
        | review["downside_risk_probability_20d"].ge(0.65),
        "avoid_level",
    ] = "avoid"
    review["avoid_reason_codes"] = review["avoid_reason_codes"].map(json.dumps)
    avoid = review.loc[review["avoid_level"] == "avoid"]
    non_avoid = review.loc[review["avoid_level"] != "avoid"]
    reason_rows = []
    exploded = review.assign(reason=review["avoid_reason_codes"].map(json.loads)).explode("reason")
    for reason, part in exploded.dropna(subset=["reason"]).groupby("reason", sort=True):
        reason_rows.append({"avoid_reason": reason, **_cohort(part)})
    yearly = []
    for year, part in review.groupby(review["as_of_date"].astype(str).str[:4].astype(int), sort=True):
        yearly.append({"year": int(year), "all": _cohort(part), "avoid": _cohort(part.loc[part["avoid_level"] == "avoid"]), "non_avoid": _cohort(part.loc[part["avoid_level"] != "avoid"])})
    metrics = {
        "axis_id": AXIS_ID,
        "split_contract": {"train_end": TRAIN_END, "eval_start": EVAL_START, "point_in_time_features_only": True},
        "input_normalization": {"non_finite_replaced_with_nan_for_median_imputation": non_finite_count},
        "upside_probability": _probability_metrics(review["winner_ret20_gt_10pct"].astype(int), review["upside_probability_20d"]),
        "downside_risk_probability": _probability_metrics(review["bad_ret20_lt_minus_5pct"].astype(int), review["downside_risk_probability_20d"]),
        "cohorts": {"all": _cohort(review), "avoid": _cohort(avoid), "non_avoid": _cohort(non_avoid)},
        "avoid_exclusion_delta": {
            "bad_rate_delta_non_avoid_vs_all": _cohort(non_avoid)["bad_ret20_lt_minus_5pct_rate"] - _cohort(review)["bad_ret20_lt_minus_5pct_rate"],
            "severe_rate_delta_non_avoid_vs_all": _cohort(non_avoid)["severe_ret20_lt_minus_10pct_rate"] - _cohort(review)["severe_ret20_lt_minus_10pct_rate"],
        },
        "by_avoid_reason": reason_rows,
        "yearly": yearly,
    }
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    review.to_parquet(output / "entry_actionability_and_avoidance_surface.parquet", index=False)
    review.head(1000).to_csv(output / "entry_actionability_and_avoidance_surface_sample.csv", index=False)
    _write_json(output / "surface_metrics.json", metrics)
    _write_json(output / "feature_contract.json", {"features": FEATURES, "offline_labels_evaluation_only": ["ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"], "event_risk_contract_status": "unavailable_not_silently_fallbacked"})
    _write_json(output / "research_decision.json", {"decision_class": "READY_REVIEW_ONLY", "research_decision": "entry_actionability_and_avoidance_surface_ready_for_manual_review", "meemee_unchanged": True, "production_ranking_changed": False, "candidate_generation_changed": False, "runtime_db_write": False, "validated_buy_count": 0})
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
