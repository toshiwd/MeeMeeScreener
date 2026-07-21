from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Iterable

import duckdb
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, precision_score, recall_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler


AXES = (
    "baseline",
    "duration",
    "high_low_compression",
    "center_movement",
    "candle_structure",
    "trend_pause",
)
WINDOWS = (10, 15, 20, 30)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def slope_share(values: np.ndarray) -> float:
    values = np.asarray(values, dtype=float)
    price_range = float(np.nanmax(values) - np.nanmin(values))
    if len(values) < 2 or price_range <= 0:
        return 0.0
    x = np.arange(len(values), dtype=float)
    x -= x.mean()
    return float(np.dot(x, values - values.mean()) / np.square(x).sum() * (len(values) - 1) / price_range)


def efficiency(values: np.ndarray) -> float:
    values = np.asarray(values, dtype=float)
    path = float(np.abs(np.diff(values)).sum())
    net = float(abs(values[-1] - values[0]))
    return net / path if path > 0 else 0.0


def load_histories(db_path: Path, cases: pd.DataFrame) -> pd.DataFrame:
    codes = sorted(set(cases["code"].astype(str).str.zfill(4)))
    placeholders = ",".join("?" for _ in codes)
    query = f"""
        select cast(code as varchar) code,
               cast(strftime(to_timestamp(date), '%Y%m%d') as integer) ymd,
               cast(o as double) o, cast(h as double) h, cast(l as double) l,
               cast(c as double) c, cast(v as double) v
        from daily_bars
        where source='pan' and c>0 and h>=l and cast(code as varchar) in ({placeholders})
          and cast(strftime(to_timestamp(date), '%Y%m%d') as integer) <= 20251231
        order by code, ymd
    """
    with duckdb.connect(str(db_path), read_only=True) as connection:
        bars = connection.execute(query, codes).fetchdf()
    bars["code"] = bars["code"].astype(str).str.zfill(4)
    return bars.drop_duplicates(["code", "ymd"], keep="last")


def event_features(history: pd.DataFrame) -> dict[str, float]:
    history = history.tail(100).reset_index(drop=True)
    close = history["c"].to_numpy(float)
    high = history["h"].to_numpy(float)
    low = history["l"].to_numpy(float)
    open_ = history["o"].to_numpy(float)
    previous = np.r_[np.nan, close[:-1]]
    true_range = np.nanmax(np.c_[high - low, np.abs(high - previous), np.abs(low - previous)], axis=1)
    result: dict[str, float] = {}
    for window in WINDOWS:
        c, h, l = close[-window:], high[-window:], low[-window:]
        price_range = float(np.max(h) - np.min(l))
        midpoint = (h + l) / 2.0
        result[f"efficiency_{window}"] = efficiency(c)
        result[f"slope_share_{window}"] = abs(slope_share(c))
        result[f"range_pct_{window}"] = price_range / close[-1]
        result[f"range_atr_{window}"] = price_range / np.nanmean(true_range[-window:])
        result[f"center_drift_{window}"] = abs(midpoint[-1] - midpoint[0]) / price_range if price_range else 0.0
        result[f"center_std_{window}"] = float(np.std(midpoint) / price_range) if price_range else 0.0
    flat_flags = []
    for end in range(max(14, len(close) - 20), len(close)):
        segment = close[end - 14 : end + 1]
        flat_flags.append(efficiency(segment) <= 0.35 and abs(slope_share(segment)) <= 0.50)
    run = 0
    for flag in reversed(flat_flags):
        if not flag:
            break
        run += 1
    result["flat_run_days"] = float(run)
    result["flat_share_20"] = float(np.mean(flat_flags)) if flat_flags else 0.0
    atr14 = float(np.mean(true_range[-14:]))
    atr60 = float(np.mean(true_range[-60:]))
    result["atr14_atr60"] = atr14 / atr60 if atr60 else np.nan
    last15 = history.tail(15)
    candle_range = (last15["h"] - last15["l"]).replace(0.0, np.nan)
    body = (last15["c"] - last15["o"]).abs()
    upper = last15["h"] - last15[["o", "c"]].max(axis=1)
    lower = last15[["o", "c"]].min(axis=1) - last15["l"]
    result["body_range_mean15"] = float((body / candle_range).mean())
    result["wick_range_mean15"] = float(((upper + lower) / candle_range).mean())
    signs = np.sign((last15["c"] - last15["o"]).to_numpy(float))
    result["direction_flip_share15"] = float(np.mean(signs[1:] * signs[:-1] < 0))
    prior_high = last15["h"].shift(1)
    prior_low = last15["l"].shift(1)
    overlap = (np.minimum(last15["h"], prior_high) - np.maximum(last15["l"], prior_low)).clip(lower=0)
    union = np.maximum(last15["h"], prior_high) - np.minimum(last15["l"], prior_low)
    result["overlap_share15"] = float((overlap / union.replace(0.0, np.nan)).mean())
    result["ret20"] = float(close[-1] / close[-21] - 1.0)
    result["ret60"] = float(close[-1] / close[-61] - 1.0)
    result["ma20_ma60"] = float(np.mean(close[-20:]) / np.mean(close[-60:]) - 1.0)
    result["prior20_ret"] = float(close[-16] / close[-36] - 1.0)
    result["prior20_efficiency"] = efficiency(close[-35:-15])
    result["prior20_slope_share"] = abs(slope_share(close[-35:-15]))
    return result


def build_feature_table(bars: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    grouped = {code: frame.sort_values("ymd") for code, frame in bars.groupby("code", sort=False)}
    rows = []
    for case in labels.itertuples(index=False):
        history = grouped[str(case.code).zfill(4)]
        history = history[history["ymd"] <= int(case.ymd)]
        if len(history) < 61:
            raise ValueError(f"insufficient history for {case.case_id}")
        rows.append({"case_id": case.case_id, **event_features(history)})
    return labels.merge(pd.DataFrame(rows), on="case_id", validate="one_to_one")


def feature_sets() -> dict[str, list[str]]:
    groups = {
        "baseline": ["efficiency_15", "slope_share_15"],
        "duration": ["efficiency_10", "efficiency_20", "efficiency_30", "slope_share_10", "slope_share_20", "slope_share_30", "flat_run_days", "flat_share_20"],
        "high_low_compression": [*[f"range_pct_{w}" for w in WINDOWS], *[f"range_atr_{w}" for w in WINDOWS], "atr14_atr60"],
        "center_movement": [*[f"center_drift_{w}" for w in WINDOWS], *[f"center_std_{w}" for w in WINDOWS]],
        "candle_structure": ["body_range_mean15", "wick_range_mean15", "direction_flip_share15", "overlap_share15"],
        "trend_pause": ["ret20", "ret60", "ma20_ma60", "prior20_ret", "prior20_efficiency", "prior20_slope_share"],
    }
    cumulative: dict[str, list[str]] = {}
    active: list[str] = []
    for axis in AXES:
        active = [*active, *groups[axis]]
        cumulative[axis] = list(active)
    return cumulative


def model(c_value: float) -> Pipeline:
    return Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", RobustScaler()),
        ("model", LogisticRegression(C=c_value, class_weight="balanced", max_iter=2000, random_state=17)),
    ])


def choose_hyperparameters(train: pd.DataFrame, features: list[str]) -> tuple[float, float]:
    candidates = []
    years = sorted(train["year"].unique())
    for c_value in (0.03, 0.1, 0.3, 1.0, 3.0):
        probabilities = pd.Series(index=train.index, dtype=float)
        for year in years:
            fit = train[train["year"] != year]
            valid = train[train["year"] == year]
            estimator = model(c_value).fit(fit[features], fit["target"])
            probabilities.loc[valid.index] = estimator.predict_proba(valid[features])[:, 1]
        for threshold in np.arange(0.30, 0.71, 0.05):
            predicted = probabilities.ge(threshold)
            precision = precision_score(train["target"], predicted, zero_division=0)
            recall = recall_score(train["target"], predicted, zero_division=0)
            balanced = balanced_accuracy_score(train["target"], predicted)
            gate_bonus = int(precision >= 0.70 and recall >= 0.70)
            candidates.append((gate_bonus, balanced, min(precision, recall), -abs(threshold - 0.5), -c_value, c_value, float(threshold)))
    best = max(candidates)
    return float(best[-2]), float(best[-1])


def evaluate_axis(frame: pd.DataFrame, features: list[str]) -> tuple[dict, pd.DataFrame]:
    predictions = []
    for year in sorted(frame["year"].unique()):
        train, test = frame[frame["year"] != year], frame[frame["year"] == year]
        c_value, threshold = choose_hyperparameters(train, features)
        estimator = model(c_value).fit(train[features], train["target"])
        probability = estimator.predict_proba(test[features])[:, 1]
        part = test[["case_id", "year", "target", "confidence"]].copy()
        part["probability"] = probability
        part["prediction"] = probability >= threshold
        part["selected_c"] = c_value
        part["selected_threshold"] = threshold
        predictions.append(part)
    out = pd.concat(predictions).sort_values("case_id").reset_index(drop=True)
    precision = precision_score(out["target"], out["prediction"], zero_division=0)
    recall = recall_score(out["target"], out["prediction"], zero_division=0)
    balanced = balanced_accuracy_score(out["target"], out["prediction"])
    by_year = [
        {"year": int(year), "rows": int(len(part)), "accuracy": float((part["target"] == part["prediction"]).mean())}
        for year, part in out.groupby("year", sort=True)
    ]
    metrics = {
        "rows": int(len(out)), "precision": float(precision), "recall": float(recall),
        "balanced_accuracy": float(balanced), "accuracy": float((out["target"] == out["prediction"]).mean()),
        "minimum_year_accuracy": min(row["accuracy"] for row in by_year), "by_year": by_year,
    }
    metrics["gate_pass"] = bool(
        metrics["balanced_accuracy"] >= 0.75 and metrics["precision"] >= 0.70
        and metrics["recall"] >= 0.70 and metrics["minimum_year_accuracy"] >= 0.65
    )
    return metrics, out


def run(db_path: Path, human_path: Path, sealed_path: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    human = pd.read_parquet(human_path)
    sealed = pd.read_parquet(sealed_path)[["case_id", "code", "ymd", "year", "sample_group"]]
    labels = human.merge(sealed, on=["case_id", "code", "ymd"], validate="one_to_one")
    labels = labels[labels["sideways_decision"] != "BORDERLINE"].copy()
    labels["target"] = labels["sideways_decision"].eq("SIDEWAYS")
    bars = load_histories(db_path, labels)
    feature_table = build_feature_table(bars, labels)
    feature_table.to_parquet(output / "feature_table.parquet", index=False)
    results, prediction_parts = [], []
    for axis, features in feature_sets().items():
        metrics, predictions = evaluate_axis(feature_table, features)
        results.append({"axis": axis, "feature_count": len(features), "features": features, **metrics})
        predictions.insert(0, "axis", axis)
        prediction_parts.append(predictions)
    pd.concat(prediction_parts, ignore_index=True).to_parquet(output / "loyo_predictions.parquet", index=False)
    best = max(results, key=lambda row: (row["gate_pass"], row["balanced_accuracy"], row["recall"], row["precision"]))
    payload = {
        "schema_version": "tradex_sideways_human_feature_axis_v1",
        "artifact_role": "authoritative_human_sideways_feature_axis_comparison",
        "review_only": True,
        "fixed_conditions": {
            "database": str(db_path), "human_freeze_sha256": sha256(human_path), "sealed_sha256": sha256(sealed_path),
            "rows_excluding_borderline": int(len(labels)), "validation": "nested_leave_one_year_out",
            "model": "median_impute_robust_scale_balanced_logistic", "future_bars_used": False,
        },
        "gate": {"balanced_accuracy_min": 0.75, "precision_min": 0.70, "recall_min": 0.70, "each_year_accuracy_min": 0.65},
        "axes": results,
        "best_axis": best["axis"],
        "judgment": "keep_for_new_blind_confirmation" if best["gate_pass"] else "hold_no_axis_passed_gate",
        "not_changed": ["existing sideways detector", "MeeMee", "ranking", "runtime DB", "trade rules"],
    }
    compare_path = output / "compare.json"
    compare_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha256(compare_path)}, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--human", type=Path, required=True)
    parser.add_argument("--sealed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.db, args.human, args.sealed, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
