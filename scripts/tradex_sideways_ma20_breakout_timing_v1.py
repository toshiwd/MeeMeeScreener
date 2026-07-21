from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, balanced_accuracy_score, recall_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

if __package__:
    from scripts.tradex_sideways_compression_band_v1 import load_bars, sha256, true_range
else:
    from tradex_sideways_compression_band_v1 import load_bars, sha256, true_range


CHECKPOINTS = tuple(range(6))
BASE_FEATURES = ["band_position", "distance_to_nearest_edge_atr14"]
MA20_FEATURES = [
    "ma20_signed_slope5_atr60", "close_ma20_signed_atr14",
    "ma20_range10_atr60", "ma20_touch_share15", "same_ma20_side_run",
]


def snapshot_features(history: pd.DataFrame, lower: float, upper: float) -> dict[str, float]:
    history = history.tail(100).reset_index(drop=True).copy()
    history["ma20"] = history.c.rolling(20, min_periods=20).mean()
    atr = true_range(history)
    atr14, atr60 = float(atr.tail(14).mean()), float(atr.tail(60).mean())
    close, ma20 = float(history.c.iloc[-1]), float(history.ma20.iloc[-1])
    width = upper - lower
    recent = history.tail(15)
    side = np.sign((history.c - history.ma20).to_numpy(float))
    current_side = side[-1]
    run = 0
    for value in side[::-1]:
        if value == 0 or value != current_side:
            break
        run += 1
    return {
        "band_position": (close - lower) / width if width > 0 else 0.5,
        "distance_to_nearest_edge_atr14": min(abs(upper - close), abs(close - lower)) / atr14 if atr14 > 0 else np.nan,
        "ma20_signed_slope5_atr60": (ma20 - float(history.ma20.iloc[-6])) / (5 * atr60) if atr60 > 0 else np.nan,
        "close_ma20_signed_atr14": (close - ma20) / atr14 if atr14 > 0 else np.nan,
        "ma20_range10_atr60": (float(history.ma20.tail(10).max()) - float(history.ma20.tail(10).min())) / atr60 if atr60 > 0 else np.nan,
        "ma20_touch_share15": float(((recent.l <= recent.ma20) & (recent.h >= recent.ma20)).mean()),
        "same_ma20_side_run": float(run),
    }


def model() -> Pipeline:
    return Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", RobustScaler()),
        ("model", LogisticRegression(C=0.3, class_weight="balanced", max_iter=2000, random_state=17)),
    ])


def loyo(frame: pd.DataFrame, features: list[str], target: str) -> tuple[dict, pd.DataFrame]:
    parts = []
    for year in sorted(frame.year.unique()):
        train, test = frame[frame.year != year], frame[frame.year == year]
        if train[target].nunique() < 2:
            continue
        estimator = model().fit(train[features], train[target])
        part = test[["case_id", "year", target]].copy()
        part["probability"] = estimator.predict_proba(test[features])[:, 1]
        part["prediction"] = part.probability >= 0.5
        parts.append(part)
    out = pd.concat(parts, ignore_index=True)
    accuracy = float(accuracy_score(out[target], out.prediction))
    balanced = float(balanced_accuracy_score(out[target], out.prediction))
    by_year = [
        {"year": int(year), "rows": int(len(part)), "accuracy": float(accuracy_score(part[target], part.prediction))}
        for year, part in out.groupby("year", sort=True)
    ]
    return {
        "rows": int(len(out)), "positive_rows": int(out[target].sum()),
        "accuracy": accuracy, "balanced_accuracy": balanced,
        "positive_recall": float(recall_score(out[target], out.prediction, zero_division=0)),
        "negative_recall": float(recall_score(~out[target], ~out.prediction, zero_division=0)),
        "minimum_year_accuracy": min(row["accuracy"] for row in by_year), "by_year": by_year,
    }, out


def run(db_path: Path, bands_path: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    bands = pd.read_parquet(bands_path)
    events = bands[(bands.sideways_decision == "SIDEWAYS") & bands.close_break_direction.isin(["UP", "DOWN"])].copy()
    bars = load_bars(db_path, sorted(events.code.unique()))
    grouped = {code: part.reset_index(drop=True) for code, part in bars.groupby("code", sort=False)}
    rows = []
    for case in events.itertuples(index=False):
        series = grouped[case.code]
        base_positions = series.index[series.ymd == int(case.ymd)].tolist()
        if len(base_positions) != 1:
            raise ValueError(f"base bar missing for {case.case_id}")
        base_index = base_positions[0]
        for checkpoint in CHECKPOINTS:
            if int(case.close_break_day) <= checkpoint:
                continue
            index = base_index + checkpoint
            features = snapshot_features(series.iloc[: index + 1], float(case.band_lower), float(case.band_upper))
            rows.append({
                "case_id": case.case_id, "code": case.code, "year": int(case.year), "base_ymd": int(case.ymd),
                "checkpoint": checkpoint, "checkpoint_ymd": int(series.iloc[index].ymd),
                "break_day": int(case.close_break_day), "direction_up": case.close_break_direction == "UP",
                "break_within_3d": int(case.close_break_day) <= checkpoint + 3,
                **features,
            })
    table = pd.DataFrame(rows)
    table.to_parquet(output / "checkpoint_features.parquet", index=False)
    results, prediction_parts = [], []
    for checkpoint in CHECKPOINTS:
        current = table[table.checkpoint == checkpoint].copy()
        for task, target in (("timing_3d", "break_within_3d"), ("direction", "direction_up")):
            for stage, features in (("band_position_only", BASE_FEATURES), ("band_plus_ma20", [*BASE_FEATURES, *MA20_FEATURES])):
                metrics, predictions = loyo(current, features, target)
                results.append({"checkpoint": checkpoint, "task": task, "stage": stage, "features": features, **metrics})
                predictions.insert(0, "stage", stage)
                predictions.insert(0, "task", task)
                predictions.insert(0, "checkpoint", checkpoint)
                prediction_parts.append(predictions)
    predictions = pd.concat(prediction_parts, ignore_index=True)
    predictions.to_parquet(output / "loyo_predictions.parquet", index=False)
    result_table = pd.DataFrame(results)
    decisions = []
    for task in ("timing_3d", "direction"):
        task_rows = result_table[result_table.task == task]
        best = task_rows.sort_values(["balanced_accuracy", "minimum_year_accuracy"], ascending=False).iloc[0]
        matching_base = task_rows[(task_rows.checkpoint == best.checkpoint) & (task_rows.stage == "band_position_only")].iloc[0]
        improved = best.stage == "band_plus_ma20" and best.balanced_accuracy > matching_base.balanced_accuracy
        decisions.append({
            "task": task, "best_checkpoint": int(best.checkpoint), "best_stage": best.stage,
            "balanced_accuracy": float(best.balanced_accuracy), "accuracy": float(best.accuracy),
            "minimum_year_accuracy": float(best.minimum_year_accuracy),
            "same_checkpoint_baseline_balanced_accuracy": float(matching_base.balanced_accuracy),
            "decision": "hold_ma20_incremental_signal" if improved else "drop_no_ma20_incremental_signal",
        })
    payload = {
        "schema_version": "tradex_sideways_ma20_breakout_timing_v1",
        "artifact_role": "authoritative_fixed_condition_ma20_breakout_timing_direction",
        "review_only": True,
        "fixed_conditions": {
            "database": str(db_path), "compression_bands_sha256": sha256(bands_path),
            "population": "human SIDEWAYS with resolved first close breakout within 20 sessions",
            "rows": int(len(events)), "checkpoints": list(CHECKPOINTS),
            "checkpoint_population": "first close breakout day greater than checkpoint",
            "timing_target": "first close breakout occurs within next 3 sessions",
            "direction_target": "first close breakout is UP",
            "validation": "leave_one_year_out_fixed_logistic", "future_features_used": False,
        },
        "changed_axis": "ma20_dynamic_departure_only", "results": results, "decisions": decisions,
        "judgment": "hold_pending_validation",
        "not_changed": ["compression band", "breakout labels", "human labels", "MeeMee", "ranking", "runtime DB", "trade rules"],
    }
    compare = output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha256(compare)}, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--bands", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.db, args.bands, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
