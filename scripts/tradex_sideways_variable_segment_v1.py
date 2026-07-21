from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

if __package__:
    from scripts.tradex_sideways_human_feature_axis_v1 import (
        build_feature_table, efficiency, evaluate_axis, feature_sets, load_histories, sha256, slope_share,
    )
else:
    from tradex_sideways_human_feature_axis_v1 import (
        build_feature_table, efficiency, evaluate_axis, feature_sets, load_histories, sha256, slope_share,
    )


SEGMENT_WINDOWS = tuple(range(10, 61, 5))
END_OFFSETS = (0, 3, 5)


def variable_segment_features(history: pd.DataFrame) -> dict[str, float]:
    history = history.tail(260).reset_index(drop=True)
    candidates: list[dict[str, float]] = []
    for offset in END_OFFSETS:
        end = len(history) - offset
        for window in SEGMENT_WINDOWS:
            if end < window:
                continue
            part = history.iloc[end - window : end]
            close = part["c"].to_numpy(float)
            high = part["h"].to_numpy(float)
            low = part["l"].to_numpy(float)
            midpoint = (high + low) / 2.0
            price_range = float(high.max() - low.min())
            previous = np.r_[np.nan, close[:-1]]
            true_range = np.nanmax(np.c_[high - low, np.abs(high - previous), np.abs(low - previous)], axis=1)
            range_atr = price_range / float(np.nanmean(true_range)) if np.nanmean(true_range) > 0 else np.nan
            center_drift = abs(midpoint[-1] - midpoint[0]) / price_range if price_range > 0 else 0.0
            row = {
                "window": float(window), "end_offset": float(offset),
                "efficiency": efficiency(close), "slope_share": abs(slope_share(close)),
                "range_pct": price_range / close[-1], "range_atr_scaled": range_atr / np.sqrt(window),
                "center_drift": center_drift,
            }
            row["plateau_score"] = (
                row["efficiency"] + row["slope_share"] + row["center_drift"]
                + 0.25 * row["range_atr_scaled"]
            )
            candidates.append(row)
    table = pd.DataFrame(candidates)
    best = table.sort_values(["plateau_score", "end_offset", "window"], ascending=[True, True, False]).iloc[0]
    result = {f"best_segment_{name}": float(best[name]) for name in (
        "window", "end_offset", "efficiency", "slope_share", "range_pct", "range_atr_scaled", "center_drift", "plateau_score"
    )}
    for name in ("efficiency", "slope_share", "range_pct", "range_atr_scaled", "center_drift", "plateau_score"):
        result[f"segment_min_{name}"] = float(table[name].min())
        result[f"segment_median_{name}"] = float(table[name].median())
    minimum = float(table["plateau_score"].min())
    result["segment_near_best_count"] = float((table["plateau_score"] <= minimum * 1.20).sum())
    current = table[table["end_offset"] == 0]
    result["current_best_window"] = float(current.loc[current["plateau_score"].idxmin(), "window"])
    result["current_best_score"] = float(current["plateau_score"].min())
    return result


def long_context_features(history: pd.DataFrame) -> dict[str, float]:
    history = history.tail(260).reset_index(drop=True)
    close = history["c"].to_numpy(float)
    high = history["h"].to_numpy(float)
    low = history["l"].to_numpy(float)
    result: dict[str, float] = {"context_history_bars": float(len(history))}
    for window in (63, 126, 252):
        if len(history) < window:
            for name in ("ret", "range_pos", "range_pct", "efficiency", "slope_share"):
                result[f"context_{name}_{window}"] = np.nan
            continue
        c, h, l = close[-window:], high[-window:], low[-window:]
        price_range = float(h.max() - l.min())
        result[f"context_ret_{window}"] = float(c[-1] / c[0] - 1.0)
        result[f"context_range_pos_{window}"] = float((c[-1] - l.min()) / price_range) if price_range else 0.5
        result[f"context_range_pct_{window}"] = float(price_range / c[-1])
        result[f"context_efficiency_{window}"] = efficiency(c)
        result[f"context_slope_share_{window}"] = abs(slope_share(c))
    for window in (20, 60, 120):
        result[f"context_ma_{window}_distance"] = float(close[-1] / np.mean(close[-window:]) - 1.0)
    result["recent15_to_year_range"] = (
        float((high[-15:].max() - low[-15:].min()) / (high[-252:].max() - low[-252:].min()))
        if len(history) >= 252 else np.nan
    )
    sampled = (
        close[np.linspace(len(close) - 252, len(close) - 1, 12).round().astype(int)] / close[-1] - 1.0
        if len(history) >= 252 else np.full(12, np.nan)
    )
    for index, value in enumerate(sampled):
        result[f"context_shape_{index:02d}"] = float(value)
    return result


def attach_new_features(bars: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    grouped = {code: frame.sort_values("ymd") for code, frame in bars.groupby("code", sort=False)}
    rows = []
    for case in base.itertuples(index=False):
        history = grouped[str(case.code).zfill(4)]
        history = history[history["ymd"] <= int(case.ymd)]
        if len(history) < 126:
            raise ValueError(f"insufficient 126-bar history for {case.case_id}")
        rows.append({"case_id": case.case_id, **variable_segment_features(history), **long_context_features(history)})
    return base.merge(pd.DataFrame(rows), on="case_id", validate="one_to_one")


def run(db_path: Path, human_path: Path, sealed_path: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    human = pd.read_parquet(human_path)
    sealed = pd.read_parquet(sealed_path)[["case_id", "code", "ymd", "year", "sample_group"]]
    labels = human.merge(sealed, on=["case_id", "code", "ymd"], validate="one_to_one")
    labels = labels[labels["sideways_decision"] != "BORDERLINE"].copy()
    labels["target"] = labels["sideways_decision"].eq("SIDEWAYS")
    bars = load_histories(db_path, labels)
    base = build_feature_table(bars, labels)
    table = attach_new_features(bars, base)
    context_252_missing_rows = int(table["context_ret_252"].isna().sum())
    table.to_parquet(output / "feature_table.parquet", index=False)
    center_features = feature_sets()["center_movement"]
    variable_features = [column for column in table.columns if column.startswith(("best_segment_", "segment_", "current_best_"))]
    context_features = [column for column in table.columns if column.startswith("context_") or column == "recent15_to_year_range"]
    stages = {
        "fixed15_center_baseline": center_features,
        "variable_segment": [*center_features, *variable_features],
        "variable_segment_plus_long_context": [*center_features, *variable_features, *context_features],
    }
    results, prediction_parts = [], []
    for axis, features in stages.items():
        metrics, predictions = evaluate_axis(table, features)
        results.append({"axis": axis, "feature_count": len(features), "features": features, **metrics})
        predictions.insert(0, "axis", axis)
        prediction_parts.append(predictions)
    pd.concat(prediction_parts, ignore_index=True).to_parquet(output / "loyo_predictions.parquet", index=False)
    best = max(results, key=lambda row: (row["gate_pass"], row["balanced_accuracy"], row["recall"], row["precision"]))
    payload = {
        "schema_version": "tradex_sideways_variable_segment_v1",
        "artifact_role": "authoritative_variable_segment_human_sideways_comparison",
        "review_only": True,
        "fixed_conditions": {
            "database": str(db_path), "human_freeze_sha256": sha256(human_path), "sealed_sha256": sha256(sealed_path),
            "rows_excluding_borderline": int(len(labels)), "validation": "nested_leave_one_year_out",
            "model_selection": "same_as_tradex_sideways_human_feature_axis_v1", "future_bars_used": False,
            "context_252_missing_rows": context_252_missing_rows,
            "context_missing_policy": "explicit_nan_then_training_fold_median_imputation",
        },
        "changed_axis": ["variable_segment_10_to_60", "long_context_63_to_252_added_second"],
        "gate": {"balanced_accuracy_min": 0.75, "precision_min": 0.70, "recall_min": 0.70, "each_year_accuracy_min": 0.65},
        "stages": results, "best_axis": best["axis"],
        "judgment": "keep_for_new_blind_confirmation" if best["gate_pass"] else "hold_no_variable_segment_candidate_passed_gate",
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
