from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

if __package__:
    from scripts.tradex_sideways_compression_band_v1 import load_bars, sha256, true_range
    from scripts.tradex_sideways_human_feature_axis_v1 import evaluate_axis
else:
    from tradex_sideways_compression_band_v1 import load_bars, sha256, true_range
    from tradex_sideways_human_feature_axis_v1 import evaluate_axis


COMPRESSION_FEATURES = [
    "band_normalized_width", "band_efficiency", "band_slope_share",
    "band_center_drift", "atr14_atr60",
]
MA20_FEATURES = [
    "ma20_slope5_atr60", "ma20_range10_atr60", "close_ma20_distance_atr14",
    "ma20_touch_share15", "ma20_cross_count15", "close_near_ma20_share15",
]


def ma20_features(history: pd.DataFrame) -> dict[str, float]:
    history = history.tail(100).reset_index(drop=True).copy()
    if len(history) < 60:
        raise ValueError("at least 60 prior bars are required")
    history["ma20"] = history.c.rolling(20, min_periods=20).mean()
    atr = true_range(history)
    atr14, atr60 = float(atr.tail(14).mean()), float(atr.tail(60).mean())
    recent = history.tail(15).copy()
    side = np.sign((recent.c - recent.ma20).to_numpy(float))
    valid_side = side[np.isfinite(side)]
    crosses = int(np.sum(valid_side[1:] * valid_side[:-1] < 0)) if len(valid_side) > 1 else 0
    touch = ((recent.l <= recent.ma20) & (recent.h >= recent.ma20)).mean()
    near = ((recent.c - recent.ma20).abs() <= 0.5 * atr14).mean() if atr14 > 0 else np.nan
    ma20 = history.ma20
    return {
        "ma20_slope5_atr60": float(abs(ma20.iloc[-1] - ma20.iloc[-6]) / (5 * atr60)) if atr60 > 0 else np.nan,
        "ma20_range10_atr60": float((ma20.tail(10).max() - ma20.tail(10).min()) / atr60) if atr60 > 0 else np.nan,
        "close_ma20_distance_atr14": float(abs(history.c.iloc[-1] - ma20.iloc[-1]) / atr14) if atr14 > 0 else np.nan,
        "ma20_touch_share15": float(touch), "ma20_cross_count15": float(crosses),
        "close_near_ma20_share15": float(near),
    }


def run(db_path: Path, human_path: Path, sealed_path: Path, bands_path: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    human = pd.read_parquet(human_path)
    sealed = pd.read_parquet(sealed_path)[["case_id", "code", "ymd", "year", "sample_group"]]
    labels = human.merge(sealed, on=["case_id", "code", "ymd"], validate="one_to_one")
    labels = labels[labels.sideways_decision != "BORDERLINE"].copy()
    labels["target"] = labels.sideways_decision.eq("SIDEWAYS")
    labels["code"] = labels.code.astype(str).str.zfill(4)
    bands = pd.read_parquet(bands_path)[["case_id", *COMPRESSION_FEATURES]]
    bars = load_bars(db_path, sorted(labels.code.unique()))
    grouped = {code: part.reset_index(drop=True) for code, part in bars.groupby("code", sort=False)}
    rows = []
    for case in labels.itertuples(index=False):
        history = grouped[case.code]
        history = history[history.ymd <= int(case.ymd)]
        rows.append({"case_id": case.case_id, **ma20_features(history)})
    table = labels.merge(bands, on="case_id", validate="one_to_one").merge(pd.DataFrame(rows), on="case_id", validate="one_to_one")
    table.to_parquet(output / "feature_table.parquet", index=False)
    stages = []
    predictions = []
    for name, features in (("compression_only", COMPRESSION_FEATURES), ("compression_plus_ma20", [*COMPRESSION_FEATURES, *MA20_FEATURES])):
        metrics, pred = evaluate_axis(table, features)
        stages.append({"axis": name, "features": features, **metrics})
        pred.insert(0, "axis", name)
        predictions.append(pred)
    pd.concat(predictions, ignore_index=True).to_parquet(output / "loyo_predictions.parquet", index=False)
    baseline, challenger = stages
    improved = challenger["balanced_accuracy"] > baseline["balanced_accuracy"] and challenger["recall"] >= baseline["recall"]
    payload = {
        "schema_version": "tradex_sideways_ma20_structure_v1",
        "artifact_role": "authoritative_fixed_condition_ma20_sideways_challenger",
        "review_only": True,
        "fixed_conditions": {
            "database": str(db_path), "human_freeze_sha256": sha256(human_path), "sealed_sha256": sha256(sealed_path),
            "rows_excluding_borderline": int(len(table)), "validation": "nested_leave_one_year_out",
            "future_features_used": False, "compression_band_changed": False,
        },
        "changed_axis": "ma20_structure_only", "stages": stages,
        "branching": {
            "changed_prediction_count": int((predictions[0].set_index("case_id").prediction != predictions[1].set_index("case_id").prediction).sum()),
            "selection_divergence_reason": "ma20 flatness, contact, crossing, and proximity",
        },
        "judgment": "keep_for_chart_review" if improved else "drop_no_fixed_condition_improvement",
        "not_changed": ["compression band", "breakout labels", "direction predictor", "MeeMee", "ranking", "runtime DB", "trade rules"],
    }
    compare = output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha256(compare)}, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--human", type=Path, required=True)
    parser.add_argument("--sealed", type=Path, required=True)
    parser.add_argument("--bands", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.db, args.human, args.sealed, args.bands, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
