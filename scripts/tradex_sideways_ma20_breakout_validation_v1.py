from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from scipy.stats import binomtest


def selected_result(compare: dict, checkpoint: int, task: str, stage: str) -> dict:
    return next(row for row in compare["results"] if row["checkpoint"] == checkpoint and row["task"] == task and row["stage"] == stage)


def run(compare_path: Path, features_path: Path, predictions_path: Path, bands_path: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    compare = json.loads(compare_path.read_text(encoding="utf-8"))
    features = pd.read_parquet(features_path)
    predictions = pd.read_parquet(predictions_path)
    bands = pd.read_parquet(bands_path)
    selected = predictions[(predictions.checkpoint == 2) & (predictions.task == "direction")]
    baseline = selected[selected.stage == "band_position_only"].set_index("case_id")
    ma20 = selected[selected.stage == "band_plus_ma20"].set_index("case_id")
    joined = baseline[["direction_up", "prediction"]].rename(columns={"prediction": "baseline_prediction"}).join(
        ma20[["prediction"]].rename(columns={"prediction": "ma20_prediction"}), validate="one_to_one"
    )
    baseline_correct = joined.baseline_prediction == joined.direction_up
    ma20_correct = joined.ma20_prediction == joined.direction_up
    correct = int(ma20_correct.sum())
    interval = binomtest(correct, len(joined)).proportion_ci(confidence_level=0.95, method="wilson")
    day2_features = features[features.checkpoint == 2].set_index("case_id").loc[joined.index]
    simple_ma20 = day2_features.close_ma20_signed_atr14 > 0
    day2_ma20 = selected_result(compare, 2, "direction", "band_plus_ma20")
    day2_base = selected_result(compare, 2, "direction", "band_position_only")
    timing0_ma20 = selected_result(compare, 0, "timing_3d", "band_plus_ma20")
    timing0_base = selected_result(compare, 0, "timing_3d", "band_position_only")
    quality = {
        "band_rows": int(len(bands)), "band_unique_cases": int(bands.case_id.nunique()),
        "human_sideways_rows": int((bands.sideways_decision == "SIDEWAYS").sum()),
        "resolved_breakout_rows": int(((bands.sideways_decision == "SIDEWAYS") & bands.close_break_direction.isin(["UP", "DOWN"])).sum()),
        "unresolved_sideways_rows": int(((bands.sideways_decision == "SIDEWAYS") & (bands.close_break_direction == "UNRESOLVED")).sum()),
        "checkpoint_feature_rows": int(len(features)), "duplicate_checkpoint_keys": int(features.duplicated(["case_id", "checkpoint"]).sum()),
        "required_feature_nulls": int(features[["band_position", "distance_to_nearest_edge_atr14", "ma20_signed_slope5_atr60", "close_ma20_signed_atr14", "ma20_range10_atr60", "ma20_touch_share15", "same_ma20_side_run"]].isna().sum().sum()),
        "day2_prediction_join_rows": int(len(joined)), "day2_expected_rows": int((features.checkpoint == 2).sum()),
    }
    validation = {
        "schema_version": "tradex_sideways_ma20_breakout_validation_v1",
        "review_only": True, "overall_assessment": "share_with_caveats",
        "source_quality": quality,
        "direction_day2": {
            "rows": int(len(joined)), "ma20_correct": correct, "ma20_accuracy": float(ma20_correct.mean()),
            "wilson_95_low": float(interval.low), "wilson_95_high": float(interval.high),
            "balanced_accuracy": float(day2_ma20["balanced_accuracy"]), "minimum_year_accuracy": float(day2_ma20["minimum_year_accuracy"]),
            "baseline_correct": int(baseline_correct.sum()), "baseline_accuracy": float(baseline_correct.mean()),
            "baseline_balanced_accuracy": float(day2_base["balanced_accuracy"]), "baseline_minimum_year_accuracy": float(day2_base["minimum_year_accuracy"]),
            "changed_predictions": int((joined.baseline_prediction != joined.ma20_prediction).sum()),
            "baseline_wrong_ma20_right": int((~baseline_correct & ma20_correct).sum()),
            "baseline_right_ma20_wrong": int((baseline_correct & ~ma20_correct).sum()),
            "simple_ma20_sign_accuracy": float((simple_ma20 == joined.direction_up).mean()),
            "decision": "hold_for_new_blind_confirmation_not_adoption",
        },
        "timing_day0": {
            "baseline_balanced_accuracy": float(timing0_base["balanced_accuracy"]),
            "ma20_balanced_accuracy": float(timing0_ma20["balanced_accuracy"]),
            "ma20_minimum_year_accuracy": float(timing0_ma20["minimum_year_accuracy"]),
            "decision": "hold_weak_early_signal_but_drop_as_confirmed_timing_rule",
        },
        "methodology_risks": [
            "direction population excludes 8 human-sideways cases without a close breakout inside 20 sessions",
            "best checkpoint was selected after comparing six checkpoints",
            "only 36 cases remain at day2 and annual cells are small",
            "MA20 adds no net correct cases at day2; it only changes class balance and annual stability",
        ],
        "not_changed": ["compression band", "breakout labels", "MeeMee", "ranking", "runtime DB", "trade rules"],
    }
    validation_path = output / "validation.json"
    validation_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def markdown(source: str) -> dict:
        return {"cell_type": "markdown", "metadata": {}, "source": source.splitlines(keepends=True)}

    def code(source: str) -> dict:
        return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": source.splitlines(keepends=True)}

    notebook = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}, "language_info": {"name": "python", "version": "3.12"}},
        "cells": [
            markdown("## tl;dr\n\n20MAはブレイク時期の確定材料にはならなかった。方向はday2で36件中30件正解だが、帯内位置だけでも30件正解であり、新規ブラインド確認前の保留とする。"),
            markdown("## Context & Methods\n\n人間SIDEWAYSのうち20営業日以内に終値突破した43件を対象に、未突破ケースだけをday0〜day5で比較。年外検証を使用。\n\n### Key Assumptions\n\n圧縮帯は基準日までの情報だけで固定。方向は最初の終値突破。"),
            code(f"from pathlib import Path\nimport json, pandas as pd\nCOMPARE=Path(r'{compare_path}')\nFEATURES=Path(r'{features_path}')\nPREDICTIONS=Path(r'{predictions_path}')\nVALIDATION=Path(r'{validation_path}')\ncompare=json.loads(COMPARE.read_text(encoding='utf-8'))\nfeatures=pd.read_parquet(FEATURES)\npredictions=pd.read_parquet(PREDICTIONS)\nvalidation=json.loads(VALIDATION.read_text(encoding='utf-8'))\nvalidation['source_quality']"),
            markdown("## Data\n\n粒度は横ばいケース×チェックポイント。すでに終値突破したケースは次のチェックポイントから除外する。"),
            code("features.groupby('checkpoint').agg(rows=('case_id','size'), cases=('case_id','nunique'), next3_breaks=('break_within_3d','sum')).reset_index()"),
            markdown("## Results\n\n方向と3日以内突破について、帯内位置のみと20MA追加を同じチェックポイントで比較する。"),
            code("pd.DataFrame(compare['results'])[['checkpoint','task','stage','rows','accuracy','balanced_accuracy','minimum_year_accuracy']]"),
            code("validation['direction_day2']"),
            markdown("## Takeaways\n\n- 20MAはday0の接近判定に弱い改善があるが、最良の時期判定ではない。\n- day2方向判定は83.3%だが、正解数は帯内位置だけと同じ。\n- 年別最低精度の改善は有用な可能性があるため、独立サンプルで再確認する。"),
        ],
    }
    notebook_path = output / "ma20_breakout_validation.ipynb"
    notebook_path.write_text(json.dumps(notebook, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return validation


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare", type=Path, required=True)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--bands", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.compare, args.features, args.predictions, args.bands, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
