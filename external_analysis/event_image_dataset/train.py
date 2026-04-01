from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, balanced_accuracy_score, precision_recall_fscore_support, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from external_analysis.event_image_dataset.paths import event_image_dataset_dir
from external_analysis.event_image_dataset.renderer import (
    CONTROL_EVALUATION_BUNDLE_ID,
    CONTROL_FEATUREIZER_SPEC_ID,
    FIDELITY_EVALUATION_BUNDLE_ID,
    FIDELITY_FEATUREIZER_SPEC_ID,
)
from external_analysis.event_image_dataset.storage import read_parquet_frame, write_parquet_frame
from external_analysis.image_rerank.artifacts import read_json, verify_roundtrip, write_json
from external_analysis.image_rerank.model import image_to_feature_vector


TRAIN_EVAL_SCHEMA_VERSION = "tradex_event_image_dataset_train_eval_v1_2"
BASELINE_METRICS_SCHEMA_VERSION = "tradex_event_image_dataset_metrics_v1_2"
FIDELITY_COMPARE_SCHEMA_VERSION = "tradex_event_image_dataset_fidelity_compare_v1_2"
REPRO_MANIFEST_SCHEMA_VERSION = "tradex_event_image_dataset_repro_manifest_v1"
REPRO_SUMMARY_SCHEMA_VERSION = "tradex_event_image_dataset_repro_summary_v1"
FIDELITY_IMAGE_FEATURE_SIZE = 48
CONTROL_IMAGE_FEATURE_SIZE = 12
NUMERIC_FEATURE_SPEC_ID = "monthly_event_numeric_day120_summary_v1"
DEFAULT_REPRO_SEEDS: tuple[int, ...] = (7, 11, 19, 29, 37)
LOWER_IS_BETTER_METRICS = {"monthly_bottom10_mean_forward_return"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json_artifact(path: Path, payload: dict[str, Any]) -> Path:
    write_json(path, payload)
    verify_roundtrip(path, payload)
    return path


def _series_stats(values: np.ndarray) -> dict[str, float | None]:
    if values.size <= 0:
        return {"count": 0, "mean": None, "std": None, "p10": None, "p50": None, "p90": None}
    return {
        "count": int(values.size),
        "mean": float(np.mean(values)),
        "std": float(np.std(values)),
        "p10": float(np.quantile(values, 0.10)),
        "p50": float(np.quantile(values, 0.50)),
        "p90": float(np.quantile(values, 0.90)),
    }


def _build_image_features(frame: pd.DataFrame, *, column: str, feature_size: int) -> np.ndarray:
    vectors = [image_to_feature_vector(path, size=feature_size) for path in frame[column].astype(str).tolist()]
    return np.asarray(vectors, dtype=np.float32)


def _build_numeric_features(frame: pd.DataFrame) -> np.ndarray:
    columns = ["return_1m_pre", "dist_ma20", "dist_ma60", "dist_ma120", "volume_change20", "position_from_60d_high", "realized_vol20"]
    return frame[columns].astype(float).fillna(0.0).to_numpy(dtype=np.float32, copy=True)


def _train_classifier(*, X_train: np.ndarray, y_train: np.ndarray, seed: int) -> Pipeline:
    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(class_weight="balanced", solver="liblinear", max_iter=1000, random_state=int(seed))),
        ]
    )
    model.fit(X_train, y_train)
    return model


def _monthly_ranking_metrics(frame: pd.DataFrame, prob_column: str) -> dict[str, float | None]:
    monthly_precision: list[float] = []
    monthly_top10_mean_return: list[float] = []
    monthly_bottom10_mean_return: list[float] = []
    monthly_spreads: list[float] = []
    for _, month_frame in frame.groupby("as_of_date", sort=True):
        ordered = month_frame.sort_values(prob_column, ascending=False).reset_index(drop=True)
        top_slice = ordered.head(min(10, len(ordered)))
        bottom_slice = ordered.tail(min(10, len(ordered)))
        if len(top_slice) <= 0 or len(bottom_slice) <= 0:
            continue
        monthly_precision.append(float(top_slice["label_id"].mean()))
        top_return = float(top_slice["forward_return_1m"].mean())
        bottom_return = float(bottom_slice["forward_return_1m"].mean())
        monthly_top10_mean_return.append(top_return)
        monthly_bottom10_mean_return.append(bottom_return)
        monthly_spreads.append(top_return - bottom_return)
    return {
        "monthly_top10_precision_up": float(np.mean(monthly_precision)) if monthly_precision else None,
        "monthly_top10_mean_forward_return": float(np.mean(monthly_top10_mean_return)) if monthly_top10_mean_return else None,
        "monthly_bottom10_mean_forward_return": float(np.mean(monthly_bottom10_mean_return)) if monthly_bottom10_mean_return else None,
        "monthly_long_short_spread": float(np.mean(monthly_spreads)) if monthly_spreads else None,
    }


def _evaluate_predictions(frame: pd.DataFrame, *, prob_column: str, pred_column: str) -> dict[str, Any]:
    labels = frame["label_id"].astype(int).to_numpy(dtype=np.int32, copy=False)
    probabilities = frame[prob_column].astype(float).to_numpy(dtype=np.float64, copy=False)
    predictions = frame[pred_column].astype(int).to_numpy(dtype=np.int32, copy=False)
    precision, recall, _, _ = precision_recall_fscore_support(labels, predictions, labels=[0, 1], zero_division=0)
    top_count = max(1, int(np.ceil(len(frame) * 0.10)))
    top_slice = frame.sort_values(prob_column, ascending=False).head(top_count)
    monthly_accuracy_values = [float((part["label_id"] == part[pred_column]).mean()) for _, part in frame.groupby("as_of_date", sort=True)]
    metrics = {
        "accuracy": float(accuracy_score(labels, predictions)),
        "balanced_accuracy": float(balanced_accuracy_score(labels, predictions)),
        "roc_auc": float(roc_auc_score(labels, probabilities)) if len(set(labels.tolist())) >= 2 else None,
        "top_decile_precision_up": float(top_slice["label_id"].mean()) if len(top_slice) > 0 else None,
        "monthly_mean_hit_rate": float(np.mean(monthly_accuracy_values)) if monthly_accuracy_values else None,
        "precision_by_class": {"bottom20_down": float(precision[0]), "top20_up": float(precision[1])},
        "recall_by_class": {"bottom20_down": float(recall[0]), "top20_up": float(recall[1])},
        "test_month_count": int(frame["as_of_date"].nunique()),
        "test_sample_count": int(len(frame)),
    }
    metrics.update(_monthly_ranking_metrics(frame, prob_column=prob_column))
    return metrics


def _metric_deltas(base: dict[str, Any], target: dict[str, Any]) -> dict[str, float | None]:
    keys = ("accuracy", "balanced_accuracy", "roc_auc", "top_decile_precision_up", "monthly_mean_hit_rate", "monthly_top10_precision_up", "monthly_top10_mean_forward_return", "monthly_bottom10_mean_forward_return", "monthly_long_short_spread")
    deltas: dict[str, float | None] = {}
    for key in keys:
        left = base.get(key)
        right = target.get(key)
        deltas[key] = None if left is None or right is None else float(right) - float(left)
    return deltas


def _mean_std(metrics_list: list[dict[str, Any]], metric_names: tuple[str, ...]) -> tuple[dict[str, float | None], dict[str, float | None]]:
    means: dict[str, float | None] = {}
    stds: dict[str, float | None] = {}
    for metric_name in metric_names:
        values = [float(item[metric_name]) for item in metrics_list if item.get(metric_name) is not None]
        if not values:
            means[metric_name] = None
            stds[metric_name] = None
            continue
        arr = np.asarray(values, dtype=np.float64)
        means[metric_name] = float(np.mean(arr))
        stds[metric_name] = float(np.std(arr))
    return means, stds


def _safe_delta(image_value: Any, numeric_value: Any) -> float | None:
    if image_value is None or numeric_value is None:
        return None
    return float(image_value) - float(numeric_value)


def _metric_prefers_higher(metric_name: str) -> bool:
    return metric_name not in LOWER_IS_BETTER_METRICS


def _metric_beats(metric_name: str, left_value: Any, right_value: Any) -> bool:
    if left_value is None or right_value is None:
        return False
    left = float(left_value)
    right = float(right_value)
    return left > right if _metric_prefers_higher(metric_name) else left < right


def _month_level_wins(predictions_frame: pd.DataFrame) -> list[dict[str, Any]]:
    wins: list[dict[str, Any]] = []
    test_frame = predictions_frame.loc[predictions_frame["split"] == "test"].copy()
    for as_of_date, month_frame in test_frame.groupby("as_of_date", sort=True):
        image_accuracy = float((month_frame["label_id"] == month_frame["image_pred_label"]).mean())
        numeric_accuracy = float((month_frame["label_id"] == month_frame["numeric_pred_label"]).mean())
        if image_accuracy > numeric_accuracy:
            winner = "image"
        elif image_accuracy < numeric_accuracy:
            winner = "numeric"
        else:
            winner = "tie"
        wins.append(
            {
                "as_of_date": int(as_of_date),
                "image_accuracy": image_accuracy,
                "numeric_accuracy": numeric_accuracy,
                "winner": winner,
            }
        )
    return wins


def _write_markdown_report(path: Path, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return path


def _build_confusion_by_period(test_frame: pd.DataFrame) -> dict[str, Any]:
    periods: list[dict[str, Any]] = []
    for as_of_date, month_frame in test_frame.groupby("as_of_date", sort=True):
        periods.append(
            {
                "as_of_date": int(as_of_date),
                "sample_count": int(len(month_frame)),
                "control_accuracy": float((month_frame["label_id"] == month_frame["control_pred_label"]).mean()),
                "image_accuracy": float((month_frame["label_id"] == month_frame["image_pred_label"]).mean()),
                "numeric_accuracy": float((month_frame["label_id"] == month_frame["numeric_pred_label"]).mean()),
            }
        )
    return {"schema_version": BASELINE_METRICS_SCHEMA_VERSION, "periods": periods}


def _build_score_distribution(test_frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "schema_version": BASELINE_METRICS_SCHEMA_VERSION,
        "control_pred_prob_up": _series_stats(test_frame["control_pred_prob_up"].astype(float).to_numpy(dtype=np.float64, copy=False)),
        "image_pred_prob_up": _series_stats(test_frame["image_pred_prob_up"].astype(float).to_numpy(dtype=np.float64, copy=False)),
        "numeric_pred_prob_up": _series_stats(test_frame["numeric_pred_prob_up"].astype(float).to_numpy(dtype=np.float64, copy=False)),
    }


def _augment_dataset_diagnostic(dataset_dir: Path, predictions_frame: pd.DataFrame) -> dict[str, Any]:
    path = dataset_dir / "dataset_diagnostic.json"
    payload = read_json(path)
    test_frame = predictions_frame.loc[predictions_frame["split"] == "test"].copy()
    weak_periods: list[dict[str, Any]] = []
    for as_of_date, month_frame in test_frame.groupby("as_of_date", sort=True):
        weak_periods.append(
            {
                "as_of_date": int(as_of_date),
                "control_accuracy": float((month_frame["label_id"] == month_frame["control_pred_label"]).mean()),
                "image_accuracy": float((month_frame["label_id"] == month_frame["image_pred_label"]).mean()),
                "numeric_accuracy": float((month_frame["label_id"] == month_frame["numeric_pred_label"]).mean()),
                "sample_count": int(len(month_frame)),
            }
        )
    weak_periods.sort(key=lambda item: (item["image_accuracy"], item["numeric_accuracy"], item["as_of_date"]))
    hard_false_positives = (
        test_frame.loc[(test_frame["label_id"] == 0) & (test_frame["image_pred_label"] == 1)]
        .sort_values("image_pred_prob_up", ascending=False)
        .head(10)
    )
    hard_false_negatives = (
        test_frame.loc[(test_frame["label_id"] == 1) & (test_frame["image_pred_label"] == 0)]
        .sort_values("image_pred_prob_up", ascending=True)
        .head(10)
    )
    payload["weak_periods"] = weak_periods[:12]
    payload["hard_false_positives"] = hard_false_positives[["sample_id", "as_of_date", "code", "fidelity_image_path", "forward_return_1m", "image_pred_prob_up"]].to_dict(orient="records")
    payload["hard_false_negatives"] = hard_false_negatives[["sample_id", "as_of_date", "code", "fidelity_image_path", "forward_return_1m", "image_pred_prob_up"]].to_dict(orient="records")
    _write_json_artifact(path, payload)
    return payload


def train_event_image_dataset(
    *,
    dataset_id: str,
    seed: int = 42,
    feature_size: int = FIDELITY_IMAGE_FEATURE_SIZE,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    training_started_at = _utc_now_iso()
    dataset_dir = event_image_dataset_dir(dataset_id)
    target_dir = Path(output_root).expanduser().resolve() if output_root is not None else dataset_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    samples_frame = read_parquet_frame(dataset_dir / "samples.parquet").sort_values(["as_of_date", "label_id", "rank_in_month", "code"]).reset_index(drop=True)
    if samples_frame.empty:
        raise RuntimeError("samples.parquet is empty")

    common_frame = samples_frame.loc[samples_frame["control_available"] & samples_frame["fidelity_available"]].copy()
    if common_frame.empty:
        raise RuntimeError("no common eligible samples available for fidelity comparison")
    train_frame = common_frame.loc[common_frame["split"] == "train"].copy()
    validation_frame = common_frame.loc[common_frame["split"] == "validation"].copy()
    test_frame = common_frame.loc[common_frame["split"] == "test"].copy()
    if train_frame.empty or test_frame.empty:
        raise RuntimeError("train/test split is empty on common eligible subset")

    y_train = train_frame["label_id"].astype(int).to_numpy(dtype=np.int32, copy=False)
    X_control_train = _build_image_features(train_frame, column="control_image_path", feature_size=CONTROL_IMAGE_FEATURE_SIZE)
    X_control_all = _build_image_features(common_frame, column="control_image_path", feature_size=CONTROL_IMAGE_FEATURE_SIZE)
    X_image_train = _build_image_features(train_frame, column="fidelity_image_path", feature_size=feature_size)
    X_image_all = _build_image_features(common_frame, column="fidelity_image_path", feature_size=feature_size)
    X_numeric_train = _build_numeric_features(train_frame)
    X_numeric_all = _build_numeric_features(common_frame)

    control_model = _train_classifier(X_train=X_control_train, y_train=y_train, seed=seed)
    image_model = _train_classifier(X_train=X_image_train, y_train=y_train, seed=seed)
    numeric_model = _train_classifier(X_train=X_numeric_train, y_train=y_train, seed=seed)

    predictions = common_frame.copy()
    predictions["control_pred_prob_up"] = control_model.predict_proba(X_control_all)[:, 1].astype(np.float64)
    predictions["control_pred_label"] = (predictions["control_pred_prob_up"] >= 0.5).astype(int)
    predictions["image_pred_prob_up"] = image_model.predict_proba(X_image_all)[:, 1].astype(np.float64)
    predictions["image_pred_label"] = (predictions["image_pred_prob_up"] >= 0.5).astype(int)
    predictions["numeric_pred_prob_up"] = numeric_model.predict_proba(X_numeric_all)[:, 1].astype(np.float64)
    predictions["numeric_pred_label"] = (predictions["numeric_pred_prob_up"] >= 0.5).astype(int)
    predictions["control_correct"] = (predictions["label_id"].astype(int) == predictions["control_pred_label"].astype(int)).astype(int)
    predictions["image_correct"] = (predictions["label_id"].astype(int) == predictions["image_pred_label"].astype(int)).astype(int)
    predictions["numeric_correct"] = (predictions["label_id"].astype(int) == predictions["numeric_pred_label"].astype(int)).astype(int)

    test_predictions = predictions.loc[predictions["split"] == "test"].copy()
    test_full_frame = samples_frame.loc[samples_frame["split"] == "test"].copy()
    if test_full_frame.empty:
        raise RuntimeError("test split is empty")
    control_metrics = _evaluate_predictions(test_predictions, prob_column="control_pred_prob_up", pred_column="control_pred_label")
    image_metrics = _evaluate_predictions(test_predictions, prob_column="image_pred_prob_up", pred_column="image_pred_label")
    numeric_metrics = _evaluate_predictions(test_predictions, prob_column="numeric_pred_prob_up", pred_column="numeric_pred_label")

    baseline_metrics = {"schema_version": BASELINE_METRICS_SCHEMA_VERSION, "dataset_id": str(dataset_id), "model_family": "image_only", "evaluation_bundle_id": FIDELITY_EVALUATION_BUNDLE_ID, "featureizer_spec_id": FIDELITY_FEATUREIZER_SPEC_ID, "seed": int(seed), "feature_size": int(feature_size), **image_metrics}
    numeric_baseline_metrics = {"schema_version": BASELINE_METRICS_SCHEMA_VERSION, "dataset_id": str(dataset_id), "model_family": "numeric_only", "seed": int(seed), "numeric_feature_spec_id": NUMERIC_FEATURE_SPEC_ID, **numeric_metrics}
    fidelity_compare = {
        "schema_version": FIDELITY_COMPARE_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "slice_definition": {"start_month": dataset_manifest["start_month"], "end_month": dataset_manifest["end_month"]},
        "bundle_ids": {"v1_1_image": CONTROL_EVALUATION_BUNDLE_ID, "v1_2_image": FIDELITY_EVALUATION_BUNDLE_ID, "numeric": NUMERIC_FEATURE_SPEC_ID},
        "same_split": True,
        "same_labels": True,
        "same_horizon": True,
        "same_universe": True,
        "same_sample_keys": True,
        "formal_compare_scope": "common eligible subset on test split only",
        "sample_key_definition": "as_of_date + code + label",
        "common_eligible_sample_count": int(len(test_predictions)),
        "dropped_by_warmup_v1_2_count": int(len(test_full_frame) - len(test_predictions)),
        "full_common_eligible_sample_count": int(len(common_frame)),
        "full_dropped_by_warmup_v1_2_count": int(len(samples_frame) - len(common_frame)),
        "test_full_sample_count": int(len(test_full_frame)),
        "full_sample_count": int(len(samples_frame)),
        "v1_1_image_metrics": control_metrics,
        "v1_2_image_metrics": image_metrics,
        "numeric_baseline_metrics": numeric_metrics,
        "delta_vs_v1_1": _metric_deltas(control_metrics, image_metrics),
        "delta_vs_numeric": _metric_deltas(numeric_metrics, image_metrics),
    }
    train_eval_manifest = {
        "schema_version": TRAIN_EVAL_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "evaluation_bundle_id": FIDELITY_EVALUATION_BUNDLE_ID,
        "renderer_spec_id": dataset_manifest["renderer_spec_id"],
        "featureizer_spec_id": dataset_manifest["featureizer_spec_id"],
        "train_months": sorted({int(value) for value in train_frame["as_of_date"].tolist()}),
        "validation_months": sorted({int(value) for value in validation_frame["as_of_date"].tolist()}),
        "test_months": sorted({int(value) for value in test_frame["as_of_date"].tolist()}),
        "seed": int(seed),
        "image_model_type": "logistic_regression",
        "numeric_model_type": "logistic_regression",
        "image_feature_size": int(feature_size),
        "control_image_feature_size": CONTROL_IMAGE_FEATURE_SIZE,
        "image_spec_id": dataset_manifest["image_spec_id"],
        "numeric_feature_spec_id": NUMERIC_FEATURE_SPEC_ID,
        "class_weight_policy": "balanced",
        "split_policy": dataset_manifest["split_policy"],
        "training_started_at": training_started_at,
        "training_finished_at": _utc_now_iso(),
    }

    predictions_path = target_dir / "predictions.parquet"
    baseline_metrics_path = target_dir / "baseline_metrics.json"
    numeric_metrics_path = target_dir / "numeric_baseline_metrics.json"
    train_eval_manifest_path = target_dir / "train_eval_manifest.json"
    confusion_by_period_path = target_dir / "confusion_by_period.json"
    score_distribution_path = target_dir / "score_distribution.json"
    fidelity_compare_path = target_dir / "fidelity_compare.json"

    write_parquet_frame(predictions_path, predictions)
    _write_json_artifact(baseline_metrics_path, baseline_metrics)
    _write_json_artifact(numeric_metrics_path, numeric_baseline_metrics)
    _write_json_artifact(train_eval_manifest_path, train_eval_manifest)
    _write_json_artifact(confusion_by_period_path, _build_confusion_by_period(test_predictions))
    _write_json_artifact(score_distribution_path, _build_score_distribution(test_predictions))
    _write_json_artifact(fidelity_compare_path, fidelity_compare)
    if output_root is None:
        _augment_dataset_diagnostic(dataset_dir, predictions)
        dataset_manifest["artifact_paths"].update(
            {
                "train_eval_manifest": str(train_eval_manifest_path),
                "baseline_metrics": str(baseline_metrics_path),
                "numeric_baseline_metrics": str(numeric_metrics_path),
                "predictions": str(predictions_path),
                "confusion_by_period": str(confusion_by_period_path),
                "score_distribution": str(score_distribution_path),
                "fidelity_compare": str(fidelity_compare_path),
            }
        )
        _write_json_artifact(dataset_dir / "dataset_manifest.json", dataset_manifest)
    return {
        "dataset_id": str(dataset_id),
        "dataset_dir": str(dataset_dir),
        "output_dir": str(target_dir),
        "seed": int(seed),
        "baseline_metrics_path": str(baseline_metrics_path),
        "numeric_baseline_metrics_path": str(numeric_metrics_path),
        "predictions_path": str(predictions_path),
        "fidelity_compare_path": str(fidelity_compare_path),
        "train_eval_manifest_path": str(train_eval_manifest_path),
    }


def run_event_image_dataset_repro(
    *,
    dataset_id: str,
    seeds: list[int] | tuple[int, ...] | None = None,
    feature_size: int = FIDELITY_IMAGE_FEATURE_SIZE,
) -> dict[str, Any]:
    dataset_dir = event_image_dataset_dir(dataset_id)
    dataset_manifest = read_json(dataset_dir / "dataset_manifest.json")
    seed_list = [int(seed) for seed in (list(seeds) if seeds is not None else list(DEFAULT_REPRO_SEEDS))]
    if not seed_list:
        raise RuntimeError("seed list is empty")

    repro_dir = dataset_dir / "repro"
    seed_runs_dir = repro_dir / "seed_runs"
    seed_runs_dir.mkdir(parents=True, exist_ok=True)

    metric_names = (
        "accuracy",
        "balanced_accuracy",
        "roc_auc",
        "top_decile_precision_up",
        "monthly_top10_precision_up",
        "monthly_top10_mean_forward_return",
        "monthly_bottom10_mean_forward_return",
        "monthly_long_short_spread",
    )

    seed_results: list[dict[str, Any]] = []
    month_win_counter: dict[int, dict[str, int]] = {}
    image_beats_numeric_counts = {metric_name: 0 for metric_name in metric_names}

    for seed in seed_list:
        seed_output_dir = seed_runs_dir / f"seed_{int(seed)}"
        train_result = train_event_image_dataset(
            dataset_id=dataset_id,
            seed=int(seed),
            feature_size=feature_size,
            output_root=seed_output_dir,
        )
        image_metrics = read_json(Path(train_result["baseline_metrics_path"]))
        numeric_metrics = read_json(Path(train_result["numeric_baseline_metrics_path"]))
        compare_metrics = read_json(Path(train_result["fidelity_compare_path"]))
        predictions_frame = read_parquet_frame(Path(train_result["predictions_path"]))
        month_level_wins = _month_level_wins(predictions_frame)
        for item in month_level_wins:
            month_key = int(item["as_of_date"])
            bucket = month_win_counter.setdefault(month_key, {"image": 0, "numeric": 0, "tie": 0})
            bucket[str(item["winner"])] += 1
        for metric_name in metric_names:
            if _metric_beats(metric_name, image_metrics.get(metric_name), numeric_metrics.get(metric_name)):
                image_beats_numeric_counts[metric_name] += 1
        seed_results.append(
            {
                "seed": int(seed),
                "output_dir": str(seed_output_dir),
                "common_eligible_sample_count": int(compare_metrics["common_eligible_sample_count"]),
                "dropped_by_warmup_v1_2_count": int(compare_metrics["dropped_by_warmup_v1_2_count"]),
                "v1_1_image_metrics": compare_metrics["v1_1_image_metrics"],
                "v1_2_image_metrics": compare_metrics["v1_2_image_metrics"],
                "numeric_baseline_metrics": compare_metrics["numeric_baseline_metrics"],
                "delta_vs_v1_1": compare_metrics["delta_vs_v1_1"],
                "delta_vs_numeric": compare_metrics["delta_vs_numeric"],
                "month_level_wins": month_level_wins,
            }
        )

    image_metric_mean, image_metric_std = _mean_std([item["v1_2_image_metrics"] for item in seed_results], metric_names)
    numeric_metric_mean, numeric_metric_std = _mean_std([item["numeric_baseline_metrics"] for item in seed_results], metric_names)

    image_beats_numeric_month_counts = {
        str(month_key): {
            "image": int(result["image"]),
            "numeric": int(result["numeric"]),
            "tie": int(result["tie"]),
        }
        for month_key, result in sorted(month_win_counter.items())
    }

    keep_strengthened_seed_count = 0
    for item in seed_results:
        beats = 0
        for metric_name in ("balanced_accuracy", "roc_auc", "monthly_long_short_spread"):
            if _metric_beats(metric_name, item["v1_2_image_metrics"].get(metric_name), item["numeric_baseline_metrics"].get(metric_name)):
                beats += 1
        if beats >= 2:
            keep_strengthened_seed_count += 1

    v12_not_above_v11_count = 0
    v12_not_above_numeric_count = 0
    long_short_negative_count = 0
    for item in seed_results:
        if not (
            float(item["v1_2_image_metrics"]["balanced_accuracy"]) > float(item["v1_1_image_metrics"]["balanced_accuracy"])
            and float(item["v1_2_image_metrics"]["roc_auc"]) > float(item["v1_1_image_metrics"]["roc_auc"])
        ):
            v12_not_above_v11_count += 1
        if not (
            float(item["v1_2_image_metrics"]["balanced_accuracy"]) > float(item["numeric_baseline_metrics"]["balanced_accuracy"])
            and float(item["v1_2_image_metrics"]["roc_auc"]) > float(item["numeric_baseline_metrics"]["roc_auc"])
        ):
            v12_not_above_numeric_count += 1
        if float(item["v1_2_image_metrics"]["monthly_long_short_spread"]) <= 0.0:
            long_short_negative_count += 1

    if (
        keep_strengthened_seed_count >= 4
        and image_metric_mean["monthly_long_short_spread"] is not None
        and float(image_metric_mean["monthly_long_short_spread"]) > 0.0
        and _metric_beats("roc_auc", image_metric_mean["roc_auc"], numeric_metric_mean["roc_auc"])
    ):
        disposition_recommendation = "keep_strengthened"
        next_single_step = "同 spec の restricted-universe 比較"
    elif v12_not_above_numeric_count >= 3 or v12_not_above_v11_count >= 3:
        disposition_recommendation = "drop_reconsider"
        next_single_step = "numeric 主体方針へ戻す"
    else:
        disposition_recommendation = "hold"
        next_single_step = "warmup 451 件の影響分解"

    stability_flags = {
        "seed_count": int(len(seed_results)),
        "keep_strengthened_seed_count": int(keep_strengthened_seed_count),
        "v12_not_above_v11_count": int(v12_not_above_v11_count),
        "v12_not_above_numeric_count": int(v12_not_above_numeric_count),
        "long_short_non_positive_seed_count": int(long_short_negative_count),
        "month_level_wins": image_beats_numeric_month_counts,
    }

    repro_manifest_path = repro_dir / "repro_manifest.json"
    repro_summary_path = repro_dir / "repro_summary.json"
    repro_report_path = repro_dir / "repro_report.md"

    common_eligible_counts = sorted({int(item["common_eligible_sample_count"]) for item in seed_results})
    dropped_counts = sorted({int(item["dropped_by_warmup_v1_2_count"]) for item in seed_results})
    repro_manifest = {
        "schema_version": REPRO_MANIFEST_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "evaluation_bundle_id": str(dataset_manifest["evaluation_bundle_id"]),
        "renderer_spec_id": str(dataset_manifest["renderer_spec_id"]),
        "featureizer_spec_id": str(dataset_manifest["featureizer_spec_id"]),
        "numeric_feature_spec_id": NUMERIC_FEATURE_SPEC_ID,
        "seed_list": seed_list,
        "same_dataset": True,
        "same_split": True,
        "same_labels": True,
        "same_horizon": True,
        "same_universe": True,
        "same_sample_keys": True,
        "formal_compare_scope": "common eligible subset on test split only",
        "common_eligible_sample_count": int(common_eligible_counts[0]),
        "dropped_by_warmup_v1_2_count": int(dropped_counts[0]),
        "artifact_paths": {
            "repro_summary": str(repro_summary_path),
            "repro_report": str(repro_report_path),
            "seed_runs_root": str(seed_runs_dir),
        },
    }
    repro_summary = {
        "schema_version": REPRO_SUMMARY_SCHEMA_VERSION,
        "dataset_id": str(dataset_id),
        "seed_results": seed_results,
        "image_metric_mean": image_metric_mean,
        "image_metric_std": image_metric_std,
        "numeric_metric_mean": numeric_metric_mean,
        "numeric_metric_std": numeric_metric_std,
        "image_beats_numeric_counts": image_beats_numeric_counts,
        "image_beats_numeric_month_counts": image_beats_numeric_month_counts,
        "stability_flags": stability_flags,
        "disposition_recommendation": disposition_recommendation,
    }

    report_lines = [
        "# TRADEX Event Image Dataset v1.2 Reproducibility Report",
        "",
        "## Summary",
        f"- dataset: `{dataset_id}`",
        f"- seeds: `{seed_list}`",
        f"- disposition recommendation: `{disposition_recommendation}`",
        "",
        "## Current State",
        f"- evaluation bundle: `{dataset_manifest['evaluation_bundle_id']}`",
        f"- renderer spec: `{dataset_manifest['renderer_spec_id']}`",
        f"- featureizer spec: `{dataset_manifest['featureizer_spec_id']}`",
        f"- common eligible sample count: `{common_eligible_counts[0]}`",
        f"- dropped by warmup v1.2 count: `{dropped_counts[0]}`",
        "",
        "## Seed Setup",
        f"- seeds: `{seed_list}`",
        f"- feature size: `{feature_size}`",
        "",
        "## Per-Seed Comparison",
    ]
    for item in seed_results:
        report_lines.extend(
            [
                f"- seed `{item['seed']}`: image ba `{item['v1_2_image_metrics']['balanced_accuracy']:.4f}`, numeric ba `{item['numeric_baseline_metrics']['balanced_accuracy']:.4f}`, image auc `{item['v1_2_image_metrics']['roc_auc']:.4f}`, numeric auc `{item['numeric_baseline_metrics']['roc_auc']:.4f}`, image ls `{item['v1_2_image_metrics']['monthly_long_short_spread']:.4f}`, numeric ls `{item['numeric_baseline_metrics']['monthly_long_short_spread']:.4f}`",
            ]
        )
    report_lines.extend(
        [
            "",
            "## Mean / Std Comparison",
            f"- image mean: `{image_metric_mean}`",
            f"- image std: `{image_metric_std}`",
            f"- numeric mean: `{numeric_metric_mean}`",
            f"- numeric std: `{numeric_metric_std}`",
            "",
            "## Month-Level Stability",
            f"- month-level win counts: `{image_beats_numeric_month_counts}`",
            "",
            "## Disposition Recommendation",
            f"- `{disposition_recommendation}`",
            "",
            "## Next Single Step",
            f"- {next_single_step}",
        ]
    )

    _write_json_artifact(repro_manifest_path, repro_manifest)
    _write_json_artifact(repro_summary_path, repro_summary)
    _write_markdown_report(repro_report_path, report_lines)

    return {
        "dataset_id": str(dataset_id),
        "repro_dir": str(repro_dir),
        "repro_manifest_path": str(repro_manifest_path),
        "repro_summary_path": str(repro_summary_path),
        "repro_report_path": str(repro_report_path),
        "disposition_recommendation": disposition_recommendation,
    }
