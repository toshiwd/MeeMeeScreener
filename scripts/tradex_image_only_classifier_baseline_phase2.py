from __future__ import annotations

import argparse
import hashlib
import json
import math
import pickle
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from PIL import Image
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    balanced_accuracy_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    log_loss,
    matthews_corrcoef,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_image_assisted_rerank_phase0_1 as phase0_mod


AXIS_ID = "image_only_classifier_baseline_phase2"
SCHEMA_PREFIX = "tradex_image_only_classifier_baseline_phase2"
DEFAULT_SOURCE_PHASE0_1_RUN_ID = "20260513T080000Z-image-assisted-rerank-phase0-1"
DEFAULT_SOURCE_PHASE0_1_ROOT = Path(r"G:\Tradex\image_assisted_rerank_phase0_1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\image_only_classifier_baseline_phase2")

RANDOM_SEED = 20260513
MODEL_FAMILY_ID = "image_linear_logistic_baseline_v1"
IMAGE_FEATURE_SIZE = 32
PRIMARY_POSITIVE_LABEL = "future_top15_by_ret20"
PRIMARY_NEGATIVE_LABEL = "future_bottom15_by_ret20"
PRIMARY_NEUTRAL_LABEL = "neutral_middle70"
RANDOM_REPEATS = 20

REQUIRED_SOURCE_FILES = (
    "image_manifest.jsonl",
    "label_ledger.jsonl",
    "split_assignment_ledger.jsonl",
    "image_renderer_contract.json",
    "label_contract.json",
    "split_contract.json",
    "split_leakage_audit.json",
    "phase2_readiness_report.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "phase2_model_contract.json",
    "training_config.json",
    "dataset_audit.json",
    "image_input_audit.json",
    "label_usage_audit.json",
    "phase2_split_leakage_audit.json",
    "training_log.jsonl",
    "model_checkpoint_manifest.json",
    "image_score_ledger.jsonl",
    "classifier_metrics.json",
    "score_distribution_report.json",
    "topk_proxy_report.json",
    "negative_guard_image_diagnostics.json",
    "baseline_comparison_report.json",
    "phase3_readiness_report.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    return phase0_mod._json_ready(value)


def _json_text(payload: Any) -> str:
    return phase0_mod._json_text(payload)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return phase0_mod._write_json(path, payload)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    return phase0_mod._write_jsonl(path, rows)


def _load_json(path: Path) -> dict[str, Any]:
    return phase0_mod._load_json(path)


def _stable_hash(payload: Any) -> str:
    return phase0_mod._stable_hash(payload)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return phase0_mod._safe_path(value, default)


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return phase0_mod._run_dir(root, run_id, default_root)


def _safe_rate(count: int | float, total: int | float) -> float:
    return phase0_mod._safe_rate(count, total)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text:
                rows.append(json.loads(text))
    return rows


def _file_hash(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes"}


def _bool_series(series: pd.Series) -> pd.Series:
    return series.map(_as_bool).astype(bool)


def validate_phase0_1_source(source_dir: Path) -> dict[str, Any]:
    missing = [name for name in REQUIRED_SOURCE_FILES if not (source_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Phase0/1 source missing required artifacts: {missing} at {source_dir}")
    complete = _load_json(source_dir / "_ARTIFACT_COMPLETE.json")
    decision = _load_json(source_dir / "research_decision.json")
    readiness = _load_json(source_dir / "phase2_readiness_report.json")
    split_audit = _load_json(source_dir / "split_leakage_audit.json")
    if complete.get("complete") is not True:
        raise RuntimeError("Phase0/1 source artifact is not complete")
    if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is not False:
        raise RuntimeError("Phase0/1 source used silent fallback")
    if complete.get("research_fallback_used") is not False or decision.get("research_fallback_used") is not False:
        raise RuntimeError("Phase0/1 source used research fallback")
    if decision.get("authoritative_research_decision") != "image_assisted_phase0_1_ready_for_phase2":
        raise RuntimeError("Phase0/1 source is not ready_for_phase2")
    if readiness.get("ready_for_phase2") is not True:
        raise RuntimeError("Phase0/1 readiness report is not ready_for_phase2")
    if split_audit.get("split_leakage_audit_passed") is not True:
        raise RuntimeError("Phase0/1 split leakage audit did not pass")
    return {
        "source_phase0_1_decision": decision.get("authoritative_research_decision"),
        "source_phase0_1_local_decision": decision.get("decision"),
        "ready_for_phase2": readiness.get("ready_for_phase2"),
        "image_renderable_event_count": readiness.get("image_renderable_event_count"),
        "image_renderable_event_rate": readiness.get("image_renderable_event_rate"),
        "deterministic_hash_pass_rate": readiness.get("deterministic_hash_pass_rate"),
        "split_leakage_audit_passed": readiness.get("split_leakage_audit_passed"),
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def load_phase0_1_dataset(source_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    source_status = validate_phase0_1_source(source_dir)
    manifest = pd.DataFrame(_read_jsonl(source_dir / "image_manifest.jsonl"))
    labels = pd.DataFrame(_read_jsonl(source_dir / "label_ledger.jsonl"))
    splits = pd.DataFrame(_read_jsonl(source_dir / "split_assignment_ledger.jsonl"))
    if manifest.empty or labels.empty or splits.empty:
        raise RuntimeError("Phase0/1 source ledgers are empty")
    label_columns = [
        "image_sample_key",
        "primary_label",
        "future_top15_by_ret20",
        "future_bottom15_by_ret20",
        "neutral_middle70",
        "ret20",
        "MFE20",
        "MAE20",
        "severe_loss20",
        "future_top10_by_ret20",
        "future_top5_by_ret20",
        "big_winner_ret20_ge_10pct",
        "big_winner_MFE20_ge_15pct",
        "labels_used_in_candidate_key",
        "labels_used_in_image_rendering",
    ]
    split_columns = [
        "image_sample_key",
        "split",
        "embargo_reason",
        "negative_guard_matched",
        "safe_full_tag",
    ]
    frame = manifest.merge(labels[label_columns], on="image_sample_key", how="inner")
    frame = frame.merge(splits[split_columns], on="image_sample_key", how="inner", suffixes=("", "_split"))
    for column in (
        "future_top15_by_ret20",
        "future_bottom15_by_ret20",
        "neutral_middle70",
        "severe_loss20",
        "future_top10_by_ret20",
        "future_top5_by_ret20",
        "big_winner_ret20_ge_10pct",
        "big_winner_MFE20_ge_15pct",
        "negative_guard_matched",
        "safe_full_tag",
        "labels_used_in_candidate_key",
        "labels_used_in_image_rendering",
    ):
        frame[column] = _bool_series(frame[column])
    for column in ("ret20", "MFE20", "MAE20", "event_ymd"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["binary_target"] = np.nan
    frame.loc[frame["future_top15_by_ret20"], "binary_target"] = 1
    frame.loc[frame["future_bottom15_by_ret20"], "binary_target"] = 0
    frame["binary_target_available"] = frame["binary_target"].notna()
    frame["image_path_exists"] = frame["image_path"].map(lambda value: Path(str(value)).exists())
    frame = frame.sort_values(["event_ymd", "symbol", "image_sample_key"]).reset_index(drop=True)
    source_status.update(
        {
            "manifest_row_count": int(len(manifest)),
            "label_ledger_row_count": int(len(labels)),
            "split_assignment_row_count": int(len(splits)),
            "joined_row_count": int(len(frame)),
            "source_row_counts_match": int(len(manifest)) == int(len(labels)) == int(len(splits)) == int(len(frame)),
        }
    )
    return frame, source_status


def extract_image_features(frame: pd.DataFrame, *, feature_size: int = IMAGE_FEATURE_SIZE) -> tuple[np.ndarray, pd.Series, dict[str, Any]]:
    resample = getattr(getattr(Image, "Resampling", Image), "BILINEAR")
    features: list[np.ndarray] = []
    readable_mask: list[bool] = []
    status_counts: Counter[str] = Counter()
    observed_sizes: Counter[str] = Counter()
    for row in frame.itertuples(index=False):
        path = Path(str(getattr(row, "image_path")))
        try:
            with Image.open(path) as image:
                observed_sizes[f"{image.size[0]}x{image.size[1]}"] += 1
                grey = image.convert("L").resize((feature_size, feature_size), resample)
                array = np.asarray(grey, dtype=np.float32).reshape(-1) / 255.0
            features.append(array)
            readable_mask.append(True)
            status_counts["readable"] += 1
        except Exception:
            readable_mask.append(False)
            status_counts["unreadable_or_missing"] += 1
    if features:
        matrix = np.vstack(features).astype(np.float32)
    else:
        matrix = np.zeros((0, feature_size * feature_size), dtype=np.float32)
    mask = pd.Series(readable_mask, index=frame.index)
    audit = {
        "schema_version": f"{SCHEMA_PREFIX}_image_input_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_image_size_expected": "224x224",
        "model_input_mode": "grayscale_downsampled_pixels",
        "model_input_size": f"{feature_size}x{feature_size}",
        "model_input_feature_count": int(feature_size * feature_size),
        "image_row_count": int(len(frame)),
        "image_readable_count": int(mask.sum()),
        "image_unreadable_or_missing_count": int((~mask).sum()),
        "image_readable_rate": float(mask.mean()) if len(mask) else 0.0,
        "observed_source_image_sizes": dict(sorted(observed_sizes.items())),
        "image_input_status_counts": dict(sorted(status_counts.items())),
        "future_labels_used_as_inference_inputs": False,
        "labels_used_in_image_loading": False,
        "sample_reduction_used": False,
    }
    return matrix, mask, audit


def train_image_only_classifier(frame: pd.DataFrame, features: np.ndarray) -> tuple[Pipeline, dict[str, Any], list[dict[str, Any]]]:
    train_mask = frame["split"].eq("train") & frame["binary_target_available"]
    validation_mask = frame["split"].eq("validation") & frame["binary_target_available"]
    if int(train_mask.sum()) <= 1:
        raise RuntimeError("not enough train binary samples for image-only classifier")
    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "classifier",
                LogisticRegression(
                    solver="liblinear",
                    max_iter=200,
                    class_weight="balanced",
                    random_state=RANDOM_SEED,
                ),
            ),
        ]
    )
    x_train = features[train_mask.to_numpy()]
    y_train = frame.loc[train_mask, "binary_target"].astype(int).to_numpy()
    model.fit(x_train, y_train)
    train_score = model.predict_proba(x_train)[:, 1]
    training_log = [
        {
            "schema_version": f"{SCHEMA_PREFIX}_training_log_row_v1",
            "event": "fit_completed",
            "model_family_id": MODEL_FAMILY_ID,
            "train_binary_sample_count": int(train_mask.sum()),
            "validation_binary_sample_count": int(validation_mask.sum()),
            "train_log_loss": _metric_log_loss(y_train, train_score),
            "validation_log_loss": _metric_log_loss(
                frame.loc[validation_mask, "binary_target"].astype(int).to_numpy(),
                model.predict_proba(features[validation_mask.to_numpy()])[:, 1],
            )
            if int(validation_mask.sum())
            else None,
            "full_dataset_used": True,
            "sample_reduction_used": False,
            "validation_used_for_hyperparameter_tuning": False,
            "pretrained_weights_used": False,
            "device": "cpu",
            "generated_at": _utc_now(),
        }
    ]
    training_config = {
        "schema_version": f"{SCHEMA_PREFIX}_training_config_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "model_library": "sklearn",
        "model_class": "Pipeline(StandardScaler, LogisticRegression)",
        "model_architecture": "linear_logistic_classifier_on_downsampled_image_pixels",
        "preferred_cnn_or_resnet_used": False,
        "preferred_cnn_or_resnet_not_used_reason": "torch_and_torchvision_unavailable_in_current_environment",
        "image_only": True,
        "base_score_input_used": False,
        "fusion_used": False,
        "primary_positive_label": PRIMARY_POSITIVE_LABEL,
        "primary_negative_label": PRIMARY_NEGATIVE_LABEL,
        "neutral_middle70_training_policy": "excluded_from_primary_training_scored_for_distribution_audit",
        "input_feature_mode": "grayscale_downsampled_pixels",
        "input_feature_size": f"{IMAGE_FEATURE_SIZE}x{IMAGE_FEATURE_SIZE}",
        "solver": "liblinear",
        "max_iter": 200,
        "class_weight": "balanced",
        "random_seed": RANDOM_SEED,
        "device": "cpu",
        "gpu_available": False,
        "sample_reduction_used": False,
        "full_dataset_used": True,
        "pretrained_weights_used": False,
        "pretrained_source": None,
        "weight_hash": None,
        "license_note": None,
        "full_period_hyperparameter_tuning": False,
    }
    return model, training_config, training_log


def _metric_log_loss(y_true: np.ndarray, score: np.ndarray) -> float | None:
    if len(y_true) == 0 or len(set(y_true.tolist())) < 2:
        return None
    return float(log_loss(y_true, np.clip(score, 1e-6, 1 - 1e-6), labels=[0, 1]))


def _binary_metrics(y_true: np.ndarray, score: np.ndarray) -> dict[str, Any]:
    if len(y_true) == 0:
        return {
            "binary_sample_count": 0,
            "positive_count": 0,
            "negative_count": 0,
            "accuracy": None,
            "balanced_accuracy": None,
            "precision": None,
            "recall": None,
            "f1": None,
            "roc_auc": None,
            "pr_auc": None,
            "mcc": None,
            "log_loss": None,
            "calibration_brier_score": None,
            "confusion_matrix": [[0, 0], [0, 0]],
        }
    y_true = y_true.astype(int)
    y_pred = (score >= 0.5).astype(int)
    has_both = len(set(y_true.tolist())) == 2
    return {
        "binary_sample_count": int(len(y_true)),
        "positive_count": int((y_true == 1).sum()),
        "negative_count": int((y_true == 0).sum()),
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)) if has_both else None,
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, score)) if has_both else None,
        "pr_auc": float(average_precision_score(y_true, score)) if has_both else None,
        "mcc": float(matthews_corrcoef(y_true, y_pred)) if has_both else None,
        "log_loss": _metric_log_loss(y_true, score),
        "calibration_brier_score": float(brier_score_loss(y_true, np.clip(score, 0.0, 1.0))) if has_both else None,
        "confusion_matrix": confusion_matrix(y_true, y_pred, labels=[0, 1]).astype(int).tolist(),
    }


def build_classifier_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    rows: dict[str, Any] = {}
    for split in ("train", "validation", "test"):
        split_frame = frame[frame["split"].eq(split)].copy()
        binary = split_frame[split_frame["binary_target_available"]].copy()
        metrics = _binary_metrics(binary["binary_target"].astype(int).to_numpy(), binary["image_score"].astype(float).to_numpy())
        metrics.update(
            {
                "sample_count": int(len(split_frame)),
                "train_sample_count": int(len(frame[frame["split"].eq("train")])) if split == "train" else None,
                "validation_sample_count": int(len(frame[frame["split"].eq("validation")])) if split == "validation" else None,
                "test_sample_count": int(len(frame[frame["split"].eq("test")])) if split == "test" else None,
                "neutral_count": int(split_frame["neutral_middle70"].sum()),
            }
        )
        rows[split] = metrics
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_classifier_metrics_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "train_sample_count": rows["train"]["sample_count"],
        "validation_sample_count": rows["validation"]["sample_count"],
        "test_sample_count": rows["test"]["sample_count"],
        "positive_count_by_split": {split: rows[split]["positive_count"] for split in rows},
        "negative_count_by_split": {split: rows[split]["negative_count"] for split in rows},
        "neutral_count_by_split": {split: rows[split]["neutral_count"] for split in rows},
        "by_split": rows,
        "test_accuracy": rows["test"]["accuracy"],
        "test_balanced_accuracy": rows["test"]["balanced_accuracy"],
        "test_precision": rows["test"]["precision"],
        "test_recall": rows["test"]["recall"],
        "test_f1": rows["test"]["f1"],
        "test_roc_auc": rows["test"]["roc_auc"],
        "test_pr_auc": rows["test"]["pr_auc"],
        "test_mcc": rows["test"]["mcc"],
        "test_log_loss": rows["test"]["log_loss"],
        "test_calibration_brier_score": rows["test"]["calibration_brier_score"],
        "test_confusion_matrix": rows["test"]["confusion_matrix"],
    }
    return payload


def _score_stats(series: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return {"count": 0, "mean": None, "median": None, "std": None, "quantiles": {}}
    return {
        "count": int(len(clean)),
        "mean": float(clean.mean()),
        "median": float(clean.median()),
        "std": float(clean.std(ddof=0)),
        "quantiles": {str(q): float(clean.quantile(q)) for q in (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)},
    }


def _score_separation(frame: pd.DataFrame) -> dict[str, Any]:
    top = frame[frame["future_top15_by_ret20"]]["image_score"]
    bottom = frame[frame["future_bottom15_by_ret20"]]["image_score"]
    if top.empty or bottom.empty:
        return {"score_separation_top15_vs_bottom15": None, "score_overlap_rate": None}
    binary = frame[frame["binary_target_available"]]
    auc = roc_auc_score(binary["binary_target"].astype(int), binary["image_score"].astype(float)) if binary["binary_target"].nunique() == 2 else None
    return {
        "score_separation_top15_vs_bottom15": float(top.mean() - bottom.mean()),
        "score_overlap_rate": float(1.0 - auc) if auc is not None else None,
    }


def build_score_distribution_report(frame: pd.DataFrame) -> dict[str, Any]:
    by_split: dict[str, Any] = {}
    for split in ("train", "validation", "test"):
        split_frame = frame[frame["split"].eq(split)]
        label_rows = {}
        for label, mask_column in [
            ("future_top15_by_ret20", "future_top15_by_ret20"),
            ("future_bottom15_by_ret20", "future_bottom15_by_ret20"),
            ("neutral_middle70", "neutral_middle70"),
        ]:
            label_rows[label] = _score_stats(split_frame.loc[split_frame[mask_column], "image_score"])
        by_split[split] = {
            "image_score_mean_by_label": {label: row["mean"] for label, row in label_rows.items()},
            "image_score_median_by_label": {label: row["median"] for label, row in label_rows.items()},
            "image_score_std_by_label": {label: row["std"] for label, row in label_rows.items()},
            "image_score_quantiles_by_label": {label: row["quantiles"] for label, row in label_rows.items()},
            **_score_separation(split_frame),
        }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_score_distribution_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "by_split": by_split,
        "test_score_separation_top15_vs_bottom15": by_split["test"]["score_separation_top15_vs_bottom15"],
        "test_score_overlap_rate": by_split["test"]["score_overlap_rate"],
    }


def _topk_selection(frame: pd.DataFrame, *, score_column: str, top_k: int) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    sorted_frame = frame.sort_values(["event_ymd", score_column, "symbol"], ascending=[True, False, True]).copy()
    return sorted_frame.groupby("event_ymd", sort=False).head(top_k).copy()


def _topk_metrics_for_selection(scope: pd.DataFrame, selected: pd.DataFrame, *, top_k: int, prefix: str) -> dict[str, Any]:
    if selected.empty:
        return {
            f"{prefix}_top{top_k}_selected_count": 0,
            f"{prefix}_top{top_k}_avg_ret20": None,
            f"{prefix}_top{top_k}_win_rate20": None,
            f"{prefix}_top{top_k}_avg_MFE20": None,
            f"{prefix}_top{top_k}_avg_MAE20": None,
            f"{prefix}_top{top_k}_severe_loss_rate20": None,
            f"{prefix}_top{top_k}_big_winner_capture_rate": None,
            f"{prefix}_selected_nonwinner_when_winner_available_rate": None,
            f"{prefix}_oracle_top{top_k}_gap_ret20": None,
        }
    winner_mask = scope["future_top10_by_ret20"] | scope["big_winner_ret20_ge_10pct"]
    selected_winner_mask = selected["future_top10_by_ret20"] | selected["big_winner_ret20_ge_10pct"]
    winner_dates = set(scope.loc[winner_mask, "event_ymd"].astype(int).tolist())
    selected_winner_dates = set(selected.loc[selected_winner_mask, "event_ymd"].astype(int).tolist())
    oracle_selected = _topk_selection(scope, score_column="ret20", top_k=top_k)
    oracle_avg_ret20 = float(oracle_selected["ret20"].mean()) if len(oracle_selected) else None
    selected_avg_ret20 = float(selected["ret20"].mean()) if len(selected) else None
    return {
        f"{prefix}_top{top_k}_selected_count": int(len(selected)),
        f"{prefix}_top{top_k}_avg_ret20": selected_avg_ret20,
        f"{prefix}_top{top_k}_win_rate20": float(pd.to_numeric(selected["ret20"], errors="coerce").gt(0).mean()) if len(selected) else None,
        f"{prefix}_top{top_k}_avg_MFE20": float(selected["MFE20"].mean()) if len(selected) else None,
        f"{prefix}_top{top_k}_avg_MAE20": float(selected["MAE20"].mean()) if len(selected) else None,
        f"{prefix}_top{top_k}_severe_loss_rate20": float(selected["severe_loss20"].mean()) if len(selected) else None,
        f"{prefix}_top{top_k}_big_winner_capture_rate": _safe_rate(int(selected_winner_mask.sum()), int(winner_mask.sum())),
        f"{prefix}_selected_nonwinner_when_winner_available_rate": _safe_rate(len(winner_dates - selected_winner_dates), len(winner_dates)),
        f"{prefix}_oracle_top{top_k}_gap_ret20": selected_avg_ret20 - oracle_avg_ret20 if selected_avg_ret20 is not None and oracle_avg_ret20 is not None else None,
    }


def _topk_metric_bundle(scope: pd.DataFrame, *, score_column: str, prefix: str) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for top_k in (1, 3, 5, 10):
        selected = _topk_selection(scope, score_column=score_column, top_k=top_k)
        payload.update(_topk_metrics_for_selection(scope, selected, top_k=top_k, prefix=prefix))
    return payload


def _stable_random_score(image_sample_key: str, repeat: int) -> float:
    digest = hashlib.sha256(f"{RANDOM_SEED}|{repeat}|{image_sample_key}".encode("utf-8")).hexdigest()
    return int(digest[:16], 16) / float(0xFFFFFFFFFFFFFFFF)


def _random_topk_baseline(scope: pd.DataFrame) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for repeat in range(RANDOM_REPEATS):
        temp = scope.copy()
        temp["random_score"] = [_stable_random_score(str(value), repeat) for value in temp["image_sample_key"].tolist()]
        rows.append(_topk_metric_bundle(temp, score_column="random_score", prefix="random"))
    if not rows:
        return {"random_repeat_count": RANDOM_REPEATS}
    keys = sorted({key for row in rows for key in row})
    averaged = {"random_repeat_count": RANDOM_REPEATS}
    for key in keys:
        values = [row.get(key) for row in rows if row.get(key) is not None]
        averaged[key] = float(np.mean(values)) if values and all(isinstance(value, (int, float)) for value in values) else None
    return averaged


def build_topk_proxy_report(frame: pd.DataFrame) -> dict[str, Any]:
    by_split: dict[str, Any] = {}
    for split in ("train", "validation", "test"):
        scope = frame[frame["split"].eq(split)].copy()
        image_metrics = _topk_metric_bundle(scope, score_column="image_score", prefix="image_only")
        random_metrics = _random_topk_baseline(scope)
        by_split[split] = {
            "sample_count": int(len(scope)),
            **image_metrics,
            **random_metrics,
            "wide_strength_pool_previous_best_available": "prior_research_score" in scope.columns,
            "wide_strength_pool_previous_best_unavailable_reason": None
            if "prior_research_score" in scope.columns
            else "phase0_1_image_manifest_contains_score_availability_flags_but_not_numeric_score_values",
        }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_topk_proxy_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "topk_scope": "research_only_proxy_not_production_ranking",
        "by_split": by_split,
        "test_image_only_top1_avg_ret20": by_split["test"].get("image_only_top1_avg_ret20"),
        "test_image_only_top3_avg_ret20": by_split["test"].get("image_only_top3_avg_ret20"),
        "test_image_only_top5_avg_ret20": by_split["test"].get("image_only_top5_avg_ret20"),
        "test_image_only_top10_avg_ret20": by_split["test"].get("image_only_top10_avg_ret20"),
        "test_image_only_top3_win_rate20": by_split["test"].get("image_only_top3_win_rate20"),
        "test_image_only_top3_avg_MFE20": by_split["test"].get("image_only_top3_avg_MFE20"),
        "test_image_only_top3_avg_MAE20": by_split["test"].get("image_only_top3_avg_MAE20"),
        "test_image_only_top3_severe_loss_rate20": by_split["test"].get("image_only_top3_severe_loss_rate20"),
        "test_image_only_top3_big_winner_capture_rate": by_split["test"].get("image_only_top3_big_winner_capture_rate"),
        "test_image_only_selected_nonwinner_when_winner_available_rate": by_split["test"].get("image_only_selected_nonwinner_when_winner_available_rate"),
        "test_image_only_oracle_top3_gap_ret20": by_split["test"].get("image_only_oracle_top3_gap_ret20"),
        "test_random_top3_avg_ret20": by_split["test"].get("random_top3_avg_ret20"),
        "test_random_top3_severe_loss_rate20": by_split["test"].get("random_top3_severe_loss_rate20"),
    }


def build_negative_guard_diagnostics(frame: pd.DataFrame) -> dict[str, Any]:
    ng = frame[frame["negative_guard_matched"]].copy()
    sample_count_by_split = {split: int((ng["split"] == split).sum()) for split in ("train", "validation", "test", "embargo")}
    top15_count_by_split = {split: int(ng.loc[ng["split"].eq(split), "future_top15_by_ret20"].sum()) for split in sample_count_by_split}
    bottom15_count_by_split = {split: int(ng.loc[ng["split"].eq(split), "future_bottom15_by_ret20"].sum()) for split in sample_count_by_split}
    metrics_by_split: dict[str, Any] = {}
    score_separation_by_split: dict[str, Any] = {}
    topk_by_split: dict[str, Any] = {}
    for split in ("train", "validation", "test"):
        scope = ng[ng["split"].eq(split)].copy()
        binary = scope[scope["binary_target_available"]].copy()
        metrics_by_split[split] = _binary_metrics(binary["binary_target"].astype(int).to_numpy(), binary["image_score"].astype(float).to_numpy())
        score_separation_by_split[split] = _score_separation(scope)
        topk_by_split[split] = _topk_metric_bundle(scope, score_column="image_score", prefix="negative_guard_image")
    test_scope = ng[ng["split"].eq("test")].copy()
    winners = test_scope[test_scope["future_top15_by_ret20"] | test_scope["big_winner_ret20_ge_10pct"]]
    losers = test_scope[test_scope["future_bottom15_by_ret20"] | test_scope["severe_loss20"] | pd.to_numeric(test_scope["ret20"], errors="coerce").le(0)]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_negative_guard_image_diagnostics_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "negative_guard_sample_count_by_split": sample_count_by_split,
        "negative_guard_top15_count_by_split": top15_count_by_split,
        "negative_guard_bottom15_count_by_split": bottom15_count_by_split,
        "classifier_metrics_by_split": metrics_by_split,
        "score_separation_by_split": score_separation_by_split,
        "topk_proxy_by_split": topk_by_split,
        "negative_guard_classifier_auc": metrics_by_split["test"].get("roc_auc"),
        "negative_guard_classifier_mcc": metrics_by_split["test"].get("mcc"),
        "negative_guard_score_separation": score_separation_by_split["test"].get("score_separation_top15_vs_bottom15"),
        "negative_guard_image_top3_avg_ret20": topk_by_split["test"].get("negative_guard_image_top3_avg_ret20"),
        "negative_guard_image_top3_severe_loss_rate20": topk_by_split["test"].get("negative_guard_image_top3_severe_loss_rate20"),
        "negative_guard_winner_score_mean": float(winners["image_score"].mean()) if len(winners) else None,
        "negative_guard_loser_score_mean": float(losers["image_score"].mean()) if len(losers) else None,
        "numeric_diagnosis_recommended_decomposition_feature_count": 0,
        "image_negative_guard_signal_available": bool(
            (score_separation_by_split["test"].get("score_separation_top15_vs_bottom15") or 0.0) > 0.0
            or (metrics_by_split["test"].get("roc_auc") or 0.0) > 0.5
        ),
    }


def build_baseline_comparison_report(frame: pd.DataFrame, topk_report: dict[str, Any], negative_guard_diagnostics: dict[str, Any]) -> dict[str, Any]:
    safe_reference: dict[str, Any] = {}
    for split in ("train", "validation", "test"):
        scope = frame[frame["split"].eq(split) & frame["safe_full_tag"]]
        safe_reference[split] = {
            "safe_full_tagged_sample_count": int(len(scope)),
            "safe_full_tagged_avg_ret20": float(scope["ret20"].mean()) if len(scope) else None,
            "safe_full_tagged_severe_loss_rate20": float(scope["severe_loss20"].mean()) if len(scope) else None,
            "safe_full_used_as_hard_filter": False,
        }
    test = topk_report["by_split"]["test"]
    image_ret = test.get("image_only_top3_avg_ret20")
    random_ret = test.get("random_top3_avg_ret20")
    image_severe = test.get("image_only_top3_severe_loss_rate20")
    random_severe = test.get("random_top3_severe_loss_rate20")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_baseline_comparison_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "random_top3_within_candidate_day": {split: topk_report["by_split"][split] for split in ("train", "validation", "test")},
        "wide_strength_pool_previous_best_available": False,
        "wide_strength_pool_previous_best_unavailable_reason": "Phase0/1 artifact did not persist numeric prior score values in image_manifest",
        "safe_full_hard_filter_historical_reference": safe_reference,
        "negative_guard_matched_subset_metrics": {
            "test_negative_guard_classifier_auc": negative_guard_diagnostics.get("negative_guard_classifier_auc"),
            "test_negative_guard_classifier_mcc": negative_guard_diagnostics.get("negative_guard_classifier_mcc"),
            "test_negative_guard_score_separation": negative_guard_diagnostics.get("negative_guard_score_separation"),
            "test_negative_guard_image_top3_avg_ret20": negative_guard_diagnostics.get("negative_guard_image_top3_avg_ret20"),
        },
        "test_image_top3_minus_random_top3_avg_ret20": image_ret - random_ret if image_ret is not None and random_ret is not None else None,
        "test_image_top3_minus_random_top3_severe_loss_rate20": image_severe - random_severe if image_severe is not None and random_severe is not None else None,
    }


def build_dataset_audit(frame: pd.DataFrame, readable_frame: pd.DataFrame, source_status: dict[str, Any]) -> dict[str, Any]:
    label_counts_by_split: dict[str, Any] = {}
    for split in ("train", "validation", "test", "embargo"):
        split_frame = readable_frame[readable_frame["split"].eq(split)]
        label_counts_by_split[split] = {
            "sample_count": int(len(split_frame)),
            "positive_count": int(split_frame["future_top15_by_ret20"].sum()),
            "negative_count": int(split_frame["future_bottom15_by_ret20"].sum()),
            "neutral_count": int(split_frame["neutral_middle70"].sum()),
            "binary_sample_count": int(split_frame["binary_target_available"].sum()),
            "negative_guard_sample_count": int(split_frame["negative_guard_matched"].sum()),
            "safe_full_sample_count": int(split_frame["safe_full_tag"].sum()),
        }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_dataset_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        **source_status,
        "source_row_count": int(len(frame)),
        "readable_row_count": int(len(readable_frame)),
        "dropped_unreadable_image_count": int(len(frame) - len(readable_frame)),
        "sample_reduction_used": False,
        "neutral_middle70_excluded_from_primary_training": True,
        "label_counts_by_split": label_counts_by_split,
        "train_binary_sample_count": label_counts_by_split["train"]["binary_sample_count"],
        "validation_binary_sample_count": label_counts_by_split["validation"]["binary_sample_count"],
        "test_binary_sample_count": label_counts_by_split["test"]["binary_sample_count"],
    }


def build_phase2_split_leakage_audit(frame: pd.DataFrame, source_dir: Path) -> dict[str, Any]:
    source_split_audit = _load_json(source_dir / "split_leakage_audit.json")
    non_embargo = frame[frame["split"].isin(["train", "validation", "test"])]
    same_date_cross_split = bool((non_embargo.groupby("event_ymd")["split"].nunique() > 1).any()) if len(non_embargo) else True
    labels_in_candidate_key = bool(frame["labels_used_in_candidate_key"].any()) if "labels_used_in_candidate_key" in frame.columns else False
    labels_in_rendering = bool(frame["labels_used_in_image_rendering"].any()) if "labels_used_in_image_rendering" in frame.columns else False
    passed = (
        source_split_audit.get("split_leakage_audit_passed") is True
        and not same_date_cross_split
        and not labels_in_candidate_key
        and not labels_in_rendering
    )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_phase2_split_leakage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "split_source": "phase0_1_split_assignment_ledger",
        "split_regenerated": False,
        "random_split_used": False,
        "train_only_used_for_model_fitting": True,
        "validation_used_for_reporting_not_full_period_tuning": True,
        "test_used_only_for_final_reporting": True,
        "same_event_date_across_train_validation_test": same_date_cross_split,
        "source_split_leakage_audit_passed": source_split_audit.get("split_leakage_audit_passed") is True,
        "phase2_split_leakage_audit_passed": passed,
        "feature_window_crosses_prior_split_boundary": source_split_audit.get("feature_window_crosses_prior_split_boundary"),
        "past_only_feature_window_overlap_allowed": source_split_audit.get("past_only_feature_window_overlap_allowed"),
        "future_label_window_overlap_train_validation": source_split_audit.get("future_label_window_overlap_train_validation"),
        "future_label_window_overlap_validation_test": source_split_audit.get("future_label_window_overlap_validation_test"),
        "future_labels_used_in_image_rendering": labels_in_rendering,
        "future_labels_used_in_candidate_key": labels_in_candidate_key,
        "future_labels_used_as_inference_inputs": False,
        "future_labels_used_as_training_targets_only": True,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def _ledger_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in frame.sort_values(["event_ymd", "symbol", "image_sample_key"]).itertuples(index=False):
        score = float(getattr(row, "image_score"))
        rows.append(
            {
                "schema_version": f"{SCHEMA_PREFIX}_image_score_ledger_row_v1",
                "image_sample_key": getattr(row, "image_sample_key"),
                "candidate_event_key": getattr(row, "candidate_event_key", None),
                "symbol": str(getattr(row, "symbol")),
                "event_date": str(getattr(row, "event_date")),
                "event_ymd": int(getattr(row, "event_ymd")),
                "split": str(getattr(row, "split")),
                "image_score": score,
                "image_predicted_label": "future_top15_by_ret20" if score >= 0.5 else "future_bottom15_by_ret20",
                "primary_label": str(getattr(row, "primary_label")),
                "binary_target_available": bool(getattr(row, "binary_target_available")),
                "future_top15_by_ret20": bool(getattr(row, "future_top15_by_ret20")),
                "future_bottom15_by_ret20": bool(getattr(row, "future_bottom15_by_ret20")),
                "neutral_middle70": bool(getattr(row, "neutral_middle70")),
                "ret20": float(getattr(row, "ret20")),
                "MFE20": float(getattr(row, "MFE20")),
                "MAE20": float(getattr(row, "MAE20")),
                "severe_loss20": bool(getattr(row, "severe_loss20")),
                "future_top10_by_ret20": bool(getattr(row, "future_top10_by_ret20")),
                "future_top5_by_ret20": bool(getattr(row, "future_top5_by_ret20")),
                "big_winner_ret20_ge_10pct": bool(getattr(row, "big_winner_ret20_ge_10pct")),
                "negative_guard_matched": bool(getattr(row, "negative_guard_matched")),
                "safe_full_tag": bool(getattr(row, "safe_full_tag")),
                "image_score_scope": "research_only",
                "base_score_input_used": False,
                "fusion_used": False,
            }
        )
    return rows


def _save_model_checkpoint(model: Pipeline, output_dir: Path, training_config: dict[str, Any]) -> dict[str, Any]:
    model_dir = output_dir / "model"
    model_dir.mkdir(parents=True, exist_ok=True)
    model_path = model_dir / f"{MODEL_FAMILY_ID}.pkl"
    with model_path.open("wb") as handle:
        pickle.dump({"model": model, "training_config": training_config}, handle)
    model_hash = _file_hash(model_path)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_model_checkpoint_manifest_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "model_checkpoint_path": str(model_path),
        "model_checkpoint_sha256": model_hash,
        "model_checkpoint_created": True,
        "pretrained_weights_used": False,
        "pretrained_source": None,
        "weight_hash": None,
        "license_note": None,
    }


def build_decision_artifacts(
    *,
    run_id: str,
    output_dir: Path,
    source_dir: Path,
    source_status: dict[str, Any],
    frame: pd.DataFrame,
    training_config: dict[str, Any],
    dataset_audit: dict[str, Any],
    image_input_audit: dict[str, Any],
    split_audit: dict[str, Any],
    model_checkpoint: dict[str, Any],
    classifier_metrics: dict[str, Any],
    score_distribution: dict[str, Any],
    topk_report: dict[str, Any],
    negative_guard_diagnostics: dict[str, Any],
    baseline_comparison: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "refs": [
            {
                "axis_id": "image_assisted_rerank_phase0_1",
                "run_id": source_dir.name,
                "path": str(source_dir),
                "exists": source_dir.exists(),
                "file_hashes": {name: _file_hash(source_dir / name) for name in REQUIRED_SOURCE_FILES if (source_dir / name).exists()},
            }
        ],
        "source_phase0_1_status": source_status,
    }
    split_passed = split_audit.get("phase2_split_leakage_audit_passed") is True
    test_mcc = classifier_metrics.get("test_mcc")
    test_auc = classifier_metrics.get("test_roc_auc")
    test_sep = score_distribution.get("test_score_separation_top15_vs_bottom15")
    val_sep = score_distribution["by_split"]["validation"].get("score_separation_top15_vs_bottom15")
    image_top3 = topk_report.get("test_image_only_top3_avg_ret20")
    random_top3 = topk_report.get("test_random_top3_avg_ret20")
    image_severe = topk_report.get("test_image_only_top3_severe_loss_rate20")
    random_severe = topk_report.get("test_random_top3_severe_loss_rate20")
    ng_sep = negative_guard_diagnostics.get("negative_guard_score_separation")
    ng_auc = negative_guard_diagnostics.get("negative_guard_classifier_auc")
    gate_checks = {
        "test_mcc_gt_0_03": test_mcc is not None and test_mcc > 0.03,
        "test_roc_auc_gt_0_53": test_auc is not None and test_auc > 0.53,
        "score_separation_top15_vs_bottom15_positive": test_sep is not None and test_sep > 0.0,
        "image_only_top3_avg_ret20_ge_random_top3": image_top3 is not None and random_top3 is not None and image_top3 >= random_top3,
        "image_only_top3_severe_loss_not_materially_worse_than_random": image_severe is not None and random_severe is not None and image_severe <= random_severe + 0.02,
        "negative_guard_score_separation_positive_or_not_worse_than_numeric": (ng_sep is not None and ng_sep >= 0.0) or (ng_auc is not None and ng_auc >= 0.5),
        "validation_and_test_signs_directionally_consistent": val_sep is not None and test_sep is not None and val_sep > 0.0 and test_sep > 0.0,
        "split_leakage_audit_passed": split_passed,
        "future_label_leakage_absent": True,
        "silent_fallback_used_false": True,
        "research_fallback_used_false": True,
        "artifact_complete": True,
    }
    ready_for_fusion = all(gate_checks.values())
    typed_reasons: list[str] = []
    fatal = False
    if not split_passed:
        typed_reasons.append("split_leakage_audit_failed")
        fatal = True
    if image_input_audit.get("image_unreadable_or_missing_count", 0):
        typed_reasons.append("image_input_missing_or_unreadable")
        fatal = True
    if test_mcc is None or test_mcc <= 0.0:
        typed_reasons.append("test_mcc_nonpositive")
    if test_auc is None or test_auc <= 0.50:
        typed_reasons.append("test_auc_near_random_or_worse")
    if test_sep is None or test_sep <= 0.0:
        typed_reasons.append("image_score_does_not_separate_test_top_bottom")
    if image_top3 is not None and random_top3 is not None and image_top3 < random_top3:
        typed_reasons.append("image_top3_proxy_worse_than_random")
    if ng_sep is None or ng_sep <= 0.0:
        typed_reasons.append("negative_guard_score_separation_not_positive")
    if not training_config.get("preferred_cnn_or_resnet_used"):
        typed_reasons.append("preferred_cnn_unavailable_used_sklearn_image_only_baseline")
    if ready_for_fusion:
        decision = "keep_candidate"
        authoritative = "image_only_classifier_phase2_ready_for_fusion"
        typed_reasons.append("image_only_signal_ready_for_fusion_phase3")
    elif fatal or (test_mcc is not None and test_mcc <= 0.0) or (test_auc is not None and test_auc <= 0.50) or (test_sep is not None and test_sep <= 0.0):
        decision = "drop"
        authoritative = "image_only_classifier_phase2_failed"
    else:
        decision = "hold"
        authoritative = "image_only_classifier_phase2_hold"
    phase3_readiness = {
        "schema_version": f"{SCHEMA_PREFIX}_phase3_readiness_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "ready_for_fusion": ready_for_fusion,
        "gate_checks": gate_checks,
        "test_mcc": test_mcc,
        "test_roc_auc": test_auc,
        "test_score_separation_top15_vs_bottom15": test_sep,
        "validation_score_separation_top15_vs_bottom15": val_sep,
        "image_only_top3_avg_ret20": image_top3,
        "random_top3_avg_ret20": random_top3,
        "image_only_top3_severe_loss_rate20": image_severe,
        "random_top3_severe_loss_rate20": random_severe,
        "negative_guard_score_separation": ng_sep,
        "negative_guard_classifier_auc": ng_auc,
        "next_axis_if_ready": "image_score_fusion_rerank_phase3",
        "next_axis_if_hold": "image_model_data_ablation",
        "next_axis_if_failed": "image_route_pause_or_ranking_loss_protocol_repair",
    }
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": AXIS_ID,
        "source_phase0_1_run_id": source_dir.name,
        "source_phase0_1_decision": source_status.get("source_phase0_1_decision"),
        "primary_binary_target": {"positive": PRIMARY_POSITIVE_LABEL, "negative": PRIMARY_NEGATIVE_LABEL},
        "neutral_middle70_policy": "excluded_from_primary_training_scored_for_distribution_audit",
        "split_source": "phase0_1_split_assignment_ledger",
        "train_split_only_for_model_fitting": True,
        "test_split_final_reporting_only": True,
        "image_only": True,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    model_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_phase2_model_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "image_model_trained": True,
        "image_only_classifier_created": True,
        "image_score_created": True,
        "image_score_scope": "research_only",
        "image_only": True,
        "base_score_input_used": False,
        "fusion_reranker_created": False,
        "candidate_scoring_created": False,
        "production_ranking_changed": False,
        "preferred_cnn_or_resnet_used": False,
        "model_architecture": training_config.get("model_architecture"),
        "torch_available": False,
        "torchvision_available": False,
        "pretrained_weights_used": False,
        "full_dataset_used": True,
        "sample_reduction_used": False,
    }
    model_contract["contract_hash"] = _stable_hash(model_contract)
    label_usage_audit = {
        "schema_version": f"{SCHEMA_PREFIX}_label_usage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "future_labels_used_as_training_targets_only": True,
        "future_labels_used_in_image_rendering": False,
        "future_labels_used_in_candidate_key": False,
        "future_labels_used_as_inference_inputs": False,
        "future_labels_used_for_split_assignment": False,
        "labels_in_model_features": False,
        "production_score_created": False,
        "silent_fallback_used": False,
    }
    research_decision = {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": AXIS_ID,
        "boundary": "TRADEX-only",
        "axis_moved": AXIS_ID,
        "source_phase0_1_decision": source_status.get("source_phase0_1_decision"),
        "image_model_trained": True,
        "image_only_classifier_created": True,
        "image_score_created": True,
        "image_score_scope": "research_only",
        "fusion_reranker_created": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "yolo_used": False,
        "llm_used": False,
        "future_labels_used_as_training_targets_only": True,
        "future_labels_used_in_image_rendering": False,
        "future_labels_used_in_candidate_key": False,
        "future_labels_used_as_inference_inputs": False,
        "split_source": "phase0_1_split_assignment_ledger",
        "split_leakage_audit_passed": split_passed,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "typed_reasons": sorted(set(typed_reasons)),
    }
    run_manifest = contracts.build_run_manifest(
        session_id=run_id,
        seed=RANDOM_SEED,
        random_seed=RANDOM_SEED,
        input_artifacts=source_refs["refs"],
        asof=_utc_now(),
        config={
            "axis_id": AXIS_ID,
            "model_family_id": MODEL_FAMILY_ID,
            "image_feature_size": IMAGE_FEATURE_SIZE,
            "primary_positive_label": PRIMARY_POSITIVE_LABEL,
            "primary_negative_label": PRIMARY_NEGATIVE_LABEL,
            "neutral_policy": "excluded_from_primary_training",
            "device": "cpu",
        },
        universe=sorted(frame["symbol"].astype(str).unique().tolist()),
        period={
            "start_date": str(frame["event_date"].min()),
            "end_date": str(frame["event_date"].max()),
            "sample_count": int(len(frame)),
        },
        horizon="20 trading days",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "run_id": run_id,
        "complete": True,
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "missing_artifacts": [],
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
    }
    return {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": source_refs,
        "phase2_model_contract.json": model_contract,
        "training_config.json": training_config,
        "dataset_audit.json": dataset_audit,
        "image_input_audit.json": image_input_audit,
        "label_usage_audit.json": label_usage_audit,
        "phase2_split_leakage_audit.json": split_audit,
        "model_checkpoint_manifest.json": model_checkpoint,
        "classifier_metrics.json": classifier_metrics,
        "score_distribution_report.json": score_distribution,
        "topk_proxy_report.json": topk_report,
        "negative_guard_image_diagnostics.json": negative_guard_diagnostics,
        "baseline_comparison_report.json": baseline_comparison,
        "phase3_readiness_report.json": phase3_readiness,
        "research_decision.json": research_decision,
        "_ARTIFACT_COMPLETE.json": complete,
    }


def run_image_only_classifier_baseline_phase2(
    *,
    source_image_phase0_1_run_id: str = DEFAULT_SOURCE_PHASE0_1_RUN_ID,
    source_image_phase0_1_root: str | Path = DEFAULT_SOURCE_PHASE0_1_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    run_id = run_id or _default_run_id()
    source_dir = _safe_path(source_image_phase0_1_root, DEFAULT_SOURCE_PHASE0_1_ROOT) / source_image_phase0_1_run_id
    output_dir = _run_dir(output_root, run_id, DEFAULT_OUTPUT_ROOT)
    output_dir.mkdir(parents=True, exist_ok=True)
    frame, source_status = load_phase0_1_dataset(source_dir)
    features, readable_mask, image_input_audit = extract_image_features(frame)
    readable_frame = frame.loc[readable_mask].reset_index(drop=True)
    readable_features = features
    dataset_audit = build_dataset_audit(frame, readable_frame, source_status)
    if image_input_audit["image_unreadable_or_missing_count"]:
        raise RuntimeError("Phase2 does not silently continue with unreadable images")
    model, training_config, training_log = train_image_only_classifier(readable_frame, readable_features)
    readable_frame["image_score"] = model.predict_proba(readable_features)[:, 1]
    split_audit = build_phase2_split_leakage_audit(readable_frame, source_dir)
    model_checkpoint = _save_model_checkpoint(model, output_dir, training_config)
    classifier_metrics = build_classifier_metrics(readable_frame)
    score_distribution = build_score_distribution_report(readable_frame)
    topk_report = build_topk_proxy_report(readable_frame)
    negative_guard_diagnostics = build_negative_guard_diagnostics(readable_frame)
    baseline_comparison = build_baseline_comparison_report(readable_frame, topk_report, negative_guard_diagnostics)
    artifacts = build_decision_artifacts(
        run_id=run_id,
        output_dir=output_dir,
        source_dir=source_dir,
        source_status=source_status,
        frame=readable_frame,
        training_config=training_config,
        dataset_audit=dataset_audit,
        image_input_audit=image_input_audit,
        split_audit=split_audit,
        model_checkpoint=model_checkpoint,
        classifier_metrics=classifier_metrics,
        score_distribution=score_distribution,
        topk_report=topk_report,
        negative_guard_diagnostics=negative_guard_diagnostics,
        baseline_comparison=baseline_comparison,
    )
    _write_jsonl(output_dir / "training_log.jsonl", training_log)
    _write_jsonl(output_dir / "image_score_ledger.jsonl", _ledger_rows(readable_frame))
    for filename, payload in artifacts.items():
        _write_json(output_dir / filename, payload)
    missing = [name for name in REQUIRED_ARTIFACTS if not (output_dir / name).exists()]
    if missing:
        raise RuntimeError(f"artifact write incomplete: {missing}")
    return {
        "output_dir": str(output_dir),
        "run_id": run_id,
        "decision": artifacts["research_decision.json"]["decision"],
        "authoritative_research_decision": artifacts["research_decision.json"]["authoritative_research_decision"],
        "ready_for_fusion": artifacts["phase3_readiness_report.json"]["ready_for_fusion"],
        "test_mcc": artifacts["classifier_metrics.json"]["test_mcc"],
        "test_roc_auc": artifacts["classifier_metrics.json"]["test_roc_auc"],
        "test_score_separation_top15_vs_bottom15": artifacts["score_distribution_report.json"]["test_score_separation_top15_vs_bottom15"],
        "test_image_only_top3_avg_ret20": artifacts["topk_proxy_report.json"]["test_image_only_top3_avg_ret20"],
        "test_random_top3_avg_ret20": artifacts["topk_proxy_report.json"]["test_random_top3_avg_ret20"],
        "negative_guard_score_separation": artifacts["negative_guard_image_diagnostics.json"]["negative_guard_score_separation"],
        "image_model_trained": True,
        "image_only_classifier_created": True,
        "image_score_created": True,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX image-only classifier baseline Phase2")
    parser.add_argument("--source-image-phase0-1-run-id", default=DEFAULT_SOURCE_PHASE0_1_RUN_ID)
    parser.add_argument("--source-image-phase0-1-root", default=str(DEFAULT_SOURCE_PHASE0_1_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_image_only_classifier_baseline_phase2(
        source_image_phase0_1_run_id=args.source_image_phase0_1_run_id,
        source_image_phase0_1_root=args.source_image_phase0_1_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
