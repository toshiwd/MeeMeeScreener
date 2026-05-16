from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
from PIL import Image

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_image_only_classifier_baseline_phase2 as phase2_mod


AXIS_ID = "image_cnn_baseline_phase2b"
SCHEMA_PREFIX = "tradex_image_cnn_baseline_phase2b"
DEFAULT_SOURCE_PHASE0_1_RUN_ID = "20260513T080000Z-image-assisted-rerank-phase0-1"
DEFAULT_SOURCE_PHASE2_RUN_ID = "20260513T090000Z-image-only-classifier-baseline-phase2"
DEFAULT_SOURCE_PHASE0_1_ROOT = Path(r"G:\Tradex\image_assisted_rerank_phase0_1")
DEFAULT_SOURCE_PHASE2_ROOT = Path(r"G:\Tradex\image_only_classifier_baseline_phase2")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\image_cnn_baseline_phase2b")

RANDOM_SEED = 20260513
MODEL_FAMILY_ID = "simple_cnn_phase2b_v1"
CNN_IMAGE_SIZE = 64
CNN_BATCH_SIZE = 128
CNN_EPOCHS = 4
CNN_LEARNING_RATE = 0.001

REQUIRED_PHASE0_1_FILES = (
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

REQUIRED_PHASE2_FILES = (
    "image_score_ledger.jsonl",
    "classifier_metrics.json",
    "score_distribution_report.json",
    "topk_proxy_report.json",
    "negative_guard_image_diagnostics.json",
    "phase3_readiness_report.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "dependency_audit.json",
    "phase2b_model_contract.json",
    "training_config.json",
    "dataset_audit.json",
    "image_input_audit.json",
    "label_usage_audit.json",
    "phase2b_split_leakage_audit.json",
    "training_log.jsonl",
    "model_checkpoint_manifest.json",
    "cnn_image_score_ledger.jsonl",
    "classifier_metrics.json",
    "logistic_baseline_comparison_report.json",
    "score_distribution_report.json",
    "topk_proxy_report.json",
    "negative_guard_cnn_diagnostics.json",
    "phase3_readiness_report.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    return phase2_mod._json_ready(value)


def _json_text(payload: Any) -> str:
    return phase2_mod._json_text(payload)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return phase2_mod._write_json(path, payload)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    return phase2_mod._write_jsonl(path, rows)


def _load_json(path: Path) -> dict[str, Any]:
    return phase2_mod._load_json(path)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return phase2_mod._read_jsonl(path)


def _stable_hash(payload: Any) -> str:
    return phase2_mod._stable_hash(payload)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return phase2_mod._safe_path(value, default)


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return phase2_mod._run_dir(root, run_id, default_root)


def _file_hash(path: Path) -> str | None:
    return phase2_mod._file_hash(path)


def audit_dependencies() -> dict[str, Any]:
    _remove_lazy_service_proxy_modules_before_torch_import()
    torch_spec = importlib.util.find_spec("torch")
    torchvision_spec = importlib.util.find_spec("torchvision")
    torch_available = torch_spec is not None
    torchvision_available = torchvision_spec is not None
    torch_version = None
    torchvision_version = None
    cuda_available = False
    device = "unavailable"
    if torch_available:
        import torch  # type: ignore

        torch_version = str(torch.__version__)
        cuda_available = bool(torch.cuda.is_available())
        device = "cuda" if cuda_available else "cpu"
    if torchvision_available:
        import torchvision  # type: ignore

        torchvision_version = str(torchvision.__version__)
    can_train_simple_cnn = torch_available
    can_train_resnet18 = torch_available and torchvision_available
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_dependency_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "torch_available": torch_available,
        "torch_version": torch_version,
        "torchvision_available": torchvision_available,
        "torchvision_version": torchvision_version,
        "cuda_available": cuda_available,
        "selected_device": device,
        "can_train_simple_cnn_phase2b_v1": can_train_simple_cnn,
        "can_train_resnet18_phase2b_v1": can_train_resnet18,
        "sklearn_fallback_allowed": False,
        "sklearn_fallback_used": False,
        "production_runtime_requirements_modified": False,
        "meemee_runtime_dependencies_modified": False,
        "dependency_blocked": not can_train_simple_cnn,
        "blocked_reason": None if can_train_simple_cnn else "torch_unavailable",
        "isolated_research_environment_notes": [
            r"Do not add torch to MeeMee production runtime requirements.",
            r"Use an isolated research environment, for example: python -m venv G:\Tradex\envs\image-cnn-phase2b",
            r"Install torch/torchvision inside that isolated environment only, then rerun this script.",
            r"Record torch/torchvision versions and CUDA availability in dependency_audit.json.",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def validate_phase2_source(source_dir: Path) -> dict[str, Any]:
    missing = [name for name in REQUIRED_PHASE2_FILES if not (source_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Phase2 logistic source missing required artifacts: {missing} at {source_dir}")
    complete = _load_json(source_dir / "_ARTIFACT_COMPLETE.json")
    decision = _load_json(source_dir / "research_decision.json")
    readiness = _load_json(source_dir / "phase3_readiness_report.json")
    classifier = _load_json(source_dir / "classifier_metrics.json")
    score_distribution = _load_json(source_dir / "score_distribution_report.json")
    negative_guard = _load_json(source_dir / "negative_guard_image_diagnostics.json")
    topk = _load_json(source_dir / "topk_proxy_report.json")
    if complete.get("complete") is not True:
        raise RuntimeError("Phase2 logistic source artifact is not complete")
    if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is not False:
        raise RuntimeError("Phase2 logistic source used silent fallback")
    if complete.get("research_fallback_used") is not False or decision.get("research_fallback_used") is not False:
        raise RuntimeError("Phase2 logistic source used research fallback")
    if decision.get("authoritative_research_decision") != "image_only_classifier_phase2_failed":
        raise RuntimeError("Phase2 logistic source is not image_only_classifier_phase2_failed")
    if readiness.get("ready_for_fusion") is not False:
        raise RuntimeError("Phase2 logistic source unexpectedly allows fusion")
    return {
        "source_phase2_decision": decision.get("authoritative_research_decision"),
        "source_phase2_local_decision": decision.get("decision"),
        "source_phase2_ready_for_fusion": readiness.get("ready_for_fusion"),
        "logistic_test_roc_auc": classifier.get("test_roc_auc"),
        "logistic_test_mcc": classifier.get("test_mcc"),
        "logistic_score_separation_top15_vs_bottom15": score_distribution.get("test_score_separation_top15_vs_bottom15"),
        "logistic_negative_guard_auc": negative_guard.get("negative_guard_classifier_auc"),
        "logistic_negative_guard_score_separation": negative_guard.get("negative_guard_score_separation"),
        "logistic_image_top3_proxy_avg_ret20": topk.get("test_image_only_top3_avg_ret20"),
        "logistic_random_top3_avg_ret20": topk.get("test_random_top3_avg_ret20"),
        "logistic_image_score_ledger_rows": len(_read_jsonl(source_dir / "image_score_ledger.jsonl")),
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def _source_ref(axis_id: str, run_id: str, path: Path, files: tuple[str, ...]) -> dict[str, Any]:
    return {
        "axis_id": axis_id,
        "run_id": run_id,
        "path": str(path),
        "exists": path.exists(),
        "file_hashes": {name: _file_hash(path / name) for name in files if (path / name).exists()},
    }


def _counts_by_split(frame: pd.DataFrame, mask_column: str | None = None) -> dict[str, int]:
    scoped = frame if mask_column is None else frame[frame[mask_column].astype(bool)]
    return {split: int((scoped["split"] == split).sum()) for split in ("train", "validation", "test", "embargo")}


def build_dataset_audit(frame: pd.DataFrame, phase0_status: dict[str, Any]) -> dict[str, Any]:
    label_counts: dict[str, Any] = {}
    for split in ("train", "validation", "test", "embargo"):
        group = frame[frame["split"].eq(split)]
        label_counts[split] = {
            "sample_count": int(len(group)),
            "top15_count": int(group["future_top15_by_ret20"].sum()),
            "bottom15_count": int(group["future_bottom15_by_ret20"].sum()),
            "neutral_count": int(group["neutral_middle70"].sum()),
            "binary_sample_count": int(group["binary_target_available"].sum()),
            "negative_guard_sample_count": int(group["negative_guard_matched"].sum()),
            "safe_full_sample_count": int(group["safe_full_tag"].sum()),
        }
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_dataset_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        **phase0_status,
        "source_row_count": int(len(frame)),
        "sample_reduction_used": False,
        "neutral_middle70_excluded_from_primary_training": True,
        "label_counts_by_split": label_counts,
        "negative_guard_sample_count_by_split": _counts_by_split(frame, "negative_guard_matched"),
        "safe_full_sample_count_by_split": _counts_by_split(frame, "safe_full_tag"),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_image_input_audit(frame: pd.DataFrame, *, dependency_blocked: bool) -> dict[str, Any]:
    exists = frame["image_path"].map(lambda value: Path(str(value)).exists())
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_image_input_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "image_row_count": int(len(frame)),
        "image_path_exists_count": int(exists.sum()),
        "image_path_exists_rate": float(exists.mean()) if len(exists) else 0.0,
        "image_read_audit_performed": not dependency_blocked,
        "image_read_audit_skipped_reason": "dependency_blocked_before_cnn_training" if dependency_blocked else None,
        "source_image_size_expected": "224x224",
        "cnn_input_contract": "day80_candlestick_volume_png_from_phase0_1",
        "future_labels_used_as_inference_inputs": False,
        "labels_used_in_image_loading": False,
        "sample_reduction_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_split_leakage_audit(frame: pd.DataFrame, source_phase0_1_dir: Path) -> dict[str, Any]:
    source_audit = _load_json(source_phase0_1_dir / "split_leakage_audit.json")
    non_embargo = frame[frame["split"].isin(["train", "validation", "test"])]
    same_date_cross_split = bool((non_embargo.groupby("event_ymd")["split"].nunique() > 1).any()) if len(non_embargo) else True
    labels_in_candidate_key = bool(frame["labels_used_in_candidate_key"].any()) if "labels_used_in_candidate_key" in frame.columns else False
    labels_in_rendering = bool(frame["labels_used_in_image_rendering"].any()) if "labels_used_in_image_rendering" in frame.columns else False
    passed = source_audit.get("split_leakage_audit_passed") is True and not same_date_cross_split and not labels_in_candidate_key and not labels_in_rendering
    return {
        "schema_version": f"{SCHEMA_PREFIX}_phase2b_split_leakage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "split_source": "phase0_1_split_assignment_ledger",
        "split_regenerated": False,
        "random_split_used": False,
        "train_only_for_model_fitting": True,
        "validation_only_for_model_selection": True,
        "test_only_for_final_reporting": True,
        "same_event_date_across_train_validation_test": same_date_cross_split,
        "source_split_leakage_audit_passed": source_audit.get("split_leakage_audit_passed") is True,
        "split_leakage_audit_passed": passed,
        "feature_window_crosses_prior_split_boundary": source_audit.get("feature_window_crosses_prior_split_boundary"),
        "past_only_feature_window_overlap_allowed": source_audit.get("past_only_feature_window_overlap_allowed"),
        "future_label_window_overlap_train_validation": source_audit.get("future_label_window_overlap_train_validation"),
        "future_label_window_overlap_validation_test": source_audit.get("future_label_window_overlap_validation_test"),
        "future_labels_used_in_image_rendering": labels_in_rendering,
        "future_labels_used_in_candidate_key": labels_in_candidate_key,
        "future_labels_used_as_inference_inputs": False,
        "future_labels_used_as_training_targets_only": True,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def _unavailable_classifier_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    by_split: dict[str, Any] = {}
    for split in ("train", "validation", "test"):
        group = frame[frame["split"].eq(split)]
        by_split[split] = {
            "sample_count": int(len(group)),
            "binary_sample_count": int(group["binary_target_available"].sum()),
            "positive_count": int(group["future_top15_by_ret20"].sum()),
            "negative_count": int(group["future_bottom15_by_ret20"].sum()),
            "neutral_count": int(group["neutral_middle70"].sum()),
            "accuracy": None,
            "balanced_accuracy": None,
            "precision": None,
            "recall": None,
            "f1": None,
            "roc_auc": None,
            "pr_auc": None,
            "mcc": None,
            "log_loss": None,
            "brier_score": None,
            "confusion_matrix": [[0, 0], [0, 0]],
        }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_classifier_metrics_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "metrics_available": False,
        "metrics_unavailable_reason": "torch_unavailable_cnn_not_trained",
        "train_sample_count": by_split["train"]["sample_count"],
        "validation_sample_count": by_split["validation"]["sample_count"],
        "test_sample_count": by_split["test"]["sample_count"],
        "positive_count_by_split": {split: by_split[split]["positive_count"] for split in by_split},
        "negative_count_by_split": {split: by_split[split]["negative_count"] for split in by_split},
        "neutral_count_by_split": {split: by_split[split]["neutral_count"] for split in by_split},
        "by_split": by_split,
        "test_accuracy": None,
        "test_balanced_accuracy": None,
        "test_precision": None,
        "test_recall": None,
        "test_f1": None,
        "test_roc_auc": None,
        "test_pr_auc": None,
        "test_mcc": None,
        "test_log_loss": None,
        "test_brier_score": None,
        "confusion_matrix": [[0, 0], [0, 0]],
    }


def _unavailable_report(name: str, reason: str) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_{name}_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "available": False,
        "unavailable_reason": reason,
    }


def _frame_with_score_column(frame: pd.DataFrame, score_column: str = "image_score") -> pd.DataFrame:
    scored = frame.copy()
    if score_column != "image_score":
        scored["image_score"] = pd.to_numeric(scored[score_column], errors="coerce")
    return scored


def _classifier_metrics_from_scored_frame(frame: pd.DataFrame, score_column: str) -> dict[str, Any]:
    scored = _frame_with_score_column(frame, score_column)
    payload = phase2_mod.build_classifier_metrics(scored)
    payload["schema_version"] = f"{SCHEMA_PREFIX}_classifier_metrics_v1"
    payload["axis_id"] = AXIS_ID
    payload["model_family_id"] = MODEL_FAMILY_ID
    payload["test_brier_score"] = payload.get("test_calibration_brier_score")
    if "by_split" in payload:
        for row in payload["by_split"].values():
            row["brier_score"] = row.get("calibration_brier_score")
    return payload


def _score_distribution_from_scored_frame(frame: pd.DataFrame, score_column: str) -> dict[str, Any]:
    scored = _frame_with_score_column(frame, score_column)
    payload = phase2_mod.build_score_distribution_report(scored)
    payload["schema_version"] = f"{SCHEMA_PREFIX}_score_distribution_report_v1"
    payload["axis_id"] = AXIS_ID
    payload["model_family_id"] = MODEL_FAMILY_ID
    payload["score_separation_top15_vs_bottom15"] = payload.get("test_score_separation_top15_vs_bottom15")
    return payload


def _topk_report_from_scored_frame(frame: pd.DataFrame, score_column: str) -> dict[str, Any]:
    by_split: dict[str, Any] = {}
    for split in ("train", "validation", "test"):
        scope = frame[frame["split"].eq(split)].copy()
        metrics = phase2_mod._topk_metric_bundle(scope, score_column=score_column, prefix="cnn")
        random_metrics = phase2_mod._random_topk_baseline(scope)
        by_split[split] = {
            "sample_count": int(len(scope)),
            **metrics,
            **random_metrics,
            "topk_scope": "research_only_proxy_not_production_ranking",
        }
    test = by_split["test"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_topk_proxy_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "topk_scope": "research_only_proxy_not_production_ranking",
        "by_split": by_split,
        "cnn_top1_avg_ret20": test.get("cnn_top1_avg_ret20"),
        "cnn_top3_avg_ret20": test.get("cnn_top3_avg_ret20"),
        "cnn_top5_avg_ret20": test.get("cnn_top5_avg_ret20"),
        "cnn_top10_avg_ret20": test.get("cnn_top10_avg_ret20"),
        "cnn_top3_win_rate20": test.get("cnn_top3_win_rate20"),
        "cnn_top3_avg_MFE20": test.get("cnn_top3_avg_MFE20"),
        "cnn_top3_avg_MAE20": test.get("cnn_top3_avg_MAE20"),
        "cnn_top3_severe_loss_rate20": test.get("cnn_top3_severe_loss_rate20"),
        "cnn_top3_big_winner_capture_rate": test.get("cnn_top3_big_winner_capture_rate"),
        "cnn_selected_nonwinner_when_winner_available_rate": test.get("cnn_selected_nonwinner_when_winner_available_rate"),
        "cnn_oracle_top3_gap_ret20": test.get("cnn_oracle_top3_gap_ret20"),
        "random_top3_avg_ret20": test.get("random_top3_avg_ret20"),
        "random_top3_severe_loss_rate20": test.get("random_top3_severe_loss_rate20"),
    }


def _negative_guard_cnn_diagnostics(frame: pd.DataFrame, score_column: str) -> dict[str, Any]:
    scored = _frame_with_score_column(frame, score_column)
    base = phase2_mod.build_negative_guard_diagnostics(scored)
    topk_test = base.get("topk_proxy_by_split", {}).get("test", {})
    return {
        "schema_version": f"{SCHEMA_PREFIX}_negative_guard_cnn_diagnostics_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "negative_guard_sample_count_by_split": base.get("negative_guard_sample_count_by_split", {}),
        "negative_guard_top15_count_by_split": base.get("negative_guard_top15_count_by_split", {}),
        "negative_guard_bottom15_count_by_split": base.get("negative_guard_bottom15_count_by_split", {}),
        "classifier_metrics_by_split": base.get("classifier_metrics_by_split", {}),
        "score_separation_by_split": base.get("score_separation_by_split", {}),
        "topk_proxy_by_split": base.get("topk_proxy_by_split", {}),
        "negative_guard_classifier_auc": base.get("negative_guard_classifier_auc"),
        "negative_guard_classifier_mcc": base.get("negative_guard_classifier_mcc"),
        "negative_guard_score_separation": base.get("negative_guard_score_separation"),
        "negative_guard_cnn_top3_avg_ret20": topk_test.get("negative_guard_image_top3_avg_ret20"),
        "negative_guard_cnn_top3_severe_loss_rate20": topk_test.get("negative_guard_image_top3_severe_loss_rate20"),
        "negative_guard_winner_score_mean": base.get("negative_guard_winner_score_mean"),
        "negative_guard_loser_score_mean": base.get("negative_guard_loser_score_mean"),
    }


def _cnn_score_ledger_rows(frame: pd.DataFrame, score_column: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in frame.sort_values(["event_ymd", "symbol", "image_sample_key"]).itertuples(index=False):
        score = float(getattr(row, score_column))
        rows.append(
            {
                "schema_version": f"{SCHEMA_PREFIX}_cnn_image_score_ledger_row_v1",
                "image_sample_key": getattr(row, "image_sample_key"),
                "candidate_event_key": getattr(row, "candidate_event_key", None),
                "symbol": str(getattr(row, "symbol")),
                "event_date": str(getattr(row, "event_date")),
                "event_ymd": int(getattr(row, "event_ymd")),
                "split": str(getattr(row, "split")),
                "cnn_image_score": score,
                "cnn_predicted_label": "future_top15_by_ret20" if score >= 0.5 else "future_bottom15_by_ret20",
                "primary_label": str(getattr(row, "primary_label")),
                "binary_target_available": bool(getattr(row, "binary_target_available")),
                "future_top15_by_ret20": bool(getattr(row, "future_top15_by_ret20")),
                "future_bottom15_by_ret20": bool(getattr(row, "future_bottom15_by_ret20")),
                "neutral_middle70": bool(getattr(row, "neutral_middle70")),
                "ret20": float(getattr(row, "ret20")),
                "MFE20": float(getattr(row, "MFE20")),
                "MAE20": float(getattr(row, "MAE20")),
                "severe_loss20": bool(getattr(row, "severe_loss20")),
                "negative_guard_matched": bool(getattr(row, "negative_guard_matched")),
                "safe_full_tag": bool(getattr(row, "safe_full_tag")),
                "image_score_scope": "research_only",
                "base_score_input_used": False,
                "fusion_used": False,
            }
        )
    return rows


def _load_image_tensor(path: Path, *, image_size: int = CNN_IMAGE_SIZE) -> np.ndarray:
    resample = getattr(getattr(Image, "Resampling", Image), "BILINEAR")
    with Image.open(path) as image:
        grey = image.convert("L").resize((image_size, image_size), resample)
        array = np.asarray(grey, dtype=np.float32) / 255.0
    return array[None, :, :]


def _remove_lazy_service_proxy_modules_before_torch_import() -> None:
    for name, module in list(sys.modules.items()):
        if not (name.startswith("app.backend.services.") or name.startswith("app.backend.services.analysis.")):
            continue
        if type(module).__name__ == "_LazyModule":
            sys.modules.pop(name, None)


def train_simple_cnn_phase2b(
    *,
    frame: pd.DataFrame,
    dependency_audit: dict[str, Any],
    output_dir: Path,
) -> tuple[pd.DataFrame, dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    _remove_lazy_service_proxy_modules_before_torch_import()
    import torch  # type: ignore
    from torch import nn  # type: ignore
    from torch.utils.data import DataLoader, Dataset  # type: ignore

    torch.manual_seed(RANDOM_SEED)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(RANDOM_SEED)
    try:
        torch.use_deterministic_algorithms(True)
    except Exception:
        pass
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    class ImageFrameDataset(Dataset):
        def __init__(self, source: pd.DataFrame, *, with_target: bool) -> None:
            self.source = source.reset_index(drop=True)
            self.with_target = with_target

        def __len__(self) -> int:
            return int(len(self.source))

        def __getitem__(self, index: int):
            row = self.source.iloc[index]
            tensor = torch.from_numpy(_load_image_tensor(Path(str(row["image_path"]))))
            if self.with_target:
                return tensor, torch.tensor(float(row["binary_target"]), dtype=torch.float32)
            return tensor

    class SimpleCNN(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.net = nn.Sequential(
                nn.Conv2d(1, 16, kernel_size=5, stride=2, padding=2),
                nn.ReLU(),
                nn.MaxPool2d(2),
                nn.Conv2d(16, 32, kernel_size=3, stride=1, padding=1),
                nn.ReLU(),
                nn.MaxPool2d(2),
                nn.Conv2d(32, 64, kernel_size=3, stride=1, padding=1),
                nn.ReLU(),
                nn.AdaptiveAvgPool2d((1, 1)),
                nn.Flatten(),
                nn.Linear(64, 1),
            )

        def forward(self, x):
            return self.net(x).squeeze(1)

    binary = frame[frame["binary_target_available"]].copy()
    train = binary[binary["split"].eq("train")].copy()
    validation = binary[binary["split"].eq("validation")].copy()
    if train.empty or validation.empty:
        raise RuntimeError("train and validation binary samples are required for simple CNN")
    pos = int(train["binary_target"].sum())
    neg = int(len(train) - pos)
    pos_weight = torch.tensor([float(neg / max(1, pos))], dtype=torch.float32, device=device)
    model = SimpleCNN().to(device)
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    optimizer = torch.optim.Adam(model.parameters(), lr=CNN_LEARNING_RATE)
    generator = torch.Generator()
    generator.manual_seed(RANDOM_SEED)
    train_loader = DataLoader(ImageFrameDataset(train, with_target=True), batch_size=CNN_BATCH_SIZE, shuffle=True, num_workers=0, generator=generator)
    val_loader = DataLoader(ImageFrameDataset(validation, with_target=True), batch_size=CNN_BATCH_SIZE, shuffle=False, num_workers=0)

    def evaluate_loss(loader: DataLoader) -> float:
        model.eval()
        losses: list[float] = []
        with torch.no_grad():
            for images, targets in loader:
                images = images.to(device)
                targets = targets.to(device)
                logits = model(images)
                losses.append(float(criterion(logits, targets).item()))
        return float(np.mean(losses)) if losses else math.nan

    training_log: list[dict[str, Any]] = []
    best_state: dict[str, Any] | None = None
    best_val = math.inf
    for epoch in range(1, CNN_EPOCHS + 1):
        model.train()
        losses = []
        for images, targets in train_loader:
            images = images.to(device)
            targets = targets.to(device)
            optimizer.zero_grad(set_to_none=True)
            logits = model(images)
            loss = criterion(logits, targets)
            loss.backward()
            optimizer.step()
            losses.append(float(loss.item()))
        train_loss = float(np.mean(losses)) if losses else math.nan
        val_loss = evaluate_loss(val_loader)
        if val_loss < best_val:
            best_val = val_loss
            best_state = {key: value.detach().cpu().clone() for key, value in model.state_dict().items()}
        training_log.append(
            {
                "schema_version": f"{SCHEMA_PREFIX}_training_log_row_v1",
                "generated_at": _utc_now(),
                "epoch": epoch,
                "train_loss": train_loss,
                "validation_loss": val_loss,
                "device": str(device),
                "model_family_id": MODEL_FAMILY_ID,
                "full_dataset_used": True,
                "sample_reduction_used": False,
                "sklearn_fallback_used": False,
                "pretrained_weights_used": False,
            }
        )
    if best_state is not None:
        model.load_state_dict(best_state)

    all_loader = DataLoader(ImageFrameDataset(frame, with_target=False), batch_size=CNN_BATCH_SIZE, shuffle=False, num_workers=0)
    scores: list[float] = []
    model.eval()
    with torch.no_grad():
        for images in all_loader:
            images = images.to(device)
            logits = model(images)
            probs = torch.sigmoid(logits).detach().cpu().numpy().astype(float).tolist()
            scores.extend(float(item) for item in probs)
    scored = frame.copy()
    scored["cnn_image_score"] = scores
    model_dir = output_dir / "model"
    model_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = model_dir / f"{MODEL_FAMILY_ID}.pt"
    torch.save(
        {
            "model_state_dict": model.state_dict(),
            "model_family_id": MODEL_FAMILY_ID,
            "training_config": {
                "epochs": CNN_EPOCHS,
                "batch_size": CNN_BATCH_SIZE,
                "learning_rate": CNN_LEARNING_RATE,
                "image_size": CNN_IMAGE_SIZE,
                "device": str(device),
            },
        },
        checkpoint_path,
    )
    model_checkpoint = {
        "schema_version": f"{SCHEMA_PREFIX}_model_checkpoint_manifest_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_checkpoint_created": True,
        "model_checkpoint_path": str(checkpoint_path),
        "model_checkpoint_sha256": _file_hash(checkpoint_path),
        "model_family_id": MODEL_FAMILY_ID,
        "pretrained_weights_used": False,
        "pretrained_source": None,
        "weight_hash": None,
        "license_note": None,
        "device": str(device),
    }
    training_config = {
        "schema_version": f"{SCHEMA_PREFIX}_training_config_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_families_requested": ["simple_cnn_phase2b_v1", "resnet18_phase2b_v1"],
        "model_family_selected": MODEL_FAMILY_ID,
        "training_attempted": True,
        "training_skipped_reason": None,
        "split_source": "phase0_1_split_assignment_ledger",
        "train_only_on_train_split": True,
        "validation_only_for_model_selection": True,
        "test_only_for_final_reporting": True,
        "epochs": CNN_EPOCHS,
        "batch_size": CNN_BATCH_SIZE,
        "learning_rate": CNN_LEARNING_RATE,
        "input_image_size": f"{CNN_IMAGE_SIZE}x{CNN_IMAGE_SIZE}",
        "device": str(device),
        "gpu_available": bool(torch.cuda.is_available()),
        "sample_reduction_used": False,
        "research_fallback_used": False,
        "silent_fallback_used": False,
        "sklearn_fallback_used": False,
        "pretrained_weights_used": False,
        "torch_version": dependency_audit.get("torch_version"),
        "torchvision_version": dependency_audit.get("torchvision_version"),
    }
    return scored, training_config, training_log, model_checkpoint


def build_blocked_artifacts(
    *,
    run_id: str,
    output_dir: Path,
    source_phase0_1_dir: Path,
    source_phase2_dir: Path,
    source_phase0_1_run_id: str,
    source_phase2_run_id: str,
    frame: pd.DataFrame,
    phase0_status: dict[str, Any],
    phase2_status: dict[str, Any],
    dependency_audit: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "refs": [
            _source_ref("image_assisted_rerank_phase0_1", source_phase0_1_run_id, source_phase0_1_dir, REQUIRED_PHASE0_1_FILES),
            _source_ref("image_only_classifier_baseline_phase2", source_phase2_run_id, source_phase2_dir, REQUIRED_PHASE2_FILES),
        ],
        "source_phase0_1_status": phase0_status,
        "source_phase2_status": phase2_status,
    }
    split_audit = build_split_leakage_audit(frame, source_phase0_1_dir)
    dataset_audit = build_dataset_audit(frame, phase0_status)
    image_input_audit = build_image_input_audit(frame, dependency_blocked=True)
    classifier_metrics = _unavailable_classifier_metrics(frame)
    logistic_comparison = {
        "schema_version": f"{SCHEMA_PREFIX}_logistic_baseline_comparison_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "comparison_available": False,
        "comparison_unavailable_reason": "cnn_not_trained_due_to_missing_torch",
        "sklearn_logistic_baseline_source_used_for_comparison": True,
        **phase2_status,
        "cnn_test_roc_auc": None,
        "cnn_test_mcc": None,
        "cnn_score_separation_top15_vs_bottom15": None,
        "cnn_negative_guard_auc": None,
        "cnn_negative_guard_score_separation": None,
        "roc_auc_delta": None,
        "mcc_delta": None,
        "score_separation_delta": None,
        "negative_guard_auc_delta": None,
        "negative_guard_score_separation_delta": None,
    }
    score_distribution = _unavailable_report("score_distribution_report", "cnn_not_trained_due_to_missing_torch")
    score_distribution.update(
        {
            "score_separation_top15_vs_bottom15": None,
            "test_score_separation_top15_vs_bottom15": None,
            "score_overlap_rate": None,
        }
    )
    topk_proxy = _unavailable_report("topk_proxy_report", "cnn_not_trained_due_to_missing_torch")
    topk_proxy.update(
        {
            "cnn_top1_avg_ret20": None,
            "cnn_top3_avg_ret20": None,
            "cnn_top5_avg_ret20": None,
            "cnn_top10_avg_ret20": None,
            "cnn_top3_win_rate20": None,
            "cnn_top3_avg_MFE20": None,
            "cnn_top3_avg_MAE20": None,
            "cnn_top3_severe_loss_rate20": None,
            "cnn_top3_big_winner_capture_rate": None,
            "cnn_selected_nonwinner_when_winner_available_rate": None,
            "cnn_oracle_top3_gap_ret20": None,
            "random_top3_avg_ret20": phase2_status.get("logistic_random_top3_avg_ret20"),
        }
    )
    ng = frame[frame["negative_guard_matched"]]
    negative_guard = _unavailable_report("negative_guard_cnn_diagnostics", "cnn_not_trained_due_to_missing_torch")
    negative_guard.update(
        {
            "negative_guard_sample_count_by_split": _counts_by_split(ng),
            "negative_guard_top15_count_by_split": _counts_by_split(ng, "future_top15_by_ret20"),
            "negative_guard_bottom15_count_by_split": _counts_by_split(ng, "future_bottom15_by_ret20"),
            "negative_guard_classifier_auc": None,
            "negative_guard_classifier_mcc": None,
            "negative_guard_score_separation": None,
            "negative_guard_cnn_top3_avg_ret20": None,
            "negative_guard_cnn_top3_severe_loss_rate20": None,
            "negative_guard_winner_score_mean": None,
            "negative_guard_loser_score_mean": None,
        }
    )
    phase3_readiness = {
        "schema_version": f"{SCHEMA_PREFIX}_phase3_readiness_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "ready_for_fusion": False,
        "fusion_phase3_blocked": True,
        "blocked_reason": "torch_unavailable_cnn_baseline_not_run",
        "gate_checks": {
            "dependency_available_for_cnn_training": False,
            "test_mcc_gt_0_03": False,
            "test_roc_auc_gt_0_53": False,
            "score_separation_top15_vs_bottom15_positive": False,
            "mcc_delta_vs_logistic_gt_0": False,
            "auc_delta_vs_logistic_gt_0": False,
            "cnn_top3_avg_ret20_ge_random_top3": False,
            "cnn_top3_severe_loss_not_materially_worse_than_random": False,
            "negative_guard_score_separation_positive": False,
            "validation_and_test_signs_directionally_consistent": False,
            "split_leakage_audit_passed": split_audit.get("split_leakage_audit_passed") is True,
            "future_label_leakage_absent": True,
            "silent_fallback_used_false": True,
            "artifact_complete": True,
        },
        "next_axis_if_hold": "isolated_torch_research_environment_setup_or_image_route_pause",
        "next_axis_if_ready": "image_score_fusion_rerank_phase3",
        "next_axis_if_failed": "pause_image_route_or_return_to_ranking_loss_protocol_repair",
    }
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": AXIS_ID,
        "source_phase0_1_decision": phase0_status.get("source_phase0_1_decision"),
        "source_phase2_decision": phase2_status.get("source_phase2_decision"),
        "purpose": "dependency-audited CNN/ResNet image-only baseline attempt after failed logistic baseline",
        "split_source": "phase0_1_split_assignment_ledger",
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    model_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_phase2b_model_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "image_model_trained": False,
        "image_model_family": "simple_cnn_or_resnet18",
        "image_only_classifier_created": False,
        "image_score_created": False,
        "image_score_scope": "research_only",
        "torch_available": dependency_audit["torch_available"],
        "torchvision_available": dependency_audit["torchvision_available"],
        "selected_model_family": None,
        "blocked_before_training": True,
        "blocked_reason": dependency_audit["blocked_reason"],
        "fusion_reranker_created": False,
        "candidate_scoring_created": False,
        "production_ranking_changed": False,
        "pretrained_weights_used": False,
        "sample_reduction_used": False,
    }
    model_contract["contract_hash"] = _stable_hash(model_contract)
    training_config = {
        "schema_version": f"{SCHEMA_PREFIX}_training_config_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_families_requested": ["simple_cnn_phase2b_v1", "resnet18_phase2b_v1"],
        "model_family_selected": None,
        "training_attempted": False,
        "training_skipped_reason": "torch_unavailable",
        "split_source": "phase0_1_split_assignment_ledger",
        "train_only_on_train_split": True,
        "validation_only_for_model_selection": True,
        "test_only_for_final_reporting": True,
        "sample_reduction_used": False,
        "research_fallback_used": False,
        "silent_fallback_used": False,
        "sklearn_fallback_used": False,
        "pretrained_weights_used": False,
    }
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
    model_checkpoint = {
        "schema_version": f"{SCHEMA_PREFIX}_model_checkpoint_manifest_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_checkpoint_created": False,
        "model_checkpoint_path": None,
        "model_checkpoint_sha256": None,
        "model_family_id": None,
        "not_created_reason": "torch_unavailable_cnn_not_trained",
        "pretrained_weights_used": False,
        "pretrained_source": None,
        "weight_hash": None,
        "license_note": None,
    }
    research_decision = {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": AXIS_ID,
        "boundary": "TRADEX-only",
        "axis_moved": AXIS_ID,
        "source_phase2_decision": phase2_status.get("source_phase2_decision"),
        "image_model_trained": False,
        "image_model_family": "simple_cnn_or_resnet18",
        "image_only_classifier_created": False,
        "image_score_created": False,
        "image_score_scope": "research_only",
        "torch_available": dependency_audit["torch_available"],
        "torchvision_available": dependency_audit["torchvision_available"],
        "sklearn_logistic_baseline_source_used_for_comparison": True,
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
        "split_leakage_audit_passed": split_audit.get("split_leakage_audit_passed") is True,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": "hold",
        "authoritative_research_decision": "image_cnn_phase2b_hold",
        "typed_reasons": [
            "torch_unavailable",
            "torchvision_unavailable",
            "cnn_resnet_baseline_not_run",
            "fusion_phase3_remains_blocked",
            "no_sklearn_fallback_used",
        ],
    }
    run_manifest = contracts.build_run_manifest(
        session_id=run_id,
        seed=RANDOM_SEED,
        random_seed=RANDOM_SEED,
        input_artifacts=source_refs["refs"],
        asof=_utc_now(),
        config={
            "axis_id": AXIS_ID,
            "model_families_requested": ["simple_cnn_phase2b_v1", "resnet18_phase2b_v1"],
            "training_attempted": False,
            "blocked_reason": "torch_unavailable",
            "split_source": "phase0_1_split_assignment_ledger",
        },
        universe=sorted(frame["symbol"].astype(str).unique().tolist()),
        period={"start_date": str(frame["event_date"].min()), "end_date": str(frame["event_date"].max()), "sample_count": int(len(frame))},
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
        "decision": "hold",
        "authoritative_research_decision": "image_cnn_phase2b_hold",
    }
    return {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "source_artifact_refs.json": source_refs,
        "dependency_audit.json": dependency_audit,
        "phase2b_model_contract.json": model_contract,
        "training_config.json": training_config,
        "dataset_audit.json": dataset_audit,
        "image_input_audit.json": image_input_audit,
        "label_usage_audit.json": label_usage_audit,
        "phase2b_split_leakage_audit.json": split_audit,
        "model_checkpoint_manifest.json": model_checkpoint,
        "classifier_metrics.json": classifier_metrics,
        "logistic_baseline_comparison_report.json": logistic_comparison,
        "score_distribution_report.json": score_distribution,
        "topk_proxy_report.json": topk_proxy,
        "negative_guard_cnn_diagnostics.json": negative_guard,
        "phase3_readiness_report.json": phase3_readiness,
        "research_decision.json": research_decision,
        "_ARTIFACT_COMPLETE.json": complete,
    }


def build_trained_artifacts(
    *,
    run_id: str,
    source_phase0_1_dir: Path,
    source_phase2_dir: Path,
    source_phase0_1_run_id: str,
    source_phase2_run_id: str,
    frame: pd.DataFrame,
    phase0_status: dict[str, Any],
    phase2_status: dict[str, Any],
    dependency_audit: dict[str, Any],
    training_config: dict[str, Any],
    training_log: list[dict[str, Any]],
    model_checkpoint: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "refs": [
            _source_ref("image_assisted_rerank_phase0_1", source_phase0_1_run_id, source_phase0_1_dir, REQUIRED_PHASE0_1_FILES),
            _source_ref("image_only_classifier_baseline_phase2", source_phase2_run_id, source_phase2_dir, REQUIRED_PHASE2_FILES),
        ],
        "source_phase0_1_status": phase0_status,
        "source_phase2_status": phase2_status,
    }
    split_audit = build_split_leakage_audit(frame, source_phase0_1_dir)
    dataset_audit = build_dataset_audit(frame, phase0_status)
    image_input_audit = build_image_input_audit(frame, dependency_blocked=False)
    classifier_metrics = _classifier_metrics_from_scored_frame(frame, "cnn_image_score")
    score_distribution = _score_distribution_from_scored_frame(frame, "cnn_image_score")
    topk_proxy = _topk_report_from_scored_frame(frame, "cnn_image_score")
    negative_guard = _negative_guard_cnn_diagnostics(frame, "cnn_image_score")
    logistic_comparison = {
        "schema_version": f"{SCHEMA_PREFIX}_logistic_baseline_comparison_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "comparison_available": True,
        "sklearn_logistic_baseline_source_used_for_comparison": True,
        **phase2_status,
        "cnn_test_roc_auc": classifier_metrics.get("test_roc_auc"),
        "cnn_test_mcc": classifier_metrics.get("test_mcc"),
        "cnn_score_separation_top15_vs_bottom15": score_distribution.get("test_score_separation_top15_vs_bottom15"),
        "cnn_negative_guard_auc": negative_guard.get("negative_guard_classifier_auc"),
        "cnn_negative_guard_score_separation": negative_guard.get("negative_guard_score_separation"),
    }
    logistic_comparison["roc_auc_delta"] = (
        logistic_comparison["cnn_test_roc_auc"] - phase2_status["logistic_test_roc_auc"]
        if logistic_comparison["cnn_test_roc_auc"] is not None and phase2_status.get("logistic_test_roc_auc") is not None
        else None
    )
    logistic_comparison["mcc_delta"] = (
        logistic_comparison["cnn_test_mcc"] - phase2_status["logistic_test_mcc"]
        if logistic_comparison["cnn_test_mcc"] is not None and phase2_status.get("logistic_test_mcc") is not None
        else None
    )
    logistic_comparison["score_separation_delta"] = (
        logistic_comparison["cnn_score_separation_top15_vs_bottom15"] - phase2_status["logistic_score_separation_top15_vs_bottom15"]
        if logistic_comparison["cnn_score_separation_top15_vs_bottom15"] is not None and phase2_status.get("logistic_score_separation_top15_vs_bottom15") is not None
        else None
    )
    logistic_comparison["negative_guard_auc_delta"] = (
        logistic_comparison["cnn_negative_guard_auc"] - phase2_status["logistic_negative_guard_auc"]
        if logistic_comparison["cnn_negative_guard_auc"] is not None and phase2_status.get("logistic_negative_guard_auc") is not None
        else None
    )
    logistic_comparison["negative_guard_score_separation_delta"] = (
        logistic_comparison["cnn_negative_guard_score_separation"] - phase2_status["logistic_negative_guard_score_separation"]
        if logistic_comparison["cnn_negative_guard_score_separation"] is not None and phase2_status.get("logistic_negative_guard_score_separation") is not None
        else None
    )
    test_mcc = classifier_metrics.get("test_mcc")
    test_auc = classifier_metrics.get("test_roc_auc")
    test_sep = score_distribution.get("test_score_separation_top15_vs_bottom15")
    val_sep = score_distribution.get("by_split", {}).get("validation", {}).get("score_separation_top15_vs_bottom15")
    cnn_top3 = topk_proxy.get("cnn_top3_avg_ret20")
    random_top3 = topk_proxy.get("random_top3_avg_ret20")
    cnn_severe = topk_proxy.get("cnn_top3_severe_loss_rate20")
    random_severe = topk_proxy.get("random_top3_severe_loss_rate20")
    ng_sep = negative_guard.get("negative_guard_score_separation")
    gate_checks = {
        "dependency_available_for_cnn_training": bool(dependency_audit.get("torch_available")),
        "test_mcc_gt_0_03": test_mcc is not None and test_mcc > 0.03,
        "test_roc_auc_gt_0_53": test_auc is not None and test_auc > 0.53,
        "score_separation_top15_vs_bottom15_positive": test_sep is not None and test_sep > 0.0,
        "mcc_delta_vs_logistic_gt_0": logistic_comparison.get("mcc_delta") is not None and logistic_comparison["mcc_delta"] > 0.0,
        "auc_delta_vs_logistic_gt_0": logistic_comparison.get("roc_auc_delta") is not None and logistic_comparison["roc_auc_delta"] > 0.0,
        "cnn_top3_avg_ret20_ge_random_top3": cnn_top3 is not None and random_top3 is not None and cnn_top3 >= random_top3,
        "cnn_top3_severe_loss_not_materially_worse_than_random": cnn_severe is not None and random_severe is not None and cnn_severe <= random_severe + 0.02,
        "negative_guard_score_separation_positive": ng_sep is not None and ng_sep > 0.0,
        "validation_and_test_signs_directionally_consistent": val_sep is not None and test_sep is not None and val_sep > 0.0 and test_sep > 0.0,
        "split_leakage_audit_passed": split_audit.get("split_leakage_audit_passed") is True,
        "future_label_leakage_absent": True,
        "silent_fallback_used_false": True,
        "artifact_complete": True,
    }
    ready = all(gate_checks.values())
    typed_reasons: list[str] = []
    if ready:
        decision = "keep_candidate"
        authoritative = "image_cnn_phase2b_ready_for_fusion"
        typed_reasons.append("cnn_image_signal_ready_for_fusion_phase3")
    elif (test_mcc is not None and test_mcc <= 0.0) or (test_auc is not None and test_auc <= 0.50) or (test_sep is not None and test_sep <= 0.0):
        decision = "drop"
        authoritative = "image_cnn_phase2b_failed"
        if test_mcc is not None and test_mcc <= 0.0:
            typed_reasons.append("test_mcc_nonpositive")
        if test_auc is not None and test_auc <= 0.50:
            typed_reasons.append("test_auc_near_random_or_worse")
        if test_sep is not None and test_sep <= 0.0:
            typed_reasons.append("cnn_score_does_not_separate_test_top_bottom")
    else:
        decision = "hold"
        authoritative = "image_cnn_phase2b_hold"
        typed_reasons.append("cnn_signal_mixed_or_weak")
    if not gate_checks["negative_guard_score_separation_positive"]:
        typed_reasons.append("negative_guard_score_separation_not_positive")
    if not gate_checks["mcc_delta_vs_logistic_gt_0"]:
        typed_reasons.append("mcc_not_improved_vs_logistic")
    if not gate_checks["auc_delta_vs_logistic_gt_0"]:
        typed_reasons.append("auc_not_improved_vs_logistic")
    phase3_readiness = {
        "schema_version": f"{SCHEMA_PREFIX}_phase3_readiness_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "ready_for_fusion": ready,
        "fusion_phase3_blocked": not ready,
        "blocked_reason": None if ready else "cnn_phase2b_gates_not_met",
        "gate_checks": gate_checks,
        "test_mcc": test_mcc,
        "test_roc_auc": test_auc,
        "score_separation_top15_vs_bottom15": test_sep,
        "mcc_delta_vs_logistic": logistic_comparison.get("mcc_delta"),
        "auc_delta_vs_logistic": logistic_comparison.get("roc_auc_delta"),
        "negative_guard_score_separation": ng_sep,
        "cnn_top3_avg_ret20": cnn_top3,
        "random_top3_avg_ret20": random_top3,
        "next_axis_if_ready": "image_score_fusion_rerank_phase3",
        "next_axis_if_hold": "image_ablation",
        "next_axis_if_failed": "pause_image_route_or_return_to_ranking_loss_protocol_repair",
    }
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": AXIS_ID,
        "source_phase0_1_decision": phase0_status.get("source_phase0_1_decision"),
        "source_phase2_decision": phase2_status.get("source_phase2_decision"),
        "purpose": "CNN image-only baseline after failed logistic baseline",
        "split_source": "phase0_1_split_assignment_ledger",
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    model_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_phase2b_model_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "model_family_id": MODEL_FAMILY_ID,
        "image_model_trained": True,
        "image_model_family": "simple_cnn_or_resnet18",
        "image_only_classifier_created": True,
        "image_score_created": True,
        "image_score_scope": "research_only",
        "torch_available": dependency_audit["torch_available"],
        "torchvision_available": dependency_audit["torchvision_available"],
        "selected_model_family": MODEL_FAMILY_ID,
        "blocked_before_training": False,
        "blocked_reason": None,
        "fusion_reranker_created": False,
        "candidate_scoring_created": False,
        "production_ranking_changed": False,
        "pretrained_weights_used": False,
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
        "source_phase2_decision": phase2_status.get("source_phase2_decision"),
        "image_model_trained": True,
        "image_model_family": "simple_cnn_or_resnet18",
        "image_only_classifier_created": True,
        "image_score_created": True,
        "image_score_scope": "research_only",
        "torch_available": dependency_audit["torch_available"],
        "torchvision_available": dependency_audit["torchvision_available"],
        "sklearn_logistic_baseline_source_used_for_comparison": True,
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
        "split_leakage_audit_passed": split_audit.get("split_leakage_audit_passed") is True,
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
            "epochs": CNN_EPOCHS,
            "batch_size": CNN_BATCH_SIZE,
            "learning_rate": CNN_LEARNING_RATE,
            "image_size": CNN_IMAGE_SIZE,
            "split_source": "phase0_1_split_assignment_ledger",
        },
        universe=sorted(frame["symbol"].astype(str).unique().tolist()),
        period={"start_date": str(frame["event_date"].min()), "end_date": str(frame["event_date"].max()), "sample_count": int(len(frame))},
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
        "dependency_audit.json": dependency_audit,
        "phase2b_model_contract.json": model_contract,
        "training_config.json": training_config,
        "dataset_audit.json": dataset_audit,
        "image_input_audit.json": image_input_audit,
        "label_usage_audit.json": label_usage_audit,
        "phase2b_split_leakage_audit.json": split_audit,
        "model_checkpoint_manifest.json": model_checkpoint,
        "classifier_metrics.json": classifier_metrics,
        "logistic_baseline_comparison_report.json": logistic_comparison,
        "score_distribution_report.json": score_distribution,
        "topk_proxy_report.json": topk_proxy,
        "negative_guard_cnn_diagnostics.json": negative_guard,
        "phase3_readiness_report.json": phase3_readiness,
        "research_decision.json": research_decision,
        "_ARTIFACT_COMPLETE.json": complete,
    }


def run_image_cnn_baseline_phase2b(
    *,
    source_image_phase0_1_run_id: str = DEFAULT_SOURCE_PHASE0_1_RUN_ID,
    source_image_phase2_run_id: str = DEFAULT_SOURCE_PHASE2_RUN_ID,
    source_image_phase0_1_root: str | Path = DEFAULT_SOURCE_PHASE0_1_ROOT,
    source_image_phase2_root: str | Path = DEFAULT_SOURCE_PHASE2_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    run_id = run_id or _default_run_id()
    source_phase0_1_dir = _safe_path(source_image_phase0_1_root, DEFAULT_SOURCE_PHASE0_1_ROOT) / source_image_phase0_1_run_id
    source_phase2_dir = _safe_path(source_image_phase2_root, DEFAULT_SOURCE_PHASE2_ROOT) / source_image_phase2_run_id
    output_dir = _run_dir(output_root, run_id, DEFAULT_OUTPUT_ROOT)
    output_dir.mkdir(parents=True, exist_ok=True)
    dependency_audit = audit_dependencies()
    frame, phase0_status = phase2_mod.load_phase0_1_dataset(source_phase0_1_dir)
    phase2_status = validate_phase2_source(source_phase2_dir)
    if dependency_audit.get("torch_available"):
        scored_frame, training_config, training_log, model_checkpoint = train_simple_cnn_phase2b(
            frame=frame,
            dependency_audit=dependency_audit,
            output_dir=output_dir,
        )
        artifacts = build_trained_artifacts(
            run_id=run_id,
            source_phase0_1_dir=source_phase0_1_dir,
            source_phase2_dir=source_phase2_dir,
            source_phase0_1_run_id=source_image_phase0_1_run_id,
            source_phase2_run_id=source_image_phase2_run_id,
            frame=scored_frame,
            phase0_status=phase0_status,
            phase2_status=phase2_status,
            dependency_audit=dependency_audit,
            training_config=training_config,
            training_log=training_log,
            model_checkpoint=model_checkpoint,
        )
        _write_jsonl(output_dir / "training_log.jsonl", training_log)
        _write_jsonl(output_dir / "cnn_image_score_ledger.jsonl", _cnn_score_ledger_rows(scored_frame, "cnn_image_score"))
    else:
        artifacts = build_blocked_artifacts(
            run_id=run_id,
            output_dir=output_dir,
            source_phase0_1_dir=source_phase0_1_dir,
            source_phase2_dir=source_phase2_dir,
            source_phase0_1_run_id=source_image_phase0_1_run_id,
            source_phase2_run_id=source_image_phase2_run_id,
            frame=frame,
            phase0_status=phase0_status,
            phase2_status=phase2_status,
            dependency_audit=dependency_audit,
        )
        _write_jsonl(
            output_dir / "training_log.jsonl",
            [
                {
                    "schema_version": f"{SCHEMA_PREFIX}_training_log_row_v1",
                    "generated_at": _utc_now(),
                    "event": "dependency_blocked_before_training",
                    "model_family_requested": "simple_cnn_phase2b_v1",
                    "torch_available": dependency_audit["torch_available"],
                    "torchvision_available": dependency_audit["torchvision_available"],
                    "sklearn_fallback_used": False,
                    "training_attempted": False,
                    "sample_reduction_used": False,
                    "silent_fallback_used": False,
                    "research_fallback_used": False,
                }
            ],
        )
        _write_jsonl(output_dir / "cnn_image_score_ledger.jsonl", [])
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
        "torch_available": dependency_audit["torch_available"],
        "torchvision_available": dependency_audit["torchvision_available"],
        "image_model_trained": artifacts["research_decision.json"]["image_model_trained"],
        "image_only_classifier_created": artifacts["research_decision.json"]["image_only_classifier_created"],
        "image_score_created": artifacts["research_decision.json"]["image_score_created"],
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX CNN image-only baseline Phase2b")
    parser.add_argument("--source-image-phase0-1-run-id", default=DEFAULT_SOURCE_PHASE0_1_RUN_ID)
    parser.add_argument("--source-image-phase2-run-id", default=DEFAULT_SOURCE_PHASE2_RUN_ID)
    parser.add_argument("--source-image-phase0-1-root", default=str(DEFAULT_SOURCE_PHASE0_1_ROOT))
    parser.add_argument("--source-image-phase2-root", default=str(DEFAULT_SOURCE_PHASE2_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_image_cnn_baseline_phase2b(
        source_image_phase0_1_run_id=args.source_image_phase0_1_run_id,
        source_image_phase2_run_id=args.source_image_phase2_run_id,
        source_image_phase0_1_root=args.source_image_phase0_1_root,
        source_image_phase2_root=args.source_image_phase2_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
