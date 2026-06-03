from __future__ import annotations

import argparse
import json
import pickle
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from PIL import Image
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score, matthews_corrcoef, roc_auc_score


AXIS_ID = "meemee_image_linear_baseline_phase8"
DEFAULT_PHASE7_DIR = Path(r"G:\Tradex\meemee_training_preparation_phase7\20260602T080906Z-meemee_training_preparation_phase7")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_image_linear_baseline_phase8")
IMAGE_SIZE = 16
BATCH_SIZE = 512
RANDOM_SEED = 20260602


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _chunks(rows: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


def _target(row: dict[str, Any]) -> int | None:
    if row["future_top15_by_ret20"]:
        return 1
    if row["future_bottom15_by_ret20"]:
        return 0
    return None


def _features(rows: list[dict[str, Any]]) -> np.ndarray:
    resample = getattr(getattr(Image, "Resampling", Image), "BILINEAR")
    values: list[np.ndarray] = []
    for row in rows:
        with Image.open(row["image_path"]) as image:
            array = np.asarray(image.convert("L").resize((IMAGE_SIZE, IMAGE_SIZE), resample), dtype=np.float32)
        values.append(array.reshape(-1) / 255.0)
    return np.vstack(values).astype(np.float32)


def _progress(path: Path, *, stage: str, processed: int, total: int, extra: dict[str, Any] | None = None) -> None:
    payload = {
        "schema_version": "tradex_meemee_image_linear_baseline_phase8_progress_v1",
        "generated_at": _utc_now(),
        "stage": stage,
        "processed_image_count": processed,
        "total_image_count": total,
        "progress_ratio": round(processed / total, 8) if total else 0.0,
    }
    payload.update(extra or {})
    _write_json(path, payload)


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    binary = [row for row in rows if row["target"] is not None]
    y = np.asarray([row["target"] for row in binary], dtype=np.int64)
    score = np.asarray([row["image_score"] for row in binary], dtype=np.float64)
    pred = (score >= 0.5).astype(np.int64)
    return {
        "image_count": len(rows),
        "binary_image_count": len(binary),
        "positive_image_count": int((y == 1).sum()),
        "negative_image_count": int((y == 0).sum()),
        "neutral_image_count": len(rows) - len(binary),
        "roc_auc": float(roc_auc_score(y, score)) if len(set(y.tolist())) == 2 else None,
        "accuracy": float(accuracy_score(y, pred)) if len(y) else None,
        "mcc": float(matthews_corrcoef(y, pred)) if len(set(y.tolist())) == 2 else None,
    }


def run(*, phase7_dir: Path, output_root: Path, batch_size: int = BATCH_SIZE) -> Path:
    phase7 = _read_json(phase7_dir / "phase7_audit.json")
    if phase7.get("ready_for_model_training") is not True:
        raise RuntimeError("Phase 7 source is not ready for model training")
    manifest = _read_jsonl(phase7_dir / "training_manifest.jsonl")
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    progress_path = output_dir / "phase8_progress.json"
    training = [row for row in manifest if row["split"] == "train" and _target(row) is not None]
    model = SGDClassifier(loss="log_loss", random_state=RANDOM_SEED)
    processed = 0
    for batch_index, batch in enumerate(_chunks(training, batch_size)):
        x = _features(batch)
        y = np.asarray([_target(row) for row in batch], dtype=np.int64)
        model.partial_fit(x, y, classes=np.asarray([0, 1], dtype=np.int64))
        processed += len(batch)
        _progress(progress_path, stage="training", processed=processed, total=len(training), extra={"batch_index": batch_index})
    with (output_dir / "model.pkl").open("wb") as handle:
        pickle.dump(model, handle)

    evaluated: list[dict[str, Any]] = []
    eval_rows = [row for row in manifest if row["split"] != "embargo"]
    processed = 0
    for batch_index, batch in enumerate(_chunks(eval_rows, batch_size)):
        scores = model.predict_proba(_features(batch))[:, 1]
        for row, score in zip(batch, scores):
            evaluated.append({
                "image_sample_key": row["image_sample_key"],
                "code": row["code"],
                "as_of": row["as_of"],
                "scale": row["scale"],
                "split": row["split"],
                "target": _target(row),
                "ret20": row["ret20"],
                "image_score": float(score),
            })
        processed += len(batch)
        _progress(progress_path, stage="evaluation", processed=processed, total=len(eval_rows), extra={"batch_index": batch_index})
    metrics = {split: _metrics([row for row in evaluated if row["split"] == split]) for split in ("train", "validation", "test")}
    test_auc = metrics["test"]["roc_auc"]
    test_mcc = metrics["test"]["mcc"]
    if test_auc is not None and test_auc > 0.53 and test_mcc is not None and test_mcc > 0.03:
        decision = "keep_candidate"
        authoritative = "keep_phase8_image_linear_baseline_for_cnn_comparison"
    elif test_auc is not None and test_auc <= 0.50:
        decision = "drop"
        authoritative = "drop_phase8_image_linear_baseline_no_edge"
    else:
        decision = "hold"
        authoritative = "hold_phase8_image_linear_baseline_weak_or_mixed"
    with (output_dir / "image_score_ledger.jsonl").open("w", encoding="utf-8") as handle:
        for row in evaluated:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    _write_json(output_dir / "classifier_metrics.json", {
        "schema_version": "tradex_meemee_image_linear_baseline_phase8_metrics_v1",
        "by_split": metrics,
        "test_roc_auc": test_auc,
        "test_mcc": test_mcc,
    })
    audit = {
        "schema_version": "tradex_meemee_image_linear_baseline_phase8_audit_v1",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "phase7_dir": str(phase7_dir),
        "model_family": "sklearn.SGDClassifier(loss=log_loss)",
        "input_mode": f"canonical_image_grayscale_{IMAGE_SIZE}x{IMAGE_SIZE}",
        "full_dataset_used": True,
        "sample_reduction_used": False,
        "training_binary_image_count": len(training),
        "evaluated_non_embargo_image_count": len(evaluated),
        "future_labels_used_as_training_targets_only": True,
        "future_labels_used_in_image_rendering": False,
        "future_labels_used_as_inference_inputs": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "model_training_executed": True,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "next_axis_if_keep_or_hold": "cnn_image_only_baseline_comparison",
        "next_axis_if_drop": "close_image_linear_branch_then_decide_independent_axis",
    }
    _write_json(output_dir / "phase8_audit.json", audit)
    _write_json(output_root / "phase8_latest_audit.json", audit)
    _progress(progress_path, stage="completed", processed=len(eval_rows), total=len(eval_rows), extra={"decision": decision})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase7-dir", type=Path, default=DEFAULT_PHASE7_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()
    print(run(phase7_dir=args.phase7_dir, output_root=args.output_root, batch_size=args.batch_size))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
