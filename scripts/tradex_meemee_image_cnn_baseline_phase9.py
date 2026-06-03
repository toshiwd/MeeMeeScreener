from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image
from sklearn.metrics import accuracy_score, matthews_corrcoef, roc_auc_score


AXIS_ID = "meemee_image_cnn_baseline_phase9"
DEFAULT_PHASE7_DIR = Path(r"G:\Tradex\meemee_training_preparation_phase7\20260602T080906Z-meemee_training_preparation_phase7")
DEFAULT_PHASE8_DIR = Path(r"G:\Tradex\meemee_image_linear_baseline_phase8\20260602T081223Z-meemee_image_linear_baseline_phase8")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_image_cnn_baseline_phase9")
IMAGE_SIZE = 64
BATCH_SIZE = 128
EPOCHS = 4
LEARNING_RATE = 0.001
RANDOM_SEED = 20260602


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _target(row: dict[str, Any]) -> int | None:
    if row["future_top15_by_ret20"]:
        return 1
    if row["future_bottom15_by_ret20"]:
        return 0
    return None


def _progress(path: Path, **payload: Any) -> None:
    _write_json(path, {"schema_version": "tradex_meemee_image_cnn_baseline_phase9_progress_v1", "generated_at": _utc_now(), **payload})


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    binary = [row for row in rows if row["target"] is not None]
    y = np.asarray([row["target"] for row in binary], dtype=np.int64)
    score = np.asarray([row["cnn_image_score"] for row in binary], dtype=np.float64)
    pred = (score >= 0.5).astype(np.int64)
    return {
        "image_count": len(rows), "binary_image_count": len(binary),
        "positive_image_count": int((y == 1).sum()), "negative_image_count": int((y == 0).sum()),
        "neutral_image_count": len(rows) - len(binary),
        "roc_auc": float(roc_auc_score(y, score)) if len(set(y.tolist())) == 2 else None,
        "accuracy": float(accuracy_score(y, pred)) if len(y) else None,
        "mcc": float(matthews_corrcoef(y, pred)) if len(set(y.tolist())) == 2 else None,
    }


def run(*, phase7_dir: Path, phase8_dir: Path, output_root: Path, epochs: int = EPOCHS, batch_size: int = BATCH_SIZE) -> Path:
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, Dataset

    if _read_json(phase7_dir / "phase7_audit.json").get("ready_for_model_training") is not True:
        raise RuntimeError("Phase 7 source is not ready for model training")
    phase8_metrics = _read_json(phase8_dir / "classifier_metrics.json")
    manifest = _read_jsonl(phase7_dir / "training_manifest.jsonl")
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    progress_path = output_dir / "phase9_progress.json"
    torch.manual_seed(RANDOM_SEED)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    resample = getattr(getattr(Image, "Resampling", Image), "BILINEAR")

    class Images(Dataset):
        def __init__(self, rows: list[dict[str, Any]], with_target: bool) -> None:
            self.rows, self.with_target = rows, with_target

        def __len__(self) -> int:
            return len(self.rows)

        def __getitem__(self, index: int):
            row = self.rows[index]
            with Image.open(row["image_path"]) as image:
                arr = np.asarray(image.convert("L").resize((IMAGE_SIZE, IMAGE_SIZE), resample), dtype=np.float32) / 255.0
            tensor = torch.from_numpy(arr[None, :, :])
            return (tensor, torch.tensor(float(_target(row)), dtype=torch.float32)) if self.with_target else tensor

    class SimpleCNN(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.net = nn.Sequential(
                nn.Conv2d(1, 16, 5, stride=2, padding=2), nn.ReLU(), nn.MaxPool2d(2),
                nn.Conv2d(16, 32, 3, padding=1), nn.ReLU(), nn.MaxPool2d(2),
                nn.Conv2d(32, 64, 3, padding=1), nn.ReLU(), nn.AdaptiveAvgPool2d((1, 1)),
                nn.Flatten(), nn.Linear(64, 1),
            )

        def forward(self, x):
            return self.net(x).squeeze(1)

    train = [row for row in manifest if row["split"] == "train" and _target(row) is not None]
    evaluation = [row for row in manifest if row["split"] != "embargo"]
    loader = DataLoader(Images(train, True), batch_size=batch_size, shuffle=True, num_workers=0)
    model = SimpleCNN().to(device)
    criterion = nn.BCEWithLogitsLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)
    total_steps = epochs * len(loader)
    step = 0
    for epoch in range(1, epochs + 1):
        model.train()
        for images, targets in loader:
            images, targets = images.to(device), targets.to(device)
            optimizer.zero_grad(set_to_none=True)
            loss = criterion(model(images), targets)
            loss.backward()
            optimizer.step()
            step += 1
            _progress(progress_path, stage="training", epoch=epoch, epochs=epochs, step=step, total_steps=total_steps, progress_ratio=round(step / total_steps, 8), loss=float(loss.item()), device=str(device))
    checkpoint = output_dir / "model.pt"
    torch.save({"state_dict": model.state_dict(), "image_size": IMAGE_SIZE, "epochs": epochs}, checkpoint)
    scored: list[dict[str, Any]] = []
    eval_loader = DataLoader(Images(evaluation, False), batch_size=batch_size, shuffle=False, num_workers=0)
    model.eval()
    processed = 0
    with torch.no_grad():
        for images in eval_loader:
            scores = torch.sigmoid(model(images.to(device))).cpu().numpy().astype(float).tolist()
            for row, score in zip(evaluation[processed:processed + len(scores)], scores):
                scored.append({"image_sample_key": row["image_sample_key"], "code": row["code"], "as_of": row["as_of"], "scale": row["scale"], "split": row["split"], "target": _target(row), "ret20": row["ret20"], "cnn_image_score": score})
            processed += len(scores)
            _progress(progress_path, stage="evaluation", processed_image_count=processed, total_image_count=len(evaluation), progress_ratio=round(processed / len(evaluation), 8), device=str(device))
    metrics = {split: _metrics([row for row in scored if row["split"] == split]) for split in ("train", "validation", "test")}
    test_auc, test_mcc = metrics["test"]["roc_auc"], metrics["test"]["mcc"]
    linear_auc, linear_mcc = phase8_metrics["test_roc_auc"], phase8_metrics["test_mcc"]
    keep = test_auc is not None and test_mcc is not None and test_auc > linear_auc and test_mcc > linear_mcc and metrics["validation"]["mcc"] is not None and metrics["validation"]["mcc"] > 0
    decision = "keep_candidate" if keep else "drop"
    authoritative = "keep_phase9_cnn_for_next_comparison" if keep else "drop_phase9_cnn_no_stable_edge_vs_linear"
    with (output_dir / "cnn_image_score_ledger.jsonl").open("w", encoding="utf-8") as handle:
        for row in scored:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    _write_json(output_dir / "classifier_metrics.json", {"schema_version": "tradex_meemee_image_cnn_baseline_phase9_metrics_v1", "by_split": metrics, "test_roc_auc": test_auc, "test_mcc": test_mcc})
    _write_json(output_dir / "linear_comparison.json", {"phase8_test_roc_auc": linear_auc, "phase8_test_mcc": linear_mcc, "phase9_test_roc_auc": test_auc, "phase9_test_mcc": test_mcc, "test_roc_auc_delta": test_auc - linear_auc, "test_mcc_delta": test_mcc - linear_mcc})
    audit = {"schema_version": "tradex_meemee_image_cnn_baseline_phase9_audit_v1", "generated_at": _utc_now(), "boundary_owner": "TRADEX", "phase7_dir": str(phase7_dir), "phase8_dir": str(phase8_dir), "model_family": "torch.SimpleCNN", "device": str(device), "epochs": epochs, "batch_size": batch_size, "full_dataset_used": True, "sample_reduction_used": False, "future_labels_used_as_training_targets_only": True, "silent_fallback_used": False, "research_fallback_used": False, "decision": decision, "authoritative_research_decision": authoritative, "production_ranking_changed": False, "runtime_db_write": False, "meemee_unchanged": True, "model_checkpoint_sha256": _sha256(checkpoint)}
    _write_json(output_dir / "phase9_audit.json", audit)
    _write_json(output_root / "phase9_latest_audit.json", audit)
    _progress(progress_path, stage="completed", processed_image_count=len(evaluation), total_image_count=len(evaluation), progress_ratio=1.0, decision=decision)
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase7-dir", type=Path, default=DEFAULT_PHASE7_DIR)
    parser.add_argument("--phase8-dir", type=Path, default=DEFAULT_PHASE8_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--epochs", type=int, default=EPOCHS)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()
    print(run(phase7_dir=args.phase7_dir, phase8_dir=args.phase8_dir, output_root=args.output_root, epochs=args.epochs, batch_size=args.batch_size))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
