from __future__ import annotations

import json
from pathlib import Path

from PIL import Image

from scripts.tradex_meemee_training_preparation_phase7 import prepare


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_phase7_prepares_verified_training_manifest_without_training(tmp_path: Path) -> None:
    phase3 = tmp_path / "phase3"
    export = tmp_path / "export"
    phase3.mkdir()
    export.mkdir()
    labels = []
    splits = []
    plan = []
    for index, split in enumerate(("train", "validation", "test", "embargo")):
        key = f"sample-{index}"
        labels.append({
            "image_sample_key": key,
            "ret5": 0.01,
            "ret10": 0.02,
            "ret20": 0.03,
            "MFE20": 0.04,
            "MAE20": -0.01,
            "future_top15_by_ret20": index == 0,
            "future_bottom15_by_ret20": index == 1,
            "neutral_middle70": index >= 2,
        })
        splits.append({"image_sample_key": key, "split": split})
        relpath = f"browser_reference_images/{key}.png"
        plan.append({"image_sample_key": key, "code": str(1000 + index), "as_of": 20250101 + index, "scale": "micro", "bars": 30, "image_relpath": relpath})
        path = export / relpath
        path.parent.mkdir(exist_ok=True)
        Image.new("RGB", (4, 4), (index, index, index)).save(path)
    _write_json(phase3 / "phase3_audit.json", {"authoritative_readiness": {"ready_for_model_training_preparation": True}})
    for name in ("split_leakage_audit.json", "class_balance_audit.json", "data_integrity_audit.json"):
        _write_json(phase3 / name, {})
    _write_jsonl(phase3 / "label_ledger.jsonl", labels)
    _write_jsonl(phase3 / "split_assignment_ledger.jsonl", splits)
    _write_jsonl(phase3 / "canonical_image_export_plan.jsonl", plan)
    _write_json(export / "canonical_export_progress_audit.json", {
        "ready_for_model_training": True,
        "remaining_image_count": 0,
        "exported_image_count": 4,
        "unique_exported_hash_count": 4,
    })

    output = prepare(phase3_dir=phase3, export_root=export, output_root=tmp_path / "out")
    audit = json.loads((output / "phase7_audit.json").read_text(encoding="utf-8"))
    manifest = [json.loads(line) for line in (output / "training_manifest.jsonl").read_text(encoding="utf-8").splitlines()]

    assert audit["training_preparation_audit_passed"] is True
    assert audit["ready_for_model_training"] is True
    assert audit["model_training_executed"] is False
    assert audit["missing_image_count"] == 0
    assert len(manifest) == 4
