from __future__ import annotations

import json
from pathlib import Path

from PIL import Image

from scripts.tradex_meemee_image_cnn_baseline_phase9 import run


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def test_phase9_trains_cnn_and_compares_phase8_without_production_mutation(tmp_path: Path) -> None:
    phase7, phase8 = tmp_path / "phase7", tmp_path / "phase8"
    phase7.mkdir()
    phase8.mkdir()
    _write_json(phase7 / "phase7_audit.json", {"ready_for_model_training": True})
    _write_json(phase8 / "classifier_metrics.json", {"test_roc_auc": 0.5, "test_mcc": 0.0})
    rows = []
    for split in ("train", "validation", "test"):
        for index, target in enumerate((0, 1, 0, 1)):
            path = tmp_path / f"{split}-{index}.png"
            Image.new("RGB", (16, 16), "black" if target == 0 else "white").save(path)
            rows.append({"image_sample_key": f"{split}-{index}", "code": str(1000 + index), "as_of": 20250101, "scale": "micro", "bars": 30, "image_path": str(path), "split": split, "future_top15_by_ret20": target == 1, "future_bottom15_by_ret20": target == 0, "neutral_middle70": False, "ret20": 0.1 if target else -0.1})
    (phase7 / "training_manifest.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    output = run(phase7_dir=phase7, phase8_dir=phase8, output_root=tmp_path / "out", epochs=1, batch_size=2)
    audit = json.loads((output / "phase9_audit.json").read_text(encoding="utf-8"))
    progress = json.loads((output / "phase9_progress.json").read_text(encoding="utf-8"))

    assert audit["production_ranking_changed"] is False
    assert audit["runtime_db_write"] is False
    assert audit["full_dataset_used"] is True
    assert progress["stage"] == "completed"
