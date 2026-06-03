from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_meemee_image_route_scale_decomposition_phase10 import run


def test_phase10_preserves_scale_when_linear_signal_is_stable(tmp_path: Path) -> None:
    phase8, phase9 = tmp_path / "phase8", tmp_path / "phase9"
    phase8.mkdir()
    phase9.mkdir()
    linear, cnn = [], []
    for split in ("validation", "test"):
        for index, target in enumerate((0, 1)):
            base = {"image_sample_key": f"{split}-{index}", "scale": "micro", "split": split, "target": target}
            linear.append({**base, "image_score": 0.1 if target == 0 else 0.9})
            cnn.append({**base, "cnn_image_score": 0.4 if target == 0 else 0.6})
    (phase8 / "image_score_ledger.jsonl").write_text("".join(json.dumps(row) + "\n" for row in linear), encoding="utf-8")
    (phase9 / "cnn_image_score_ledger.jsonl").write_text("".join(json.dumps(row) + "\n" for row in cnn), encoding="utf-8")

    output = run(phase8_dir=phase8, phase9_dir=phase9, output_root=tmp_path / "out")
    audit = json.loads((output / "phase10_audit.json").read_text(encoding="utf-8"))

    assert audit["decision"] == "preserve_reusable_scale_signal"
    assert audit["reusable_stable_linear_scales"] == ["micro"]
    assert audit["model_retraining_executed"] is False
