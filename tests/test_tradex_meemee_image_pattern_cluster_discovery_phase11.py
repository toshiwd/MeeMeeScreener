from __future__ import annotations

import json
from pathlib import Path

from PIL import Image

from scripts import tradex_meemee_image_pattern_cluster_discovery_phase11 as mod


def _write_image(path: Path, value: int) -> None:
    Image.new("L", (8, 8), color=value).save(path)


def test_phase11_keeps_cluster_when_direction_replicates(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(mod, "MIN_TRAIN_COUNT", 2)
    monkeypatch.setattr(mod, "MIN_EVAL_COUNT", 1)
    phase7 = tmp_path / "phase7"
    phase7.mkdir()
    rows = []
    for split in ("train", "validation", "test"):
        for index, (value, ret20) in enumerate(((20, -0.10), (30, -0.08), (220, 0.10), (230, 0.12))):
            key = f"{split}-{index}"
            for scale in mod.SCALES:
                image = tmp_path / f"{key}-{scale}.png"
                _write_image(image, value)
                rows.append({
                    "image_sample_key": key,
                    "code": key,
                    "as_of": 20260101 + index,
                    "scale": scale,
                    "image_path": str(image),
                    "split": split,
                    "ret20": ret20,
                    "future_top15_by_ret20": ret20 > 0,
                    "future_bottom15_by_ret20": ret20 < 0,
                })
    (phase7 / "training_manifest.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    output = mod.run(phase7_dir=phase7, output_root=tmp_path / "out", cluster_count=2)
    decision = json.loads((output / "research_decision.json").read_text(encoding="utf-8"))
    compare = json.loads((output / "image_pattern_cluster_compare.json").read_text(encoding="utf-8"))

    assert decision["authoritative_rollup_decision"] == "keep_stable_image_pattern_clusters_for_manual_review"
    assert decision["meemee_unchanged"] is True
    assert compare["feature_contract"]["labels_used_for_clustering"] is False
    assert list((tmp_path / "out" / "feature_cache" / "train").glob("*.npy"))
    assert list((tmp_path / "out" / "feature_cache" / "all").glob("*.npy"))
