from __future__ import annotations

import json
from pathlib import Path

from PIL import Image

from scripts.tradex_meemee_multiscale_image_dataset_finalize_phase1 import finalize


def test_finalize_marks_complete_browser_export_as_phase1_pass(tmp_path: Path) -> None:
    image_path = tmp_path / "browser_reference_images" / "sample.png"
    image_path.parent.mkdir()
    Image.new("RGB", (4, 4), "white").save(image_path)
    (tmp_path / "image_manifest.jsonl").write_text(
        json.dumps({
            "image_sample_key": "sample",
            "code": "1001",
            "as_of": 20250101,
            "scale": "micro",
            "image_relpath": "browser_reference_images/sample.png",
        }) + "\n",
        encoding="utf-8",
    )
    (tmp_path / "phase1_audit.json").write_text('{"judgment":"hold_for_canonical_browser_export"}\n', encoding="utf-8")

    report = finalize(dataset_dir=tmp_path)
    audit = json.loads((tmp_path / "phase1_audit.json").read_text(encoding="utf-8"))

    assert report["canonical_browser_export_complete"] is True
    assert report["exported_image_count"] == 1
    assert report["judgment"] == "pass_phase1_dataset_pilot"
    assert audit["judgment"] == "pass_phase1_dataset_pilot"
    assert audit["remaining_gate"] is None
