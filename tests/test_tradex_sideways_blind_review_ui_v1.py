from __future__ import annotations

import json

from scripts.tradex_sideways_blind_review_ui_v1 import build_ui


def test_build_ui_hides_machine_labels_and_exports_sideways_contract(tmp_path) -> None:
    images = tmp_path / "capture" / "images"
    images.mkdir(parents=True)
    (images / "SW001.png").write_bytes(b"png")
    manifest = tmp_path / "capture" / "review_image_manifest.jsonl"
    manifest.write_text(json.dumps({"case_id": "SW001", "code": "1000", "ymd": 20250101, "image_relpath": "images/SW001.png"}) + "\n", encoding="utf-8")
    output = tmp_path / "ui"
    html = build_ui(manifest, output).read_text(encoding="utf-8")
    contract = json.loads((output / "annotation_contract.json").read_text(encoding="utf-8"))
    assert "SIDEWAYS" in html and "NOT_SIDEWAYS" in html and "BORDERLINE" in html
    assert "fetch('/save-annotations'" in html
    assert contract["machine_labels_visible"] is False
    assert contract["outcomes_visible"] is False
    assert contract["required_fields"] == ["sideways_decision", "confidence"]
