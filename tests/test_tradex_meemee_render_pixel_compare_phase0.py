from __future__ import annotations

import json
from pathlib import Path

from PIL import Image

from scripts.tradex_meemee_render_pixel_compare_phase0 import compare


def test_pixel_compare_writes_canonical_browser_equivalence_gate(tmp_path: Path) -> None:
    research = tmp_path / "research"
    browser = research / "browser"
    (research / "images").mkdir(parents=True)
    browser.mkdir()
    Image.new("RGB", (4, 4), "white").save(research / "images" / "sample.png")
    Image.new("RGBA", (4, 4), (0, 0, 0, 0)).save(browser / "sample.png")
    (research / "phase0_audit.json").write_text('{"judgment":"hold_for_playwright_pixel_comparison"}\n', encoding="utf-8")

    report = compare(research_dir=research, browser_dir=browser)
    gate = json.loads((research / "equivalence_gate_report.json").read_text(encoding="utf-8"))
    audit = json.loads((research / "phase0_audit.json").read_text(encoding="utf-8"))

    assert report["comparison_background"] == "white_composite"
    assert report["mean_changed_pixel_ratio"] == 0.0
    assert gate["canonical_browser_reference_complete"] is True
    assert gate["training_dataset_renderer"] == "canonical_browser_reference"
    assert gate["judgment"] == "pass_phase0_render_contract"
    assert audit["judgment"] == "pass_phase0_render_contract"
    assert audit["remaining_gate"] is None
