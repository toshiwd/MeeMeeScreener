from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageChops


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _white_composite(path: Path) -> Image.Image:
    image = Image.open(path).convert("RGBA")
    background = Image.new("RGBA", image.size, "white")
    return Image.alpha_composite(background, image).convert("RGB")


def compare(*, research_dir: Path, browser_dir: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    diff_dir = research_dir / "pixel_diffs"
    diff_dir.mkdir(exist_ok=True)
    for research_path in sorted((research_dir / "images").glob("*.png")):
        browser_path = browser_dir / research_path.name
        if not browser_path.exists():
            rows.append({"image": research_path.name, "status": "missing_browser_reference"})
            continue
        research = _white_composite(research_path)
        browser = _white_composite(browser_path)
        if research.size != browser.size:
            rows.append({"image": research_path.name, "status": "size_mismatch", "research_size": research.size, "browser_size": browser.size})
            continue
        a = np.asarray(research, dtype=np.int16)
        b = np.asarray(browser, dtype=np.int16)
        delta = np.abs(a - b)
        changed = np.any(delta > 0, axis=2)
        ImageChops.difference(research, browser).save(diff_dir / research_path.name)
        rows.append({
            "image": research_path.name,
            "status": "compared",
            "mean_absolute_channel_delta": round(float(delta.mean()), 6),
            "changed_pixel_ratio": round(float(changed.mean()), 6),
            "max_channel_delta": int(delta.max()),
        })
    compared = [row for row in rows if row["status"] == "compared"]
    report = {
        "schema_version": "meemee_render_pixel_compare_phase0_v1",
        "research_dir": str(research_dir),
        "browser_dir": str(browser_dir),
        "expected_image_count": len(list((research_dir / "images").glob("*.png"))),
        "compared_image_count": len(compared),
        "missing_or_invalid_count": len(rows) - len(compared),
        "mean_changed_pixel_ratio": round(sum(row["changed_pixel_ratio"] for row in compared) / len(compared), 6) if compared else None,
        "mean_absolute_channel_delta": round(sum(row["mean_absolute_channel_delta"] for row in compared) / len(compared), 6) if compared else None,
        "comparison_background": "white_composite",
        "judgment": "comparison_complete_review_required" if compared and len(compared) == len(rows) else "comparison_incomplete",
        "rows": rows,
    }
    _write_json(research_dir / "pixel_comparison_report.json", report)
    gate = {
        "schema_version": "meemee_equivalent_render_gate_phase0_v1",
        "canonical_renderer": "MeeMee ThumbnailCanvas.drawChart via Playwright browser export",
        "proxy_renderer": "TRADEX deterministic PIL renderer",
        "canonical_browser_reference_count": len(compared),
        "expected_image_count": len(rows),
        "canonical_browser_reference_complete": bool(compared) and len(compared) == len(rows),
        "proxy_comparison_background": "white_composite",
        "proxy_mean_changed_pixel_ratio": report["mean_changed_pixel_ratio"],
        "proxy_mean_absolute_channel_delta": report["mean_absolute_channel_delta"],
        "proxy_role": "comparison_audit_only",
        "training_dataset_renderer": "canonical_browser_reference",
        "judgment": "pass_phase0_render_contract" if compared and len(compared) == len(rows) else "hold_incomplete_browser_reference",
        "non_scope": ["production ranking mutation", "runtime DB write", "MeeMee UI mutation"],
    }
    _write_json(research_dir / "equivalence_gate_report.json", gate)
    audit_path = research_dir / "phase0_audit.json"
    if audit_path.exists():
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        audit["equivalence_gate"] = gate
        audit["judgment"] = gate["judgment"]
        audit["remaining_gate"] = None if gate["judgment"] == "pass_phase0_render_contract" else "canonical browser reference export incomplete"
        _write_json(audit_path, audit)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--research-dir", type=Path, required=True)
    parser.add_argument("--browser-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(compare(research_dir=args.research_dir, browser_dir=args.browser_dir), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
