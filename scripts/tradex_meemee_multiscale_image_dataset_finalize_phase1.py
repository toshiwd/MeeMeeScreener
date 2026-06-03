from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def finalize(*, dataset_dir: Path) -> dict[str, Any]:
    manifest = _read_jsonl(dataset_dir / "image_manifest.jsonl")
    rows: list[dict[str, Any]] = []
    for row in manifest:
        path = dataset_dir / str(row["image_relpath"])
        rows.append({
            "image_sample_key": row["image_sample_key"],
            "code": row["code"],
            "as_of": row["as_of"],
            "scale": row["scale"],
            "image_relpath": row["image_relpath"],
            "exists": path.exists(),
            "sha256": _sha256(path) if path.exists() else None,
        })
    present = [row for row in rows if row["exists"]]
    hashes = [row["sha256"] for row in present]
    complete = len(present) == len(rows) and bool(rows)
    report = {
        "schema_version": "tradex_meemee_multiscale_image_dataset_browser_export_audit_v1",
        "canonical_renderer": "MeeMee ThumbnailCanvas.drawChart via Playwright browser export",
        "expected_image_count": len(rows),
        "exported_image_count": len(present),
        "missing_image_count": len(rows) - len(present),
        "unique_image_hash_count": len(set(hashes)),
        "canonical_browser_export_complete": complete,
        "rows": rows,
        "judgment": "pass_phase1_dataset_pilot" if complete else "hold_incomplete_canonical_browser_export",
    }
    _write_json(dataset_dir / "browser_export_audit.json", report)
    audit_path = dataset_dir / "phase1_audit.json"
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    audit["browser_export_audit"] = {
        key: value for key, value in report.items() if key != "rows"
    }
    audit["judgment"] = report["judgment"]
    audit["remaining_gate"] = None if complete else "canonical browser export incomplete"
    _write_json(audit_path, audit)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(finalize(dataset_dir=args.dataset_dir), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
