from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "decisive_trigger_blind_image_bind_v1"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n" for row in rows), encoding="utf-8")


def run(template: Path, manifests: list[Path], output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=False)
    cases = _read_jsonl(template)
    captures = [row for manifest in manifests for row in _read_jsonl(manifest)]
    index: dict[str, dict[str, Any]] = {}
    duplicate_keys: list[str] = []
    for row in captures:
        key = f"{row.get('code')}:{row.get('as_of')}"
        if key in index:
            duplicate_keys.append(key)
        index[key] = row
    joined = []
    errors = []
    for case in cases:
        key = f"{case['code']}:{case['as_of']}"
        capture = index.get(key)
        if capture is None:
            errors.append({"review_id": case["review_id"], "error": "missing_capture", "key": key})
            continue
        if capture.get("centered_screenshot") or capture.get("viewport_fallback"):
            errors.append({"review_id": case["review_id"], "error": "future_or_fallback_capture_prohibited", "key": key})
            continue
        if capture.get("review_timeframes") != ["monthly", "weekly", "daily"]:
            errors.append({"review_id": case["review_id"], "error": "timeframe_contract_mismatch", "key": key})
            continue
        image_path = Path(capture["saved_path"])
        if not image_path.exists():
            errors.append({"review_id": case["review_id"], "error": "image_missing_on_disk", "key": key})
            continue
        joined.append({
            "review_id": case["review_id"],
            "code": case["code"],
            "as_of": case["as_of"],
            "chart_cutoff": case["as_of"],
            "review_timeframes": ["monthly", "weekly", "daily"],
            "future_bars_visible": False,
            "outcome_revealed": False,
            "saved_path": str(image_path.resolve()),
            "image_sha256": _sha256(image_path),
        })
    if duplicate_keys:
        errors.append({"error": "duplicate_capture_keys", "keys": sorted(set(duplicate_keys))})
    if len(joined) != len(cases):
        errors.append({"error": "case_cardinality_mismatch", "cases": len(cases), "joined": len(joined)})
    manifest_path = output / "review_image_manifest.jsonl"
    audit_path = output / "audit.json"
    _write_jsonl(manifest_path, joined)
    audit = {
        "schema_version": f"{AXIS_ID}.audit.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "boundary_owner": "TRADEX",
        "source_template": str(template.resolve()),
        "source_manifests": [str(path.resolve()) for path in manifests],
        "case_count": len(cases),
        "capture_rows_read": len(captures),
        "joined_count": len(joined),
        "unique_image_hash_count": len({row["image_sha256"] for row in joined}),
        "errors": errors,
        "outcomes_exposed": False,
        "future_bars_visible": False,
        "judgment": "pass_strict_blind_image_bind" if not errors else "fail_strict_blind_image_bind",
    }
    _write_json(audit_path, audit)
    _write_json(output / "_ARTIFACT_COMPLETE.json", {
        "complete": not errors,
        "authoritative": "audit.json",
        "audit_sha256": _sha256(audit_path),
        "review_image_manifest_sha256": _sha256(manifest_path),
    })
    if errors:
        raise RuntimeError(json.dumps(errors, ensure_ascii=False))
    return {"output": str(output.resolve()), "judgment": audit["judgment"], "joined_count": len(joined)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", required=True, type=Path)
    parser.add_argument("--manifest", required=True, action="append", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.template, args.manifest, args.output), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
