from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "meemee_training_preparation_phase7"
DEFAULT_PHASE3_DIR = Path(r"G:\Tradex\meemee_multiscale_dataset_scale_phase3\20260601T080600Z-meemee_multiscale_dataset_scale_phase3")
DEFAULT_EXPORT_ROOT = Path(r"G:\Tradex\meemee_canonical_export_phase4")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_training_preparation_phase7")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def prepare(*, phase3_dir: Path, export_root: Path, output_root: Path) -> Path:
    required_phase3 = (
        "phase3_audit.json",
        "label_ledger.jsonl",
        "split_assignment_ledger.jsonl",
        "canonical_image_export_plan.jsonl",
        "split_leakage_audit.json",
        "class_balance_audit.json",
        "data_integrity_audit.json",
    )
    missing = [name for name in required_phase3 if not (phase3_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Phase 3 source missing artifacts: {missing}")
    export_audit_path = export_root / "canonical_export_progress_audit.json"
    if not export_audit_path.exists():
        raise FileNotFoundError(f"Canonical export audit missing: {export_audit_path}")

    phase3 = _read_json(phase3_dir / "phase3_audit.json")
    export_audit = _read_json(export_audit_path)
    readiness = phase3.get("authoritative_readiness") or {}
    if readiness.get("ready_for_model_training_preparation") is not True:
        raise RuntimeError("Phase 3 is not ready for model-training preparation")
    if export_audit.get("ready_for_model_training") is not True:
        raise RuntimeError("Canonical browser export is not verified for model training")
    if export_audit.get("remaining_image_count") != 0:
        raise RuntimeError("Canonical browser export still has pending images")
    if export_audit.get("unique_exported_hash_count") != export_audit.get("exported_image_count"):
        raise RuntimeError("Canonical browser export hashes are not unique")

    labels = _read_jsonl(phase3_dir / "label_ledger.jsonl")
    splits = _read_jsonl(phase3_dir / "split_assignment_ledger.jsonl")
    plan = _read_jsonl(phase3_dir / "canonical_image_export_plan.jsonl")
    label_by_key = {row["image_sample_key"]: row for row in labels}
    split_by_key = {row["image_sample_key"]: row for row in splits}
    rows: list[dict[str, Any]] = []
    split_counts: Counter[str] = Counter()
    class_counts: dict[str, Counter[str]] = defaultdict(Counter)
    missing_images: list[str] = []
    for image in plan:
        key = image["image_sample_key"]
        label = label_by_key[key]
        split = split_by_key[key]["split"]
        class_name = "top15" if label["future_top15_by_ret20"] else "bottom15" if label["future_bottom15_by_ret20"] else "neutral70"
        image_path = export_root / image["image_relpath"]
        if not image_path.exists():
            missing_images.append(str(image["image_relpath"]))
        split_counts[split] += 1
        class_counts[split][class_name] += 1
        rows.append({
            "image_sample_key": key,
            "code": image["code"],
            "as_of": image["as_of"],
            "scale": image["scale"],
            "bars": image["bars"],
            "image_path": str(image_path),
            "split": split,
            "future_top15_by_ret20": label["future_top15_by_ret20"],
            "future_bottom15_by_ret20": label["future_bottom15_by_ret20"],
            "neutral_middle70": label["neutral_middle70"],
            "ret5": label["ret5"],
            "ret10": label["ret10"],
            "ret20": label["ret20"],
            "MFE20": label["MFE20"],
            "MAE20": label["MAE20"],
        })
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    audit_passed = (
        len(label_by_key) == len(labels)
        and len(split_by_key) == len(splits)
        and len(rows) == len(plan)
        and not missing_images
        and all(split_counts[name] > 0 for name in ("train", "validation", "test", "embargo"))
    )
    audit = {
        "schema_version": "tradex_meemee_training_preparation_phase7_audit_v1",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "phase3_dir": str(phase3_dir),
        "export_root": str(export_root),
        "training_manifest_row_count": len(rows),
        "sample_bundle_count": len(labels),
        "planned_image_count": len(plan),
        "missing_image_count": len(missing_images),
        "missing_images": missing_images[:100],
        "split_image_counts": dict(sorted(split_counts.items())),
        "class_image_counts_by_split": {
            split: dict(sorted(counts.items())) for split, counts in sorted(class_counts.items())
        },
        "full_export_hash_audit_reused": True,
        "full_export_unique_hash_count": export_audit["unique_exported_hash_count"],
        "labels_used_as_training_targets_only": True,
        "labels_used_in_image_rendering": False,
        "labels_used_as_inference_inputs": False,
        "training_preparation_audit_passed": audit_passed,
        "ready_for_model_training": audit_passed,
        "model_training_executed": False,
        "judgment": "pass_phase7_ready_for_model_training" if audit_passed else "hold_phase7_training_preparation_failed",
        "non_scope": ["model training execution", "probability calibration", "production ranking mutation", "runtime DB write", "MeeMee UI mutation"],
    }
    _write_jsonl(output_dir / "training_manifest.jsonl", rows)
    _write_json(output_dir / "training_preparation_contract.json", {
        "schema_version": "tradex_meemee_training_preparation_phase7_contract_v1",
        "boundary_owner": "TRADEX",
        "sample_unit": "canonical_scale_image",
        "split_policy": "reuse_phase3_train_validation_test_embargo_assignment",
        "label_policy": "future labels are targets only; never rendering or inference inputs",
        "image_policy": "reuse phase4 canonical browser images verified by final full SHA-256 audit",
        "non_scope": audit["non_scope"],
    })
    _write_json(output_dir / "phase7_audit.json", audit)
    _write_json(output_root / "phase7_latest_audit.json", audit)
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase3-dir", type=Path, default=DEFAULT_PHASE3_DIR)
    parser.add_argument("--export-root", type=Path, default=DEFAULT_EXPORT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(prepare(phase3_dir=args.phase3_dir, export_root=args.export_root, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
