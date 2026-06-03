from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "meemee_multiscale_temporal_split_phase2"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_multiscale_temporal_split_phase2")
DEFAULT_PHASE1_DIR = Path(r"G:\Tradex\meemee_multiscale_image_dataset_phase1\20260601T074201Z-meemee_multiscale_image_dataset_phase1")


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


def _validate_phase1(phase1_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    required = ("phase1_audit.json", "dataset_contract.json", "label_contract.json", "image_manifest.jsonl", "label_ledger.jsonl")
    missing = [name for name in required if not (phase1_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Phase 1 source missing artifacts: {missing}")
    audit = _read_json(phase1_dir / "phase1_audit.json")
    if audit.get("judgment") != "pass_phase1_dataset_pilot":
        raise RuntimeError("Phase 1 source is not a passed pilot")
    if audit.get("point_in_time_payload_passed") is not True or audit.get("label_isolation_passed") is not True:
        raise RuntimeError("Phase 1 source point-in-time or label-isolation audit failed")
    manifest = _read_jsonl(phase1_dir / "image_manifest.jsonl")
    labels = _read_jsonl(phase1_dir / "label_ledger.jsonl")
    if not labels or any(row.get("label_end_as_of") is None for row in labels):
        raise RuntimeError("Phase 1 source does not expose label_end_as_of for direct overlap audit")
    return manifest, labels, audit


def _split_boundaries(unique_dates: list[int]) -> tuple[int, int, set[int]]:
    if len(unique_dates) < 6:
        raise RuntimeError("at least six unique as_of dates are required for train/validation/test/embargo pilot")
    validation_index = max(2, int(len(unique_dates) * 0.60))
    test_index = max(validation_index + 2, int(len(unique_dates) * 0.80))
    test_index = min(test_index, len(unique_dates) - 1)
    validation_start = unique_dates[validation_index]
    test_start = unique_dates[test_index]
    return validation_start, test_start, {unique_dates[validation_index - 1], unique_dates[test_index - 1]}


def build_split(*, phase1_dir: Path, output_root: Path) -> Path:
    manifest, labels, phase1_audit = _validate_phase1(phase1_dir)
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    unique_dates = sorted({int(row["as_of"]) for row in labels})
    validation_start, test_start, embargo_dates = _split_boundaries(unique_dates)
    assignments: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for row in labels:
        as_of = int(row["as_of"])
        if as_of in embargo_dates:
            split, reason = "embargo", "pre_boundary_sparse_pilot_embargo"
        elif as_of < validation_start:
            split, reason = "train", None
        elif as_of < test_start:
            split, reason = "validation", None
        else:
            split, reason = "test", None
        counts[split] += 1
        assignments.append({
            "image_sample_key": row["image_sample_key"],
            "code": row["code"],
            "as_of": as_of,
            "label_start_as_of": int(row["label_start_as_of"]),
            "label_end_as_of": int(row["label_end_as_of"]),
            "split": split,
            "embargo_reason": reason,
        })
    non_embargo = [row for row in assignments if row["split"] != "embargo"]
    train = [row for row in assignments if row["split"] == "train"]
    validation = [row for row in assignments if row["split"] == "validation"]
    test = [row for row in assignments if row["split"] == "test"]
    train_validation_overlap = any(row["label_end_as_of"] >= validation_start for row in train)
    validation_test_overlap = any(row["label_end_as_of"] >= test_start for row in validation)
    same_date_cross_split = any(
        len({row["split"] for row in non_embargo if row["as_of"] == as_of}) > 1 for as_of in unique_dates
    )
    image_keys = {row["image_sample_key"] for row in manifest}
    label_keys = {row["image_sample_key"] for row in labels}
    split_keys = {row["image_sample_key"] for row in assignments}
    split_nonempty = all(counts[name] > 0 for name in ("train", "validation", "test", "embargo"))
    passed = (
        split_nonempty
        and not train_validation_overlap
        and not validation_test_overlap
        and not same_date_cross_split
        and image_keys == label_keys == split_keys
    )
    contract = {
        "schema_version": "tradex_meemee_multiscale_temporal_split_phase2_contract_v1",
        "boundary_owner": "TRADEX",
        "source_phase1_dir": str(phase1_dir),
        "split_policy": "chronological_sparse_pilot_with_pre_boundary_anchor_embargo",
        "label_horizon_trading_days": 20,
        "validation_start_as_of": validation_start,
        "test_start_as_of": test_start,
        "embargo_as_of_dates": sorted(embargo_dates),
        "split_counts": dict(sorted(counts.items())),
        "non_scope": ["model training", "probability calibration", "production ranking mutation", "runtime DB write", "MeeMee UI mutation"],
    }
    leakage = {
        "schema_version": "tradex_meemee_multiscale_temporal_split_phase2_leakage_audit_v1",
        "boundary_owner": "TRADEX",
        "phase1_point_in_time_payload_passed": phase1_audit.get("point_in_time_payload_passed") is True,
        "phase1_label_isolation_passed": phase1_audit.get("label_isolation_passed") is True,
        "split_train_validation_test_embargo_non_empty": split_nonempty,
        "same_as_of_across_train_validation_test": same_date_cross_split,
        "future_label_window_overlap_train_validation": train_validation_overlap,
        "future_label_window_overlap_validation_test": validation_test_overlap,
        "manifest_label_split_keys_match": image_keys == label_keys == split_keys,
        "split_leakage_audit_passed": passed,
    }
    readiness = {
        "schema_version": "tradex_meemee_multiscale_temporal_split_phase2_readiness_v1",
        "boundary_owner": "TRADEX",
        "split_leakage_audit_passed": passed,
        "pilot_sample_bundle_count": len(labels),
        "pilot_manifest_image_count": len(manifest),
        "split_counts": dict(sorted(counts.items())),
        "ready_for_dataset_scale_generation": passed,
        "ready_for_model_training": False,
        "training_block_reason": "dataset_scale_generation_and_full_split_balance_audit_required",
        "judgment": "pass_phase2_ready_for_dataset_scale_generation" if passed else "hold_split_leakage_audit_failed",
    }
    _write_json(output_dir / "split_contract.json", contract)
    _write_jsonl(output_dir / "split_assignment_ledger.jsonl", assignments)
    _write_json(output_dir / "split_leakage_audit.json", leakage)
    _write_json(output_dir / "dataset_scale_readiness.json", readiness)
    _write_json(output_dir / "phase2_audit.json", {
        "schema_version": "tradex_meemee_multiscale_temporal_split_phase2_audit_v1",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "source_phase1_dir": str(phase1_dir),
        "authoritative_readiness": readiness,
        "judgment": readiness["judgment"],
    })
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase1-dir", type=Path, default=DEFAULT_PHASE1_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(build_split(phase1_dir=args.phase1_dir, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
