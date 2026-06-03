from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_meemee_multiscale_temporal_split_phase2 import build_split


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_phase2_sparse_temporal_split_emits_non_overlapping_embargo_ledger(tmp_path: Path) -> None:
    phase1 = tmp_path / "phase1"
    phase1.mkdir()
    dates = [20240131, 20240430, 20240731, 20241031, 20250131, 20250430]
    labels = [
        {
            "image_sample_key": f"k{index}",
            "code": "1001",
            "as_of": as_of,
            "label_start_as_of": as_of + 1,
            "label_end_as_of": as_of + 28,
        }
        for index, as_of in enumerate(dates)
    ]
    manifest = [{"image_sample_key": row["image_sample_key"], "code": "1001", "as_of": row["as_of"]} for row in labels]
    _write_json(phase1 / "phase1_audit.json", {
        "judgment": "pass_phase1_dataset_pilot",
        "point_in_time_payload_passed": True,
        "label_isolation_passed": True,
    })
    _write_json(phase1 / "dataset_contract.json", {})
    _write_json(phase1 / "label_contract.json", {})
    _write_jsonl(phase1 / "image_manifest.jsonl", manifest)
    _write_jsonl(phase1 / "label_ledger.jsonl", labels)

    output = build_split(phase1_dir=phase1, output_root=tmp_path / "out")
    leakage = json.loads((output / "split_leakage_audit.json").read_text(encoding="utf-8"))
    readiness = json.loads((output / "dataset_scale_readiness.json").read_text(encoding="utf-8"))

    assert leakage["split_leakage_audit_passed"] is True
    assert leakage["future_label_window_overlap_train_validation"] is False
    assert leakage["future_label_window_overlap_validation_test"] is False
    assert readiness["ready_for_dataset_scale_generation"] is True
    assert readiness["ready_for_model_training"] is False


def test_phase2_leakage_audit_holds_when_train_label_window_crosses_validation_boundary(tmp_path: Path) -> None:
    phase1 = tmp_path / "phase1"
    phase1.mkdir()
    dates = [20240131, 20240430, 20240731, 20241031, 20250131, 20250430]
    labels = [
        {
            "image_sample_key": f"k{index}",
            "code": "1001",
            "as_of": as_of,
            "label_start_as_of": as_of + 1,
            "label_end_as_of": 20241130 if as_of == 20240430 else as_of + 28,
        }
        for index, as_of in enumerate(dates)
    ]
    manifest = [{"image_sample_key": row["image_sample_key"], "code": "1001", "as_of": row["as_of"]} for row in labels]
    _write_json(phase1 / "phase1_audit.json", {
        "judgment": "pass_phase1_dataset_pilot",
        "point_in_time_payload_passed": True,
        "label_isolation_passed": True,
    })
    _write_json(phase1 / "dataset_contract.json", {})
    _write_json(phase1 / "label_contract.json", {})
    _write_jsonl(phase1 / "image_manifest.jsonl", manifest)
    _write_jsonl(phase1 / "label_ledger.jsonl", labels)

    output = build_split(phase1_dir=phase1, output_root=tmp_path / "out")
    leakage = json.loads((output / "split_leakage_audit.json").read_text(encoding="utf-8"))
    readiness = json.loads((output / "dataset_scale_readiness.json").read_text(encoding="utf-8"))

    assert leakage["future_label_window_overlap_train_validation"] is True
    assert leakage["split_leakage_audit_passed"] is False
    assert readiness["ready_for_dataset_scale_generation"] is False
    assert readiness["judgment"] == "hold_split_leakage_audit_failed"
