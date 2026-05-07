from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


LATEST_ROOT = Path(r"G:\Tradex\rejected_row_instrumentation_v1")


def _latest_bundle() -> Path:
    candidates = [p for p in LATEST_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise AssertionError("no rejected-row instrumentation bundle found")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load(bundle: Path, name: str) -> dict:
    return json.loads((bundle / name).read_text(encoding="utf-8"))


def test_latest_bundle_reports_key_repair_needed() -> None:
    bundle = _latest_bundle()
    decision = _load(bundle, "rejected_row_instrumentation_v1_decision.json")
    summary = _load(bundle, "long_side_top15_winner_loss_trace.json")
    stage_inventory = _load(bundle, "candidate_admission_stage_inventory.json")

    assert decision["decision"] == "instrumentation_partial_needs_key_repair"
    assert decision["status"] == "instrumentation_partial_needs_key_repair"
    assert summary["long_side_top15_winner_count"] == 28
    assert summary["stable_key_traceable_count"] == 17
    assert summary["exact_key_traceable_count"] == 0
    assert summary["key_repair_needed_count"] == 17
    assert summary["absent_before_min_pool_count"] == 11
    assert stage_inventory["stages"][0]["stage_name"] == "raw_candidate_source"
    assert stage_inventory["stages"][-1]["stage_name"] == "side_specific_long_active_surface"


def test_latest_bundle_emits_trace_and_reject_artifacts() -> None:
    bundle = _latest_bundle()
    trace = pd.read_parquet(bundle / "candidate_admission_trace_rows.parquet")
    rejected = pd.read_parquet(bundle / "rejected_candidate_rows.parquet")
    accepted = pd.read_parquet(bundle / "accepted_candidate_rows.parquet")
    winner_trace = pd.read_parquet(bundle / "long_side_top15_winner_loss_trace_rows.parquet")

    assert trace.shape[0] == rejected.shape[0] + accepted.shape[0]
    assert {"stage_name", "accepted", "reject_reason_bucket", "stable_candidate_key"}.issubset(trace.columns)
    assert {"candidate_idx", "first_seen_stage", "final_stage_reached", "loss_class"}.issubset(winner_trace.columns)
    assert winner_trace.shape[0] == 28
    assert rejected["accepted"].eq(False).all()
    assert accepted["accepted"].eq(True).all()
    assert trace["stage_name"].nunique() >= 5
    assert trace["stable_candidate_key"].notna().all()
