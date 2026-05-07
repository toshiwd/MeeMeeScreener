from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


LATEST_ROOT = Path(r"G:\Tradex\native_rejected_row_logging_v1")


def _latest_bundle() -> Path:
    candidates = [p for p in LATEST_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise AssertionError("no native rejected-row logging bundle found")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load(bundle: Path, name: str) -> dict:
    return json.loads((bundle / name).read_text(encoding="utf-8"))


def test_latest_bundle_reports_native_logging_still_insufficient() -> None:
    bundle = _latest_bundle()
    decision = _load(bundle, "native_rejected_row_logging_v1_decision.json")
    summary = _load(bundle, "native_rejected_row_logging_summary.json")
    audit = _load(bundle, "native_long_side_refinement_audit.json")
    recommendation = _load(bundle, "native_long_side_refinement_recommendation.json")

    assert decision["decision"] == "native_logging_still_insufficient"
    assert decision["status"] == "native_logging_still_insufficient"
    assert summary["native_hook_available"] is True
    assert summary["canonical_key_complete"] is True
    assert summary["trace_counts"]["trace_rows"] == 14256
    assert audit["feasibility"]["accepted_winner_count"] == 17
    assert audit["feasibility"]["source_absent_winner_count"] == 11
    assert audit["feasibility"]["native_logging_still_insufficient"] is True
    assert audit["feasibility"]["missing_fields"]["source_absent_candidate_pool_tier"] is True
    assert audit["feasibility"]["missing_fields"]["source_absent_min_pool_reject_subreason"] is True
    assert recommendation["recommended_next_action"] == "native_logging_still_insufficient"


def test_latest_bundle_emits_native_trace_artifacts() -> None:
    bundle = _latest_bundle()
    trace = pd.read_parquet(bundle / "native_candidate_admission_trace_rows.parquet")
    winner_trace = pd.read_parquet(bundle / "native_long_side_top15_loss_trace_rows.parquet")
    rejected = pd.read_parquet(bundle / "native_rejected_candidate_rows.parquet")
    accepted = pd.read_parquet(bundle / "native_accepted_candidate_rows.parquet")

    assert trace.shape == (14256, 40)
    assert trace["accepted"].value_counts(dropna=False).to_dict() == {False: 7536, True: 6720}
    assert trace["canonical_candidate_key"].notna().all()
    assert trace["canonical_candidate_key"].nunique() == 2376
    assert winner_trace.shape[0] == 28
    assert set(winner_trace["loss_category"].unique()) == {
        "source_absent_before_min_pool",
        "accepted_to_long_active",
        "accepted_but_buried_by_champion_rank",
    }
    assert rejected["accepted"].eq(False).all()
    assert accepted["accepted"].eq(True).all()

