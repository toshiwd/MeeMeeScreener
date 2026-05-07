from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


LATEST_ROOT = Path(r"G:\Tradex\long_side_candidate_generation_refinement_audit_v3")


def _latest_bundle() -> Path:
    candidates = [p for p in LATEST_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise AssertionError("no long-side refinement v3 bundle found")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load(bundle: Path, name: str) -> dict:
    return json.loads((bundle / name).read_text(encoding="utf-8"))


def test_latest_bundle_reports_native_reject_logging_needed() -> None:
    bundle = _latest_bundle()
    decision = _load(bundle, "long_side_candidate_generation_refinement_audit_v3_decision.json")
    summary = _load(bundle, "long_side_repaired_loss_attribution_audit.json")
    feasibility = _load(bundle, "long_side_admission_or_recall_signal_feasibility_audit.json")

    assert decision["decision"] == "ready_to_implement_native_rejected_row_logging"
    assert decision["status"] == "ready_to_implement_native_rejected_row_logging"
    assert summary["winner_count"] == 28
    assert summary["canonical_key_complete"] is True
    assert summary["category_counts"]["source_absent_before_min_pool"] == 11
    assert summary["category_counts"]["accepted_and_selected"] == 11
    assert summary["category_counts"]["accepted_but_buried_by_champion_rank"] == 5
    assert summary["category_counts"]["accepted_but_missed_by_reranker"] == 1
    assert feasibility["accepted_winner_count"] == 17
    assert feasibility["source_absent_winner_count"] == 11
    assert feasibility["native_reject_logging_supported"] is True


def test_latest_bundle_emits_repaired_trace_artifacts() -> None:
    bundle = _latest_bundle()
    repaired = pd.read_parquet(bundle / "long_side_repaired_loss_attribution_rows.parquet")
    accepted = pd.read_parquet(bundle / "long_side_accepted_winner_ranking_path_rows.parquet")
    absent = pd.read_parquet(bundle / "long_side_source_absent_winner_rows.parquet")
    contrast = pd.read_parquet(bundle / "long_side_admission_recall_feature_contrast.parquet")

    assert repaired.shape[0] == 28
    assert repaired["canonical_candidate_key"].notna().all()
    assert repaired["canonical_candidate_key"].nunique() == 28
    assert set(repaired["v3_loss_category"].unique()) == {
        "source_absent_before_min_pool",
        "accepted_and_selected",
        "accepted_but_buried_by_champion_rank",
        "accepted_but_missed_by_reranker",
    }
    assert accepted.shape[0] == 17
    assert absent.shape[0] == 11
    assert accepted["v3_loss_category"].isin(
        [
            "accepted_and_selected",
            "accepted_but_buried_by_champion_rank",
            "accepted_but_missed_by_reranker",
            "accepted_to_long_active",
        ]
    ).all()
    assert set(contrast["group"]) == {"accepted_winners", "source_absent_winners"}
