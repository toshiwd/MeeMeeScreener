from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


LATEST_ROOT = Path(r"G:\Tradex\long_side_candidate_generation_refinement_audit_v1")


def _latest_bundle() -> Path:
    candidates = [p for p in LATEST_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise AssertionError("no long-side candidate-generation refinement audit bundle found")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load(bundle: Path, name: str) -> dict:
    return json.loads((bundle / name).read_text(encoding="utf-8"))


def test_latest_bundle_recommends_rejected_row_instrumentation() -> None:
    bundle = _latest_bundle()
    decision = _load(bundle, "long_side_candidate_generation_refinement_audit_v1_decision.json")
    recommendation = _load(bundle, "long_side_candidate_generation_refinement_recommendation.json")
    source = _load(bundle, "long_side_source_instrumentation_audit.json")
    miss = _load(bundle, "long_side_candidate_generation_miss_audit.json")

    assert decision["decision"] == "needs_rejected_row_instrumentation"
    assert decision["status"] == "needs_rejected_row_instrumentation"
    assert recommendation["recommended_next_axis"] == "needs_rejected_row_instrumentation"
    assert source["rejected_rows_available"] is False
    assert source["rejected_rows_logged_as_standalone_bundle"] is False
    assert source["stable_reject_keys_logged"] is False
    assert source["reject_reason_buckets_logged"] is False
    assert miss["long_active_top15_count"] == 2
    assert miss["prefilter_source_long_top15_count"] == 28
    assert miss["traceable_prefilter_top15_to_long_active_exact_key_count"] == 0
    assert miss["miss_classes"]["winner_absent_from_pool"] == 28
    assert miss["miss_classes"]["insufficient_source_instrumentation"] == 28


def test_latest_bundle_top15_winner_rows_are_long_backfill() -> None:
    bundle = _latest_bundle()
    winners = pd.read_parquet(bundle / "long_side_top15_winner_rows.parquet")

    assert len(winners) == 2
    assert set(winners["candidate_pool_tier"].astype(str).unique()) == {"risk_flagged_backfill"}
    assert winners["winner_status"].eq("within_top5").all()
    assert winners["long_active_present"].all()
    assert winners["prefilter_exact_present"].eq(False).all()
    assert winners["two_stage_exact_present"].eq(False).all()
    assert {"anchor_date", "symbol", "tree_hgb_path_value_score", "tree_hgb_path_value_rank"}.issubset(winners.columns)
