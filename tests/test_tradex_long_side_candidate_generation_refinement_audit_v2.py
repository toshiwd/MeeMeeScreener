from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


LATEST_ROOT = Path(r"G:\Tradex\long_side_candidate_generation_refinement_audit_v2")


def _latest_bundle() -> Path:
    candidates = [p for p in LATEST_ROOT.iterdir() if p.is_dir()]
    if not candidates:
        raise AssertionError("no long-side candidate-generation refinement audit v2 bundle found")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _load(bundle: Path, name: str) -> dict:
    return json.loads((bundle / name).read_text(encoding="utf-8"))


def test_latest_bundle_recommends_exact_key_repair() -> None:
    bundle = _latest_bundle()
    decision = _load(bundle, "long_side_candidate_generation_refinement_audit_v2_decision.json")
    trace_quality = _load(bundle, "long_side_trace_quality_audit.json")
    loss = _load(bundle, "long_side_top15_loss_attribution_v2.json")

    assert decision["decision"] == "ready_to_repair_exact_candidate_keys"
    assert decision["status"] == "ready_to_repair_exact_candidate_keys"
    assert trace_quality["winner_count"] == 28
    assert trace_quality["stable_key_traceable_count"] == 17
    assert trace_quality["exact_key_traceable_count"] == 0
    assert loss["source_absent_before_min_pool_count"] == 11
    assert loss["key_repair_required_count"] == 17
    assert loss["accepted_to_long_active_count"] == 0


def test_latest_bundle_emits_required_artifacts() -> None:
    bundle = _latest_bundle()
    trace_rows = pd.read_parquet(bundle / "long_side_top15_loss_attribution_rows.parquet")
    stage_rows = pd.read_parquet(bundle / "long_side_stage_bottleneck_rows.parquet")
    score_rows = pd.read_parquet(bundle / "long_side_trace_score_rank_bucket_summary.parquet")
    tier_rows = pd.read_parquet(bundle / "long_side_trace_tier_summary.parquet")
    exact_rows = pd.read_parquet(bundle / "exact_key_failure_examples.parquet")

    assert trace_rows.shape[0] == 28
    assert "loss_class_v2" in trace_rows.columns
    assert stage_rows["stage_name"].nunique() == 6
    assert score_rows.shape[0] == 9
    assert tier_rows.shape[0] >= 3
    assert exact_rows.shape[0] == 17
    assert trace_rows["stage_presence_high_recall_min_pool"].sum() == 17
    assert trace_rows["loss_class_v2"].value_counts().get("source_absent_before_min_pool", 0) == 11
    assert trace_rows["loss_class_v2"].value_counts().get("key_repair_required", 0) == 17
