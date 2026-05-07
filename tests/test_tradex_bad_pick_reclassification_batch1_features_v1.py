from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_bad_pick_reclassification_batch1_features_v1 import (
    run_bad_pick_reclassification_batch1_features_v1,
)


def test_bad_pick_reclassification_batch1_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_bad_pick_reclassification_batch1_features_v1(output_root=tmp_path / "batch1_reclassification")
    session_dir = Path(result["output_dir"])

    required = {
        "run_manifest.json",
        "input_resolution.json",
        "batch1_input_validation.json",
        "batch1_reclassification_rows.parquet",
        "batch1_root_cause_taxonomy_summary.json",
        "before_after_batch1_reclassification_summary.json",
        "batch1_added_top15_bottom15_contrast.json",
        "batch1_boundary_pairwise.parquet",
        "batch1_boundary_pairwise_summary.json",
        "batch1_future_challenger_candidates.json",
        "bad_pick_reclassification_batch1_features_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
        "field_level_coverage_matrix.parquet",
        "candidate_prefilter_rows_feature_enriched_v1.parquet",
        "observable_regime_false_positive_feature_enriched_v1.parquet",
    }
    assert required.issubset({path.name for path in session_dir.iterdir()})

    validation = json.loads((session_dir / "batch1_input_validation.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "bad_pick_reclassification_batch1_features_v1_decision.json").read_text(encoding="utf-8"))
    taxonomy = json.loads((session_dir / "batch1_root_cause_taxonomy_summary.json").read_text(encoding="utf-8"))
    contrast = json.loads((session_dir / "batch1_added_top15_bottom15_contrast.json").read_text(encoding="utf-8"))
    pairwise = json.loads((session_dir / "batch1_boundary_pairwise_summary.json").read_text(encoding="utf-8"))

    assert validation["candidate_row_count"] == 2542
    assert validation["orfp_row_count"] == 365
    assert validation["previous_unknown_row_count"] == 585
    assert validation["no_lookahead_audit_passed"] is True
    assert validation["row_count_reconciled"] is True
    assert decision["decision"] in {
        "ready_for_single_axis_challenger_design",
        "needs_batch1_formula_revision",
        "needs_batch2_feature_sources",
        "explanation_only",
        "insufficient_signal",
    }
    assert taxonomy["row_count"] == 585
    assert len(taxonomy["family_counts"]) > 0
    assert pairwise["pair_count"] >= pairwise["matched_near_miss_count"]
    assert "top5" in contrast["topk"] and "top10" in contrast["topk"]

    rows = pd.read_parquet(session_dir / "batch1_reclassification_rows.parquet")
    assert len(rows) == 585
    for column in [
        "batch1_root_cause_code",
        "batch1_confidence",
        "batch1_is_candidate_for_future_challenger",
        "entry_strength_score",
        "signal_quality_bucket",
        "decision_candle_quality",
        "volume_participation_bucket",
        "liquidity_quality_bucket",
        "higher_timeframe_headroom_bucket",
    ]:
        assert column in rows.columns
