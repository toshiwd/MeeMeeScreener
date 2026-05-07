from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_bad_pick_reclassification_batch2_volume_features_v1 import (
    run_bad_pick_reclassification_batch2_volume_features_v1,
)


def test_bad_pick_reclassification_batch2_volume_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_bad_pick_reclassification_batch2_volume_features_v1(
        output_root=tmp_path / "batch2_volume_reclassification",
    )
    session_dir = Path(result["output_dir"])

    required = {
        "run_manifest.json",
        "input_resolution.json",
        "batch2_volume_input_validation.json",
        "batch2_volume_reclassification_rows.parquet",
        "batch2_volume_root_cause_taxonomy_summary.json",
        "before_after_batch2_volume_reclassification_summary.json",
        "batch2_volume_added_top15_bottom15_contrast.json",
        "batch2_volume_boundary_pairwise.parquet",
        "batch2_volume_boundary_pairwise_summary.json",
        "batch2_volume_future_challenger_candidates.json",
        "bad_pick_reclassification_batch2_volume_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
        "batch2_volume_feature_distribution_summary.parquet",
    }
    assert required.issubset({path.name for path in session_dir.iterdir()})

    validation = json.loads((session_dir / "batch2_volume_input_validation.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "bad_pick_reclassification_batch2_volume_v1_decision.json").read_text(encoding="utf-8"))
    taxonomy = json.loads((session_dir / "batch2_volume_root_cause_taxonomy_summary.json").read_text(encoding="utf-8"))
    contrast = json.loads((session_dir / "batch2_volume_added_top15_bottom15_contrast.json").read_text(encoding="utf-8"))
    pairwise = json.loads((session_dir / "batch2_volume_boundary_pairwise_summary.json").read_text(encoding="utf-8"))
    candidates = json.loads((session_dir / "batch2_volume_future_challenger_candidates.json").read_text(encoding="utf-8"))

    assert validation["batch1_reclassification_row_count"] == 585
    assert validation["volume_candidate_row_count"] == 2542
    assert validation["volume_orfp_row_count"] == 365
    assert validation["row_count_reconciled"] is True
    assert validation["no_lookahead_audit_passed"] is True
    assert validation["required_columns_present"]["batch1_reclassification"] is True
    assert validation["required_columns_present"]["volume_candidate"] is True
    assert validation["required_columns_present"]["volume_orfp"] is True

    assert decision["decision"] in {
        "ready_for_single_axis_challenger_design",
        "needs_batch2_volume_formula_revision",
        "needs_batch2_event_sources",
        "explanation_only",
        "insufficient_signal",
    }
    assert taxonomy["row_count"] == 585
    assert len(taxonomy["family_counts"]) > 0
    assert pairwise["pair_count"] >= pairwise["matched_near_miss_count"]
    assert "top5" in contrast["topk"] and "top10" in contrast["topk"]
    assert candidates["candidate_families"]

    rows = pd.read_parquet(session_dir / "batch2_volume_reclassification_rows.parquet")
    assert len(rows) == 585
    for column in [
        "batch2_volume_root_cause_code",
        "batch2_volume_confidence",
        "batch2_volume_family",
        "entry_strength_score",
        "signal_quality_bucket",
        "decision_candle_quality",
        "liquidity_quality_bucket",
        "higher_timeframe_headroom_bucket",
        "vol_ratio5_20_repaired",
        "volume_zscore_20",
        "turnover_value_ratio5_20",
        "participation_quality_bucket",
        "volume_confirmation_repaired_flag",
    ]:
        assert column in rows.columns
