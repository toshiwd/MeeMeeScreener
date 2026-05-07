from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_feature_surface_upgrade_plan_v1 import run_feature_surface_upgrade_plan_v1


FEATURE_SURFACE = Path(
    r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\candidate_prefilter_rows_context_enriched.parquet"
)
UNKNOWN_SURFACE = Path(
    r"G:\Tradex\bad_pick_unknown_reclassification_enriched_v1\20260501T053110Z-7a584991\enriched_unknown_reclassification_rows.parquet"
)
FREEZE_SESSION = Path(
    r"G:\Tradex\research_freeze_summaries\observable_regime_false_positive_require_confirmation\20260501T090354Z-098449"
)
REBUILD_SESSION = Path(
    r"G:\Tradex\observable_regime_false_positive_bottom15_summary_rebuild_v1\20260501T085017Z-012155"
)
RECLASSIFICATION_SESSION = Path(
    r"G:\Tradex\bad_pick_unknown_reclassification_enriched_v1\20260501T053110Z-7a584991"
)
BACKFILL_SESSION = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646")


def test_feature_surface_upgrade_plan_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_feature_surface_upgrade_plan_v1(
        output_root=tmp_path / "feature_surface_upgrade_plan_v1",
        feature_surface=FEATURE_SURFACE,
        unknown_surface=UNKNOWN_SURFACE,
        freeze_session=FREEZE_SESSION,
        rebuild_session=REBUILD_SESSION,
        reclassification_session=RECLASSIFICATION_SESSION,
        backfill_session=BACKFILL_SESSION,
    )

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "feature_surface_inventory.json",
        "feature_candidate_design.json",
        "feature_surface_batch1_recommendation.json",
        "feature_surface_build_plan.json",
        "feature_surface_validation_plan.json",
        "feature_surface_upgrade_plan_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
        "field_coverage_matrix.parquet",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    decision = json.loads((session_dir / "feature_surface_upgrade_plan_v1_decision.json").read_text(encoding="utf-8"))
    inventory = json.loads((session_dir / "feature_surface_inventory.json").read_text(encoding="utf-8"))
    candidate_design = json.loads((session_dir / "feature_candidate_design.json").read_text(encoding="utf-8"))
    batch1 = json.loads((session_dir / "feature_surface_batch1_recommendation.json").read_text(encoding="utf-8"))
    build_plan = json.loads((session_dir / "feature_surface_build_plan.json").read_text(encoding="utf-8"))
    validation_plan = json.loads((session_dir / "feature_surface_validation_plan.json").read_text(encoding="utf-8"))
    artifact_complete = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    input_resolution = json.loads((session_dir / "input_resolution.json").read_text(encoding="utf-8"))

    assert decision["decision"] == "ready_to_implement_feature_surface_batch1"
    assert decision["batch1_ready"] is True
    assert "conditional_high_value_flag" in batch1["selected_feature_names"]
    assert "decision_candle_quality" in batch1["selected_feature_names"]
    assert "higher_timeframe_headroom_bucket" in batch1["selected_feature_names"]
    assert "liquidity_quality_bucket" in batch1["selected_feature_names"]
    assert "entry_strength_score" in batch1["selected_feature_names"]
    assert inventory["summary"]["missing_and_requiring_new_upstream_source_count"] > 0
    assert any(row["feature_name"] == "volume_participation_bucket" for row in candidate_design["features"])
    assert build_plan["row_preservation_contract"]["preserve_original_row_count"] is True
    assert validation_plan["validation_axes"]
    assert artifact_complete["decision"] == "ready_to_implement_feature_surface_batch1"
    assert input_resolution["all_paths_exist"] is True

    coverage = pd.read_parquet(session_dir / "field_coverage_matrix.parquet")
    assert not coverage.empty
    assert set(["field_name", "category", "candidate_coverage", "unknown_coverage"]).issubset(coverage.columns)
    assert "vol_ratio5_20" in set(coverage["field_name"])
