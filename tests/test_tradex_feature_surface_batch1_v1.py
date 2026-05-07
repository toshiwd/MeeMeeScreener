from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_feature_surface_batch1_v1 import run_feature_surface_batch1_v1


FEATURE_SURFACE = Path(
    r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\candidate_prefilter_rows_context_enriched.parquet"
)
PLAN_SESSION = Path(r"G:\Tradex\feature_surface_upgrade_plan_v1\20260501T091723Z-838354")
ORFP_SESSION = Path(r"G:\Tradex\research_freeze_summaries\observable_regime_false_positive_require_confirmation\20260501T090354Z-098449")
REBUILD_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_bottom15_summary_rebuild_v1\20260501T085017Z-012155")


def test_feature_surface_batch1_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_feature_surface_batch1_v1(
        output_root=tmp_path / "feature_surface_batch1_v1",
        feature_surface=FEATURE_SURFACE,
        plan_session=PLAN_SESSION,
        orfp_session=ORFP_SESSION,
        rebuild_session=REBUILD_SESSION,
    )

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "feature_formula_contract.json",
        "feature_coverage_summary.json",
        "feature_missingness_summary.json",
        "no_lookahead_feature_audit.json",
        "added_top15_vs_bottom15_feature_contrast.json",
        "observable_regime_false_positive_feature_summary.json",
        "feature_surface_batch1_v1_decision.json",
        "candidate_prefilter_rows_feature_enriched_v1.parquet",
        "observable_regime_false_positive_feature_enriched_v1.parquet",
        "_ARTIFACT_COMPLETE.json",
        "field_level_coverage_matrix.parquet",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    enriched = pd.read_parquet(session_dir / "candidate_prefilter_rows_feature_enriched_v1.parquet")
    orfp = pd.read_parquet(session_dir / "observable_regime_false_positive_feature_enriched_v1.parquet")

    assert len(enriched) == 2542
    assert "conditional_high_value_flag" in enriched.columns
    assert "entry_strength_score" in enriched.columns
    assert "signal_quality_bucket" in enriched.columns
    assert "decision_candle_quality" in enriched.columns
    assert "volume_participation_bucket" in enriched.columns
    assert "liquidity_quality_bucket" in enriched.columns
    assert "higher_timeframe_headroom_bucket" in enriched.columns

    feature_formula = json.loads((session_dir / "feature_formula_contract.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "feature_surface_batch1_v1_decision.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "feature_coverage_summary.json").read_text(encoding="utf-8"))
    no_lookahead = json.loads((session_dir / "no_lookahead_feature_audit.json").read_text(encoding="utf-8"))
    contrast = json.loads((session_dir / "added_top15_vs_bottom15_feature_contrast.json").read_text(encoding="utf-8"))
    orfp_summary = json.loads((session_dir / "observable_regime_false_positive_feature_summary.json").read_text(encoding="utf-8"))

    assert feature_formula["schema_version"].startswith("tradex_feature_surface_batch1_v1")
    assert decision["decision"] == "ready_to_rerun_bad_pick_reclassification_with_batch1_features"
    assert no_lookahead["candidate_surface"]["status"] == "pass"
    assert no_lookahead["orfp_surface"]["status"] == "pass"
    assert coverage["features"]["conditional_high_value_flag"]["candidate"]["coverage_rate"] >= 0.97
    assert coverage["features"]["decision_candle_quality"]["candidate"]["coverage_rate"] >= 0.97
    assert coverage["features"]["higher_timeframe_headroom_bucket"]["candidate"]["coverage_rate"] >= 0.97
    assert coverage["features"]["liquidity_quality_bucket"]["candidate"]["coverage_rate"] >= 0.97
    assert coverage["features"]["volume_participation_bucket"]["candidate"]["coverage_rate"] == 1.0
    assert coverage["features"]["volume_participation_bucket"]["candidate"]["missing_reason_distribution"].get("vol_ratio5_20", 0) > 0
    assert contrast["topk"]["top5"]["added_top15_count"] >= 0
    assert orfp_summary["family_code"] == "observable_regime_false_positive"
