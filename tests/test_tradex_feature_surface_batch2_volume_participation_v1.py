from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_feature_surface_batch2_volume_participation_v1 import (
    _build_volume_repair_coverage_summary,
    _repair_volume_features,
    run_batch2_volume_participation,
)


def test_batch2_volume_participation_smoke_writes_required_artifacts(tmp_path: Path) -> None:
    result = run_batch2_volume_participation(
        output_root=tmp_path / "feature_surface_batch2_volume_participation_v1",
        batch1_session=Path(r"G:\Tradex\feature_surface_batch1_v1\20260501T093159Z-820266"),
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["output_dir"])
    required = {
        "run_manifest.json",
        "input_resolution.json",
        "volume_join_contract.json",
        "volume_feature_formula_contract.json",
        "volume_repair_coverage_summary.json",
        "volume_feature_missingness_summary.json",
        "no_lookahead_volume_feature_audit.json",
        "added_top15_vs_bottom15_volume_contrast.json",
        "orfp_volume_feature_summary.json",
        "feature_surface_batch2_volume_participation_v1_decision.json",
        "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet",
        "observable_regime_false_positive_batch2_volume_enriched_v1.parquet",
        "_ARTIFACT_COMPLETE.json",
    }
    names = {path.name for path in session_dir.iterdir()}
    assert required.issubset(names)

    decision = json.loads((session_dir / "feature_surface_batch2_volume_participation_v1_decision.json").read_text(encoding="utf-8"))
    no_lookahead = json.loads((session_dir / "no_lookahead_volume_feature_audit.json").read_text(encoding="utf-8"))
    join_contract = json.loads((session_dir / "volume_join_contract.json").read_text(encoding="utf-8"))
    formula_contract = json.loads((session_dir / "volume_feature_formula_contract.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "volume_repair_coverage_summary.json").read_text(encoding="utf-8"))
    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    input_resolution = json.loads((session_dir / "input_resolution.json").read_text(encoding="utf-8"))

    assert decision["decision"] == "ready_to_rerun_reclassification_with_batch2_volume"
    assert no_lookahead["candidate_surface"]["status"] == "pass"
    assert no_lookahead["orfp_surface"]["status"] == "pass"
    assert join_contract["ml_feature_daily_join"]["no_lookahead_rule"]
    assert "vol_ratio5_20_repaired" in formula_contract["features"]
    assert "volume_zscore_20" in formula_contract["features"]
    assert coverage["features"]["vol_ratio5_20_repaired"]["candidate"]["coverage_rate"] is not None
    assert coverage["features"]["volume_zscore_20"]["candidate"]["coverage_rate"] is not None
    assert manifest["input_roots"]["batch1_session"].endswith(r"feature_surface_batch1_v1\20260501T093159Z-820266")
    assert input_resolution["resolved_paths"]["batch1_session"].endswith(r"feature_surface_batch1_v1\20260501T093159Z-820266")

    candidate = pd.read_parquet(session_dir / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet")
    orfp = pd.read_parquet(session_dir / "observable_regime_false_positive_batch2_volume_enriched_v1.parquet")
    assert len(candidate) > 0
    assert len(orfp) > 0
    for column in (
        "vol_ratio5_20_repaired",
        "vol_ratio5_20_repair_status",
        "volume_zscore_20",
        "turnover_value_ratio5_20",
        "participation_quality_bucket",
        "volume_confirmation_repaired_flag",
    ):
        assert column in candidate.columns
        assert column in orfp.columns


def test_repair_volume_features_handles_missing_candidate_column() -> None:
    frame = pd.DataFrame(
        {
            "ml_vol_ratio5_20": [1.5, None],
            "anchor_dt": [1, 2],
        }
    )

    repaired = _repair_volume_features(frame)

    assert "vol_ratio5_20_repaired" in repaired.columns
    assert repaired.loc[0, "vol_ratio5_20_repaired"] == 1.5
    assert repaired.loc[1, "vol_ratio5_20_repaired"] is pd.NA or pd.isna(repaired.loc[1, "vol_ratio5_20_repaired"])
    assert repaired.loc[0, "vol_ratio5_20_repair_status"] == "repaired"


def test_volume_repair_coverage_summary_handles_nullable_boolean_status() -> None:
    frame = pd.DataFrame(
        {
            "vol_ratio5_20_repaired": [1.0, None, 2.0],
            "vol_ratio5_20_repair_status": pd.Series([True, None, False], dtype="boolean"),
            "vol_ratio5_20_repair_missing_reason": ["", "missing source", ""],
            "volume_participation_bucket": ["high", "low", "high"],
            "volume_participation_bucket_feature_status": pd.Series([True, None, False], dtype="boolean"),
            "volume_participation_bucket_missing_reason": ["", "missing", ""],
            "volume_zscore_20": [0.1, 0.2, None],
            "volume_zscore_20_feature_status": pd.Series([False, None, True], dtype="boolean"),
            "volume_zscore_20_missing_reason": ["", "", "missing source"],
            "turnover_value_ratio5_20": [0.5, None, 0.8],
            "turnover_value_ratio5_20_feature_status": pd.Series([True, True, None], dtype="boolean"),
            "turnover_value_ratio5_20_missing_reason": ["", "", "missing source"],
            "participation_quality_bucket": ["a", "b", None],
            "participation_quality_bucket_feature_status": pd.Series([None, True, False], dtype="boolean"),
            "participation_quality_bucket_missing_reason": ["missing", "", ""],
            "volume_confirmation_repaired_flag": [True, False, None],
            "volume_confirmation_repaired_flag_feature_status": pd.Series([True, None, False], dtype="boolean"),
            "volume_confirmation_repaired_flag_missing_reason": ["", "missing", ""],
        }
    )

    summary = _build_volume_repair_coverage_summary(frame, frame)

    vol_status = summary["features"]["vol_ratio5_20_repaired"]["candidate"]["status_distribution"]
    participation_status = summary["features"]["participation_quality_bucket"]["candidate"]["status_distribution"]
    assert vol_status["True"] == 1
    assert vol_status["False"] == 1
    assert vol_status["missing"] == 1
    assert participation_status["missing"] == 1
