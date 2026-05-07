from __future__ import annotations

from pathlib import Path

from scripts import tradex_shadow_reranker_forward_validation_v2 as mod


def test_forward_validation_v2_generates_readiness_and_validation_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(mod, "READINESS_ROOT", tmp_path / "readiness")
    monkeypatch.setattr(mod, "VALIDATION_ROOT", tmp_path / "validation")

    result = mod.run()

    readiness_dir = Path(result["readiness"]["output_dir"])
    validation_dir = Path(result["validation"]["output_dir"])

    assert result["readiness"]["decision"] == "ready_to_run_forward_validation"
    assert result["validation"]["decision"] == "insufficient_forward_sample"
    assert result["validation"]["validation_summary"]["comparison_summary"]["top5_forward_delta"] is not None

    readiness_expected = {
        "run_manifest.json",
        "input_resolution.json",
        "surface_discovery_summary.json",
        "forward_outcome_availability.json",
        "frozen_feature_contract_check.json",
        "forward_readiness_decision.json",
        "_ARTIFACT_COMPLETE.json",
    }
    validation_expected = {
        "run_manifest.json",
        "input_resolution.json",
        "forward_data_availability_audit.json",
        "forward_model_replay_contract.json",
        "forward_variant_pool_comparison.json",
        "forward_topk_membership_diff.parquet",
        "forward_stability_audit.json",
        "forward_leakage_audit.json",
        "shadow_reranker_forward_validation_v2_decision.json",
        "_ARTIFACT_COMPLETE.json",
    }

    assert readiness_expected == {path.name for path in readiness_dir.iterdir()}
    assert validation_expected.issubset({path.name for path in validation_dir.iterdir()})
