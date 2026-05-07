from __future__ import annotations

from pathlib import Path

from scripts.tradex_champion_top5_capture_boundary_promoter_v1_review_gates import (
    _build_freeze_outputs,
    _publish_review_outputs,
    _resolve_run_root,
)
from scripts.tradex_reflectability_funnel_common_v1 import _load_json


SOURCE_ROOT = _resolve_run_root(Path(r"G:\Tradex\champion_top5_capture_boundary_promoter_v1"))


def _replay_stub(source_root: Path):
    compare = _load_json(source_root / "compare.json")
    monthly = _load_json(source_root / "monthly_top5_capture_summary.json")
    topk = _load_json(source_root / "topk_effectiveness_summary.json")
    branching = _load_json(source_root / "branching_probe.json")
    promo = _load_json(source_root / "promotion_quality_summary.json")
    decision = _load_json(source_root / "decision_summary.json")
    return {
        "compare": compare,
        "monthly_top5_capture_summary": monthly,
        "topk_effectiveness_summary": topk,
        "branching_probe": branching,
        "promotion_quality_summary": promo,
        "decision_summary": decision,
    }


def test_keep_freeze_outputs_capture_keep_evidence(tmp_path: Path) -> None:
    payload = _build_freeze_outputs(source_root=SOURCE_ROOT, output_root=tmp_path / "freeze")

    freeze = payload["freeze_decision"]
    promotion_contract = payload["promotion_contract_summary"]
    keep_evidence = payload["keep_evidence_summary"]

    assert freeze["decision"] == "keep_freeze"
    assert freeze["final_decision"] == "keep"
    assert freeze["source_artifact_root"] == str(SOURCE_ROOT.resolve())
    assert "monthly_top5_capture_improved" in freeze["keep_reasons"]
    assert promotion_contract["static_gate_mode"] == "static_non_optimized_v1"
    assert promotion_contract["same_condition_contract"]["same_universe"] is True
    assert keep_evidence["anti_leakage_passed"] is True
    assert keep_evidence["monthly_top5_capture_delta_mean"] > 0

    for name in [
        "freeze_decision.json",
        "freeze_reason.json",
        "keep_evidence_summary.json",
        "reusable_findings.json",
        "non_reusable_findings.json",
        "promotion_contract_summary.json",
        "_ARTIFACT_COMPLETE.json",
    ]:
        assert Path(payload["artifact_paths"][name]).exists()


def test_publish_review_gate_passes_and_writes_shadow_bundle(tmp_path: Path) -> None:
    freeze_payload = _build_freeze_outputs(source_root=SOURCE_ROOT, output_root=tmp_path / "freeze")
    publish_payload = _publish_review_outputs(
        source_root=SOURCE_ROOT,
        freeze_root=Path(freeze_payload["output_root"]),
        output_root=tmp_path / "review",
        bundle_root=tmp_path / "bundle",
        replay_runner=lambda replay_root: _replay_stub(SOURCE_ROOT),
    )

    assert publish_payload["decision"] == "pass_to_manual_review"
    assert publish_payload["publish_review_decision"]["decision_reason"] == "source_ready_for_manual_review"
    assert publish_payload["feature_availability_audit"]["pass"] is True
    assert publish_payload["feature_availability_audit"]["features"][2]["feature"] == "path_value_score_v1"
    assert publish_payload["feature_availability_audit"]["features"][2]["available_in_regular_ranking_generation"] is True
    assert publish_payload["reproducibility_audit"]["matches_within_tolerance"] is True
    assert publish_payload["anti_leakage_recheck"]["pass"] is True
    assert publish_payload["meemee_exposure_assessment"]["suitable_for"] == "publish_review_only"
    assert publish_payload["meemee_exposure_assessment"]["is_reflectable_to_meemee_now"] is False
    assert publish_payload["shadow_publish_bundle_manifest"]["bundle_status"] == "complete"

    for name in [
        "publish_review_contract.json",
        "source_artifact_integrity.json",
        "feature_availability_audit.json",
        "ranking_adjustment_contract.json",
        "static_gate_contract.json",
        "reproducibility_audit.json",
        "anti_leakage_recheck.json",
        "shadow_publish_bundle_manifest.json",
        "publish_review_decision.json",
        "meemee_exposure_assessment.json",
        "_ARTIFACT_COMPLETE.json",
    ]:
        assert Path(publish_payload["artifact_paths"][name]).exists()

    for name in [
        "published_logic_artifact.json",
        "published_logic_manifest.json",
        "validation_summary.json",
        "source_artifact_refs.json",
        "ranking_adjustment_contract.json",
        "meemee_exposure_assessment.json",
        "bundle_manifest.json",
    ]:
        assert (tmp_path / "bundle" / name).exists()

