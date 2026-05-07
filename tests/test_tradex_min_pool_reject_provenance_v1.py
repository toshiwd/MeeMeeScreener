from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_native_rejected_row_logging_v1 import _best_provenance_row
from scripts.tradex_min_pool_reject_provenance_v1 import _materialize_exact_artifacts


def test_best_provenance_row_prefers_tier_over_reason_only() -> None:
    reason_only = pd.Series(
        {
            "candidate_pool_tier": None,
            "candidate_pool_reason": "raw_source_backfill_high_recall",
        }
    )
    tier_row = pd.Series(
        {
            "candidate_pool_tier": "KEEP_PRIMARY",
            "candidate_pool_reason": "broad_prefilter_strict_pool",
        }
    )
    chosen = _best_provenance_row(reason_only, tier_row)
    assert chosen is tier_row


def test_materialize_exact_artifacts_writes_exact_bundle_aliases(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()

    json_payload = {"ok": True}
    source_jsons = {
        "min_pool_reject_hook_inventory.json": {"hooks": []},
        "min_pool_rejected_row_logging_summary.json": json_payload,
        "min_pool_reject_reason_bucket_summary.json": json_payload,
        "min_pool_long_side_refinement_audit.json": {"decision_hint": "native_logging_still_insufficient", "feasibility": {}},
        "min_pool_long_side_refinement_recommendation.json": {"recommended_next_action": "native_logging_still_insufficient", "reason": "stub"},
        "min_pool_rejected_row_logging_v1_decision.json": {"decision": "native_logging_still_insufficient", "status": "native_logging_still_insufficient", "reason": "stub"},
        "min_pool_stage_row_count_reconciliation.json": json_payload,
        "run_manifest.json": json_payload,
        "input_resolution.json": json_payload,
        "min_pool_long_side_top15_loss_trace.json": json_payload,
    }
    for name, payload in source_jsons.items():
        (bundle_root / name).write_text(json.dumps(payload), encoding="utf-8")

    frame = pd.DataFrame([{"value": 1}])
    for name in [
        "min_pool_candidate_admission_trace_rows.parquet",
        "min_pool_rejected_candidate_rows.parquet",
        "min_pool_accepted_candidate_rows.parquet",
        "min_pool_long_side_top15_loss_trace_rows.parquet",
    ]:
        frame.to_parquet(bundle_root / name, index=False)

    _materialize_exact_artifacts(bundle_root)

    expected = [
        "min_pool_decision_logic_inventory.json",
        "min_pool_reject_provenance_schema.json",
        "min_pool_provenance_trace_rows.parquet",
        "min_pool_rejected_rows.parquet",
        "min_pool_accepted_rows.parquet",
        "min_pool_provenance_logging_summary.json",
        "min_pool_reject_subreason_summary.json",
        "min_pool_refinement_audit.json",
        "min_pool_refinement_recommendation.json",
        "min_pool_reject_provenance_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    for name in expected:
        assert (bundle_root / name).exists(), name

    artifact_complete = json.loads((bundle_root / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert "min_pool_reject_provenance_schema.json" in artifact_complete["required_artifacts"]
    assert "min_pool_reject_provenance_v1_decision.json" in artifact_complete["required_artifacts"]

    decision = json.loads((bundle_root / "min_pool_reject_provenance_v1_decision.json").read_text(encoding="utf-8"))
    recommendation = json.loads((bundle_root / "min_pool_refinement_recommendation.json").read_text(encoding="utf-8"))
    assert decision["decision"] == "min_pool_provenance_still_insufficient"
    assert recommendation["recommended_next_action"] == "min_pool_provenance_still_insufficient"
