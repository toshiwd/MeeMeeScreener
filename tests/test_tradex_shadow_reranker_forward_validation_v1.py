from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_shadow_reranker_forward_validation_v1 import (
    run_shadow_reranker_forward_validation_v1,
)


def test_forward_validation_emits_insufficient_data_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "shadow_reranker_forward_validation_v1"
    result = run_shadow_reranker_forward_validation_v1(output_root=output_root, limit_anchor_dates=2, jobs=2)

    assert result["decision"] == "insufficient_forward_data"
    assert result["forward_validatable_row_count"] == 0
    assert result["jobs_supported"] == 1

    session_dir = Path(result["output_dir"])
    expected_files = [
        "run_manifest.json",
        "input_resolution.json",
        "forward_data_availability_audit.json",
        "forward_model_replay_contract.json",
        "forward_variant_pool_comparison.json",
        "forward_topk_membership_diff.parquet",
        "forward_stability_audit.json",
        "forward_leakage_audit.json",
        "shadow_reranker_forward_validation_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    for name in expected_files:
        assert (session_dir / name).exists(), name

    decision = json.loads((session_dir / "shadow_reranker_forward_validation_v1_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] == "insufficient_forward_data"
    assert decision["forward_validatable_row_count"] == 0
