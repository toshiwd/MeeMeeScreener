from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_bad_pick_unknown_reclassification_v1 import (
    run_bad_pick_unknown_reclassification_v1,
)


SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
BAD_PICK_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
SELECTION_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_selection_only_ledger.json"
)
POLICY_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json"
)


def test_unknown_reclassification_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "bad_pick_unknown_reclassification_v1"
    result = run_bad_pick_unknown_reclassification_v1(
        source_rows_parquet=SOURCE_ROWS_PARQUET,
        bad_pick_session=BAD_PICK_SESSION,
        selection_ledger_path=SELECTION_LEDGER,
        policy_ledger_path=POLICY_LEDGER,
        output_root=output_root,
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()
    required_files = (
        "run_manifest.json",
        "input_resolution.json",
        "unknown_cohort_summary.json",
        "missingness_audit_summary.json",
        "unknown_reclassification_rows.parquet",
        "unknown_boundary_pairwise.parquet",
        "unknown_boundary_pairwise_summary.json",
        "new_root_cause_taxonomy_summary.json",
        "new_root_cause_family_breakdown.json",
        "data_gap_recommendations.json",
        "future_challenger_candidates.json",
        "bad_pick_unknown_reclassification_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    input_resolution = json.loads((session_dir / "input_resolution.json").read_text(encoding="utf-8"))
    cohort = json.loads((session_dir / "unknown_cohort_summary.json").read_text(encoding="utf-8"))
    missingness = json.loads((session_dir / "missingness_audit_summary.json").read_text(encoding="utf-8"))
    taxonomy = json.loads((session_dir / "new_root_cause_taxonomy_summary.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "bad_pick_unknown_reclassification_v1_decision.json").read_text(encoding="utf-8"))
    artifact = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_bad_pick_unknown_reclassification_v1_manifest_v1"
    assert input_resolution["authoritative_for_audit"] is True
    assert cohort["unknown_count"] >= 0
    assert "missing_context_data" in missingness["missingness_category_counts"]
    assert taxonomy["unknown_count"] == cohort["unknown_count"]
    assert decision["decision"] in {
        "ready_for_single_axis_challenger_design",
        "data_pipeline_improvement_required",
        "explanation_only",
        "insufficient_signal",
    }
    assert artifact["parse_status"]["run_manifest"] is True
    assert artifact["row_reconciliation"]["unknown_rows_reclassified"] == cohort["unknown_count"]

    unknown_rows = pd.read_parquet(session_dir / "unknown_reclassification_rows.parquet")
    pairwise = pd.read_parquet(session_dir / "unknown_boundary_pairwise.parquet")
    assert not unknown_rows.empty
    assert not pairwise.empty
    assert {
        "reclassified_root_cause_code",
        "reclassification_confidence",
        "missingness_class",
        "evidence_fields_used",
        "missing_fields",
    }.issubset(set(unknown_rows.columns))
    assert {"best_near_miss_rank", "best_near_miss_symbol", "score_gap", "forward_ret_20d_gap", "path_value_gap"}.issubset(set(pairwise.columns))
