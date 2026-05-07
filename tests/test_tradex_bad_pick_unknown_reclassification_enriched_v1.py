from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_bad_pick_unknown_reclassification_enriched_v1 import (
    run_bad_pick_unknown_reclassification_enriched_v1,
)


BACKFILL_SESSION = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646")
ENRICHED_CANDIDATE = BACKFILL_SESSION / "candidate_prefilter_rows_context_enriched.parquet"
ENRICHED_UNKNOWN = BACKFILL_SESSION / "unknown_reclassification_rows_context_enriched.parquet"
ORIGINAL_BAD_PICK_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
ORIGINAL_RECLASSIFICATION_ROWS = Path(r"G:\Tradex\bad_pick_unknown_reclassification_v1\20260501T043137Z-302dd27c\unknown_reclassification_rows.parquet")


def test_enriched_unknown_reclassification_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "bad_pick_unknown_reclassification_enriched_v1"
    result = run_bad_pick_unknown_reclassification_enriched_v1(
        candidate_enriched_path=ENRICHED_CANDIDATE,
        unknown_enriched_path=ENRICHED_UNKNOWN,
        backfill_session=BACKFILL_SESSION,
        original_bad_pick_session=ORIGINAL_BAD_PICK_SESSION,
        original_reclassification_rows=ORIGINAL_RECLASSIFICATION_ROWS,
        output_root=output_root,
        limit_anchor_dates=2,
        jobs=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()
    required_files = (
        "run_manifest.json",
        "input_resolution.json",
        "enriched_input_validation.json",
        "enriched_unknown_cohort_summary.json",
        "before_after_reclassification_summary.json",
        "enriched_unknown_reclassification_rows.parquet",
        "enriched_unknown_boundary_pairwise.parquet",
        "enriched_unknown_boundary_pairwise_summary.json",
        "enriched_root_cause_taxonomy_summary.json",
        "enriched_root_cause_family_breakdown.json",
        "future_challenger_candidates.json",
        "remaining_data_gap_recommendations.json",
        "bad_pick_unknown_reclassification_enriched_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    validation = json.loads((session_dir / "enriched_input_validation.json").read_text(encoding="utf-8"))
    cohort = json.loads((session_dir / "enriched_unknown_cohort_summary.json").read_text(encoding="utf-8"))
    taxonomy = json.loads((session_dir / "enriched_root_cause_taxonomy_summary.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "bad_pick_unknown_reclassification_enriched_v1_decision.json").read_text(encoding="utf-8"))
    artifact = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert validation["no_lookahead_audit_status"] == "pass"
    assert validation["forbidden_fields_used_as_policy_inputs"] == []
    assert cohort["unknown_count"] >= 0
    assert taxonomy["unknown_count"] == cohort["unknown_count"]
    assert decision["decision"] in {
        "ready_for_single_axis_challenger_design",
        "data_pipeline_improvement_required",
        "explanation_only",
        "insufficient_signal",
    }
    assert artifact["parse_status"]["run_manifest"] is True
    assert artifact["row_reconciliation"]["unknown_row_count_preserved"] == 1

    enriched_unknown = pd.read_parquet(session_dir / "enriched_unknown_reclassification_rows.parquet")
    pairwise = pd.read_parquet(session_dir / "enriched_unknown_boundary_pairwise.parquet")
    assert not enriched_unknown.empty
    assert not pairwise.empty
    assert "monthly_main_state_ctx_backfilled" in enriched_unknown.columns
    assert "reclassified_root_cause_code" in enriched_unknown.columns
    assert enriched_unknown["reclassified_root_cause_code"].notna().all()
    assert {"best_near_miss_rank", "best_near_miss_symbol", "score_gap", "forward_ret_20d_gap", "path_value_gap"}.issubset(set(pairwise.columns))
