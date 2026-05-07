from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_forward_surface_generation_readiness_v1 import run_forward_surface_generation_readiness


def test_forward_surface_generation_readiness_emits_required_artifacts(tmp_path: Path) -> None:
    result = run_forward_surface_generation_readiness(tmp_path / "forward_surface_generation_readiness_v1")

    assert result["decision"] == "pipeline_repair_required"
    assert result["status"] == "pipeline_repair_required"
    assert result["jobs_supported"] == 1
    assert result["latest_candidate_surface_date"] == "2026-01-19"
    assert result["latest_daily_bar_date"] == "2026-04-30"

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "forward_surface_data_freshness_audit.json",
        "forward_surface_pipeline_dependency_audit.json",
        "forward_surface_blocker_summary.json",
        "forward_surface_generation_recommendation.json",
        "forward_surface_generation_readiness_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    decision = json.loads((session_dir / "forward_surface_generation_readiness_v1_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] == "pipeline_repair_required"
    assert decision["recommended_next_action"] == "repair_feature_surface_pipeline"
    assert decision["frozen_challenger"] == "tree_hgb_path_value"

    freshness = json.loads((session_dir / "forward_surface_data_freshness_audit.json").read_text(encoding="utf-8"))
    assert freshness["table_dates"]["latest_daily_bar_date"] == "2026-04-30"
    assert freshness["table_dates"]["latest_feature_snapshot_date"] == "2026-04-30"
    assert freshness["table_dates"]["latest_ml_feature_daily_date"] == "2026-03-13"
    assert freshness["surface_window"]["latest_candidate_surface_date"] == "2026-01-19"
    assert freshness["surface_window"]["gap_days_available_market_vs_latest_candidate_surface"] == 101

    blockers = json.loads((session_dir / "forward_surface_blocker_summary.json").read_text(encoding="utf-8"))
    assert blockers["primary_blocker"] == "feature_surface_not_built"
    assert "forward_outcomes_not_matured" in blockers["secondary_blockers"]

    recommendation = json.loads((session_dir / "forward_surface_generation_recommendation.json").read_text(encoding="utf-8"))
    assert recommendation["next_action"] == "repair_feature_surface_pipeline"
