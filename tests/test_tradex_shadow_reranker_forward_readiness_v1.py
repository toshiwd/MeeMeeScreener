from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_shadow_reranker_forward_readiness_v1 import run_forward_readiness_checker


def test_forward_readiness_checker_emits_required_artifacts(tmp_path: Path) -> None:
    result = run_forward_readiness_checker(tmp_path / "shadow_reranker_forward_readiness_v1")

    assert result["decision"] == "waiting_for_new_candidate_surface"
    assert result["status"] == "waiting_for_new_candidate_surface"
    assert result["jobs_supported"] == 1
    assert result["newest_max_candidate_date"] == "2026-01-19"

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "surface_discovery_summary.json",
        "forward_outcome_availability.json",
        "frozen_feature_contract_check.json",
        "forward_readiness_decision.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    decision = json.loads((session_dir / "forward_readiness_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] == "waiting_for_new_candidate_surface"
    assert decision["frozen_challenger"] == "tree_hgb_path_value"
    assert decision["no_lookahead_passed"] is True

    surface = json.loads((session_dir / "surface_discovery_summary.json").read_text(encoding="utf-8"))
    assert surface["max_candidate_date"] == "2026-01-19"
    assert surface["newer_surface_exists_beyond_frozen_window"] is False
    assert surface["all_candidate_surfaces_with_no_lookahead_pass"] is True

    feature = json.loads((session_dir / "frozen_feature_contract_check.json").read_text(encoding="utf-8"))
    assert feature["feature_contract_matches"] is True
    assert feature["status"] == "pass"
