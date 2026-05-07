from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_shadow_reranker_forward_defer_summary_v1 import build_defer_summary


def test_defer_summary_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "research_defer_summaries"
    result = build_defer_summary(output_root, session_id="session-test")

    assert result["decision"] == "defer_shadow_reranker_forward_validation"
    assert result["status"] == "waiting_for_forward_surface"
    assert result["forward_candidate_row_count"] == 0
    assert result["jobs_supported"] == 1

    session_dir = Path(result["output_dir"])
    expected = {
        "run_manifest.json",
        "input_resolution.json",
        "defer_decision.json",
        "frozen_shadow_challenger_summary.json",
        "forward_data_gap_summary.json",
        "reopen_conditions.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    defer_decision = json.loads((session_dir / "defer_decision.json").read_text(encoding="utf-8"))
    assert defer_decision["decision"] == "defer_shadow_reranker_forward_validation"
    assert defer_decision["status"] == "waiting_for_forward_surface"
    assert defer_decision["promote_ready"] is False
    assert defer_decision["meemee_reflectable"] is False
    assert defer_decision["reason"] == "no_forward_validatable_rows_exist_beyond_frozen_challenger_window"

    frozen = json.loads((session_dir / "frozen_shadow_challenger_summary.json").read_text(encoding="utf-8"))
    assert frozen["model_type"] == "sklearn.ensemble.HistGradientBoostingRegressor"
    assert frozen["target_label"] == "path_value_score_v1"
    assert frozen["feature_count"] == 33
    assert frozen["known_risks"] == [
        "weak global OOS Spearman",
        "top-K-local signal",
        "top20 unchanged",
        "shadow-only result",
    ]

    gap = json.loads((session_dir / "forward_data_gap_summary.json").read_text(encoding="utf-8"))
    assert gap["latest_available_candidate_date"] == "2026-01-19"
    assert gap["candidate_row_count"] == 0
    assert gap["anchor_date_count"] == 0
    assert "candidate / feature surface" in gap["required_minimum_data_for_next_attempt"]["new_surface_required"]

    reopen = json.loads((session_dir / "reopen_conditions.json").read_text(encoding="utf-8"))
    assert "2026-01-19" in reopen["frozen_forward_window_end"]
    assert "MeeMee reflection" in reopen["do_not_reopen_for"]
