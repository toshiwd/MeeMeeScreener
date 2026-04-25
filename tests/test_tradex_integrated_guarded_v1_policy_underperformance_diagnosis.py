from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_integrated_guarded_v1_policy_underperformance_diagnosis import run_diagnosis


def test_policy_underperformance_diagnosis_detects_lower_bucket_long_drag(tmp_path: Path) -> None:
    output_dir = tmp_path / "integrated_guarded_v1_policy_underperformance"
    result = run_diagnosis(output_dir=output_dir)

    summary = result["summary"]
    assert summary["diagnosis_decision"] == "selection_keep_policy_repair_needed"
    assert summary["primary_failure_reason"] == "lower_rank_long_hold_only_drag"
    assert summary["top_contributing_rank_bucket"] == "top11_20"
    assert summary["top_contributing_side"] == "long"
    assert summary["whether_late_exit_remains_suppressed"] is True
    assert summary["whether_lower_bucket_drag_still_exists"] is True
    assert summary["selection_only_edge_preserved"] is True

    expected_files = [
        "integrated_guarded_v1_policy_underperformance_diagnosis.json",
        "integrated_guarded_v1_policy_gap_by_rank_bucket.json",
        "integrated_guarded_v1_policy_gap_by_side.json",
        "integrated_guarded_v1_policy_gap_by_action.json",
        "integrated_guarded_v1_policy_gap_by_anchor.json",
        "integrated_guarded_v1_policy_gap_by_symbol.json",
    ]
    for name in expected_files:
        assert (output_dir / name).exists(), name


def test_policy_underperformance_diagnosis_json_is_parseable(tmp_path: Path) -> None:
    output_dir = tmp_path / "integrated_guarded_v1_policy_underperformance_json"
    run_diagnosis(output_dir=output_dir)

    for name in [
        "integrated_guarded_v1_policy_underperformance_diagnosis.json",
        "integrated_guarded_v1_policy_gap_by_rank_bucket.json",
        "integrated_guarded_v1_policy_gap_by_side.json",
        "integrated_guarded_v1_policy_gap_by_action.json",
        "integrated_guarded_v1_policy_gap_by_anchor.json",
        "integrated_guarded_v1_policy_gap_by_symbol.json",
    ]:
        payload = json.loads((output_dir / name).read_text(encoding="utf-8"))
        assert payload
