from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_ma_context_shape_direct_adjustment_freeze_summary_v1 import (
    build_freeze_artifacts,
    write_freeze_artifacts,
)


def test_build_freeze_artifacts_reports_frozen_lineage() -> None:
    artifacts = build_freeze_artifacts()

    lineage = artifacts["lineage_summary"]
    decision = artifacts["freeze_decision"]
    reusable = artifacts["remaining_reusable_signals"]
    next_axis = artifacts["next_axis_recommendation"]

    assert lineage["decision"] == "freeze_direct_ranking_adjustment"
    assert lineage["decision_reason"] == "analysis_signal_exists_but_same_condition_topk_improvement_failed"
    assert lineage["branches_reviewed"] == 8
    assert lineage["analysis_only_count"] == 1
    assert lineage["ranking_challenger_count"] == 7

    branch_names = {branch["branch_name"] for branch in lineage["branches"]}
    assert {
        "ma_state_family_bad_pick_pruner_v1",
        "ma_state_family_bad_pick_pruner_v1_1_narrow_penalty",
        "ma_state_family_regime_only_bad_pick_pruner_v1",
        "ma_state_family_risk_on_trend_bad_pick_pruner_v1",
        "ma_state_family_high_value_boost_v1",
        "multi_timeframe_context_gated_high_value_boost_v1",
        "conditional_high_value_candle_shape_modifier_v1",
        "context_gated_candle_shape_modifier_boost_prune_v1",
    } == branch_names

    assert decision["decision"] == "freeze_direct_ranking_adjustment"
    assert decision["authoritative_rollup_decision"] == "freeze_direct_ranking_adjustment"
    assert decision["retained_analysis_signal_count"] == 4

    assert len(reusable["signals"]) == 4
    assert next_axis["recommended_axis"] == "candidate_generation_pre_filtering_with_context_shape_signals"


def test_write_freeze_artifacts_emits_required_json(tmp_path: Path) -> None:
    session_root = write_freeze_artifacts(output_root=tmp_path / "freeze", session_id="session-test")

    expected = {
        "lineage_summary.json",
        "freeze_decision.json",
        "remaining_reusable_signals.json",
        "next_axis_recommendation.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in session_root.iterdir()}

    for name in expected:
        payload = json.loads((session_root / name).read_text(encoding="utf-8"))
        assert isinstance(payload, dict)

    lineage = json.loads((session_root / "lineage_summary.json").read_text(encoding="utf-8"))
    assert lineage["decision"] == "freeze_direct_ranking_adjustment"
    assert lineage["branches_reviewed"] == 8
