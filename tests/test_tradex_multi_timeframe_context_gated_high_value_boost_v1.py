from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_multi_timeframe_context_gated_high_value_boost_v1 import (
    run_multi_timeframe_context_gated_high_value_boost_v1,
)


CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
PRIOR_GLOBAL_BOOST_SESSION = Path(r"G:\Tradex\ma_state_family_high_value_boost_v1\20260429T084326Z-2ae0f0de")


def test_context_gated_high_value_boost_smoke_run(tmp_path: Path) -> None:
    output_root = tmp_path / "context_gated_high_value_boost"
    result = run_multi_timeframe_context_gated_high_value_boost_v1(
        source_context_session=CONTEXT_SESSION,
        source_family_session=SOURCE_FAMILY_SESSION,
        prior_global_boost_session=PRIOR_GLOBAL_BOOST_SESSION,
        output_root=output_root,
        limit_anchor_dates=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()

    required_files = (
        "run_manifest.json",
        "multi_timeframe_context_gated_high_value_boost_v1_compare.json",
        "multi_timeframe_context_gated_high_value_boost_v1_decision.json",
        "boost_coverage_summary.json",
        "global_boost_vs_context_gated_delta.json",
        "monthly_comparison.json",
        "context_comparison.json",
        "topk_membership_diff.parquet",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    compare = json.loads((session_dir / "multi_timeframe_context_gated_high_value_boost_v1_compare.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "multi_timeframe_context_gated_high_value_boost_v1_decision.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "boost_coverage_summary.json").read_text(encoding="utf-8"))
    delta = json.loads((session_dir / "global_boost_vs_context_gated_delta.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_multi_timeframe_context_gated_high_value_boost_v1_manifest_v1"
    assert manifest["no_lookahead_inherited"] is True
    assert manifest["monthly_context_no_lookahead"] is True
    assert manifest["weekly_context_no_lookahead"] is True
    assert manifest["source_context_session_id"] == "20260429T091138Z-7d26cb7c"
    assert manifest["source_family_session_id"] == "20260429T062945Z-87844c56"
    assert manifest["prior_global_boost_session_id"] == "20260429T084326Z-2ae0f0de"
    assert coverage["conditional_high_value_gate_count"] == 1608
    assert coverage["matched_context_rate"] is not None
    assert coverage["boost_applied_rows"] >= 0
    assert compare["same_condition_contract"]["universe"] == "integrated_guarded_v1_candidate_snapshots"
    assert compare["boost_formula"]["conditional_high_value_boost"] == 0.06
    assert decision["recommendation"] in {"keep", "hold", "drop"}
    assert delta["schema_version"] == "tradex_multi_timeframe_context_gated_high_value_boost_v1_global_boost_delta_v1"

    parquet = pd.read_parquet(session_dir / "topk_membership_diff.parquet")
    assert not parquet.empty
    assert {"score_adjustment", "challenger_score", "conditional_high_value", "state_family_id"}.issubset(set(parquet.columns))

