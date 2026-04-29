from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_candidate_generation_pre_filter_context_shape_v1 import (
    _build_prefilter_bucket,
    _build_prefilter_reason,
    write_artifacts,
)


CANDIDATE_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
SHAPE_SESSION = Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e")
FREEZE_SESSION = Path(r"G:\Tradex\research_freeze_summaries\ma_context_shape_direct_adjustment_line\20260429T143302Z-8f34ef9d")


def test_prefilter_bucket_rules_cover_expected_cases() -> None:
    positive = pd.Series(
        {
            "conditional_high_value": True,
            "shape_classification": "shape_positive_modifier",
            "shape_joined": True,
            "stable_bad_pick_family": False,
            "family_classification": "stable_high_value_family",
            "family_bad_pick_regime": "C:risk_on_trend",
            "dominant_regime_context": "C:risk_on_trend",
        }
    )
    watch = pd.Series(
        {
            "conditional_high_value": True,
            "shape_classification": "shape_context_dependent",
            "shape_joined": True,
            "stable_bad_pick_family": False,
            "family_classification": "stable_high_value_family",
            "family_bad_pick_regime": "C:risk_on_trend",
            "dominant_regime_context": "C:risk_on_trend",
        }
    )
    downgrade = pd.Series(
        {
            "conditional_high_value": False,
            "shape_classification": "shape_missing",
            "shape_joined": False,
            "stable_bad_pick_family": False,
            "family_classification": "regime_dependent_family",
            "family_bad_pick_regime": "C:risk_on_trend",
            "dominant_regime_context": "C:risk_on_trend",
        }
    )
    exclude = pd.Series(
        {
            "conditional_high_value": False,
            "shape_classification": "shape_missing",
            "shape_joined": False,
            "stable_bad_pick_family": True,
            "family_classification": "stable_bad_pick_family",
            "family_bad_pick_regime": "C:risk_on_trend",
            "dominant_regime_context": "C:risk_on_trend",
        }
    )

    assert _build_prefilter_bucket(positive) == "KEEP_PRIMARY"
    assert _build_prefilter_bucket(watch) == "KEEP_WATCH"
    assert _build_prefilter_bucket(downgrade) == "DOWNGRADE"
    assert _build_prefilter_bucket(exclude) == "EXCLUDE"
    assert _build_prefilter_reason(positive) == ["conditional_high_value", "positive_shape_modifier"]
    assert "bad_pick_diagnostic" in _build_prefilter_reason(downgrade)


def test_candidate_generation_pre_filter_smoke_run_writes_required_artifacts(tmp_path: Path) -> None:
    output_root = tmp_path / "candidate_generation_pre_filter_context_shape"
    session_dir = write_artifacts(
        output_root=output_root,
        candidate_input_dir=CANDIDATE_INPUT_DIR,
        family_session=FAMILY_SESSION,
        context_session=CONTEXT_SESSION,
        shape_session=SHAPE_SESSION,
        freeze_session=FREEZE_SESSION,
        limit_anchor_dates=2,
    )

    assert session_dir.exists()
    required_files = (
        "run_manifest.json",
        "candidate_prefilter_policy.json",
        "candidate_prefilter_coverage_summary.json",
        "candidate_pool_comparison.json",
        "monthly_comparison.json",
        "context_comparison.json",
        "topk_membership_diff.parquet",
        "candidate_prefilter_rows.parquet",
        "candidate_generation_pre_filter_context_shape_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    policy = json.loads((session_dir / "candidate_prefilter_policy.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "candidate_prefilter_coverage_summary.json").read_text(encoding="utf-8"))
    compare = json.loads((session_dir / "candidate_pool_comparison.json").read_text(encoding="utf-8"))
    monthly = json.loads((session_dir / "monthly_comparison.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "candidate_generation_pre_filter_context_shape_v1_decision.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_candidate_generation_pre_filter_context_shape_v1_manifest_v1"
    assert manifest["same_condition_contract"]["candidate_universe"] == "integrated_guarded_v1_candidate_snapshots"
    assert policy["schema_version"] == "tradex_candidate_generation_pre_filter_context_shape_v1_policy_v1"
    assert policy["freeze_reference"]["freeze_decision"] == "freeze_direct_ranking_adjustment"
    assert coverage["no_lookahead_inherited"] is True
    assert coverage["join_coverage"]["shape_join_rate"] is not None
    assert compare["same_condition_contract"]["pre_filter_is_analysis_only"] is True
    assert monthly["topk"]["5"]["summary_by_pool"]["prefilter_primary_only"]["zero_pass_month_count"] >= 0
    assert decision["authoritative_rollup_decision"] in {"keep", "hold", "drop"}

    parquet = pd.read_parquet(session_dir / "candidate_prefilter_rows.parquet")
    assert not parquet.empty
    assert {"prefilter_bucket", "prefilter_reason", "shape_classification", "conditional_high_value"}.issubset(set(parquet.columns))
    assert parquet["prefilter_bucket"].isin({"KEEP_PRIMARY", "KEEP_WATCH", "DOWNGRADE", "EXCLUDE"}).all()
