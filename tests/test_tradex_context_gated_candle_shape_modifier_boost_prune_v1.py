from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_context_gated_candle_shape_modifier_boost_prune_v1 import (
    _apply_shape_adjustments,
    _build_shape_lookup,
    _score_shape_adjustment,
    run_context_gated_candle_shape_modifier_boost_prune_v1,
)


CONTEXT_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
SHAPE_SESSION = Path(r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e")
SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
PRIOR_GLOBAL_BOOST_SESSION = Path(r"G:\Tradex\ma_state_family_high_value_boost_v1\20260429T084326Z-2ae0f0de")
PRIOR_CONTEXT_GATED_BOOST_SESSION = Path(r"G:\Tradex\multi_timeframe_context_gated_high_value_boost_v1\20260429T094730Z-7e1acdee")


def test_score_shape_adjustment_respects_gate_and_shape_classification() -> None:
    boost = _score_shape_adjustment(
        conditional_high_value=True,
        candle_shape_modifier="gap_down_bear",
        shape_classification="shape_positive_modifier",
        delta_path_value_score_v1=0.5,
        positive_modifiers={"gap_down_bear"},
        context_dependent_modifiers={"upper_wick_then_bear"},
        prune_enabled=True,
    )
    prune = _score_shape_adjustment(
        conditional_high_value=True,
        candle_shape_modifier="upper_wick_then_bear",
        shape_classification="shape_context_dependent",
        delta_path_value_score_v1=-0.1,
        positive_modifiers={"gap_down_bear"},
        context_dependent_modifiers={"upper_wick_then_bear"},
        prune_enabled=True,
    )
    none = _score_shape_adjustment(
        conditional_high_value=False,
        candle_shape_modifier="gap_down_bear",
        shape_classification="shape_positive_modifier",
        delta_path_value_score_v1=0.5,
        positive_modifiers={"gap_down_bear"},
        context_dependent_modifiers={"upper_wick_then_bear"},
        prune_enabled=True,
    )

    assert boost == (0.04, "conditional_high_value:shape_positive_modifier", True, False)
    assert prune == (-0.03, "conditional_high_value:shape_context_dependent_negative_delta", False, True)
    assert none == (0.0, "outside_conditional_high_value", False, False)


def test_apply_shape_adjustments_leaves_unmatched_rows_unboosted() -> None:
    shape_lookup_frame = pd.DataFrame(
        [
            {
                "candle_shape_modifier": "gap_down_bear",
                "shape_classification": "shape_positive_modifier",
                "delta_mean_path_value_score_v1": 0.12,
                "delta_mean_forward_ret_20d": 0.05,
                "delta_bottom15_rate": -0.03,
                "delta_top15_rate": 0.04,
                "sample_count": 100,
                "unique_symbol_count": 20,
                "month_count": 8,
                "mean_forward_ret_5d": 0.01,
                "mean_forward_ret_10d": 0.02,
                "mean_forward_ret_20d": 0.03,
                "median_forward_ret_20d": 0.03,
                "mean_path_value_score_v1": 0.04,
                "median_path_value_score_v1": 0.04,
                "mean_mfe_20d": 0.10,
                "mean_mae_20d": -0.02,
                "plus5_before_minus5_rate": 0.6,
                "minus5_before_plus5_rate": 0.4,
                "top15_rate": 0.2,
                "bottom15_rate": 0.1,
                "positive_month_rate": 0.7,
                "worst_month_mean_path_value": -0.1,
                "best_month_mean_path_value": 0.2,
            },
            {
                "candle_shape_modifier": "upper_wick_then_bear",
                "shape_classification": "shape_context_dependent",
                "delta_mean_path_value_score_v1": -0.02,
                "delta_mean_forward_ret_20d": -0.01,
                "delta_bottom15_rate": 0.01,
                "delta_top15_rate": -0.01,
                "sample_count": 80,
                "unique_symbol_count": 18,
                "month_count": 7,
                "mean_forward_ret_5d": -0.01,
                "mean_forward_ret_10d": -0.01,
                "mean_forward_ret_20d": -0.02,
                "median_forward_ret_20d": -0.02,
                "mean_path_value_score_v1": -0.01,
                "median_path_value_score_v1": -0.01,
                "mean_mfe_20d": 0.03,
                "mean_mae_20d": -0.04,
                "plus5_before_minus5_rate": 0.4,
                "minus5_before_plus5_rate": 0.5,
                "top15_rate": 0.15,
                "bottom15_rate": 0.25,
                "positive_month_rate": 0.4,
                "worst_month_mean_path_value": -0.2,
                "best_month_mean_path_value": 0.1,
            },
        ]
    )
    shape_lookup_frame, shape_lookup, positive_modifiers, context_dependent_modifiers = _build_shape_lookup(shape_lookup_frame)
    assert len(shape_lookup_frame) == 2
    assert positive_modifiers == ["gap_down_bear"]
    assert context_dependent_modifiers == ["upper_wick_then_bear"]

    frame = pd.DataFrame(
        [
            {
                "candidate_idx": 0,
                "anchor_date": "2024-03-15",
                "trade_date": 20240315,
                "side": "long",
                "symbol": "A",
                "score": 0.5,
                "conditional_high_value": True,
                "candle_shape_modifier": "gap_down_bear",
            },
            {
                "candidate_idx": 1,
                "anchor_date": "2024-03-15",
                "trade_date": 20240315,
                "side": "long",
                "symbol": "B",
                "score": 0.3,
                "conditional_high_value": True,
                "candle_shape_modifier": "upper_wick_then_bear",
            },
            {
                "candidate_idx": 2,
                "anchor_date": "2024-03-15",
                "trade_date": 20240315,
                "side": "long",
                "symbol": "C",
                "score": 0.1,
                "conditional_high_value": False,
                "candle_shape_modifier": "unmatched",
            },
        ]
    )

    out = _apply_shape_adjustments(
        frame,
        shape_lookup=shape_lookup,
        positive_modifiers=set(positive_modifiers),
        context_dependent_modifiers=set(context_dependent_modifiers),
        prune_enabled=True,
    )

    assert out.loc[0, "score_adjustment"] == 0.04
    assert out.loc[1, "score_adjustment"] == -0.03
    assert out.loc[2, "score_adjustment"] == 0.0
    assert bool(out.loc[0, "shape_boost_applied"]) is True
    assert bool(out.loc[1, "shape_prune_applied"]) is True
    assert out.loc[2, "shape_adjustment_reason"] == "outside_conditional_high_value"


def test_context_gated_candle_shape_boost_prune_smoke_run(tmp_path: Path) -> None:
    output_root = tmp_path / "context_gated_candle_shape_modifier_boost_prune"
    result = run_context_gated_candle_shape_modifier_boost_prune_v1(
        source_context_session=CONTEXT_SESSION,
        source_shape_session=SHAPE_SESSION,
        source_family_session=SOURCE_FAMILY_SESSION,
        prior_global_boost_session=PRIOR_GLOBAL_BOOST_SESSION,
        prior_context_gated_boost_session=PRIOR_CONTEXT_GATED_BOOST_SESSION,
        output_root=output_root,
        limit_anchor_dates=2,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()

    required_files = (
        "run_manifest.json",
        "context_gated_candle_shape_modifier_boost_prune_v1_compare.json",
        "context_gated_candle_shape_modifier_boost_prune_v1_decision.json",
        "shape_adjustment_coverage_summary.json",
        "prior_boost_delta.json",
        "shape_comparison.json",
        "monthly_context_comparison.json",
        "weekly_context_comparison.json",
        "topk_membership_diff.parquet",
        "_ARTIFACT_COMPLETE.json",
    )
    for file_name in required_files:
        assert (session_dir / file_name).exists()

    manifest = json.loads((session_dir / "run_manifest.json").read_text(encoding="utf-8"))
    compare = json.loads((session_dir / "context_gated_candle_shape_modifier_boost_prune_v1_compare.json").read_text(encoding="utf-8"))
    decision = json.loads((session_dir / "context_gated_candle_shape_modifier_boost_prune_v1_decision.json").read_text(encoding="utf-8"))
    coverage = json.loads((session_dir / "shape_adjustment_coverage_summary.json").read_text(encoding="utf-8"))
    prior_delta = json.loads((session_dir / "prior_boost_delta.json").read_text(encoding="utf-8"))

    assert manifest["schema_version"] == "tradex_context_gated_candle_shape_modifier_boost_prune_v1_manifest_v1"
    assert manifest["no_lookahead_inherited"] is True
    assert manifest["monthly_context_no_lookahead"] is True
    assert manifest["weekly_context_no_lookahead"] is True
    assert manifest["source_context_session_id"] == "20260429T091138Z-7d26cb7c"
    assert manifest["source_shape_session_id"] == "20260429T105018Z-26bc381e"
    assert manifest["source_family_session_id"] == "20260429T062945Z-87844c56"
    assert manifest["prior_global_boost_session_id"] == "20260429T084326Z-2ae0f0de"
    assert manifest["prior_context_gated_boost_session_id"] == "20260429T094730Z-7e1acdee"
    assert compare["same_condition_contract"]["universe"] == "integrated_guarded_v1_candidate_snapshots"
    assert compare["boost_formula"]["conditional_shape_boost"] == 0.04
    assert compare["boost_formula"]["conditional_shape_prune"] == -0.03
    assert decision["recommendation"] in {"keep", "hold", "drop"}
    assert coverage["candidate_rows"] > 0
    assert coverage["matched_shape_rate"] is not None
    assert coverage["shape_delta_join_available"] is True
    assert coverage["shape_positive_modifier_count"] == 12
    assert coverage["shape_context_dependent_count"] == 4
    assert prior_delta["schema_version"] == "tradex_context_gated_candle_shape_modifier_boost_prune_v1_prior_boost_delta_v1"

    parquet = pd.read_parquet(session_dir / "topk_membership_diff.parquet")
    assert not parquet.empty
    assert {"score_adjustment", "challenger_score", "candle_shape_modifier", "shape_classification", "shape_vs_base_slice_delta_path_value_score_v1"}.issubset(set(parquet.columns))
    assert parquet.loc[parquet["shape_joined"].fillna(False).astype(bool), "monthly_context_no_lookahead"].all()
    assert parquet.loc[parquet["shape_joined"].fillna(False).astype(bool), "weekly_context_no_lookahead"].all()
