from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_score_component_overweight_decomposition_v1 import (
    BAD_PICK_ROOT_CAUSE,
    _build_score_component_inventory,
    _boundary_pair_rows,
    run_score_component_overweight_decomposition_v1,
)


def test_score_component_inventory_groups_are_reported() -> None:
    frame = pd.DataFrame(
        {
            "score": [0.7, 0.5],
            "candidate_score": [0.7, 0.5],
            "champion_score": [0.7, 0.5],
            "challenger_score": [0.7, 0.5],
            "candidate_rank": [1, 2],
            "champion_rank": [1, 2],
            "challenger_rank": [1, 2],
            "rank": [1, 2],
            "forward_ret_20d": [0.1, -0.1],
            "path_value_score_v1": [0.2, -0.2],
            "dist_ma20_pct": [0.03, 0.08],
            "dist_ma60_pct": [0.04, 0.09],
            "body_ratio": [0.3, 0.6],
            "upper_wick_ratio": [0.2, 0.1],
            "lower_wick_ratio": [0.2, 0.1],
            "gap_pct": [0.0, 0.01],
            "vol_ratio5_20": [1.0, 0.7],
            "liquidity20d": [1000.0, 2000.0],
            "monthly_context": ["monthly_overextended", "monthly_range"],
            "weekly_context": ["weekly_overextended", "weekly_range"],
            "daily_main_state_ctx": ["daily_up_mid", "daily_reversal_up_candidate"],
            "dominant_regime_context": ["C:risk_on_trend", "C:neutral_range"],
            "market_regime_bucket": ["unknown", "unknown"],
            "family_classification": ["regime_dependent_family", "stable_high_value_family"],
            "shape_classification": ["shape_positive_modifier", "shape_context_dependent"],
            "candle_shape_modifier": ["bull_large", "no_clear_shape"],
            "conditional_high_value": [True, False],
        }
    )
    inventory = _build_score_component_inventory(frame)
    assert inventory["schema_version"].startswith("tradex_score_component_overweight_decomposition_v1")
    assert "score" in inventory["total_score_fields"]
    assert any(group["group"] == "ma_extension_proxy" for group in inventory["proxy_component_groups"])
    assert any(row["field"] == "score" and row["available"] for row in inventory["observed_score_related_fields"])


def test_boundary_pair_rows_include_near_miss_features() -> None:
    selected = pd.DataFrame(
        {
            "anchor_date": ["2024-01-01", "2024-01-01"],
            "symbol": ["AAA", "BBB"],
            "side": ["long", "long"],
            "month_bucket": ["2024-01", "2024-01"],
            "champion_rank": [5, 6],
            "score": [0.68, 0.60],
            "forward_ret_20d": [-0.10, 0.05],
            "path_value_score_v1": [-0.20, 0.10],
            "mfe_20d": [0.15, 0.20],
            "mae_20d": [-0.08, -0.04],
            "dist_ma20_pct": [0.05, 0.03],
            "dist_ma60_pct": [0.07, 0.04],
            "body_ratio": [0.55, 0.30],
            "upper_wick_ratio": [0.10, 0.20],
            "lower_wick_ratio": [0.15, 0.25],
            "gap_pct": [0.02, 0.01],
            "vol_ratio5_20": [0.9, 1.1],
            "liquidity20d": [1500.0, 1600.0],
            "candle_body_ratio": [0.51, 0.42],
            "candle_upper_wick_ratio": [0.09, 0.18],
            "candle_lower_wick_ratio": [0.12, 0.21],
            "candle_triplet_up_prob": [0.4, 0.6],
            "candle_triplet_down_prob": [0.5, 0.3],
            "monthly_context": ["monthly_overextended", "monthly_range"],
            "weekly_context": ["weekly_overextended", "weekly_range"],
            "daily_main_state_ctx": ["daily_up_mid", "daily_reversal_up_candidate"],
            "monthly_main_state_ctx": ["monthly_up_top_warning", "monthly_range_mid"],
            "weekly_main_state_ctx": ["weekly_up_late", "weekly_range"],
            "dominant_regime_context": ["C:risk_on_trend", "C:risk_on_trend"],
            "market_regime_bucket": ["unknown", "unknown"],
            "family_classification": ["regime_dependent_family", "stable_high_value_family"],
            "shape_classification": ["shape_positive_modifier", "shape_context_dependent"],
            "candle_shape_modifier": ["bull_large", "no_clear_shape"],
            "family_mean_path_value_score_v1": [0.03, 0.05],
            "family_median_path_value_score_v1": [0.03, 0.05],
            "family_top15_rate": [0.12, 0.20],
            "family_bottom15_rate": [0.14, 0.10],
            "family_positive_month_rate": [0.58, 0.64],
            "bad_pick_scope": ["top5", "top10"],
            "root_cause_code": [BAD_PICK_ROOT_CAUSE, "none"],
            "root_cause_confidence": ["medium", "unknown"],
            "is_bad_pick": [True, False],
            "is_good_pick": [False, True],
            "is_neutral_pick": [False, False],
            "best_near_miss_symbol": ["BBB", "AAA"],
            "best_near_miss_rank": [6, 5],
            "score_gap": [0.08, -0.02],
            "forward_ret_20d_gap": [-0.15, 0.20],
            "path_value_gap": [-0.30, 0.30],
            "rank_gap": [1, -1],
            "champion_selected_top5": [True, False],
            "champion_selected_top10": [True, True],
            "champion_selected_top20": [True, True],
        }
    )
    pairs = _boundary_pair_rows(selected, selected.loc[[0]])
    assert len(pairs) == 1
    row = pairs.iloc[0].to_dict()
    assert row["near_miss_joined"] is True
    assert row["selected_score"] == 0.68
    assert row["near_miss_score"] == 0.60
    assert row["score_gap"] == 0.08
    assert row["selected_forward_ret_20d"] == -0.10
    assert row["near_miss_forward_ret_20d"] == 0.05


def test_smoke_run_emits_authoritative_artifacts(tmp_path: Path) -> None:
    result = run_score_component_overweight_decomposition_v1(
        output_root=tmp_path,
        limit_anchor_dates=2,
        jobs=2,
    )
    session_dir = Path(result["session_dir"])
    assert session_dir.exists()
    required = [
        "run_manifest.json",
        "input_resolution.json",
        "score_component_inventory.json",
        "score_overweight_cohort_summary.json",
        "score_component_contrast_summary.json",
        "score_component_pairwise_boundary.parquet",
        "score_component_pairwise_boundary_summary.json",
        "score_component_regime_breakdown.json",
        "score_component_failure_patterns.json",
        "score_component_challenger_hypotheses.json",
        "score_component_overweight_decomposition_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    for name in required:
        assert (session_dir / name).exists(), name
    decision = json.loads((session_dir / "score_component_overweight_decomposition_v1_decision.json").read_text(encoding="utf-8"))
    assert decision["decision"] in {"ready_for_single_axis_challenger_design", "needs_more_input_data", "insufficient_signal", "explanation_only"}
    pairwise = pd.read_parquet(session_dir / "score_component_pairwise_boundary.parquet")
    assert len(pairwise) > 0
    assert "score_gap" in pairwise.columns
