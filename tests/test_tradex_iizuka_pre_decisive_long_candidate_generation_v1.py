from __future__ import annotations

import pandas as pd

from scripts.tradex_iizuka_pre_decisive_long_candidate_generation_v1 import (
    _build_candidate_contract,
    _build_feature_availability_audit,
    _compute_candidate_scores,
    _compare_topk,
)


def test_feature_availability_audit_marks_core_fields_ready() -> None:
    frame = pd.DataFrame(
        {
            "anchor_date": ["2025-01-01"],
            "symbol": ["1234"],
            "side": ["long"],
            "candidate_idx": [0],
            "close": [100.0],
            "ma7": [98.0],
            "ma20": [95.0],
            "ma60": [90.0],
            "ma7_prev1": [97.0],
            "ma20_prev1": [94.0],
            "ma60_prev1": [89.0],
            "candle_body_ratio": [0.5],
            "vol_ratio5_20": [1.2],
            "monthly_context_no_lookahead": [True],
            "weekly_context_no_lookahead": [True],
            "stable_bad_pick_family": [False],
            "signal_quality_bucket": ["signal_quality_high"],
            "decision_candle_quality": ["candle_strong"],
            "shape_classification": ["shape_positive_modifier"],
            "volume_participation_bucket": ["volume_confirmed"],
            "support_wick": [True],
            "bull_engulfing": [False],
            "morning_star": [False],
            "bull_marubozu": [False],
            "monthly_main_state_ctx_backfilled": ["monthly_range_mid"],
            "weekly_main_state_ctx_backfilled": ["weekly_up_mid"],
            "daily_main_state_ctx_backfilled": ["daily_reversal_up_candidate"],
        }
    )
    audit = _build_feature_availability_audit(frame)
    assert audit["decision"]["typed"] == "ready_to_design_iizuka_candidate_contract"
    assert audit["core_minimum_fields"]["close_vs_20ma"] is True
    assert audit["core_minimum_fields"]["ma20_slope_or_direction"] is True


def test_score_prefers_high_quality_rows() -> None:
    frame = pd.DataFrame(
        {
            "anchor_date": ["2025-01-01", "2025-01-01"],
            "symbol": ["1111", "2222"],
            "side": ["long", "long"],
            "candidate_idx": [0, 1],
            "entry_strength_score": [2.0, 2.0],
            "signal_quality_bucket": ["signal_quality_high", "signal_quality_low"],
            "decision_candle_quality": ["candle_strong", "candle_weak"],
            "volume_participation_bucket": ["volume_confirmed", "volume_weak"],
            "shape_classification": ["shape_positive_modifier", "shape_missing"],
            "daily_main_state_ctx_backfilled": ["daily_reversal_up_candidate", "daily_down_mid"],
            "weekly_main_state_ctx_backfilled": ["weekly_up_mid", "weekly_down_mid"],
            "monthly_main_state_ctx_backfilled": ["monthly_range_mid", "monthly_down_mid"],
            "ma20_slope_1": [0.01, -0.02],
            "vol_ratio5_20": [1.2, 0.8],
            "dist_ma20_pct": [0.03, 0.12],
            "dist_ma60_pct": [0.06, 0.18],
            "drawdown60": [-0.04, -0.01],
            "rebound60": [0.2, 0.08],
            "cnt_20_above": [12, 4],
            "cnt_7_above": [8, 2],
            "stable_bad_pick_family": [False, False],
            "decision_candle_quality": ["candle_strong", "candle_weak"],
            "monthly_context_no_lookahead": [True, True],
            "weekly_context_no_lookahead": [True, True],
            "support_wick": [True, False],
            "bull_engulfing": [False, False],
            "morning_star": [False, False],
            "bull_marubozu": [False, False],
        }
    )
    scored = _compute_candidate_scores(frame)
    assert scored.loc[0, "iizuka_candidate_score"] > scored.loc[1, "iizuka_candidate_score"]


def test_contract_has_research_only_flag() -> None:
    contract = _build_candidate_contract()
    assert contract["candidate_contract_name"] == "iizuka_pre_decisive_long_candidate_v1"
    assert contract["research_only"] is True


def test_compare_topk_uses_surface_key_for_membership_examples() -> None:
    champion = pd.DataFrame(
        {
            "surface_key": ["2025-01-01|1111|long", "2025-01-01|2222|long"],
            "anchor_date": ["2025-01-01", "2025-01-01"],
            "symbol": ["1111", "2222"],
            "side": ["long", "long"],
            "champion_score": [9.0, 8.0],
            "champion_rank": [1, 2],
            "forward_ret_20d": [0.2, 0.1],
            "path_value_score_v1": [0.3, 0.1],
            "top15_label": [True, False],
            "bottom15_label": [False, True],
            "top20pct_label": [True, False],
            "champion_selected_top5": [True, True],
            "champion_selected_top10": [True, True],
            "champion_selected_top20": [True, True],
        }
    )
    challenger = pd.DataFrame(
        {
            "surface_key": ["2025-01-01|1111|long", "2025-01-01|3333|long"],
            "anchor_date": ["2025-01-01", "2025-01-01"],
            "symbol": ["1111", "3333"],
            "side": ["long", "long"],
            "iizuka_candidate_score": [9.5, 7.0],
            "iizuka_candidate_rank": [1, 2],
            "iizuka_candidate_reason": ["reason_a", "reason_b"],
            "iizuka_context_block_pass": [True, True],
            "iizuka_compression_block_pass": [True, True],
            "iizuka_trigger_proximity_block_pass": [True, True],
            "iizuka_risk_block_pass": [True, True],
            "forward_ret_20d": [0.2, 0.05],
            "path_value_score_v1": [0.3, 0.02],
            "top15_label": [True, False],
            "bottom15_label": [False, False],
            "top20pct_label": [True, False],
        }
    )
    comparison, diff_frame, failure_mode, headroom = _compare_topk(champion, challenger)
    assert not diff_frame.empty
    assert "surface_key" in diff_frame.columns
    assert all("surface_key" in item for item in headroom["per_k"]["5"]["missed_top15_examples"] + headroom["per_k"]["5"]["gained_top15_examples"])
    assert "key" not in headroom["per_k"]["5"]["missed_top15_examples"][0]
    assert comparison["per_k"][0]["membership_changed_count"] == 2
