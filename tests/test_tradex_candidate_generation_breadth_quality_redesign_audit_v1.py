from __future__ import annotations

import pandas as pd

from scripts.tradex_candidate_generation_breadth_quality_redesign_audit_v1 import (
    _build_candidate_pool_breadth_audit,
    _build_winner_inclusion_audit,
    _recommend_axis,
    _rank_within_groups,
)


def test_rank_within_groups_uses_expected_tie_breaks() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "AAA",
                "candidate_idx": 0,
                "forward_ret_20d": 0.10,
                "path_value_score_v1": 0.20,
                "mae_20d": 0.30,
                "reference_score": 0.9,
                "diagnostic_score": 0.7,
                "model_score": 0.8,
                "rank": 2,
                "top15_label": True,
                "bottom15_label": False,
                "side_aware_group_top20pct_forward_ret_20d_label": True,
                "month_bucket": "2026-01",
            },
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "AAB",
                "candidate_idx": 1,
                "forward_ret_20d": 0.10,
                "path_value_score_v1": 0.30,
                "mae_20d": 0.40,
                "reference_score": 0.8,
                "diagnostic_score": 0.9,
                "model_score": 0.7,
                "rank": 1,
                "top15_label": False,
                "bottom15_label": False,
                "side_aware_group_top20pct_forward_ret_20d_label": False,
                "month_bucket": "2026-01",
            },
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "AAC",
                "candidate_idx": 2,
                "forward_ret_20d": 0.05,
                "path_value_score_v1": 0.10,
                "mae_20d": 0.20,
                "reference_score": 0.7,
                "diagnostic_score": 0.6,
                "model_score": 0.6,
                "rank": 3,
                "top15_label": False,
                "bottom15_label": True,
                "side_aware_group_top20pct_forward_ret_20d_label": False,
                "month_bucket": "2026-01",
            },
        ]
    )
    oracle = _rank_within_groups(
        frame,
        sort_cols=["forward_ret_20d", "path_value_score_v1", "mae_20d", "candidate_idx", "symbol"],
        ascending=[False, False, True, True, True],
        prefix="oracle",
    )
    assert int(oracle.loc[oracle["oracle_rank"] == 1, "candidate_idx"].iloc[0]) == 1
    assert bool(oracle.loc[oracle["oracle_rank"] == 1, "side_aware_group_top20pct_forward_ret_20d_label"].iloc[0]) is False


def test_breadth_audit_flags_thin_groups() -> None:
    frame = pd.DataFrame(
        [
            {"anchor_date": "2026-01-01", "side": "long", "symbol": "AAA", "candidate_idx": 0, "forward_ret_20d": 0.1, "path_value_score_v1": 0.2, "mae_20d": 0.1, "month_bucket": "2026-01", "top15_label": True, "bottom15_label": False, "side_aware_group_top20pct_forward_ret_20d_label": True},
            {"anchor_date": "2026-01-01", "side": "long", "symbol": "AAB", "candidate_idx": 1, "forward_ret_20d": 0.1, "path_value_score_v1": 0.2, "mae_20d": 0.1, "month_bucket": "2026-01", "top15_label": False, "bottom15_label": False, "side_aware_group_top20pct_forward_ret_20d_label": False},
            {"anchor_date": "2026-01-02", "side": "short", "symbol": "BAA", "candidate_idx": 2, "forward_ret_20d": -0.1, "path_value_score_v1": -0.2, "mae_20d": 0.2, "month_bucket": "2026-01", "top15_label": False, "bottom15_label": False, "side_aware_group_top20pct_forward_ret_20d_label": False},
        ]
    )
    breadth, group_rows = _build_candidate_pool_breadth_audit(frame, frame)
    assert breadth["row_count"] == 3
    assert breadth["overall_thin_groups"]["top5"] == 2
    assert breadth["overall_thin_groups"]["top10"] == 2
    assert breadth["overall_thin_groups"]["top20"] == 2
    assert group_rows.shape[0] == 2


def test_winner_inclusion_recommendation_prefers_high_recall_when_top15_is_sparse() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "AAA",
                "candidate_idx": 0,
                "forward_ret_20d": 0.4,
                "path_value_score_v1": 0.2,
                "mae_20d": 0.1,
                "month_bucket": "2026-01",
                "split": "oos",
                "reference_score": 0.9,
                "diagnostic_score": 0.8,
                "model_score": 0.7,
                "rank": 1,
                "top15_label": False,
                "bottom15_label": False,
                "side_aware_group_top20pct_forward_ret_20d_label": True,
            },
            {
                "anchor_date": "2026-01-01",
                "side": "long",
                "symbol": "AAB",
                "candidate_idx": 1,
                "forward_ret_20d": 0.1,
                "path_value_score_v1": 0.1,
                "mae_20d": 0.2,
                "month_bucket": "2026-01",
                "split": "oos",
                "reference_score": 0.8,
                "diagnostic_score": 0.7,
                "model_score": 0.6,
                "rank": 2,
                "top15_label": False,
                "bottom15_label": True,
                "side_aware_group_top20pct_forward_ret_20d_label": False,
            },
        ]
    )
    label_sensitivity = pd.DataFrame(
        [
            {"name": "group_top20_forward_ret_20d", "definition": "x", "positive_count": 1, "positive_rate": 0.5, "long_positive_count": 1, "short_positive_count": 0, "group_positive_count": 1, "long_group_positive_count": 1, "short_group_positive_count": 0, "long_positive_rate": 0.5, "short_positive_rate": 0.0, "class_imbalance": "1:1"},
        ]
    )
    winner_audit, _ = _build_winner_inclusion_audit(frame, label_sensitivity)
    breadth = {"overall_thin_groups": {"top5": 15, "top10": 18, "top20": 22}}
    winner_audit["group_count"] = 33
    winner_audit["profiles"]["top15_label"]["group_positive_count"] = 9
    oracle = {"breadth_headroom": {"top5": {"oracle_top15_capture_rate": 0.5, "champion_top15_capture_rate": 0.0}, "top10": {"oracle_top15_capture_rate": 0.5, "champion_top15_capture_rate": 0.0}, "top20": {"oracle_top15_capture_rate": 0.5, "champion_top15_capture_rate": 0.0}}}
    axis, payload, decision = _recommend_axis(breadth, winner_audit, oracle)
    assert axis == "high_recall_candidate_pool_v1"
    assert decision == "ready_to_design_high_recall_candidate_pool"
    assert "recall" in payload["reason"]
