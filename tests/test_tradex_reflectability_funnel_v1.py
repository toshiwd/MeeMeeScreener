from __future__ import annotations

import pandas as pd

from scripts.tradex_champion_topk_bad_pick_veto_v1 import (
    _build_adjusted_group,
    _build_monthly_top5_capture_comparison,
    _symbol_trailing_median,
)
from scripts.tradex_reflectability_gap_audit_v1 import _classify_blockers, _score_blocker


def test_symbol_trailing_median_uses_only_past_rows() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["AAA", "AAA", "AAA", "BBB"],
            "anchor_date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-01"],
            "vol_ratio5_20": [1.0, 3.0, 2.0, 4.0],
        }
    )

    trailing = _symbol_trailing_median(frame, "vol_ratio5_20")

    assert pd.isna(trailing.iloc[0])
    assert trailing.iloc[1] == 1.0
    assert trailing.iloc[2] == 2.0
    assert pd.isna(trailing.iloc[3])


def test_adjusted_group_preserves_rank_when_no_veto_applies() -> None:
    group = pd.DataFrame(
        {
            "anchor_date": ["2025-01-01"] * 4,
            "side": ["long"] * 4,
            "symbol": ["AAA", "BBB", "CCC", "DDD"],
            "champion_rank": [1, 2, 3, 4],
            "champion_score": [0.9, 0.8, 0.7, 0.6],
            "score": [0.9, 0.8, 0.7, 0.6],
            "monthly_context": ["monthly_range"] * 4,
            "weekly_context": ["weekly_range"] * 4,
            "vol_ratio5_20": [1.1, 1.0, 1.2, 0.95],
            "candle_body_ratio": [0.4, 0.5, 0.6, 0.3],
            "gap_pct": [0.0, 0.0, 0.0, 0.0],
            "champion_selected_top5": [True, True, True, True],
            "champion_selected_top10": [True, True, True, True],
            "champion_selected_top20": [True, True, True, True],
            "top15_label": [True, True, True, True],
            "bottom15_label": [False, False, False, False],
        }
    )

    adjusted = _build_adjusted_group(group)

    assert adjusted["veto_applied"].sum() == 0
    assert adjusted["adjusted_rank"].tolist() == [1, 2, 3, 4]
    assert adjusted["candidate_selected_top10"].tolist() == [True, True, True, True]


def test_adjusted_group_demotes_top10_veto_candidate_without_expanding_top10() -> None:
    group = pd.DataFrame(
        {
            "anchor_date": ["2025-01-01"] * 12,
            "side": ["long"] * 12,
            "symbol": [f"S{i:02d}" for i in range(12)],
            "champion_rank": list(range(1, 13)),
            "champion_score": [0.99 - i * 0.05 for i in range(12)],
            "score": [0.99 - i * 0.05 for i in range(12)],
            "monthly_context": ["monthly_range"] * 8 + ["monthly_overextended"] * 4,
            "weekly_context": ["weekly_range"] * 8 + ["weekly_overextended"] * 4,
            "vol_ratio5_20": [1.1] * 8 + [0.72, 0.73, 1.05, 1.02],
            "candle_body_ratio": [0.4] * 12,
            "gap_pct": [0.0] * 12,
            "champion_selected_top5": [True] * 5 + [False] * 7,
            "champion_selected_top10": [True] * 10 + [False] * 2,
            "champion_selected_top20": [True] * 12,
            "top15_label": [True] * 8 + [False, False, False, False],
            "bottom15_label": [False] * 8 + [True, True, False, False],
            "symbol_vol_ratio_median": [1.1] * 8 + [0.75, 0.74, 1.0, 1.0],
        }
    )

    adjusted = _build_adjusted_group(group)

    assert int(adjusted["veto_applied"].sum()) == 2
    assert int(adjusted["candidate_selected_top10"].sum()) == 10
    vetoed = adjusted[adjusted["veto_applied"].fillna(False).astype(bool)]
    assert (vetoed["adjusted_rank"] > 10).all()


def test_audit_blocker_priority_prefers_no_branching_over_minor_issues() -> None:
    blockers = _classify_blockers(
        {
            "artifact_complete": True,
            "publishability": "publish_review_only",
            "changed_top10_members_count": 0,
            "fallback_status": "authoritative",
            "candidate_local_decision": "hold",
            "authoritative_rollup_decision": "hold",
            "top10_mean_ret20": 0.01,
            "topk": {"10": {"delta": {"bottom15_contamination_rate": -0.01}}},
            "monthly_top5_capture": 0.5,
        }
    )

    assert _score_blocker(blockers) == "no_branching"


def test_monthly_top5_capture_comparison_reports_month_level_delta() -> None:
    symbols = [chr(ord("A") + i) for i in range(10)]
    frame = pd.DataFrame(
        {
            "anchor_date": ["2025-01-03"] * 10 + ["2025-02-04"] * 10,
            "month_bucket": ["2025-01"] * 10 + ["2025-02"] * 10,
            "symbol": symbols * 2,
            "forward_ret_20d": list(range(10, 0, -1)) + list(range(10, 0, -1)),
            "champion_score": list(range(10, 0, -1)) + list(range(10, 0, -1)),
            "adjusted_score": list(range(10, 0, -1)) + list(range(1, 11)),
            "family_regime_context": ["regime_a"] * 20,
        }
    )

    summary = _build_monthly_top5_capture_comparison(frame)

    assert summary["months_evaluated"] == 2
    assert summary["monthly_top5_capture_delta"]["mean"] == -0.5
    assert summary["zero_capture_months"] == 1
    assert summary["degraded_months"] == 1
    assert summary["improved_months"] == 0
    assert summary["unchanged_months"] == 1
