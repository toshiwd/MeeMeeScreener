from __future__ import annotations

import pandas as pd

from scripts.tradex_iizuka_pre_decisive_long_candidate_v2_forward_accumulation import (
    _anchor_date_range,
    _attach_safe_ordering,
    _build_decision,
    _parquet_compatible_frame,
)


def test_anchor_date_range_only_returns_weekdays() -> None:
    dates = _anchor_date_range("2026-03-26", "2026-04-03")
    assert dates == ["2026-03-27", "2026-03-30", "2026-03-31", "2026-04-01", "2026-04-02", "2026-04-03"]


def test_attach_safe_ordering_only_scores_active_lane() -> None:
    frame = pd.DataFrame(
        [
            {
                "surface_key": "2026-04-01|1001|long",
                "anchor_date": "2026-04-01",
                "symbol": "1001",
                "iizuka_v2_role": "active",
                "iizuka_v2_candidate_score": 9.0,
                "champion_rank": 2,
                "support_wick": True,
                "bull_engulfing": False,
                "decision_candle_quality": "candle_strong",
                "shape_classification": "shape_positive_modifier",
                "close_vs_ma20_pct": 0.01,
                "close_vs_ma60_pct": 0.02,
            },
            {
                "surface_key": "2026-04-01|1002|long",
                "anchor_date": "2026-04-01",
                "symbol": "1002",
                "iizuka_v2_role": "diagnostic_only",
                "iizuka_v2_candidate_score": 8.0,
                "champion_rank": 1,
                "support_wick": False,
                "bull_engulfing": False,
                "decision_candle_quality": "candle_mixed",
                "shape_classification": "shape_context_dependent",
                "close_vs_ma20_pct": 0.05,
                "close_vs_ma60_pct": 0.06,
            },
        ]
    )
    merged, safe = _attach_safe_ordering(frame)
    assert safe["top10_safe_candidate_rank"].tolist() == [1]
    assert merged.loc[merged["symbol"] == "1001", "top10_safe_candidate_rank"].iloc[0] == 1
    assert pd.isna(merged.loc[merged["symbol"] == "1002", "top10_safe_candidate_rank"].iloc[0])


def test_decision_keeps_when_breadth_expands_and_metrics_hold() -> None:
    comparison = {
        "per_k": [
            {
                "top_k": 10,
                "approved_v2_active": {"mean_forward_ret_20d": 0.10, "bottom15_contamination_rate": 0.20, "non_positive_return_rate": 0.50, "row_count": 10, "group_count": 4, "symbol_count": 10},
                "top10_safe_ordering_v1": {"mean_forward_ret_20d": 0.11, "bottom15_contamination_rate": 0.19, "non_positive_return_rate": 0.49, "row_count": 10, "group_count": 4, "symbol_count": 10},
                "changed_top10_members_count": 2,
            },
            {
                "top_k": 20,
                "approved_v2_active": {"mean_forward_ret_20d": 0.09, "bottom15_contamination_rate": 0.20, "non_positive_return_rate": 0.50, "row_count": 20, "group_count": 4, "symbol_count": 20},
                "top10_safe_ordering_v1": {"mean_forward_ret_20d": 0.10, "bottom15_contamination_rate": 0.19, "non_positive_return_rate": 0.49, "row_count": 20, "group_count": 4, "symbol_count": 20},
                "changed_top20_members_count": 2,
            },
        ]
    }
    month_audit = {"active_month_count": 4, "improvement_concentration": {"one_month_dominated": False}}
    no_lookahead = {"no_lookahead_pass": True}
    leakage = {"leakage_free": True}
    decision = _build_decision(comparison, month_audit, no_lookahead, leakage)
    assert decision["decision"] == "keep_candidate_for_longer_forward_watch"
    assert decision["status"] == "keep"


def test_parquet_compatible_frame_serializes_nested_objects() -> None:
    frame = pd.DataFrame(
        [
            {
                "surface_key": "2026-04-01|1001|long",
                "basis_payload": {"foo": [1, 2], "bar": {"x": True}},
                "plain_text": "ok",
            }
        ]
    )
    out = _parquet_compatible_frame(frame)
    assert isinstance(out.loc[0, "basis_payload"], str)
    assert out.loc[0, "plain_text"] == "ok"
