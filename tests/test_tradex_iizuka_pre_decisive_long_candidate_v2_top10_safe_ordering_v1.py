from __future__ import annotations

import pandas as pd

from scripts.tradex_iizuka_pre_decisive_long_candidate_v2_top10_safe_ordering_v1 import (
    _build_contract,
    _build_decision,
    _build_month_audit,
    _build_safe_frame,
    _build_failure_audit,
)


def test_safe_ranking_is_score_anchored_with_small_structural_adjustments() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-05",
                "symbol": "1111",
                "surface_key": "2026-01-05|1111|long",
                "side": "long",
                "champion_rank": 2,
                "iizuka_v2_candidate_score": 8.0,
                "signal_quality_bucket": "signal_quality_high",
                "volume_participation_bucket": "volume_neutral",
                "decision_candle_quality": "candle_strong",
                "shape_classification": "shape_positive_modifier",
                "support_wick": True,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.01,
                "close_vs_ma60_pct": 0.03,
                "forward_ret_20d": 0.12,
                "path_value_score_v1": 0.25,
                "top15_label": True,
                "bottom15_label": False,
            },
            {
                "anchor_date": "2026-01-05",
                "symbol": "2222",
                "surface_key": "2026-01-05|2222|long",
                "side": "long",
                "champion_rank": 1,
                "iizuka_v2_candidate_score": 7.5,
                "signal_quality_bucket": "signal_quality_mid",
                "volume_participation_bucket": "volume_confirmed",
                "decision_candle_quality": "candle_mixed",
                "shape_classification": "shape_context_dependent",
                "support_wick": False,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.09,
                "close_vs_ma60_pct": 0.11,
                "forward_ret_20d": -0.04,
                "path_value_score_v1": -0.05,
                "top15_label": False,
                "bottom15_label": True,
            },
        ]
    )
    safe = _build_safe_frame(frame)
    assert safe["candidate_contract_name"].eq("v2_score_anchored_top10_safe_ordering_v1").all()
    assert safe["research_only"].eq(True).all()
    assert safe["top10_safe_candidate_rank"].tolist() == [1, 2]
    assert safe.iloc[0]["symbol"] == "1111"


def test_contract_and_month_audit_expose_required_shape() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-05",
                "symbol": "1111",
                "surface_key": "2026-01-05|1111|long",
                "side": "long",
                "champion_rank": 1,
                "iizuka_v2_candidate_score": 8.0,
                "signal_quality_bucket": "signal_quality_high",
                "volume_participation_bucket": "volume_neutral",
                "decision_candle_quality": "candle_strong",
                "shape_classification": "shape_positive_modifier",
                "support_wick": True,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.01,
                "close_vs_ma60_pct": 0.03,
                "forward_ret_20d": 0.12,
                "path_value_score_v1": 0.25,
                "top15_label": True,
                "bottom15_label": False,
            },
            {
                "anchor_date": "2026-02-05",
                "symbol": "2222",
                "surface_key": "2026-02-05|2222|long",
                "side": "long",
                "champion_rank": 2,
                "iizuka_v2_candidate_score": 7.0,
                "signal_quality_bucket": "signal_quality_mid",
                "volume_participation_bucket": "volume_confirmed",
                "decision_candle_quality": "candle_mixed",
                "shape_classification": "shape_context_dependent",
                "support_wick": True,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.02,
                "close_vs_ma60_pct": 0.04,
                "forward_ret_20d": -0.04,
                "path_value_score_v1": -0.05,
                "top15_label": False,
                "bottom15_label": True,
            },
        ]
    )
    safe = _build_safe_frame(frame)
    contract = _build_contract(safe)
    month_audit = _build_month_audit(pd.concat([safe.assign(iizuka_v2_role="active")], ignore_index=True))

    assert contract["candidate_contract_name"] == "v2_score_anchored_top10_safe_ordering_v1"
    assert contract["required_non_outcome_fields_present"]["iizuka_v2_candidate_score"] is True
    assert month_audit["active_month_count"] == 2
    assert set(month_audit["per_k"].keys()) == {"5", "10", "20"}
    assert "one_month_dominated" in month_audit["improvement_concentration"]


def test_failure_audit_reports_dropped_challenger_churn() -> None:
    approved = pd.DataFrame(
        [
            {
                "surface_key": "2026-01-05|1111|long",
                "anchor_date": "2026-01-05",
                "symbol": "1111",
                "signal_quality_bucket": "signal_quality_high",
                "volume_participation_bucket": "volume_neutral",
                "decision_candle_quality": "candle_strong",
                "shape_classification": "shape_positive_modifier",
                "support_wick": True,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.01,
                "close_vs_ma60_pct": 0.03,
                "iizuka_v2_candidate_score": 8.0,
                "iizuka_v2_candidate_rank": 1.0,
                "iizuka_v2_reason": "active|context_pass|compression_pass|risk_pass|volume_neutral_default",
                "forward_ret_20d": 0.12,
                "path_value_score_v1": 0.25,
                "top15_label": True,
                "bottom15_label": False,
            }
        ]
    )
    diff = pd.DataFrame(
        [
            {
                "top_k": 10,
                "surface_key": "2026-01-05|1111|long",
                "selected_in_v2_active": True,
                "selected_in_challenger": False,
            }
        ]
    )
    inputs = {
        "approved_active_rows": approved,
        "dropped_diff": diff,
    }
    audit = _build_failure_audit(inputs)
    assert audit["baseline"] == "approved_v2_active_surface"
    assert audit["top10_entry_count"] == 0 or audit["top10_leaver_count"] == 1


def test_decision_holds_when_top10_improves_but_breadth_is_still_narrow() -> None:
    comparison = {
        "per_k": [
            {
                "top_k": 10,
                "approved_v2_active": {"mean_forward_ret_20d": 0.10, "bottom15_contamination_rate": 0.20, "non_positive_return_rate": 0.50, "row_count": 10, "group_count": 1, "symbol_count": 1, "month_count": 1},
                "safe_challenger": {"mean_forward_ret_20d": 0.11, "bottom15_contamination_rate": 0.20, "non_positive_return_rate": 0.50, "row_count": 10, "group_count": 1, "symbol_count": 1, "month_count": 1},
                "changed_top10_members_count": 2,
            },
            {
                "top_k": 20,
                "approved_v2_active": {"mean_forward_ret_20d": 0.09, "bottom15_contamination_rate": 0.20, "non_positive_return_rate": 0.50, "row_count": 20, "group_count": 1, "symbol_count": 1, "month_count": 1},
                "safe_challenger": {"mean_forward_ret_20d": 0.12, "bottom15_contamination_rate": 0.20, "non_positive_return_rate": 0.50, "row_count": 20, "group_count": 1, "symbol_count": 1, "month_count": 1},
            },
        ]
    }
    month_audit = {"active_month_count": 3, "improvement_concentration": {"one_month_dominated": False}}
    no_lookahead = {"no_lookahead_pass": True}
    leakage = {"leakage_free": True}
    decision = _build_decision(comparison, month_audit, no_lookahead, leakage)
    assert decision["decision"] == "hold"
    assert "breadth" in decision["reason"]
