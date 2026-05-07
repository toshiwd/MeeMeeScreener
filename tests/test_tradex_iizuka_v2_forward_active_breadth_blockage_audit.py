from __future__ import annotations

import pandas as pd

from scripts.tradex_iizuka_v2_forward_active_breadth_blockage_audit import (
    _build_date_key_alignment_audit,
    _build_feasibility,
    _build_gate_stage_waterfall,
    _build_missing_feature_audit,
)


def test_gate_stage_waterfall_classifies_forward_failures() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-03-27",
                "symbol": "1001",
                "side": "long",
                "surface_key": "2026-03-27|1001|long",
                "iizuka_v2_role": "diagnostic_only",
                "iizuka_v2_reason": "diagnostic_only|context_block_failed|compression_block_failed|risk_block_failed|trigger_gate_failed",
                "iizuka_v2_diagnostic_reason": "context_block_failed|compression_block_failed|risk_block_failed|trigger_gate_failed",
                "iizuka_v2_exclusion_reason": "",
                "iizuka_context_block_pass": False,
                "iizuka_compression_block_pass": False,
                "iizuka_risk_block_pass": False,
                "volume_participation_bucket": "volume_weak",
                "support_wick": False,
                "bull_engulfing": False,
                "drawdown60": 0.1,
                "rebound60": 0.2,
            },
            {
                "anchor_date": "2026-03-30",
                "symbol": "1002",
                "side": "long",
                "surface_key": "2026-03-30|1002|long",
                "iizuka_v2_role": "excluded",
                "iizuka_v2_reason": "excluded|bull_marubozu",
                "iizuka_v2_diagnostic_reason": "",
                "iizuka_v2_exclusion_reason": "bull_marubozu",
                "iizuka_context_block_pass": False,
                "iizuka_compression_block_pass": False,
                "iizuka_risk_block_pass": False,
                "volume_participation_bucket": "volume_confirmed",
                "support_wick": True,
                "bull_engulfing": False,
                "drawdown60": 0.05,
                "rebound60": 0.1,
            },
            {
                "anchor_date": "2026-04-01",
                "symbol": "1003",
                "side": "long",
                "surface_key": "2026-04-01|1003|long",
                "iizuka_v2_role": "active",
                "iizuka_v2_reason": "active|context_pass|compression_pass|risk_pass|volume_neutral_default",
                "iizuka_v2_diagnostic_reason": "",
                "iizuka_v2_exclusion_reason": "",
                "iizuka_context_block_pass": True,
                "iizuka_compression_block_pass": True,
                "iizuka_risk_block_pass": True,
                "volume_participation_bucket": "volume_neutral",
                "support_wick": True,
                "bull_engulfing": True,
                "drawdown60": 0.03,
                "rebound60": 0.15,
            },
        ]
    )
    waterfall, stage_frame = _build_gate_stage_waterfall(frame, "2026-03-26")
    assert waterfall["stage_counts"]["diagnostic_only"] == 1
    assert waterfall["stage_counts"]["explicit_exclusion"] == 1
    assert waterfall["stage_counts"]["active_pass"] == 1
    assert set(stage_frame["primary_blockage"]) == {"frozen_gate_base_block", "explicit_exclusion", "active_pass"}


def test_missing_feature_audit_flags_forward_score_gap_but_not_alignment_gap() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-03-26",
                "symbol": "2001",
                "side": "long",
                "surface_key": "2026-03-26|2001|long",
                "canonical_candidate_key": "2026-03-26|2001|long",
                "key": "2026-03-26|2001|long",
                "research_fallback_label_source": "ml_label_20d",
                "iizuka_candidate_score": 0.5,
                "iizuka_v2_candidate_score": 0.5,
                "iizuka_candidate_rank": 1,
                "iizuka_v2_candidate_rank": 1,
                "signal_quality_bucket": "signal_quality_high",
                "volume_participation_bucket": "volume_neutral",
                "decision_candle_quality": "candle_strong",
                "shape_classification": "shape_positive_modifier",
                "support_wick": True,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.01,
                "close_vs_ma60_pct": 0.02,
                "iizuka_context_block_pass": True,
                "iizuka_compression_block_pass": True,
                "iizuka_risk_block_pass": True,
                "drawdown60": 0.03,
                "rebound60": 0.2,
                "stable_bad_pick_family": False,
                "bull_marubozu": False,
                "ma20_distance_bucket": "near",
                "ma60_distance_bucket": "near",
            },
            {
                "anchor_date": "2026-04-01",
                "symbol": "2002",
                "side": "long",
                "surface_key": "2026-04-01|2002|long",
                "canonical_candidate_key": "2026-04-01|2002|long",
                "key": "2026-04-01|2002|long",
                "research_fallback_label_source": "ml_label_20d",
                "signal_quality_bucket": "signal_quality_high",
                "volume_participation_bucket": "volume_neutral",
                "decision_candle_quality": "candle_strong",
                "shape_classification": "shape_positive_modifier",
                "support_wick": True,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.02,
                "close_vs_ma60_pct": 0.04,
                "iizuka_context_block_pass": False,
                "iizuka_compression_block_pass": False,
                "iizuka_risk_block_pass": False,
                "drawdown60": None,
                "rebound60": None,
                "stable_bad_pick_family": False,
                "bull_marubozu": False,
                "ma20_distance_bucket": "near",
                "ma60_distance_bucket": "near",
            },
        ]
    )
    audit = _build_missing_feature_audit(frame, "2026-03-26")
    assert audit["classification"] == "gate_restrictiveness"
    assert audit["secondary_findings"]["forward_candidate_score_missing_rate"] == 1.0
    assert audit["secondary_findings"]["historical_candidate_score_missing_rate"] == 0.0


def test_date_alignment_and_feasibility_hold_when_forward_slice_has_no_active_pass() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-03-26",
                "symbol": "3001",
                "side": "long",
                "surface_key": "2026-03-26|3001|long",
                "research_fallback_label_source": "ml_label_20d",
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "iizuka_v2_role": "active",
                "iizuka_v2_reason": "active",
                "iizuka_v2_diagnostic_reason": "",
                "iizuka_v2_exclusion_reason": "",
            },
            {
                "anchor_date": "2026-03-27",
                "symbol": "3002",
                "side": "long",
                "surface_key": "2026-03-27|3002|long",
                "research_fallback_label_source": "ml_label_20d",
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "iizuka_v2_role": "diagnostic_only",
                "iizuka_v2_reason": "diagnostic_only|context_block_failed",
                "iizuka_v2_diagnostic_reason": "context_block_failed",
                "iizuka_v2_exclusion_reason": "",
            },
            {
                "anchor_date": "2026-03-30",
                "symbol": "3003",
                "side": "long",
                "surface_key": "2026-03-30|3003|long",
                "research_fallback_label_source": "ml_label_20d",
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "iizuka_v2_role": "excluded",
                "iizuka_v2_reason": "excluded|bull_marubozu",
                "iizuka_v2_diagnostic_reason": "",
                "iizuka_v2_exclusion_reason": "bull_marubozu",
            },
        ]
    )
    no_lookahead = {
        "no_lookahead_pass": True,
        "research_fallback_label_source": "ml_label_20d",
    }
    date_audit = _build_date_key_alignment_audit(frame, "2026-03-26")
    assert date_audit["calendar_correct"] is True
    assert date_audit["forward_duplicate_key_count"] == 0
    assert date_audit["forward_overlap_with_approved_exists"] is False
    coverage = {
        "forward_row_count": 2,
        "active_rows_forward": 0,
        "diagnostic_only_rows_forward": 1,
        "excluded_rows_forward": 1,
    }
    waterfall = {"stage_counts": {"base_context_block_fail": 1, "compression_block_fail": 1, "current_risk_baseline_fail": 1, "trigger_lane_fail": 1, "explicit_exclusion": 1}}
    missing = {"secondary_findings": {"forward_candidate_score_missing_rate": 1.0}}
    feasibility = _build_feasibility(coverage, waterfall, missing, date_audit)
    assert feasibility["classification"] == "no_forward_setup_hold"
