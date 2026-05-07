from __future__ import annotations

import pandas as pd

from scripts.tradex_iizuka_pre_decisive_long_candidate_v2_challenger_design import (
    _build_challenger_frame,
    _build_contract,
    _build_month_audit,
)


def test_challenger_rank_prioritizes_signal_and_volume_lanes() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2026-01-05",
                "symbol": "1111",
                "surface_key": "2026-01-05|1111|long",
                "side": "long",
                "champion_rank": 3,
                "iizuka_v2_candidate_score": 8.0,
                "signal_quality_bucket": "signal_quality_high",
                "volume_participation_bucket": "volume_neutral",
                "decision_candle_quality": "candle_strong",
                "shape_classification": "shape_positive_modifier",
                "support_wick": True,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.01,
                "close_vs_ma60_pct": 0.03,
            },
            {
                "anchor_date": "2026-01-05",
                "symbol": "2222",
                "surface_key": "2026-01-05|2222|long",
                "side": "long",
                "champion_rank": 2,
                "iizuka_v2_candidate_score": 9.5,
                "signal_quality_bucket": "signal_quality_high",
                "volume_participation_bucket": "volume_confirmed",
                "decision_candle_quality": "candle_strong",
                "shape_classification": "shape_positive_modifier",
                "support_wick": True,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.01,
                "close_vs_ma60_pct": 0.03,
            },
            {
                "anchor_date": "2026-01-05",
                "symbol": "3333",
                "surface_key": "2026-01-05|3333|long",
                "side": "long",
                "champion_rank": 1,
                "iizuka_v2_candidate_score": 10.0,
                "signal_quality_bucket": "signal_quality_mid",
                "volume_participation_bucket": "volume_neutral",
                "decision_candle_quality": "candle_mixed",
                "shape_classification": "shape_context_dependent",
                "support_wick": False,
                "bull_engulfing": False,
                "close_vs_ma20_pct": 0.04,
                "close_vs_ma60_pct": 0.07,
            },
        ]
    )

    challenger = _build_challenger_frame(frame)

    assert challenger["research_only"].eq(True).all()
    assert challenger["candidate_contract_name"].eq("iizuka_pre_decisive_long_candidate_v2_challenger_design").all()
    assert challenger["v2_challenger_candidate_rank"].tolist() == [1, 2, 3]
    assert challenger.iloc[0]["symbol"] == "1111"
    assert challenger.iloc[1]["symbol"] == "2222"
    assert challenger.iloc[2]["symbol"] == "3333"


def test_contract_and_month_audit_expose_required_structure() -> None:
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
    challenger = _build_challenger_frame(frame)
    contract = _build_contract(challenger)
    comparison_frame = challenger.copy()
    comparison_frame["iizuka_v2_role"] = "active"
    comparison_frame["iizuka_v2_reason"] = "active|context_pass|compression_pass|risk_pass"
    month_audit = _build_month_audit(comparison_frame, {"per_k": []})

    assert contract["candidate_contract_name"] == "iizuka_pre_decisive_long_candidate_v2_challenger_design"
    assert contract["required_non_outcome_fields_present"]["signal_quality_bucket"] is True
    assert month_audit["active_month_count"] == 2
    assert set(month_audit["per_k"].keys()) == {"5", "10", "20"}
    assert len(month_audit["per_k"]["20"]) == 2
    assert "one_month_dominated" in month_audit["improvement_concentration"]
