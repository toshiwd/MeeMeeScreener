from __future__ import annotations

import pandas as pd

from scripts.tradex_iizuka_pre_decisive_long_candidate_v2 import (
    _classify_v2_row,
    _enrich_frame,
    _select_output_columns,
)


def test_v2_classification_respects_long_only_and_trigger_contract() -> None:
    active_row = pd.Series(
        {
            "side": "long",
            "iizuka_context_block_pass": True,
            "iizuka_compression_block_pass": True,
            "iizuka_risk_block_pass": True,
            "stable_bad_pick_family": False,
            "bull_marubozu": False,
            "ma20_distance_bucket": "near",
            "ma60_distance_bucket": "near",
            "volume_participation_bucket": "volume_neutral",
            "support_wick": False,
            "bull_engulfing": False,
            "drawdown60": -0.05,
            "rebound60": 0.12,
        }
    )
    diagnostic_row = pd.Series(
        {
            "side": "long",
            "iizuka_context_block_pass": True,
            "iizuka_compression_block_pass": True,
            "iizuka_risk_block_pass": True,
            "stable_bad_pick_family": False,
            "bull_marubozu": False,
            "ma20_distance_bucket": "near",
            "ma60_distance_bucket": "near",
            "volume_participation_bucket": "volume_confirmed",
            "support_wick": False,
            "bull_engulfing": False,
            "drawdown60": -0.05,
            "rebound60": 0.12,
        }
    )
    excluded_row = pd.Series(
        {
            "side": "short",
            "iizuka_context_block_pass": True,
            "iizuka_compression_block_pass": True,
            "iizuka_risk_block_pass": True,
            "stable_bad_pick_family": False,
            "bull_marubozu": True,
            "ma20_distance_bucket": "very_extended",
            "ma60_distance_bucket": "very_extended",
            "volume_participation_bucket": "volume_neutral",
            "support_wick": True,
            "bull_engulfing": False,
            "drawdown60": -0.05,
            "rebound60": 0.12,
        }
    )

    assert _classify_v2_row(active_row)[0] == "active"
    assert _classify_v2_row(diagnostic_row)[0] == "diagnostic_only"
    assert _classify_v2_row(excluded_row)[0] == "excluded"


def test_enrich_frame_preserves_contract_fields_and_ranks_active_rows() -> None:
    frame = pd.DataFrame(
        [
            {
                "anchor_date": "2025-01-01",
                "symbol": "1111",
                "side": "long",
                "champion_rank": 2,
                "iizuka_candidate_score": 4.0,
                "iizuka_context_block_pass": True,
                "iizuka_compression_block_pass": True,
                "iizuka_risk_block_pass": True,
                "stable_bad_pick_family": False,
                "bull_marubozu": False,
                "ma20_distance_bucket": "near",
                "ma60_distance_bucket": "near",
                "volume_participation_bucket": "volume_confirmed",
                "support_wick": False,
                "bull_engulfing": False,
                "drawdown60": -0.05,
                "rebound60": 0.12,
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "top15_label": True,
                "bottom15_label": False,
                "forward_ret_20d": 0.1,
                "path_value_score_v1": 0.2,
            },
            {
                "anchor_date": "2025-01-01",
                "symbol": "2222",
                "side": "long",
                "champion_rank": 1,
                "iizuka_candidate_score": 6.0,
                "iizuka_context_block_pass": True,
                "iizuka_compression_block_pass": True,
                "iizuka_risk_block_pass": True,
                "stable_bad_pick_family": False,
                "bull_marubozu": False,
                "ma20_distance_bucket": "near",
                "ma60_distance_bucket": "near",
                "volume_participation_bucket": "volume_confirmed",
                "support_wick": True,
                "bull_engulfing": False,
                "drawdown60": -0.05,
                "rebound60": 0.12,
                "monthly_context_no_lookahead": True,
                "weekly_context_no_lookahead": True,
                "top15_label": False,
                "bottom15_label": True,
                "forward_ret_20d": -0.1,
                "path_value_score_v1": -0.2,
            },
        ]
    )

    enriched = _enrich_frame(frame)

    assert enriched["candidate_contract_name"].eq("iizuka_pre_decisive_long_candidate_v2").all()
    assert enriched["research_only"].eq(True).all()
    assert enriched["research_fallback_label_source"].eq("ml_label_20d").all()
    assert enriched["iizuka_v2_role"].tolist() == ["diagnostic_only", "active"]
    assert enriched.loc[enriched["iizuka_v2_role"] == "active", "iizuka_v2_candidate_rank"].tolist() == [1.0]
    assert set(["iizuka_v2_role", "iizuka_v2_reason", "iizuka_v2_active_pass"]).issubset(_select_output_columns(enriched))
