from __future__ import annotations

import pandas as pd

from scripts.tradex_iizuka_pre_decisive_contract_redesign_audit_v1 import (
    _build_structural_levers,
    _build_v2_proposal,
    _build_v2_role,
)


def test_v2_role_classification_distinguishes_active_diagnostic_and_excluded() -> None:
    active_row = pd.Series(
        {
            "iizuka_context_block_pass": True,
            "iizuka_compression_block_pass": True,
            "iizuka_risk_block_pass": True,
            "bull_marubozu": False,
            "ma20_distance_bucket": "moderate",
            "ma60_distance_bucket": "near",
            "volume_participation_bucket": "volume_neutral",
            "support_wick": False,
            "bull_engulfing": False,
        }
    )
    diagnostic_row = pd.Series(
        {
            "iizuka_context_block_pass": True,
            "iizuka_compression_block_pass": True,
            "iizuka_risk_block_pass": True,
            "bull_marubozu": False,
            "ma20_distance_bucket": "moderate",
            "ma60_distance_bucket": "near",
            "volume_participation_bucket": "volume_confirmed",
            "support_wick": False,
            "bull_engulfing": False,
        }
    )
    excluded_row = pd.Series(
        {
            "iizuka_context_block_pass": True,
            "iizuka_compression_block_pass": True,
            "iizuka_risk_block_pass": True,
            "bull_marubozu": True,
            "ma20_distance_bucket": "moderate",
            "ma60_distance_bucket": "near",
            "volume_participation_bucket": "volume_neutral",
            "support_wick": True,
            "bull_engulfing": False,
        }
    )

    assert _build_v2_role(active_row) == "active"
    assert _build_v2_role(diagnostic_row) == "diagnostic_only"
    assert _build_v2_role(excluded_row) == "excluded"


def test_v2_proposal_preserves_active_neutral_volume_and_vetoes_marubozu() -> None:
    frame = pd.DataFrame(
        [
            {
                "iizuka_context_block_pass": True,
                "iizuka_compression_block_pass": True,
                "iizuka_risk_block_pass": True,
                "bull_marubozu": False,
                "ma20_distance_bucket": "moderate",
                "ma60_distance_bucket": "near",
                "volume_participation_bucket": "volume_neutral",
                "support_wick": False,
                "bull_engulfing": False,
                "top15_label": True,
                "bottom15_label": False,
                "forward_ret_20d": 0.1,
                "path_value_score_v1": 0.2,
            },
            {
                "iizuka_context_block_pass": True,
                "iizuka_compression_block_pass": True,
                "iizuka_risk_block_pass": True,
                "bull_marubozu": True,
                "ma20_distance_bucket": "moderate",
                "ma60_distance_bucket": "near",
                "volume_participation_bucket": "volume_confirmed",
                "support_wick": True,
                "bull_engulfing": False,
                "top15_label": False,
                "bottom15_label": True,
                "forward_ret_20d": -0.2,
                "path_value_score_v1": -0.3,
            },
        ]
    )

    levers = _build_structural_levers(frame)
    proposal = _build_v2_proposal(frame, levers)

    assert proposal["proposal_contract_name"] == "iizuka_pre_decisive_long_candidate_v2"
    assert "volume_neutral is active by default" in proposal["active_rows_definition"]["trigger"]
    assert "bull_marubozu is excluded" in proposal["active_rows_definition"]["vetoes"]
    assert levers["role_counts_on_current_surface"]["active_rows"] == 1
    assert levers["role_counts_on_current_surface"]["excluded_rows"] == 1
