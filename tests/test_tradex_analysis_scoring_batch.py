from __future__ import annotations

from app.backend.services.analysis.analysis_decision import compute_analysis_decision_core
from external_analysis.contracts.analysis_input import AnalysisInputContract
from external_analysis.runtime.orchestrator import run_tradex_analysis
from external_analysis.runtime.score_aggregate import aggregate_tradex_score_decision
from external_analysis.runtime.score_axes import (
    score_ev_upside_downside,
    score_short_bias_penalty,
    score_trend_direction,
    score_turning_momentum,
)
from external_analysis.runtime.score_context import prepare_tradex_score_context


def test_tradex_axis_scorers_feed_the_aggregator_without_drift() -> None:
    kwargs = dict(
        analysis_p_up=0.81,
        analysis_p_down=0.07,
        analysis_p_turn_up=0.54,
        analysis_p_turn_down=0.31,
        analysis_ev_net=0.012,
        playbook_up_score_bonus=0.02,
        playbook_down_score_bonus=0.01,
        additive_signals={"bonusEstimate": 0.03},
        sell_analysis={"trendDown": False},
    )
    score_context = prepare_tradex_score_context(**kwargs)
    trend_axis = score_trend_direction(score_context)
    turning_axis = score_turning_momentum(score_context)
    ev_axis = score_ev_upside_downside(score_context)
    short_axis = score_short_bias_penalty(score_context)

    aggregated = aggregate_tradex_score_decision(
        trend_axis=trend_axis,
        turning_axis=turning_axis,
        ev_axis=ev_axis,
        short_axis=short_axis,
    )
    computed = compute_analysis_decision_core(score_context)

    assert aggregated == computed
    assert trend_axis["direction_up_component"] == 0.41800000000000004
    assert trend_axis["direction_down_component"] == 0.060500000000000005
    assert turning_axis["turn_up_component"] == 0.09720000000000001
    assert turning_axis["turn_down_component"] == 0.0682
    assert ev_axis["up_ev_component"] == 0.2145
    assert ev_axis["down_ev_component"] == 0.08449999999999999
    assert short_axis["short_score_norm"] == 0.0


def test_tradex_analysis_regression_snapshot_bullish() -> None:
    output = run_tradex_analysis(
        AnalysisInputContract(
            symbol="7203",
            asof="2026-03-19",
            analysis_p_up=0.81,
            analysis_p_down=0.07,
            analysis_p_turn_up=0.54,
            analysis_p_turn_down=0.31,
            analysis_ev_net=0.012,
            playbook_up_score_bonus=0.02,
            playbook_down_score_bonus=0.01,
            additive_signals={"bonusEstimate": 0.03},
            sell_analysis={"trendDown": False},
            scenarios=(
                {"key": "up", "label": "buy", "tone": "up", "score": 0.81, "publish_ready": True},
                {"key": "range", "label": "neutral", "tone": "neutral", "score": 0.12, "publish_ready": False},
                {"key": "down", "label": "sell", "tone": "down", "score": 0.07, "publish_ready": False},
            ),
        )
    ).to_dict()

    assert output == {
        "symbol": "7203",
        "asof": "2026-03-19",
        "side_ratios": {"buy": 0.7297000000000001, "neutral": 0.437, "sell": 0.2132},
        "confidence": 0.7297000000000001,
        "reasons": [
            "tone=up",
            "pattern=上昇継続（押し目再開）",
            "environment=上昇優位",
            "version=2026-03-04-v2",
        ],
        "candidate_comparisons": [
            {
                "candidate_key": "up",
                "baseline_key": "up",
                "comparison_scope": "decision_scenarios",
                "score": 0.7297000000000001,
                "score_delta": 0.0,
                "rank": 1,
                "reasons": ["key=up", "label=上昇継続（押し目再開）", "tone=up"],
                "publish_ready": None,
            },
            {
                "candidate_key": "range",
                "baseline_key": "up",
                "comparison_scope": "decision_scenarios",
                "score": 0.437,
                "score_delta": -0.2927000000000001,
                "rank": 2,
                "reasons": ["key=range", "label=往復レンジ（上下振れ）", "tone=neutral"],
                "publish_ready": None,
            },
            {
                "candidate_key": "down",
                "baseline_key": "up",
                "comparison_scope": "decision_scenarios",
                "score": 0.2132,
                "score_delta": -0.5165000000000002,
                "rank": 3,
                "reasons": ["key=down", "label=下落継続（戻り売り優位）", "tone=down"],
                "publish_ready": None,
            },
        ],
        "publish_readiness": {
            "ready": False,
            "status": "not_evaluated",
            "reasons": [],
            "candidate_key": None,
            "approved": None,
        },
        "override_state": {
            "present": False,
            "source": None,
            "logic_key": None,
            "logic_version": None,
            "reason": None,
        },
        "source": "tradex_analysis",
        "schema_version": "tradex_analysis_output_v1",
    }


def test_tradex_analysis_regression_snapshot_range() -> None:
    output = run_tradex_analysis(
        AnalysisInputContract(
            symbol="9432",
            asof="2026-03-19",
            analysis_p_up=0.46,
            analysis_p_down=0.41,
            analysis_p_turn_up=0.38,
            analysis_p_turn_down=0.36,
            analysis_ev_net=-0.002,
            playbook_up_score_bonus=0.0,
            playbook_down_score_bonus=0.02,
            additive_signals={"bonusEstimate": -0.01, "boxBottomAligned": True, "monthlyRangeProb": 0.67, "monthlyRangePos": 0.42},
            sell_analysis={"trendDown": False, "trendDownStrict": False, "shortScore": 96, "distMa20Signed": 0.3, "ma20Slope": 0.01, "ma60Slope": 0.0},
            scenarios=(
                {"key": "down", "label": "sell", "tone": "down", "score": 0.54},
                {"key": "up", "label": "buy", "tone": "up", "score": 0.39},
            ),
        )
    ).to_dict()

    assert output == {
        "symbol": "9432",
        "asof": "2026-03-19",
        "side_ratios": {"buy": 0.4220666666666667, "neutral": 0.778, "sell": 0.44386666666666663},
        "confidence": 0.778,
        "reasons": [
            "tone=neutral",
            "pattern=往復レンジ（上下振れ）",
            "environment=レンジ優位（先回り買い監視）",
            "version=2026-03-04-v2",
        ],
        "candidate_comparisons": [
            {
                "candidate_key": "range",
                "baseline_key": "neutral",
                "comparison_scope": "decision_scenarios",
                "score": 0.778,
                "score_delta": 0.0,
                "rank": 1,
                "reasons": ["key=range", "label=往復レンジ（上下振れ）", "tone=neutral"],
                "publish_ready": None,
            },
            {
                "candidate_key": "down",
                "baseline_key": "neutral",
                "comparison_scope": "decision_scenarios",
                "score": 0.44386666666666663,
                "score_delta": -0.3341333333333334,
                "rank": 2,
                "reasons": ["key=down", "label=下落継続（戻り売り優位）", "tone=down"],
                "publish_ready": None,
            },
            {
                "candidate_key": "up",
                "baseline_key": "neutral",
                "comparison_scope": "decision_scenarios",
                "score": 0.4220666666666667,
                "score_delta": -0.3559333333333333,
                "rank": 3,
                "reasons": ["key=up", "label=上昇継続（押し目再開）", "tone=up"],
                "publish_ready": None,
            },
        ],
        "publish_readiness": {
            "ready": False,
            "status": "not_evaluated",
            "reasons": [],
            "candidate_key": None,
            "approved": None,
        },
        "override_state": {
            "present": False,
            "source": None,
            "logic_key": None,
            "logic_version": None,
            "reason": None,
        },
        "source": "tradex_analysis",
        "schema_version": "tradex_analysis_output_v1",
    }
