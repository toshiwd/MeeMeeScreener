from __future__ import annotations

import pytest

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


def _snapshot(output: dict[str, object]) -> dict[str, object]:
    return {
        "tone": str(output["reasons"][0]).split("=", 1)[1],
        "confidence": output["confidence"],
        "side_ratios": output["side_ratios"],
        "candidate_keys": [item["candidate_key"] for item in output["candidate_comparisons"]],
        "reason_prefixes": [str(reason).split("=", 1)[0] for reason in output["reasons"]],
        "candidate_reason_prefixes": [
            [str(reason).split("=", 1)[0] for reason in item["reasons"]]
            for item in output["candidate_comparisons"]
        ],
    }


def _run_snapshot(contract: AnalysisInputContract) -> dict[str, object]:
    return _snapshot(run_tradex_analysis(contract).to_dict())


@pytest.mark.parametrize(
    ("contract", "expected"),
    [
        (
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
            ),
            {
                "tone": "up",
                "confidence": 0.7297000000000001,
                "side_ratios": {"buy": 0.7297000000000001, "neutral": 0.437, "sell": 0.2132},
                "candidate_keys": ["up", "range", "down"],
                "reason_prefixes": ["tone", "pattern", "environment", "version"],
            },
        ),
        (
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
                additive_signals={
                    "bonusEstimate": -0.01,
                    "boxBottomAligned": True,
                    "monthlyRangeProb": 0.67,
                    "monthlyRangePos": 0.42,
                },
                sell_analysis={
                    "trendDown": False,
                    "trendDownStrict": False,
                    "shortScore": 96,
                    "distMa20Signed": 0.3,
                    "ma20Slope": 0.01,
                    "ma60Slope": 0.0,
                },
                scenarios=(
                    {"key": "down", "label": "sell", "tone": "down", "score": 0.54},
                    {"key": "up", "label": "buy", "tone": "up", "score": 0.39},
                ),
            ),
            {
                "tone": "neutral",
                "confidence": 0.778,
                "side_ratios": {"buy": 0.4220666666666667, "neutral": 0.778, "sell": 0.44386666666666663},
                "candidate_keys": ["range", "down", "up"],
                "reason_prefixes": ["tone", "pattern", "environment", "version"],
            },
        ),
        (
            AnalysisInputContract(
                symbol="9984",
                asof="2026-03-19",
                analysis_p_up=0.22,
                analysis_p_down=0.76,
                analysis_p_turn_up=0.25,
                analysis_p_turn_down=0.71,
                analysis_ev_net=-0.021,
                playbook_up_score_bonus=0.0,
                playbook_down_score_bonus=0.03,
                additive_signals={"bonusEstimate": -0.02, "monthlyRangeProb": 0.16, "monthlyRangePos": 0.88},
                sell_analysis={
                    "trendDown": True,
                    "trendDownStrict": True,
                    "shortScore": 138,
                    "distMa20Signed": -0.6,
                    "ma20Slope": -0.03,
                    "ma60Slope": -0.01,
                },
                scenarios=(
                    {"key": "down", "label": "sell", "tone": "down", "score": 0.83},
                    {"key": "range", "label": "neutral", "tone": "neutral", "score": 0.18},
                    {"key": "up", "label": "buy", "tone": "up", "score": 0.08},
                ),
            ),
            {
                "tone": "down",
                "confidence": 0.7810333333333335,
                "side_ratios": {"buy": 0.15924999999999997, "neutral": 0.45399999999999996, "sell": 0.7810333333333335},
                "candidate_keys": ["down", "range", "up"],
                "reason_prefixes": ["tone", "pattern", "environment", "version"],
            },
        ),
        (
            AnalysisInputContract(
                symbol="4751",
                asof="2026-03-19",
                analysis_p_up=0.57,
                analysis_p_down=0.28,
                analysis_p_turn_up=0.73,
                analysis_p_turn_down=0.18,
                analysis_ev_net=0.005,
                playbook_up_score_bonus=0.01,
                playbook_down_score_bonus=0.0,
                additive_signals={"bonusEstimate": 0.01, "mtfStrongAligned": True},
                sell_analysis={"trendDown": False, "shortScore": 60, "distMa20Signed": 0.2, "ma20Slope": 0.01, "ma60Slope": 0.0},
                scenarios=(
                    {"key": "up", "label": "buy", "tone": "up", "score": 0.68},
                    {"key": "range", "label": "neutral", "tone": "neutral", "score": 0.31},
                    {"key": "down", "label": "sell", "tone": "down", "score": 0.21},
                ),
            ),
            {
                "tone": "up",
                "confidence": 0.6159833333333333,
                "side_ratios": {"buy": 0.6159833333333333, "neutral": 0.613, "sell": 0.2839333333333333},
                "candidate_keys": ["up", "range", "down"],
                "reason_prefixes": ["tone", "pattern", "environment", "version"],
            },
        ),
        (
            AnalysisInputContract(
                symbol="5801",
                asof="2026-03-19",
                analysis_p_up=0.31,
                analysis_p_down=0.58,
                analysis_p_turn_up=0.17,
                analysis_p_turn_down=0.76,
                analysis_ev_net=-0.008,
                playbook_up_score_bonus=0.0,
                playbook_down_score_bonus=0.02,
                additive_signals={"bonusEstimate": -0.01},
                sell_analysis={"trendDown": True, "trendDownStrict": False, "shortScore": 104, "distMa20Signed": -0.2, "ma20Slope": -0.01, "ma60Slope": -0.01},
                scenarios=(
                    {"key": "down", "label": "sell", "tone": "down", "score": 0.71},
                    {"key": "range", "label": "neutral", "tone": "neutral", "score": 0.26},
                    {"key": "up", "label": "buy", "tone": "up", "score": 0.14},
                ),
            ),
            {
                "tone": "down",
                "confidence": 0.6573666666666667,
                "side_ratios": {"buy": 0.26076666666666665, "neutral": 0.603, "sell": 0.6573666666666667},
                "candidate_keys": ["down", "range", "up"],
                "reason_prefixes": ["tone", "pattern", "environment", "version"],
            },
        ),
        (
            AnalysisInputContract(
                symbol="8035",
                asof="2026-03-19",
                analysis_p_up=0.49,
                analysis_p_down=0.45,
                analysis_p_turn_up=0.41,
                analysis_p_turn_down=0.37,
                analysis_ev_net=0.001,
                playbook_up_score_bonus=0.01,
                playbook_down_score_bonus=0.01,
                additive_signals={"bonusEstimate": 0.0, "monthlyRangeProb": 0.52, "monthlyRangePos": 0.47},
                sell_analysis={"trendDown": False, "trendDownStrict": False, "shortScore": 88, "distMa20Signed": 0.08, "ma20Slope": 0.0, "ma60Slope": 0.0},
                scenarios=(
                    {"key": "up", "label": "buy", "tone": "up", "score": 0.48},
                    {"key": "range", "label": "neutral", "tone": "neutral", "score": 0.47},
                    {"key": "down", "label": "sell", "tone": "down", "score": 0.44},
                ),
            ),
            {
                "tone": "neutral",
                "confidence": 0.79,
                "side_ratios": {"buy": 0.48521666666666663, "neutral": 0.79, "sell": 0.43239999999999995},
                "candidate_keys": ["range", "up", "down"],
                "reason_prefixes": ["tone", "pattern", "environment", "version"],
            },
        ),
    ],
)
def test_tradex_analysis_regression_snapshots(contract: AnalysisInputContract, expected: dict[str, object]) -> None:
    output = _run_snapshot(contract)
    assert output["tone"] == expected["tone"]
    assert output["confidence"] == pytest.approx(expected["confidence"])
    assert output["side_ratios"] == expected["side_ratios"]
    assert output["candidate_keys"] == expected["candidate_keys"]
    assert output["reason_prefixes"] == expected["reason_prefixes"]
    assert output["candidate_reason_prefixes"] == [
        ["key", "label", "tone"],
        ["key", "label", "tone"],
        ["key", "label", "tone"],
    ]


def test_tradex_analysis_reason_sequence_is_stable_for_identical_inputs() -> None:
    contract = AnalysisInputContract(
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

    first = run_tradex_analysis(contract).to_dict()
    second = run_tradex_analysis(contract).to_dict()

    assert first["reasons"] == second["reasons"]
    assert [item["reasons"] for item in first["candidate_comparisons"]] == [
        item["reasons"] for item in second["candidate_comparisons"]
    ]
    assert [str(reason).split("=", 1)[0] for reason in first["reasons"]] == [
        "tone",
        "pattern",
        "environment",
        "version",
    ]
    assert str(first["reasons"][0]) == "tone=up"
    assert str(first["reasons"][1]) == "pattern=上昇継続（押し目再開）"
    assert str(first["reasons"][3]) == "version=2026-03-04-v2"
    assert [item["reasons"] for item in first["candidate_comparisons"]] == [
        ["key=up", "label=上昇継続（押し目再開）", "tone=up"],
        ["key=range", "label=往復レンジ（上下振れ）", "tone=neutral"],
        ["key=down", "label=下落継続（戻り売り優位）", "tone=down"],
    ]


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
    assert trend_axis["score"] == pytest.approx(max(trend_axis["components"].values()))
    assert turning_axis["score"] == pytest.approx(max(turning_axis["components"].values()))
    assert ev_axis["score"] == pytest.approx(max(ev_axis["components"].values()))
    assert short_axis["score"] == pytest.approx(1.0 - short_axis["components"]["short_penalty_component"])
    assert trend_axis["components"]["direction_up_component"] == pytest.approx(0.41800000000000004)
    assert trend_axis["components"]["direction_down_component"] == pytest.approx(0.060500000000000005)
    assert turning_axis["components"]["turn_up_component"] == pytest.approx(0.09720000000000001)
    assert turning_axis["components"]["turn_down_component"] == pytest.approx(0.0682)
    assert ev_axis["components"]["up_ev_component"] == pytest.approx(0.2145)
    assert ev_axis["components"]["down_ev_component"] == pytest.approx(0.08449999999999999)
    assert short_axis["signals"]["short_score_norm"] == 0.0
