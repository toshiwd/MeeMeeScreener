from __future__ import annotations

import pytest

from app.backend.services.analysis.analysis_decision import build_analysis_decision, compute_analysis_decision_core
from external_analysis.runtime import analysis_adapter
from external_analysis.runtime.analysis_adapter import build_tradex_analysis_payload
from external_analysis.runtime.score_axes import (
    score_ev_upside_downside,
    score_short_bias_penalty,
    score_trend_direction,
    score_turning_momentum,
)
from external_analysis.runtime.score_context import prepare_tradex_score_context
from external_analysis.runtime.score_finalize import build_tradex_score_reasons, finalize_tradex_score_output
from external_analysis.runtime.input_normalization import NormalizedTradexAnalysisInput


def test_score_context_preparation_normalizes_inputs() -> None:
    context = prepare_tradex_score_context(
        analysis_p_up=None,
        analysis_p_down=0.2,
        analysis_p_turn_up=0.7,
        analysis_p_turn_down=0.3,
        analysis_ev_net=0.012,
        playbook_up_score_bonus=0.02,
        playbook_down_score_bonus=0.01,
        additive_signals={
            "bonusEstimate": 0.03,
            "mtfStrongAligned": True,
            "trendUpStrict": True,
            "monthlyBreakoutUpProb": 0.9,
        },
        sell_analysis={
            "trendDown": True,
            "aScore": 12,
            "bScore": 18,
            "distMa20Signed": 0.5,
            "ma20Slope": 0.02,
            "ma60Slope": 0.01,
        },
    )

    assert context["up_prob"] == 0.8
    assert context["down_prob"] == 0.2
    assert context["turn_up"] == 0.7
    assert context["turn_down"] == 0.3
    assert context["trend_down"] is True
    assert context["short_score"] == 30.0
    assert context["short_score_norm"] == 0.0
    assert context["strong_up_context"] is True


def test_score_finalization_assembles_public_decision_shape() -> None:
    decision = finalize_tradex_score_output(
        {
            "tone": "up",
            "side_label": "買い",
            "pattern_label": "pattern-a",
            "environment_label": "上昇優位",
            "confidence": 0.72,
            "buy_prob": 0.81,
            "neutral_prob": 0.12,
            "sell_prob": 0.07,
            "version": "2026-03-04-v2",
            "scenarios": [
                {"key": "up", "label": "上昇継続（押し目再開）", "tone": "up", "score": 0.81},
                {"key": "range", "label": "往復レンジ（上下振れ）", "tone": "neutral", "score": 0.12},
                {"key": "down", "label": "下落継続（戻り売り優位）", "tone": "down", "score": 0.07},
            ],
        }
    )

    assert decision == {
        "tone": "up",
        "sideLabel": "買い",
        "patternLabel": "pattern-a",
        "environmentLabel": "上昇優位",
        "confidence": 0.72,
        "buyProb": 0.81,
        "sellProb": 0.07,
        "neutralProb": 0.12,
        "version": "2026-03-04-v2",
        "scenarios": [
            {"key": "up", "label": "上昇継続（押し目再開）", "tone": "up", "score": 0.81},
            {"key": "range", "label": "往復レンジ（上下振れ）", "tone": "neutral", "score": 0.12},
            {"key": "down", "label": "下落継続（戻り売り優位）", "tone": "down", "score": 0.07},
        ],
    }
    assert build_tradex_score_reasons(decision) == (
        "tone=up",
        "pattern=pattern-a",
        "environment=上昇優位",
        "version=2026-03-04-v2",
    )


def test_tradex_analysis_adapter_composes_prepare_core_finalize_in_order(monkeypatch) -> None:
    calls: list[str] = []

    def fake_prepare(**kwargs):
        calls.append("prepare")
        return {
            "additive_signals": {"boxBottomAligned": True},
            "sell_analysis": {"trendDown": False},
            "up_prob": 0.6,
            "down_prob": 0.25,
            "turn_up": 0.5,
            "turn_down": 0.3,
            "ev_bias": 0.1,
            "additive_bias": 0.05,
            "up_playbook_bias": 0.05,
            "down_playbook_bias": 0.0,
            "trend_down": False,
            "trend_down_strict": False,
            "trend_down_penalty": 0.0,
            "trend_down_boost": 0.3,
            "short_score_norm": 0.0,
            "bullish_structure": False,
            "short_signal_confirmed": False,
            "strong_up_context": False,
            "analysis_ev_net": 0.01,
        }

    def fake_core(score_context):
        calls.append("core")
        assert score_context["up_prob"] == 0.6
        return {
            "tone": "neutral",
            "side_label": "中立",
            "pattern_label": "pattern",
            "environment_label": "environment",
            "confidence": 0.5,
            "buy_prob": 0.4,
            "neutral_prob": 0.3,
            "sell_prob": 0.3,
            "version": "2026-03-20",
            "scenarios": [{"key": "range", "label": "往復レンジ（上下振れ）", "tone": "neutral", "score": 0.3}],
        }

    def fake_finalize(core):
        calls.append("finalize")
        assert core["tone"] == "neutral"
        return {
            "tone": "neutral",
            "sideLabel": "中立",
            "patternLabel": "pattern",
            "environmentLabel": "environment",
            "confidence": 0.5,
            "buyProb": 0.4,
            "neutralProb": 0.3,
            "sellProb": 0.3,
            "version": "2026-03-20",
            "scenarios": [{"key": "range", "label": "往復レンジ（上下振れ）", "tone": "neutral", "score": 0.3}],
        }

    monkeypatch.setattr(analysis_adapter, "prepare_tradex_score_context", fake_prepare)
    monkeypatch.setattr(analysis_adapter, "compute_analysis_decision_core", fake_core)
    monkeypatch.setattr(analysis_adapter, "finalize_tradex_score_output", fake_finalize)
    monkeypatch.setattr(analysis_adapter, "build_candidate_comparison_payloads", lambda *args, **kwargs: ())

    output = build_tradex_analysis_payload(
        NormalizedTradexAnalysisInput(
            symbol="7203",
            asof="2026-03-19",
            decision_kwargs={
                "analysis_p_up": 0.6,
                "analysis_p_down": 0.25,
                "analysis_p_turn_up": 0.5,
                "analysis_p_turn_down": 0.3,
                "analysis_ev_net": 0.01,
                "playbook_up_score_bonus": 0.01,
                "playbook_down_score_bonus": 0.0,
                "additive_signals": {"bonusEstimate": 0.02},
                "sell_analysis": {"trendDown": False},
            },
            publish_readiness={"ready": True, "status": "approved", "reasons": ["validation_ok"]},
            override_state={"present": False},
            scenarios=(),
            source="tradex",
        )
    )

    assert calls[:3] == ["prepare", "core", "finalize"]
    assert output["decision"]["tone"] == "neutral"
    assert [item["candidate_key"] for item in output["candidate_comparisons"]] == []


def test_build_analysis_decision_wrapper_matches_extracted_pipeline() -> None:
    kwargs = {
        "analysis_p_up": 0.61,
        "analysis_p_down": 0.25,
        "analysis_p_turn_up": 0.42,
        "analysis_p_turn_down": 0.23,
        "analysis_ev_net": 0.004,
        "playbook_up_score_bonus": 0.01,
        "playbook_down_score_bonus": 0.0,
        "additive_signals": {"bonusEstimate": 0.015},
        "sell_analysis": {"trendDown": True},
    }

    wrapped = build_analysis_decision(**kwargs)
    prepared = prepare_tradex_score_context(**kwargs)
    core = compute_analysis_decision_core(prepared)
    expected = finalize_tradex_score_output(core)

    assert wrapped == expected


def test_axis_scores_use_a_stable_small_shape() -> None:
    score_context = prepare_tradex_score_context(
        analysis_p_up=0.63,
        analysis_p_down=0.22,
        analysis_p_turn_up=0.51,
        analysis_p_turn_down=0.28,
        analysis_ev_net=0.007,
        playbook_up_score_bonus=0.01,
        playbook_down_score_bonus=0.0,
        additive_signals={"bonusEstimate": 0.02, "boxBottomAligned": True},
        sell_analysis={"trendDown": False, "shortScore": 84},
    )

    for axis in (
        score_trend_direction(score_context),
        score_turning_momentum(score_context),
        score_ev_upside_downside(score_context),
        score_short_bias_penalty(score_context),
    ):
        payload = axis.to_dict()
        assert tuple(payload.keys()) == ("score", "signals", "components", "reasons")
        assert isinstance(payload["signals"], dict)
        assert isinstance(payload["components"], dict)
        assert isinstance(payload["reasons"], list)
        assert axis["score"] == pytest.approx(payload["score"])
