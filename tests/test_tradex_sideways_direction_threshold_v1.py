from __future__ import annotations

import pandas as pd

from scripts import tradex_sideways_direction_threshold_v1 as subject


def test_classify_state_has_up_down_and_unresolved() -> None:
    result = subject.classify_state(pd.Series([0.6, -0.7, 0.2]), 0.5)
    assert result.tolist() == ["up", "down", "unresolved"]


def test_zero_threshold_uses_sign_and_keeps_exact_zero_unresolved() -> None:
    result = subject.classify_state(pd.Series([0.01, -0.01, 0.0]), 0.0)
    assert result.tolist() == ["up", "down", "unresolved"]


def test_state_metrics_reports_side_specific_precision_and_coverage() -> None:
    frame = pd.DataFrame(
        {"direction_state": ["up", "up", "down", "unresolved"], "direction_up": [1, 0, 0, 1]}
    )
    metrics = subject.state_metrics(frame)
    assert metrics["coverage"] == 0.75
    assert metrics["direction_accuracy"] == 2 / 3
    assert metrics["up_precision"] == 0.5
    assert metrics["down_precision"] == 1.0


def test_select_minimum_threshold_requires_overall_and_yearly_pass() -> None:
    results = {
        "atr_0.00": {"threshold_atr": 0.0, "overall_gate_pass": True, "yearly_stability_pass": False},
        "atr_0.25": {"threshold_atr": 0.25, "overall_gate_pass": True, "yearly_stability_pass": True},
        "atr_0.50": {"threshold_atr": 0.50, "overall_gate_pass": True, "yearly_stability_pass": True},
    }
    assert subject.select_minimum_threshold(results) == 0.25


def test_zero_threshold_contract_matches_sign_classifier() -> None:
    states = subject.state_contract_descriptions(0.0)
    assert states["up"] == "two-session close move > 0 ATR14"
    assert states["down"] == "two-session close move < 0 ATR14"
    assert states["unresolved"] == "two-session close move = 0 ATR14"
