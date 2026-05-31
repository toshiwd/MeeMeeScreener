from __future__ import annotations

import pandas as pd

from scripts import tradex_fresh_runtime_score_rank_depth_gate_v1 as mod


def _scored() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260101, "code": "1001", "fresh_runtime_research_watch_rank": 1, "fresh_runtime_research_watch_score": 0.9, "ret20": 0.2},
            {"as_of_date": 20260101, "code": "1002", "fresh_runtime_research_watch_rank": 6, "fresh_runtime_research_watch_score": 0.8, "ret20": -0.1},
            {"as_of_date": 20260101, "code": "1003", "fresh_runtime_research_watch_rank": 30, "fresh_runtime_research_watch_score": 0.4, "ret20": 0.0},
            {"as_of_date": 20260101, "code": "1004", "fresh_runtime_research_watch_rank": 101, "fresh_runtime_research_watch_score": 0.1, "ret20": 0.01},
        ]
    )


def test_rank_depth_metrics_splits_by_rank() -> None:
    metrics = mod.rank_depth_metrics(_scored())
    assert metrics["top5"]["sample_count"] == 1
    assert metrics["top10"]["sample_count"] == 2
    assert metrics["rank21_100"]["sample_count"] == 1
    assert metrics["remaining"]["sample_count"] == 1


def test_buyability_gate_requires_support_and_quality() -> None:
    metrics = mod.rank_depth_metrics(_scored())
    gate = mod.buyability_gate(metrics)
    assert gate["any_buyability_gate_pass"] is False
    assert gate["thresholds"]["sample_count_min"] == 1000


def test_decide_keep_when_any_depth_passes() -> None:
    gate = {"any_buyability_gate_pass": True}
    metrics = {"top5": {"mean_ret20": 0.05}, "top20": {"mean_ret20": 0.04}}
    decision, decision_class, reasons = mod.decide(gate, metrics)
    assert decision == "fresh_runtime_rank_depth_keep_for_next_validation"
    assert decision_class == "KEEP"
    assert reasons


def test_decide_hold_when_top5_improves_but_gate_fails() -> None:
    gate = {"any_buyability_gate_pass": False}
    metrics = {"top5": {"mean_ret20": 0.05}, "top20": {"mean_ret20": 0.04}}
    decision, decision_class, reasons = mod.decide(gate, metrics)
    assert decision == "fresh_runtime_rank_depth_promising_but_not_buyable"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert reasons
