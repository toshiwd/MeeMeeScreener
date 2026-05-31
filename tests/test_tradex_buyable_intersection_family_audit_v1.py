from __future__ import annotations

import pandas as pd

from scripts import tradex_buyable_intersection_family_audit_v1 as mod


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260101, "code": "1001", "fresh_runtime_research_watch_rank": 10, "buy_entry_qualified": True, "buy_breakout_surface": True, "ret20": 0.1, "variant_a_entry_qualified_top100": True, "variant_b_entry_qualified_top50": True, "variant_c_entry_qualified_top20": True},
            {"as_of_date": 20260101, "code": "1002", "fresh_runtime_research_watch_rank": 120, "buy_entry_qualified": True, "buy_breakout_surface": True, "ret20": -0.1, "variant_a_entry_qualified_top100": False, "variant_b_entry_qualified_top50": False, "variant_c_entry_qualified_top20": False},
        ]
    )


def test_variant_metrics_intersection_counts() -> None:
    metrics = mod.variant_metrics(_frame())
    assert metrics["entry_qualified_all"]["sample_count"] == 2
    assert metrics["variant_a_entry_qualified_top100"]["sample_count"] == 1
    assert metrics["variant_c_entry_qualified_top20"]["bad_rate_ret20_lt_minus_5pct"] == 0.0


def test_buyability_gate_requires_breadth() -> None:
    gate = mod.buyability_gate(mod.variant_metrics(_frame()))
    assert gate["any_buyability_gate_pass"] is False
    assert gate["thresholds"]["average_candidates_per_date_min"] == 1.0


def test_decide_keep_when_any_variant_passes() -> None:
    decision, decision_class, reasons = mod.decide({"any_buyability_gate_pass": True}, {})
    assert decision == "intersection_family_keep_for_forward_validation"
    assert decision_class == "KEEP"
    assert reasons


def test_decide_hold_when_return_positive_but_gate_fails() -> None:
    metrics = {"variant_a_entry_qualified_top100": {"mean_ret20": 0.04}}
    decision, decision_class, reasons = mod.decide({"any_buyability_gate_pass": False}, metrics)
    assert decision == "intersection_family_promising_but_not_buyable"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert reasons
