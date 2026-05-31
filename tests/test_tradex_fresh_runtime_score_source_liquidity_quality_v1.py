from __future__ import annotations

import pandas as pd

from scripts import tradex_fresh_runtime_score_source_liquidity_quality_v1 as mod


def _top() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260101, "code": "1001", "score_bucket": "top20", "fresh_runtime_research_watch_rank": 1, "ret20": 0.1, "close": 1000.0, "v": 1000, "source": "pan", "volume20_avg": 1000.0, "turnover20_value": 1_000_000.0, "volume_vs_20d_avg": 1.0},
            {"as_of_date": 20260101, "code": "1002", "score_bucket": "top20", "fresh_runtime_research_watch_rank": 2, "ret20": -0.1, "close": 50.0, "v": 100, "source": "yahoo", "volume20_avg": 100.0, "turnover20_value": 5_000.0, "volume_vs_20d_avg": 8.0},
        ]
    )


def test_variant_metrics_keeps_liquid_pan_rows() -> None:
    top = _top()
    top["quality_a_price_liquidity"] = [True, False]
    top["quality_b_source_pan_liquid"] = [True, False]
    top["quality_c_avoid_extreme_price_turnover"] = [True, False]
    metrics = mod.variant_metrics(top)
    assert metrics["raw_top20"]["sample_count"] == 2
    assert metrics["quality_b_source_pan_liquid"]["sample_count"] == 1
    assert metrics["quality_b_source_pan_liquid"]["bad_rate_ret20_lt_minus_5pct"] == 0.0


def test_buyability_gate_requires_support() -> None:
    top = _top()
    top["quality_a_price_liquidity"] = [True, False]
    top["quality_b_source_pan_liquid"] = [True, False]
    top["quality_c_avoid_extreme_price_turnover"] = [True, False]
    gate = mod.buyability_gate(mod.variant_metrics(top))
    assert gate["any_buyability_gate_pass"] is False
    assert gate["thresholds"]["sample_count_min"] == 1000


def test_decide_hold_when_quality_reduces_risk() -> None:
    metrics = {
        "raw_top20": {"bad_rate_ret20_lt_minus_5pct": 0.3, "severe_rate_ret20_lt_minus_10pct": 0.2},
        "quality_a_price_liquidity": {"bad_rate_ret20_lt_minus_5pct": 0.1, "severe_rate_ret20_lt_minus_10pct": 0.05},
    }
    decision, decision_class, reasons = mod.decide({"any_buyability_gate_pass": False}, metrics)
    assert decision == "fresh_runtime_source_liquidity_improved_but_not_buyable"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert reasons


def test_decide_keep_when_gate_passes() -> None:
    decision, decision_class, reasons = mod.decide({"any_buyability_gate_pass": True}, {})
    assert decision == "fresh_runtime_source_liquidity_keep_for_next_validation"
    assert decision_class == "KEEP"
    assert reasons
