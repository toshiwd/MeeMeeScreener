from __future__ import annotations

import pandas as pd

from scripts import tradex_fresh_runtime_score_regime_containment_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260101, "code": "1001", "fresh_runtime_research_watch_rank": 1, "ret20": 0.1, "regime_id": "risk_on_trend", "breadth_above_ma20": 0.6, "advancers_ratio": 0.6, "index_close_vs_ma20": 0.01, "market_atr_pct": 0.01},
            {"as_of_date": 20260102, "code": "1002", "fresh_runtime_research_watch_rank": 1, "ret20": -0.1, "regime_id": "risk_off", "breadth_above_ma20": 0.3, "advancers_ratio": 0.3, "index_close_vs_ma20": -0.05, "market_atr_pct": 0.04},
        ]
    )


def test_variant_metrics_separates_risk_on() -> None:
    metrics = mod.variant_metrics(_rows().assign(variant_a_risk_on_only=[True, False], variant_b_breadth_support=[True, False], variant_c_strict_regime=[True, False]))
    assert metrics["raw_top20"]["sample_count"] == 2
    assert metrics["variant_a_risk_on_only"]["sample_count"] == 1
    assert metrics["variant_a_risk_on_only"]["bad_rate_ret20_lt_minus_5pct"] == 0.0


def test_regime_bucket_metrics_groups_regime() -> None:
    buckets = mod.regime_bucket_metrics(_rows())
    assert "risk_on_trend" in buckets
    assert buckets["risk_off"]["sample_count"] == 1


def test_buyability_gate_requires_support() -> None:
    metrics = {
        "raw_top20": {"sample_count": 2},
        "variant_a_risk_on_only": {"sample_count": 1, "date_count": 1, "kept_share": 0.5, "outcome_coverage_rate": 1.0, "mean_ret20": 0.1, "winner_rate_ret20_gt_10pct": 0.0, "bad_rate_ret20_lt_minus_5pct": 0.0, "severe_rate_ret20_lt_minus_10pct": 0.0},
    }
    gate = mod.buyability_gate(metrics)
    assert gate["any_buyability_gate_pass"] is False
    assert gate["thresholds"]["date_count_min"] == 50


def test_decide_hold_when_risk_improves() -> None:
    metrics = {
        "raw_top20": {"bad_rate_ret20_lt_minus_5pct": 0.3, "severe_rate_ret20_lt_minus_10pct": 0.2},
        "variant_a_risk_on_only": {"bad_rate_ret20_lt_minus_5pct": 0.1, "severe_rate_ret20_lt_minus_10pct": 0.05},
    }
    decision, decision_class, reasons = mod.decide({"any_buyability_gate_pass": False}, metrics)
    assert decision == "fresh_runtime_regime_containment_improved_but_not_buyable"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert reasons
