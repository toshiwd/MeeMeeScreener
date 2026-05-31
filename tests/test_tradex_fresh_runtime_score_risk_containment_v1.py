from __future__ import annotations

import pandas as pd

from scripts import tradex_fresh_runtime_score_risk_containment_v1 as mod


def _top() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260101, "code": "1001", "score_bucket": "top20", "fresh_runtime_research_watch_rank": 1, "fresh_runtime_research_watch_score": 0.9, "ret20": 0.12, "upper_wick_ratio": 0.2, "gap_pct": 0.01, "recent_high_distance_pct": -0.02, "volume_vs_20d_avg": 1.2, "body_ratio": 0.5, "close_vs_ma20_pct": 0.05, "recent_low_distance_pct": 0.1},
            {"as_of_date": 20260101, "code": "1002", "score_bucket": "top20", "fresh_runtime_research_watch_rank": 2, "fresh_runtime_research_watch_score": 0.8, "ret20": -0.12, "upper_wick_ratio": 0.8, "gap_pct": 0.12, "recent_high_distance_pct": 0.08, "volume_vs_20d_avg": 6.0, "body_ratio": 0.02, "close_vs_ma20_pct": 0.3, "recent_low_distance_pct": 0.0},
        ]
    )


def test_apply_variants_marks_point_in_time_risk_containment() -> None:
    rows = mod.apply_variants(_top())
    assert rows.loc[rows["code"] == "1001", "variant_a_contained"].iloc[0] == True
    assert rows.loc[rows["code"] == "1002", "variant_a_contained"].iloc[0] == False
    assert rows.loc[rows["code"] == "1001", "variant_c_contained"].iloc[0] == True


def test_variant_metrics_reports_kept_share() -> None:
    rows = mod.apply_variants(_top())
    metrics = mod.variant_metrics(rows)
    assert metrics["raw_top20"]["sample_count"] == 2
    assert metrics["variant_a_contained"]["sample_count"] == 1
    assert metrics["variant_a_contained"]["kept_share"] == 0.5


def test_buyability_gate_requires_support() -> None:
    rows = mod.apply_variants(_top())
    metrics = mod.variant_metrics(rows)
    gate = mod.buyability_gate(metrics)
    assert gate["any_buyability_gate_pass"] is False
    assert gate["thresholds"]["kept_share_min"] == 0.25


def test_decide_hold_when_risk_improves_but_gate_fails() -> None:
    metrics = {
        "raw_top20": {"bad_rate_ret20_lt_minus_5pct": 0.3, "severe_rate_ret20_lt_minus_10pct": 0.2},
        "variant_a_contained": {"bad_rate_ret20_lt_minus_5pct": 0.1, "severe_rate_ret20_lt_minus_10pct": 0.05},
    }
    decision, decision_class, reasons = mod.decide({"any_buyability_gate_pass": False}, metrics)
    assert decision == "fresh_runtime_risk_containment_improved_but_not_buyable"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert reasons
