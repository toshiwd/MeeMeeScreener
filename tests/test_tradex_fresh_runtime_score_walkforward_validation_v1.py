from __future__ import annotations

import pandas as pd

from scripts import tradex_fresh_runtime_score_walkforward_validation_v1 as mod


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": 20260101,
                "code": "1001",
                "close": 100.0,
                "ma7": 99.0,
                "ma20": 95.0,
                "ma60": 90.0,
                "diff20_pct": 0.05,
                "cnt_20_above": 15,
                "cnt_7_above": 7,
                "o": 99.0,
                "h": 102.0,
                "l": 98.0,
                "c": 100.0,
                "v": 1200,
                "prev_close": 98.0,
                "volume20_avg": 1000.0,
                "recent_high20": 102.0,
                "recent_low20": 80.0,
                "close_fwd5": 105.0,
                "close_fwd20": 120.0,
            },
            {
                "as_of_date": 20260101,
                "code": "1002",
                "close": 100.0,
                "ma7": 101.0,
                "ma20": 102.0,
                "ma60": 103.0,
                "diff20_pct": -0.03,
                "cnt_20_above": 1,
                "cnt_7_above": 1,
                "o": 101.0,
                "h": 102.0,
                "l": 95.0,
                "c": 100.0,
                "v": 900,
                "prev_close": 101.0,
                "volume20_avg": 1000.0,
                "recent_high20": 120.0,
                "recent_low20": 95.0,
                "close_fwd5": 98.0,
                "close_fwd20": 90.0,
            },
        ]
    )


def test_build_scored_frame_ranks_without_using_outcome_as_feature() -> None:
    scored = mod.build_scored_frame(_frame())
    assert "fresh_runtime_research_watch_rank" in scored.columns
    assert scored.sort_values("fresh_runtime_research_watch_rank").iloc[0]["code"] == "1001"
    assert round(float(scored.loc[scored["code"] == "1001", "ret20"].iloc[0]), 6) == 0.2


def test_bucket_metrics_reports_top20() -> None:
    scored = mod.build_scored_frame(_frame())
    metrics = mod.bucket_metrics(scored)
    assert metrics["top20"]["sample_count"] == 2
    assert metrics["top20"]["winner_rate_ret20_gt_10pct"] == 0.5


def test_gate_requires_sample_and_stability_support() -> None:
    scored = mod.build_scored_frame(_frame())
    metrics = mod.bucket_metrics(scored)
    dates = mod.date_metrics(scored)
    gate = mod.buyability_gate(metrics, dates)
    assert gate["coverage_gate_pass"] is False
    assert gate["buyability_gate_pass"] is False


def test_decide_drops_when_top20_no_edge() -> None:
    metrics = {
        "top20": {"mean_ret20": 0.0, "sample_count": 1000, "date_count": 50, "outcome_coverage_rate": 1.0, "winner_rate_ret20_gt_10pct": 0.1, "bad_rate_ret20_lt_minus_5pct": 0.3, "severe_rate_ret20_lt_minus_10pct": 0.2},
        "remaining": {"mean_ret20": 0.01},
    }
    gate = {"buyability_gate_pass": False}
    decision, decision_class, reasons = mod.decide(gate, metrics)
    assert decision == "fresh_runtime_score_no_buyability_edge"
    assert decision_class == "DROP"
    assert reasons
