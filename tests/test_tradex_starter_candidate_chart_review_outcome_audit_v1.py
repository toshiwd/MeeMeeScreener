from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_candidate_chart_review_outcome_audit_v1 as mod


def test_bucket_metrics_computes_required_rates() -> None:
    rows = pd.DataFrame(
        [
            {"manual_judgment": "starter_ready", "decision_date": 20260508, "code": "A", "ret5": 0.01, "ret10": 0.02, "ret20": 0.04, "max_drawdown_20d": -0.02, "trigger_hit": True, "invalidation_hit": False, "trigger_then_ret20": 0.04},
            {"manual_judgment": "starter_ready", "decision_date": 20260508, "code": "B", "ret5": -0.01, "ret10": 0.00, "ret20": -0.06, "max_drawdown_20d": -0.08, "trigger_hit": False, "invalidation_hit": True, "trigger_then_ret20": None},
        ]
    )

    metrics = mod.bucket_metrics(rows)

    assert metrics["starter_ready"]["sample_count"] == 2
    assert metrics["starter_ready"]["hit_rate_ret20_gt_0"] == 0.5
    assert metrics["starter_ready"]["bad_rate_ret20_lt_minus_5pct"] == 0.5


def test_decide_sample_insufficient_for_single_date() -> None:
    rows = pd.DataFrame(
        [
            {"manual_judgment": "starter_ready", "decision_date": 20260508, "code": "A", "ret20": 0.1},
            {"manual_judgment": "avoid", "decision_date": 20260508, "code": "B", "ret20": -0.1},
        ]
    )
    metrics = mod.bucket_metrics(rows)
    comps = mod.comparisons(rows, metrics)

    assert mod.decide(rows, comps) == "sample_insufficient"


def test_audit_rows_uses_future_only() -> None:
    bars = pd.DataFrame(
        [
            {"code": "A", "ymd": 20260508, "c": 100.0, "h": 101.0, "l": 99.0},
            {"code": "A", "ymd": 20260511, "c": 103.0, "h": 104.0, "l": 102.0},
            {"code": "A", "ymd": 20260512, "c": 106.0, "h": 107.0, "l": 105.0},
        ]
    )
    rows = pd.DataFrame([{"code": "A", "decision_date": 20260508}])

    audited = mod.audit_rows(rows, bars)

    assert audited.loc[0, "forward_bar_count"] == 2
    assert bool(audited.loc[0, "trigger_hit"]) is True
