from __future__ import annotations

import pandas as pd

from scripts import tradex_high_upside_reserve_family_discovery_v1 as mod


def test_feature_contract_classifies_outcomes_and_features() -> None:
    c = mod.feature_contract(["baseline_rank", "ret20"])
    assert c["fields"]["baseline_rank"]["classification"] == "point_in_time_feature"
    assert c["fields"]["ret20"]["classification"] == "outcome_only"
    assert c["fields"]["ret20_derived_terms"]["classification"] == "forbidden_future_leak"


def test_assign_buckets_uses_oos_probability_cutoffs() -> None:
    rows = pd.DataFrame(
        {
            "oos_eval": [True] * 100,
            "winner_probability": [i / 100 for i in range(100)],
        }
    )
    out = mod.assign_buckets(rows)
    assert (out["high_upside_bucket"] == "top_1pct").sum() >= 1
    assert (out["high_upside_bucket"] == "top_10pct").sum() >= 5
    assert (out["high_upside_bucket"] == "remaining_reserve").sum() > 80


def test_bucket_metric_reports_downside_to_upside_ratio() -> None:
    frame = pd.DataFrame(
        {
            "decision_date": [1, 1, 2],
            "code": ["A", "B", "C"],
            "ret5": [0.01, -0.01, 0.02],
            "ret20": [0.12, -0.06, 0.01],
            "winner_label": [True, False, False],
            "bad_label": [False, True, False],
            "severe_label": [False, False, False],
        }
    )
    m = mod.bucket_metric(frame)
    assert m["sample_count"] == 3
    assert m["winner_rate_ret20_gt_10pct"] == 1 / 3
    assert m["downside_to_upside_ratio"] == 1.0


def test_decide_keep_when_upside_and_containment_are_operational() -> None:
    metrics = {
        "top_5pct": {"mean_ret20": 0.07, "winner_rate_ret20_gt_10pct": 0.35, "bad_rate_ret20_lt_minus_5pct": 0.2, "severe_rate_ret20_lt_minus_10pct": 0.1},
        "remaining_reserve": {"mean_ret20": 0.02, "winner_rate_ret20_gt_10pct": 0.15},
    }
    profiles = {
        "best_risk_containment_profile": {
            "family_bad_rate": 0.2,
            "family_severe_rate": 0.1,
            "risk_containment_kept_share": 0.5,
            "family_average_candidates_per_date": 2.0,
        }
    }
    decision, _ = mod.decide(metrics, {}, profiles)
    assert decision == "high_upside_family_keep_for_pattern_portfolio_pretest"
