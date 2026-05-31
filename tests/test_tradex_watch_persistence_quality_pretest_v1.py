from __future__ import annotations

import pandas as pd

from scripts import tradex_watch_persistence_quality_pretest_v1 as mod


def test_add_persistence_features_uses_prior_only() -> None:
    rows = pd.DataFrame(
        [
            {"code": "A", "as_of_date": 20240131, "decision_date": 20240131},
            {"code": "A", "as_of_date": 20240215, "decision_date": 20240215},
            {"code": "A", "as_of_date": 20240430, "decision_date": 20240430},
        ]
    )

    out = mod.add_persistence_features(rows)

    assert bool(out.loc[0, "first_time_watch_flag"]) is True
    assert out.loc[1, "prior_watch_count_20d"] == 1
    assert bool(out.loc[1, "repeated_watch_2plus_flag"]) is True
    assert out.loc[2, "prior_watch_count_60d"] == 0


def test_bucket_metrics_compares_repeated_to_first_time() -> None:
    rows = pd.DataFrame(
        [
            {"code": "A", "as_of_date": 20240131, "first_time_watch_flag": True, "repeated_watch_2plus_flag": False, "repeated_watch_3plus_flag": False, "consecutive_watch_2plus_flag": False, "reappeared_after_gap_flag": False, "ret20": 0.01, "pattern_type": "pullback"},
            {"code": "A", "as_of_date": 20240215, "first_time_watch_flag": False, "repeated_watch_2plus_flag": True, "repeated_watch_3plus_flag": False, "consecutive_watch_2plus_flag": True, "reappeared_after_gap_flag": False, "ret20": 0.04, "pattern_type": "pullback"},
        ]
    )

    metrics = mod.bucket_metrics(rows)

    assert metrics["first_time_watch"]["sample_count"] == 1
    assert metrics["repeated_watch_2plus"]["comparison_vs_first_time_watch"]["mean_ret20_delta"] > 0


def test_decide_worse_than_first_time() -> None:
    metrics = {
        "repeated_watch_2plus": {
            "sample_count": 40,
            "date_count": 20,
            "comparison_vs_first_time_watch": {"mean_ret20_delta": -0.01, "bad_rate_delta": 0.0, "severe_rate_delta": 0.0},
        }
    }

    assert mod.decide(metrics) == "watch_persistence_worse_than_first_time"


def test_decide_keep_with_enough_support_and_improvement() -> None:
    metrics = {
        "repeated_watch_2plus": {
            "sample_count": 40,
            "date_count": 20,
            "comparison_vs_first_time_watch": {"mean_ret20_delta": 0.02, "bad_rate_delta": -0.01, "severe_rate_delta": 0.0},
        }
    }

    assert mod.decide(metrics) == "watch_persistence_keep_for_candidate_pool_pretest"
