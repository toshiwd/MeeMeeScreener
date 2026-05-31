from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_candidate_chart_review_historical_replay_v1 as mod


def test_monthly_grid_dates_uses_last_available_monthly_date_and_excludes_recent() -> None:
    selected, excluded = mod.monthly_grid_dates(
        [20240104, 20240131, 20240201, 20260501, 20260508],
        confirmed_max_date=20260514,
        min_year=2024,
    )

    assert selected == [20240131, 20240201]
    assert {row["candidate_date"] for row in excluded} == {20260501, 20260508}
    assert {row["reason"] for row in excluded} == {"ret20_not_fully_observable_by_calendar_cutoff"}


def test_decide_replay_promising_but_underpowered_when_ready_better_but_thin() -> None:
    rows = pd.DataFrame(
        [
            {"manual_judgment": "starter_ready", "decision_date": 20240131, "code": "A", "ret20": 0.10},
            {"manual_judgment": "starter_ready", "decision_date": 20240229, "code": "B", "ret20": 0.08},
            {"manual_judgment": "avoid", "decision_date": 20240131, "code": "C", "ret20": -0.06},
            {"manual_judgment": "wait_for_trigger", "decision_date": 20240229, "code": "D", "ret20": -0.01},
        ]
    )
    comparisons = mod.label_comparison_metrics(rows)

    assert mod.decide_replay(rows, comparisons) == "promising_but_underpowered"


def test_decide_replay_no_clear_when_ready_not_better_and_bad_not_lower() -> None:
    rows = pd.DataFrame(
        [
            {"manual_judgment": "starter_ready", "decision_date": 20240131, "code": "A", "ret20": -0.06},
            {"manual_judgment": "starter_ready", "decision_date": 20240229, "code": "B", "ret20": 0.02},
            {"manual_judgment": "avoid", "decision_date": 20240131, "code": "C", "ret20": -0.04},
            {"manual_judgment": "wait_for_trigger", "decision_date": 20240229, "code": "D", "ret20": 0.01},
        ]
    )
    comparisons = mod.label_comparison_metrics(rows)

    assert mod.decide_replay(rows, comparisons) in {"no_clear_separation", "worse_than_non_ready"}


def test_label_comparison_metrics_has_required_comparisons() -> None:
    rows = pd.DataFrame(
        [
            {"manual_judgment": "starter_ready", "decision_date": 20240131, "code": "A", "ret20": 0.10, "research_candidate_source_family": "pullback_reclaim_source"},
            {"manual_judgment": "starter_ready", "decision_date": 20240229, "code": "B", "ret20": 0.02, "research_candidate_source_family": "breakout_retest_source"},
            {"manual_judgment": "avoid", "decision_date": 20240131, "code": "C", "ret20": -0.04, "research_candidate_source_family": "breakout_retest_source"},
            {"manual_judgment": "wait_for_trigger", "decision_date": 20240229, "code": "D", "ret20": 0.01, "research_candidate_source_family": "early_trend_source"},
        ]
    )

    comparisons = mod.label_comparison_metrics(rows)

    assert "starter_ready_vs_all_non_ready" in comparisons
    assert "pullback_starter_ready_vs_breakout_starter_ready" in comparisons
    assert "early_trend_starter_ready_vs_other_starter_ready" in comparisons
    assert mod.any_comparison_underpowered(comparisons) is True
