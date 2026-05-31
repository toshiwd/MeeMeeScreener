from __future__ import annotations

import pandas as pd

from scripts import tradex_recent_event_risk_buyability_pretest_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260501, "code": "1001", "selected_event_snapshot_date": "2026-04-26", "earnings_nearby_flag": True, "ex_rights_nearby_flag": False, "ret20": -0.10, "winner_ret20_gt_10pct": False, "bad_ret20_lt_minus_5pct": True, "severe_ret20_lt_minus_10pct": True},
            {"as_of_date": 20260501, "code": "1002", "selected_event_snapshot_date": "2026-04-26", "earnings_nearby_flag": False, "ex_rights_nearby_flag": True, "ret20": 0.08, "winner_ret20_gt_10pct": False, "bad_ret20_lt_minus_5pct": False, "severe_ret20_lt_minus_10pct": False},
            {"as_of_date": 20250101, "code": "1003", "selected_event_snapshot_date": None, "earnings_nearby_flag": pd.NA, "ex_rights_nearby_flag": pd.NA, "ret20": 0.01, "winner_ret20_gt_10pct": False, "bad_ret20_lt_minus_5pct": False, "severe_ret20_lt_minus_10pct": False},
        ]
    )


def test_add_recent_event_selection_marks_exclusion_and_candidate() -> None:
    rows = mod.add_recent_event_selection(_rows())
    assert bool(rows.loc[0, "event_risk_exclusion_flag"])
    assert not bool(rows.loc[0, "recent_event_buyability_candidate_flag"])
    assert bool(rows.loc[1, "recent_event_buyability_candidate_flag"])
    assert rows.loc[2, "recent_event_decision_bucket"] == "not_event_covered"


def test_selected_vs_excluded_quality_direction() -> None:
    rows = mod.add_recent_event_selection(_rows())
    metrics = mod.build_metrics(rows)
    diff = mod.selected_vs_excluded(metrics)
    assert diff["selected_minus_excluded_mean_ret20"] > 0
    assert diff["selected_minus_excluded_bad_rate"] < 0


def test_decide_keep_with_enough_support_and_risk_improvement() -> None:
    metrics = {
        "selected_after_event_risk": {"sample_count": 10000, "date_count": 25, "mean_ret20": 0.02},
        "recent_event_covered": {"mean_ret20": -0.01},
        "excluded_earnings_nearby": {},
    }
    diff = {"selected_minus_excluded_bad_rate": -0.10, "selected_minus_excluded_severe_rate": -0.05}
    decision, cls, reasons = mod.decide(metrics, diff)
    assert decision == "recent_event_risk_ready_for_current_period_buyability_pretest"
    assert cls == "KEEP"
    assert reasons


def test_feature_contract_marks_outcomes_offline() -> None:
    contract = mod.feature_contract()
    assert contract["fields"]["ret20"]["classification"] == "offline_outcome_only"
    assert contract["fields"]["event_risk_exclusion_flag"]["classification"] == "point_in_time_feature"
