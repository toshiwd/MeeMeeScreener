from __future__ import annotations

import pandas as pd

from scripts import tradex_family_definition_v2_candidate_evaluation as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20250101, "code": "1001", "high_upside_contained_reserve_family_v2": True, "constructive_pullback_confirmation_family_v2": False, "volatility_compression_pre_breakout_family_v2": False, "ret5": 0.01, "ret20": 0.12, "winner_ret20_gt_10pct": True, "bad_ret20_lt_minus_5pct": False, "severe_ret20_lt_minus_10pct": False},
            {"as_of_date": 20250102, "code": "1002", "high_upside_contained_reserve_family_v2": True, "constructive_pullback_confirmation_family_v2": True, "volatility_compression_pre_breakout_family_v2": False, "ret5": -0.01, "ret20": -0.08, "winner_ret20_gt_10pct": False, "bad_ret20_lt_minus_5pct": True, "severe_ret20_lt_minus_10pct": False},
        ]
    )


def test_metric_counts_rates_and_zero_dates() -> None:
    rows = _rows()
    m = mod.metric(rows[rows["high_upside_contained_reserve_family_v2"]], {20250101, 20250102, 20250103})
    assert m["sample_count"] == 2
    assert m["zero_candidate_date_count"] == 1
    assert m["winner_rate_ret20_gt_10pct"] == 0.5
    assert m["bad_rate_ret20_lt_minus_5pct"] == 0.5


def test_overlap_matrix_counts_overlap() -> None:
    matrix = mod.overlap_matrix(_rows())
    assert matrix["high_upside_contained_reserve_family_v2"]["constructive_pullback_confirmation_family_v2"]["overlap_count"] == 1


def test_family_decision_drop_for_weak_positive() -> None:
    decisions = mod.family_decisions({"x": {"mean_ret20": 0.01, "winner_rate_ret20_gt_10pct": 0.1, "bad_rate_ret20_lt_minus_5pct": 0.2, "severe_rate_ret20_lt_minus_10pct": 0.08, "sample_count": 1000, "date_count": 300}})
    assert decisions["x"]["decision"] == "drop"
    assert decisions["x"]["decision_class"] == "DROP"


def test_family_decision_keep_for_strong_family() -> None:
    decisions = mod.family_decisions({"x": {"mean_ret20": 0.03, "winner_rate_ret20_gt_10pct": 0.16, "bad_rate_ret20_lt_minus_5pct": 0.15, "severe_rate_ret20_lt_minus_10pct": 0.05, "sample_count": 1000, "date_count": 300}})
    assert decisions["x"]["decision"] == "keep_for_next_stage"


def test_overall_decision_maps_hold() -> None:
    decision, reasons, cls = mod.overall_decision({"x": {"decision_class": "HOLD_UNDERPOWERED"}})
    assert decision == "promising_but_underpowered"
    assert cls == "HOLD_UNDERPOWERED"
    assert reasons
