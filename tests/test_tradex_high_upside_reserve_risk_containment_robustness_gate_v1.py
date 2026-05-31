from __future__ import annotations

import pandas as pd

from scripts import tradex_high_upside_reserve_risk_containment_robustness_gate_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"decision_date": 20250101, "code": "1001", "ret20": 0.12, "kept_by_fixed_variant": True, "period_half": "2025H1", "weekly_monthly_uptrend_proxy": True, "primary_family": "a"},
            {"decision_date": 20250101, "code": "1002", "ret20": -0.08, "kept_by_fixed_variant": False, "period_half": "2025H1", "weekly_monthly_uptrend_proxy": True, "primary_family": "a"},
            {"decision_date": 20250102, "code": "1003", "ret20": 0.07, "kept_by_fixed_variant": True, "period_half": "2025H1", "weekly_monthly_uptrend_proxy": False, "primary_family": "b"},
            {"decision_date": 20250103, "code": "1004", "ret20": -0.12, "kept_by_fixed_variant": False, "period_half": "2025H1", "weekly_monthly_uptrend_proxy": False, "primary_family": "b"},
        ]
    )


def test_date_concentration_counts_zero_candidate_dates() -> None:
    audit = mod.date_concentration_audit(_rows())
    assert audit["sample_count"] == 2
    assert audit["date_count"] == 2
    assert audit["raw_top5_date_count"] == 3
    assert audit["zero_candidate_date_count"] == 1
    assert audit["top_10_dates_share_of_samples"] == 1.0


def test_candidate_breadth_reports_kept_share() -> None:
    audit = mod.candidate_breadth_audit(_rows())
    assert audit["raw_sample_count"] == 4
    assert audit["kept_sample_count"] == 2
    assert audit["kept_share"] == 0.5
    assert audit["dates_with_at_least_two_candidates"] == 0


def test_kept_removed_quality_overall() -> None:
    quality = mod.kept_removed_quality(_rows())
    assert quality["kept_mean_ret20"] == 0.095
    assert quality["removed_mean_ret20"] == -0.1
    assert quality["kept_bad_rate"] == 0.0
    assert quality["removed_bad_rate"] == 1.0


def test_decide_underpowered_when_kept_share_fails_support() -> None:
    overall = {"kept_share": 0.289, "mean_ret20": 0.08, "bad_rate": 0.23, "severe_rate": 0.13}
    date_audit = {"date_count": 87, "top_10_dates_share_of_samples": 0.20, "zero_candidate_date_count": 166}
    period_metrics = {"periods": {"2025H1": {"sample_count": 20, "mean_ret20": 0.05, "bad_rate": 0.2}}}
    quality = {"overall": {"kept_mean_ret20": 0.08, "removed_mean_ret20": 0.04}}
    decision, reason = mod.decide(overall, date_audit, period_metrics, quality)
    assert decision == "risk_containment_promising_but_underpowered"
    assert reason


def test_decide_keep_when_support_and_stability_pass() -> None:
    overall = {"kept_share": 0.31, "mean_ret20": 0.08, "bad_rate": 0.22, "severe_rate": 0.12}
    date_audit = {"date_count": 110, "top_10_dates_share_of_samples": 0.20, "zero_candidate_date_count": 120}
    period_metrics = {
        "periods": {
            "2024H1": {"sample_count": 30, "mean_ret20": 0.04, "bad_rate": 0.2},
            "2024H2": {"sample_count": 30, "mean_ret20": 0.08, "bad_rate": 0.2},
            "2025H1": {"sample_count": 30, "mean_ret20": 0.06, "bad_rate": 0.2},
        }
    }
    quality = {"overall": {"kept_mean_ret20": 0.08, "removed_mean_ret20": 0.04}}
    decision, _ = mod.decide(overall, date_audit, period_metrics, quality)
    assert decision == "risk_containment_keep_for_pattern_portfolio_pretest"
