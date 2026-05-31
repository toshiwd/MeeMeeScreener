from __future__ import annotations

import pandas as pd

from scripts import tradex_intersection_family_current_period_risk_containment_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": 20260520 + idx,
                "code": f"{1000 + idx}",
                "fresh_runtime_research_watch_rank": idx + 1,
                "buy_entry_qualified": True,
                "ret20": ret20,
                "failed_high_flag": failed,
                "bearish_body_flag": bearish,
                "upper_wick_ratio": upper,
                "atr14_pct": atr,
                "realized_vol20": vol,
                "close_vs_ma20_pct": ext,
                "recent_high_distance_pct": high_dist,
                "weekly_supportive_flag": True,
                "monthly_supportive_flag": False,
            }
            for idx, (ret20, failed, bearish, upper, atr, vol, ext, high_dist) in enumerate(
                [
                    (0.12, False, False, 0.20, 0.03, 0.04, 0.05, 0.01),
                    (0.04, False, False, 0.30, 0.04, 0.05, 0.02, 0.02),
                    (-0.08, True, False, 0.60, 0.10, 0.11, 0.18, 0.05),
                ]
            )
        ]
    )


def test_add_variants_uses_fixed_point_in_time_risk_conditions() -> None:
    rows = mod.add_variants(_rows())
    assert rows["variant_a_candle_risk_clean"].tolist() == [True, True, False]
    assert rows["variant_b_volatility_extension_clean"].tolist() == [True, True, False]
    assert rows["variant_c_combined_context_risk_clean"].tolist() == [True, True, False]


def test_metric_payload_reports_risk_rates() -> None:
    metrics = mod.metric_payload(_rows())
    assert metrics["sample_count"] == 3
    assert metrics["winner_rate_ret20_gt_10pct"] == 1 / 3
    assert metrics["bad_rate_ret20_lt_minus_5pct"] == 1 / 3


def test_selected_vs_removed_shows_delta() -> None:
    rows = mod.add_variants(_rows())
    result = mod.selected_vs_removed(rows)
    assert result["variant_c_combined_context_risk_clean"]["selected_minus_removed_ret20"] > 0
    assert result["variant_c_combined_context_risk_clean"]["selected_minus_removed_bad_rate"] < 0


def test_decide_keeps_only_when_current_period_gate_passes() -> None:
    best = (
        "variant_c_combined_context_risk_clean",
        {
            "sample_count": 25,
            "date_count": 12,
            "selected_share": 0.5,
            "mean_ret20": 0.05,
            "winner_rate_ret20_gt_10pct": 0.24,
            "bad_rate_ret20_lt_minus_5pct": 0.12,
            "severe_rate_ret20_lt_minus_10pct": 0.04,
        },
    )
    baseline = {"mean_ret20": 0.01, "bad_rate_ret20_lt_minus_5pct": 0.3}
    audit = {"no_lookahead_pass": True}
    decision, decision_class, reasons = mod.decide(best, baseline, audit)
    assert decision == "intersection_current_period_risk_containment_buyable_ready"
    assert decision_class == "KEEP"
    assert "variant_c_combined_context_risk_clean_passed_current_period_buyability_gate" in reasons
