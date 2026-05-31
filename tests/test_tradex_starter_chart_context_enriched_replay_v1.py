from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_chart_context_enriched_replay_v1 as mod


def test_feature_signatures_do_not_use_outcome_terms() -> None:
    row = pd.Series(
        {
            "pattern_type": "breakout",
            "close_vs_ma20_pct": 0.10,
            "upper_wick_ratio": 0.40,
            "monthly_supportive_flag": True,
            "failed_high_flag": True,
            "ret20": -0.10,
        }
    )

    sigs = mod.feature_signatures_for_row(row)

    assert "breakout+close_extended_vs_ma20" in sigs
    assert "breakout+upper_wick_high" in sigs
    assert not any(any(term in sig for term in mod.FORBIDDEN_SIGNATURE_TERMS) for sig in sigs)


def test_daily_context_uses_only_bars_through_decision_date() -> None:
    rows = []
    for i in range(70):
        ymd = int((pd.Timestamp("2024-01-01") + pd.Timedelta(days=i)).strftime("%Y%m%d"))
        rows.append({"code": "A", "ymd": ymd, "date": i, "dt": pd.Timestamp(str(ymd)), "o": 100 + i, "h": 101 + i, "l": 99 + i, "c": 100 + i, "v": 1000})
    rows.append({"code": "A", "ymd": 20250101, "date": 999, "dt": pd.Timestamp("2025-01-01"), "o": 1000, "h": 1000, "l": 1000, "c": 1000, "v": 1000})
    bars = pd.DataFrame(rows)

    ctx = mod.daily_context("A", bars, 20240229)

    assert ctx["chart_context_available"] is True
    assert ctx["close_vs_ma7_pct"] < 0.1


def test_missing_column_audit_reports_absent_context() -> None:
    enriched = pd.DataFrame([{"close_vs_ma7_pct": 0.01}])

    audit = mod.missing_column_audit(enriched)

    assert "close_vs_ma20_pct" in audit["missing_columns"]
    assert "close_vs_ma7_pct" not in audit["missing_columns"]


def test_decide_underpowered_when_signatures_have_small_support() -> None:
    rows = pd.DataFrame([{"chart_context_available": True} for _ in range(5)])
    metrics = {"x": {"sample_count": 5, "comparison_vs_untagged_rows": {"mean_ret20_delta_tagged_minus_untagged": -0.1}}}

    assert mod.decide(rows, ["x"], metrics, {}) == "feature_context_created_but_underpowered"
