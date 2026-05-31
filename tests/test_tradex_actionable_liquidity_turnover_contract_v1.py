from __future__ import annotations

import pandas as pd

from scripts import tradex_actionable_liquidity_turnover_contract_v1 as mod


def _rows() -> pd.DataFrame:
    rows = []
    for date in [20250101, 20250102]:
        for i in range(1, 6):
            rows.append(
                {
                    "as_of_date": date,
                    "code": f"10{i}",
                    "turnover20": 1000 * i,
                    "turnover_z20": -1.2 if i == 1 else 0.2,
                    "vol_ratio5_20": 0.7,
                    "volume_vs_20d_avg": 0.5 if i == 1 else 1.1,
                    "ret5": 0.0,
                    "ret20": -0.08 if i == 1 else 0.02,
                    "winner_ret20_gt_10pct": False,
                    "bad_ret20_lt_minus_5pct": i == 1,
                    "severe_ret20_lt_minus_10pct": False,
                }
            )
    return pd.DataFrame(rows)


def test_build_liquidity_flags_marks_low_turnover_risk() -> None:
    rows = mod.build_liquidity_flags(_rows())
    risky = rows[rows["code"] == "101"]
    assert risky["actionable_liquidity_turnover_risk_flag"].all()
    assert set(risky["liquidity_bucket"]) == {"liquidity_high_risk"}
    assert "low_turnover20_by_date" in risky.iloc[0]["liquidity_risk_reason_codes"]


def test_bucket_metrics_counts_all_rows() -> None:
    rows = mod.build_liquidity_flags(_rows())
    metrics = mod.bucket_metrics(rows)
    assert sum(item["sample_count"] for item in metrics.values()) == len(rows)
    assert metrics["liquidity_high_risk"]["sample_count"] == 2


def test_decide_keep_when_point_in_time_and_risk_separates() -> None:
    metrics = {
        "liquidity_low_risk": {"bad_rate_ret20_lt_minus_5pct": 0.10, "severe_rate_ret20_lt_minus_10pct": 0.02},
        "liquidity_high_risk": {"bad_rate_ret20_lt_minus_5pct": 0.15, "severe_rate_ret20_lt_minus_10pct": 0.04},
    }
    decision, cls, reasons = mod.decide({"classification": "available_actionable_point_in_time"}, metrics)
    assert decision == "actionable_liquidity_turnover_contract_ready_for_risk_integration"
    assert cls == "KEEP"
    assert reasons


def test_decide_blocks_when_source_not_actionable() -> None:
    metrics = {
        "liquidity_low_risk": {"bad_rate_ret20_lt_minus_5pct": 0.10, "severe_rate_ret20_lt_minus_10pct": 0.02},
        "liquidity_high_risk": {"bad_rate_ret20_lt_minus_5pct": 0.15, "severe_rate_ret20_lt_minus_10pct": 0.04},
    }
    decision, cls, _ = mod.decide({"classification": "available_but_not_actionable"}, metrics)
    assert decision == "blocked_missing_actionable_liquidity_turnover_contract"
    assert cls == "BLOCKED"


def test_feature_contract_keeps_outcomes_offline() -> None:
    contract = mod.feature_contract()
    assert contract["fields"]["ret20"]["classification"] == "offline_outcome_only"
    assert contract["fields"]["turnover20"]["classification"] == "available_actionable_point_in_time"
    assert contract["fields"]["ret20_derived_tags"]["classification"] == "forbidden_future_leak"
