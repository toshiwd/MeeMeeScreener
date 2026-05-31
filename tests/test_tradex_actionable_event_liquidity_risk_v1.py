from __future__ import annotations

import pandas as pd

from scripts import tradex_actionable_event_liquidity_risk_v1 as mod


def _rows() -> pd.DataFrame:
    rows = []
    for date in [20250101, 20250102]:
        for i in range(1, 6):
            rows.append(
                {
                    "as_of_date": date,
                    "code": f"10{i}",
                    "volume_vs_20d_avg": 0.4 if i == 1 else (3.0 if i == 5 else 1.0),
                    "atr14_pct": 0.20 if i == 5 else 0.02 * i,
                    "realized_vol20": 0.18 if i == 5 else 0.01 * i,
                    "gap_up_flag": i == 5,
                    "gap_down_flag": i == 1,
                    "failed_high_flag": i == 1,
                    "weekly_failed_high_flag": False,
                    "upper_wick_ratio": 0.9 if i == 1 else 0.1,
                    "bearish_body_flag": i == 1,
                    "body_ratio": 0.2 * i,
                    "ret5": 0.0,
                    "ret20": -0.08 if i == 1 else 0.03,
                    "winner_ret20_gt_10pct": False,
                    "bad_ret20_lt_minus_5pct": i == 1,
                    "severe_ret20_lt_minus_10pct": False,
                }
            )
    return pd.DataFrame(rows)


def test_build_proxy_risk_marks_high_risk_rows() -> None:
    risk = mod.build_proxy_risk(_rows())
    high = risk[risk["risk_bucket"] == "high_risk"]
    assert not high.empty
    assert high["actionable_event_liquidity_risk_v1"].all()
    assert "failed_high" in high.iloc[0]["risk_reason_codes"]
    assert not risk["actionable_contract_complete"].any()


def test_risk_metrics_include_buckets() -> None:
    risk = mod.build_proxy_risk(_rows())
    metrics = mod.risk_metrics(risk)
    assert set(metrics) == {"low_risk", "medium_risk", "high_risk"}
    assert sum(m["sample_count"] for m in metrics.values()) == len(risk)


def test_decide_proxy_when_only_proxy_sources_separate() -> None:
    metrics = {
        "low_risk": {"bad_rate_ret20_lt_minus_5pct": 0.10, "severe_rate_ret20_lt_minus_10pct": 0.02},
        "high_risk": {"bad_rate_ret20_lt_minus_5pct": 0.20, "severe_rate_ret20_lt_minus_10pct": 0.04},
    }
    decision, cls, reasons = mod.decide({"true_actionable_event_liquidity_available": False}, metrics, True)
    assert decision == "proxy_risk_contract_created_but_not_actionable"
    assert cls == "BLOCKED"
    assert reasons


def test_decide_blocks_when_no_actionable_and_no_proxy_edge() -> None:
    metrics = {
        "low_risk": {"bad_rate_ret20_lt_minus_5pct": 0.10, "severe_rate_ret20_lt_minus_10pct": 0.02},
        "high_risk": {"bad_rate_ret20_lt_minus_5pct": 0.11, "severe_rate_ret20_lt_minus_10pct": 0.025},
    }
    decision, cls, _ = mod.decide({"true_actionable_event_liquidity_available": False}, metrics, True)
    assert decision == "blocked_missing_actionable_event_liquidity_sources"
    assert cls == "BLOCKED"


def test_feature_contract_marks_outcomes_offline() -> None:
    feasibility = {
        "groups": {
            "event_flags_json": {"classification": "unavailable"},
            "liquidity_flags_json": {"classification": "unavailable"},
            "earnings_date_planned_disclosure": {"classification": "available_but_not_actionable"},
            "ex_rights_dividend_shareholder_benefit": {"classification": "available_but_not_actionable"},
        }
    }
    contract = mod.feature_contract(set(_rows().columns), feasibility)
    assert contract["fields"]["ret20"]["classification"] == "offline_outcome_only"
    assert contract["fields"]["volume_vs_20d_avg"]["classification"] == "available_proxy_only"
    assert contract["fields"]["earnings_planned"]["classification"] == "available_but_not_actionable"
