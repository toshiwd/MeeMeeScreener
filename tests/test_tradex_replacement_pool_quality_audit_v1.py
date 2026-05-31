from __future__ import annotations

import pandas as pd

from scripts import tradex_replacement_pool_quality_audit_v1 as mod


def test_assign_rank_bucket() -> None:
    assert mod.assign_rank_bucket(1) == "rank_1_5"
    assert mod.assign_rank_bucket(10) == "rank_6_10"
    assert mod.assign_rank_bucket(31) == "rank_31_50"
    assert mod.assign_rank_bucket(101) is None


def test_rank_bucket_metrics_reports_required_bucket() -> None:
    rows = pd.DataFrame(
        [
            {"decision_date": 20240101, "baseline_rank": 1, "ret5": 0.01, "ret10": 0.02, "ret20": 0.03},
            {"decision_date": 20240101, "baseline_rank": 11, "ret5": 0.02, "ret10": 0.03, "ret20": 0.12},
            {"decision_date": 20240102, "baseline_rank": 51, "ret5": -0.01, "ret10": -0.02, "ret20": -0.11},
        ]
    )
    metrics = mod.rank_bucket_metrics(rows)
    bucket = metrics[metrics["rank_bucket"].eq("rank_11_20")].iloc[0]
    assert bucket["sample_count"] == 1
    assert bucket["winner_rate_ret20_gt_10pct"] == 1.0


def test_decide_positive_selection_when_oracle_positive_but_next_rank_not() -> None:
    bucket_df = pd.DataFrame()
    capacity = {
        "top10_metrics": {"mean_ret20": 0.02, "bad_rate_ret20_lt_minus_5pct": 0.2},
        "removed_bottom_top10_metrics": {"mean_ret20": -0.05, "bad_rate_ret20_lt_minus_5pct": 0.7},
        "rank_11_30_metrics": {"mean_ret20": 0.01, "bad_rate_ret20_lt_minus_5pct": 0.3},
        "rank_11_50_metrics": {"mean_ret20": 0.01, "bad_rate_ret20_lt_minus_5pct": 0.3},
        "interpretation_flags": {
            "reserve_pool_has_winners_11_50": True,
            "oracle_replacements_positive": True,
            "next_rank_replacements_positive": False,
        },
    }
    contract = {"rank_contract_available": True, "ret20_contract_available": True}
    decision = mod.decide(bucket_df, capacity, contract)
    assert decision["research_decision"] == "replacement_pool_requires_positive_selection_research"


def test_decide_supports_more_demotion_when_reserve_beats_removed_bottom_top10() -> None:
    capacity = {
        "removed_bottom_top10_metrics": {"mean_ret20": -0.08, "bad_rate_ret20_lt_minus_5pct": 0.9},
        "rank_11_30_metrics": {"mean_ret20": 0.01, "bad_rate_ret20_lt_minus_5pct": 0.25},
        "rank_11_50_metrics": {"mean_ret20": 0.01, "bad_rate_ret20_lt_minus_5pct": 0.25},
        "interpretation_flags": {
            "reserve_pool_has_winners_11_50": True,
            "oracle_replacements_positive": True,
            "next_rank_replacements_positive": True,
        },
    }
    contract = {"rank_contract_available": True, "ret20_contract_available": True}
    decision = mod.decide(pd.DataFrame(), capacity, contract)
    assert decision["research_decision"] == "replacement_pool_supports_more_demotion_research"


def test_decide_blocks_missing_contract() -> None:
    decision = mod.decide(pd.DataFrame(), {}, {"rank_contract_available": False, "ret20_contract_available": True})
    assert decision["research_decision"] == "blocked_missing_rank_or_outcome_contract"
