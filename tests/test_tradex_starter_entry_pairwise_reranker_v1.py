from __future__ import annotations

import pandas as pd
import pytest

from scripts import tradex_starter_entry_pairwise_reranker_v1 as mod


def _row(period: str, date: int, code: str, baseline_rank: int, pairwise_rank: int, ret20: float) -> dict:
    return {
        "validation_period": period,
        "decision_date": date,
        "code": code,
        "baseline_rank": baseline_rank,
        "bad_risk_v1_rank": baseline_rank,
        "utility_rank": baseline_rank,
        "pairwise_rank": pairwise_rank,
        "ret20": ret20,
        "starter_good": ret20 >= 0.05,
        "starter_bad": ret20 <= -0.05,
        "selected_loser": ret20 <= -0.05,
        "selected_winner": ret20 >= 0.05,
        "immediate_adverse_entry": ret20 < 0,
        "mae20": min(ret20, 0.0),
        "mfe20": max(ret20, 0.0),
        "same_date_ret20_rank_pct": 0.9 if ret20 > 0 else 0.1,
    }


def test_pair_indices_stays_within_date_and_caps() -> None:
    rows = pd.DataFrame(
        [
            {"decision_date": 20240101, "code": "A", "baseline_rank": 1, "starter_good_abs": True, "starter_good_cross_sectional": False, "starter_bad_abs": False, "starter_bad_cross_sectional": False},
            {"decision_date": 20240101, "code": "B", "baseline_rank": 2, "starter_good_abs": False, "starter_good_cross_sectional": False, "starter_bad_abs": True, "starter_bad_cross_sectional": False},
            {"decision_date": 20240102, "code": "C", "baseline_rank": 1, "starter_good_abs": True, "starter_good_cross_sectional": False, "starter_bad_abs": False, "starter_bad_cross_sectional": False},
        ]
    )

    pos, neg, report = mod.pair_indices(rows, cap_per_date=10)

    assert len(pos) == 1
    assert len(neg) == 1
    assert report["dates_with_pairs"] == 1
    assert rows.loc[pos[0], "decision_date"] == rows.loc[neg[0], "decision_date"]


def test_comparison_includes_all_four_rankings() -> None:
    rows = pd.DataFrame(
        [
            _row("2024", 20240101, "A", 1, 6, -0.10),
            _row("2024", 20240101, "B", 2, 1, 0.12),
            _row("2024", 20240101, "C", 3, 2, 0.03),
            _row("2024", 20240101, "D", 4, 3, 0.02),
            _row("2024", 20240101, "E", 5, 4, -0.08),
            _row("2024", 20240101, "F", 6, 5, 0.10),
        ]
    )

    comp = mod.comparison(rows)
    row = comp[(comp["period"] == "2024") & (comp["topk"] == 5)].iloc[0]

    assert row["baseline_mean_ret20"] < row["pairwise_v1_mean_ret20"]
    assert row["bad_risk_v1_mean_ret20"] == row["baseline_mean_ret20"]
    assert row["utility_v1_mean_ret20"] == row["baseline_mean_ret20"]
    assert row["pairwise_delta_mean_ret20"] > 0


def test_replacement_quality_added_minus_removed() -> None:
    rows = pd.DataFrame(
        [
            _row("2024", 20240101, "A", 1, 6, -0.10),
            _row("2024", 20240101, "B", 2, 1, 0.12),
            _row("2024", 20240101, "C", 3, 2, 0.03),
            _row("2024", 20240101, "D", 4, 3, 0.02),
            _row("2024", 20240101, "E", 5, 4, -0.08),
            _row("2024", 20240101, "F", 6, 5, 0.10),
        ]
    )

    repl = mod.replacement(rows)
    row = repl[(repl["period"] == "2024") & (repl["topk"] == 5)].iloc[0]

    assert row["changed_members_count"] == 2
    assert row["added_minus_removed_ret20"] == pytest.approx(0.20)


def test_decide_keep_when_pairwise_gates_pass() -> None:
    comp = pd.DataFrame(
        [
            {"period": "2024_2026_combined", "topk": 10, "pairwise_delta_mean_ret20": 0.006, "pairwise_delta_starter_bad_rate": -0.05, "pairwise_delta_selected_loser_rate": -0.04},
            {"period": "2024", "topk": 10, "pairwise_delta_mean_ret20": 0.0, "pairwise_delta_starter_bad_rate": -0.05, "pairwise_delta_selected_loser_rate": -0.04},
            {"period": "2025", "topk": 10, "pairwise_delta_mean_ret20": 0.0, "pairwise_delta_starter_bad_rate": -0.05, "pairwise_delta_selected_loser_rate": -0.04},
            {"period": "2026_label_safe", "topk": 10, "pairwise_delta_mean_ret20": 0.0, "pairwise_delta_starter_bad_rate": -0.05, "pairwise_delta_selected_loser_rate": -0.04},
        ]
    )
    repl = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "added_minus_removed_ret20": 0.01}])

    decision = mod.decide(comp, repl)

    assert decision["research_decision"] == "keep_for_formal_challenger_compare"
