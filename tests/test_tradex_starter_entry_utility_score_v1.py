from __future__ import annotations

import pandas as pd
import pytest

from scripts import tradex_starter_entry_utility_score_v1 as mod


def _row(period: str, date: int, code: str, baseline_rank: int, utility_rank: int, prior_rank: int, ret20: float) -> dict:
    return {
        "validation_period": period,
        "decision_date": date,
        "code": code,
        "baseline_rank": baseline_rank,
        "utility_rank": utility_rank,
        "bad_risk_v1_rank": prior_rank,
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


def test_comparison_reports_baseline_prior_and_utility() -> None:
    rows = pd.DataFrame(
        [
            _row("2024", 20240101, "A", 1, 2, 1, -0.10),
            _row("2024", 20240101, "B", 2, 1, 2, 0.12),
            _row("2024", 20240101, "C", 3, 3, 3, 0.03),
            _row("2024", 20240101, "D", 4, 4, 4, 0.02),
            _row("2024", 20240101, "E", 5, 6, 5, -0.08),
            _row("2024", 20240101, "F", 6, 5, 6, 0.10),
        ]
    )

    comp = mod.comparison(rows)
    row = comp[(comp["period"] == "2024") & (comp["topk"] == 5)].iloc[0]

    assert row["utility_mean_ret20"] > row["baseline_mean_ret20"]
    assert row["bad_risk_v1_mean_ret20"] == row["baseline_mean_ret20"]
    assert row["utility_delta_mean_ret20"] > 0


def test_replacement_quality_tracks_added_minus_removed() -> None:
    rows = pd.DataFrame(
        [
            _row("2024", 20240101, "A", 1, 2, 1, -0.10),
            _row("2024", 20240101, "B", 2, 1, 2, 0.12),
            _row("2024", 20240101, "C", 3, 3, 3, 0.03),
            _row("2024", 20240101, "D", 4, 4, 4, 0.02),
            _row("2024", 20240101, "E", 5, 6, 5, -0.08),
            _row("2024", 20240101, "F", 6, 5, 6, 0.10),
        ]
    )

    repl = mod.replacement(rows)
    row = repl[(repl["period"] == "2024") & (repl["topk"] == 5)].iloc[0]

    assert row["changed_members_count"] == 2
    assert row["added_minus_removed_ret20"] == pytest.approx(0.18)


def test_decide_keep_when_utility_gates_pass() -> None:
    rows = []
    for period in ["2024", "2025", "2026_label_safe", "2024_2026_combined"]:
        rows.append(
            {
                "period": period,
                "topk": 10,
                "utility_delta_mean_ret20": 0.006,
                "utility_delta_starter_bad_rate": -0.05,
                "utility_delta_selected_loser_rate": -0.04,
            }
        )
    comp = pd.DataFrame(rows)
    repl = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "added_minus_removed_ret20": 0.01}])

    decision = mod.decide(comp, repl)

    assert decision["research_decision"] == "keep_for_formal_challenger_compare"


def test_reports_bad_suppression_summary() -> None:
    comp = pd.DataFrame(
        [
            {
                "period": "2024_2026_combined",
                "topk": 10,
                "baseline_mean_ret20": 0.02,
                "utility_mean_ret20": 0.03,
                "utility_delta_starter_bad_rate": -0.10,
                "utility_delta_selected_loser_rate": -0.05,
                "utility_severe_loss_rate": 0.20,
                "baseline_severe_loss_rate": 0.25,
            }
        ]
    )
    repl = pd.DataFrame([{"period": "2024_2026_combined", "topk": 10, "added_minus_removed_ret20": 0.01}])
    rows = pd.DataFrame()

    _, bad = mod.reports(rows, comp, repl)

    assert bad["starter_bad_delta"] == -0.10
    assert bad["selected_loser_delta"] == -0.05
    assert bad["severe_loss_delta"] == pytest.approx(-0.05)
