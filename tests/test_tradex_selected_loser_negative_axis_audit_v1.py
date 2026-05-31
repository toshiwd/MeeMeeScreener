from __future__ import annotations

import pandas as pd

from scripts.tradex_selected_loser_negative_axis_audit_v1 import (
    contrast_rows,
    decide,
    stable_feature_matrix,
    winner_damage_check,
)


def test_contrast_rows_compares_selected_loser_to_non_loser() -> None:
    rows = pd.DataFrame([_row(i, 0.10 if i > 5 else -0.10, failed=i <= 5) for i in range(1, 11)])

    contrast = contrast_rows(rows, "selected_non_loser")
    failed = contrast[(contrast["feature"] == "failed_high_update") & (contrast["year"] == 2024) & (contrast["topk"] == 10)].iloc[0]

    assert failed["selected_loser_true_rate"] == 1.0
    assert failed["selected_non_loser_true_rate"] == 0.0


def test_stable_feature_matrix_classifies_consistent_sign() -> None:
    rows = []
    for year in (2024, 2025, 2026):
        for topk in (5, 10):
            rows.append(
                {
                    "year": year,
                    "topk": topk,
                    "feature": "failed_high_update",
                    "feature_group": "candle_failure",
                    "selected_loser_coverage": 1.0,
                    "selected_non_loser_coverage": 1.0,
                    "mean_diff_loser_minus_compare": 1.0,
                    "true_rate_diff_loser_minus_compare": 1.0,
                }
            )
    non_loser = pd.DataFrame(rows)
    winner = non_loser.rename(columns={"selected_non_loser_coverage": "selected_winner_coverage"}).copy()

    matrix = stable_feature_matrix(non_loser, winner)

    assert matrix[matrix["feature"] == "failed_high_update"]["classification"].iloc[0] == "stable_loser_feature"


def test_winner_damage_check_excludes_dropped_family_features() -> None:
    matrix = pd.DataFrame(
        [
            {"feature": "above60_streak", "feature_group": "trend_age", "classification": "stable_loser_feature", "mean_abs_diff_vs_non_loser": 10, "mean_diff_vs_non_loser": -10},
            {"feature": "failed_high_update", "feature_group": "candle_failure", "classification": "stable_loser_feature", "mean_abs_diff_vs_non_loser": 1, "mean_diff_vs_non_loser": 1},
        ]
    )
    rows = pd.DataFrame([_row(i, -0.10 if i <= 5 else 0.10, failed=i <= 5) for i in range(1, 11)])

    damage, axes = winner_damage_check(rows, matrix)

    assert "above60_streak" not in set(damage["feature"])
    assert axes[0]["axis_name"] == "failed_high_update_high_risk"


def test_decide_reports_source_gap_when_no_axes_and_missing_inputs() -> None:
    decision = decide(pd.DataFrame(), [], ["selected_loser_rows.csv"])

    assert decision["research_decision"] == "source_or_feature_gap"


def _row(rank: int, ret20: float, failed: bool) -> dict[str, object]:
    return {
        "decision_ymd": 20240105,
        "code": str(rank),
        "year": 2024,
        "baseline_rank_recalc": rank,
        "ret20": ret20,
        "ret20_num": ret20,
        "winner20": ret20 > 0,
        "loser20": ret20 < 0,
        "failed_high_update": failed,
        "above60_streak": rank,
        "monthly_box_breakout_proxy": False,
        "monthly_high_zone_proxy": False,
        "monthly_box_inside_proxy": True,
    }
