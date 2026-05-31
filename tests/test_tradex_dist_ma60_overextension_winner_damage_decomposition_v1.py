from __future__ import annotations

import pandas as pd

from scripts.tradex_dist_ma60_overextension_winner_damage_decomposition_v1 import (
    changed_sets,
    concentration,
    refinement_axes,
    year_stability,
)


def test_changed_sets_reconstructs_removed_and_added_rows() -> None:
    rows = pd.DataFrame(
        [
            _row("A", 2024, 1, 2, -0.1),
            _row("B", 2024, 2, 1, 0.1),
            _row("C", 2024, 3, 3, 0.0),
        ]
    )

    removed, added = changed_sets(rows, 2024, 1)

    assert set(removed["code"]) == {"A"}
    assert set(added["code"]) == {"B"}


def test_year_stability_detects_stable_sign() -> None:
    decomp = pd.DataFrame(
        [
            {"feature": "upper_wick_ratio", "diff_helpful_minus_harmful": -0.2, "effect_size": -0.4},
            {"feature": "upper_wick_ratio", "diff_helpful_minus_harmful": -0.1, "effect_size": -0.3},
        ]
    )

    stable = year_stability(decomp)

    assert bool(stable.iloc[0]["stable_sign"])


def test_refinement_axes_excludes_dropped_families() -> None:
    stability = pd.DataFrame(
        [
            {"feature": "above20_streak", "stable_sign": True, "mean_abs_effect_size": 1.0},
            {"feature": "upper_wick_ratio", "stable_sign": True, "mean_abs_effect_size": 0.3},
        ]
    )

    axes = refinement_axes(stability)

    assert axes[0]["axis_name"] == "upper_wick_ratio_context_for_dist_ma60_guard"


def test_concentration_reports_harmful_removed_share() -> None:
    ledger = pd.DataFrame(
        [
            {"cohort": "harmful_removed", "year": 2024, "decision_ymd": 20240105, "code": "A"},
            {"cohort": "helpful_removed", "year": 2024, "decision_ymd": 20240105, "code": "B"},
        ]
    )

    result = concentration(ledger)

    assert result["harmful_removed_count"] == 1
    assert result["largest_year_share"] == 1.0


def _row(code: str, year: int, base_rank: int, chal_rank: int, ret20: float) -> dict[str, object]:
    return {
        "decision_ymd": int(f"{year}0105"),
        "code": code,
        "year": year,
        "baseline_rank_recalc": base_rank,
        "challenger_rank": chal_rank,
        "ret20_num": ret20,
        "winner20": ret20 >= 0.05,
        "loser20": ret20 <= -0.05,
    }
