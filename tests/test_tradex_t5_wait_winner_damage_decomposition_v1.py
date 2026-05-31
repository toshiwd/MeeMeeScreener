from __future__ import annotations

import pandas as pd

from scripts.tradex_t5_wait_winner_damage_decomposition_v1 import load_rows, wait_help_harm_summary, year_stability


def test_wait_help_harm_summary_counts_helped_and_harmed() -> None:
    rows = pd.DataFrame(
        [
            _row("A", True, False, 0.10, False, False),
            _row("B", False, True, -0.05, False, False),
        ]
    )
    rows["delay_helped_loser"] = rows["loser20"] & ((rows["delta_ret20"] > 0) | (~rows["delayed_loser20"]))
    rows["delay_not_helped_loser"] = rows["loser20"] & ~rows["delay_helped_loser"]
    rows["delay_harmed_winner"] = rows["winner20"] & ((rows["delta_ret20"] < 0) | (~rows["delayed_winner20_abs"]))
    rows["delay_not_harmed_winner"] = rows["winner20"] & ~rows["delay_harmed_winner"]
    rows["neutral_rows"] = ~rows["winner20"] & ~rows["loser20"]

    summary = wait_help_harm_summary(rows)

    assert int(summary[summary["cohort"] == "delay_helped_loser"]["count"].iloc[0]) == 1
    assert int(summary[summary["cohort"] == "delay_harmed_winner"]["count"].iloc[0]) == 1


def test_year_stability_summarizes_feature_sign() -> None:
    decomp = pd.DataFrame(
        [
            {"feature": "upper_wick_ratio", "diff_helped_minus_harmed": 0.2, "effect_size": 0.4},
            {"feature": "upper_wick_ratio", "diff_helped_minus_harmed": 0.1, "effect_size": 0.3},
        ]
    )

    stable = year_stability(decomp)

    assert bool(stable.iloc[0]["stable_sign"])


def _row(code: str, loser: bool, winner: bool, delta: float, delayed_loser: bool, delayed_winner: bool) -> dict[str, object]:
    return {
        "decision_ymd": 20240105,
        "code": code,
        "year": 2024,
        "baseline_rank_recalc": 1,
        "path_available": True,
        "loser20": loser,
        "winner20": winner,
        "delayed_loser20": delayed_loser,
        "delayed_winner20_abs": delayed_winner,
        "delta_ret20": delta,
        "baseline_mfe20": 0.1,
        "delayed_mfe20": 0.05,
        "baseline_mae20": -0.1,
        "delayed_mae20": -0.05,
    }
