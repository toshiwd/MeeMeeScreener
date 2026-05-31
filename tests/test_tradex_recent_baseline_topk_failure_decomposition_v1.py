from __future__ import annotations

import pandas as pd

from scripts.tradex_recent_baseline_topk_failure_decomposition_v1 import (
    add_labels,
    classify_failure_modes,
    topk_failure_tables,
)


def test_add_labels_uses_absolute_or_cross_sectional_winner_loser() -> None:
    rows = pd.DataFrame([_row(i, i, ret) for i, ret in enumerate([-0.10, -0.02, 0.00, 0.02, 0.06], start=1)])

    labeled = add_labels(rows)

    assert labeled.loc[labeled["code"] == "5", "winner20"].iloc[0] is True or bool(labeled.loc[labeled["code"] == "5", "winner20"].iloc[0])
    assert bool(labeled.loc[labeled["code"] == "1", "loser20"].iloc[0])


def test_topk_failure_tables_report_selected_losers_and_missed_winners() -> None:
    rows = pd.DataFrame([_row(i, i, 0.08 if i in {7, 8, 9} else (-0.08 if i <= 3 else 0.0)) for i in range(1, 11)])
    labeled = add_labels(rows)

    selected, missed, distance = topk_failure_tables(labeled)

    top5_selected = selected[(selected["year"] == 2024) & (selected["topk"] == 5)].iloc[0]
    top5_missed = missed[(missed["year"] == 2024) & (missed["topk"] == 5)].iloc[0]
    assert top5_selected["selected_loser_n"] >= 3
    assert top5_missed["missed_winner_n"] >= 3
    assert not distance.empty


def test_classify_failure_modes_prefers_selected_loser_when_topk_is_dirty() -> None:
    selected = pd.DataFrame(
        [{"year": 2024, "topk": 5, "selected_n": 5, "selected_loser_n": 3, "selected_loser_rate": 0.6}]
    )
    missed = pd.DataFrame(
        [{"year": 2024, "topk": 5, "candidate_winner_n": 5, "captured_winner_n": 1, "missed_winner_n": 4, "winner_capture_rate": 0.2}]
    )
    distance = pd.DataFrame(
        [{"year": 2024, "topk": 5, "rank_distance_bucket": "topk_plus_1_5", "winner_n": 2, "winner_share": 0.4}]
    )

    table, decision = classify_failure_modes(selected, missed, distance)

    assert table.iloc[0]["failure_mode"] == "selected_loser_failure"
    assert decision["primary_failure_mode"] == "selected_loser_failure"


def _row(rank: int, code: int, ret20: float) -> dict[str, object]:
    return {
        "decision_ymd": 20240105,
        "code": str(code),
        "candidate_rank": rank,
        "selection_score": 100 - rank,
        "selected_for_buy": True,
        "source_year": 2024,
        "year": 2024,
        "monthly_box_breakout_proxy": False,
        "monthly_high_zone_proxy": False,
        "monthly_box_inside_proxy": True,
        "above60_streak": rank,
        "days_since_ma60_reclaim": rank,
        "ret20": ret20,
        "ret40": ret20,
        "mae20": min(ret20, 0),
        "mfe20": max(ret20, 0),
        "monthly_box_breakout_bool": False,
        "above60_streak_numeric": rank,
        "gated_score_delta": 0,
        "ungated_score_delta": 0,
        "baseline_score": 100 - rank,
        "baseline_rank_recalc": rank,
    }
